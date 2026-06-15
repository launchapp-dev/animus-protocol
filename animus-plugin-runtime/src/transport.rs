//! Transport-backend stdio JSON-RPC entrypoints.
//!
//! Transport plugins (HTTP, GraphQL, gRPC, WebSocket, ...) expose an external
//! surface and translate inbound requests into control RPCs against the
//! daemon. They implement
//! [`TransportBackend`](animus_transport_protocol::TransportBackend) and call
//! [`transport_backend_main`] (or
//! [`transport_backend_main_with_capabilities`]) from `#[tokio::main]`.
//!
//! These entrypoints were originally part of the crate root; they were
//! accidentally dropped in the v0.1.14 sync (commit `aed9f42`) and restored
//! here as a dedicated module, adapted to the current
//! [`animus_plugin_protocol`] wire types (`PluginCapabilities.projections`,
//! `InitializeResult.kind_capabilities`, `PluginManifest.env_required` /
//! `notification_buffer_size`).

use std::io::{self, IsTerminal, Write};
use std::sync::Arc;

use animus_plugin_protocol::{
    error_codes, HealthCheckResult, InitializeResult, PluginCapabilities, PluginInfo,
    PluginManifest, RpcError, RpcRequest, RpcResponse, PROTOCOL_VERSION,
};
use animus_transport_protocol::{
    BackendError as TransportBackendError, TransportBackend, TransportConfig,
    TRANSPORT_METHOD_SCHEMA, TRANSPORT_METHOD_SHUTDOWN, TRANSPORT_METHOD_START,
};
use anyhow::Result;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Stdout};
use tokio::sync::Mutex;

// =====================================================================
// Public entrypoints
// =====================================================================

/// Run a transport-backend plugin's stdio JSON-RPC loop.
///
/// Call this from `#[tokio::main]` in a plugin binary. `info` is the static
/// identity returned in the `initialize` response and `--manifest` output;
/// `backend` is the
/// [`TransportBackend`](animus_transport_protocol::TransportBackend)
/// implementation that handles the `transport/*` domain methods.
///
/// The function returns when stdin closes (clean shutdown) or on a fatal I/O
/// error.
///
/// # CLI behavior
///
/// If `--manifest` (or `-m`) appears in `std::env::args()`, the function
/// prints a [`PluginManifest`] derived from `info` and the backend's
/// declared capabilities to stdout, then exits with code `0`.
///
/// # Lifecycle
///
/// Unlike subject/trigger/log-storage backends, transports own an external
/// listener with explicit start and shutdown phases. The runtime dispatches
/// [`TRANSPORT_METHOD_START`](animus_transport_protocol::TRANSPORT_METHOD_START)
/// and
/// [`TRANSPORT_METHOD_SHUTDOWN`](animus_transport_protocol::TRANSPORT_METHOD_SHUTDOWN)
/// directly into the trait; the backend MUST return once the listener is
/// bound from `start` and MUST drain in-flight requests before returning
/// from `shutdown`.
///
/// # Example
///
/// ```ignore
/// use animus_plugin_protocol::PluginInfo;
/// use animus_plugin_runtime::transport_backend_main;
/// use animus_transport_protocol::PLUGIN_KIND_TRANSPORT_BACKEND;
///
/// #[tokio::main]
/// async fn main() -> anyhow::Result<()> {
///     let info = PluginInfo {
///         name: "animus-transport-http".into(),
///         version: env!("CARGO_PKG_VERSION").into(),
///         plugin_kind: PLUGIN_KIND_TRANSPORT_BACKEND.into(),
///         description: Some("HTTP transport backend".into()),
///     };
///     transport_backend_main(info, my_backend::HttpTransport::new()).await
/// }
/// ```
pub async fn transport_backend_main<B: TransportBackend + 'static>(
    info: PluginInfo,
    backend: B,
) -> Result<()> {
    transport_backend_main_with_capabilities(info, backend, Vec::new()).await
}

/// Run a transport-backend plugin's stdio JSON-RPC loop, advertising
/// additional capability strings alongside the runtime-derived defaults.
///
/// Passing an empty vector is exactly equivalent to calling
/// [`transport_backend_main`]. Used by the web-ui wrapper to advertise its
/// `web_ui` capability alongside the standard `transport/*` methods.
pub async fn transport_backend_main_with_capabilities<B: TransportBackend + 'static>(
    info: PluginInfo,
    backend: B,
    extra_capabilities: Vec<String>,
) -> Result<()> {
    let capabilities = transport_capabilities(&backend, &extra_capabilities);
    if parse_manifest_flag() {
        print_manifest_and_exit(&info, &capabilities);
    }
    refuse_terminal_stdin(&info.name);

    let backend = Arc::new(backend);
    let stdout = Arc::new(Mutex::new(tokio::io::stdout()));
    let mut reader = BufReader::new(tokio::io::stdin());

    while let Some(request) = read_frame(&mut reader).await? {
        let info = info.clone();
        let capabilities = capabilities.clone();
        let backend = backend.clone();
        let stdout = stdout.clone();
        tokio::spawn(async move {
            handle_transport_request(request, info, capabilities, backend, stdout).await;
        });
    }
    Ok(())
}

// =====================================================================
// Dispatch
// =====================================================================

async fn handle_transport_request<B: TransportBackend + 'static>(
    request: RpcRequest,
    info: PluginInfo,
    capabilities: PluginCapabilities,
    backend: Arc<B>,
    stdout: Arc<Mutex<Stdout>>,
) {
    let id = request.id.clone();
    // TODO(codex-p2): this restored loop mirrors the pre-aed9f42 behavior and,
    // like the other `*_main` loops, does not validate the host
    // `protocol_version` on `initialize` nor gate `transport/start` behind a
    // received `initialized`. Hardening these is a cross-kind protocol change
    // (subject/provider loops share the gap) and is deliberately deferred from
    // this regression-restoration so the entrypoints stay faithful to their
    // original contract.
    let response = match request.method.as_str() {
        "initialize" => Some(initialize_response(id, &info, &capabilities)),
        "initialized" => None,
        "$/ping" => Some(RpcResponse::ok(id, json!({}))),
        "health/check" => Some(transport_health_response(id, backend.health().await)),
        "shutdown" => Some(RpcResponse::ok(id, json!({}))),
        TRANSPORT_METHOD_SCHEMA => Some(match serde_json::to_value(backend.schema()) {
            Ok(value) => RpcResponse::ok(id, value),
            Err(error) => RpcResponse::err(id, encoding_error("transport/schema", error)),
        }),
        TRANSPORT_METHOD_START => {
            let config = match deserialize_params::<TransportConfig>(request.params, false) {
                Ok(value) => value,
                Err(error) => {
                    write_frame(&stdout, &RpcResponse::err(id, error)).await;
                    return;
                }
            };
            Some(match backend.start(config).await {
                Ok(reply) => match serde_json::to_value(reply) {
                    Ok(value) => RpcResponse::ok(id, value),
                    Err(error) => RpcResponse::err(id, encoding_error("transport/start", error)),
                },
                Err(error) => RpcResponse::err(id, error.into()),
            })
        }
        TRANSPORT_METHOD_SHUTDOWN => Some(match backend.shutdown().await {
            Ok(()) => RpcResponse::ok(id, json!({ "shutdown": true })),
            Err(error) => RpcResponse::err(id, error.into()),
        }),
        "exit" => std::process::exit(0),
        other if other.starts_with("$/") => None,
        other => Some(method_not_found(id, &info.name, other)),
    };

    if let Some(response) = response {
        write_frame(&stdout, &response).await;
    }
}

// =====================================================================
// Capability derivation
// =====================================================================

fn transport_capabilities<B: TransportBackend>(
    backend: &B,
    extra_capabilities: &[String],
) -> PluginCapabilities {
    let schema = backend.schema();
    let mut methods = vec![
        TRANSPORT_METHOD_START.to_string(),
        TRANSPORT_METHOD_SHUTDOWN.to_string(),
        TRANSPORT_METHOD_SCHEMA.to_string(),
        "health/check".to_string(),
    ];
    append_unique_capabilities(&mut methods, extra_capabilities);
    PluginCapabilities {
        methods,
        // Transport backends advertise streaming when their external surface
        // supports it; the runtime forwards that through verbatim so the
        // daemon can decide whether to route streaming control methods
        // (`daemon/events`, `daemon/logs`) through this transport.
        streaming: schema.supports_streaming,
        progress: false,
        cancellation: false,
        projections: Vec::new(),
        subject_kinds: Vec::new(),
        mcp_tools: Vec::new(),
    }
}

/// Append `extras` to `methods`, skipping any entries already present.
///
/// The dedup is order-preserving: the first occurrence wins. Stable ordering
/// matters because the testkit's conformance gating reads
/// `init.capabilities.methods` and humans read it in `--manifest` output.
fn append_unique_capabilities(methods: &mut Vec<String>, extras: &[String]) {
    for extra in extras {
        if !methods.iter().any(|m| m == extra) {
            methods.push(extra.clone());
        }
    }
}

// =====================================================================
// Shared helpers
// =====================================================================

/// Read one newline-delimited JSON-RPC request frame from `reader`.
///
/// Returns `Ok(None)` on EOF and `Ok(Some(_))` for each successfully parsed
/// frame. Frames that fail to parse are skipped.
async fn read_frame<R>(reader: &mut R) -> Result<Option<RpcRequest>>
where
    R: tokio::io::AsyncBufRead + Unpin,
{
    let mut buf = String::new();
    loop {
        buf.clear();
        let bytes = reader.read_line(&mut buf).await?;
        if bytes == 0 {
            return Ok(None);
        }
        let trimmed = buf.trim();
        if trimmed.is_empty() {
            continue;
        }
        match serde_json::from_str::<RpcRequest>(trimmed) {
            Ok(request) => return Ok(Some(request)),
            Err(_) => continue,
        }
    }
}

async fn write_frame<T: serde::Serialize>(stdout: &Arc<Mutex<Stdout>>, frame: &T) {
    if let Ok(mut payload) = serde_json::to_string(frame) {
        payload.push('\n');
        let mut guard = stdout.lock().await;
        let _ = guard.write_all(payload.as_bytes()).await;
        let _ = guard.flush().await;
    }
}

/// Check if `--manifest` (or `-m`) was passed on the command line.
fn parse_manifest_flag() -> bool {
    std::env::args()
        .skip(1)
        .any(|arg| arg == "--manifest" || arg == "-m")
}

/// Print a [`PluginManifest`] derived from `info` + `capabilities` to stdout
/// and exit with code `0`.
fn print_manifest_and_exit(info: &PluginInfo, capabilities: &PluginCapabilities) -> ! {
    let manifest = PluginManifest {
        name: info.name.clone(),
        version: info.version.clone(),
        plugin_kind: info.plugin_kind.clone(),
        description: info.description.clone().unwrap_or_default(),
        protocol_version: PROTOCOL_VERSION.to_string(),
        capabilities: capabilities.methods.clone(),
        env_required: Vec::new(),
        notification_buffer_size: None,
    };
    let mut stdout = io::stdout().lock();
    let _ = writeln!(
        stdout,
        "{}",
        serde_json::to_string(&manifest).expect("serialize manifest")
    );
    let _ = stdout.flush();
    std::process::exit(0);
}

fn refuse_terminal_stdin(plugin_name: &str) {
    if io::stdin().is_terminal() {
        eprintln!("{plugin_name} is a STDIO plugin; pipe JSON-RPC on stdin or pass --manifest");
        std::process::exit(2);
    }
}

fn initialize_response(
    id: Option<Value>,
    info: &PluginInfo,
    capabilities: &PluginCapabilities,
) -> RpcResponse {
    let result = InitializeResult {
        protocol_version: PROTOCOL_VERSION.to_string(),
        plugin_info: info.clone(),
        capabilities: capabilities.clone(),
        kind_capabilities: std::collections::HashMap::new(),
    };
    match serde_json::to_value(result) {
        Ok(value) => RpcResponse::ok(id, value),
        Err(error) => RpcResponse::err(id, encoding_error("initialize", error)),
    }
}

fn transport_health_response(
    id: Option<Value>,
    result: std::result::Result<HealthCheckResult, TransportBackendError>,
) -> RpcResponse {
    match result {
        Ok(health) => match serde_json::to_value(health) {
            Ok(value) => RpcResponse::ok(id, value),
            Err(error) => RpcResponse::err(id, encoding_error("health/check", error)),
        },
        Err(error) => RpcResponse::err(id, error.into()),
    }
}

fn deserialize_params<T: for<'de> Deserialize<'de>>(
    params: Option<Value>,
    allow_missing: bool,
) -> std::result::Result<T, RpcError> {
    match params {
        Some(value) => serde_json::from_value::<T>(value).map_err(|error| RpcError {
            code: error_codes::INVALID_PARAMS,
            message: format!("invalid params: {error}"),
            data: None,
        }),
        None => {
            if allow_missing {
                serde_json::from_value::<T>(Value::Object(serde_json::Map::new())).map_err(
                    |error| RpcError {
                        code: error_codes::INVALID_PARAMS,
                        message: format!("invalid params: {error}"),
                        data: None,
                    },
                )
            } else {
                Err(RpcError {
                    code: error_codes::INVALID_PARAMS,
                    message: "missing params".to_string(),
                    data: None,
                })
            }
        }
    }
}

fn method_not_found(id: Option<Value>, plugin_name: &str, method: &str) -> RpcResponse {
    RpcResponse::err(
        id,
        RpcError {
            code: error_codes::METHOD_NOT_FOUND,
            message: format!("method '{method}' not implemented by {plugin_name}"),
            data: None,
        },
    )
}

fn encoding_error(method: &str, error: serde_json::Error) -> RpcError {
    RpcError {
        code: error_codes::INTERNAL_ERROR,
        message: format!("failed to encode {method} result: {error}"),
        data: None,
    }
}
