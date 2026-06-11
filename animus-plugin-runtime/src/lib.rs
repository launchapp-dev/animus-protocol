//! Shared stdio JSON-RPC 2.0 runtime for Animus plugins.
//!
//! This crate is the wire layer plugin authors call from `main`. It owns the
//! protocol envelope, the [`initialize`](animus_plugin_protocol::InitializeResult)
//! / [`initialized`] / [`health/check`](animus_plugin_protocol::HealthCheckResult)
//! / `$/ping` / `shutdown` lifecycle, the `--manifest` discovery shortcut, and
//! dispatch to the kind-specific trait method on the supplied backend.
//!
//! Plugin authors implement either
//! [`SubjectBackend`](animus_subject_protocol::SubjectBackend),
//! [`ProviderBackend`](animus_provider_protocol::ProviderBackend),
//! [`TriggerBackend`](animus_trigger_protocol::TriggerBackend), or
//! [`LogStorageBackend`](animus_log_storage_protocol::LogStorageBackend),
//! build a [`PluginInfo`](animus_plugin_protocol::PluginInfo), and call the
//! matching entrypoint:
//!
//! - [`subject_backend_main`] for subject backends (Linear, Jira, GitHub
//!   Issues, native task store, ...).
//! - [`provider_main`] for LLM provider plugins (Claude Code, Codex, Gemini,
//!   OpenAI-compat, on-prem, ...).
//! - [`trigger_backend_main`] for trigger backends (Slack, generic webhooks,
//!   file watchers, cron, ...).
//! - [`log_storage_backend_main`] for log storage backends (local file, Loki,
//!   Splunk, ClickHouse, ...).
//! - [`transport_backend_main`] for transport backends (HTTP, GraphQL, gRPC,
//!   WebSocket, MQTT, ...) that expose external surfaces and translate
//!   inbound requests into control RPCs against the daemon.
//! - [`run_provider`] (in [`session_provider`]) for provider plugins that
//!   wrap an [`animus_session_backend::SessionBackend`] CLI wrapper instead
//!   of implementing the provider trait directly.
//!
//! Each entrypoint runs the stdio loop indefinitely: it reads
//! newline-delimited JSON-RPC frames from stdin, dispatches to the trait, and
//! writes responses to stdout. The loop returns cleanly on stdin EOF and
//! bubbles fatal errors up via [`anyhow::Result`].
//!
//! # Scope
//!
//! This crate has a small surface — the wire loop and the lifecycle helpers.
//! Provider implementations that own their session lifecycle handle it inside
//! [`ProviderBackend::run_agent`](animus_provider_protocol::ProviderBackend::run_agent)
//! and return the aggregated [`AgentRunResponse`] when the run completes.
//! Providers that wrap an `animus-session-backend` CLI wrapper can instead
//! use the [`session_provider`] module, which drives the wrapped
//! [`SessionEvent`](animus_session_backend::SessionEvent) stream and handles
//! streaming notifications, exit-code aggregation, and host control-id
//! cancel translation on their behalf.
//!
//! # See also
//!
//! - The [`spec.md`](https://github.com/launchapp-dev/animus-protocol/blob/main/spec.md)
//!   companion file in this repository — the language-agnostic protocol spec.
//! - [`animus_plugin_protocol`] for the wire types this runtime serializes.
//! - [`animus_subject_protocol`] for the subject-backend trait.
//! - [`animus_provider_protocol`] for the provider-backend trait.
//! - [`animus_trigger_protocol`] for the trigger-backend trait.
//! - [`animus_log_storage_protocol`] for the log-storage-backend trait.
//! - [`animus_transport_protocol`] for the transport-backend trait.

#![warn(missing_docs)]

pub mod log;
pub mod session_provider;

pub use session_provider::{run_provider, ProviderInfo, SessionBackendProvider};

use std::io::{self, IsTerminal, Write};
use std::sync::Arc;

use animus_log_storage_protocol::{
    BackendError as LogStorageBackendError, LogEntry, LogQuery, LogStorageBackend,
    METHOD_LOG_STORAGE_QUERY, METHOD_LOG_STORAGE_SCHEMA, METHOD_LOG_STORAGE_STORE,
    METHOD_LOG_STORAGE_TAIL, NOTIFICATION_LOG_STORAGE_EVENT,
};
use animus_plugin_protocol::{
    error_codes, HealthCheckResult, HealthStatus, InitializeResult, PluginCapabilities, PluginInfo,
    PluginManifest, RpcError, RpcNotification, RpcRequest, RpcResponse, PROTOCOL_VERSION,
};
use animus_provider_protocol::{
    AgentCancelRequest, AgentNotification, AgentRespondParams, AgentResumeRequest, AgentRunRequest,
    NotificationSink, ProviderBackend, METHOD_AGENT_CANCEL, METHOD_AGENT_RESPOND,
    METHOD_AGENT_RESUME, METHOD_AGENT_RUN,
};
use animus_subject_protocol::{
    BackendError as SubjectBackendError, SubjectBackend, SubjectFilter, SubjectId, SubjectPatch,
    METHOD_SUBJECT_GET, METHOD_SUBJECT_LIST, METHOD_SUBJECT_SCHEMA, METHOD_SUBJECT_UPDATE,
    METHOD_SUBJECT_WATCH, NOTIFICATION_SUBJECT_CHANGED,
};
use animus_transport_protocol::{
    BackendError as TransportBackendError, TransportBackend, TransportConfig,
    TRANSPORT_METHOD_SCHEMA, TRANSPORT_METHOD_SHUTDOWN, TRANSPORT_METHOD_START,
};
use animus_trigger_protocol::{
    BackendError as TriggerBackendError, TriggerBackend, METHOD_TRIGGER_ACK, METHOD_TRIGGER_SCHEMA,
    METHOD_TRIGGER_WATCH, NOTIFICATION_TRIGGER_EVENT,
};
use anyhow::Result;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader, Stdout};
use tokio::sync::Mutex;

// =====================================================================
// Public entrypoints
// =====================================================================

/// Run a subject-backend plugin's stdio JSON-RPC loop.
///
/// Call this from `#[tokio::main]` in a plugin binary. `info` is the static
/// identity returned in the `initialize` response and `--manifest` output;
/// `backend` is the
/// [`SubjectBackend`](animus_subject_protocol::SubjectBackend) implementation
/// that handles the `subject/*` domain methods.
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
/// # Example
///
/// ```ignore
/// use animus_plugin_protocol::{PluginInfo, PLUGIN_KIND_SUBJECT_BACKEND};
/// use animus_plugin_runtime::subject_backend_main;
///
/// #[tokio::main]
/// async fn main() -> anyhow::Result<()> {
///     let info = PluginInfo {
///         name: "animus-subject-linear".into(),
///         version: env!("CARGO_PKG_VERSION").into(),
///         plugin_kind: PLUGIN_KIND_SUBJECT_BACKEND.into(),
///         description: Some("Linear subject backend".into()),
///     };
///     subject_backend_main(info, my_backend::LinearBackend::new()).await
/// }
/// ```
pub async fn subject_backend_main<B: SubjectBackend + 'static>(
    info: PluginInfo,
    backend: B,
) -> Result<()> {
    subject_backend_main_with_capabilities(info, backend, Vec::new()).await
}

/// Run a subject-backend plugin's stdio JSON-RPC loop, advertising
/// additional capability strings alongside the runtime-derived defaults.
///
/// Use this when the backend wants to opt in to host-side test scenarios
/// or feature flags that the runtime cannot detect from the trait — e.g.
/// the testkit's `$harness/*` opt-in capabilities. Any string in
/// `extra_capabilities` is appended (deduplicated) to
/// [`PluginCapabilities::methods`] returned in the `initialize` response.
/// Passing an empty vector is exactly equivalent to calling
/// [`subject_backend_main`].
///
/// Added in protocol v0.1.13. Plugins built against v0.1.12 continue to
/// compile and run unchanged — the new entrypoint is purely additive.
pub async fn subject_backend_main_with_capabilities<B: SubjectBackend + 'static>(
    info: PluginInfo,
    backend: B,
    extra_capabilities: Vec<String>,
) -> Result<()> {
    let capabilities = subject_capabilities(&backend, &extra_capabilities);
    if parse_manifest_flag() {
        print_manifest_and_exit(&info, &capabilities);
    }
    refuse_terminal_stdin(&info.name);

    let backend = Arc::new(backend);
    let stdout = Arc::new(Mutex::new(tokio::io::stdout()));
    install_log_forwarder(stdout.clone());
    let mut reader = BufReader::new(tokio::io::stdin());

    while let Some(request) = read_frame(&mut reader).await? {
        let info = info.clone();
        let capabilities = capabilities.clone();
        let backend = backend.clone();
        let stdout = stdout.clone();
        tokio::spawn(async move {
            handle_subject_request(request, info, capabilities, backend, stdout).await;
        });
    }
    Ok(())
}

/// Run a provider-plugin stdio JSON-RPC loop.
///
/// Call this from `#[tokio::main]` in a plugin binary. `info` is the static
/// identity returned in the `initialize` response and `--manifest` output;
/// `backend` is the
/// [`ProviderBackend`](animus_provider_protocol::ProviderBackend)
/// implementation that handles the `agent/*` domain methods.
///
/// The function returns when stdin closes (clean shutdown) or on a fatal I/O
/// error.
///
/// # CLI behavior
///
/// If `--manifest` (or `-m`) appears in `std::env::args()`, the function
/// prints a [`PluginManifest`] derived from `info` and the provider's
/// declared capabilities to stdout, then exits with code `0`.
///
/// # Example
///
/// ```ignore
/// use animus_plugin_protocol::{PluginInfo, PLUGIN_KIND_PROVIDER};
/// use animus_plugin_runtime::provider_main;
///
/// #[tokio::main]
/// async fn main() -> anyhow::Result<()> {
///     let info = PluginInfo {
///         name: "animus-provider-claude".into(),
///         version: env!("CARGO_PKG_VERSION").into(),
///         plugin_kind: PLUGIN_KIND_PROVIDER.into(),
///         description: Some("Claude Code CLI provider".into()),
///     };
///     provider_main(info, my_provider::ClaudeProvider::new()).await
/// }
/// ```
pub async fn provider_main<P: ProviderBackend + 'static>(
    info: PluginInfo,
    backend: P,
) -> Result<()> {
    provider_main_with_capabilities(info, backend, Vec::new()).await
}

/// Run a provider-plugin stdio JSON-RPC loop, advertising additional
/// capability strings alongside the runtime-derived defaults.
///
/// Use this when the provider wants to opt in to host-side test
/// scenarios or feature flags that the runtime cannot detect from the
/// trait — e.g. the testkit's `$harness/cancellation-loop-v2` or
/// `$harness/oai-style` opt-in capabilities. Any string in
/// `extra_capabilities` is appended (deduplicated) to
/// [`PluginCapabilities::methods`] returned in the `initialize`
/// response. Passing an empty vector is exactly equivalent to calling
/// [`provider_main`].
///
/// Added in protocol v0.1.13. Plugins built against v0.1.12 continue to
/// compile and run unchanged — the new entrypoint is purely additive.
///
/// # Example
///
/// ```ignore
/// use animus_plugin_protocol::{PluginInfo, PLUGIN_KIND_PROVIDER};
/// use animus_plugin_runtime::provider_main_with_capabilities;
///
/// #[tokio::main]
/// async fn main() -> anyhow::Result<()> {
///     let info = PluginInfo { /* … */ };
///     provider_main_with_capabilities(
///         info,
///         my_provider::OaiProvider::new(),
///         vec!["$harness/oai-style".to_string()],
///     ).await
/// }
/// ```
pub async fn provider_main_with_capabilities<P: ProviderBackend + 'static>(
    info: PluginInfo,
    backend: P,
    extra_capabilities: Vec<String>,
) -> Result<()> {
    let capabilities = provider_capabilities(&backend, &extra_capabilities);
    if parse_manifest_flag() {
        print_manifest_and_exit(&info, &capabilities);
    }
    refuse_terminal_stdin(&info.name);

    let backend = Arc::new(backend);
    let stdout = Arc::new(Mutex::new(tokio::io::stdout()));
    install_log_forwarder(stdout.clone());
    let mut reader = BufReader::new(tokio::io::stdin());

    while let Some(request) = read_frame(&mut reader).await? {
        let info = info.clone();
        let capabilities = capabilities.clone();
        let backend = backend.clone();
        let stdout = stdout.clone();
        tokio::spawn(async move {
            handle_provider_request(request, info, capabilities, backend, stdout).await;
        });
    }
    Ok(())
}

/// Run a trigger-backend plugin's stdio JSON-RPC loop.
///
/// Call this from `#[tokio::main]` in a plugin binary. `info` is the static
/// identity returned in the `initialize` response and `--manifest` output;
/// `backend` is the
/// [`TriggerBackend`](animus_trigger_protocol::TriggerBackend) implementation
/// that handles the `trigger/*` domain methods.
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
/// # Streaming
///
/// On the first `trigger/watch` request, the runtime calls
/// [`TriggerBackend::watch`], replies immediately with
/// `{ "watching": true }`, and spawns a task that drains the returned
/// stream — emitting each [`TriggerEvent`](animus_trigger_protocol::TriggerEvent)
/// as a [`NOTIFICATION_TRIGGER_EVENT`] notification carrying the original
/// watch-request id in `params.id`. Stream-level errors are forwarded as
/// notifications with an `error` field and terminate the watch.
///
/// # Example
///
/// ```ignore
/// use animus_plugin_protocol::{PluginInfo, PLUGIN_KIND_TRIGGER_BACKEND};
/// use animus_plugin_runtime::trigger_backend_main;
///
/// #[tokio::main]
/// async fn main() -> anyhow::Result<()> {
///     let info = PluginInfo {
///         name: "animus-trigger-slack".into(),
///         version: env!("CARGO_PKG_VERSION").into(),
///         plugin_kind: PLUGIN_KIND_TRIGGER_BACKEND.into(),
///         description: Some("Slack trigger backend".into()),
///     };
///     trigger_backend_main(info, my_backend::SlackBackend::new()).await
/// }
/// ```
pub async fn trigger_backend_main<B: TriggerBackend + 'static>(
    info: PluginInfo,
    backend: B,
) -> Result<()> {
    trigger_backend_main_with_capabilities(info, backend, Vec::new()).await
}

/// Run a trigger-backend plugin's stdio JSON-RPC loop, advertising
/// additional capability strings alongside the runtime-derived defaults.
///
/// See [`provider_main_with_capabilities`] for the same pattern applied
/// to provider backends. Passing an empty vector is exactly equivalent
/// to calling [`trigger_backend_main`].
///
/// Added in protocol v0.1.13. Plugins built against v0.1.12 continue to
/// compile and run unchanged.
pub async fn trigger_backend_main_with_capabilities<B: TriggerBackend + 'static>(
    info: PluginInfo,
    backend: B,
    extra_capabilities: Vec<String>,
) -> Result<()> {
    let capabilities = trigger_capabilities(&backend, &extra_capabilities);
    if parse_manifest_flag() {
        print_manifest_and_exit(&info, &capabilities);
    }
    refuse_terminal_stdin(&info.name);

    let backend = Arc::new(backend);
    let stdout = Arc::new(Mutex::new(tokio::io::stdout()));
    install_log_forwarder(stdout.clone());
    let mut reader = BufReader::new(tokio::io::stdin());

    while let Some(request) = read_frame(&mut reader).await? {
        let info = info.clone();
        let capabilities = capabilities.clone();
        let backend = backend.clone();
        let stdout = stdout.clone();
        tokio::spawn(async move {
            handle_trigger_request(request, info, capabilities, backend, stdout).await;
        });
    }
    Ok(())
}

/// Run a log-storage-backend plugin's stdio JSON-RPC loop.
///
/// Call this from `#[tokio::main]` in a plugin binary. `info` is the static
/// identity returned in the `initialize` response and `--manifest` output;
/// `backend` is the
/// [`LogStorageBackend`](animus_log_storage_protocol::LogStorageBackend)
/// implementation that handles the `log_storage/*` domain methods.
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
/// # Streaming
///
/// On `log_storage/tail` the runtime calls
/// [`LogStorageBackend::tail`](animus_log_storage_protocol::LogStorageBackend::tail),
/// replies immediately with `{ "tailing": true }`, and spawns a task that
/// drains the returned stream — emitting each [`LogEntry`] as a
/// [`NOTIFICATION_LOG_STORAGE_EVENT`] notification carrying the original
/// tail-request id in `params.id`. Stream-level errors are forwarded as
/// notifications with an `error` field and terminate the tail.
///
/// # Example
///
/// ```ignore
/// use animus_plugin_protocol::PluginInfo;
/// use animus_plugin_runtime::log_storage_backend_main;
/// use animus_log_storage_protocol::PLUGIN_KIND_LOG_STORAGE_BACKEND;
///
/// #[tokio::main]
/// async fn main() -> anyhow::Result<()> {
///     let info = PluginInfo {
///         name: "animus-log-storage-file".into(),
///         version: env!("CARGO_PKG_VERSION").into(),
///         plugin_kind: PLUGIN_KIND_LOG_STORAGE_BACKEND.into(),
///         description: Some("Local events.jsonl log storage".into()),
///     };
///     log_storage_backend_main(info, my_backend::FileBackend::new()).await
/// }
/// ```
pub async fn log_storage_backend_main<B: LogStorageBackend + 'static>(
    info: PluginInfo,
    backend: B,
) -> Result<()> {
    log_storage_backend_main_with_capabilities(info, backend, Vec::new()).await
}

/// Run a log-storage-backend plugin's stdio JSON-RPC loop, advertising
/// additional capability strings alongside the runtime-derived defaults.
///
/// See [`provider_main_with_capabilities`] for the same pattern applied
/// to provider backends. Passing an empty vector is exactly equivalent
/// to calling [`log_storage_backend_main`].
///
/// Added in protocol v0.1.13. Plugins built against v0.1.12 continue to
/// compile and run unchanged.
pub async fn log_storage_backend_main_with_capabilities<B: LogStorageBackend + 'static>(
    info: PluginInfo,
    backend: B,
    extra_capabilities: Vec<String>,
) -> Result<()> {
    let capabilities = log_storage_capabilities(&backend, &extra_capabilities);
    if parse_manifest_flag() {
        print_manifest_and_exit(&info, &capabilities);
    }
    refuse_terminal_stdin(&info.name);

    let backend = Arc::new(backend);
    let stdout = Arc::new(Mutex::new(tokio::io::stdout()));
    install_log_forwarder(stdout.clone());
    let mut reader = BufReader::new(tokio::io::stdin());

    while let Some(request) = read_frame(&mut reader).await? {
        let info = info.clone();
        let capabilities = capabilities.clone();
        let backend = backend.clone();
        let stdout = stdout.clone();
        tokio::spawn(async move {
            handle_log_storage_request(request, info, capabilities, backend, stdout).await;
        });
    }
    Ok(())
}

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
/// See [`provider_main_with_capabilities`] for the same pattern applied
/// to provider backends. Passing an empty vector is exactly equivalent
/// to calling [`transport_backend_main`].
///
/// Added in protocol v0.1.13. Plugins built against v0.1.12 continue to
/// compile and run unchanged.
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
    install_log_forwarder(stdout.clone());
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
// Subject dispatch
// =====================================================================

#[derive(Debug, Deserialize)]
struct SubjectGetParams {
    id: SubjectId,
}

#[derive(Debug, Deserialize)]
struct SubjectUpdateParams {
    id: SubjectId,
    #[serde(default)]
    patch: SubjectPatch,
}

async fn handle_subject_request<B: SubjectBackend + 'static>(
    request: RpcRequest,
    info: PluginInfo,
    capabilities: PluginCapabilities,
    backend: Arc<B>,
    stdout: Arc<Mutex<Stdout>>,
) {
    let id = request.id.clone();
    let method = request.method.as_str();

    // Literal protocol-level methods bypass kind/verb routing.
    let literal_response = match method {
        "initialize" => Some(Some(initialize_response(id.clone(), &info, &capabilities))),
        "initialized" => Some(None),
        "$/ping" => Some(Some(RpcResponse::ok(id.clone(), json!({})))),
        "health/check" => Some(Some(health_response(id.clone(), backend.health().await))),
        "shutdown" => Some(Some(RpcResponse::ok(id.clone(), json!({})))),
        other if other.starts_with("$/") => Some(None),
        _ => None,
    };
    if let Some(maybe_response) = literal_response {
        if let Some(response) = maybe_response {
            write_response(&stdout, &response).await;
        }
        return;
    }

    // Domain methods on the subject wire are `<kind>/<verb>` (e.g.
    // `task/list`, `issue/get`). Legacy `subject/<verb>` callers are
    // accepted as a pass-through where `kind` is left untouched.
    let (kind, verb) = subject_method_parts(method);
    let response = match verb {
        "list" => {
            let mut filter = match deserialize_params::<SubjectFilter>(request.params, true) {
                Ok(value) => value,
                Err(error) => {
                    write_response(&stdout, &RpcResponse::err(id, error)).await;
                    return;
                }
            };
            inject_kind_into_filter(&mut filter, kind);
            Some(match backend.list(filter).await {
                Ok(list) => match serde_json::to_value(list) {
                    Ok(value) => RpcResponse::ok(id, value),
                    Err(error) => RpcResponse::err(id, encoding_error("subject/list", error)),
                },
                Err(error) => RpcResponse::err(id, error.into()),
            })
        }
        "get" => {
            let params = match deserialize_params::<SubjectGetParams>(request.params, false) {
                Ok(value) => value,
                Err(error) => {
                    write_response(&stdout, &RpcResponse::err(id, error)).await;
                    return;
                }
            };
            Some(match backend.get(&params.id).await {
                Ok(subject) => match serde_json::to_value(subject) {
                    Ok(value) => RpcResponse::ok(id, value),
                    Err(error) => RpcResponse::err(id, encoding_error("subject/get", error)),
                },
                Err(error) => RpcResponse::err(id, error.into()),
            })
        }
        "update" => {
            let params = match deserialize_params::<SubjectUpdateParams>(request.params, false) {
                Ok(value) => value,
                Err(error) => {
                    write_response(&stdout, &RpcResponse::err(id, error)).await;
                    return;
                }
            };
            Some(match backend.update(&params.id, params.patch).await {
                Ok(subject) => match serde_json::to_value(subject) {
                    Ok(value) => RpcResponse::ok(id, value),
                    Err(error) => RpcResponse::err(id, encoding_error("subject/update", error)),
                },
                Err(error) => RpcResponse::err(id, error.into()),
            })
        }
        "delete" => {
            let params = match deserialize_params::<SubjectGetParams>(request.params, false) {
                Ok(value) => value,
                Err(error) => {
                    write_response(&stdout, &RpcResponse::err(id, error)).await;
                    return;
                }
            };
            Some(match backend.delete(&params.id).await {
                Ok(response) => match serde_json::to_value(response) {
                    Ok(value) => RpcResponse::ok(id, value),
                    Err(error) => RpcResponse::err(id, encoding_error("subject/delete", error)),
                },
                Err(error) => RpcResponse::err(id, error.into()),
            })
        }
        "watch" => match backend.watch().await {
            Some(stream) => {
                let request_id = id.clone();
                let stdout = stdout.clone();
                tokio::spawn(async move {
                    drive_watch_stream(request_id, stream, stdout).await;
                });
                Some(RpcResponse::ok(id, json!({ "watching": true })))
            }
            None => Some(RpcResponse::err(
                id,
                RpcError {
                    code: error_codes::METHOD_NOT_SUPPORTED,
                    message: format!("{} does not implement subject/watch", info.name),
                    data: None,
                },
            )),
        },
        "schema" => Some(match serde_json::to_value(backend.schema()) {
            Ok(value) => RpcResponse::ok(id, value),
            Err(error) => RpcResponse::err(id, encoding_error("subject/schema", error)),
        }),
        _ => Some(method_not_found(id, &info.name, method)),
    };

    if let Some(response) = response {
        write_response(&stdout, &response).await;
    }
}

/// Split a subject wire method into its `(kind, verb)` parts.
///
/// The daemon's `SubjectRouter` dispatches calls as `<kind>/<verb>` (e.g.
/// `task/list`, `issue/get`) where the prefix is the subject `kind` claimed
/// by a backend. Legacy `subject/<verb>` callers are accepted for backwards
/// compatibility; in that case the literal `subject` prefix is treated as
/// "no specific kind" and returned as `kind = ""` so callers know not to
/// inject it into a [`SubjectFilter`].
///
/// Bare verbs (no `/`) are reported as `kind = ""`, `verb = method`.
fn subject_method_parts(method: &str) -> (&str, &str) {
    match method.split_once('/') {
        Some(("subject", verb)) => ("", verb),
        Some((prefix, verb)) => (prefix, verb),
        None => ("", method),
    }
}

/// Inject the wire-level `kind` into a [`SubjectFilter`] when the filter
/// does not already constrain on kind. This lets multi-kind backends (e.g.
/// `animus-subject-sqlite` advertising both `task` and `issue`) know which
/// kind the caller addressed in the wire method (e.g. `task/list`).
fn inject_kind_into_filter(filter: &mut SubjectFilter, kind: &str) {
    if kind.is_empty() {
        return;
    }
    if filter.kind.is_empty() {
        filter.kind.push(kind.to_string());
    }
}

async fn drive_watch_stream(
    request_id: Option<Value>,
    mut stream: animus_subject_protocol::EventStream,
    stdout: Arc<Mutex<Stdout>>,
) {
    use futures_core::Stream;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    // The outer `Pin<Box<dyn Stream>>` is `Unpin`, so `Pin::new(&mut stream)`
    // suffices to project a `Pin<&mut dyn Stream>` for `poll_next`. We avoid
    // pulling in `futures-util` and use `std::future::poll_fn` instead.
    std::future::poll_fn(|cx: &mut Context<'_>| loop {
        match Pin::new(&mut stream).poll_next(cx) {
            Poll::Ready(Some(event)) => {
                let event_value = match serde_json::to_value(&event) {
                    Ok(value) => value,
                    Err(_) => continue,
                };
                let mut payload = serde_json::Map::new();
                if let Some(id) = request_id.clone() {
                    payload.insert("id".to_string(), id);
                }
                payload.insert("event".to_string(), event_value);
                let notification = RpcNotification::new(
                    NOTIFICATION_SUBJECT_CHANGED,
                    Some(Value::Object(payload)),
                );
                let stdout = stdout.clone();
                tokio::spawn(async move {
                    write_notification(&stdout, &notification).await;
                });
            }
            Poll::Ready(None) => return Poll::Ready(()),
            Poll::Pending => return Poll::Pending,
        }
    })
    .await;
}

// =====================================================================
// Provider dispatch
// =====================================================================

async fn handle_provider_request<P: ProviderBackend + 'static>(
    request: RpcRequest,
    info: PluginInfo,
    capabilities: PluginCapabilities,
    backend: Arc<P>,
    stdout: Arc<Mutex<Stdout>>,
) {
    let id = request.id.clone();
    let response = match request.method.as_str() {
        "initialize" => Some(initialize_response(id, &info, &capabilities)),
        "initialized" => None,
        "$/ping" => Some(RpcResponse::ok(id, json!({}))),
        "health/check" => Some(provider_health_response(id, backend.health().await)),
        "shutdown" => Some(RpcResponse::ok(id, json!({}))),
        METHOD_AGENT_RUN => {
            let request_payload = match deserialize_params::<AgentRunRequest>(request.params, false)
            {
                Ok(value) => value,
                Err(error) => {
                    write_response(&stdout, &RpcResponse::err(id, error)).await;
                    return;
                }
            };
            let (sink, forwarder) = build_notification_sink(stdout.clone());
            let result = backend.run_agent_streaming(request_payload, sink).await;
            // Sink was consumed by `run_agent_streaming`; once the future
            // resolves, the receiver-side channel sees EOF and the
            // forwarder drains any buffered notifications.
            forwarder.close().await;
            Some(match result {
                Ok(reply) => match serde_json::to_value(reply) {
                    Ok(value) => RpcResponse::ok(id, value),
                    Err(error) => RpcResponse::err(id, encoding_error("agent/run", error)),
                },
                Err(error) => RpcResponse::err(id, error.into()),
            })
        }
        METHOD_AGENT_RESUME => {
            let request_payload =
                match deserialize_params::<AgentResumeRequest>(request.params, false) {
                    Ok(value) => value,
                    Err(error) => {
                        write_response(&stdout, &RpcResponse::err(id, error)).await;
                        return;
                    }
                };
            let (sink, forwarder) = build_notification_sink(stdout.clone());
            let result = backend.resume_agent_streaming(request_payload, sink).await;
            forwarder.close().await;
            Some(match result {
                Ok(reply) => match serde_json::to_value(reply) {
                    Ok(value) => RpcResponse::ok(id, value),
                    Err(error) => RpcResponse::err(id, encoding_error("agent/resume", error)),
                },
                Err(error) => RpcResponse::err(id, error.into()),
            })
        }
        METHOD_AGENT_CANCEL => {
            let params = match deserialize_params::<AgentCancelRequest>(request.params, false) {
                Ok(value) => value,
                Err(error) => {
                    write_response(&stdout, &RpcResponse::err(id, error)).await;
                    return;
                }
            };
            Some(match backend.cancel_agent(&params.session_id).await {
                Ok(()) => RpcResponse::ok(
                    id,
                    json!({ "session_id": params.session_id, "cancelled": true }),
                ),
                Err(error) => RpcResponse::err(id, error.into()),
            })
        }
        METHOD_AGENT_RESPOND => {
            let params = match deserialize_params::<AgentRespondParams>(request.params, false) {
                Ok(value) => value,
                Err(error) => {
                    write_response(&stdout, &RpcResponse::err(id, error)).await;
                    return;
                }
            };
            Some(match backend.respond_interaction(params).await {
                Ok(reply) => match serde_json::to_value(reply) {
                    Ok(value) => RpcResponse::ok(id, value),
                    Err(error) => RpcResponse::err(id, encoding_error("agent/respond", error)),
                },
                Err(error) => RpcResponse::err(id, error.into()),
            })
        }
        other if other.starts_with("$/") => None,
        other => Some(method_not_found(id, &info.name, other)),
    };

    if let Some(response) = response {
        write_response(&stdout, &response).await;
    }
}

/// Handle to a notification-forwarder task spawned by
/// [`build_notification_sink`].
///
/// Provider plugins emit [`AgentNotification`] frames through the
/// [`NotificationSink`] half; the runtime spawns a dedicated task that
/// reads each emission off an unbounded mpsc queue and writes it to stdout
/// as a JSON-RPC notification. Calling [`NotificationForwarder::close`]
/// after the provider's `run_agent_streaming` future resolves drains any
/// buffered notifications before we serialize the final response, which is
/// important so per-spec ordering (`notifications` followed by the terminal
/// `response`) is preserved on the wire.
pub(crate) struct NotificationForwarder {
    task: tokio::task::JoinHandle<()>,
}

impl NotificationForwarder {
    /// Wait for the forwarder task to drain any buffered notifications.
    ///
    /// The caller MUST drop every [`NotificationSink`] clone before calling
    /// this — otherwise the underlying mpsc channel stays open and the
    /// forwarder will block forever on `recv`. The dispatch path in
    /// [`handle_provider_request`] consumes the sink by passing it into
    /// `run_agent_streaming`, so by the time the call returns the only
    /// remaining `tx` lives inside the sink closure, which is dropped with
    /// the sink itself.
    pub(crate) async fn close(self) {
        // Best-effort join; we don't surface errors back through the RPC
        // response. A panic on the task is already surfaced through tokio's
        // panic hook.
        let _ = self.task.await;
    }
}

/// Construct a [`NotificationSink`] that forwards each emitted
/// [`AgentNotification`] as a JSON-RPC notification on the supplied
/// shared stdout handle, plus a [`NotificationForwarder`] that owns the
/// background drain task. The returned sink may be cloned freely by
/// providers; the drain task exits once every clone is dropped (which
/// happens naturally when the provider's `run_agent_streaming` future
/// resolves and the sink we passed in goes out of scope).
pub(crate) fn build_notification_sink(
    stdout: Arc<Mutex<Stdout>>,
) -> (NotificationSink, NotificationForwarder) {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<AgentNotification>();
    let sink = NotificationSink::new(move |notification| {
        // Ignore send errors — they only happen after the receiver was
        // closed, which means the run already returned and any late
        // emission would be ordered after the terminal response anyway.
        let _ = tx.send(notification);
    });
    let task = tokio::spawn(async move {
        while let Some(notification) = rx.recv().await {
            let frame = RpcNotification::new(notification.method(), Some(notification.payload()));
            write_notification(&stdout, &frame).await;
        }
    });
    (sink, NotificationForwarder { task })
}

// =====================================================================
// Trigger dispatch
// =====================================================================

#[derive(Debug, Deserialize)]
struct TriggerAckParams {
    event_id: String,
}

async fn handle_trigger_request<B: TriggerBackend + 'static>(
    request: RpcRequest,
    info: PluginInfo,
    capabilities: PluginCapabilities,
    backend: Arc<B>,
    stdout: Arc<Mutex<Stdout>>,
) {
    let id = request.id.clone();
    let response = match request.method.as_str() {
        "initialize" => Some(initialize_response(id, &info, &capabilities)),
        "initialized" => None,
        "$/ping" => Some(RpcResponse::ok(id, json!({}))),
        "health/check" => Some(trigger_health_response(id, backend.health().await)),
        "shutdown" => Some(RpcResponse::ok(id, json!({}))),
        METHOD_TRIGGER_SCHEMA => Some(match serde_json::to_value(backend.schema()) {
            Ok(value) => RpcResponse::ok(id, value),
            Err(error) => RpcResponse::err(id, encoding_error("trigger/schema", error)),
        }),
        METHOD_TRIGGER_WATCH => match backend.watch().await {
            Ok(stream) => {
                let request_id = id.clone();
                let stdout = stdout.clone();
                tokio::spawn(async move {
                    drive_trigger_stream(request_id, stream, stdout).await;
                });
                Some(RpcResponse::ok(id, json!({ "watching": true })))
            }
            Err(error) => Some(RpcResponse::err(id, error.into())),
        },
        METHOD_TRIGGER_ACK => {
            let params = match deserialize_params::<TriggerAckParams>(request.params, false) {
                Ok(value) => value,
                Err(error) => {
                    write_response(&stdout, &RpcResponse::err(id, error)).await;
                    return;
                }
            };
            Some(match backend.ack(&params.event_id).await {
                Ok(()) => {
                    RpcResponse::ok(id, json!({ "event_id": params.event_id, "acked": true }))
                }
                Err(error) => RpcResponse::err(id, error.into()),
            })
        }
        other if other.starts_with("$/") => None,
        other => Some(method_not_found(id, &info.name, other)),
    };

    if let Some(response) = response {
        write_response(&stdout, &response).await;
    }
}

async fn drive_trigger_stream(
    request_id: Option<Value>,
    mut stream: animus_trigger_protocol::TriggerStream,
    stdout: Arc<Mutex<Stdout>>,
) {
    use futures_core::Stream;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    // The outer `Pin<Box<dyn Stream>>` is `Unpin`, so `Pin::new(&mut stream)`
    // suffices to project a `Pin<&mut dyn Stream>` for `poll_next`. We avoid
    // pulling in `futures-util` and use `std::future::poll_fn` instead.
    std::future::poll_fn(|cx: &mut Context<'_>| loop {
        match Pin::new(&mut stream).poll_next(cx) {
            Poll::Ready(Some(Ok(event))) => {
                let event_value = match serde_json::to_value(&event) {
                    Ok(value) => value,
                    Err(_) => continue,
                };
                let mut payload = serde_json::Map::new();
                if let Some(id) = request_id.clone() {
                    payload.insert("id".to_string(), id);
                }
                payload.insert("event".to_string(), event_value);
                let notification =
                    RpcNotification::new(NOTIFICATION_TRIGGER_EVENT, Some(Value::Object(payload)));
                let stdout = stdout.clone();
                tokio::spawn(async move {
                    write_notification(&stdout, &notification).await;
                });
            }
            Poll::Ready(Some(Err(error))) => {
                let rpc_error: RpcError = error.into();
                let error_value = match serde_json::to_value(&rpc_error) {
                    Ok(value) => value,
                    Err(_) => continue,
                };
                let mut payload = serde_json::Map::new();
                if let Some(id) = request_id.clone() {
                    payload.insert("id".to_string(), id);
                }
                payload.insert("error".to_string(), error_value);
                let notification =
                    RpcNotification::new(NOTIFICATION_TRIGGER_EVENT, Some(Value::Object(payload)));
                let stdout = stdout.clone();
                tokio::spawn(async move {
                    write_notification(&stdout, &notification).await;
                });
                return Poll::Ready(());
            }
            Poll::Ready(None) => return Poll::Ready(()),
            Poll::Pending => return Poll::Pending,
        }
    })
    .await;
}

// =====================================================================
// Log storage dispatch
// =====================================================================

#[derive(Debug, Deserialize)]
struct LogStorageStoreParams {
    entries: Vec<LogEntry>,
}

async fn handle_log_storage_request<B: LogStorageBackend + 'static>(
    request: RpcRequest,
    info: PluginInfo,
    capabilities: PluginCapabilities,
    backend: Arc<B>,
    stdout: Arc<Mutex<Stdout>>,
) {
    let id = request.id.clone();
    let response = match request.method.as_str() {
        "initialize" => Some(initialize_response(id, &info, &capabilities)),
        "initialized" => None,
        "$/ping" => Some(RpcResponse::ok(id, json!({}))),
        "health/check" => Some(log_storage_health_response(id, backend.health().await)),
        "shutdown" => Some(RpcResponse::ok(id, json!({}))),
        METHOD_LOG_STORAGE_SCHEMA => Some(match serde_json::to_value(backend.schema()) {
            Ok(value) => RpcResponse::ok(id, value),
            Err(error) => RpcResponse::err(id, encoding_error("log_storage/schema", error)),
        }),
        METHOD_LOG_STORAGE_STORE => {
            let params = match deserialize_params::<LogStorageStoreParams>(request.params, false) {
                Ok(value) => value,
                Err(error) => {
                    write_response(&stdout, &RpcResponse::err(id, error)).await;
                    return;
                }
            };
            let count = params.entries.len();
            Some(match backend.store(params.entries).await {
                Ok(()) => RpcResponse::ok(id, json!({ "stored": count })),
                Err(error) => RpcResponse::err(id, error.into()),
            })
        }
        METHOD_LOG_STORAGE_QUERY => {
            let filter = match deserialize_params::<LogQuery>(request.params, true) {
                Ok(value) => value,
                Err(error) => {
                    write_response(&stdout, &RpcResponse::err(id, error)).await;
                    return;
                }
            };
            Some(match backend.query(filter).await {
                Ok(result) => match serde_json::to_value(result) {
                    Ok(value) => RpcResponse::ok(id, value),
                    Err(error) => RpcResponse::err(id, encoding_error("log_storage/query", error)),
                },
                Err(error) => RpcResponse::err(id, error.into()),
            })
        }
        METHOD_LOG_STORAGE_TAIL => {
            let filter = match deserialize_params::<LogQuery>(request.params, true) {
                Ok(value) => value,
                Err(error) => {
                    write_response(&stdout, &RpcResponse::err(id, error)).await;
                    return;
                }
            };
            match backend.tail(filter).await {
                Ok(stream) => {
                    let request_id = id.clone();
                    let stdout = stdout.clone();
                    tokio::spawn(async move {
                        drive_log_storage_stream(request_id, stream, stdout).await;
                    });
                    Some(RpcResponse::ok(id, json!({ "tailing": true })))
                }
                Err(error) => Some(RpcResponse::err(id, error.into())),
            }
        }
        other if other.starts_with("$/") => None,
        other => Some(method_not_found(id, &info.name, other)),
    };

    if let Some(response) = response {
        write_response(&stdout, &response).await;
    }
}

async fn drive_log_storage_stream(
    request_id: Option<Value>,
    mut stream: animus_log_storage_protocol::LogStream,
    stdout: Arc<Mutex<Stdout>>,
) {
    use futures_core::Stream;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    // The outer `Pin<Box<dyn Stream>>` is `Unpin`, so `Pin::new(&mut stream)`
    // suffices to project a `Pin<&mut dyn Stream>` for `poll_next`. We avoid
    // pulling in `futures-util` and use `std::future::poll_fn` instead.
    std::future::poll_fn(|cx: &mut Context<'_>| loop {
        match Pin::new(&mut stream).poll_next(cx) {
            Poll::Ready(Some(Ok(entry))) => {
                let entry_value = match serde_json::to_value(&entry) {
                    Ok(value) => value,
                    Err(_) => continue,
                };
                let mut payload = serde_json::Map::new();
                if let Some(id) = request_id.clone() {
                    payload.insert("id".to_string(), id);
                }
                payload.insert("entry".to_string(), entry_value);
                let notification = RpcNotification::new(
                    NOTIFICATION_LOG_STORAGE_EVENT,
                    Some(Value::Object(payload)),
                );
                let stdout = stdout.clone();
                tokio::spawn(async move {
                    write_notification(&stdout, &notification).await;
                });
            }
            Poll::Ready(Some(Err(error))) => {
                let rpc_error: RpcError = error.into();
                let error_value = match serde_json::to_value(&rpc_error) {
                    Ok(value) => value,
                    Err(_) => continue,
                };
                let mut payload = serde_json::Map::new();
                if let Some(id) = request_id.clone() {
                    payload.insert("id".to_string(), id);
                }
                payload.insert("error".to_string(), error_value);
                let notification = RpcNotification::new(
                    NOTIFICATION_LOG_STORAGE_EVENT,
                    Some(Value::Object(payload)),
                );
                let stdout = stdout.clone();
                tokio::spawn(async move {
                    write_notification(&stdout, &notification).await;
                });
                return Poll::Ready(());
            }
            Poll::Ready(None) => return Poll::Ready(()),
            Poll::Pending => return Poll::Pending,
        }
    })
    .await;
}

// =====================================================================
// Transport dispatch
// =====================================================================

async fn handle_transport_request<B: TransportBackend + 'static>(
    request: RpcRequest,
    info: PluginInfo,
    capabilities: PluginCapabilities,
    backend: Arc<B>,
    stdout: Arc<Mutex<Stdout>>,
) {
    let id = request.id.clone();
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
                    write_response(&stdout, &RpcResponse::err(id, error)).await;
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
        other if other.starts_with("$/") => None,
        other => Some(method_not_found(id, &info.name, other)),
    };

    if let Some(response) = response {
        write_response(&stdout, &response).await;
    }
}

// =====================================================================
// Shared helpers
// =====================================================================

/// Read one newline-delimited JSON-RPC request frame from `reader`.
///
/// Returns `Ok(None)` on EOF and `Ok(Some(_))` for each successfully parsed
/// frame. Frames that fail to parse are skipped (and the loop continues with
/// the next line) — the JSON-RPC 2.0 spec advises against sending unsolicited
/// error responses for parse failures on notifications, so the runtime errs
/// on the side of silence here.
pub(crate) async fn read_frame<R>(reader: &mut R) -> Result<Option<RpcRequest>>
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

/// Serialize and write a JSON-RPC response frame to the shared stdout handle.
pub(crate) async fn write_response(stdout: &Arc<Mutex<Stdout>>, response: &RpcResponse) {
    write_frame(stdout, response).await;
}

/// Serialize and write a JSON-RPC notification frame to the shared stdout
/// handle.
pub(crate) async fn write_notification(
    stdout: &Arc<Mutex<Stdout>>,
    notification: &RpcNotification,
) {
    write_frame(stdout, notification).await;
}

/// Install the global plugin-log emitter for this process and spawn the
/// forwarder task that drains queued notifications onto the shared stdout.
/// Called by each `*_main` entrypoint before entering the read loop so the
/// [`log::info!`], [`log::warn!`], [`log::error!`], etc. macros become live.
pub(crate) fn install_log_forwarder(stdout: Arc<Mutex<Stdout>>) {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<RpcNotification>();
    log::install_emitter(tx);
    tokio::spawn(async move {
        while let Some(notification) = rx.recv().await {
            write_notification(&stdout, &notification).await;
        }
    });
}

pub(crate) async fn write_frame<T: serde::Serialize, W: AsyncWrite + Unpin>(
    stdout: &Arc<Mutex<W>>,
    frame: &T,
) {
    if let Ok(mut payload) = serde_json::to_string(frame) {
        payload.push('\n');
        let mut guard = stdout.lock().await;
        let _ = guard.write_all(payload.as_bytes()).await;
        let _ = guard.flush().await;
    }
}

/// Check if `--manifest` (or `-m`) was passed on the command line.
pub(crate) fn parse_manifest_flag() -> bool {
    std::env::args()
        .skip(1)
        .any(|arg| arg == "--manifest" || arg == "-m")
}

/// Print a [`PluginManifest`] derived from `info` + `capabilities` to stdout
/// and exit with code `0`.
pub(crate) fn print_manifest_and_exit(info: &PluginInfo, capabilities: &PluginCapabilities) -> ! {
    let manifest = PluginManifest {
        name: info.name.clone(),
        version: info.version.clone(),
        plugin_kind: info.plugin_kind.clone(),
        description: info.description.clone().unwrap_or_default(),
        protocol_version: PROTOCOL_VERSION.to_string(),
        capabilities: capabilities.methods.clone(),
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

pub(crate) fn refuse_terminal_stdin(plugin_name: &str) {
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
    };
    match serde_json::to_value(result) {
        Ok(value) => RpcResponse::ok(id, value),
        Err(error) => RpcResponse::err(id, encoding_error("initialize", error)),
    }
}

fn health_response(
    id: Option<Value>,
    result: Result<HealthCheckResult, SubjectBackendError>,
) -> RpcResponse {
    match result {
        Ok(health) => match serde_json::to_value(health) {
            Ok(value) => RpcResponse::ok(id, value),
            Err(error) => RpcResponse::err(id, encoding_error("health/check", error)),
        },
        Err(error) => RpcResponse::err(
            id,
            RpcError {
                code: error_codes::INTERNAL_ERROR,
                message: format!("health/check failed: {error}"),
                data: Some(json!({"status": HealthStatus::Unhealthy})),
            },
        ),
    }
}

fn provider_health_response(
    id: Option<Value>,
    result: Result<HealthCheckResult, animus_provider_protocol::BackendError>,
) -> RpcResponse {
    match result {
        Ok(health) => match serde_json::to_value(health) {
            Ok(value) => RpcResponse::ok(id, value),
            Err(error) => RpcResponse::err(id, encoding_error("health/check", error)),
        },
        Err(error) => RpcResponse::err(id, error.into()),
    }
}

fn trigger_health_response(
    id: Option<Value>,
    result: Result<HealthCheckResult, TriggerBackendError>,
) -> RpcResponse {
    match result {
        Ok(health) => match serde_json::to_value(health) {
            Ok(value) => RpcResponse::ok(id, value),
            Err(error) => RpcResponse::err(id, encoding_error("health/check", error)),
        },
        Err(error) => RpcResponse::err(id, error.into()),
    }
}

fn log_storage_health_response(
    id: Option<Value>,
    result: Result<HealthCheckResult, LogStorageBackendError>,
) -> RpcResponse {
    match result {
        Ok(health) => match serde_json::to_value(health) {
            Ok(value) => RpcResponse::ok(id, value),
            Err(error) => RpcResponse::err(id, encoding_error("health/check", error)),
        },
        Err(error) => RpcResponse::err(id, error.into()),
    }
}

fn transport_health_response(
    id: Option<Value>,
    result: Result<HealthCheckResult, TransportBackendError>,
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
                // Default-able request types deserialize fine from an empty
                // object even when the wire frame omitted `params` entirely.
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

// =====================================================================
// Capability derivation
// =====================================================================

/// Append `extras` to `methods`, skipping any entries already present.
///
/// The dedup is order-preserving: the first occurrence wins, so a
/// plugin that accidentally lists a method already advertised by the
/// runtime (e.g. `agent/run`) keeps the runtime's slot and silently
/// drops the duplicate. Stable ordering matters because the testkit's
/// conformance gating reads `init.capabilities.methods` and humans
/// read it in `--manifest` output.
fn append_unique_capabilities(methods: &mut Vec<String>, extras: &[String]) {
    for extra in extras {
        if !methods.iter().any(|m| m == extra) {
            methods.push(extra.clone());
        }
    }
}

fn subject_capabilities<B: SubjectBackend>(
    backend: &B,
    extra_capabilities: &[String],
) -> PluginCapabilities {
    let schema = backend.schema();
    let mut methods = vec![
        METHOD_SUBJECT_LIST.to_string(),
        METHOD_SUBJECT_GET.to_string(),
        METHOD_SUBJECT_UPDATE.to_string(),
        METHOD_SUBJECT_SCHEMA.to_string(),
        "health/check".to_string(),
    ];
    if schema.supports_watch {
        methods.push(METHOD_SUBJECT_WATCH.to_string());
    }
    append_unique_capabilities(&mut methods, extra_capabilities);
    PluginCapabilities {
        methods,
        streaming: schema.supports_watch,
        progress: false,
        cancellation: false,
        subject_kinds: schema.kinds.clone(),
        mcp_tools: Vec::new(),
    }
}

fn provider_capabilities<P: ProviderBackend>(
    backend: &P,
    extra_capabilities: &[String],
) -> PluginCapabilities {
    let manifest = backend.manifest();
    let mut methods = vec![METHOD_AGENT_RUN.to_string(), "health/check".to_string()];
    if manifest.capabilities.resume {
        methods.push(METHOD_AGENT_RESUME.to_string());
    }
    if manifest.capabilities.cancellation {
        methods.push(METHOD_AGENT_CANCEL.to_string());
    }
    append_unique_capabilities(&mut methods, extra_capabilities);
    PluginCapabilities {
        methods,
        streaming: manifest.capabilities.streaming,
        progress: false,
        cancellation: manifest.capabilities.cancellation,
        subject_kinds: Vec::new(),
        mcp_tools: Vec::new(),
    }
}

fn trigger_capabilities<B: TriggerBackend>(
    backend: &B,
    extra_capabilities: &[String],
) -> PluginCapabilities {
    let schema = backend.schema();
    let mut methods = vec![
        METHOD_TRIGGER_WATCH.to_string(),
        METHOD_TRIGGER_SCHEMA.to_string(),
        "health/check".to_string(),
    ];
    if schema.supports_ack {
        methods.push(METHOD_TRIGGER_ACK.to_string());
    }
    append_unique_capabilities(&mut methods, extra_capabilities);
    PluginCapabilities {
        methods,
        // Trigger backends are always streaming — `trigger/watch` is the
        // primary surface and emits `trigger/event` notifications.
        streaming: true,
        progress: false,
        cancellation: false,
        subject_kinds: Vec::new(),
        mcp_tools: Vec::new(),
    }
}

fn log_storage_capabilities<B: LogStorageBackend>(
    backend: &B,
    extra_capabilities: &[String],
) -> PluginCapabilities {
    let schema = backend.schema();
    let mut methods = vec![
        METHOD_LOG_STORAGE_STORE.to_string(),
        METHOD_LOG_STORAGE_SCHEMA.to_string(),
        "health/check".to_string(),
    ];
    if schema.supports_query {
        methods.push(METHOD_LOG_STORAGE_QUERY.to_string());
    }
    if schema.supports_tail {
        methods.push(METHOD_LOG_STORAGE_TAIL.to_string());
    }
    append_unique_capabilities(&mut methods, extra_capabilities);
    PluginCapabilities {
        methods,
        // `streaming` is true only when the backend implements
        // `log_storage/tail` — that's the surface that emits
        // `log_storage/event` notifications.
        streaming: schema.supports_tail,
        progress: false,
        cancellation: false,
        subject_kinds: Vec::new(),
        mcp_tools: Vec::new(),
    }
}

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
        // Transport backends advertise streaming when their external
        // surface supports it; the runtime forwards that through verbatim
        // so the daemon can decide whether to route streaming control
        // methods (`daemon/events`, `daemon/logs`) through this transport.
        streaming: schema.supports_streaming,
        progress: false,
        cancellation: false,
        subject_kinds: Vec::new(),
        mcp_tools: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use animus_subject_protocol::{
        Subject, SubjectList, SubjectSchema, SubjectStatus, NOTIFICATION_SUBJECT_CHANGED,
    };
    use async_trait::async_trait;
    use std::sync::Mutex as StdMutex;

    #[test]
    fn deserialize_params_rejects_missing_when_required() {
        let result: std::result::Result<SubjectGetParams, RpcError> =
            deserialize_params(None, false);
        let error = result.expect_err("missing params should error");
        assert_eq!(error.code, error_codes::INVALID_PARAMS);
    }

    #[test]
    fn deserialize_params_allows_missing_for_defaultable_types() {
        let filter: SubjectFilter =
            deserialize_params(None, true).expect("default-able type should accept null");
        assert!(filter.status.is_empty());
    }

    #[test]
    fn method_not_found_uses_protocol_constant() {
        let response = method_not_found(Some(json!(1)), "plugin", "subject/bogus");
        let error = response.error.expect("error payload");
        assert_eq!(error.code, error_codes::METHOD_NOT_FOUND);
    }

    // -----------------------------------------------------------------
    // Subject wire dispatch: <kind>/<verb> routing.
    //
    // The in-tree daemon's SubjectRouter dispatches by the kind prefix
    // (e.g. `task/list`, `issue/get`) per
    // `crates/orchestrator-plugin-host/src/subject_router.rs:38-42`.
    // The runtime must accept those and route to the right verb on the
    // backend, injecting the kind into SubjectFilter so multi-kind
    // backends can disambiguate.
    // -----------------------------------------------------------------

    #[derive(Default)]
    struct RecordingBackend {
        last_list_filter: StdMutex<Option<SubjectFilter>>,
        kinds: Vec<String>,
    }

    impl RecordingBackend {
        fn new(kinds: Vec<&str>) -> Self {
            Self {
                last_list_filter: StdMutex::new(None),
                kinds: kinds.into_iter().map(ToOwned::to_owned).collect(),
            }
        }
    }

    #[async_trait]
    impl SubjectBackend for RecordingBackend {
        async fn list(
            &self,
            filter: SubjectFilter,
        ) -> std::result::Result<SubjectList, SubjectBackendError> {
            *self.last_list_filter.lock().unwrap() = Some(filter);
            let fetched_at = serde_json::from_value(json!("2026-01-01T00:00:00Z"))
                .expect("static timestamp parses");
            Ok(SubjectList {
                subjects: Vec::new(),
                next_cursor: None,
                fetched_at,
            })
        }

        async fn get(&self, id: &SubjectId) -> std::result::Result<Subject, SubjectBackendError> {
            Err(SubjectBackendError::NotFound(id.0.clone()))
        }

        async fn update(
            &self,
            id: &SubjectId,
            _patch: SubjectPatch,
        ) -> std::result::Result<Subject, SubjectBackendError> {
            Err(SubjectBackendError::NotFound(id.0.clone()))
        }

        async fn watch(&self) -> Option<animus_subject_protocol::EventStream> {
            None
        }

        fn schema(&self) -> SubjectSchema {
            SubjectSchema {
                kinds: self.kinds.clone(),
                status_values: vec![SubjectStatus::Ready],
                supports_watch: false,
                supports_create: false,
                supports_pagination: false,
                native_status_values: Vec::new(),
                status_dispatch_hints: Vec::new(),
                custom_fields: Vec::new(),
            }
        }

        async fn health(&self) -> std::result::Result<HealthCheckResult, SubjectBackendError> {
            Ok(HealthCheckResult {
                status: HealthStatus::Healthy,
                uptime_ms: None,
                memory_usage_bytes: None,
                last_error: None,
            })
        }
    }

    fn test_info(name: &str) -> PluginInfo {
        PluginInfo {
            name: name.to_string(),
            version: "0.0.0-test".to_string(),
            plugin_kind: "subject_backend".to_string(),
            description: None,
        }
    }

    fn test_stdout() -> Arc<Mutex<Stdout>> {
        Arc::new(Mutex::new(tokio::io::stdout()))
    }

    #[test]
    fn subject_method_parts_splits_kind_and_verb() {
        assert_eq!(subject_method_parts("task/list"), ("task", "list"));
        assert_eq!(subject_method_parts("issue/get"), ("issue", "get"));
        // Legacy `subject/<verb>` keeps the wire compatible: kind blank.
        assert_eq!(subject_method_parts("subject/list"), ("", "list"));
        // Bare verbs (defensive) report no kind.
        assert_eq!(subject_method_parts("schema"), ("", "schema"));
    }

    #[test]
    fn inject_kind_into_filter_skips_legacy_subject_prefix() {
        let mut filter = SubjectFilter::default();
        inject_kind_into_filter(&mut filter, "");
        assert!(filter.kind.is_empty());

        let mut filter = SubjectFilter::default();
        inject_kind_into_filter(&mut filter, "task");
        assert_eq!(filter.kind, vec!["task".to_string()]);

        // Caller-supplied kind wins over the wire-derived kind.
        let mut filter = SubjectFilter {
            kind: vec!["issue".to_string()],
            ..SubjectFilter::default()
        };
        inject_kind_into_filter(&mut filter, "task");
        assert_eq!(filter.kind, vec!["issue".to_string()]);
    }

    #[tokio::test]
    async fn subject_dispatch_recognizes_kind_slash_verb() {
        let backend = Arc::new(RecordingBackend::new(vec!["task"]));
        let info = test_info("animus-subject-recording");
        let capabilities = subject_capabilities(&*backend, &[]);
        let request = RpcRequest::new(json!(1), "task/list", Some(json!({})));

        handle_subject_request(request, info, capabilities, backend.clone(), test_stdout()).await;

        let recorded = backend
            .last_list_filter
            .lock()
            .unwrap()
            .clone()
            .expect("backend.list should have been invoked");
        // Any non-error landing on `list` is the success signal here.
        assert!(recorded.status.is_empty());
    }

    #[tokio::test]
    async fn subject_dispatch_injects_kind_into_filter() {
        let backend = Arc::new(RecordingBackend::new(vec!["task", "issue"]));
        let info = test_info("animus-subject-multi");
        let capabilities = subject_capabilities(&*backend, &[]);
        let request = RpcRequest::new(json!(2), "task/list", Some(json!({})));

        handle_subject_request(request, info, capabilities, backend.clone(), test_stdout()).await;

        let recorded = backend
            .last_list_filter
            .lock()
            .unwrap()
            .clone()
            .expect("backend.list should have been invoked");
        assert_eq!(recorded.kind, vec!["task".to_string()]);
    }

    #[tokio::test]
    async fn subject_dispatch_rejects_unknown_verb() {
        let backend = Arc::new(RecordingBackend::new(vec!["task"]));
        let info = test_info("animus-subject-recording");
        let capabilities = subject_capabilities(&*backend, &[]);
        let request = RpcRequest::new(json!(3), "task/madeup", Some(json!({})));

        // No panic / no backend invocation. We verify the dispatcher does
        // not reach the recording `list` path for an unknown verb.
        handle_subject_request(request, info, capabilities, backend.clone(), test_stdout()).await;

        assert!(backend.last_list_filter.lock().unwrap().is_none());
    }

    // Smoke: legacy `subject/list` still dispatches, with no kind injected.
    #[tokio::test]
    async fn subject_dispatch_accepts_legacy_subject_prefix() {
        let backend = Arc::new(RecordingBackend::new(vec!["task"]));
        let info = test_info("animus-subject-recording");
        let capabilities = subject_capabilities(&*backend, &[]);
        let request = RpcRequest::new(json!(4), "subject/list", Some(json!({})));

        handle_subject_request(request, info, capabilities, backend.clone(), test_stdout()).await;

        let recorded = backend
            .last_list_filter
            .lock()
            .unwrap()
            .clone()
            .expect("backend.list should have been invoked for legacy prefix");
        assert!(recorded.kind.is_empty());
    }

    // Notification import keeps the public re-export wired so other
    // crates can rely on it; this is a compile-time check only.
    #[test]
    fn notification_constants_in_scope() {
        let _ = NOTIFICATION_SUBJECT_CHANGED;
    }

    // -----------------------------------------------------------------
    // Provider streaming dispatch: AgentNotification frames must travel
    // through the runtime's NotificationSink in the order the provider
    // emits them, and the final AgentRunResponse must arrive afterwards.
    //
    // The fixtures here mirror the in-tree
    // `crates/animus-plugin-runtime/src/lib.rs::handle_agent_run`
    // behavior — five notification methods (`agent/output`,
    // `agent/thinking`, `agent/toolCall`, `agent/toolResult`,
    // `agent/error`) all carrying the active session id.
    // -----------------------------------------------------------------

    use animus_provider_protocol::{
        AgentNotification, AgentResumeRequest, AgentRunRequest, AgentRunResponse,
        BackendError as ProviderBackendError, NotificationSink, ProviderBackend,
        ProviderCapabilities, ProviderManifest, NOTIFICATION_AGENT_ERROR,
        NOTIFICATION_AGENT_OUTPUT, NOTIFICATION_AGENT_THINKING, NOTIFICATION_AGENT_TOOL_CALL,
        NOTIFICATION_AGENT_TOOL_RESULT,
    };

    /// A provider backend that emits a scripted sequence of
    /// `AgentNotification`s through the supplied sink before returning a
    /// canned response. Used by the streaming-dispatch tests.
    struct ScriptedProvider {
        session_id: String,
        script: Vec<AgentNotification>,
        capture_streaming: StdMutex<Vec<AgentNotification>>,
        used_streaming: StdMutex<bool>,
    }

    impl ScriptedProvider {
        fn new(session_id: &str, script: Vec<AgentNotification>) -> Self {
            Self {
                session_id: session_id.to_string(),
                script,
                capture_streaming: StdMutex::new(Vec::new()),
                used_streaming: StdMutex::new(false),
            }
        }
    }

    #[async_trait]
    impl ProviderBackend for ScriptedProvider {
        fn manifest(&self) -> ProviderManifest {
            ProviderManifest {
                name: "animus-provider-scripted".into(),
                version: "0.0.0-test".into(),
                description: "Test fixture".into(),
                supported_models: vec!["scripted-1".into()],
                tool: "scripted".into(),
                capabilities: ProviderCapabilities {
                    streaming: true,
                    resume: true,
                    cancellation: false,
                    write_capable: false,
                    mcp: false,
                },
            }
        }

        async fn run_agent(
            &self,
            _request: AgentRunRequest,
        ) -> std::result::Result<AgentRunResponse, ProviderBackendError> {
            // Non-streaming path: just return the canned response.
            Ok(canned_response(&self.session_id))
        }

        async fn run_agent_streaming(
            &self,
            _request: AgentRunRequest,
            sink: NotificationSink,
        ) -> std::result::Result<AgentRunResponse, ProviderBackendError> {
            *self.used_streaming.lock().unwrap() = true;
            for notification in &self.script {
                self.capture_streaming
                    .lock()
                    .unwrap()
                    .push(notification.clone());
                sink.emit(notification.clone());
            }
            Ok(canned_response(&self.session_id))
        }

        async fn resume_agent(
            &self,
            _request: AgentResumeRequest,
        ) -> std::result::Result<AgentRunResponse, ProviderBackendError> {
            Ok(canned_response(&self.session_id))
        }

        async fn cancel_agent(
            &self,
            _session_id: &str,
        ) -> std::result::Result<(), ProviderBackendError> {
            Ok(())
        }

        async fn health(&self) -> std::result::Result<HealthCheckResult, ProviderBackendError> {
            Ok(HealthCheckResult {
                status: HealthStatus::Healthy,
                uptime_ms: None,
                memory_usage_bytes: None,
                last_error: None,
            })
        }
    }

    fn canned_response(session_id: &str) -> AgentRunResponse {
        AgentRunResponse {
            session_id: session_id.to_string(),
            exit_code: 0,
            output: "final".into(),
            metadata: Vec::new(),
            tool_calls: Vec::new(),
            tool_results: Vec::new(),
            thinking: Vec::new(),
            errors: Vec::new(),
            duration_ms: 0,
            backend: "scripted".into(),
            tokens_used: None,
            decision_verdict: None,
        }
    }

    /// Replace stdout's underlying handle with an `Arc<Mutex<Stdout>>` that
    /// proxies into a `Vec<u8>` recorder. We can't actually swap
    /// `tokio::io::Stdout`, so the tests instead drive the sink directly
    /// and record what the forwarder writes.
    ///
    /// The strategy here: build a sink + forwarder against a real
    /// `tokio::io::stdout()` (which is fine in unit tests — they just
    /// print to the test runner), but record what the *sink* receives via
    /// a parallel recorder closure. That way we verify ordering and
    /// frame payload without worrying about stdout interception.
    fn recording_sink() -> (NotificationSink, Arc<StdMutex<Vec<RpcNotification>>>) {
        let recorder: Arc<StdMutex<Vec<RpcNotification>>> = Arc::new(StdMutex::new(Vec::new()));
        let r2 = recorder.clone();
        let sink = NotificationSink::new(move |notification| {
            let frame = RpcNotification::new(notification.method(), Some(notification.payload()));
            r2.lock().unwrap().push(frame);
        });
        (sink, recorder)
    }

    #[tokio::test]
    async fn provider_streaming_emits_five_notification_kinds_in_order() {
        let script = vec![
            AgentNotification::Output {
                session_id: "sess-1".into(),
                text: "hello".into(),
                is_final: false,
            },
            AgentNotification::Thinking {
                session_id: "sess-1".into(),
                text: "let me think".into(),
            },
            AgentNotification::ToolCall {
                session_id: "sess-1".into(),
                name: "shell".into(),
                arguments: json!({"cmd": "echo hi"}),
                server: None,
            },
            AgentNotification::ToolResult {
                session_id: "sess-1".into(),
                name: "shell".into(),
                output: json!("hi\n"),
                success: true,
            },
            AgentNotification::Error {
                session_id: "sess-1".into(),
                message: "soft fail".into(),
                recoverable: true,
            },
        ];

        let provider = ScriptedProvider::new("sess-1", script);
        let (sink, recorder) = recording_sink();

        // Drive the streaming path directly so the test doesn't depend on
        // tokio's stdout (which would scramble the test runner's output).
        let response = provider
            .run_agent_streaming(
                AgentRunRequest {
                    session_id: None,
                    prompt: "go".into(),
                    model: Some("scripted-1".into()),
                    system_prompt: None,
                    cwd: std::path::PathBuf::from("/"),
                    project_root: None,
                    permission_mode: None,
                    timeout_secs: None,
                    env: Default::default(),
                    mcp_servers: None,
                    tools: None,
                    response_schema: None,
                    runtime_contract: None,
                    extras: Default::default(),
                },
                sink,
            )
            .await
            .expect("streaming run");

        assert_eq!(response.session_id, "sess-1");
        let recorded = recorder.lock().unwrap();
        assert_eq!(recorded.len(), 5);
        assert_eq!(recorded[0].method, NOTIFICATION_AGENT_OUTPUT);
        assert_eq!(recorded[1].method, NOTIFICATION_AGENT_THINKING);
        assert_eq!(recorded[2].method, NOTIFICATION_AGENT_TOOL_CALL);
        assert_eq!(recorded[3].method, NOTIFICATION_AGENT_TOOL_RESULT);
        assert_eq!(recorded[4].method, NOTIFICATION_AGENT_ERROR);

        // Session id is carried on every frame per spec § 10.3.
        for frame in recorded.iter() {
            let params = frame.params.as_ref().expect("notification params");
            assert_eq!(params["session_id"], "sess-1");
        }
    }

    #[tokio::test]
    async fn provider_streaming_default_impl_delegates_to_run_agent() {
        // A provider that does NOT override `run_agent_streaming` (it
        // inherits the default impl). The recording sink must stay empty.
        struct NonStreamingProvider {
            session_id: String,
        }

        #[async_trait]
        impl ProviderBackend for NonStreamingProvider {
            fn manifest(&self) -> ProviderManifest {
                ProviderManifest {
                    name: "animus-provider-no-stream".into(),
                    version: "0.0.0-test".into(),
                    description: "Test fixture".into(),
                    supported_models: vec!["scripted-1".into()],
                    tool: "scripted".into(),
                    capabilities: ProviderCapabilities::default(),
                }
            }

            async fn run_agent(
                &self,
                _request: AgentRunRequest,
            ) -> std::result::Result<AgentRunResponse, ProviderBackendError> {
                Ok(canned_response(&self.session_id))
            }

            async fn resume_agent(
                &self,
                _request: AgentResumeRequest,
            ) -> std::result::Result<AgentRunResponse, ProviderBackendError> {
                Ok(canned_response(&self.session_id))
            }

            async fn cancel_agent(
                &self,
                _session_id: &str,
            ) -> std::result::Result<(), ProviderBackendError> {
                Ok(())
            }

            async fn health(&self) -> std::result::Result<HealthCheckResult, ProviderBackendError> {
                Ok(HealthCheckResult {
                    status: HealthStatus::Healthy,
                    uptime_ms: None,
                    memory_usage_bytes: None,
                    last_error: None,
                })
            }
        }

        let provider = NonStreamingProvider {
            session_id: "sess-2".into(),
        };
        let (sink, recorder) = recording_sink();

        let response = provider
            .run_agent_streaming(
                AgentRunRequest {
                    session_id: None,
                    prompt: "go".into(),
                    model: None,
                    system_prompt: None,
                    cwd: std::path::PathBuf::from("/"),
                    project_root: None,
                    permission_mode: None,
                    timeout_secs: None,
                    env: Default::default(),
                    mcp_servers: None,
                    tools: None,
                    response_schema: None,
                    runtime_contract: None,
                    extras: Default::default(),
                },
                sink,
            )
            .await
            .expect("default streaming run");

        assert_eq!(response.session_id, "sess-2");
        assert!(
            recorder.lock().unwrap().is_empty(),
            "default impl should not emit any notifications"
        );
    }

    #[tokio::test]
    async fn build_notification_sink_drains_buffered_notifications_on_close() {
        // Direct end-to-end: build a sink + forwarder against a real
        // stdout handle, emit through the sink, close the forwarder, and
        // verify the join handle completes promptly (within a tight
        // budget). The sink's own emissions are tested above; here we
        // exercise the runtime helper that wires sink → JSON-RPC stdout.
        let stdout = Arc::new(Mutex::new(tokio::io::stdout()));
        let (sink, forwarder) = build_notification_sink(stdout);
        sink.emit(AgentNotification::Output {
            session_id: "sess-3".into(),
            text: "hi".into(),
            is_final: true,
        });
        sink.emit(AgentNotification::Error {
            session_id: "sess-3".into(),
            message: "boom".into(),
            recoverable: false,
        });
        // Drop the sink so the forwarder sees EOF on its channel.
        drop(sink);

        // Close must complete promptly once the sink drops — if it hangs
        // the test framework's per-test timeout will catch it. We
        // separately verify in the in-process tests above that ordering
        // is preserved.
        forwarder.close().await;
    }

    // -----------------------------------------------------------------
    // extra_capabilities extension point (added in protocol v0.1.13).
    //
    // The capability helpers must:
    //   1. Append every extra string to `PluginCapabilities.methods`
    //      after the runtime-derived defaults.
    //   2. Deduplicate against the defaults so a plugin that re-lists
    //      `agent/run` (or similar) doesn't double up.
    //   3. Preserve insertion order across extras for deterministic
    //      `--manifest` output.
    //   4. Be backwards-compatible with an empty extras slice (the
    //      pre-v0.1.13 default).
    // -----------------------------------------------------------------

    #[test]
    fn provider_capabilities_appends_extras() {
        struct StubProvider;

        #[async_trait]
        impl ProviderBackend for StubProvider {
            fn manifest(&self) -> ProviderManifest {
                ProviderManifest {
                    name: "stub".into(),
                    version: "0".into(),
                    description: "".into(),
                    supported_models: vec![],
                    tool: "stub".into(),
                    capabilities: ProviderCapabilities {
                        streaming: true,
                        resume: false,
                        cancellation: true,
                        write_capable: false,
                        mcp: false,
                    },
                }
            }
            async fn run_agent(
                &self,
                _: AgentRunRequest,
            ) -> std::result::Result<AgentRunResponse, ProviderBackendError> {
                Ok(canned_response("x"))
            }
            async fn resume_agent(
                &self,
                _: AgentResumeRequest,
            ) -> std::result::Result<AgentRunResponse, ProviderBackendError> {
                Ok(canned_response("x"))
            }
            async fn cancel_agent(&self, _: &str) -> std::result::Result<(), ProviderBackendError> {
                Ok(())
            }
            async fn health(&self) -> std::result::Result<HealthCheckResult, ProviderBackendError> {
                Ok(HealthCheckResult {
                    status: HealthStatus::Healthy,
                    uptime_ms: None,
                    memory_usage_bytes: None,
                    last_error: None,
                })
            }
        }

        let extras = vec![
            "$harness/cancellation-loop-v2".to_string(),
            "$harness/oai-style".to_string(),
        ];
        let caps = provider_capabilities(&StubProvider, &extras);

        // Defaults still present, in order.
        assert_eq!(caps.methods[0], METHOD_AGENT_RUN);
        assert_eq!(caps.methods[1], "health/check");
        assert_eq!(caps.methods[2], METHOD_AGENT_CANCEL);
        // Extras appended in declared order.
        assert_eq!(caps.methods[3], "$harness/cancellation-loop-v2");
        assert_eq!(caps.methods[4], "$harness/oai-style");
        assert_eq!(caps.methods.len(), 5);
    }

    #[test]
    fn provider_capabilities_dedupes_against_defaults() {
        struct StubProvider;

        #[async_trait]
        impl ProviderBackend for StubProvider {
            fn manifest(&self) -> ProviderManifest {
                ProviderManifest {
                    name: "stub".into(),
                    version: "0".into(),
                    description: "".into(),
                    supported_models: vec![],
                    tool: "stub".into(),
                    capabilities: ProviderCapabilities::default(),
                }
            }
            async fn run_agent(
                &self,
                _: AgentRunRequest,
            ) -> std::result::Result<AgentRunResponse, ProviderBackendError> {
                Ok(canned_response("x"))
            }
            async fn resume_agent(
                &self,
                _: AgentResumeRequest,
            ) -> std::result::Result<AgentRunResponse, ProviderBackendError> {
                Ok(canned_response("x"))
            }
            async fn cancel_agent(&self, _: &str) -> std::result::Result<(), ProviderBackendError> {
                Ok(())
            }
            async fn health(&self) -> std::result::Result<HealthCheckResult, ProviderBackendError> {
                Ok(HealthCheckResult {
                    status: HealthStatus::Healthy,
                    uptime_ms: None,
                    memory_usage_bytes: None,
                    last_error: None,
                })
            }
        }

        // `agent/run` collides with the default; should appear exactly
        // once and keep the runtime's slot.
        let extras = vec![
            METHOD_AGENT_RUN.to_string(),
            "$harness/custom".to_string(),
            "$harness/custom".to_string(),
        ];
        let caps = provider_capabilities(&StubProvider, &extras);
        let count_run = caps
            .methods
            .iter()
            .filter(|m| *m == METHOD_AGENT_RUN)
            .count();
        let count_custom = caps
            .methods
            .iter()
            .filter(|m| *m == "$harness/custom")
            .count();
        assert_eq!(count_run, 1, "agent/run must dedupe against default");
        assert_eq!(count_custom, 1, "duplicate extras must collapse");
    }

    #[test]
    fn subject_capabilities_extras_flow_through() {
        let backend = RecordingBackend::new(vec!["task"]);
        let extras = vec!["$harness/subject-feature".to_string()];
        let caps = subject_capabilities(&backend, &extras);
        assert!(caps
            .methods
            .contains(&"$harness/subject-feature".to_string()));
        // Default capability still in place.
        assert!(caps.methods.iter().any(|m| m == METHOD_SUBJECT_LIST));
    }

    #[test]
    fn capability_helpers_unchanged_when_extras_empty() {
        // The v0.1.12 baseline: extras=&[] must produce the same method
        // list (length and contents) as the pre-extension implementation.
        let backend = RecordingBackend::new(vec!["task"]);
        let caps = subject_capabilities(&backend, &[]);
        assert_eq!(
            caps.methods,
            vec![
                METHOD_SUBJECT_LIST.to_string(),
                METHOD_SUBJECT_GET.to_string(),
                METHOD_SUBJECT_UPDATE.to_string(),
                METHOD_SUBJECT_SCHEMA.to_string(),
                "health/check".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn provider_initialize_response_carries_extra_capabilities() {
        // End-to-end: the extension point must reach the `initialize`
        // RPC reply, since that's what the testkit's gating reads. We
        // construct the response builder directly (the same path
        // `handle_provider_request` uses) and verify the method appears.
        struct StubProvider;

        #[async_trait]
        impl ProviderBackend for StubProvider {
            fn manifest(&self) -> ProviderManifest {
                ProviderManifest {
                    name: "stub".into(),
                    version: "0".into(),
                    description: "".into(),
                    supported_models: vec![],
                    tool: "stub".into(),
                    capabilities: ProviderCapabilities::default(),
                }
            }
            async fn run_agent(
                &self,
                _: AgentRunRequest,
            ) -> std::result::Result<AgentRunResponse, ProviderBackendError> {
                Ok(canned_response("x"))
            }
            async fn resume_agent(
                &self,
                _: AgentResumeRequest,
            ) -> std::result::Result<AgentRunResponse, ProviderBackendError> {
                Ok(canned_response("x"))
            }
            async fn cancel_agent(&self, _: &str) -> std::result::Result<(), ProviderBackendError> {
                Ok(())
            }
            async fn health(&self) -> std::result::Result<HealthCheckResult, ProviderBackendError> {
                Ok(HealthCheckResult {
                    status: HealthStatus::Healthy,
                    uptime_ms: None,
                    memory_usage_bytes: None,
                    last_error: None,
                })
            }
        }

        let info = PluginInfo {
            name: "stub".into(),
            version: "0".into(),
            plugin_kind: "provider".into(),
            description: None,
        };
        let extras = vec!["$harness/oai-style".to_string()];
        let caps = provider_capabilities(&StubProvider, &extras);
        let response = initialize_response(Some(json!(1)), &info, &caps);
        let value = response.result.expect("initialize response has result");
        let methods = value["capabilities"]["methods"]
            .as_array()
            .expect("methods array");
        let advertises = methods.iter().any(|m| m == "$harness/oai-style");
        assert!(
            advertises,
            "initialize response must surface extra capability: {methods:?}"
        );
    }
}
