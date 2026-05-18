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
//!
//! Each entrypoint runs the stdio loop indefinitely: it reads
//! newline-delimited JSON-RPC frames from stdin, dispatches to the trait, and
//! writes responses to stdout. The loop returns cleanly on stdin EOF and
//! bubbles fatal errors up via [`anyhow::Result`].
//!
//! # Scope (v0.1.0)
//!
//! This crate intentionally has a small surface — the wire loop and the
//! lifecycle helpers. It does **not** ship session-management helpers (e.g.
//! event channels, `SessionRequest` builders, child-process plumbing). Those
//! belong to a separate `animus-session-backend` crate that providers may
//! depend on in the future. For v0.1.0, provider implementations handle their
//! own session lifecycle inside
//! [`ProviderBackend::run_agent`](animus_provider_protocol::ProviderBackend::run_agent)
//! and return the aggregated [`AgentRunResponse`] when the run completes.
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

#![warn(missing_docs)]

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
    AgentCancelRequest, AgentResumeRequest, AgentRunRequest, ProviderBackend, METHOD_AGENT_CANCEL,
    METHOD_AGENT_RESUME, METHOD_AGENT_RUN,
};
use animus_subject_protocol::{
    BackendError as SubjectBackendError, SubjectBackend, SubjectFilter, SubjectId, SubjectPatch,
    METHOD_SUBJECT_GET, METHOD_SUBJECT_LIST, METHOD_SUBJECT_SCHEMA, METHOD_SUBJECT_UPDATE,
    METHOD_SUBJECT_WATCH, NOTIFICATION_SUBJECT_CHANGED,
};
use animus_trigger_protocol::{
    BackendError as TriggerBackendError, TriggerBackend, METHOD_TRIGGER_ACK, METHOD_TRIGGER_SCHEMA,
    METHOD_TRIGGER_WATCH, NOTIFICATION_TRIGGER_EVENT,
};
use anyhow::Result;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Stdout};
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
    let capabilities = subject_capabilities(&backend);
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
    let capabilities = provider_capabilities(&backend);
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
    let capabilities = trigger_capabilities(&backend);
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
    let capabilities = log_storage_capabilities(&backend);
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
            handle_log_storage_request(request, info, capabilities, backend, stdout).await;
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
    let response = match request.method.as_str() {
        "initialize" => Some(initialize_response(id, &info, &capabilities)),
        "initialized" => None,
        "$/ping" => Some(RpcResponse::ok(id, json!({}))),
        "health/check" => Some(health_response(id, backend.health().await)),
        "shutdown" => Some(RpcResponse::ok(id, json!({}))),
        METHOD_SUBJECT_LIST => {
            let filter = match deserialize_params::<SubjectFilter>(request.params, true) {
                Ok(value) => value,
                Err(error) => {
                    write_response(&stdout, &RpcResponse::err(id, error)).await;
                    return;
                }
            };
            Some(match backend.list(filter).await {
                Ok(list) => match serde_json::to_value(list) {
                    Ok(value) => RpcResponse::ok(id, value),
                    Err(error) => RpcResponse::err(id, encoding_error("subject/list", error)),
                },
                Err(error) => RpcResponse::err(id, error.into()),
            })
        }
        METHOD_SUBJECT_GET => {
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
        METHOD_SUBJECT_UPDATE => {
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
        METHOD_SUBJECT_WATCH => match backend.watch().await {
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
        METHOD_SUBJECT_SCHEMA => Some(match serde_json::to_value(backend.schema()) {
            Ok(value) => RpcResponse::ok(id, value),
            Err(error) => RpcResponse::err(id, encoding_error("subject/schema", error)),
        }),
        other if other.starts_with("$/") => None,
        other => Some(method_not_found(id, &info.name, other)),
    };

    if let Some(response) = response {
        write_response(&stdout, &response).await;
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
            Some(match backend.run_agent(request_payload).await {
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
            Some(match backend.resume_agent(request_payload).await {
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
        other if other.starts_with("$/") => None,
        other => Some(method_not_found(id, &info.name, other)),
    };

    if let Some(response) = response {
        write_response(&stdout, &response).await;
    }
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

async fn write_frame<T: serde::Serialize>(stdout: &Arc<Mutex<Stdout>>, frame: &T) {
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

fn subject_capabilities<B: SubjectBackend>(backend: &B) -> PluginCapabilities {
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
    PluginCapabilities {
        methods,
        streaming: schema.supports_watch,
        progress: false,
        cancellation: false,
        subject_kinds: schema.kinds.clone(),
        mcp_tools: Vec::new(),
    }
}

fn provider_capabilities<P: ProviderBackend>(backend: &P) -> PluginCapabilities {
    let manifest = backend.manifest();
    let mut methods = vec![METHOD_AGENT_RUN.to_string(), "health/check".to_string()];
    if manifest.capabilities.resume {
        methods.push(METHOD_AGENT_RESUME.to_string());
    }
    if manifest.capabilities.cancellation {
        methods.push(METHOD_AGENT_CANCEL.to_string());
    }
    PluginCapabilities {
        methods,
        streaming: manifest.capabilities.streaming,
        progress: false,
        cancellation: manifest.capabilities.cancellation,
        subject_kinds: Vec::new(),
        mcp_tools: Vec::new(),
    }
}

fn trigger_capabilities<B: TriggerBackend>(backend: &B) -> PluginCapabilities {
    let schema = backend.schema();
    let mut methods = vec![
        METHOD_TRIGGER_WATCH.to_string(),
        METHOD_TRIGGER_SCHEMA.to_string(),
        "health/check".to_string(),
    ];
    if schema.supports_ack {
        methods.push(METHOD_TRIGGER_ACK.to_string());
    }
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

fn log_storage_capabilities<B: LogStorageBackend>(backend: &B) -> PluginCapabilities {
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
