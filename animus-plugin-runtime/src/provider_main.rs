//! Provider-protocol entrypoints (`provider_main`,
//! `provider_main_with_capabilities`) restored alongside the
//! [`SessionBackend`](animus_session_backend::session::session_backend::SessionBackend)-wrapping
//! [`run_provider`](crate::run_provider) entry.
//!
//! Plugins that implement
//! [`ProviderBackend`](animus_provider_protocol::ProviderBackend) directly
//! (driving `agent/run`, `agent/resume`, `agent/cancel`, `agent/respond`)
//! call [`provider_main`] (or [`provider_main_with_capabilities`]) from
//! `#[tokio::main]`. This is the provider-protocol path; it coexists with
//! [`run_provider`](crate::run_provider), which wraps a `SessionBackend` CLI
//! wrapper instead.
//!
//! The two share nothing but the wire framing — [`provider_main`] drives the
//! `animus_provider_protocol::ProviderBackend` trait, while
//! [`run_provider`](crate::run_provider) drives the crate-local
//! [`ProviderBackend`](crate::ProviderBackend) `SessionBackend` adapter.

use std::io::{self, IsTerminal, Write};
use std::sync::Arc;

use animus_plugin_protocol::{
    error_codes, HealthCheckResult, InitializeResult, PluginCapabilities, PluginInfo,
    PluginManifest, RpcError, RpcNotification, RpcRequest, RpcResponse, PROTOCOL_VERSION,
};
use animus_provider_protocol::{
    AgentCancelRequest, AgentNotification, AgentRespondParams, AgentResumeRequest, AgentRunRequest,
    NotificationSink, ProviderBackend, METHOD_AGENT_CANCEL, METHOD_AGENT_RESPOND,
    METHOD_AGENT_RESUME, METHOD_AGENT_RUN,
};
use anyhow::Result;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::io::{BufReader, Stdout};
use tokio::sync::Mutex;

use crate::{install_log_forwarder, write_frame};

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
/// Use this when the provider wants to opt in to host-side test scenarios
/// or feature flags that the runtime cannot detect from the trait — e.g.
/// the testkit's `$harness/cancellation-loop-v2` or `$harness/oai-style`
/// opt-in capabilities. Any string in `extra_capabilities` is appended
/// (deduplicated) to [`PluginCapabilities::methods`] returned in the
/// `initialize` response. Passing an empty vector is exactly equivalent to
/// calling [`provider_main`].
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
        "exit" => std::process::exit(0),
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
    /// Set true by [`NotificationForwarder::close`] to tell the forwarder
    /// task to flush the currently-queued notifications and then exit.
    closing: Arc<std::sync::atomic::AtomicBool>,
    /// Wakes the forwarder task so it observes `closing` promptly even when
    /// it is parked on `recv()` with an empty queue.
    stop: Arc<tokio::sync::Notify>,
}

impl NotificationForwarder {
    /// Drain any buffered notifications, then stop the forwarder task.
    ///
    /// This intentionally does NOT wait for the mpsc channel to reach EOF.
    /// The protocol allows a provider to stash a [`NotificationSink`] clone
    /// in a per-session task that outlives `run_agent_streaming`; if any
    /// such clone is still alive the channel never closes, so waiting on it
    /// would hang the terminal `agent/run` response forever. Instead, once
    /// the provider's run future resolves we signal the task to flush what
    /// is queued and then exit — preserving per-spec ordering
    /// (notifications before the terminal response) for the notifications
    /// the run actually emitted, while late emissions from a leaked clone
    /// are dropped (they would otherwise be ordered after the response
    /// anyway, which the spec forbids).
    pub(crate) async fn close(self) {
        self.closing
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.stop.notify_one();
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
/// providers; the drain task flushes the queue and exits when either every
/// clone is dropped (channel EOF) or [`NotificationForwarder::close`] is
/// called after the run future resolves — whichever happens first.
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
    let stop = Arc::new(tokio::sync::Notify::new());
    let closing = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let task_stop = stop.clone();
    let task_closing = closing.clone();
    let task = tokio::spawn(async move {
        loop {
            // Once close() is signaled, drain ONLY the notifications already
            // queued (a bounded snapshot) and exit. We must not keep awaiting
            // `recv()` here: a leaked sink clone that keeps emitting would
            // otherwise starve the stop path and hang close() forever.
            if task_closing.load(std::sync::atomic::Ordering::SeqCst) {
                // Close the receiver FIRST so further `send`s are rejected;
                // this bounds the drain to the snapshot of notifications
                // already queued. Without this, a leaked sink clone emitting
                // faster than we drain could keep `try_recv()` returning
                // `Ok` indefinitely and hang close().
                rx.close();
                while let Ok(notification) = rx.try_recv() {
                    let frame =
                        RpcNotification::new(notification.method(), Some(notification.payload()));
                    write_notification(&stdout, &frame).await;
                }
                break;
            }

            tokio::select! {
                biased;
                // close() may fire while the queue is empty and we are parked
                // on recv(); the wakeup re-checks `closing` at the top of the
                // loop on the next iteration.
                _ = task_stop.notified() => continue,
                maybe = rx.recv() => match maybe {
                    Some(notification) => {
                        let frame = RpcNotification::new(
                            notification.method(),
                            Some(notification.payload()),
                        );
                        write_notification(&stdout, &frame).await;
                    }
                    // Channel EOF: every sink clone was dropped.
                    None => break,
                },
            }
        }
    });
    (
        sink,
        NotificationForwarder {
            task,
            closing,
            stop,
        },
    )
}

// =====================================================================
// Shared helpers (provider-protocol path)
// =====================================================================

/// Read one newline-delimited JSON-RPC request frame from `reader`.
///
/// Returns `Ok(None)` on EOF and `Ok(Some(_))` for each successfully parsed
/// frame. Frames that fail to parse are skipped (and the loop continues with
/// the next line).
async fn read_frame<R>(reader: &mut R) -> Result<Option<RpcRequest>>
where
    R: tokio::io::AsyncBufRead + Unpin,
{
    use tokio::io::AsyncBufReadExt;
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
async fn write_response(stdout: &Arc<Mutex<Stdout>>, response: &RpcResponse) {
    write_frame(stdout, response).await;
}

/// Serialize and write a JSON-RPC notification frame to the shared stdout
/// handle.
async fn write_notification(stdout: &Arc<Mutex<Stdout>>, notification: &RpcNotification) {
    write_frame(stdout, notification).await;
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

fn provider_health_response(
    id: Option<Value>,
    result: std::result::Result<HealthCheckResult, animus_provider_protocol::BackendError>,
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
/// The dedup is order-preserving: the first occurrence wins, so a plugin
/// that accidentally lists a method already advertised by the runtime (e.g.
/// `agent/run`) keeps the runtime's slot and silently drops the duplicate.
fn append_unique_capabilities(methods: &mut Vec<String>, extras: &[String]) {
    for extra in extras {
        if !methods.iter().any(|m| m == extra) {
            methods.push(extra.clone());
        }
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
        // `PluginCapabilities.cancellation` advertises JSON-RPC
        // `$/cancelRequest` lifecycle support, which this runtime does NOT
        // honor (the dispatcher drops `$/*` notifications other than ping).
        // In-flight runs are cancelled via the `agent/cancel` DOMAIN method
        // (advertised in `methods` above when the provider supports it), not
        // via `$/cancelRequest`. Leave this false so hosts that trust the
        // flag don't send lifecycle cancels that silently no-op.
        cancellation: false,
        projections: Vec::new(),
        subject_kinds: Vec::new(),
        mcp_tools: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use animus_plugin_protocol::HealthStatus;
    use animus_provider_protocol::{
        AgentNotification, AgentResumeRequest, AgentRunRequest, AgentRunResponse,
        BackendError as ProviderBackendError, NotificationSink, ProviderBackend,
        ProviderCapabilities, ProviderManifest, NOTIFICATION_AGENT_ERROR,
        NOTIFICATION_AGENT_OUTPUT, NOTIFICATION_AGENT_THINKING, NOTIFICATION_AGENT_TOOL_CALL,
        NOTIFICATION_AGENT_TOOL_RESULT,
    };
    use std::sync::Mutex as StdMutex;

    /// The kernel sends `approvals` (the kernel-mediated approval gate flag)
    /// as a top-level RPC param with no typed field on [`AgentRunRequest`].
    /// `extras` is `#[serde(flatten)]`, so it MUST survive deserialization
    /// into `request.extras` or the provider (e.g. oai) never sees the gate
    /// flag and approvals fail OPEN. This is the regression guard.
    #[test]
    fn agent_run_request_carries_approvals_through_extras() {
        let request = deserialize_params::<AgentRunRequest>(
            Some(json!({
                "prompt": "go",
                "cwd": "/tmp",
                "approvals": true,
                "custom_extra": "x",
            })),
            false,
        )
        .expect("parse agent/run params");
        assert_eq!(
            request.extras.get("approvals").and_then(Value::as_bool),
            Some(true),
            "approvals must reach AgentRunRequest.extras via the flatten field"
        );
        assert_eq!(
            request.extras.get("custom_extra").and_then(Value::as_str),
            Some("x"),
            "unmapped extras must not be dropped during deserialization"
        );
    }

    struct ScriptedProvider {
        session_id: String,
        script: Vec<AgentNotification>,
    }

    impl ScriptedProvider {
        fn new(session_id: &str, script: Vec<AgentNotification>) -> Self {
            Self {
                session_id: session_id.to_string(),
                script,
            }
        }
    }

    #[async_trait::async_trait]
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
            Ok(canned_response(&self.session_id))
        }

        async fn run_agent_streaming(
            &self,
            _request: AgentRunRequest,
            sink: NotificationSink,
        ) -> std::result::Result<AgentRunResponse, ProviderBackendError> {
            for notification in &self.script {
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

        for frame in recorded.iter() {
            let params = frame.params.as_ref().expect("notification params");
            assert_eq!(params["session_id"], "sess-1");
        }
    }

    #[tokio::test]
    async fn provider_streaming_default_impl_delegates_to_run_agent() {
        struct NonStreamingProvider {
            session_id: String,
        }

        #[async_trait::async_trait]
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
        drop(sink);
        forwarder.close().await;
    }

    #[tokio::test]
    async fn close_does_not_hang_when_a_sink_clone_outlives_the_run() {
        // Regression: a provider may stash a NotificationSink clone in a
        // per-session task that outlives run_agent_streaming. The mpsc
        // channel then never reaches EOF, so close() must not wait for it —
        // it flushes the queue and stops. Without the bounded-drain fix this
        // await blocks forever and the terminal agent/run response is lost.
        let stdout = Arc::new(Mutex::new(tokio::io::stdout()));
        let (sink, forwarder) = build_notification_sink(stdout);
        sink.emit(AgentNotification::Output {
            session_id: "sess-leak".into(),
            text: "buffered".into(),
            is_final: true,
        });
        // Keep a clone alive PAST close() — the leaked-clone scenario.
        let _lingering = sink.clone();
        drop(sink);

        tokio::time::timeout(std::time::Duration::from_secs(5), forwarder.close())
            .await
            .expect("close() must return even while a sink clone is still alive");
    }

    #[tokio::test]
    async fn close_does_not_hang_when_a_leaked_clone_keeps_emitting() {
        // Stronger regression for the starvation edge: a leaked sink clone
        // emits continuously, keeping the channel non-empty. A biased
        // drain-first loop would let recv() win forever and never observe
        // the stop signal; close() must still return promptly.
        let stdout = Arc::new(Mutex::new(tokio::io::stdout()));
        let (sink, forwarder) = build_notification_sink(stdout);
        let lingering = sink.clone();
        drop(sink);

        // Background producer that keeps emitting past close().
        let producer = tokio::spawn(async move {
            for _ in 0..100_000 {
                lingering.emit(AgentNotification::Output {
                    session_id: "sess-flood".into(),
                    text: "x".into(),
                    is_final: false,
                });
                tokio::task::yield_now().await;
            }
        });

        tokio::time::timeout(std::time::Duration::from_secs(5), forwarder.close())
            .await
            .expect("close() must not be starved by a continuously-emitting clone");
        producer.abort();
    }

    #[test]
    fn provider_capabilities_appends_extras() {
        struct StubProvider;

        #[async_trait::async_trait]
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

        assert_eq!(caps.methods[0], METHOD_AGENT_RUN);
        assert_eq!(caps.methods[1], "health/check");
        assert_eq!(caps.methods[2], METHOD_AGENT_CANCEL);
        assert_eq!(caps.methods[3], "$harness/cancellation-loop-v2");
        assert_eq!(caps.methods[4], "$harness/oai-style");
        assert_eq!(caps.methods.len(), 5);
    }

    #[test]
    fn provider_capabilities_dedupes_against_defaults() {
        struct StubProvider;

        #[async_trait::async_trait]
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

    #[tokio::test]
    async fn provider_initialize_response_carries_extra_capabilities() {
        struct StubProvider;

        #[async_trait::async_trait]
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
