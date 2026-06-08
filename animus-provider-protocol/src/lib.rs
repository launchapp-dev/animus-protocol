//! `ProviderBackend` trait and request/response shapes for Animus LLM
//! provider plugins.
//!
//! Provider plugins wrap an LLM CLI (Claude Code, Codex, Gemini, opencode,
//! ...) or HTTP API (OpenAI-compatible, on-prem hosted models, ...) so the
//! Animus daemon can spawn agent runs through a uniform interface. Each
//! provider runs as its own stdio child process, just like subject backends,
//! and speaks the same JSON-RPC 2.0 envelope defined in
//! [`animus-plugin-protocol`].
//!
//! The trait below is the Rust-side surface plugin authors implement.
//! Wire-level method names (`agent/run`, `agent/resume`, `agent/cancel`,
//! `health/check`) are exported as constants so non-Rust SDK authors can bind
//! to the same names.
//!
//! Streaming results (`agent/output`, `agent/thinking`, `agent/toolCall`,
//! `agent/toolResult`, `agent/error`) are emitted as JSON-RPC notifications
//! carrying the original `agent/run` request id. The runtime in
//! [`animus-plugin-runtime`] handles wiring the trait's event channel onto
//! the wire; trait implementers only emit events.

#![warn(missing_docs)]

use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

use animus_plugin_protocol::{error_codes, HealthCheckResult, RpcError};
use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

// =====================================================================
// Method-name constants
// =====================================================================

/// `agent/run` — start a new agent session.
pub const METHOD_AGENT_RUN: &str = "agent/run";

/// `agent/resume` — resume a prior agent session by id.
pub const METHOD_AGENT_RESUME: &str = "agent/resume";

/// `agent/cancel` — cancel an in-flight agent session.
pub const METHOD_AGENT_CANCEL: &str = "agent/cancel";

/// `agent/output` — server-streaming notification for incremental output.
pub const NOTIFICATION_AGENT_OUTPUT: &str = "agent/output";

/// `agent/thinking` — server-streaming notification for visible reasoning.
pub const NOTIFICATION_AGENT_THINKING: &str = "agent/thinking";

/// `agent/toolCall` — server-streaming notification when the agent invokes
/// a tool.
pub const NOTIFICATION_AGENT_TOOL_CALL: &str = "agent/toolCall";

/// `agent/toolResult` — server-streaming notification when a tool returns.
pub const NOTIFICATION_AGENT_TOOL_RESULT: &str = "agent/toolResult";

/// `agent/error` — server-streaming notification for recoverable or fatal
/// errors mid-run.
pub const NOTIFICATION_AGENT_ERROR: &str = "agent/error";

// =====================================================================
// Manifest
// =====================================================================

/// Static manifest describing what a provider plugin supports.
///
/// Returned by both the one-shot `--manifest` CLI mode and the `initialize`
/// JSON-RPC handshake. The fields here are the provider-specific overlay on
/// top of [`animus_plugin_protocol::PluginManifest`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ProviderManifest {
    /// Plugin name (e.g. `"animus-provider-claude"`).
    pub name: String,
    /// Plugin semver.
    pub version: String,
    /// Human-readable description.
    pub description: String,
    /// Concrete model identifiers this provider can route to.
    ///
    /// Examples: `["claude-sonnet-4-6", "claude-opus-4-7"]`,
    /// `["gpt-5", "gpt-5-mini"]`. Hosts use this to validate the `model`
    /// field of an [`AgentRunRequest`] before dispatching.
    pub supported_models: Vec<String>,
    /// Tool name passed through to the wrapped CLI (`"claude"`, `"codex"`,
    /// `"gemini"`, ...). Custom HTTP providers may set this to their plugin
    /// name.
    pub tool: String,
    /// Capability flags.
    pub capabilities: ProviderCapabilities,
}

/// Provider capability flags.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ProviderCapabilities {
    /// Provider emits `agent/output` deltas as the model produces them.
    #[serde(default)]
    pub streaming: bool,
    /// Provider supports `agent/resume` against prior session ids.
    #[serde(default)]
    pub resume: bool,
    /// Provider supports `agent/cancel`.
    #[serde(default)]
    pub cancellation: bool,
    /// Provider can edit files in the working directory (vs. read-only
    /// research providers).
    #[serde(default)]
    pub write_capable: bool,
    /// Provider supports MCP server bridging (i.e. accepts the
    /// `mcp_servers` field of [`AgentRunRequest`]).
    #[serde(default)]
    pub mcp: bool,
}

// =====================================================================
// Run requests / responses
// =====================================================================

/// Parameters for an `agent/run` (or `agent/resume`) call.
///
/// The same struct is reused for both methods; resume calls additionally
/// carry the prior `session_id` so the provider knows which transcript to
/// continue. The shape is intentionally tolerant of provider-specific
/// extensions via [`AgentRunRequest::extras`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AgentRunRequest {
    /// Existing session id when resuming. `None` for fresh runs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,

    /// User prompt (latest turn).
    pub prompt: String,

    /// Concrete model identifier. Must appear in
    /// [`ProviderManifest::supported_models`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,

    /// Optional system prompt. Many providers prefer this be set once at
    /// session start and ignored on subsequent turns; the provider decides.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_prompt: Option<String>,

    /// Working directory the agent should operate from. The provider
    /// passes this through to the wrapped CLI (e.g. `cwd` for `claude`).
    pub cwd: PathBuf,

    /// Project root, if distinct from `cwd` (e.g. running from a subdir).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_root: Option<PathBuf>,

    /// Permission mode (`"safe"`, `"acceptEdits"`, `"bypassPermissions"`,
    /// ...). Provider-specific; consult the provider's manifest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permission_mode: Option<String>,

    /// Hard timeout in seconds. The runtime will issue `agent/cancel` if
    /// this elapses.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_secs: Option<u64>,

    /// Environment variables to inject into the spawned child.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub env: HashMap<String, String>,

    /// MCP server descriptors for the provider to bridge.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mcp_servers: Option<Value>,

    /// Tool allow/deny config. Provider-specific shape.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tools: Option<Value>,

    /// Optional response schema (JSON Schema) the provider should constrain
    /// the model to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_schema: Option<Value>,

    /// Runtime contract envelope (workflow-runner-supplied metadata).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_contract: Option<Value>,

    /// Provider-specific extras the daemon doesn't interpret.
    #[serde(default, flatten)]
    pub extras: HashMap<String, Value>,
}

/// Parameters for an `agent/resume` call.
///
/// Re-exports [`AgentRunRequest`] under the resume name so callers are
/// explicit about intent. The wire shape is identical; the runtime
/// distinguishes by RPC method name.
pub type AgentResumeRequest = AgentRunRequest;

/// Parameters for an `agent/cancel` call.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AgentCancelRequest {
    /// Session id to cancel.
    pub session_id: String,
}

/// Final response to `agent/run` or `agent/resume`.
///
/// Streaming notifications are sent during the run; this is the aggregated
/// terminal payload. Hosts may persist it as the canonical run record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AgentRunResponse {
    /// Provider-issued session id. Stable for the life of the session;
    /// usable with `agent/resume` later.
    pub session_id: String,

    /// Process exit code from the wrapped CLI, if any.
    pub exit_code: i32,

    /// Concatenated final assistant output.
    pub output: String,

    /// Free-form metadata entries the provider chose to surface.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub metadata: Vec<Value>,

    /// All tool invocations the agent made during the run.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<Value>,

    /// All tool results returned to the agent.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_results: Vec<Value>,

    /// Visible reasoning traces (when the model produced any).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub thinking: Vec<String>,

    /// Errors emitted during the run (recoverable or terminal).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<String>,

    /// Total wall-clock duration of the run.
    pub duration_ms: u64,

    /// Provider-specific backend label (e.g. `"claude-code:1.0.0"`).
    pub backend: String,

    /// Token-accounting summary, if the provider tracks it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tokens_used: Option<TokenUsage>,

    /// Optional verdict for review/QA agents that produce pass/fail
    /// decisions. Free-form so review providers can shape their own
    /// envelopes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decision_verdict: Option<Value>,
}

/// Token-accounting summary for an agent run.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TokenUsage {
    /// Input tokens consumed.
    pub input: u64,
    /// Output tokens generated.
    pub output: u64,
    /// Tokens served from a prompt cache, if applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cached: Option<u64>,
    /// Tokens written to a prompt cache, if applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_writes: Option<u64>,
}

// =====================================================================
// Errors
// =====================================================================

/// Errors a provider may return.
#[derive(Debug, thiserror::Error)]
pub enum BackendError {
    /// Caller asked for a model the provider doesn't support.
    #[error("model not supported: {0}")]
    ModelNotSupported(String),

    /// Wrapped CLI failed to start.
    #[error("session start failed: {0}")]
    SessionStartFailed(String),

    /// Wrapped CLI exited with a non-zero status mid-run.
    #[error("agent run failed: {0}")]
    RunFailed(String),

    /// Provider was cancelled.
    #[error("cancelled")]
    Cancelled,

    /// Provider (or its upstream) is temporarily unavailable.
    #[error("provider unavailable: {0}")]
    Unavailable(String),

    /// Anything else.
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl From<BackendError> for RpcError {
    fn from(error: BackendError) -> Self {
        match error {
            BackendError::ModelNotSupported(msg) => RpcError {
                code: error_codes::INVALID_PARAMS,
                message: format!("model not supported: {msg}"),
                data: Some(serde_json::json!({"category": "model_not_supported"})),
            },
            BackendError::SessionStartFailed(msg) => RpcError {
                code: error_codes::INTERNAL_ERROR,
                message: format!("session start failed: {msg}"),
                data: Some(serde_json::json!({"category": "session_start_failed"})),
            },
            BackendError::RunFailed(msg) => RpcError {
                code: error_codes::INTERNAL_ERROR,
                message: format!("agent run failed: {msg}"),
                data: Some(serde_json::json!({"category": "run_failed"})),
            },
            BackendError::Cancelled => RpcError {
                code: error_codes::REQUEST_CANCELLED,
                message: "cancelled".to_string(),
                data: Some(serde_json::json!({"category": "cancelled"})),
            },
            BackendError::Unavailable(msg) => RpcError {
                code: error_codes::INTERNAL_ERROR,
                message: format!("provider unavailable: {msg}"),
                data: Some(serde_json::json!({"category": "unavailable"})),
            },
            BackendError::Other(error) => RpcError {
                code: error_codes::INTERNAL_ERROR,
                message: error.to_string(),
                data: Some(serde_json::json!({"category": "other"})),
            },
        }
    }
}

// =====================================================================
// Streaming notification surface
// =====================================================================

/// A streaming notification a provider may emit mid-run.
///
/// The runtime in [`animus-plugin-runtime`] wraps these into JSON-RPC
/// notifications using the wire-method constants ([`NOTIFICATION_AGENT_OUTPUT`],
/// [`NOTIFICATION_AGENT_THINKING`], [`NOTIFICATION_AGENT_TOOL_CALL`],
/// [`NOTIFICATION_AGENT_TOOL_RESULT`], [`NOTIFICATION_AGENT_ERROR`]) and
/// forwards them to the host on the same channel as the eventual
/// [`AgentRunResponse`] reply. Providers only construct the variants — they
/// don't need to touch JSON-RPC themselves.
///
/// `session_id` is filled in by the provider once known. All variants are
/// safe to emit before [`AgentRunResponse`] is returned; emissions after the
/// response are ignored by the runtime.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum AgentNotification {
    /// Incremental text the model has produced. Maps to
    /// [`NOTIFICATION_AGENT_OUTPUT`].
    Output {
        /// Stable session id this delta belongs to.
        session_id: String,
        /// Text delta.
        text: String,
        /// `true` when this is the final aggregated text for the turn.
        #[serde(default)]
        is_final: bool,
    },
    /// Visible reasoning from the model. Maps to
    /// [`NOTIFICATION_AGENT_THINKING`].
    Thinking {
        /// Stable session id.
        session_id: String,
        /// Reasoning text.
        text: String,
    },
    /// Agent invoked a tool. Maps to [`NOTIFICATION_AGENT_TOOL_CALL`].
    ToolCall {
        /// Stable session id.
        session_id: String,
        /// Tool name.
        name: String,
        /// Tool arguments.
        arguments: Value,
        /// MCP server that hosts the tool, if known.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        server: Option<String>,
    },
    /// Tool returned a result. Maps to [`NOTIFICATION_AGENT_TOOL_RESULT`].
    ToolResult {
        /// Stable session id.
        session_id: String,
        /// Tool name.
        name: String,
        /// Tool output.
        output: Value,
        /// True if the tool reported success.
        success: bool,
    },
    /// Error encountered mid-run. Maps to [`NOTIFICATION_AGENT_ERROR`].
    Error {
        /// Stable session id.
        session_id: String,
        /// Error message.
        message: String,
        /// True if the run continues after this error.
        recoverable: bool,
    },
}

impl AgentNotification {
    /// Wire-method constant for the JSON-RPC notification this variant maps to.
    pub fn method(&self) -> &'static str {
        match self {
            AgentNotification::Output { .. } => NOTIFICATION_AGENT_OUTPUT,
            AgentNotification::Thinking { .. } => NOTIFICATION_AGENT_THINKING,
            AgentNotification::ToolCall { .. } => NOTIFICATION_AGENT_TOOL_CALL,
            AgentNotification::ToolResult { .. } => NOTIFICATION_AGENT_TOOL_RESULT,
            AgentNotification::Error { .. } => NOTIFICATION_AGENT_ERROR,
        }
    }

    /// The wire payload for the notification (i.e. its `params`).
    ///
    /// The shapes here match `spec.md` § 10.3 — `{ session_id, text, final }`
    /// for output, `{ session_id, text }` for thinking, `{ session_id, name,
    /// arguments, server }` for tool calls, `{ session_id, name, output,
    /// success }` for tool results, and `{ session_id, message, recoverable }`
    /// for errors.
    pub fn payload(&self) -> Value {
        match self {
            AgentNotification::Output {
                session_id,
                text,
                is_final,
            } => serde_json::json!({
                "session_id": session_id,
                "text": text,
                "final": is_final,
            }),
            AgentNotification::Thinking { session_id, text } => serde_json::json!({
                "session_id": session_id,
                "text": text,
            }),
            AgentNotification::ToolCall {
                session_id,
                name,
                arguments,
                server,
            } => serde_json::json!({
                "session_id": session_id,
                "name": name,
                "arguments": arguments,
                "server": server,
            }),
            AgentNotification::ToolResult {
                session_id,
                name,
                output,
                success,
            } => serde_json::json!({
                "session_id": session_id,
                "name": name,
                "output": output,
                "success": success,
            }),
            AgentNotification::Error {
                session_id,
                message,
                recoverable,
            } => serde_json::json!({
                "session_id": session_id,
                "message": message,
                "recoverable": recoverable,
            }),
        }
    }
}

/// Sink a provider emits [`AgentNotification`]s through during an
/// `agent/run` or `agent/resume` call.
///
/// The runtime in [`animus-plugin-runtime`] constructs a sink that forwards
/// each emission as a JSON-RPC notification on stdout. Tests can construct a
/// recording sink by passing a closure that pushes into a `Vec` behind a
/// `Mutex`.
///
/// `emit` is synchronous and fire-and-forget — the underlying channel never
/// applies back-pressure. Providers that need to drop events under load
/// should batch or coalesce on their side before calling `emit`.
///
/// Sinks are cheap to clone (`Arc` under the hood) — providers may stash a
/// clone in a per-session task or pass the sink down into a parser loop.
#[derive(Clone)]
pub struct NotificationSink {
    inner: Arc<dyn Fn(AgentNotification) + Send + Sync>,
}

impl NotificationSink {
    /// Construct a sink from any send+sync callable.
    pub fn new<F>(f: F) -> Self
    where
        F: Fn(AgentNotification) + Send + Sync + 'static,
    {
        Self { inner: Arc::new(f) }
    }

    /// Construct a no-op sink. Useful for in-process unit tests that don't
    /// care about streaming and for the back-compat path on
    /// [`ProviderBackend::run_agent`].
    pub fn noop() -> Self {
        Self::new(|_| {})
    }

    /// Emit a notification through the sink.
    pub fn emit(&self, notification: AgentNotification) {
        (self.inner)(notification);
    }
}

impl fmt::Debug for NotificationSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NotificationSink").finish_non_exhaustive()
    }
}

// =====================================================================
// The trait
// =====================================================================

/// What a provider plugin implements.
///
/// The Animus daemon uses this trait via the runtime in
/// [`animus-plugin-runtime`]. Trait implementers don't deal with JSON-RPC
/// directly; they receive deserialized request structs and return
/// deserialized response structs (or errors).
///
/// # Streaming
///
/// `run_agent` returns the *final* aggregated response. Incremental output
/// is delivered through a side channel the runtime supplies — this trait
/// surface intentionally hides that detail. See the runtime crate for the
/// concrete `EventEmitter` shape used in v0.4.0; in the meantime, providers
/// can use the wire constants (e.g. [`NOTIFICATION_AGENT_OUTPUT`]) to drive
/// their own streaming if they bypass the runtime.
#[async_trait]
pub trait ProviderBackend: Send + Sync + 'static {
    /// Static manifest. Should be cheap (preferably a constant).
    fn manifest(&self) -> ProviderManifest;

    /// Start a fresh agent session.
    ///
    /// This is the non-streaming entrypoint and remains the canonical surface
    /// for providers that don't have incremental output to share. Providers
    /// that *do* want to stream should also override
    /// [`run_agent_streaming`](Self::run_agent_streaming) and emit
    /// [`AgentNotification`]s through the supplied [`NotificationSink`] as
    /// events arrive.
    async fn run_agent(&self, request: AgentRunRequest) -> Result<AgentRunResponse, BackendError>;

    /// Streaming variant of [`run_agent`](Self::run_agent).
    ///
    /// The default implementation forwards to `run_agent` and ignores the
    /// sink — existing providers continue to compile and behave exactly as
    /// before. Providers that wrap a session backend with an event stream
    /// (e.g. the CLI providers wrapping `animus-session-backend`) should
    /// override this method to call `sink.emit(...)` for each
    /// [`AgentNotification`] before returning the aggregated response.
    ///
    /// The runtime forwards every emission as a JSON-RPC notification on
    /// stdout using [`AgentNotification::method`] and
    /// [`AgentNotification::payload`].
    async fn run_agent_streaming(
        &self,
        request: AgentRunRequest,
        _sink: NotificationSink,
    ) -> Result<AgentRunResponse, BackendError> {
        self.run_agent(request).await
    }

    /// Resume a prior session by id. Providers without resume support
    /// should advertise `capabilities.resume = false` in the manifest and
    /// return [`BackendError::Other`] with a clear message if called.
    async fn resume_agent(
        &self,
        request: AgentResumeRequest,
    ) -> Result<AgentRunResponse, BackendError>;

    /// Streaming variant of [`resume_agent`](Self::resume_agent).
    ///
    /// See [`run_agent_streaming`](Self::run_agent_streaming) for the
    /// semantics; the default impl is the same back-compat shim.
    async fn resume_agent_streaming(
        &self,
        request: AgentResumeRequest,
        _sink: NotificationSink,
    ) -> Result<AgentRunResponse, BackendError> {
        self.resume_agent(request).await
    }

    /// Cancel an in-flight session.
    async fn cancel_agent(&self, session_id: &str) -> Result<(), BackendError>;

    /// Provider health.
    async fn health(&self) -> Result<HealthCheckResult, BackendError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_round_trips() {
        let manifest = ProviderManifest {
            name: "animus-provider-claude".into(),
            version: "0.1.0".into(),
            description: "Claude Code CLI provider".into(),
            supported_models: vec!["claude-sonnet-4-6".into()],
            tool: "claude".into(),
            capabilities: ProviderCapabilities {
                streaming: true,
                resume: true,
                cancellation: true,
                write_capable: true,
                mcp: true,
            },
        };
        let v = serde_json::to_value(&manifest).unwrap();
        let back: ProviderManifest = serde_json::from_value(v).unwrap();
        assert_eq!(back, manifest);
    }

    #[test]
    fn cancel_maps_to_request_cancelled() {
        let rpc: RpcError = BackendError::Cancelled.into();
        assert_eq!(rpc.code, error_codes::REQUEST_CANCELLED);
    }

    #[test]
    fn agent_notification_method_and_payload_match_spec() {
        let output = AgentNotification::Output {
            session_id: "s1".into(),
            text: "hi".into(),
            is_final: true,
        };
        assert_eq!(output.method(), NOTIFICATION_AGENT_OUTPUT);
        let payload = output.payload();
        assert_eq!(payload["session_id"], "s1");
        assert_eq!(payload["text"], "hi");
        assert_eq!(payload["final"], true);

        let tool_call = AgentNotification::ToolCall {
            session_id: "s2".into(),
            name: "shell".into(),
            arguments: serde_json::json!({"cmd": "ls"}),
            server: Some("local".into()),
        };
        assert_eq!(tool_call.method(), NOTIFICATION_AGENT_TOOL_CALL);
        let payload = tool_call.payload();
        assert_eq!(payload["name"], "shell");
        assert_eq!(payload["server"], "local");
        assert_eq!(payload["arguments"]["cmd"], "ls");
    }

    #[test]
    fn notification_sink_records_emissions_in_order() {
        use std::sync::Mutex;

        let recorder: Arc<Mutex<Vec<AgentNotification>>> = Arc::new(Mutex::new(Vec::new()));
        let r2 = recorder.clone();
        let sink = NotificationSink::new(move |n| r2.lock().unwrap().push(n));

        sink.emit(AgentNotification::Output {
            session_id: "s".into(),
            text: "first".into(),
            is_final: false,
        });
        sink.emit(AgentNotification::Thinking {
            session_id: "s".into(),
            text: "reason".into(),
        });
        sink.emit(AgentNotification::Error {
            session_id: "s".into(),
            message: "boom".into(),
            recoverable: true,
        });

        let recorded = recorder.lock().unwrap();
        assert_eq!(recorded.len(), 3);
        assert_eq!(recorded[0].method(), NOTIFICATION_AGENT_OUTPUT);
        assert_eq!(recorded[1].method(), NOTIFICATION_AGENT_THINKING);
        assert_eq!(recorded[2].method(), NOTIFICATION_AGENT_ERROR);
    }
}
