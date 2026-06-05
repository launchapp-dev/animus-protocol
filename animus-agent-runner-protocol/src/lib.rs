//! Protocol types for `agent_runner` plugins.
//!
//! # DEPRECATED in animus-cli v0.5.3
//!
//! The standalone `agent-runner` sidecar was deleted from `animus-cli` in
//! v0.5.3. The CLI's `animus agent {run, status, cancel, control}` family
//! now talks directly to provider plugins via
//! `orchestrator_plugin_host::session::SessionBackendResolver` — there is
//! no longer a Unix-domain socket, a shared-secret auth handshake, or any
//! per-process sidecar to talk to. As a consequence, no consumer of this
//! crate exists inside the published animus-cli surface and no first-party
//! `agent_runner` plugin will ship.
//!
//! This crate is preserved at v0.1.1 as a historical reference for anyone
//! still running v0.5.2 or earlier. New plugin authors should target the
//! provider-plugin surface (`animus-provider-protocol` /
//! `animus-session-backend`) instead.
//!
//! Agent-runner plugins owned the sidecar process that actually spawned
//! the coding-agent CLI (claude, codex, gemini, opencode, oai-runner,
//! ...), supervised its lifetime, parsed its stdout into structured
//! events, and reported cost / token / artifact / tool-call telemetry
//! back to the host.
//!
//! The v0.5 reference implementation will be
//! `launchapp-dev/animus-agent-runner` (a lift-and-shift of the in-tree
//! `crates/agent-runner/` sidecar). For v0.5.4 and earlier the agent-runner
//! still ships in-tree and talks to the daemon over a Unix-domain socket
//! with a shared-secret auth handshake; this crate defines the stdio
//! JSON-RPC surface that replaces that socket in v0.5.5+.
//!
//! Plugin authors implement the `agent_runner/*` method family:
//!
//! - [`METHOD_AGENT_RUNNER_RUN`] — start a new agent run. The plugin
//!   responds once with [`AgentRunStarted`] when the run is accepted,
//!   then emits a stream of [`METHOD_AGENT_RUNNER_EVENT`] notifications
//!   carrying [`AgentRunEvent`] frames until the run reaches a terminal
//!   state.
//! - [`METHOD_AGENT_RUNNER_CONTROL`] — apply a [`AgentControlAction`]
//!   (terminate is the only universally-supported action; pause / resume
//!   are advertised via [`AgentRunnerCapabilities::pause_resume_support`]
//!   and reject otherwise).
//! - [`METHOD_AGENT_RUNNER_AGENT_STATUS`] — point-in-time status for one
//!   run.
//! - [`METHOD_AGENT_RUNNER_RUNNER_STATUS`] — sidecar-wide health and
//!   active-run count.
//! - [`METHOD_AGENT_RUNNER_MODEL_STATUS`] — preflight: are these models
//!   reachable (CLI binary present, API key present, ...)?
//!
//! ## Auth posture
//!
//! Unlike the v0.5.4 Unix-socket transport, the stdio surface inherits
//! the established Animus plugin auth posture: the daemon spawns the
//! plugin process as a direct child, owns its stdin / stdout / stderr,
//! and tears it down via process signals. There is no token handshake —
//! parent-spawn is the auth. This matches `animus-queue-protocol`,
//! `animus-workflow-runner-protocol`, `animus-notifier-protocol`, and
//! every other v0.5 kind.
//!
//! ## Project binding
//!
//! Some agent-runner plugins multiplex a single sidecar across every
//! project the daemon supervises (the v0.5 reference implementation does
//! exactly that — one sidecar per logged-in user, not per project). For
//! that reason this crate does NOT bind `project_root` at `initialize`
//! time the way the queue / workflow-runner crates do. Instead each
//! request that needs a project root carries it explicitly as
//! [`AgentRunRequest::project_root`]; plugins that prefer the
//! `project_binding` init-extension MAY still honor it and reject
//! requests whose `project_root` mismatches the bound value.

#![warn(missing_docs)]

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use animus_plugin_protocol::{error_codes, RpcError};

// =====================================================================
// Plugin-kind wire literal
// =====================================================================

/// `PluginKind` wire value for this kind.
///
/// Plugin manifests (`plugin.toml`) and discovery filters compare against
/// this exact string.
pub const KIND: &str = "agent_runner";

/// Per-crate semver protocol version. Reported via
/// [`animus_plugin_protocol::KindCapability::crate_version`].
pub const PROTOCOL_VERSION: &str = "0.1.0";

// =====================================================================
// Method-name constants
// =====================================================================

/// `agent_runner/run` — start a new agent run.
///
/// Request: [`AgentRunRequest`]. Response: [`AgentRunStarted`] (one-shot
/// acknowledgment that the run was accepted into the sidecar's
/// supervisor; the actual completion is announced via terminal
/// [`AgentRunEvent::Finished`] / [`AgentRunEvent::Error`] notifications
/// on the [`METHOD_AGENT_RUNNER_EVENT`] channel).
pub const METHOD_AGENT_RUNNER_RUN: &str = "agent_runner/run";

/// `agent_runner/control` — apply [`AgentControlAction`] to a running
/// agent. Request: [`AgentControlRequest`]. Response:
/// [`AgentControlResponse`].
pub const METHOD_AGENT_RUNNER_CONTROL: &str = "agent_runner/control";

/// `agent_runner/agent_status` — point-in-time status for a single run.
/// Request: [`AgentStatusRequest`]. Response:
/// [`AgentStatusQueryResponse`] (a tagged union of success +
/// error-with-code so callers can distinguish "not found" from "the
/// plugin failed").
pub const METHOD_AGENT_RUNNER_AGENT_STATUS: &str = "agent_runner/agent_status";

/// `agent_runner/runner_status` — sidecar-wide active-run count,
/// protocol version, optional metrics. Request:
/// [`RunnerStatusRequest`]. Response: [`RunnerStatusResponse`].
pub const METHOD_AGENT_RUNNER_RUNNER_STATUS: &str = "agent_runner/runner_status";

/// `agent_runner/model_status` — preflight availability check for a list
/// of model ids (is the CLI binary on PATH, is the API key present, has
/// the operator disabled this model, ...). Request:
/// [`ModelStatusRequest`]. Response: [`ModelStatusResponse`].
pub const METHOD_AGENT_RUNNER_MODEL_STATUS: &str = "agent_runner/model_status";

/// `agent_runner/event` — JSON-RPC notification (no `id`) the plugin
/// emits to push run lifecycle and output frames to the host.
///
/// Params shape: [`AgentRunEventParams`] (`{ "event": <AgentRunEvent> }`).
/// The `run_id` is embedded in the [`AgentRunEvent`] payload itself so a
/// single subscriber can demultiplex events for many concurrent runs.
pub const METHOD_AGENT_RUNNER_EVENT: &str = "agent_runner/event";

// =====================================================================
// Common newtypes
// =====================================================================

/// Stable identifier for one agent run.
///
/// Mirrors `protocol::RunId` from `animus-cli` (a transparent newtype
/// over `String`). Kept in this crate so plugin authors don't need to
/// depend on the main protocol crate.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct RunId(pub String);

impl RunId {
    /// Construct a [`RunId`] from any string-like value.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Borrow the inner string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Stable identifier for a coding-agent model (e.g. `"claude-sonnet-4-6"`,
/// `"gpt-5-codex-high"`).
///
/// Mirrors `protocol::ModelId` from `animus-cli` (a transparent newtype
/// over `String`). The plugin is responsible for mapping the id to the
/// underlying CLI invocation; the host treats it as opaque.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct ModelId(pub String);

impl ModelId {
    /// Construct a [`ModelId`] from any string-like value.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Borrow the inner string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

// =====================================================================
// agent_runner/run
// =====================================================================

/// Parameters for [`METHOD_AGENT_RUNNER_RUN`].
///
/// The `context` payload is intentionally opaque (a free-form JSON
/// value): it carries the host's "runtime contract" — tool, model,
/// prompt, optional CLI session resume plan, optional MCP transport
/// config. Plugin authors who want to validate it ahead of spawn time
/// SHOULD do so before sending [`AgentRunStarted`].
///
/// Well-known top-level keys produced by the in-tree
/// `build_runtime_contract_with_resume_and_mcp_config` helper:
///
/// - `tool`: string — coding-agent CLI name.
/// - `model`: string — backend-specific model id.
/// - `prompt`: string — initial user message.
/// - `cli`: object — capability / resume / executable hints.
/// - `mcp`: object — endpoint, agent id, stdio command, enforce_only
///   flag, allowed-tool prefixes.
///
/// New keys may be added without bumping the protocol version; plugin
/// implementations MUST ignore keys they do not recognize.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AgentRunRequest {
    /// Host's reported semver of this crate. Plugins SHOULD compare the
    /// major component against their built-against [`PROTOCOL_VERSION`]
    /// and respond with [`error_codes_agent_runner::PROTOCOL_VERSION_MISMATCH`]
    /// if the host is ahead of them.
    pub protocol_version: String,
    /// Run id chosen by the host. Plugins MUST use this id verbatim in
    /// every emitted [`AgentRunEvent`]; they MUST NOT mutate it.
    pub run_id: RunId,
    /// Model the host wants the agent to run.
    pub model: ModelId,
    /// Project root the run targets. Sandboxes / workspace-guard checks
    /// inside the plugin MUST refuse to escape this directory.
    pub project_root: String,
    /// Opaque runtime contract; see the type docstring for well-known
    /// keys.
    pub context: Value,
    /// Soft wall-clock timeout for the run, in seconds. `None` means
    /// "no host-imposed timeout"; the plugin MAY still enforce its own
    /// upper bound.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_secs: Option<u64>,
}

/// Response for [`METHOD_AGENT_RUNNER_RUN`].
///
/// The run is acknowledged synchronously; lifecycle progress is reported
/// asynchronously via [`METHOD_AGENT_RUNNER_EVENT`] notifications.
///
/// `reattached` is `true` when the host re-sent a run request for a
/// `run_id` that was already running in the sidecar (e.g. after a daemon
/// restart). In that case the plugin SHOULD replay the buffered events
/// it already emitted, then continue streaming live events.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AgentRunStarted {
    /// Echo of [`AgentRunRequest::run_id`].
    pub run_id: RunId,
    /// `true` iff the run was already in flight and this call is
    /// effectively a re-attach to an existing supervisor.
    #[serde(default)]
    pub reattached: bool,
    /// RFC 3339 timestamp the plugin recorded as the run's actual start
    /// time. Useful for cost / duration accounting on the host side.
    pub started_at: String,
}

// =====================================================================
// agent_runner/event (notification)
// =====================================================================

/// Notification payload for [`METHOD_AGENT_RUNNER_EVENT`].
///
/// Wrapped in a `params` object so JSON-RPC notification framing can
/// carry it without ambiguity. The `event` field always includes a
/// `run_id` so a single subscriber can demultiplex many concurrent
/// runs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AgentRunEventParams {
    /// The actual lifecycle / output event.
    pub event: AgentRunEvent,
}

/// One frame in the agent run lifecycle stream.
///
/// Variants are encoded with a `kind` tag so non-Rust subscribers can
/// switch on a string discriminant. Wire shapes mirror
/// `protocol::AgentRunEvent` from `animus-cli` 1:1.
///
/// The host treats [`AgentRunEvent::Finished`] and [`AgentRunEvent::Error`]
/// as terminal — no further events are expected for the same `run_id`
/// after one of those is emitted.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AgentRunEvent {
    /// Run was accepted by the supervisor and the agent CLI was spawned
    /// (or a replay session was opened).
    Started {
        /// Run id this event belongs to.
        run_id: RunId,
        /// RFC 3339 spawn timestamp.
        timestamp: String,
    },
    /// One chunk of agent CLI output. May be a partial line; subscribers
    /// MUST NOT assume newline boundaries.
    OutputChunk {
        /// Run id this event belongs to.
        run_id: RunId,
        /// Which stream produced the chunk.
        stream_type: OutputStreamType,
        /// The output bytes interpreted as UTF-8.
        text: String,
    },
    /// Telemetry sample emitted mid-run (cost, token counts, ...).
    Metadata {
        /// Run id this event belongs to.
        run_id: RunId,
        /// Cumulative USD cost reported by the agent CLI, if known.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cost: Option<f64>,
        /// Cumulative token usage reported by the agent CLI, if known.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tokens: Option<TokenUsage>,
    },
    /// Terminal error frame. No further events for `run_id` after this.
    Error {
        /// Run id this event belongs to.
        run_id: RunId,
        /// Human-readable error string. Plugins SHOULD use a leading
        /// machine-readable tag (e.g. `"protocol_version_mismatch: ..."`)
        /// when the cause maps to a known [`error_codes_agent_runner`]
        /// value.
        error: String,
    },
    /// Terminal completion frame. No further events for `run_id` after
    /// this.
    Finished {
        /// Run id this event belongs to.
        run_id: RunId,
        /// Exit code of the agent CLI, if it produced one. `None` means
        /// the CLI did not return a code (e.g. killed by signal before a
        /// status was recorded).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        exit_code: Option<i32>,
        /// Total run wall-clock duration in milliseconds.
        duration_ms: u64,
    },
    /// Agent invoked a tool. Mid-run frame; not terminal.
    ToolCall {
        /// Run id this event belongs to.
        run_id: RunId,
        /// Structured tool-call description.
        tool_info: ToolCallInfo,
    },
    /// Tool finished. Mid-run frame; not terminal.
    ToolResult {
        /// Run id this event belongs to.
        run_id: RunId,
        /// Structured tool-result description.
        result_info: ToolResultInfo,
    },
    /// Agent produced an artifact (file, code snippet, image, ...).
    /// Mid-run frame; not terminal.
    Artifact {
        /// Run id this event belongs to.
        run_id: RunId,
        /// Structured artifact description.
        artifact_info: ArtifactInfo,
    },
    /// Agent emitted private chain-of-thought / reasoning text. Mid-run
    /// frame; not terminal.
    Thinking {
        /// Run id this event belongs to.
        run_id: RunId,
        /// The reasoning text.
        content: String,
    },
}

/// Which stream of the agent CLI a chunk came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum OutputStreamType {
    /// Standard output.
    Stdout,
    /// Standard error.
    Stderr,
    /// Synthetic stream the runner uses for sidecar-internal messages
    /// (e.g. supervisor warnings) routed through the same event channel.
    System,
}

/// Token usage report attached to a [`AgentRunEvent::Metadata`] frame.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TokenUsage {
    /// Input / prompt tokens.
    pub input: u32,
    /// Output / completion tokens.
    pub output: u32,
    /// Reasoning tokens (for models that report them separately).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<u32>,
    /// Cached-prompt-read tokens.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_read: Option<u32>,
    /// Cached-prompt-write tokens.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_write: Option<u32>,
}

/// Structured description of one tool invocation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ToolCallInfo {
    /// Tool name (e.g. `"Bash"`, `"Read"`, `"Edit"`).
    pub tool_name: String,
    /// Tool parameters, opaque to this crate.
    pub parameters: Value,
    /// RFC 3339 invocation timestamp.
    pub timestamp: String,
}

/// Structured description of one tool result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ToolResultInfo {
    /// Tool name (matches the corresponding [`ToolCallInfo::tool_name`]).
    pub tool_name: String,
    /// Tool result payload, opaque to this crate.
    pub result: Value,
    /// Wall-clock duration of the tool call in milliseconds.
    pub duration_ms: u64,
    /// `true` iff the tool reported success.
    pub success: bool,
}

/// Structured description of one artifact the agent produced.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ArtifactInfo {
    /// Stable artifact id.
    pub artifact_id: String,
    /// Coarse artifact category.
    pub artifact_type: ArtifactType,
    /// Filesystem path for file-backed artifacts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_path: Option<String>,
    /// Size in bytes, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    /// MIME type, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
}

/// Coarse classification of an artifact.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactType {
    /// File on disk.
    File,
    /// Code snippet.
    Code,
    /// Image.
    Image,
    /// Document.
    Document,
    /// Structured data (JSON, CSV, ...).
    Data,
    /// Anything else.
    Other,
}

// =====================================================================
// agent_runner/control
// =====================================================================

/// Parameters for [`METHOD_AGENT_RUNNER_CONTROL`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AgentControlRequest {
    /// Run id to act on.
    pub run_id: RunId,
    /// Action to apply.
    pub action: AgentControlAction,
}

/// Lifecycle actions the host can request on a run.
///
/// The v0.5 reference implementation supports [`Self::Terminate`] only;
/// [`Self::Pause`] and [`Self::Resume`] are reserved for future runners
/// and currently return `success: false` with an explanatory `message`.
/// Plugins that DO implement pause / resume MUST advertise it via
/// [`AgentRunnerCapabilities::pause_resume_support`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum AgentControlAction {
    /// Pause a running agent (advisory).
    Pause,
    /// Resume a paused agent (advisory).
    Resume,
    /// Forcefully terminate a running agent.
    Terminate,
}

/// Response for [`METHOD_AGENT_RUNNER_CONTROL`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AgentControlResponse {
    /// Echo of [`AgentControlRequest::run_id`].
    pub run_id: RunId,
    /// `true` iff the requested action was applied. Idempotent: a
    /// `terminate` on an already-stopped run reports `false` (the
    /// action did not change state) but is not an error.
    pub success: bool,
    /// Optional human-readable detail.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

// =====================================================================
// agent_runner/agent_status
// =====================================================================

/// Parameters for [`METHOD_AGENT_RUNNER_AGENT_STATUS`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AgentStatusRequest {
    /// Run id to query.
    pub run_id: RunId,
}

/// Tagged-union response for [`METHOD_AGENT_RUNNER_AGENT_STATUS`].
///
/// Encoded as `{ "kind": "status" | "error", "payload": ... }` so a
/// caller can distinguish "the plugin doesn't know this run" from "the
/// plugin failed" without reading a free-form error string.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", content = "payload", rename_all = "snake_case")]
pub enum AgentStatusQueryResponse {
    /// Run exists and the plugin returned a snapshot.
    Status(AgentStatusResponse),
    /// Run does not exist, or another structured error occurred.
    Error(AgentStatusErrorResponse),
}

/// Snapshot of one run's lifecycle state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AgentStatusResponse {
    /// Echo of [`AgentStatusRequest::run_id`].
    pub run_id: RunId,
    /// Current lifecycle status.
    pub status: AgentStatus,
    /// Wall-clock elapsed time since [`Self::started_at`], in
    /// milliseconds.
    pub elapsed_ms: u64,
    /// RFC 3339 spawn timestamp.
    pub started_at: String,
    /// RFC 3339 terminal timestamp, if the run has finished.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<String>,
}

/// Structured error wrapper for [`AgentStatusQueryResponse::Error`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AgentStatusErrorResponse {
    /// Echo of [`AgentStatusRequest::run_id`].
    pub run_id: RunId,
    /// Machine-readable error category.
    pub code: AgentStatusErrorCode,
    /// Human-readable detail.
    pub message: String,
}

/// Error categories returned in [`AgentStatusErrorResponse::code`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AgentStatusErrorCode {
    /// The plugin has no record of this run id (never started, or
    /// already evicted from in-memory state).
    NotFound,
}

/// Possible lifecycle states reported by [`AgentStatusResponse::status`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum AgentStatus {
    /// Run has been accepted but has not yet emitted
    /// [`AgentRunEvent::Started`].
    Starting,
    /// Run is actively producing output.
    Running,
    /// Run is paused (only meaningful if the plugin advertises
    /// `pause_resume_support`).
    Paused,
    /// Run terminated successfully.
    Completed,
    /// Run terminated with an error.
    Failed,
    /// Run terminated because the host's timeout fired.
    Timeout,
    /// Run terminated because a `Terminate` control action was applied.
    Terminated,
}

// =====================================================================
// agent_runner/runner_status
// =====================================================================

/// Parameters for [`METHOD_AGENT_RUNNER_RUNNER_STATUS`].
///
/// Intentionally empty today; reserved for future filter / projection
/// flags. `deny_unknown_fields` is NOT applied so forward-compat
/// requests don't break older plugins.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RunnerStatusRequest {}

/// Response for [`METHOD_AGENT_RUNNER_RUNNER_STATUS`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct RunnerStatusResponse {
    /// Number of in-flight runs the plugin is supervising.
    pub active_agents: usize,
    /// Per-crate protocol version the plugin was built against. Mirrors
    /// [`PROTOCOL_VERSION`].
    pub protocol_version: String,
    /// Optional build hash so operators can correlate a running sidecar
    /// with a specific binary build.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build_id: Option<String>,
    /// Optional free-form telemetry blob (queue depths, supervisor
    /// counters, ...). Opaque to this crate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics: Option<Value>,
}

// =====================================================================
// agent_runner/model_status
// =====================================================================

/// Parameters for [`METHOD_AGENT_RUNNER_MODEL_STATUS`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ModelStatusRequest {
    /// Models to probe.
    pub models: Vec<ModelId>,
}

/// Response for [`METHOD_AGENT_RUNNER_MODEL_STATUS`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ModelStatusResponse {
    /// One entry per [`ModelStatusRequest::models`] id, in the same
    /// order.
    pub statuses: Vec<ModelStatus>,
}

/// Availability snapshot for one model.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ModelStatus {
    /// Model id this snapshot describes.
    pub model: ModelId,
    /// Availability classification.
    pub availability: ModelAvailability,
    /// Optional human-readable detail (e.g. which env var is missing).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<String>,
}

/// Availability categories returned in [`ModelStatus::availability`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ModelAvailability {
    /// CLI binary present, API key present, model not disabled.
    Available,
    /// The CLI binary for this model is not on `PATH`.
    MissingCli,
    /// The required API key environment variable is missing.
    MissingApiKey,
    /// The operator disabled this model.
    Disabled,
    /// The plugin couldn't classify availability (unexpected error).
    Error,
}

// =====================================================================
// Manifest + capabilities
// =====================================================================

/// Static manifest an agent-runner plugin declares at install time.
///
/// Mirrors the manifest pattern used by the workflow-runner protocol so
/// `animus plugin install` and `animus plugin info` surfaces can render
/// a consistent shape across kinds.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AgentRunnerManifest {
    /// Plugin name.
    pub name: String,
    /// Plugin semver.
    pub version: String,
    /// Description.
    pub description: String,
    /// Capability flags.
    pub capabilities: AgentRunnerCapabilities,
}

/// Backend-specific capability flags serialized into
/// [`animus_plugin_protocol::KindCapability::extra`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
pub struct AgentRunnerCapabilities {
    /// Plugin understands [`AgentControlAction::Pause`] /
    /// [`AgentControlAction::Resume`]. When `false`, the v0.5 reference
    /// runner reports `success: false` on those actions.
    #[serde(default)]
    pub pause_resume_support: bool,
    /// Plugin can re-attach to an in-flight run after a daemon restart,
    /// replaying buffered events on the [`METHOD_AGENT_RUNNER_RUN`] call
    /// rather than rejecting the duplicate.
    #[serde(default)]
    pub replay_support: bool,
    /// Plugin emits [`AgentRunEvent::Thinking`] frames for models that
    /// expose private reasoning text.
    #[serde(default)]
    pub thinking_events: bool,
    /// Plugin emits [`AgentRunEvent::ToolCall`] /
    /// [`AgentRunEvent::ToolResult`] frames.
    #[serde(default)]
    pub tool_events: bool,
    /// Plugin emits [`AgentRunEvent::Artifact`] frames.
    #[serde(default)]
    pub artifact_events: bool,
    /// Plugin enforces a per-run workspace-guard that refuses to spawn
    /// the agent CLI outside [`AgentRunRequest::project_root`].
    #[serde(default)]
    pub workspace_guard: bool,
}

// =====================================================================
// Errors
// =====================================================================

/// JSON-RPC error codes specific to the agent-runner protocol. The
/// `-32500..-32599` range is reserved for this kind (the `-32400..-32499`
/// range is owned by `animus-memory-store-protocol`).
pub mod error_codes_agent_runner {
    /// Host-reported protocol version is incompatible with what the
    /// plugin was built against. Plugin should respond on
    /// [`super::METHOD_AGENT_RUNNER_RUN`] with an error response carrying
    /// this code AND emit a terminal [`super::AgentRunEvent::Error`] so
    /// subscribers see the same failure on the event channel.
    pub const PROTOCOL_VERSION_MISMATCH: i32 = -32501;
    /// Caller addressed a `run_id` the plugin has no record of.
    pub const RUN_NOT_FOUND: i32 = -32502;
    /// Caller tried to start a new run with a `run_id` that is already
    /// in flight and the plugin's
    /// [`super::AgentRunnerCapabilities::replay_support`] is `false`.
    pub const RUN_ALREADY_RUNNING: i32 = -32503;
    /// Run is in a terminal state; further control actions are no-ops.
    pub const RUN_TERMINAL: i32 = -32504;
    /// Requested control action is not supported by this plugin (e.g.,
    /// `pause` without `pause_resume_support`).
    pub const CONTROL_ACTION_UNSUPPORTED: i32 = -32505;
    /// Model id requested in [`super::AgentRunRequest::model`] is not
    /// known to this plugin (no mapped CLI invocation).
    pub const MODEL_UNAVAILABLE: i32 = -32506;
    /// [`super::AgentRunRequest::context`] failed plugin-side
    /// validation (missing well-known key, malformed value, ...).
    pub const INVALID_RUNTIME_CONTRACT: i32 = -32507;
}

/// Errors an agent-runner backend may return.
///
/// Convertible into [`RpcError`] via the standard `From` impl so plugin
/// authors can write `?` against backend operations without a manual
/// mapping table.
#[derive(Debug, thiserror::Error)]
pub enum AgentRunnerBackendError {
    /// Caller addressed a `run_id` the plugin has no record of.
    #[error("run not found: {0}")]
    RunNotFound(String),

    /// Caller tried to start a new run with a `run_id` already in flight.
    #[error("run already running: {0}")]
    RunAlreadyRunning(String),

    /// Run is in a terminal state.
    #[error("run terminal: {0}")]
    RunTerminal(String),

    /// Requested control action is not supported by this plugin.
    #[error("control action unsupported: {0}")]
    ControlActionUnsupported(String),

    /// Model id is not mapped to a CLI invocation.
    #[error("model unavailable: {0}")]
    ModelUnavailable(String),

    /// [`AgentRunRequest::context`] failed plugin-side validation.
    #[error("invalid runtime contract: {0}")]
    InvalidRuntimeContract(String),

    /// Host-reported protocol version is incompatible.
    #[error("protocol version mismatch: {0}")]
    ProtocolVersionMismatch(String),

    /// Request was malformed at the domain level.
    #[error("invalid request: {0}")]
    InvalidRequest(String),

    /// Backend (or its upstream) is temporarily unavailable.
    #[error("backend unavailable: {0}")]
    Unavailable(String),

    /// Anything else.
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl From<AgentRunnerBackendError> for RpcError {
    fn from(error: AgentRunnerBackendError) -> Self {
        match error {
            AgentRunnerBackendError::RunNotFound(message) => RpcError {
                code: error_codes_agent_runner::RUN_NOT_FOUND,
                message,
                data: Some(serde_json::json!({"category": "run_not_found"})),
            },
            AgentRunnerBackendError::RunAlreadyRunning(message) => RpcError {
                code: error_codes_agent_runner::RUN_ALREADY_RUNNING,
                message,
                data: Some(serde_json::json!({"category": "run_already_running"})),
            },
            AgentRunnerBackendError::RunTerminal(message) => RpcError {
                code: error_codes_agent_runner::RUN_TERMINAL,
                message,
                data: Some(serde_json::json!({"category": "run_terminal"})),
            },
            AgentRunnerBackendError::ControlActionUnsupported(message) => RpcError {
                code: error_codes_agent_runner::CONTROL_ACTION_UNSUPPORTED,
                message,
                data: Some(serde_json::json!({"category": "control_action_unsupported"})),
            },
            AgentRunnerBackendError::ModelUnavailable(message) => RpcError {
                code: error_codes_agent_runner::MODEL_UNAVAILABLE,
                message,
                data: Some(serde_json::json!({"category": "model_unavailable"})),
            },
            AgentRunnerBackendError::InvalidRuntimeContract(message) => RpcError {
                code: error_codes_agent_runner::INVALID_RUNTIME_CONTRACT,
                message,
                data: Some(serde_json::json!({"category": "invalid_runtime_contract"})),
            },
            AgentRunnerBackendError::ProtocolVersionMismatch(message) => RpcError {
                code: error_codes_agent_runner::PROTOCOL_VERSION_MISMATCH,
                message,
                data: Some(serde_json::json!({"category": "protocol_version_mismatch"})),
            },
            AgentRunnerBackendError::InvalidRequest(message) => RpcError {
                code: error_codes::INVALID_PARAMS,
                message,
                data: Some(serde_json::json!({"category": "invalid_request"})),
            },
            AgentRunnerBackendError::Unavailable(message) => RpcError {
                code: error_codes::INTERNAL_ERROR,
                message: format!("backend unavailable: {message}"),
                data: Some(serde_json::json!({"category": "unavailable"})),
            },
            AgentRunnerBackendError::Other(error) => RpcError {
                code: error_codes::INTERNAL_ERROR,
                message: error.to_string(),
                data: Some(serde_json::json!({"category": "other"})),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_request_round_trips() {
        let req = AgentRunRequest {
            protocol_version: PROTOCOL_VERSION.to_string(),
            run_id: RunId::new("run-1"),
            model: ModelId::new("claude-sonnet-4-6"),
            project_root: "/repo".into(),
            context: serde_json::json!({
                "tool": "claude",
                "model": "claude-sonnet-4-6",
                "prompt": "implement TASK-1"
            }),
            timeout_secs: Some(900),
        };
        let v = serde_json::to_value(&req).unwrap();
        let back: AgentRunRequest = serde_json::from_value(v).unwrap();
        assert_eq!(back, req);
        // Transparent newtype: RunId/ModelId serialize as bare strings.
        let raw = serde_json::to_value(&req).unwrap();
        assert_eq!(raw.get("run_id").and_then(Value::as_str), Some("run-1"));
        assert_eq!(
            raw.get("model").and_then(Value::as_str),
            Some("claude-sonnet-4-6")
        );
    }

    #[test]
    fn run_request_omits_optional_timeout() {
        let req = AgentRunRequest {
            protocol_version: PROTOCOL_VERSION.to_string(),
            run_id: RunId::new("run-2"),
            model: ModelId::new("gpt-5-codex-high"),
            project_root: "/repo".into(),
            context: serde_json::json!({}),
            timeout_secs: None,
        };
        let v = serde_json::to_value(&req).unwrap();
        assert!(v.get("timeout_secs").is_none());
        let back: AgentRunRequest = serde_json::from_value(v).unwrap();
        assert_eq!(back, req);
    }

    #[test]
    fn run_event_round_trips_finished() {
        let evt = AgentRunEvent::Finished {
            run_id: RunId::new("run-1"),
            exit_code: Some(0),
            duration_ms: 12_345,
        };
        let v = serde_json::to_value(&evt).unwrap();
        assert_eq!(v.get("kind").and_then(Value::as_str), Some("finished"));
        let back: AgentRunEvent = serde_json::from_value(v).unwrap();
        assert_eq!(back, evt);
    }

    #[test]
    fn run_event_round_trips_output_chunk() {
        let evt = AgentRunEvent::OutputChunk {
            run_id: RunId::new("run-1"),
            stream_type: OutputStreamType::Stdout,
            text: "hello\nworld".into(),
        };
        let v = serde_json::to_value(&evt).unwrap();
        assert_eq!(v.get("kind").and_then(Value::as_str), Some("output_chunk"));
        assert_eq!(v.get("stream_type").and_then(Value::as_str), Some("stdout"));
        let back: AgentRunEvent = serde_json::from_value(v).unwrap();
        assert_eq!(back, evt);
    }

    #[test]
    fn event_params_wraps_event() {
        let params = AgentRunEventParams {
            event: AgentRunEvent::Started {
                run_id: RunId::new("run-7"),
                timestamp: "2026-06-04T00:00:00Z".into(),
            },
        };
        let v = serde_json::to_value(&params).unwrap();
        assert_eq!(
            v.pointer("/event/kind").and_then(Value::as_str),
            Some("started")
        );
        let back: AgentRunEventParams = serde_json::from_value(v).unwrap();
        assert_eq!(back, params);
    }

    #[test]
    fn control_request_round_trips() {
        let req = AgentControlRequest {
            run_id: RunId::new("run-1"),
            action: AgentControlAction::Terminate,
        };
        let v = serde_json::to_value(&req).unwrap();
        assert_eq!(v.get("action").and_then(Value::as_str), Some("terminate"));
        let back: AgentControlRequest = serde_json::from_value(v).unwrap();
        assert_eq!(back, req);
    }

    #[test]
    fn agent_status_query_response_tags_variants() {
        let ok = AgentStatusQueryResponse::Status(AgentStatusResponse {
            run_id: RunId::new("run-1"),
            status: AgentStatus::Running,
            elapsed_ms: 500,
            started_at: "2026-06-04T00:00:00Z".into(),
            completed_at: None,
        });
        let v = serde_json::to_value(&ok).unwrap();
        assert_eq!(v.get("kind").and_then(Value::as_str), Some("status"));
        assert!(v.get("payload").is_some());

        let err = AgentStatusQueryResponse::Error(AgentStatusErrorResponse {
            run_id: RunId::new("run-x"),
            code: AgentStatusErrorCode::NotFound,
            message: "no record".into(),
        });
        let v = serde_json::to_value(&err).unwrap();
        assert_eq!(v.get("kind").and_then(Value::as_str), Some("error"));
        assert_eq!(
            v.pointer("/payload/code").and_then(Value::as_str),
            Some("not_found")
        );
    }

    #[test]
    fn runner_status_round_trips() {
        let r = RunnerStatusResponse {
            active_agents: 3,
            protocol_version: PROTOCOL_VERSION.into(),
            build_id: Some("abc123".into()),
            metrics: Some(serde_json::json!({"queued": 0})),
        };
        let v = serde_json::to_value(&r).unwrap();
        let back: RunnerStatusResponse = serde_json::from_value(v).unwrap();
        assert_eq!(back, r);
    }

    #[test]
    fn model_status_round_trips() {
        let r = ModelStatusResponse {
            statuses: vec![
                ModelStatus {
                    model: ModelId::new("claude-sonnet-4-6"),
                    availability: ModelAvailability::Available,
                    details: None,
                },
                ModelStatus {
                    model: ModelId::new("gpt-5-codex-high"),
                    availability: ModelAvailability::MissingApiKey,
                    details: Some("OPENAI_API_KEY missing".into()),
                },
            ],
        };
        let v = serde_json::to_value(&r).unwrap();
        let back: ModelStatusResponse = serde_json::from_value(v).unwrap();
        assert_eq!(back, r);
    }

    #[test]
    fn capabilities_default_is_conservative() {
        let caps = AgentRunnerCapabilities::default();
        assert!(!caps.pause_resume_support);
        assert!(!caps.replay_support);
        assert!(!caps.thinking_events);
        assert!(!caps.tool_events);
        assert!(!caps.artifact_events);
        assert!(!caps.workspace_guard);
    }

    #[test]
    fn backend_error_run_not_found_maps_to_kind_code() {
        let rpc: RpcError = AgentRunnerBackendError::RunNotFound("run-x".into()).into();
        assert_eq!(rpc.code, error_codes_agent_runner::RUN_NOT_FOUND);
    }

    #[test]
    fn backend_error_invalid_request_maps_to_invalid_params() {
        let rpc: RpcError = AgentRunnerBackendError::InvalidRequest("bad payload".into()).into();
        assert_eq!(rpc.code, error_codes::INVALID_PARAMS);
    }
}
