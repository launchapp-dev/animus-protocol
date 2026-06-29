//! Request and Response shapes for every control-protocol method.
//!
//! Most domain types are re-exported from the existing
//! [`animus_subject_protocol`], [`animus_log_storage_protocol`], and
//! [`animus_trigger_protocol`] crates rather than re-declared here. The goal
//! is: one schema per concept across the whole protocol surface.
//!
//! The shapes in this module are deliberately permissive — fields default to
//! `None` / empty / `false` when omitted on the wire, so future protocol
//! revisions can extend them without breaking v0.1.3 clients.

use std::collections::BTreeMap;
use std::path::PathBuf;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use animus_actor::Actor;
use animus_log_storage_protocol::{LogEntry, LogLevel};
use animus_subject_protocol::{Subject, SubjectFilter, SubjectId, SubjectPatch, SubjectStatus};

// =====================================================================
// Subject requests / responses
// =====================================================================

/// Request for `subject/list`.
///
/// A wrapper around [`SubjectFilter`] so the wire shape matches the rest of
/// the control protocol (single `params` object) and future fields can be
/// added without breaking clients.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SubjectListRequest {
    /// Filter constraints. See [`SubjectFilter`].
    #[serde(default)]
    pub filter: SubjectFilter,
}

/// Response for `subject/list`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubjectListResponse {
    /// Subjects in this page.
    pub subjects: Vec<Subject>,
    /// Opaque cursor for the next page, or `None` if exhausted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    /// When the snapshot was taken.
    pub fetched_at: DateTime<Utc>,
}

/// Request for `subject/get`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubjectGetRequest {
    /// Subject id to fetch.
    pub id: SubjectId,
}

/// Request for `subject/create`.
///
/// Backends that don't support creation MUST advertise
/// `supports_create = false` and return
/// [`ControlError::NotSupported`](crate::ControlError::NotSupported).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubjectCreateRequest {
    /// Subject kind (e.g. `"task"`, `"issue"`).
    pub kind: String,
    /// Short title.
    pub title: String,
    /// Long-form body (markdown encouraged).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
    /// Initial normalized status. Defaults to [`SubjectStatus::Ready`] when
    /// omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<SubjectStatus>,
    /// Optional priority on a 0..=4 scale.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<u8>,
    /// Optional labels.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub labels: Vec<String>,
    /// Optional initial assignee.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assignee: Option<String>,
    /// Optional backend-specific custom fields.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub custom: BTreeMap<String, Value>,
}

/// Request for `subject/update`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubjectUpdateRequest {
    /// Subject id to update.
    pub id: SubjectId,
    /// Patch to apply.
    pub patch: SubjectPatch,
}

/// Request for `subject/next`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SubjectNextRequest {
    /// Restrict to a specific subject kind (e.g. `"task"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
}

/// Response for `subject/next` — the next ready subject, or `None` if the
/// queue is drained.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubjectNextResponse {
    /// The next dispatchable subject, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject: Option<Subject>,
}

/// Request for `subject/status`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubjectStatusRequest {
    /// Subject id to update.
    pub id: SubjectId,
    /// New normalized status.
    pub status: SubjectStatus,
}

/// Request for `subject/watch`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SubjectWatchRequest {
    /// Restrict to a specific subject kind (e.g. `"task"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// Optional filter applied to emitted events.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<SubjectFilter>,
}

// =====================================================================
// Plugin requests / responses
// =====================================================================

/// Request for `plugin/list`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PluginListRequest {
    /// Include manifest warnings in the response. Defaults to `false`.
    #[serde(default)]
    pub include_warnings: bool,
    /// Restrict to one plugin kind (e.g. `"provider"`, `"subject_backend"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
}

/// Response for `plugin/list`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PluginListResponse {
    /// Installed plugins.
    pub plugins: Vec<PluginInfo>,
    /// Manifest warnings, present when `include_warnings = true`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<PluginWarning>,
}

/// One installed plugin, returned by `plugin/list` and `plugin/info`.
///
/// Mirrors the daemon's existing JSON envelope shape so the existing CLI /
/// MCP / GraphQL responses can be served verbatim once the control surface
/// is wired through.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PluginInfo {
    /// Plugin name (manifest `name`).
    pub name: String,
    /// Semantic version from the manifest.
    pub version: String,
    /// Plugin kind: `"provider"`, `"subject_backend"`, `"trigger_backend"`,
    /// `"log_storage_backend"`, or `"custom"`.
    pub kind: String,
    /// Installation source (registry id, local path, git URL).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    /// Whether the plugin's signature has been verified.
    #[serde(default)]
    pub signature_verified: bool,
    /// Free-form description.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Resolved binary path on disk.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binary_path: Option<PathBuf>,
}

/// A non-fatal manifest warning for an installed plugin.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PluginWarning {
    /// Plugin the warning applies to.
    pub plugin: String,
    /// Human-readable message.
    pub message: String,
}

/// Request for `plugin/info`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PluginInfoRequest {
    /// Plugin name.
    pub name: String,
}

/// Request for `plugin/install`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PluginInstallRequest {
    /// Source — registry id, local path, or git URL.
    pub source: String,
    /// Optional pinned version.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    /// Skip interactive confirmation prompts.
    #[serde(default)]
    pub yes: bool,
    /// Skip signature verification (insecure; disallowed in CI).
    #[serde(default)]
    pub allow_unsigned: bool,
}

/// Response for `plugin/install`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PluginInstallResponse {
    /// The freshly installed plugin.
    pub plugin: PluginInfo,
    /// Install transcript (binary copy, signature check, lifecycle ping).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub steps: Vec<String>,
}

/// Request for `plugin/uninstall`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PluginUninstallRequest {
    /// Plugin name.
    pub name: String,
}

/// Request for `plugin/ping`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PluginPingRequest {
    /// Plugin name.
    pub name: String,
}

/// Response for `plugin/ping`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PluginPingResponse {
    /// Whether the plugin responded to the lifecycle ping.
    pub ok: bool,
    /// Round-trip latency in milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<u64>,
    /// Error category if `ok = false`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Request for `plugin/call`.
///
/// Opaque pass-through to a plugin domain method. Used by the
/// `animus.plugin.call` MCP tool.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PluginCallRequest {
    /// Plugin name.
    pub name: String,
    /// JSON-RPC method to invoke inside the plugin.
    pub method: String,
    /// JSON-RPC params. Defaults to `null`.
    #[serde(default)]
    pub params: Value,
}

/// Response for `plugin/call` — the plugin's raw JSON result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PluginCallResponse(pub Value);

/// Request for `plugin/search`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PluginSearchRequest {
    /// Free-text query string.
    pub query: String,
    /// Restrict to a plugin kind.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// Restrict to a tag.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
}

/// One plugin registry entry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PluginRegistryEntry {
    /// Registry id (e.g. `"animus-subject-linear"`).
    pub id: String,
    /// Plugin display name.
    pub name: String,
    /// Latest published version.
    pub version: String,
    /// Plugin kind.
    pub kind: String,
    /// Free-form description.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Source URL (registry, git).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// Tags.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    /// Whether this entry is already installed locally.
    #[serde(default)]
    pub installed: bool,
}

/// Response for `plugin/search` and `plugin/browse`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PluginSearchResponse {
    /// Matched registry entries.
    pub entries: Vec<PluginRegistryEntry>,
}

/// Request for `plugin/browse`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PluginBrowseRequest {
    /// Restrict to a plugin kind.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// Only include installed plugins.
    #[serde(default)]
    pub installed: bool,
    /// Only include not-yet-installed plugins.
    #[serde(default)]
    pub available: bool,
}

/// Request for `plugin/update`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PluginUpdateRequest {
    /// Restrict to a single plugin. `None` means all.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Restrict to a release tag (e.g. `"stable"`, `"prerelease"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
    /// If true, list available upgrades without applying them.
    #[serde(default)]
    pub dry_run: bool,
}

/// Response for `plugin/update`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PluginUpdateResponse {
    /// Per-plugin update results.
    pub updates: Vec<PluginUpdateEntry>,
}

/// One row of the `plugin/update` response.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PluginUpdateEntry {
    /// Plugin name.
    pub name: String,
    /// Currently installed version.
    pub from_version: String,
    /// Latest available version.
    pub to_version: String,
    /// Whether an upgrade was applied (always `false` when `dry_run = true`).
    pub applied: bool,
}

// =====================================================================
// Daemon requests / responses
// =====================================================================

/// Response for `daemon/status`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DaemonStatusResponse {
    /// Whether the daemon process is running.
    pub running: bool,
    /// PID, when running.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    /// Uptime in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uptime_seconds: Option<u64>,
    /// Daemon version string.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    /// Project root the daemon is scoped to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_root: Option<PathBuf>,
    /// Path to the daemon log file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_path: Option<PathBuf>,
}

/// Response for `daemon/health`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DaemonHealthResponse {
    /// Top-level health verdict.
    pub status: DaemonHealthStatus,
    /// Per-plugin health.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub plugins: Vec<PluginHealth>,
    /// Last-error string, when degraded/unhealthy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

/// Coarse health verdict reported by `daemon/health`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DaemonHealthStatus {
    /// All subsystems healthy.
    Healthy,
    /// At least one non-critical subsystem is degraded.
    Degraded,
    /// A critical subsystem is failing.
    Unhealthy,
    /// Daemon is not running.
    Down,
}

/// Health snapshot for a single plugin process.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PluginHealth {
    /// Plugin name.
    pub name: String,
    /// Plugin kind.
    pub kind: String,
    /// Health verdict.
    pub status: DaemonHealthStatus,
    /// Uptime in milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uptime_ms: Option<u64>,
    /// Last error string from the plugin, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

/// Response for `daemon/agents`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DaemonAgentsResponse {
    /// Currently active agents.
    pub agents: Vec<AgentInfo>,
}

/// One active agent session.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentInfo {
    /// Session id.
    pub session_id: String,
    /// Provider name (`"claude"`, `"codex"`, `"gemini"`, ...).
    pub provider: String,
    /// Model name.
    pub model: String,
    /// Workflow id this agent is bound to, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<String>,
    /// Phase id within the workflow, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase_id: Option<String>,
    /// When the session started.
    pub started_at: DateTime<Utc>,
}

/// Request for `daemon/events`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DaemonEventsRequest {
    /// Start from events after this timestamp.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub since: Option<DateTime<Utc>>,
}

/// One event delivered by the `daemon/events` stream.
///
/// The daemon emits events for workflow / phase / queue / plugin state
/// changes. The `kind` discriminator is opaque to the protocol; subscribers
/// match on it. Existing daemon event types (`workflow_started`,
/// `phase_completed`, `queue_drained`, etc.) carry their payload in
/// `payload`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DaemonRunEvent {
    /// Event id (monotonic per daemon process).
    pub id: String,
    /// When the event occurred.
    pub occurred_at: DateTime<Utc>,
    /// Event kind discriminator.
    pub kind: String,
    /// Free-form event payload.
    #[serde(default)]
    pub payload: Value,
}

/// Request for `daemon/logs`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DaemonLogsRequest {
    /// Start from entries at or after this timestamp.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub since: Option<DateTime<Utc>>,
    /// Minimum severity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub level: Option<LogLevel>,
    /// Restrict to one plugin's logs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plugin: Option<String>,
    /// If `true`, continue streaming after the historical tail.
    #[serde(default)]
    pub follow: bool,
}

/// One log entry delivered by the `daemon/logs` stream.
///
/// Wraps the existing [`LogEntry`] from `animus-log-storage-protocol` so the
/// log surface and the control surface share a single schema.
pub type DaemonLogEntry = LogEntry;

// =====================================================================
// Workflow requests / responses
// =====================================================================

/// A workflow's lifecycle status. Mirrors the daemon's existing
/// `WorkflowStatus` so existing JSON envelopes stay valid.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WorkflowStatus {
    /// Queued but not yet started.
    Pending,
    /// Currently running.
    Running,
    /// Paused waiting on a checkpoint.
    Paused,
    /// Finished successfully.
    Completed,
    /// Finished with a failure.
    Failed,
    /// Cancelled by an operator.
    Cancelled,
}

/// Request for `workflow/list`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct WorkflowListRequest {
    /// Restrict to runs in this status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<WorkflowStatus>,
    /// Pagination cursor returned by a prior call.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    /// Page size.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

/// Response for `workflow/list`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowListResponse {
    /// Workflow run summaries in this page.
    pub runs: Vec<WorkflowRunSummary>,
    /// Next-page cursor, or `None` if exhausted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

/// One row of `workflow/list`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowRunSummary {
    /// Workflow run id.
    pub id: String,
    /// Workflow definition name.
    pub definition: String,
    /// Current status.
    pub status: WorkflowStatus,
    /// Subject id this run is bound to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject_id: Option<SubjectId>,
    /// When the run started.
    pub started_at: DateTime<Utc>,
    /// When the run finished, if it has.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<DateTime<Utc>>,
}

/// Request for `workflow/get`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowGetRequest {
    /// Workflow run id.
    pub id: String,
}

/// Detailed workflow run, returned by `workflow/get`.
///
/// The full set of fields (phase history, run-step events, decisions,
/// checkpoints) lives under `detail` as opaque JSON so the protocol crate
/// doesn't need to mirror the entire daemon-side schema.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowRun {
    /// Summary fields.
    #[serde(flatten)]
    pub summary: WorkflowRunSummary,
    /// Full run detail. Schema is daemon-defined and stable across patch
    /// releases.
    #[serde(default)]
    pub detail: Value,
}

/// Request for `workflow/run`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowRunRequest {
    /// Task / subject id to run a workflow for.
    pub task_id: String,
    /// Workflow definition name (e.g. `"default"`, `"review"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub definition: Option<String>,
    /// Free-form parameters passed into the workflow template context.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub params: BTreeMap<String, Value>,
    /// Transport-asserted caller identity, relayed verbatim by the daemon to
    /// the workflow runner and downstream plugins for scoping. `None` for
    /// system/daemon-initiated runs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<Actor>,
}

/// Response for `workflow/run` and `workflow/execute`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowRunStart {
    /// The freshly created workflow run id.
    pub workflow_id: String,
    /// Status the run was initialized in (typically `pending` or `running`).
    pub status: WorkflowStatus,
    /// When the run started.
    pub started_at: DateTime<Utc>,
}

/// Request for `workflow/execute`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowExecuteRequest {
    /// Workflow definition name.
    pub definition: String,
    /// Free-form parameters passed into the workflow template context.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub params: BTreeMap<String, Value>,
    /// Optional subject id to associate the run with.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject_id: Option<SubjectId>,
    /// Transport-asserted caller identity (see [`WorkflowRunRequest::actor`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<Actor>,
}

/// Request for `workflow/pause`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowPauseRequest {
    /// Workflow run id.
    pub id: String,
}

/// Request for `workflow/resume`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowResumeRequest {
    /// Workflow run id.
    pub id: String,
    /// Optional human feedback to inject when resuming an approval-gated
    /// workflow. Surfaced to the next phase so the agent can react to the
    /// reviewer's comments.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub feedback: Option<String>,
}

/// Request for `workflow/events`.
///
/// Both filters are optional and combine with AND semantics: an event is
/// delivered when it matches the `workflow_id` filter (or it is `None`) AND
/// its `kind` is in `kinds` (or `kinds` is `None`). Added in v0.1.10.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct WorkflowEventsRequest {
    /// Restrict the stream to events for a single workflow run. `None`
    /// streams events for every workflow the daemon emits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<String>,
    /// Restrict the stream to specific event kinds (e.g.
    /// `["phase_started", "phase_completed", "workflow_completed"]`).
    /// `None` streams every kind.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kinds: Option<Vec<String>>,
}

/// One event delivered by the `workflow/events` stream.
///
/// The `kind` discriminator is opaque to the protocol; subscribers match on
/// it. Common values include `phase_started`, `phase_completed`,
/// `workflow_completed`, `workflow_failed`, but daemons may emit any kind
/// they like — clients SHOULD ignore unknown kinds. Added in v0.1.10.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowEvent {
    /// Workflow run id this event belongs to.
    pub workflow_id: String,
    /// Event kind discriminator.
    pub kind: String,
    /// Free-form, kind-specific event payload.
    #[serde(default)]
    pub payload: Value,
    /// When the event occurred.
    pub occurred_at: DateTime<Utc>,
}

/// Request for `workflow/cancel`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowCancelRequest {
    /// Workflow run id.
    pub id: String,
    /// Optional reason recorded with the cancellation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

// =====================================================================
// Agent requests / responses
// =====================================================================

/// Request for `agent/run`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentRunRequest {
    /// Provider name (`"claude"`, `"codex"`, `"gemini"`, ...).
    pub provider: String,
    /// Model name.
    pub model: String,
    /// Prompt text.
    pub prompt: String,
    /// Optional system message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system: Option<String>,
    /// Optional working directory override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<PathBuf>,
    /// Optional environment overrides.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
}

/// Response for `agent/run`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentRunResult {
    /// Session id used to subsequently call `agent/status` / `agent/cancel`.
    pub session_id: String,
    /// Provider-reported model.
    pub model: String,
    /// Final response text.
    pub output: String,
    /// Optional token usage stats.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<AgentUsage>,
}

/// Token usage reported by an agent session.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentUsage {
    /// Prompt tokens consumed.
    pub prompt_tokens: u64,
    /// Response tokens generated.
    pub completion_tokens: u64,
    /// Total cost in micro-USD, when reported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_micro_usd: Option<u64>,
}

/// Request for `agent/status`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentStatusRequest {
    /// Session id.
    pub id: String,
}

/// Response for `agent/status`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentStatus {
    /// Session id.
    pub session_id: String,
    /// Lifecycle status.
    pub status: AgentLifecycle,
    /// Provider.
    pub provider: String,
    /// Model.
    pub model: String,
    /// When the session started.
    pub started_at: DateTime<Utc>,
    /// When the session ended, if it has.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<DateTime<Utc>>,
    /// Final error string if status is [`AgentLifecycle::Failed`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

/// Lifecycle states for an agent session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentLifecycle {
    /// Session is in flight.
    Running,
    /// Session finished successfully.
    Completed,
    /// Session failed.
    Failed,
    /// Session was cancelled.
    Cancelled,
}

/// Request for `agent/cancel`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentCancelRequest {
    /// Session id to cancel.
    pub session_id: String,
}

// =====================================================================
// Queue requests / responses
// =====================================================================

/// Coarse status of a queue entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum QueueEntryStatus {
    /// Eligible to dispatch.
    Ready,
    /// Held by an operator.
    Held,
    /// Currently dispatched to a workflow run.
    InFlight,
    /// Dispatched and completed.
    Done,
    /// Dropped (removed without dispatch).
    Dropped,
}

/// One queue entry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueueEntry {
    /// Queue entry id.
    pub id: String,
    /// Subject id the entry references.
    pub subject_id: SubjectId,
    /// Current status.
    pub status: QueueEntryStatus,
    /// Priority on a 0..=4 scale (0 = none, 4 = critical).
    pub priority: u8,
    /// When the entry was enqueued.
    pub enqueued_at: DateTime<Utc>,
    /// Optional reason set with `queue/hold`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hold_reason: Option<String>,
}

/// Request for `queue/list`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct QueueListRequest {
    /// Restrict to entries in this status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<QueueEntryStatus>,
    /// Pagination cursor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    /// Page size.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

/// Response for `queue/list`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueueListResponse {
    /// Queue entries in this page.
    pub entries: Vec<QueueEntry>,
    /// Next-page cursor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

/// Request for `queue/enqueue`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueueEnqueueRequest {
    /// Subject / task id to enqueue.
    pub task_id: String,
    /// Optional priority (0..=4). Defaults to 2 (medium).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<u8>,
}

/// Request for `queue/drop`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueueDropRequest {
    /// Queue entry id.
    pub id: String,
}

/// Request for `queue/hold`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueueHoldRequest {
    /// Queue entry id.
    pub id: String,
    /// Optional reason.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// Request for `queue/release`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueueReleaseRequest {
    /// Queue entry id.
    pub id: String,
}

/// Request for `queue/reorder`.
///
/// Supports both single-entry and multi-entry reordering. Exactly one of
/// `id` or `subject_ids` must be provided. When `subject_ids` is used the
/// entries are placed contiguously relative to the anchor in the order given.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueueReorderRequest {
    /// Queue entry id to move (single-entry form).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// Queue entry ids to move as a contiguous group (multi-entry form).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subject_ids: Vec<String>,
    /// Optional anchor entry id. If set, the moved entries are placed
    /// adjacent to the anchor on the `position` side.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anchor_id: Option<String>,
    /// Where to place the entries relative to the anchor (or the queue ends if
    /// `anchor_id` is `None`).
    pub position: QueueReorderPosition,
}

/// Placement directive used by `queue/reorder`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum QueueReorderPosition {
    /// Place at the front of the queue (or before the anchor).
    Front,
    /// Place at the back of the queue (or after the anchor).
    Back,
    /// Place immediately before the anchor. Requires `anchor_id`.
    Before,
    /// Place immediately after the anchor. Requires `anchor_id`.
    After,
}

/// Response for `queue/stats`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueueStats {
    /// Number of entries currently ready.
    pub ready: u64,
    /// Number of entries currently held.
    pub held: u64,
    /// Number of entries currently in flight.
    pub in_flight: u64,
    /// Entries completed in the recent window.
    pub done_recent: u64,
    /// Entries dropped in the recent window.
    pub dropped_recent: u64,
}

// =====================================================================
// Project requests / responses
// =====================================================================

/// Request for `project/init`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ProjectInitRequest {
    /// Optional project template name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub template: Option<String>,
    /// Skip interactive prompts.
    #[serde(default)]
    pub yes: bool,
}

/// Request for `project/setup`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ProjectSetupRequest {
    /// Whether to wire up MCP configuration.
    #[serde(default)]
    pub mcp: bool,
    /// Whether to start the daemon as part of setup.
    #[serde(default)]
    pub start_daemon: bool,
}

/// Response shared by `project/init` and `project/setup`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectInfo {
    /// Project root directory.
    pub project_root: PathBuf,
    /// Repo scope identifier used for runtime state path.
    pub repo_scope: String,
    /// Whether `.animus/` was newly created.
    pub created: bool,
}

/// Response for `project/status`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectStatusResponse {
    /// Project root directory.
    pub project_root: PathBuf,
    /// Repo scope identifier.
    pub repo_scope: String,
    /// Number of workflow definitions visible to the project.
    pub workflow_definition_count: usize,
    /// Number of tracked subjects (tasks).
    pub task_count: usize,
    /// Number of active workflow runs.
    pub active_runs: usize,
    /// Number of queue entries.
    pub queue_size: usize,
}

// =====================================================================
// Streaming lifecycle
// =====================================================================

/// Payload carried by the `subscription/closed` notification (added in v0.1.12).
///
/// Emitted on a server-streaming subscription right before the daemon stops
/// sending notifications for that stream. `id` echoes the originating
/// streaming request id (matching the `params.id` convention used by every
/// `<group>/<event>` notification, see §14.3 of `spec.md`), so a single
/// client demultiplexing many subscriptions over one socket can route the
/// terminal frame to the right subscription.
///
/// `reason` is free-form for operator log messages and SHOULD NOT be parsed
/// by clients beyond surfacing it to the user.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubscriptionClosedPayload {
    /// Originating streaming request id (mirrors `params.id` on every
    /// notification belonging to this subscription).
    pub id: Value,
    /// Operator-facing close reason (e.g. `"daemon shutting down"`,
    /// `"workflow completed"`, `"subscription budget exceeded"`).
    pub reason: String,
}

// =====================================================================
// Misc
// =====================================================================

/// Empty response body. Used by mutating methods that have no result.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Unit {}

#[cfg(test)]
mod tests {
    use super::*;
    use animus_subject_protocol::SubjectStatus;

    fn fixed_ts() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-05-20T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    #[test]
    fn workflow_status_serializes_kebab_case() {
        assert_eq!(
            serde_json::to_value(WorkflowStatus::Running).unwrap(),
            serde_json::json!("running")
        );
        assert_eq!(
            serde_json::to_value(WorkflowStatus::Completed).unwrap(),
            serde_json::json!("completed")
        );
    }

    #[test]
    fn subject_list_request_round_trips() {
        let req = SubjectListRequest {
            filter: SubjectFilter {
                status: vec![SubjectStatus::Ready],
                kind: vec!["task".into()],
                ..Default::default()
            },
        };
        let v = serde_json::to_value(&req).unwrap();
        let back: SubjectListRequest = serde_json::from_value(v).unwrap();
        assert_eq!(back, req);
    }

    #[test]
    fn unit_serializes_as_empty_object() {
        let v = serde_json::to_value(Unit::default()).unwrap();
        assert_eq!(v, serde_json::json!({}));
    }

    #[test]
    fn workflow_run_summary_round_trips() {
        let s = WorkflowRunSummary {
            id: "wf-1".into(),
            definition: "default".into(),
            status: WorkflowStatus::Running,
            subject_id: Some(SubjectId::new("native:TASK-1")),
            started_at: fixed_ts(),
            finished_at: None,
        };
        let v = serde_json::to_value(&s).unwrap();
        let back: WorkflowRunSummary = serde_json::from_value(v).unwrap();
        assert_eq!(back, s);
    }

    #[test]
    fn workflow_run_request_actor_omitted_when_none() {
        let req = WorkflowRunRequest {
            task_id: "TASK-1".into(),
            definition: None,
            params: BTreeMap::new(),
            actor: None,
        };
        let v = serde_json::to_value(&req).unwrap();
        assert!(v.get("actor").is_none(), "actor must be omitted when None");
        let back: WorkflowRunRequest = serde_json::from_value(v).unwrap();
        assert_eq!(back, req);
    }

    #[test]
    fn workflow_run_request_round_trips_with_actor() {
        let req = WorkflowRunRequest {
            task_id: "TASK-1".into(),
            definition: Some("review".into()),
            params: BTreeMap::new(),
            actor: Some(Actor {
                user_id: "u-1".into(),
                claims: vec![animus_actor::CLAIM_ADMIN.into()],
                tenant_id: Some("t-1".into()),
            }),
        };
        let v = serde_json::to_value(&req).unwrap();
        assert!(v.get("actor").is_some());
        let back: WorkflowRunRequest = serde_json::from_value(v).unwrap();
        assert_eq!(back, req);
    }

    #[test]
    fn workflow_run_request_deserializes_without_actor() {
        let back: WorkflowRunRequest =
            serde_json::from_value(serde_json::json!({ "task_id": "TASK-1" })).unwrap();
        assert!(back.actor.is_none());
    }

    #[test]
    fn workflow_execute_request_actor_round_trips() {
        let none = WorkflowExecuteRequest {
            definition: "default".into(),
            params: BTreeMap::new(),
            subject_id: None,
            actor: None,
        };
        let v = serde_json::to_value(&none).unwrap();
        assert!(v.get("actor").is_none());
        assert_eq!(
            serde_json::from_value::<WorkflowExecuteRequest>(v).unwrap(),
            none
        );

        let some = WorkflowExecuteRequest {
            definition: "default".into(),
            params: BTreeMap::new(),
            subject_id: None,
            actor: Some(Actor::new("u-2")),
        };
        let v = serde_json::to_value(&some).unwrap();
        assert!(v.get("actor").is_some());
        assert_eq!(
            serde_json::from_value::<WorkflowExecuteRequest>(v).unwrap(),
            some
        );
    }

    #[test]
    fn plugin_install_request_defaults_omit_optional_fields() {
        let req = PluginInstallRequest {
            source: "animus-subject-linear".into(),
            version: None,
            yes: false,
            allow_unsigned: false,
        };
        let v = serde_json::to_value(&req).unwrap();
        assert!(v.get("version").is_none());
        assert_eq!(v.get("yes"), Some(&Value::Bool(false)));
    }
}
