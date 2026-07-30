//! JSON-RPC method-name constants for the control protocol.
//!
//! Methods are grouped by domain (`subject`, `plugin`, `daemon`, `workflow`,
//! `agent`, `queue`, `project`) and named `<group>/<verb>`. Streaming methods
//! follow the convention `<group>/watch` for subscriptions tied to a single
//! resource and `<group>/events` for broader event streams; both emit
//! notifications via a paired `<group>/<event>` notification method.
//!
//! Method names are deliberately exposed as `const &'static str` rather than an
//! enum so transports can use them as map keys, log targets, and JSON-RPC
//! method literals without a translation step.

// =====================================================================
// Subject operations
// =====================================================================

/// `subject/list` — list dispatchable subjects, optionally filtered.
pub const METHOD_SUBJECT_LIST: &str = "subject/list";

/// `subject/get` — fetch a single subject by id.
pub const METHOD_SUBJECT_GET: &str = "subject/get";

/// `subject/create` — create a new subject in a writable backend.
pub const METHOD_SUBJECT_CREATE: &str = "subject/create";

/// `subject/update` — apply a patch to a subject.
pub const METHOD_SUBJECT_UPDATE: &str = "subject/update";

/// `subject/next` — return the next ready subject for dispatch.
pub const METHOD_SUBJECT_NEXT: &str = "subject/next";

/// `subject/status` — set the normalized status of a subject.
pub const METHOD_SUBJECT_STATUS: &str = "subject/status";

/// `subject/watch` — open a server-streaming subscription of subject changes.
pub const METHOD_SUBJECT_WATCH: &str = "subject/watch";

/// `subject/changed` — notification emitted by [`METHOD_SUBJECT_WATCH`] streams.
pub const NOTIFICATION_SUBJECT_CHANGED: &str = "subject/changed";

/// `subscription/closed` — terminal notification emitted by the daemon on
/// any open server-streaming subscription right before the daemon closes
/// the stream. Carries [`crate::types::SubscriptionClosedPayload`]. Added
/// in v0.1.12.
///
/// Servers that emit this notification let the client distinguish a
/// daemon-initiated terminal close (subscription resource gone, daemon
/// shutting down, capacity exhausted) from a transport-level disconnect.
/// Clients that don't recognize the notification still observe the stream
/// close via socket EOF, matching pre-v0.1.12 behavior.
pub const NOTIFICATION_SUBSCRIPTION_CLOSED: &str = "subscription/closed";

// =====================================================================
// Plugin operations
// =====================================================================

/// `plugin/list` — list installed plugins.
pub const METHOD_PLUGIN_LIST: &str = "plugin/list";

/// `plugin/info` — return detailed info for a single installed plugin.
pub const METHOD_PLUGIN_INFO: &str = "plugin/info";

/// `plugin/install` — install a plugin from a registry entry or local path.
pub const METHOD_PLUGIN_INSTALL: &str = "plugin/install";

/// `plugin/uninstall` — remove an installed plugin.
pub const METHOD_PLUGIN_UNINSTALL: &str = "plugin/uninstall";

/// `plugin/ping` — health-check ping into a named plugin.
pub const METHOD_PLUGIN_PING: &str = "plugin/ping";

/// `plugin/call` — opaque pass-through invocation of a custom plugin method.
pub const METHOD_PLUGIN_CALL: &str = "plugin/call";

/// `plugin/search` — search the plugin registry by free-text query.
pub const METHOD_PLUGIN_SEARCH: &str = "plugin/search";

/// `plugin/browse` — list registry entries by kind / install status.
pub const METHOD_PLUGIN_BROWSE: &str = "plugin/browse";

/// `plugin/update` — check for and apply plugin upgrades.
pub const METHOD_PLUGIN_UPDATE: &str = "plugin/update";

// =====================================================================
// Daemon operations
// =====================================================================

/// `daemon/status` — return process status (PID, uptime, version).
pub const METHOD_DAEMON_STATUS: &str = "daemon/status";

/// `daemon/health` — full health snapshot incl. per-plugin health.
pub const METHOD_DAEMON_HEALTH: &str = "daemon/health";

/// `daemon/start` — start the daemon if not running. No-op if running.
pub const METHOD_DAEMON_START: &str = "daemon/start";

/// `daemon/stop` — stop the daemon.
pub const METHOD_DAEMON_STOP: &str = "daemon/stop";

/// `daemon/restart` — stop + start the daemon.
pub const METHOD_DAEMON_RESTART: &str = "daemon/restart";

/// `daemon/agents` — list currently active agents (running provider sessions).
pub const METHOD_DAEMON_AGENTS: &str = "daemon/agents";

/// `daemon/events` — open a server-streaming subscription of daemon run events.
pub const METHOD_DAEMON_EVENTS: &str = "daemon/events";

/// `daemon/event` — notification emitted by [`METHOD_DAEMON_EVENTS`] streams.
pub const NOTIFICATION_DAEMON_EVENT: &str = "daemon/event";

/// `daemon/logs` — open a streaming log query (filter + optional follow).
pub const METHOD_DAEMON_LOGS: &str = "daemon/logs";

/// `daemon/log` — notification emitted by [`METHOD_DAEMON_LOGS`] streams.
pub const NOTIFICATION_DAEMON_LOG: &str = "daemon/log";

// =====================================================================
// Workflow operations
// =====================================================================

/// `workflow/list` — list workflow runs, optionally filtered by status.
pub const METHOD_WORKFLOW_LIST: &str = "workflow/list";

/// `workflow/get` — return a single workflow run by id.
pub const METHOD_WORKFLOW_GET: &str = "workflow/get";

/// `workflow/run` — start a new workflow run for a task / subject.
pub const METHOD_WORKFLOW_RUN: &str = "workflow/run";

/// `workflow/execute` — execute an ad-hoc workflow without binding to a task.
pub const METHOD_WORKFLOW_EXECUTE: &str = "workflow/execute";

/// `workflow/pause` — pause an in-flight run at the next safe checkpoint.
pub const METHOD_WORKFLOW_PAUSE: &str = "workflow/pause";

/// `workflow/resume` — resume a paused workflow run.
pub const METHOD_WORKFLOW_RESUME: &str = "workflow/resume";

/// `workflow/cancel` — cancel a workflow run.
pub const METHOD_WORKFLOW_CANCEL: &str = "workflow/cancel";

/// `workflow/events` — open a server-streaming subscription of workflow-scoped
/// events (phase transitions, completions, failures). Added in v0.1.10.
pub const METHOD_WORKFLOW_EVENTS: &str = "workflow/events";

/// `workflow/event` — notification emitted by [`METHOD_WORKFLOW_EVENTS`] streams.
pub const NOTIFICATION_WORKFLOW_EVENT: &str = "workflow/event";

// =====================================================================
// Agent operations
// =====================================================================

/// `agent/run` — start an agent session (one-shot provider invocation).
pub const METHOD_AGENT_RUN: &str = "agent/run";

/// `agent/status` — fetch live status for an agent session.
pub const METHOD_AGENT_STATUS: &str = "agent/status";

/// `agent/cancel` — cancel an in-flight agent session.
pub const METHOD_AGENT_CANCEL: &str = "agent/cancel";

// =====================================================================
// Queue operations
// =====================================================================

/// `queue/list` — list queue entries, optionally filtered by status.
pub const METHOD_QUEUE_LIST: &str = "queue/list";

/// `queue/enqueue` — push a new entry onto the dispatch queue.
pub const METHOD_QUEUE_ENQUEUE: &str = "queue/enqueue";

/// `queue/drop` — remove an entry from the queue (without dispatching).
pub const METHOD_QUEUE_DROP: &str = "queue/drop";

/// `queue/hold` — pause an entry so the dispatcher won't pick it up.
pub const METHOD_QUEUE_HOLD: &str = "queue/hold";

/// `queue/release` — clear a held entry so it becomes dispatchable.
pub const METHOD_QUEUE_RELEASE: &str = "queue/release";

/// `queue/reorder` — change the relative order of queue entries.
pub const METHOD_QUEUE_REORDER: &str = "queue/reorder";

/// `queue/stats` — return per-status counts and recent throughput.
pub const METHOD_QUEUE_STATS: &str = "queue/stats";

/// `fleet/status` — generation-fenced coding scheduler capacity, reservations,
/// lease expiry, and recovery state.
pub const METHOD_FLEET_STATUS: &str = "fleet/status";

// =====================================================================
// Project operations
// =====================================================================

/// `project/init` — initialize Animus in the current working directory.
pub const METHOD_PROJECT_INIT: &str = "project/init";

/// `project/setup` — finalize project setup (MCP wiring, daemon settings).
pub const METHOD_PROJECT_SETUP: &str = "project/setup";

/// `project/status` — return a snapshot of project workflow/task/run state.
pub const METHOD_PROJECT_STATUS: &str = "project/status";
