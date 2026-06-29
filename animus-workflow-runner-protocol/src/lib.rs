//! Protocol types for `workflow_runner` plugins.
//!
//! Workflow runners execute Animus workflow YAML by orchestrating phases,
//! evaluating decision contracts, handling rework loops, and applying
//! post-success actions. The v0.5 reference implementation is
//! `launchapp-dev/animus-workflow-runner-default` (a lift-and-shift of the
//! in-tree `workflow-runner-v2` crate).
//!
//! Plugin authors implement two JSON-RPC methods:
//!
//! - [`METHOD_WORKFLOW_EXECUTE`] — drive an entire workflow run from start
//!   to a terminal status (or `manual_pending` pause). Request:
//!   [`WorkflowExecuteRequest`]. Response: [`WorkflowExecuteResult`].
//! - [`METHOD_WORKFLOW_RUN_PHASE`] — execute a single phase (used by the
//!   daemon's per-phase scheduler). Request: [`WorkflowPhaseRunRequest`].
//!   Response: [`WorkflowPhaseRunResult`].
//!
//! Project root is bound at `initialize` time via the
//! `init_extensions.project_binding` extension; it is NOT a per-request
//! field. See `docs/architecture/v0.5-protocol-specs.md` §"Common
//! conventions" for the binding shape.

#![warn(missing_docs)]

pub use animus_actor::{Actor, CLAIM_ADMIN};
use animus_subject_protocol::{SubjectDispatch, SubjectRef};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// `PluginKind` wire value for this kind.
pub const KIND: &str = "workflow_runner";

/// Method name for the full-workflow execution request.
pub const METHOD_WORKFLOW_EXECUTE: &str = "workflow/execute";

/// Method name for the single-phase execution request.
pub const METHOD_WORKFLOW_RUN_PHASE: &str = "workflow/run_phase";

/// Per-crate semver protocol version. Reported via
/// [`animus_plugin_protocol::KindCapability::crate_version`].
pub const PROTOCOL_VERSION: &str = "0.2.0";

// =====================================================================
// Status vocabulary (referenced from string fields below).
// =====================================================================

/// Allowed values for [`WorkflowExecuteResult::workflow_status`].
///
/// Additive vocabulary policy: consumers MUST default-match unknown status
/// strings to [`RUNNING`] semantics so older clients continue to behave
/// safely when newer runners emit values they have not learned yet. New
/// constants since v0.2.0: [`PAUSED`] and [`PENDING`].
pub mod workflow_status {
    /// Workflow completed all phases successfully.
    pub const COMPLETED: &str = "completed";
    /// Workflow is still running (returned only when a single phase was
    /// requested or the workflow paused mid-stream).
    pub const RUNNING: &str = "running";
    /// Workflow is paused for a manual gate; the host MUST NOT advance it.
    /// Added in protocol v0.2.0.
    pub const PAUSED: &str = "paused";
    /// Workflow is queued but has not yet started. Added in protocol
    /// v0.2.0.
    pub const PENDING: &str = "pending";
    /// Workflow failed in a terminal way.
    pub const FAILED: &str = "failed";
    /// Workflow was escalated to a human reviewer.
    pub const ESCALATED: &str = "escalated";
    /// Workflow was cancelled by the host or by an upstream signal.
    pub const CANCELLED: &str = "cancelled";

    /// Parsed workflow status, including an `Unknown` fallback so callers
    /// can default-match forward-compatible wire values without losing the
    /// original string.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum Parsed {
        /// [`COMPLETED`]
        Completed,
        /// [`RUNNING`]
        Running,
        /// [`PAUSED`]
        Paused,
        /// [`PENDING`]
        Pending,
        /// [`FAILED`]
        Failed,
        /// [`ESCALATED`]
        Escalated,
        /// [`CANCELLED`]
        Cancelled,
        /// Wire value not recognized by this version of the protocol crate.
        /// Consumers SHOULD treat this as [`Parsed::Running`] for safety.
        Unknown(String),
    }

    /// Parse a wire status string into [`Parsed`]. Unknown strings round-
    /// trip via [`Parsed::Unknown`] rather than erroring; this is the
    /// additive-vocabulary contract.
    pub fn parse(s: &str) -> Parsed {
        match s {
            COMPLETED => Parsed::Completed,
            RUNNING => Parsed::Running,
            PAUSED => Parsed::Paused,
            PENDING => Parsed::Pending,
            FAILED => Parsed::Failed,
            ESCALATED => Parsed::Escalated,
            CANCELLED => Parsed::Cancelled,
            other => Parsed::Unknown(other.to_string()),
        }
    }
}

/// Allowed values for [`PhaseResultSnapshot::status`] /
/// [`WorkflowPhaseRunResult::phase_status`].
pub mod phase_status {
    /// Phase completed successfully (with a verdict).
    pub const COMPLETED: &str = "completed";
    /// Phase requested rework on a prior phase.
    pub const REWORK: &str = "rework";
    /// Phase chose to skip (e.g., gate not satisfied).
    pub const CLOSED: &str = "closed";
    /// Phase failed terminally.
    pub const FAILED: &str = "failed";
    /// Phase paused awaiting human action.
    pub const MANUAL_PENDING: &str = "manual_pending";
}

// =====================================================================
// workflow/execute
// =====================================================================

/// Parameters for [`METHOD_WORKFLOW_EXECUTE`].
///
/// Either `subject_dispatch` must be set OR (`subject_ref` + one of
/// `task_id` / `requirement_id` / (`title` + `description`)). Generic
/// subject backends MUST use `subject_dispatch`; task and requirement
/// backends MAY use the convenience fields.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct WorkflowExecuteRequest {
    // NOTE: project_root is bound at initialize-time (see
    // `animus_plugin_protocol::InitializeParams::init_extensions`); it is
    // NOT a per-request field.
    /// Existing workflow id to resume, or `None` to start a fresh run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<String>,
    /// Generic dispatch envelope (preferred for non-task/requirement subjects).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject_dispatch: Option<SubjectDispatch>,
    /// Identifies which subject to run when `subject_dispatch` is not set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject_ref: Option<SubjectRef>,
    /// Task id (used only when `subject_dispatch` is None and
    /// `subject_ref.kind == "animus.task"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    /// Requirement id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requirement_id: Option<String>,
    /// For custom ad-hoc subjects without an existing `subject_ref`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// For custom ad-hoc subjects.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Workflow YAML ref (e.g., `"standard"`, `"research-first"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_ref: Option<String>,
    /// Initial input JSON for workflow variables.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input: Option<Value>,
    /// Workflow scalar variables.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub vars: HashMap<String, String>,
    /// Force a specific model.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Force a specific tool (`"claude"`, `"codex"`, `"gemini"`, ...).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool: Option<String>,
    /// Per-phase timeout in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase_timeout_secs: Option<u64>,
    /// Single-phase filter: run only this phase id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase_filter: Option<String>,
    /// Opaque phase routing config (backend-specific).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase_routing: Option<Value>,
    /// Opaque MCP runtime config.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mcp_config: Option<Value>,
    /// Transport-asserted caller identity, relayed verbatim from the daemon so
    /// the runner can pass it to subject/journal/config plugins for scoping.
    /// `None` for system-initiated runs with no actor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<Actor>,
}

/// Result of [`METHOD_WORKFLOW_EXECUTE`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct WorkflowExecuteResult {
    /// Unique workflow id (echoed on resume or freshly allocated on start).
    pub workflow_id: String,
    /// Resolved workflow ref.
    pub workflow_ref: String,
    /// Final status; one of [`workflow_status`] values.
    pub workflow_status: String,
    /// Subject id this run targeted.
    pub subject_id: String,
    /// Working directory used for phases.
    pub execution_cwd: String,
    /// Phase ids that were requested by this run.
    pub phases_requested: Vec<String>,
    /// Number of phases completed.
    pub phases_completed: usize,
    /// Total phases in the workflow.
    pub phases_total: usize,
    /// Total wall-clock duration.
    pub total_duration_secs: u64,
    /// Per-phase results.
    pub phase_results: Vec<PhaseResultSnapshot>,
    /// Post-success action outcome (push, PR creation, merge, etc.).
    pub post_success: Value,
    /// True iff `workflow_status == COMPLETED`.
    pub success: bool,
    /// Phase events emitted during execution (replaces in-process callback).
    #[serde(default)]
    pub phase_events: Vec<PhaseEvent>,
}

/// A single phase's result snapshot returned in [`WorkflowExecuteResult`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct PhaseResultSnapshot {
    /// Phase id (workflow-local).
    pub phase_id: String,
    /// Status; one of [`phase_status`] values.
    pub status: String,
    /// Duration of this phase in seconds.
    pub duration_secs: u64,
    /// Backend-specific outcome payload.
    pub outcome: Value,
    /// Backend-specific metadata payload.
    pub metadata: Value,
    /// Next phase id, if available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_phase_id: Option<String>,
    /// Close reason if verdict was a skip / close.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub close_reason: Option<String>,
}

/// Event emitted by the runner during a workflow. Daemon callers receive
/// the full vector in [`WorkflowExecuteResult::phase_events`]; real-time
/// streaming is deferred to v0.6.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PhaseEvent {
    /// Phase started.
    Started {
        /// Phase id.
        phase_id: String,
        /// Attempt number (0, 1, ...).
        attempt: u32,
        /// RFC 3339 timestamp.
        ts: String,
    },
    /// Phase recorded a decision contract verdict.
    Decision {
        /// Phase id.
        phase_id: String,
        /// `"advance"`, `"rework"`, `"skip"`, `"fail"`.
        verdict: String,
        /// Optional confidence score, 0.0 – 1.0.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        confidence: Option<f32>,
        /// RFC 3339 timestamp.
        ts: String,
    },
    /// Phase finished with a final status.
    Completed {
        /// Phase id.
        phase_id: String,
        /// Status; one of [`phase_status`] values.
        status: String,
        /// RFC 3339 timestamp.
        ts: String,
    },
}

// =====================================================================
// workflow/run_phase
// =====================================================================

/// Parameters for [`METHOD_WORKFLOW_RUN_PHASE`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct WorkflowPhaseRunRequest {
    // project_root bound at initialize-time; NOT a per-request field.
    /// Execution working directory for the phase.
    pub execution_cwd: String,
    /// Workflow id.
    pub workflow_id: String,
    /// Workflow ref.
    pub workflow_ref: String,
    /// Subject id.
    pub subject_id: String,
    /// Subject title for prompts.
    pub subject_title: String,
    /// Subject description for prompts.
    pub subject_description: String,
    /// Phase id to run.
    pub phase_id: String,
    /// Attempt counter (0, 1, 2, ...).
    pub phase_attempt: u32,
    /// Optional timeout override (seconds).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase_timeout_secs: Option<u64>,
    /// Model override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_override: Option<String>,
    /// Tool override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_override: Option<String>,
    /// Task complexity hint: `"minimal" | "low" | "medium" | "high" | "critical"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_complexity: Option<String>,
    /// Rework context from a prior phase's `rework` verdict.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rework_context: Option<String>,
    /// Pipeline variables.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub pipeline_vars: HashMap<String, String>,
    /// Dispatch input JSON (opaque).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dispatch_input: Option<String>,
    /// Schedule input JSON (opaque).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule_input: Option<String>,
    /// Phase routing config (opaque).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase_routing: Option<Value>,
    /// Opaque MCP runtime config — same shape as
    /// [`WorkflowExecuteRequest::mcp_config`]. Lets phase-level retries
    /// pass MCP server config to the agent runner.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mcp_config: Option<Value>,
    /// Transport-asserted caller identity (see [`WorkflowExecuteRequest::actor`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<Actor>,
}

/// Result of [`METHOD_WORKFLOW_RUN_PHASE`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct WorkflowPhaseRunResult {
    /// One of `"completed"`, `"manual_pending"`, `"failed"`.
    pub phase_status: String,
    /// Duration in seconds.
    pub duration_secs: u64,
    /// Backend-specific outcome.
    pub outcome: Value,
    /// Backend-specific metadata.
    pub metadata: Value,
    /// Execution signals emitted during the phase.
    #[serde(default)]
    pub signals: Vec<Value>,
    /// Model used.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Tool used.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool: Option<String>,
}

// =====================================================================
// Manifest + capabilities
// =====================================================================

/// Static manifest a workflow_runner plugin declares at install time.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct WorkflowRunnerManifest {
    /// Plugin name.
    pub name: String,
    /// Plugin semver.
    pub version: String,
    /// Description.
    pub description: String,
    /// Capability flags.
    pub capabilities: WorkflowRunnerCapabilities,
}

/// Backend-specific capability flags serialized into
/// [`animus_plugin_protocol::KindCapability::extra`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, Default)]
pub struct WorkflowRunnerCapabilities {
    /// Plugin parses agent text output to extract `PhaseDecision`.
    #[serde(default)]
    pub phase_decision_parsing: bool,
    /// Plugin propagates `rework_context` to subsequent phases.
    #[serde(default)]
    pub rework_context_support: bool,
    /// Plugin executes post-success actions (push / merge / PR).
    #[serde(default)]
    pub post_success_actions: bool,
    /// Plugin replays persisted phase markers on restart.
    #[serde(default)]
    pub crash_recovery: bool,
    /// Plugin honors `manual_pending` phase statuses.
    #[serde(default)]
    pub manual_pause_support: bool,
}

// =====================================================================
// Error codes
// =====================================================================

/// JSON-RPC error codes specific to the workflow_runner protocol. The
/// `-32100..-32199` range is reserved for this kind.
pub mod error_codes {
    /// Workflow id not found.
    pub const WORKFLOW_NOT_FOUND: i32 = -32101;
    /// Phase id not found within workflow.
    pub const PHASE_NOT_FOUND: i32 = -32102;
    /// Workflow already in a terminal state.
    pub const WORKFLOW_TERMINAL: i32 = -32103;
    /// Project root mismatch (plugin is bound to a different project).
    pub const PROJECT_BINDING_MISMATCH: i32 = -32104;
    /// Manual gate not satisfied; workflow paused.
    pub const MANUAL_GATE_PENDING: i32 = -32105;
    /// Decision contract evaluation failed (parser, validator).
    pub const DECISION_CONTRACT_INVALID: i32 = -32106;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn execute_request_round_trip() {
        let req = WorkflowExecuteRequest {
            workflow_id: None,
            subject_dispatch: None,
            subject_ref: None,
            task_id: Some("TASK-1".into()),
            requirement_id: None,
            title: None,
            description: None,
            workflow_ref: Some("standard".into()),
            input: None,
            vars: HashMap::new(),
            model: None,
            tool: None,
            phase_timeout_secs: None,
            phase_filter: None,
            phase_routing: None,
            mcp_config: None,
            actor: None,
        };
        let v = serde_json::to_value(&req).unwrap();
        assert!(v.get("actor").is_none(), "actor must be omitted when None");
        let back: WorkflowExecuteRequest = serde_json::from_value(v).unwrap();
        assert_eq!(back.task_id.as_deref(), Some("TASK-1"));
        assert_eq!(back.workflow_ref.as_deref(), Some("standard"));
        assert!(back.actor.is_none());
    }

    #[test]
    fn execute_request_round_trips_with_actor() {
        let req = WorkflowExecuteRequest {
            workflow_id: None,
            subject_dispatch: None,
            subject_ref: None,
            task_id: Some("TASK-1".into()),
            requirement_id: None,
            title: None,
            description: None,
            workflow_ref: Some("standard".into()),
            input: None,
            vars: HashMap::new(),
            model: None,
            tool: None,
            phase_timeout_secs: None,
            phase_filter: None,
            phase_routing: None,
            mcp_config: None,
            actor: Some(Actor::new("u-1")),
        };
        let v = serde_json::to_value(&req).unwrap();
        assert!(v.get("actor").is_some());
        let back: WorkflowExecuteRequest = serde_json::from_value(v).unwrap();
        assert_eq!(back.actor, req.actor);
    }

    #[test]
    fn execute_request_deserializes_without_actor() {
        let back: WorkflowExecuteRequest = serde_json::from_value(serde_json::json!({})).unwrap();
        assert!(back.actor.is_none());
    }

    #[test]
    fn workflow_status_parse_recognizes_v02_additions() {
        use workflow_status::{parse, Parsed};
        assert_eq!(parse("paused"), Parsed::Paused);
        assert_eq!(parse("pending"), Parsed::Pending);
        assert_eq!(parse("running"), Parsed::Running);
        assert_eq!(parse("completed"), Parsed::Completed);
        match parse("future-value") {
            Parsed::Unknown(s) => assert_eq!(s, "future-value"),
            other => panic!("expected Unknown, got {other:?}"),
        }
    }

    #[test]
    fn phase_run_request_carries_mcp_config() {
        let req = WorkflowPhaseRunRequest {
            execution_cwd: "/tmp".into(),
            workflow_id: "wf_1".into(),
            workflow_ref: "standard".into(),
            subject_id: "TASK-1".into(),
            subject_title: "t".into(),
            subject_description: "d".into(),
            phase_id: "impl".into(),
            phase_attempt: 0,
            phase_timeout_secs: None,
            model_override: None,
            tool_override: None,
            task_complexity: None,
            rework_context: None,
            pipeline_vars: HashMap::new(),
            dispatch_input: None,
            schedule_input: None,
            phase_routing: None,
            mcp_config: Some(serde_json::json!({"servers": []})),
            actor: None,
        };
        let v = serde_json::to_value(&req).unwrap();
        assert!(v.get("mcp_config").is_some());
        assert!(v.get("actor").is_none(), "actor must be omitted when None");
        let back: WorkflowPhaseRunRequest = serde_json::from_value(v).unwrap();
        assert_eq!(back.mcp_config, req.mcp_config);
        assert!(back.actor.is_none());
    }

    #[test]
    fn phase_run_request_round_trips_with_actor() {
        let req = WorkflowPhaseRunRequest {
            execution_cwd: "/tmp".into(),
            workflow_id: "wf_1".into(),
            workflow_ref: "standard".into(),
            subject_id: "TASK-1".into(),
            subject_title: "t".into(),
            subject_description: "d".into(),
            phase_id: "impl".into(),
            phase_attempt: 0,
            phase_timeout_secs: None,
            model_override: None,
            tool_override: None,
            task_complexity: None,
            rework_context: None,
            pipeline_vars: HashMap::new(),
            dispatch_input: None,
            schedule_input: None,
            phase_routing: None,
            mcp_config: None,
            actor: Some(Actor {
                user_id: "u-1".into(),
                claims: vec![animus_actor::CLAIM_ADMIN.into()],
                tenant_id: Some("t-1".into()),
            }),
        };
        let v = serde_json::to_value(&req).unwrap();
        assert!(v.get("actor").is_some());
        let back: WorkflowPhaseRunRequest = serde_json::from_value(v).unwrap();
        assert_eq!(back.actor, req.actor);
    }

    #[test]
    fn phase_event_round_trips() {
        let e = PhaseEvent::Decision {
            phase_id: "impl".into(),
            verdict: "advance".into(),
            confidence: Some(0.9),
            ts: "2026-05-31T00:00:00Z".into(),
        };
        let v = serde_json::to_value(&e).unwrap();
        assert_eq!(v.get("kind"), Some(&serde_json::json!("decision")));
        let back: PhaseEvent = serde_json::from_value(v).unwrap();
        assert_eq!(back, e);
    }
}
