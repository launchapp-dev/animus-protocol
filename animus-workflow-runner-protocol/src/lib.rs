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
use animus_execution_protocol::ExecutionFence;
use animus_subject_protocol::{SubjectDispatch, SubjectRef};
use chrono::{DateTime, Utc};
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
pub const PROTOCOL_VERSION: &str = "0.4.0";

/// Stable schema id for a proof-carrying publication receipt.
pub const PUBLICATION_RECEIPT_SCHEMA_ID: &str = "animus.publication-receipt.v1";
/// Current publication receipt version.
pub const PUBLICATION_RECEIPT_VERSION: u32 = 1;

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

/// Qualified subject identity and generation fenced into a publication.
///
/// `qualified_id` uses the canonical `<kind>:<native-id>` shape (for example
/// `task:TASK-1173`). `generation` is the subject generation leased for this
/// execution; a receipt for an older generation must never satisfy a newer run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PublicationSubjectGeneration {
    /// Canonical qualified subject id.
    pub qualified_id: String,
    /// Subject generation leased by this workflow execution.
    pub generation: u64,
}

/// Pull-request proof attached to a publication receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PublicationPullRequest {
    /// Repository-local pull request number.
    pub number: u64,
    /// Canonical pull request URL.
    pub url: String,
    /// Head SHA observed from the pull request provider after create/update.
    pub head_sha: String,
}

/// Component that issued a publication receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum PublicationReceiptIssuer {
    /// The workflow runner owns publication.
    Runner {
        /// Concrete runner/plugin name.
        component: String,
        /// Concrete runner/plugin version.
        version: String,
    },
    /// A named workflow phase owns publication.
    Phase {
        /// Phase id declared by the workflow publication contract.
        phase_id: String,
        /// Component or command implementation that emitted the receipt.
        component: String,
        /// Concrete component/command version.
        version: String,
    },
}

/// Durable proof that exactly one owner published a workflow result.
///
/// Compatibility contract: v0.3+ consumers accept schema
/// `animus.publication-receipt.v1`, version `1`. Unknown schema ids or versions
/// fail closed. Older v0.2 consumers ignore the additive optional receipt field
/// on runner results, but MUST NOT be used for workflows whose publication is
/// required because they cannot verify this proof.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PublicationReceipt {
    /// Stable schema id; must equal [`PUBLICATION_RECEIPT_SCHEMA_ID`].
    pub schema: String,
    /// Receipt version; must equal [`PUBLICATION_RECEIPT_VERSION`].
    pub version: u32,
    /// Workflow execution id.
    pub workflow_id: String,
    /// Workflow generation fenced into the publication lease.
    pub workflow_generation: u64,
    /// Qualified subject/task and its leased generation.
    pub subject: PublicationSubjectGeneration,
    /// Commit SHA produced by the workflow.
    pub commit_sha: String,
    /// Git tree SHA for the published commit.
    pub tree_sha: String,
    /// Canonical remote URL used for publication.
    pub remote: String,
    /// Fully qualified remote ref, for example `refs/heads/animus/TASK-1173`.
    pub remote_ref: String,
    /// SHA independently observed at `remote_ref` after publication.
    pub observed_remote_sha: String,
    /// Durable ref from which unpublished/recovery work can be restored.
    pub recovery_ref: String,
    /// Pull-request proof when the publication policy requires a PR.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pull_request: Option<PublicationPullRequest>,
    /// Exact owner/component that issued this receipt.
    pub issuer: PublicationReceiptIssuer,
    /// UTC RFC 3339 issue timestamp.
    pub issued_at: DateTime<Utc>,
}

impl PublicationReceipt {
    /// Validate the version fence and the remote/PR proof relationships.
    pub fn validate(&self) -> Result<(), String> {
        if self.schema != PUBLICATION_RECEIPT_SCHEMA_ID {
            return Err(format!(
                "unsupported publication receipt schema '{}'; expected '{}'",
                self.schema, PUBLICATION_RECEIPT_SCHEMA_ID
            ));
        }
        if self.version != PUBLICATION_RECEIPT_VERSION {
            return Err(format!(
                "unsupported publication receipt version {}; expected {}",
                self.version, PUBLICATION_RECEIPT_VERSION
            ));
        }
        for (label, value) in [
            ("workflow_id", self.workflow_id.as_str()),
            ("subject.qualified_id", self.subject.qualified_id.as_str()),
            ("remote", self.remote.as_str()),
            ("remote_ref", self.remote_ref.as_str()),
            ("recovery_ref", self.recovery_ref.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(format!("publication receipt {label} must not be empty"));
            }
        }
        if self.workflow_generation == 0 {
            return Err(
                "publication receipt workflow_generation must be greater than zero".to_string(),
            );
        }
        if self.subject.generation == 0 {
            return Err(
                "publication receipt subject.generation must be greater than zero".to_string(),
            );
        }
        let Some((kind, native_id)) = self.subject.qualified_id.split_once(':') else {
            return Err(
                "publication receipt subject.qualified_id must use <kind>:<native-id> form"
                    .to_string(),
            );
        };
        if kind.trim().is_empty() || native_id.trim().is_empty() {
            return Err(
                "publication receipt subject.qualified_id must use <kind>:<native-id> form"
                    .to_string(),
            );
        }
        for (label, sha) in [
            ("commit_sha", self.commit_sha.as_str()),
            ("tree_sha", self.tree_sha.as_str()),
            ("observed_remote_sha", self.observed_remote_sha.as_str()),
        ] {
            if !is_git_object_id(sha) {
                return Err(format!(
                    "publication receipt {label} must be a 40- or 64-character hexadecimal git object id"
                ));
            }
        }
        if self.observed_remote_sha != self.commit_sha {
            return Err(
                "publication receipt observed_remote_sha must equal commit_sha".to_string(),
            );
        }
        if let Some(pr) = &self.pull_request {
            if pr.number == 0 {
                return Err("publication receipt pull_request.number must be positive".to_string());
            }
            if pr.url.trim().is_empty() {
                return Err("publication receipt pull_request.url must not be empty".to_string());
            }
            if !is_git_object_id(&pr.head_sha) {
                return Err("publication receipt pull_request.head_sha must be a 40- or 64-character hexadecimal git object id".to_string());
            }
            if pr.head_sha != self.commit_sha {
                return Err(
                    "publication receipt pull_request.head_sha must equal commit_sha".to_string(),
                );
            }
        }
        match &self.issuer {
            PublicationReceiptIssuer::Runner { component, version }
            | PublicationReceiptIssuer::Phase {
                component, version, ..
            } => {
                if component.trim().is_empty() || version.trim().is_empty() {
                    return Err(
                        "publication receipt issuer component and version must not be empty"
                            .to_string(),
                    );
                }
            }
        }
        if let PublicationReceiptIssuer::Phase { phase_id, .. } = &self.issuer {
            if phase_id.trim().is_empty() {
                return Err("publication receipt issuer phase_id must not be empty".to_string());
            }
        }
        Ok(())
    }

    /// Validate this proof against the exact execution generation authorized by
    /// the queue/daemon. A structurally valid receipt for a stale generation is
    /// still rejected.
    pub fn validate_against_execution(&self, execution: &ExecutionFence) -> Result<(), String> {
        self.validate()?;
        execution.validate()?;
        if self.workflow_id != execution.workflow_id {
            return Err(
                "publication receipt workflow_id does not match execution fence".to_string(),
            );
        }
        if self.workflow_generation != execution.workflow_generation {
            return Err(
                "publication receipt workflow_generation does not match execution fence"
                    .to_string(),
            );
        }
        let Some(subject) = execution.subject.as_ref() else {
            return Err(
                "publication receipt requires a subject-bearing execution fence".to_string(),
            );
        };
        if self.subject.qualified_id != subject.qualified_id
            || self.subject.generation != subject.generation
        {
            return Err(
                "publication receipt subject generation does not match execution fence".to_string(),
            );
        }
        if let Some(repository) = execution.repository.as_ref() {
            if self.remote_ref != repository.head_ref {
                return Err(
                    "publication receipt remote_ref does not match repository reservation"
                        .to_string(),
                );
            }
        }
        Ok(())
    }
}

fn is_git_object_id(value: &str) -> bool {
    matches!(value.len(), 40 | 64) && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

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
    /// Exact generation/lease/reservation authority for this execution. Hosts
    /// requiring resilient coding MUST provide it and runners MUST fail closed
    /// when it is absent or mismatched. Optional only for legacy/non-fenced
    /// workflows during migration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_fence: Option<ExecutionFence>,
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

impl WorkflowExecuteRequest {
    /// Validate the optional/required scheduler authority before a runner
    /// creates or resumes journal state.
    pub fn validate_execution_fence(&self, required: bool) -> Result<(), String> {
        let Some(execution) = self.execution_fence.as_ref() else {
            return if required {
                Err("workflow execution requires execution_fence".to_string())
            } else {
                Ok(())
            };
        };
        execution.validate()?;
        if self.workflow_id.as_deref() != Some(execution.workflow_id.as_str()) {
            return Err("workflow request workflow_id does not match execution fence".to_string());
        }
        Ok(())
    }
}

/// Result of [`METHOD_WORKFLOW_EXECUTE`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct WorkflowExecuteResult {
    /// Unique workflow id (echoed on resume or freshly allocated on start).
    pub workflow_id: String,
    /// Echo of the validated execution fence. A parent uses this to ensure the
    /// terminal result and publication receipt belong to the leased generation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_fence: Option<ExecutionFence>,
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
    /// Legacy opaque post-success action outcome. New publication-aware
    /// consumers use [`Self::publication_receipt`] instead.
    pub post_success: Value,
    /// Typed, versioned publication proof. Required by the host before it marks
    /// a workflow with `publication.required=true` completed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publication_receipt: Option<PublicationReceipt>,
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
    /// Receipt emitted by this phase when it is the declared publication owner.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publication_receipt: Option<PublicationReceipt>,
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
    /// Exact execution authority inherited from the full workflow request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_fence: Option<ExecutionFence>,
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

impl WorkflowPhaseRunRequest {
    /// Validate that a phase executes under the exact parent workflow fence.
    pub fn validate_execution_fence(&self, required: bool) -> Result<(), String> {
        let Some(execution) = self.execution_fence.as_ref() else {
            return if required {
                Err("workflow phase requires execution_fence".to_string())
            } else {
                Ok(())
            };
        };
        execution.validate()?;
        if self.workflow_id != execution.workflow_id {
            return Err("workflow phase workflow_id does not match execution fence".to_string());
        }
        Ok(())
    }
}

/// Result of [`METHOD_WORKFLOW_RUN_PHASE`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct WorkflowPhaseRunResult {
    /// One of `"completed"`, `"manual_pending"`, `"failed"`.
    pub phase_status: String,
    /// Echo of the validated execution fence for parent-side generation checks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_fence: Option<ExecutionFence>,
    /// Duration in seconds.
    pub duration_secs: u64,
    /// Backend-specific outcome.
    pub outcome: Value,
    /// Backend-specific metadata.
    pub metadata: Value,
    /// Receipt emitted by this phase when it is the declared publication owner.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publication_receipt: Option<PublicationReceipt>,
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
    /// Plugin validates and returns `animus.publication-receipt.v1` and obeys
    /// explicit single-owner publication config.
    #[serde(default)]
    pub publication_receipt_v1: bool,
    /// Validates and echoes `animus.execution-fence.v1`, and validates any
    /// publication receipt against its workflow/subject generation.
    #[serde(default)]
    pub execution_fence_v1: bool,
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
            execution_fence: None,
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
            execution_fence: None,
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
            execution_fence: None,
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
            execution_fence: None,
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

    fn valid_publication_receipt() -> PublicationReceipt {
        let sha = "0123456789abcdef0123456789abcdef01234567".to_string();
        PublicationReceipt {
            schema: PUBLICATION_RECEIPT_SCHEMA_ID.to_string(),
            version: PUBLICATION_RECEIPT_VERSION,
            workflow_id: "wf-123".to_string(),
            workflow_generation: 7,
            subject: PublicationSubjectGeneration {
                qualified_id: "task:TASK-1173".to_string(),
                generation: 4,
            },
            commit_sha: sha.clone(),
            tree_sha: "89abcdef0123456789abcdef0123456789abcdef".to_string(),
            remote: "https://github.com/launchapp-dev/animus-protocol.git".to_string(),
            remote_ref: "refs/heads/animus/TASK-1173".to_string(),
            observed_remote_sha: sha.clone(),
            recovery_ref: "refs/animus/recovery/wf-123".to_string(),
            pull_request: Some(PublicationPullRequest {
                number: 42,
                url: "https://github.com/launchapp-dev/animus-protocol/pull/42".to_string(),
                head_sha: sha,
            }),
            issuer: PublicationReceiptIssuer::Phase {
                phase_id: "publish".to_string(),
                component: "portal-code-open-pr".to_string(),
                version: "1.0.0".to_string(),
            },
            issued_at: "2026-07-30T01:00:00Z".parse().unwrap(),
        }
    }

    #[test]
    fn publication_receipt_round_trips_without_loss_and_validates() {
        let receipt = valid_publication_receipt();
        receipt.validate().expect("valid receipt");
        let json = serde_json::to_value(&receipt).expect("serialize");
        assert_eq!(
            json.get("schema"),
            Some(&serde_json::json!(PUBLICATION_RECEIPT_SCHEMA_ID))
        );
        let decoded: PublicationReceipt = serde_json::from_value(json).expect("deserialize");
        assert_eq!(decoded, receipt);
    }

    #[test]
    fn v02_execute_result_without_receipt_remains_wire_compatible() {
        let legacy = serde_json::json!({
            "workflow_id": "wf-legacy",
            "workflow_ref": "standard",
            "workflow_status": "completed",
            "subject_id": "TASK-1",
            "execution_cwd": "/tmp/work",
            "phases_requested": ["implement"],
            "phases_completed": 1,
            "phases_total": 1,
            "total_duration_secs": 2,
            "phase_results": [],
            "post_success": null,
            "success": true,
            "phase_events": []
        });
        let result: WorkflowExecuteResult =
            serde_json::from_value(legacy).expect("v0.2 result still deserializes");
        assert!(result.publication_receipt.is_none());
    }

    #[test]
    fn publication_receipt_rejects_unverified_remote_sha() {
        let mut receipt = valid_publication_receipt();
        receipt.observed_remote_sha = "abcdef0123456789abcdef0123456789abcdef01".to_string();
        let error = receipt.validate().expect_err("mismatched remote proof");
        assert!(error.contains("observed_remote_sha must equal commit_sha"));
    }

    #[test]
    fn publication_receipt_rejects_unknown_version() {
        let mut receipt = valid_publication_receipt();
        receipt.version = PUBLICATION_RECEIPT_VERSION + 1;
        let error = receipt.validate().expect_err("unknown version must fail");
        assert!(error.contains("unsupported publication receipt version"));
    }

    #[test]
    fn publication_receipt_denies_unknown_fields() {
        let receipt = valid_publication_receipt();
        let mut json = serde_json::to_value(receipt).unwrap();
        json.as_object_mut()
            .unwrap()
            .insert("untrusted".to_string(), serde_json::json!(true));
        let error = serde_json::from_value::<PublicationReceipt>(json)
            .expect_err("unknown receipt fields must fail");
        assert!(error.to_string().contains("unknown field"));
    }

    #[test]
    fn publication_receipt_is_fenced_to_exact_execution_generation() {
        use animus_execution_protocol::{
            QueueLeaseFence, RepositoryReservation, SubjectGeneration, EXECUTION_FENCE_SCHEMA_ID,
            EXECUTION_FENCE_VERSION,
        };
        let execution = ExecutionFence {
            schema: EXECUTION_FENCE_SCHEMA_ID.to_string(),
            version: EXECUTION_FENCE_VERSION,
            workflow_id: "wf-123".to_string(),
            workflow_generation: 7,
            subject: Some(SubjectGeneration {
                qualified_id: "task:TASK-1173".to_string(),
                generation: 4,
            }),
            queue_lease: Some(QueueLeaseFence {
                entry_id: "entry-1".to_string(),
                owner_id: "daemon-a".to_string(),
                generation: 2,
                expires_at: "2026-07-30T02:00:00Z".parse().unwrap(),
            }),
            repository: Some(RepositoryReservation {
                repository: "https://github.com/launchapp-dev/animus-protocol.git".to_string(),
                base_ref: "refs/heads/main".to_string(),
                head_ref: "refs/heads/animus/TASK-1173".to_string(),
            }),
        };
        let receipt = valid_publication_receipt();
        receipt.validate_against_execution(&execution).unwrap();

        let mut stale = execution;
        stale.subject.as_mut().unwrap().generation += 1;
        assert!(receipt
            .validate_against_execution(&stale)
            .unwrap_err()
            .contains("subject generation"));
    }
}
