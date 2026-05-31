//! Protocol types for `durable_store` plugins.
//!
//! Durable stores provide reservation-fenced step persistence so the daemon
//! can recover from crashes without re-executing already-committed side
//! effects. The v0.5 reference implementation is
//! `launchapp-dev/animus-step-durable-dbos` (a TypeScript plugin backed by
//! DBOS Postgres — Option A from the DBOS analysis).
//!
//! Plugin authors implement six JSON-RPC methods. The contract is:
//!
//! 1. The caller (daemon) issues [`METHOD_DURABLE_BEGIN_WORKFLOW_RUN`] to
//!    register a fresh phase execution.
//! 2. Before each side-effecting step, the caller issues
//!    [`METHOD_DURABLE_BEGIN_STEP`]. The plugin checks committed steps and
//!    live reservations:
//!    - [`step_status::ALREADY_COMMITTED`] / [`step_status::PRIOR_ERROR`]
//!      → caller short-circuits the side effect.
//!    - [`step_status::IN_PROGRESS`] → another caller has the reservation;
//!      back off.
//!    - [`step_status::NEW`] → caller proceeds to execute the side effect.
//! 3. After the side effect, the caller issues
//!    [`METHOD_DURABLE_COMMIT_STEP`] with the outcome (success or error).
//! 4. If the caller abandons before commit, it issues
//!    [`METHOD_DURABLE_ABANDON_STEP`] to release the reservation.
//! 5. On daemon restart, [`METHOD_DURABLE_RECOVER_IN_FLIGHT`] reports
//!    workflows that had outstanding reservations.
//!
//! PRIOR_ERROR is terminal for an `idempotency_key`. Callers that want to
//! retry after a permanent error must use a different key.
//!
//! Project root is bound at `initialize` time via the
//! `init_extensions.project_binding` extension.

#![warn(missing_docs)]

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// `PluginKind` wire value for this kind.
pub const KIND: &str = "durable_store";

/// Per-crate semver protocol version.
pub const PROTOCOL_VERSION: &str = "0.1.0";

/// Register a fresh phase execution.
pub const METHOD_DURABLE_BEGIN_WORKFLOW_RUN: &str = "durable/begin_workflow_run";
/// Reserve / look up an idempotency_key before executing a side effect.
pub const METHOD_DURABLE_BEGIN_STEP: &str = "durable/begin_step";
/// Commit a step outcome and release its reservation.
pub const METHOD_DURABLE_COMMIT_STEP: &str = "durable/commit_step";
/// Release a reservation without committing.
pub const METHOD_DURABLE_ABANDON_STEP: &str = "durable/abandon_step";
/// Return workflow runs with outstanding reservations or pending state.
pub const METHOD_DURABLE_RECOVER_IN_FLIGHT: &str = "durable/recover_in_flight";
/// Query a run's full committed-step history.
pub const METHOD_DURABLE_QUERY_RUN: &str = "durable/query_run";

// =====================================================================
// Status vocabulary
// =====================================================================

/// Allowed values for [`BeginStepResponse::status`].
pub mod step_status {
    /// First time the plugin has seen this idempotency_key. Caller proceeds
    /// to execute the side effect, then calls `commit_step`.
    pub const NEW: &str = "new";
    /// Another caller has an outstanding reservation for this
    /// idempotency_key with an unexpired lease. The current caller MUST
    /// NOT execute the side effect; it should wait, retry, or surface to
    /// the daemon for arbitration.
    pub const IN_PROGRESS: &str = "in_progress";
    /// Step was previously committed successfully. `prior_output` is set.
    pub const ALREADY_COMMITTED: &str = "already_committed";
    /// Step was previously committed with an error. `prior_error` is set.
    /// PRIOR_ERROR is TERMINAL for this idempotency_key — the plugin will
    /// never re-execute the side effect for this key. Callers that want to
    /// retry after a permanent error must use a different idempotency_key
    /// (typically by incrementing an attempt counter). `abandon_step`
    /// does NOT clear committed errors.
    pub const PRIOR_ERROR: &str = "prior_error";
}

/// Allowed values for [`CommitStepRequest::outcome`] and
/// [`StepRecord::outcome`].
pub mod commit_outcome {
    /// Step committed successfully.
    pub const SUCCESS: &str = "success";
    /// Step committed with an error (terminal — see PRIOR_ERROR).
    pub const ERROR: &str = "error";
}

/// Allowed values for [`QueryRunResponse::status`].
pub mod run_status {
    /// Run is still in progress.
    pub const PENDING: &str = "pending";
    /// Run completed successfully.
    pub const SUCCESS: &str = "success";
    /// Run terminated with an error.
    pub const ERROR: &str = "error";
    /// Run was cancelled.
    pub const CANCELLED: &str = "cancelled";
}

// =====================================================================
// Types
// =====================================================================

/// Request for [`METHOD_DURABLE_BEGIN_WORKFLOW_RUN`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct BeginWorkflowRunRequest {
    /// Caller-supplied workflow run id.
    pub run_id: String,
    /// Caller-supplied phase id (DBOS workflow IDs are keyed by
    /// `(run_id, phase_id)`; different phases are different workflows).
    pub phase_id: String,
    /// Opaque inputs payload.
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub inputs: Value,
}

/// Response for [`METHOD_DURABLE_BEGIN_WORKFLOW_RUN`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct BeginWorkflowRunResponse {
    /// Plugin-issued, monotonically increasing epoch. The plugin persists
    /// the current epoch counter to its durable store; on restart, it
    /// resumes from the persisted value, not zero. The daemon does not
    /// store epoch values across restarts; instead it queries
    /// [`METHOD_DURABLE_RECOVER_IN_FLIGHT`] with `since_epoch = 0` after
    /// restart to get all in-flight runs.
    pub epoch: u64,
}

/// Request for [`METHOD_DURABLE_BEGIN_STEP`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct BeginStepRequest {
    /// Run id (must match the one used in `begin_workflow_run`).
    pub run_id: String,
    /// Phase id.
    pub phase_id: String,
    /// Stable name for this step within the phase
    /// (e.g., `"spawn_agent"`, `"apply_git_ops"`).
    pub step_name: String,
    /// Caller-supplied idempotency key. Daemon convention:
    /// `format!("{}:{}:{}", run_id, phase_id, step_name)`. The plugin
    /// uses this key to deduplicate replays.
    pub idempotency_key: String,
    /// Backend-specific payload.
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub payload: Value,
    /// Reservation TTL in seconds. If unset, the plugin uses its
    /// configured default (manifest-declared, typically 300). Caller MAY
    /// override per-step for known-long operations.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reservation_ttl_secs: Option<u64>,
}

/// Response for [`METHOD_DURABLE_BEGIN_STEP`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct BeginStepResponse {
    /// Plugin-issued step id (used as the handle for `commit_step` /
    /// `abandon_step`).
    pub step_id: String,
    /// Value from [`step_status`].
    pub status: String,
    /// Present when `status == ALREADY_COMMITTED`. Contains the previously
    /// committed `output` so the caller can short-circuit the side effect.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prior_output: Option<Value>,
    /// Present when `status == PRIOR_ERROR`. Contains the previously
    /// committed error.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prior_error: Option<StepError>,
    /// Present when `status == IN_PROGRESS`. RFC 3339 timestamp of when
    /// the existing reservation expires.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reservation_expires_at: Option<String>,
}

/// Request for [`METHOD_DURABLE_COMMIT_STEP`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct CommitStepRequest {
    /// Step id from [`BeginStepResponse`].
    pub step_id: String,
    /// Required: `"success"` or `"error"`. Use [`commit_outcome`] constants.
    /// This is the authoritative dedupe signal for replay — `output` and
    /// `error` payloads MAY be null (no-payload commits are valid in both
    /// directions), so the outcome cannot be inferred from payload presence.
    pub outcome: String,
    /// Optional success payload.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<Value>,
    /// Optional error payload.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<StepError>,
}

/// Structured error payload returned with PRIOR_ERROR or carried by
/// [`CommitStepRequest`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct StepError {
    /// Backend-specific error code.
    pub code: String,
    /// Human-readable message.
    pub message: String,
    /// Backend-specific details.
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub details: Value,
}

/// Response for [`METHOD_DURABLE_COMMIT_STEP`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct CommitStepResponse {
    /// `true` if the commit was persisted (or already persisted under
    /// idempotent retry).
    pub ack: bool,
}

/// Request for [`METHOD_DURABLE_ABANDON_STEP`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AbandonStepRequest {
    /// Step id whose reservation should be released.
    pub step_id: String,
    /// Optional audit reason.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// Response for [`METHOD_DURABLE_ABANDON_STEP`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct AbandonStepResponse {
    /// `true` if a reservation was found and released.
    pub ack: bool,
}

/// Request for [`METHOD_DURABLE_RECOVER_IN_FLIGHT`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct RecoverInFlightRequest {
    /// Epoch cursor. Daemon passes `0` after restart to get all in-flight
    /// runs; subsequent polls pass the highest epoch seen previously.
    pub since_epoch: u64,
}

/// Response for [`METHOD_DURABLE_RECOVER_IN_FLIGHT`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct RecoverInFlightResponse {
    /// Workflow runs that have outstanding reservations or pending state.
    pub in_flight: Vec<InFlightRun>,
}

/// A run that had outstanding reservations at recovery time.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct InFlightRun {
    /// Run id.
    pub run_id: String,
    /// Phase id.
    pub phase_id: String,
    /// Step name of the last committed step, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_committed_step: Option<String>,
    /// Backend-specific replay state (e.g., DBOS workflow status payload).
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub replay_state: Value,
}

/// Request for [`METHOD_DURABLE_QUERY_RUN`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct QueryRunRequest {
    /// Run id to query.
    pub run_id: String,
    /// Phase id (DBOS workflow id is keyed by `(run_id, phase_id)`).
    pub phase_id: String,
}

/// Response for [`METHOD_DURABLE_QUERY_RUN`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct QueryRunResponse {
    /// Run id (echoed).
    pub run_id: String,
    /// Phase id (echoed).
    pub phase_id: String,
    /// Status value from [`run_status`].
    pub status: String,
    /// Committed step records, in commit order.
    pub steps: Vec<StepRecord>,
}

/// A committed step record returned by [`METHOD_DURABLE_QUERY_RUN`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct StepRecord {
    /// Step id.
    pub step_id: String,
    /// Step name.
    pub step_name: String,
    /// Idempotency key.
    pub idempotency_key: String,
    /// RFC 3339 commit timestamp.
    pub committed_at: String,
    /// `"success"` or `"error"` — the authoritative outcome (see
    /// [`commit_outcome`]). Independent of payload nulls.
    pub outcome: String,
    /// Optional success payload.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<Value>,
    /// Optional error payload.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<StepError>,
}

// =====================================================================
// Manifest + capabilities
// =====================================================================

/// Capability flags for durable_store plugins.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, Default)]
pub struct DurableStoreCapabilities {
    /// Default reservation TTL in seconds. If unset on a `begin_step` call,
    /// the plugin uses this value. Hosts SHOULD respect this when picking
    /// timeouts.
    #[serde(default)]
    pub default_reservation_ttl_secs: u64,
    /// Maximum serialized payload size (bytes) the backend accepts for a
    /// single step's `output` or `error`. Larger payloads should be passed
    /// by reference via Animus's artifact store.
    #[serde(default)]
    pub max_payload_bytes: u64,
    /// `true` if the backend supports the `recover_in_flight` cursor /
    /// epoch model.
    #[serde(default)]
    pub supports_recovery: bool,
}

// =====================================================================
// Error codes
// =====================================================================

/// JSON-RPC error codes for the durable_store protocol. The
/// `-32300..-32399` range is reserved for this kind.
pub mod error_codes {
    /// Run id not found (begin_workflow_run was never called).
    pub const RUN_NOT_FOUND: i32 = -32301;
    /// Step id not found (commit_step / abandon_step on unknown reservation).
    pub const STEP_NOT_FOUND: i32 = -32302;
    /// Reservation expired before commit.
    pub const RESERVATION_EXPIRED: i32 = -32303;
    /// Backend (Postgres / DBOS) unavailable.
    pub const BACKEND_UNAVAILABLE: i32 = -32304;
    /// Project root mismatch.
    pub const PROJECT_BINDING_MISMATCH: i32 = -32305;
    /// Replay determinism violated (step issued out of order vs. recorded).
    pub const REPLAY_NONDETERMINISM: i32 = -32306;
    /// `CommitStepRequest.outcome` was not a valid `commit_outcome` value.
    pub const INVALID_COMMIT_OUTCOME: i32 = -32307;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn begin_step_response_round_trips_all_statuses() {
        for status in [
            step_status::NEW,
            step_status::IN_PROGRESS,
            step_status::ALREADY_COMMITTED,
            step_status::PRIOR_ERROR,
        ] {
            let r = BeginStepResponse {
                step_id: "step_1".into(),
                status: status.into(),
                prior_output: None,
                prior_error: None,
                reservation_expires_at: None,
            };
            let v = serde_json::to_value(&r).unwrap();
            let back: BeginStepResponse = serde_json::from_value(v).unwrap();
            assert_eq!(back.status, status);
        }
    }

    #[test]
    fn commit_step_request_requires_outcome() {
        let r = CommitStepRequest {
            step_id: "step_1".into(),
            outcome: commit_outcome::SUCCESS.into(),
            output: Some(serde_json::json!({"k": "v"})),
            error: None,
        };
        let v = serde_json::to_value(&r).unwrap();
        assert_eq!(v.get("outcome"), Some(&serde_json::json!("success")));
        let back: CommitStepRequest = serde_json::from_value(v).unwrap();
        assert_eq!(back.outcome, commit_outcome::SUCCESS);
    }
}
