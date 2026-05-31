//! Protocol types for `queue` plugins.
//!
//! Queue plugins own a per-project priority FIFO of `SubjectDispatch`
//! envelopes awaiting scheduling. The v0.5 reference implementation is
//! `launchapp-dev/animus-queue-default` (a lift-and-shift of the in-tree
//! `orchestrator-daemon-runtime/src/queue/` modules).
//!
//! Plugin authors implement the `queue/*` method family. The daemon polls
//! the queue plugin for items via [`METHOD_QUEUE_LEASE`] (the atomic
//! dispatch path) or [`METHOD_QUEUE_LIST`] (for read-only inspection) and
//! decides how many to lease per tick based on its own capacity logic.
//! Capacity policy stays in the kernel — the queue plugin just provides
//! ordered access.
//!
//! Project root is bound at `initialize` time via the
//! `init_extensions.project_binding` extension; it is NOT a per-request
//! field.

#![warn(missing_docs)]

use animus_subject_protocol::SubjectDispatch;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// `PluginKind` wire value for this kind.
pub const KIND: &str = "queue";

/// Per-crate semver protocol version.
pub const PROTOCOL_VERSION: &str = "0.1.0";

/// Add a dispatch to the queue.
pub const METHOD_QUEUE_ENQUEUE: &str = "queue/enqueue";
/// Read-only paginated view of the queue.
pub const METHOD_QUEUE_LIST: &str = "queue/list";
/// Atomic dispatch path: claim up to `max` pending entries and transition
/// them to Assigned in one transaction.
pub const METHOD_QUEUE_LEASE: &str = "queue/lease";
/// Fast aggregate counts.
pub const METHOD_QUEUE_STATS: &str = "queue/stats";
/// Mark an entry held (non-dispatchable until released).
pub const METHOD_QUEUE_HOLD: &str = "queue/hold";
/// Release a held entry back to pending.
pub const METHOD_QUEUE_RELEASE: &str = "queue/release";
/// Drop an entry from the queue.
pub const METHOD_QUEUE_DROP: &str = "queue/drop";
/// Atomically reorder entries by id.
pub const METHOD_QUEUE_REORDER: &str = "queue/reorder";
/// Transition a single entry from Pending to Assigned (used by callers
/// that prefer list+mark over atomic lease for testing/inspection).
pub const METHOD_QUEUE_MARK_ASSIGNED: &str = "queue/mark_assigned";
/// Notify the queue that a workflow has reached a terminal state so the
/// queue can prune the corresponding assigned entry.
pub const METHOD_QUEUE_COMPLETION: &str = "queue/completion";

// =====================================================================
// Status vocabulary
// =====================================================================

/// Allowed status values for queue entries.
pub mod status {
    /// Entry is waiting to be leased.
    pub const PENDING: &str = "pending";
    /// Entry has been leased; a workflow is running against it.
    pub const ASSIGNED: &str = "assigned";
    /// Entry is held (operator intervention; non-dispatchable).
    pub const HELD: &str = "held";
}

/// Allowed status values for [`QueueCompletionRequest::status`].
pub mod completion_status {
    /// Workflow completed successfully.
    pub const COMPLETED: &str = "completed";
    /// Workflow failed.
    pub const FAILED: &str = "failed";
    /// Workflow was cancelled.
    pub const CANCELLED: &str = "cancelled";
}

// =====================================================================
// Types
// =====================================================================

/// Request for [`METHOD_QUEUE_ENQUEUE`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct QueueEnqueueRequest {
    /// Full dispatch envelope to enqueue.
    pub subject_dispatch: SubjectDispatch,
}

/// Response for [`METHOD_QUEUE_ENQUEUE`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct QueueEnqueueResponse {
    /// `true` if a new entry was created. `false` if the dispatch was
    /// rejected as a duplicate of an existing pending/assigned entry
    /// (idempotent enqueue).
    pub enqueued: bool,
    /// Stable entry id assigned by the plugin. Used by all subsequent
    /// mutation calls.
    pub entry_id: String,
    /// Convenience: the subject id from the dispatch envelope.
    pub subject_id: String,
}

/// Request for [`METHOD_QUEUE_LIST`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, Default)]
pub struct QueueListRequest {
    /// Filter by status (values from [`status`]). Empty means all.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub status: Vec<String>,
    /// Pagination limit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    /// Pagination offset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<usize>,
}

/// Response for [`METHOD_QUEUE_LIST`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct QueueListResponse {
    /// Returned entries.
    pub entries: Vec<QueueEntry>,
    /// Total entries matching the filter.
    pub total: usize,
    /// Aggregate stats.
    pub stats: QueueStats,
}

/// A queue entry shape returned by list / lease.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct QueueEntry {
    /// Stable entry id (unique within the project queue). Mutation calls
    /// target this id.
    pub entry_id: String,
    /// Subject id from the dispatch envelope.
    pub subject_id: String,
    /// Task id, if this entry's subject is a built-in task.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    /// Full dispatch envelope — included so the daemon can lease an entry
    /// and start work without a second roundtrip.
    pub subject_dispatch: SubjectDispatch,
    /// Status value from [`status`].
    pub status: String,
    /// Workflow id attached to an Assigned entry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<String>,
    /// RFC 3339 enqueue timestamp.
    pub enqueued_at: String,
    /// RFC 3339 assignment timestamp.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assigned_at: Option<String>,
    /// RFC 3339 hold timestamp.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub held_at: Option<String>,
}

/// Queue aggregate counts.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, Default)]
pub struct QueueStats {
    /// Total entries.
    pub total: usize,
    /// Pending entries.
    pub pending: usize,
    /// Assigned entries.
    pub assigned: usize,
    /// Held entries.
    pub held: usize,
}

/// Request for [`METHOD_QUEUE_LEASE`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct QueueLeaseRequest {
    /// Maximum number of entries to lease in this call.
    pub max: usize,
    /// Optional daemon-provided workflow ids to attach to leased entries.
    /// If set, length MUST be exactly `max` (plugin returns an error
    /// otherwise). If `None`, the plugin generates synthetic UUIDs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_ids: Option<Vec<String>>,
}

/// Response for [`METHOD_QUEUE_LEASE`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct QueueLeaseResponse {
    /// Leased entries (already transitioned to Assigned).
    pub leased: Vec<QueueEntry>,
}

/// Request for [`METHOD_QUEUE_HOLD`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct QueueHoldRequest {
    /// Entry id to hold.
    pub entry_id: String,
    /// Optional audit reason.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// Generic mutation result used by hold / release / drop / mark_assigned /
/// completion.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct QueueMutationResponse {
    /// `true` if the entry state changed. `false` if the entry was already
    /// in the requested state (idempotent no-op) or was not found.
    pub changed: bool,
    /// `true` if the entry was not found (idempotent on missing).
    #[serde(default)]
    pub not_found: bool,
}

/// Request for [`METHOD_QUEUE_RELEASE`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct QueueReleaseRequest {
    /// Entry id to release.
    pub entry_id: String,
}

/// Request for [`METHOD_QUEUE_DROP`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct QueueDropRequest {
    /// Entry id to drop.
    pub entry_id: String,
}

/// Request for [`METHOD_QUEUE_REORDER`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct QueueReorderRequest {
    /// New order (partial — entries not in this list keep their existing
    /// position).
    pub entry_ids: Vec<String>,
}

/// Response for [`METHOD_QUEUE_REORDER`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct QueueReorderResponse {
    /// Count of entries whose position changed.
    pub reordered_count: usize,
}

/// Request for [`METHOD_QUEUE_MARK_ASSIGNED`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct QueueMarkAssignedRequest {
    /// Entry id to transition.
    pub entry_id: String,
    /// Workflow id to attach.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<String>,
}

/// Request for [`METHOD_QUEUE_COMPLETION`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct QueueCompletionRequest {
    /// Entry id whose workflow terminated.
    pub entry_id: String,
    /// Terminal status (from [`completion_status`]).
    pub status: String,
    /// Workflow ref that ran.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_ref: Option<String>,
    /// Workflow id that ran.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<String>,
}

// =====================================================================
// Manifest + capabilities
// =====================================================================

/// Capability flags for queue plugins.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, Default)]
pub struct QueueCapabilities {
    /// `true` if the backend honors `priority` on `SubjectDispatch` (v0.5
    /// reference implementation is strict FIFO within Pending status; later
    /// backends may weight by priority).
    #[serde(default)]
    pub priority_weighted: bool,
    /// Maximum batch size accepted on [`METHOD_QUEUE_LEASE`]. Hosts clamp
    /// requested `max` to this value.
    #[serde(default)]
    pub max_lease_batch: u32,
}

// =====================================================================
// Error codes
// =====================================================================

/// JSON-RPC error codes for the queue protocol. The `-32200..-32299`
/// range is reserved for this kind.
pub mod error_codes {
    /// Entry id not found.
    pub const QUEUE_ENTRY_NOT_FOUND: i32 = -32201;
    /// Entry was not in the expected pre-mutation status (e.g.,
    /// `mark_assigned` on an already-assigned entry).
    pub const QUEUE_ENTRY_ALREADY_ASSIGNED: i32 = -32202;
    /// Entry was not in Pending status (e.g., `release` on a non-held).
    pub const QUEUE_ENTRY_NOT_PENDING: i32 = -32203;
    /// Atomic reorder failed (e.g., supplied id list contained duplicates).
    pub const QUEUE_REORDER_FAILED: i32 = -32204;
    /// Lock acquisition timed out.
    pub const QUEUE_LOCK_ACQUISITION_FAILED: i32 = -32205;
    /// `workflow_ids.len()` did not match `max` on a lease request.
    pub const QUEUE_LEASE_WORKFLOW_ID_COUNT_MISMATCH: i32 = -32206;
    /// Project root mismatch.
    pub const PROJECT_BINDING_MISMATCH: i32 = -32207;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enqueue_response_round_trips() {
        let r = QueueEnqueueResponse {
            enqueued: true,
            entry_id: "ent_1".into(),
            subject_id: "TASK-1".into(),
        };
        let v = serde_json::to_value(&r).unwrap();
        let back: QueueEnqueueResponse = serde_json::from_value(v).unwrap();
        assert_eq!(back, r);
    }

    #[test]
    fn mutation_response_omits_not_found_when_false() {
        let r = QueueMutationResponse {
            changed: true,
            not_found: false,
        };
        let v = serde_json::to_value(&r).unwrap();
        assert!(v.get("not_found").is_some_and(|v| !v.as_bool().unwrap()));
        let back: QueueMutationResponse = serde_json::from_value(v).unwrap();
        assert_eq!(back, r);
    }
}
