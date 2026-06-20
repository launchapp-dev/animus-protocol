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

use animus_subject_protocol::{SubjectDispatch, SubjectId};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// `PluginKind` wire value for this kind.
pub const KIND: &str = "queue";

/// Per-crate semver protocol version.
pub const PROTOCOL_VERSION: &str = "0.3.2";

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
/// Return an Assigned entry back to Pending without canceling it (used
/// when the daemon discovers the subject is already being worked on by
/// another in-flight lease). Distinct from [`METHOD_QUEUE_RELEASE`] which
/// targets a Held entry.
pub const METHOD_QUEUE_RELEASE_PENDING: &str = "queue/release_pending";
/// Report the earliest future `run_at` across pending deferred entries so
/// the daemon can sleep until exactly that instant (precise wake) instead
/// of relying on its heartbeat. No params. Returns
/// [`QueueNextDeadlineResponse`].
pub const METHOD_QUEUE_NEXT_DEADLINE: &str = "queue/next_deadline";

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
    /// Optional RFC 3339 earliest-dispatch time. When set and in the
    /// future, the entry is enqueued as deferred: it stays in
    /// [`status::PENDING`] but is excluded from [`METHOD_QUEUE_LEASE`]
    /// until this instant passes. `None` means dispatch as soon as
    /// capacity allows (today's behavior).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_at: Option<String>,
    /// Optional grace window, in seconds, applied after `run_at`. If a
    /// deferred entry is still pending past `run_at + expire_after_secs`
    /// (e.g. the daemon was down through its window), the plugin drops it
    /// on its next sweep instead of dispatching late. `None` means never
    /// expire — always fire late whenever the daemon next leases. Ignored
    /// when `run_at` is `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expire_after_secs: Option<u64>,
}

/// Response for [`METHOD_QUEUE_ENQUEUE`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct QueueEnqueueResponse {
    /// `true` if a new entry was created. For immediate (non-deferred)
    /// enqueues this stays idempotent: `false` if the dispatch was
    /// rejected as a duplicate of an existing pending/assigned entry.
    /// Deferred enqueues (`run_at` set) are always created — scheduling
    /// the same subject for distinct times is legitimate — so `enqueued`
    /// is `true` and any collision is surfaced via `warning` instead.
    pub enqueued: bool,
    /// Stable entry id assigned by the plugin. Used by all subsequent
    /// mutation calls.
    pub entry_id: String,
    /// Convenience: the subject id from the dispatch envelope.
    pub subject_id: String,
    /// Non-fatal advisory. Set when the enqueue succeeded but the caller
    /// may want to reconsider — most commonly that another pending,
    /// deferred, or assigned entry already exists for this subject. The
    /// duplicate is still enqueued; the caller (agent or operator) decides
    /// whether to drop it. `None` when there is nothing to flag.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warning: Option<String>,
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
    /// RFC 3339 earliest-dispatch time for a deferred entry. While `now`
    /// is before this instant the entry is pending-but-not-leasable.
    /// `None` for ordinary (dispatch-ASAP) entries.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_at: Option<String>,
    /// Grace window in seconds after `run_at` before the entry is expired
    /// and dropped on sweep. `None` means never expire. Ignored when
    /// `run_at` is `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expire_after_secs: Option<u64>,
}

/// Queue aggregate counts.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, Default)]
pub struct QueueStats {
    /// Total entries.
    pub total: usize,
    /// Pending entries (includes deferred entries not yet leasable).
    pub pending: usize,
    /// Assigned entries.
    pub assigned: usize,
    /// Held entries.
    pub held: usize,
    /// Subset of `pending` that is deferred — `run_at` is still in the
    /// future, so these are not yet leasable. Lets callers distinguish
    /// "scheduled for later" from "ready to dispatch" without inspecting
    /// every entry. `0` on backends that predate deferred dispatch.
    #[serde(default)]
    pub deferred: usize,
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
    /// Subjects to skip during lease selection.
    ///
    /// Entries whose `subject_dispatch.subject_key()` matches any id in
    /// this list stay in Pending status and are not returned in the
    /// lease response. No state transition occurs for them. Hosts use
    /// this to tell the queue "this subject already has an in-flight
    /// workflow" so it advances past the head-of-line entry instead of
    /// returning it for the daemon to immediately release back.
    /// Backward-compat: `None` / omitted is identical to today's behavior.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exclude_subjects: Option<Vec<SubjectId>>,
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

/// Request for [`METHOD_QUEUE_RELEASE_PENDING`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct QueueReleasePendingParams {
    /// Entry id to return to Pending.
    pub entry_id: String,
    /// Audit reason describing why the entry is being released back.
    pub reason: String,
}

/// Response for [`METHOD_QUEUE_RELEASE_PENDING`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct QueueReleasePendingResponse {
    /// Entry id whose status was changed.
    pub entry_id: String,
    /// New status — always [`status::PENDING`] on success.
    pub status: String,
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

/// Response for [`METHOD_QUEUE_NEXT_DEADLINE`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, Default)]
pub struct QueueNextDeadlineResponse {
    /// Earliest future `run_at` (RFC 3339) across pending deferred entries,
    /// or `None` when the queue holds no future-dated entries. The daemon
    /// uses this to wake precisely at the next deferred entry's dispatch
    /// time. Expired entries are swept before this is computed, so a value
    /// here is always in the future relative to the plugin's clock.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_run_at: Option<String>,
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
    /// Entry was not in Assigned status (e.g., `release_pending` on a
    /// pending or held entry). The error `data` payload SHOULD include the
    /// entry's actual status.
    pub const QUEUE_ENTRY_NOT_ASSIGNED: i32 = -32208;
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
            warning: None,
        };
        let v = serde_json::to_value(&r).unwrap();
        // `warning: None` is omitted from the wire and legacy responses
        // without the field decode cleanly.
        assert!(v.get("warning").is_none());
        let back: QueueEnqueueResponse = serde_json::from_value(v).unwrap();
        assert_eq!(back, r);
    }

    #[test]
    fn next_deadline_response_round_trips() {
        let some = QueueNextDeadlineResponse {
            next_run_at: Some("2030-01-01T15:00:00Z".into()),
        };
        let v = serde_json::to_value(&some).unwrap();
        assert_eq!(
            v.get("next_run_at").and_then(|t| t.as_str()),
            Some("2030-01-01T15:00:00Z")
        );
        assert_eq!(
            serde_json::from_value::<QueueNextDeadlineResponse>(v).unwrap(),
            some
        );

        // Empty queue: field omitted, decodes back to None.
        let none = QueueNextDeadlineResponse { next_run_at: None };
        let v = serde_json::to_value(&none).unwrap();
        assert!(v.get("next_run_at").is_none());
        assert_eq!(
            serde_json::from_value::<QueueNextDeadlineResponse>(v).unwrap(),
            none
        );
    }

    #[test]
    fn enqueue_response_carries_warning() {
        let r = QueueEnqueueResponse {
            enqueued: true,
            entry_id: "ent_2".into(),
            subject_id: "TASK-2".into(),
            warning: Some("subject TASK-2 already has 1 queued entry".into()),
        };
        let v = serde_json::to_value(&r).unwrap();
        assert_eq!(
            v.get("warning").and_then(|w| w.as_str()),
            Some("subject TASK-2 already has 1 queued entry")
        );
        let back: QueueEnqueueResponse = serde_json::from_value(v).unwrap();
        assert_eq!(back, r);
    }

    fn sample_dispatch(id: &str) -> SubjectDispatch {
        use animus_subject_protocol::SubjectRef;
        let requested_at = "2030-01-01T00:00:00Z"
            .parse::<chrono::DateTime<chrono::Utc>>()
            .unwrap();
        SubjectDispatch::for_subject_with_metadata(
            SubjectRef::task(id),
            "standard",
            "test",
            requested_at,
        )
    }

    #[test]
    fn enqueue_request_round_trips_deferred() {
        let req = QueueEnqueueRequest {
            subject_dispatch: sample_dispatch("TASK-9"),
            run_at: Some("2030-01-01T15:00:00Z".into()),
            expire_after_secs: Some(600),
        };
        let v = serde_json::to_value(&req).unwrap();
        assert_eq!(
            v.get("run_at").and_then(|t| t.as_str()),
            Some("2030-01-01T15:00:00Z")
        );
        assert_eq!(
            v.get("expire_after_secs").and_then(|t| t.as_u64()),
            Some(600)
        );
        let back: QueueEnqueueRequest = serde_json::from_value(v).unwrap();
        assert_eq!(back, req);
    }

    #[test]
    fn enqueue_request_omits_deferral_when_immediate() {
        let req = QueueEnqueueRequest {
            subject_dispatch: sample_dispatch("TASK-10"),
            run_at: None,
            expire_after_secs: None,
        };
        let v = serde_json::to_value(&req).unwrap();
        assert!(v.get("run_at").is_none());
        assert!(v.get("expire_after_secs").is_none());
        // Legacy enqueue payloads (no deferral fields) still decode.
        let legacy = serde_json::json!({ "subject_dispatch": v.get("subject_dispatch").unwrap() });
        let back: QueueEnqueueRequest = serde_json::from_value(legacy).unwrap();
        assert_eq!(back.run_at, None);
        assert_eq!(back.expire_after_secs, None);
    }

    #[test]
    fn entry_round_trips_with_deferral_fields() {
        let entry = QueueEntry {
            entry_id: "ent_3".into(),
            subject_id: "TASK-11".into(),
            task_id: Some("TASK-11".into()),
            subject_dispatch: sample_dispatch("TASK-11"),
            status: status::PENDING.into(),
            workflow_id: None,
            enqueued_at: "2030-01-01T00:00:00Z".into(),
            assigned_at: None,
            held_at: None,
            run_at: Some("2030-01-01T15:00:00Z".into()),
            expire_after_secs: Some(600),
        };
        let v = serde_json::to_value(&entry).unwrap();
        let back: QueueEntry = serde_json::from_value(v).unwrap();
        assert_eq!(back, entry);
        // Stats default keeps `deferred` at zero for legacy payloads.
        let legacy_stats: QueueStats = serde_json::from_value(
            serde_json::json!({ "total": 1, "pending": 1, "assigned": 0, "held": 0 }),
        )
        .unwrap();
        assert_eq!(legacy_stats.deferred, 0);
    }

    #[test]
    fn release_pending_round_trips() {
        let p = QueueReleasePendingParams {
            entry_id: "ent_1".into(),
            reason: "duplicate-in-flight".into(),
        };
        let v = serde_json::to_value(&p).unwrap();
        let back: QueueReleasePendingParams = serde_json::from_value(v).unwrap();
        assert_eq!(back, p);

        let r = QueueReleasePendingResponse {
            entry_id: "ent_1".into(),
            status: status::PENDING.into(),
        };
        let v = serde_json::to_value(&r).unwrap();
        let back: QueueReleasePendingResponse = serde_json::from_value(v).unwrap();
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

    #[test]
    fn lease_request_round_trips_with_exclude_subjects() {
        let req = QueueLeaseRequest {
            max: 3,
            workflow_ids: Some(vec!["wf-1".into(), "wf-2".into(), "wf-3".into()]),
            exclude_subjects: Some(vec![
                SubjectId::new("TASK-1"),
                SubjectId::new("linear:ENG-7"),
            ]),
        };
        let v = serde_json::to_value(&req).unwrap();
        let back: QueueLeaseRequest = serde_json::from_value(v.clone()).unwrap();
        assert_eq!(back, req);
        // Transparent newtype: SubjectId serializes as a bare string.
        let arr = v
            .get("exclude_subjects")
            .and_then(|v| v.as_array())
            .expect("exclude_subjects present");
        assert_eq!(arr[0].as_str(), Some("TASK-1"));
        assert_eq!(arr[1].as_str(), Some("linear:ENG-7"));
    }

    #[test]
    fn lease_request_omits_exclude_subjects_when_none() {
        let req = QueueLeaseRequest {
            max: 1,
            workflow_ids: None,
            exclude_subjects: None,
        };
        let v = serde_json::to_value(&req).unwrap();
        assert!(v.get("exclude_subjects").is_none());
        // Older clients that omit the field MUST still decode as None.
        let legacy = serde_json::json!({ "max": 1 });
        let back: QueueLeaseRequest = serde_json::from_value(legacy).unwrap();
        assert_eq!(back.exclude_subjects, None);
        assert_eq!(back.workflow_ids, None);
        assert_eq!(back.max, 1);
    }
}
