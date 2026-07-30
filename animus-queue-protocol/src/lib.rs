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

use animus_execution_protocol::{ExecutionFence, RepositoryReservation, SubjectGeneration};
use animus_subject_protocol::{SubjectDispatch, SubjectId};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// `PluginKind` wire value for this kind.
pub const KIND: &str = "queue";

/// Per-crate semver protocol version.
pub const PROTOCOL_VERSION: &str = "0.4.0";

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
/// Generation-fenced enqueue with durable idempotency and repository ownership.
pub const METHOD_QUEUE_ENQUEUE_V2: &str = "queue/v2/enqueue";
/// Generation-fenced atomic lease. Only Pending entries are eligible; expired
/// assigned leases require explicit recovery through [`METHOD_QUEUE_LEASE_RECOVER`].
pub const METHOD_QUEUE_LEASE_V2: &str = "queue/v2/lease";
/// Renew an exact live lease using compare-and-swap ownership.
pub const METHOD_QUEUE_LEASE_RENEW: &str = "queue/v2/lease/renew";
/// Transfer an expired lease to a new daemon owner while preserving workflow
/// and subject generations.
pub const METHOD_QUEUE_LEASE_RECOVER: &str = "queue/v2/lease/recover";
/// Generation-fenced completion.
pub const METHOD_QUEUE_COMPLETION_V2: &str = "queue/v2/completion";
/// Generation-fenced return to Pending.
pub const METHOD_QUEUE_RELEASE_PENDING_V2: &str = "queue/v2/release_pending";

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

// =====================================================================
// Generation-fenced queue v2
// =====================================================================

/// Enqueue request for the generation-fenced scheduler.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct QueueEnqueueV2Request {
    /// Full dispatch envelope to enqueue.
    pub subject_dispatch: SubjectDispatch,
    /// Stable producer idempotency key (for example a GitHub delivery id plus
    /// trigger id). Repeating a key returns the original entry and generation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    /// Exact repository/head-ref reservation for coding work. Non-code queue
    /// entries may omit it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repository: Option<RepositoryReservation>,
    /// Optional RFC 3339 earliest-dispatch time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_at: Option<String>,
    /// Optional grace window after `run_at`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expire_after_secs: Option<u64>,
}

impl QueueEnqueueV2Request {
    /// Validate fields the queue can check before allocating a generation.
    pub fn validate(&self) -> Result<(), String> {
        if self.subject_dispatch.subject.is_none() {
            return Err("generation-fenced enqueue requires a subject".to_string());
        }
        if self.subject_dispatch.workflow_ref.trim().is_empty() {
            return Err("generation-fenced enqueue requires workflow_ref".to_string());
        }
        if self
            .idempotency_key
            .as_ref()
            .is_some_and(|key| key.trim().is_empty())
        {
            return Err("idempotency_key must not be empty when present".to_string());
        }
        if let Some(repository) = &self.repository {
            repository.validate()?;
        }
        Ok(())
    }
}

/// Enqueue response carrying the immutable subject generation allocated to the
/// entry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct QueueEnqueueV2Response {
    /// `true` when this call created the entry; `false` for an idempotent replay.
    pub enqueued: bool,
    /// Stable queue entry id.
    pub entry_id: String,
    /// Immutable subject generation allocated to the entry.
    pub subject: SubjectGeneration,
    /// Optional advisory that does not weaken the generation fence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warning: Option<String>,
}

/// One queue entry paired with its complete execution ownership fence.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FencedQueueEntry {
    /// Existing queue entry payload/display shape.
    pub entry: QueueEntry,
    /// Exact generation and CAS lease ownership.
    pub execution: ExecutionFence,
}

impl FencedQueueEntry {
    /// Validate the fence and its correspondence to the queue entry.
    pub fn validate(&self) -> Result<(), String> {
        self.execution.validate_queue_backed()?;
        let lease = self
            .execution
            .queue_lease
            .as_ref()
            .expect("validated queue lease");
        if lease.entry_id != self.entry.entry_id {
            return Err("execution queue entry id does not match entry".to_string());
        }
        if self.entry.workflow_id.as_deref() != Some(self.execution.workflow_id.as_str()) {
            return Err("execution workflow id does not match entry".to_string());
        }
        if let Some(subject) = &self.execution.subject {
            let expected_suffix = format!(":{}", self.entry.subject_id);
            if subject.qualified_id != self.entry.subject_id
                && !subject.qualified_id.ends_with(&expected_suffix)
            {
                return Err("execution subject id does not match entry".to_string());
            }
        }
        Ok(())
    }
}

/// Request for generation-fenced atomic leasing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct QueueLeaseV2Request {
    /// Maximum number of entries to lease.
    pub max: usize,
    /// Stable daemon/scheduler instance id that will own new leases.
    pub owner_id: String,
    /// Kernel-preallocated workflow ids. Length must equal `max`; a queue entry
    /// that already owns a workflow id keeps it and does not consume a new id.
    pub workflow_ids: Vec<String>,
    /// Active execution fences to exclude from selection and collision checks.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub exclude: Vec<ExecutionFence>,
}

impl QueueLeaseV2Request {
    /// Validate count, owner, workflow ids, and all exclusion fences.
    pub fn validate(&self) -> Result<(), String> {
        if self.max == 0 {
            return Err("queue lease max must be greater than zero".to_string());
        }
        if self.owner_id.trim().is_empty() {
            return Err("queue lease owner_id must not be empty".to_string());
        }
        if self.workflow_ids.len() != self.max {
            return Err(format!(
                "workflow_ids length {} did not match max {}",
                self.workflow_ids.len(),
                self.max
            ));
        }
        let mut unique = std::collections::HashSet::new();
        for workflow_id in &self.workflow_ids {
            if workflow_id.trim().is_empty() || !unique.insert(workflow_id) {
                return Err("workflow_ids must be non-empty and unique".to_string());
            }
        }
        for fence in &self.exclude {
            fence.validate()?;
        }
        Ok(())
    }
}

/// Why a v2 lease candidate was left untouched.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum QueueLeaseBlockReason {
    /// The exact subject generation already has an active execution.
    SubjectGenerationActive,
    /// Another active execution owns the repository/head ref.
    RepositoryRefCollision,
    /// The entry has an expired assignment that must be reconciled/recovered,
    /// never handed out as fresh work.
    ExpiredLeaseRecoveryRequired,
    /// The stored entry lacks the identity needed for fail-closed scheduling.
    MissingExecutionIdentity,
}

/// A candidate intentionally not leased by the v2 scheduler.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct QueueLeaseBlock {
    /// Queue entry that remained untouched.
    pub entry_id: String,
    /// Typed collision/recovery reason.
    pub reason: QueueLeaseBlockReason,
    /// Existing execution that caused the conflict, when applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conflicts_with: Option<ExecutionFence>,
}

/// Response for [`METHOD_QUEUE_LEASE_V2`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct QueueLeaseV2Response {
    /// Newly leased entries with complete ownership fences.
    pub leased: Vec<FencedQueueEntry>,
    /// Candidates intentionally left in place due to a typed fence.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub blocked: Vec<QueueLeaseBlock>,
}

/// Request to renew an exact, still-owned lease.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct QueueLeaseRenewRequest {
    /// Current full execution fence.
    pub execution: ExecutionFence,
    /// Optional requested TTL; the backend may clamp it to policy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_secs: Option<u64>,
}

impl QueueLeaseRenewRequest {
    /// Validate the current queue-backed fence and requested TTL.
    pub fn validate(&self) -> Result<(), String> {
        self.execution.validate_queue_backed()?;
        if self.ttl_secs == Some(0) {
            return Err("lease renewal ttl_secs must be greater than zero".to_string());
        }
        Ok(())
    }
}

/// Request to transfer an expired lease to a new daemon owner.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct QueueLeaseRecoverRequest {
    /// Last durable execution fence. The backend CAS-checks its owner and lease
    /// generation, then preserves workflow/subject generations.
    pub execution: ExecutionFence,
    /// New daemon/scheduler instance id.
    pub new_owner_id: String,
    /// Optional requested TTL; the backend may clamp it to policy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_secs: Option<u64>,
}

impl QueueLeaseRecoverRequest {
    /// Validate an ownership-transfer request without weakening the execution
    /// generation.
    pub fn validate(&self) -> Result<(), String> {
        self.execution.validate_queue_backed()?;
        if self.new_owner_id.trim().is_empty() {
            return Err("lease recovery new_owner_id must not be empty".to_string());
        }
        let current_owner = &self
            .execution
            .queue_lease
            .as_ref()
            .expect("validated queue lease")
            .owner_id;
        if current_owner == &self.new_owner_id {
            return Err("lease recovery must transfer to a different owner_id".to_string());
        }
        if self.ttl_secs == Some(0) {
            return Err("lease recovery ttl_secs must be greater than zero".to_string());
        }
        Ok(())
    }
}

/// Result category for a generation-fenced queue mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum QueueLeaseMutationOutcome {
    /// Mutation succeeded and the response contains the current fence.
    Applied,
    /// The entry is already in the requested terminal/pending state.
    AlreadyApplied,
    /// No entry exists.
    NotFound,
    /// Owner, lease generation, workflow generation, or subject generation did
    /// not match. The caller has no authority to mutate the entry.
    StaleFence,
    /// Recovery was requested before backend-clock expiry.
    LeaseStillLive,
    /// Entry exists but is not assigned.
    NotAssigned,
}

/// Response shared by renew, recover, completion, and release-pending v2.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct QueueLeaseMutationResponse {
    /// Typed mutation outcome.
    pub outcome: QueueLeaseMutationOutcome,
    /// Current fence after success, including a transferred/incremented lease
    /// generation after recovery.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution: Option<ExecutionFence>,
    /// Human-readable diagnostic safe for logs/UI.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// Generation-fenced terminal completion request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct QueueCompletionV2Request {
    /// Exact current execution fence.
    pub execution: ExecutionFence,
    /// Terminal status from [`completion_status`].
    pub status: String,
    /// Workflow ref that ran.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_ref: Option<String>,
}

impl QueueCompletionV2Request {
    /// Validate the exact fence and terminal status vocabulary.
    pub fn validate(&self) -> Result<(), String> {
        self.execution.validate_queue_backed()?;
        if !matches!(
            self.status.as_str(),
            completion_status::COMPLETED | completion_status::FAILED | completion_status::CANCELLED
        ) {
            return Err("queue completion status is not terminal".to_string());
        }
        Ok(())
    }
}

/// Generation-fenced return-to-pending request. The queue preserves the
/// canonical workflow id/generation for the next lease attempt.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct QueueReleasePendingV2Request {
    /// Exact current execution fence.
    pub execution: ExecutionFence,
    /// Auditable reason for returning the entry to Pending.
    pub reason: String,
}

impl QueueReleasePendingV2Request {
    /// Validate the exact fence and auditable reason.
    pub fn validate(&self) -> Result<(), String> {
        self.execution.validate_queue_backed()?;
        if self.reason.trim().is_empty() {
            return Err("queue release-pending reason must not be empty".to_string());
        }
        Ok(())
    }
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
    /// Supports `queue/v2/*`: monotonic subject/workflow/lease generations,
    /// CAS renew/recovery/mutations, durable idempotency, and repository-ref
    /// collision fencing.
    #[serde(default)]
    pub generation_fenced_leases_v1: bool,
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
    /// A generation-fenced mutation was attempted by a stale owner/generation.
    pub const QUEUE_STALE_FENCE: i32 = -32209;
    /// An expired assignment requires explicit reconcile/recover, not fresh lease.
    pub const QUEUE_RECOVERY_REQUIRED: i32 = -32210;
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

    fn sample_execution(entry_id: &str, workflow_id: &str) -> ExecutionFence {
        use animus_execution_protocol::{
            QueueLeaseFence, EXECUTION_FENCE_SCHEMA_ID, EXECUTION_FENCE_VERSION,
        };
        ExecutionFence {
            schema: EXECUTION_FENCE_SCHEMA_ID.to_string(),
            version: EXECUTION_FENCE_VERSION,
            workflow_id: workflow_id.to_string(),
            workflow_generation: 1,
            subject: Some(SubjectGeneration {
                qualified_id: "task:TASK-1175".to_string(),
                generation: 4,
            }),
            queue_lease: Some(QueueLeaseFence {
                entry_id: entry_id.to_string(),
                owner_id: "daemon-a".to_string(),
                generation: 2,
                expires_at: "2030-01-01T00:00:00Z".parse().unwrap(),
            }),
            repository: Some(RepositoryReservation {
                repository: "https://github.com/launchapp-dev/animus-cli.git".to_string(),
                base_ref: "refs/heads/main".to_string(),
                head_ref: "refs/heads/animus/TASK-1175".to_string(),
            }),
        }
    }

    #[test]
    fn v2_lease_requires_unique_preallocated_ids() {
        let request = QueueLeaseV2Request {
            max: 2,
            owner_id: "daemon-a".to_string(),
            workflow_ids: vec!["wf-1".to_string(), "wf-1".to_string()],
            exclude: Vec::new(),
        };
        assert!(request.validate().unwrap_err().contains("unique"));

        let request = QueueLeaseV2Request {
            workflow_ids: vec!["wf-1".to_string(), "wf-2".to_string()],
            ..request
        };
        request.validate().unwrap();
    }

    #[test]
    fn fenced_entry_round_trips_and_matches_queue_identity() {
        let fenced = FencedQueueEntry {
            entry: QueueEntry {
                entry_id: "entry-1".to_string(),
                subject_id: "TASK-1175".to_string(),
                task_id: Some("TASK-1175".to_string()),
                subject_dispatch: sample_dispatch("TASK-1175"),
                status: status::ASSIGNED.to_string(),
                workflow_id: Some("workflow-1".to_string()),
                enqueued_at: "2029-12-31T00:00:00Z".to_string(),
                assigned_at: Some("2029-12-31T01:00:00Z".to_string()),
                held_at: None,
                run_at: None,
                expire_after_secs: None,
            },
            execution: sample_execution("entry-1", "workflow-1"),
        };
        fenced.validate().unwrap();
        let value = serde_json::to_value(&fenced).unwrap();
        assert_eq!(
            serde_json::from_value::<FencedQueueEntry>(value).unwrap(),
            fenced
        );

        let mut stale = fenced;
        stale.execution.workflow_id = "workflow-2".to_string();
        assert!(stale.validate().unwrap_err().contains("workflow id"));
    }

    #[test]
    fn recovery_request_preserves_execution_and_names_new_owner() {
        let request = QueueLeaseRecoverRequest {
            execution: sample_execution("entry-1", "workflow-1"),
            new_owner_id: "daemon-b".to_string(),
            ttl_secs: Some(600),
        };
        request.validate().unwrap();
        let value = serde_json::to_value(&request).unwrap();
        let decoded: QueueLeaseRecoverRequest = serde_json::from_value(value).unwrap();
        assert_eq!(decoded.execution.workflow_generation, 1);
        assert_eq!(decoded.new_owner_id, "daemon-b");
    }
}
