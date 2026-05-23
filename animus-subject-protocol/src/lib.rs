//! `SubjectBackend` trait and normalized `Subject` schema.
//!
//! Animus dispatches `SubjectDispatch` envelopes off a queue and into
//! `workflow-runner` subprocesses. The set of subjects available for dispatch
//! comes from one or more *subject backends* — pluggable sources of work
//! items. Native `animus task` is a backend; so are external systems of
//! record like Linear, Jira, GitHub Issues, Notion, Asana, Zendesk, and
//! anything else with an API.
//!
//! This crate defines:
//!
//! - The normalized cross-backend [`Subject`] schema and its supporting types
//!   ([`SubjectId`], [`SubjectStatus`], [`SubjectFilter`], [`SubjectPatch`],
//!   [`SubjectList`], [`SubjectSchema`], [`SubjectAttachment`],
//!   [`StatusDispatchHint`]).
//! - The Rust-side [`SubjectBackend`] trait that plugin authors implement.
//! - The JSON-RPC method-name constants used on the wire (e.g.
//!   [`METHOD_SUBJECT_LIST`]).
//! - [`BackendError`] mapping backend failures to JSON-RPC error responses.
//! - The [`SubjectChangedEvent`] notification shape used by `subject/watch`.
//!
//! # Flexible status (v0.1.1+)
//!
//! Beyond the normalized [`SubjectStatus`] bucket, subjects carry an optional
//! [`Subject::native_status`] (the backend's raw status string, e.g.
//! `"In Review"` or `"Shipped"`) and a free-form [`Subject::status_metadata`]
//! payload. The schema declares [`SubjectSchema::status_dispatch_hints`] —
//! per-native-status mappings that name a `dispatch_label` workflow YAML can
//! gate on without coupling to any one backend's vocabulary.
//!
//! Plugin authors typically depend on this crate alongside
//! [`animus-plugin-runtime`], implement [`SubjectBackend`] for their type, and
//! call `animus_plugin_runtime::subject_backend_main(my_backend).await` from
//! `main`.

#![warn(missing_docs)]

use std::collections::BTreeMap;
use std::pin::Pin;

use animus_plugin_protocol::{error_codes, HealthCheckResult, RpcError};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures_core::Stream;
use serde::{Deserialize, Serialize};
use serde_json::Value;

// =====================================================================
// Method-name constants (the JSON-RPC wire methods)
// =====================================================================

/// `subject/list` — return ready/filtered subjects for dispatch.
pub const METHOD_SUBJECT_LIST: &str = "subject/list";

/// `subject/get` — fetch a single subject by id.
pub const METHOD_SUBJECT_GET: &str = "subject/get";

/// `subject/update` — apply a [`SubjectPatch`] to a subject.
pub const METHOD_SUBJECT_UPDATE: &str = "subject/update";

/// `subject/delete` — permanently remove a subject by id. Optional; backends
/// that do not support deletion MUST respond with
/// [`error_codes::METHOD_NOT_SUPPORTED`]. Added in v0.1.8.
pub const METHOD_SUBJECT_DELETE: &str = "subject/delete";

/// `subject/watch` — start a server-streaming subscription. Optional;
/// polling-only backends respond with [`error_codes::METHOD_NOT_SUPPORTED`].
pub const METHOD_SUBJECT_WATCH: &str = "subject/watch";

/// `subject/schema` — capability declaration; returns [`SubjectSchema`].
pub const METHOD_SUBJECT_SCHEMA: &str = "subject/schema";

/// `subject/changed` — notification method emitted by `subject/watch`
/// streams.
pub const NOTIFICATION_SUBJECT_CHANGED: &str = "subject/changed";

// =====================================================================
// Subject identity
// =====================================================================

/// Backend-qualified identifier for a subject.
///
/// Convention is `"<backend>:<native_id>"`, e.g. `"linear:ENG-123"`,
/// `"jira:PROJ-456"`, `"github:owner/repo#789"`, `"native:TASK-001"`. The
/// daemon treats the value as opaque; only the originating backend
/// interprets the native portion.
///
/// The `backend:` prefix is reserved. Plugin authors should always emit
/// prefixed ids so cross-backend collisions are impossible.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SubjectId(pub String);

impl SubjectId {
    /// Construct a new id from any string-like value.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Borrow the inner string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for SubjectId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for SubjectId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for SubjectId {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

// =====================================================================
// Normalized status
// =====================================================================

/// Normalized cross-backend subject status.
///
/// Backend-native states (`"Backlog"`, `"In Review"`, `"Won't Fix"`, ...) map
/// into one of these five via the `status_map` declared per-subject in
/// workflow YAML. The mapping lives in configuration, not code, so adding a
/// new backend never requires extending this enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SubjectStatus {
    /// Eligible for dispatch.
    Ready,
    /// Currently being worked by a workflow run (or by a human, in the
    /// upstream system).
    InProgress,
    /// Cannot proceed; awaiting unblock.
    Blocked,
    /// Successfully completed.
    Done,
    /// Abandoned without completion.
    Cancelled,
}

// =====================================================================
// The Subject schema
// =====================================================================

/// A normalized cross-backend representation of a unit of dispatchable work.
///
/// Subjects flow from backends into the daemon's dispatch queue and back as
/// updates after a workflow run completes. Backend-specific fields the
/// daemon doesn't interpret live in [`Subject::custom`] and are addressable
/// from workflow YAML via templating (e.g.
/// `{{subject.custom.story_points}}`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Subject {
    /// Backend-qualified identifier. See [`SubjectId`].
    pub id: SubjectId,

    /// Subject kind. Backend-defined. Examples: `"task"`, `"issue"`,
    /// `"epic"`, `"ticket"`, `"document"`, `"lead"`, `"contract"`,
    /// `"incident"`.
    pub kind: String,

    /// Short human-readable title.
    pub title: String,

    /// Long-form description (markdown encouraged).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// Normalized status. See [`SubjectStatus`].
    pub status: SubjectStatus,

    /// Optional priority on a 0..=4 scale: 0 = none, 1 = low, 2 = medium,
    /// 3 = high, 4 = critical.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<u8>,

    /// Free-form assignee identifier. Format is backend-specific; commonly
    /// a username, email, or `"agent:<name>"` for an Animus agent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assignee: Option<String>,

    /// Labels/tags. Backend-defined string set.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub labels: Vec<String>,

    /// Parent subject, if any (e.g. an epic for an issue).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<SubjectId>,

    /// Child subjects, if any.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub children: Vec<SubjectId>,

    /// Permalink to the subject in its native system, if one exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,

    /// When the subject was first created in its native system.
    pub created_at: DateTime<Utc>,

    /// When the subject was last updated in its native system.
    pub updated_at: DateTime<Utc>,

    /// Backend-specific fields the daemon does not interpret. Workflows
    /// can read these via templating.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub custom: BTreeMap<String, Value>,

    /// Backend-raw status string, e.g. `"In Review"`, `"Spec"`, `"Shipped"`,
    /// `"Cycle 12 / blocked on infra"`. The normalized [`Subject::status`]
    /// is the coarse-grained bucket; this preserves the upstream signal so
    /// workflows can dispatch on rich vocabularies. Optional —
    /// backends that have nothing more specific than the normalized status
    /// should leave this `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_status: Option<String>,

    /// Free-form backend status payload — state id, color, type, ordering
    /// position, transition rules, etc. Workflows may read this via
    /// templating (e.g. `{{subject.status_metadata.color}}`). Defaults to
    /// JSON `null`, which is skipped on serialization so v0.1.0 consumers
    /// see no extra field.
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub status_metadata: Value,

    /// First-class attachments the subject carries — documents, URLs,
    /// uploaded files, comment threads. Backends like Linear surface
    /// attached spec docs here; workflow YAML can gate on attachment
    /// presence (`requires_document_attachment: true`) to drive
    /// document-aware phases. Defaults to empty, which is skipped on
    /// serialization for v0.1.0 wire compatibility.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<SubjectAttachment>,
}

/// An attachment on a [`Subject`] — document, URL, file, comment thread, or
/// any other backend-defined artifact carried alongside the work item.
///
/// The `kind` field is opaque to the daemon; workflow YAML matches on it.
/// Convention: `"document"`, `"url"`, `"file"`, `"comment-thread"`. Backends
/// MAY introduce custom kinds.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubjectAttachment {
    /// Stable identifier for this attachment within the backend.
    pub id: String,

    /// Attachment category. Conventional values: `"document"`, `"url"`,
    /// `"file"`, `"comment-thread"`. Backends MAY emit custom values.
    pub kind: String,

    /// Location-of-truth URI. Examples: `linear://issue/ENG-123/doc/spec`,
    /// `https://docs.example.com/spec.md`, `file:///tmp/handoff.md`.
    pub uri: String,

    /// Human-readable title, if known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,

    /// MIME type, if known (e.g. `"text/markdown"`, `"application/pdf"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,

    /// Free-form backend metadata (size, author, revision, etc.). Defaults
    /// to JSON `null` and is skipped on serialization at that default.
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub metadata: Value,
}

// =====================================================================
// Filtering and listing
// =====================================================================

/// Filter passed to `subject/list`.
///
/// All fields are optional and combined with AND semantics. Empty `Vec`
/// fields mean "no constraint on this dimension". `cursor` is opaque to the
/// daemon — backends issue and accept their own pagination tokens.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SubjectFilter {
    /// Match subjects whose status is one of these.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub status: Vec<SubjectStatus>,

    /// Match subjects whose `kind` is one of these.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub kind: Vec<String>,

    /// Match subjects assigned to one of these identifiers.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub assignee: Vec<String>,

    /// Match subjects that have at least one of these labels.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub labels_any: Vec<String>,

    /// Match subjects that have all of these labels.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub labels_all: Vec<String>,

    /// Match subjects updated at or after this timestamp.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_since: Option<DateTime<Utc>>,

    /// Pagination cursor returned by a prior `subject/list` call.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,

    /// Suggested page size. Backends are free to clamp this.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,

    /// Match subjects whose backend-raw [`Subject::native_status`] equals
    /// this value. Use when the normalized [`SubjectStatus`] is too coarse
    /// — e.g. match Linear `"In Review"` but not `"In Progress"`, which
    /// both map to [`SubjectStatus::InProgress`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_status: Option<String>,

    /// Match subjects whose dispatch label (resolved by the backend from
    /// its [`SubjectSchema::status_dispatch_hints`] table) equals this
    /// value. Workflow YAML can use this to gate a phase on a label like
    /// `"code-review"` without coupling to any one backend's vocabulary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dispatch_label: Option<String>,

    /// Match subjects that carry at least one [`SubjectAttachment`] whose
    /// `kind` equals this value. Useful for `requires_document_attachment`
    /// style gates.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub has_attachment_kind: Option<String>,
}

/// Result of `subject/list`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubjectList {
    /// Subjects in this page.
    pub subjects: Vec<Subject>,

    /// Opaque cursor for the next page, or `None` if exhausted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,

    /// When the backend snapshot was taken. Used by the daemon for cache
    /// freshness reasoning.
    pub fetched_at: DateTime<Utc>,
}

// =====================================================================
// Patches
// =====================================================================

/// A patch applied to a subject via `subject/update`.
///
/// All fields are optional. Missing fields are not modified. The
/// double-`Option` on [`SubjectPatch::assignee`] distinguishes "not modified"
/// (`None`) from "explicitly clear" (`Some(None)`). Labels are partitioned
/// into add/remove sets to avoid lost-write races on the labels list as a
/// whole.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SubjectPatch {
    /// Set the normalized status. Backends translate to their native value
    /// using the workflow-YAML `status_map`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<SubjectStatus>,

    /// Set, change, or clear the assignee. `Some(None)` means clear.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assignee: Option<Option<String>>,

    /// Labels to add (deduplicated against existing labels).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub labels_add: Vec<String>,

    /// Labels to remove.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub labels_remove: Vec<String>,

    /// Optional comment to post alongside the update. Backends that don't
    /// support comments may surface this as a summary in their native
    /// activity log.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,

    /// Backend-specific custom fields to merge. An explicit JSON `null`
    /// value clears the field at that key.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub custom: BTreeMap<String, Value>,
}

// =====================================================================
// Delete
// =====================================================================

/// Request payload for `subject/delete`. Added in v0.1.8.
///
/// Backends that do not support deletion MUST reject with the
/// [`error_codes::METHOD_NOT_SUPPORTED`] JSON-RPC error code so the daemon
/// can fall back to status-only soft-cancel semantics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeleteSubjectRequest {
    /// Id of the subject to delete.
    pub id: SubjectId,
}

/// Response payload for `subject/delete`. Added in v0.1.8.
///
/// Carries a single `ok` flag so the wire shape remains an object (matching
/// every other subject verb) and so future fields — e.g. a tombstone id or a
/// `permanent: bool` discriminator — can be added without breaking v0.1.8
/// clients.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteSubjectResponse {
    /// True when the backend successfully deleted the subject.
    pub ok: bool,
}

// =====================================================================
// Schema / capability declaration
// =====================================================================

/// Capability declaration returned by `subject/schema`.
///
/// The daemon uses this to adapt behavior without runtime guessing — for
/// example, to skip `subject/watch` for polling-only backends, or to
/// pre-populate a UI with the subject's available custom-field values.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubjectSchema {
    /// Subject kinds this backend produces.
    pub kinds: Vec<String>,

    /// Normalized status values this backend can emit.
    pub status_values: Vec<SubjectStatus>,

    /// Whether `subject/watch` is implemented.
    pub supports_watch: bool,

    /// Whether the backend can create new subjects (reserved for v0.4.x).
    pub supports_create: bool,

    /// Whether `subject/list` honors `cursor`.
    pub supports_pagination: bool,

    /// Native (pre-mapping) status values the backend uses upstream. Useful
    /// for documenting `status_map` entries in workflow YAML.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub native_status_values: Vec<String>,

    /// Status dispatch hints: for each backend-native status the backend
    /// declares what normalized [`SubjectStatus`] it bucketizes to and an
    /// optional `dispatch_label` workflow YAML can fire phases on.
    ///
    /// `dispatch_label` decouples workflow YAML from any one backend's
    /// vocabulary. Multiple backends (Linear `"In Review"`, GitHub
    /// `"awaiting-review"`, Jira `"Code Review"`) can advertise the same
    /// `dispatch_label = "code-review"`, and a workflow phase keyed on
    /// that label fires for all of them.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub status_dispatch_hints: Vec<StatusDispatchHint>,

    /// Custom field declarations.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub custom_fields: Vec<CustomFieldSpec>,
}

/// A mapping from a backend-native status string to its normalized bucket
/// and optional dispatch label.
///
/// See [`SubjectSchema::status_dispatch_hints`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StatusDispatchHint {
    /// The backend's native status string, e.g. `"In Review"`, `"Spec"`,
    /// `"Shipped"`.
    pub native_status: String,

    /// The normalized [`SubjectStatus`] bucket this native value maps into.
    pub maps_to: SubjectStatus,

    /// Optional workflow dispatch label. When set, workflow YAML can
    /// trigger phases keyed on this label (e.g.
    /// `triggered_by_dispatch_label: code-review`) regardless of which
    /// backend the subject came from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dispatch_label: Option<String>,

    /// Optional human-readable description of the semantics of this
    /// native status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// Description of one custom field a backend exposes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CustomFieldSpec {
    /// Field key as it appears in [`Subject::custom`].
    pub key: String,
    /// Field type.
    #[serde(rename = "type")]
    pub kind: CustomFieldKind,
    /// For [`CustomFieldKind::Enum`] fields, the enumerated values.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub values: Option<Vec<String>>,
}

/// The type of a custom field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CustomFieldKind {
    /// Free-form string.
    String,
    /// Numeric (integer or float).
    Number,
    /// Boolean.
    Bool,
    /// Enumerated string; allowed values in [`CustomFieldSpec::values`].
    Enum,
    /// Date/time (ISO 8601).
    Date,
}

// =====================================================================
// Watch streams
// =====================================================================

/// Stream of subject change events delivered by `subject/watch`.
///
/// Each item is sent on the wire as a [`NOTIFICATION_SUBJECT_CHANGED`]
/// notification carrying the original watch-request id in `params.id`.
pub type EventStream = Pin<Box<dyn Stream<Item = SubjectChangedEvent> + Send>>;

/// Notification payload for `subject/changed`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubjectChangedEvent {
    /// Affected subject id.
    pub id: SubjectId,
    /// What kind of change occurred.
    pub change_kind: ChangeKind,
    /// The subject's new state.
    pub subject: Subject,

    /// Backend-raw status before the change, if known. Useful for
    /// `change_kind = "status-changed"` events where the normalized bucket
    /// did not move but the native status did (e.g. Linear `"Todo"` →
    /// `"In Review"` both bucket to [`SubjectStatus::InProgress`]).
    /// Defaults to `None`; skipped on serialization for v0.1.0
    /// wire compatibility.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_native_status: Option<String>,

    /// Workflow dispatch label before the change, if known. Paired with
    /// the new [`ChangeKind::DispatchLabelChanged`] so downstream consumers
    /// can react to label transitions independently of status transitions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_dispatch_label: Option<String>,
}

/// Categorization of a subject change event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ChangeKind {
    /// Subject was newly created upstream.
    Created,
    /// Subject was updated (any field).
    Updated,
    /// Subject's normalized status transitioned.
    StatusChanged,
    /// Subject was deleted upstream.
    Deleted,
    /// Subject's workflow dispatch label moved, independent of any change
    /// to the normalized [`SubjectStatus`] bucket. Emitted when the
    /// backend's native status moved between two values that share the
    /// same normalized bucket but have different
    /// [`StatusDispatchHint::dispatch_label`]s.
    DispatchLabelChanged,
    /// A [`SubjectAttachment`] was added to the subject.
    AttachmentAdded,
    /// A [`SubjectAttachment`] was removed from the subject.
    AttachmentRemoved,
}

// =====================================================================
// Errors
// =====================================================================

/// Errors a backend may return.
///
/// These map to JSON-RPC error responses via the [`From`] impl below.
/// Backend authors typically produce these directly from their trait
/// implementation; the runtime translates to wire-level [`RpcError`].
#[derive(Debug, thiserror::Error)]
pub enum BackendError {
    /// Subject does not exist.
    #[error("subject not found: {0}")]
    NotFound(String),

    /// Caller lacks permission for the requested action.
    #[error("permission denied: {0}")]
    PermissionDenied(String),

    /// Request was malformed at the domain level (distinct from
    /// JSON-RPC `invalid_params` which catches wire-shape problems).
    #[error("invalid request: {0}")]
    InvalidRequest(String),

    /// Backend (or its upstream) is temporarily unavailable.
    #[error("backend unavailable: {0}")]
    Unavailable(String),

    /// Backend recognized the method but does not implement it (e.g. a
    /// read-only backend rejecting `subject/delete`). Added in v0.1.8 so the
    /// runtime can map it to JSON-RPC `method_not_supported` (-32001) and the
    /// daemon can fall back to alternate semantics. v0.1.7 callers that match
    /// against [`BackendError`] exhaustively will need a wildcard arm.
    #[error("method not supported: {0}")]
    Unsupported(String),

    /// Anything else.
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl From<BackendError> for RpcError {
    fn from(error: BackendError) -> Self {
        match error {
            BackendError::NotFound(message) => RpcError {
                code: error_codes::INVALID_PARAMS,
                message: format!("not found: {message}"),
                data: Some(serde_json::json!({"category": "not_found"})),
            },
            BackendError::PermissionDenied(message) => RpcError {
                code: error_codes::INVALID_REQUEST,
                message: format!("permission denied: {message}"),
                data: Some(serde_json::json!({"category": "permission_denied"})),
            },
            BackendError::InvalidRequest(message) => RpcError {
                code: error_codes::INVALID_PARAMS,
                message,
                data: Some(serde_json::json!({"category": "invalid_request"})),
            },
            BackendError::Unavailable(message) => RpcError {
                code: error_codes::INTERNAL_ERROR,
                message: format!("backend unavailable: {message}"),
                data: Some(serde_json::json!({"category": "unavailable"})),
            },
            BackendError::Unsupported(message) => RpcError {
                code: error_codes::METHOD_NOT_SUPPORTED,
                message,
                data: Some(serde_json::json!({"category": "unsupported"})),
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
// The trait
// =====================================================================

/// What a subject backend plugin implements.
///
/// Backends are stateless from the trait's perspective — they read and write
/// against their upstream system on each call. The runtime handles the
/// JSON-RPC envelope, lifecycle methods, and (for streaming backends) wiring
/// the [`EventStream`] returned by [`SubjectBackend::watch`] into outgoing
/// [`NOTIFICATION_SUBJECT_CHANGED`] notifications.
///
/// # Example
///
/// ```ignore
/// use animus_subject_protocol::{
///     BackendError, EventStream, Subject, SubjectBackend, SubjectFilter, SubjectId,
///     SubjectList, SubjectPatch, SubjectSchema,
/// };
/// use animus_plugin_protocol::{HealthCheckResult, HealthStatus};
/// use async_trait::async_trait;
///
/// pub struct MyBackend;
///
/// #[async_trait]
/// impl SubjectBackend for MyBackend {
///     async fn list(&self, _filter: SubjectFilter) -> Result<SubjectList, BackendError> {
///         todo!()
///     }
///     async fn get(&self, _id: &SubjectId) -> Result<Subject, BackendError> {
///         todo!()
///     }
///     async fn update(&self, _id: &SubjectId, _patch: SubjectPatch) -> Result<Subject, BackendError> {
///         todo!()
///     }
///     async fn watch(&self) -> Option<EventStream> {
///         None
///     }
///     fn schema(&self) -> SubjectSchema {
///         todo!()
///     }
///     async fn health(&self) -> Result<HealthCheckResult, BackendError> {
///         Ok(HealthCheckResult {
///             status: HealthStatus::Healthy,
///             uptime_ms: None,
///             memory_usage_bytes: None,
///             last_error: None,
///         })
///     }
/// }
/// ```
#[async_trait]
pub trait SubjectBackend: Send + Sync + 'static {
    /// Return subjects matching `filter`. Called every daemon tick to
    /// discover ready work.
    async fn list(&self, filter: SubjectFilter) -> Result<SubjectList, BackendError>;

    /// Return one subject by id, or [`BackendError::NotFound`].
    async fn get(&self, id: &SubjectId) -> Result<Subject, BackendError>;

    /// Apply a patch and return the updated subject.
    async fn update(&self, id: &SubjectId, patch: SubjectPatch) -> Result<Subject, BackendError>;

    /// Permanently delete a subject by id. Added in v0.1.8.
    ///
    /// The default implementation returns
    /// [`BackendError::Unsupported`] so v0.1.7 backends compile against
    /// v0.1.8 unchanged. The wire dispatch maps `Unsupported` to
    /// JSON-RPC `method_not_supported` (-32001) so callers can fall back to
    /// status-only soft-cancel semantics without probing capabilities up
    /// front. Backends that opt in should override this method.
    async fn delete(&self, _id: &SubjectId) -> Result<DeleteSubjectResponse, BackendError> {
        Err(BackendError::Unsupported(
            "subject/delete not implemented by this backend".into(),
        ))
    }

    /// Open a stream of subject change events, or `None` if this backend is
    /// polling-only. Polling-only backends should also set
    /// [`SubjectSchema::supports_watch`] to `false`.
    async fn watch(&self) -> Option<EventStream>;

    /// Capability declaration. Should be cheap to compute (preferably a
    /// constant).
    fn schema(&self) -> SubjectSchema;

    /// Backend health. The daemon polls this on a schedule.
    async fn health(&self) -> Result<HealthCheckResult, BackendError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subject_status_serializes_kebab_case() {
        let v = serde_json::to_value(SubjectStatus::InProgress).unwrap();
        assert_eq!(v, serde_json::json!("in-progress"));
    }

    #[test]
    fn subject_id_round_trip() {
        let id = SubjectId::new("linear:ENG-123");
        let v = serde_json::to_value(&id).unwrap();
        assert_eq!(v, serde_json::json!("linear:ENG-123"));
        let back: SubjectId = serde_json::from_value(v).unwrap();
        assert_eq!(back, id);
    }

    #[test]
    fn patch_double_option_distinguishes_clear_from_unset() {
        let unset = SubjectPatch::default();
        let clear = SubjectPatch {
            assignee: Some(None),
            ..Default::default()
        };
        let set_to_alice = SubjectPatch {
            assignee: Some(Some("alice".to_string())),
            ..Default::default()
        };

        // Unset should not serialize an `assignee` key at all.
        let unset_v = serde_json::to_value(&unset).unwrap();
        assert!(
            unset_v.get("assignee").is_none(),
            "unset should omit assignee"
        );

        // Clear serializes as JSON null.
        let clear_v = serde_json::to_value(&clear).unwrap();
        assert_eq!(clear_v.get("assignee"), Some(&Value::Null));

        // Set serializes as the string.
        let set_v = serde_json::to_value(&set_to_alice).unwrap();
        assert_eq!(set_v.get("assignee"), Some(&Value::String("alice".into())));
    }

    #[test]
    fn backend_error_maps_to_rpc_error() {
        let rpc: RpcError = BackendError::NotFound("nope".into()).into();
        assert_eq!(rpc.code, error_codes::INVALID_PARAMS);
    }

    fn sample_subject_with_extras() -> Subject {
        Subject {
            id: SubjectId::new("linear:ENG-123"),
            kind: "issue".into(),
            title: "Implement flexible status".into(),
            description: None,
            status: SubjectStatus::InProgress,
            priority: None,
            assignee: None,
            labels: vec![],
            parent: None,
            children: vec![],
            url: None,
            created_at: "2026-05-01T12:00:00Z".parse().unwrap(),
            updated_at: "2026-05-13T13:55:00Z".parse().unwrap(),
            custom: BTreeMap::new(),
            native_status: Some("In Review".into()),
            status_metadata: serde_json::json!({"color": "#FFAA00", "state_id": "abc"}),
            attachments: vec![SubjectAttachment {
                id: "att-1".into(),
                kind: "document".into(),
                uri: "linear://issue/ENG-123/doc/spec".into(),
                title: Some("Spec".into()),
                mime_type: Some("text/markdown".into()),
                metadata: serde_json::json!({"revision": 3}),
            }],
        }
    }

    fn sample_subject_v0_1_0_shape() -> Subject {
        Subject {
            id: SubjectId::new("linear:ENG-123"),
            kind: "issue".into(),
            title: "Implement flexible status".into(),
            description: None,
            status: SubjectStatus::InProgress,
            priority: None,
            assignee: None,
            labels: vec![],
            parent: None,
            children: vec![],
            url: None,
            created_at: "2026-05-01T12:00:00Z".parse().unwrap(),
            updated_at: "2026-05-13T13:55:00Z".parse().unwrap(),
            custom: BTreeMap::new(),
            native_status: None,
            status_metadata: Value::Null,
            attachments: vec![],
        }
    }

    #[test]
    fn subject_with_native_status_round_trips_json() {
        let subject = sample_subject_with_extras();
        let v = serde_json::to_value(&subject).unwrap();
        assert_eq!(
            v.get("native_status"),
            Some(&Value::String("In Review".into()))
        );
        let metadata = v.get("status_metadata").expect("status_metadata present");
        assert_eq!(
            metadata.get("color"),
            Some(&Value::String("#FFAA00".into()))
        );
        let back: Subject = serde_json::from_value(v).unwrap();
        assert_eq!(back, subject);
    }

    #[test]
    fn subject_without_native_status_serializes_clean_v0_1_0_shape() {
        let subject = sample_subject_v0_1_0_shape();
        let v = serde_json::to_value(&subject).unwrap();
        // None of the v0.1.1-added fields may appear when at their defaults.
        assert!(
            v.get("native_status").is_none(),
            "native_status must be omitted"
        );
        assert!(
            v.get("status_metadata").is_none(),
            "status_metadata must be omitted"
        );
        assert!(
            v.get("attachments").is_none(),
            "attachments must be omitted"
        );
        // And the existing v0.1.0 fields are still present + correct.
        assert_eq!(v.get("id"), Some(&Value::String("linear:ENG-123".into())));
        assert_eq!(v.get("status"), Some(&Value::String("in-progress".into())));

        // A v0.1.0-shaped JSON (no new fields) must deserialize cleanly via
        // the v0.1.1 deserializer.
        let v0_1_0_json = serde_json::json!({
            "id": "linear:ENG-123",
            "kind": "issue",
            "title": "Implement flexible status",
            "status": "in-progress",
            "created_at": "2026-05-01T12:00:00Z",
            "updated_at": "2026-05-13T13:55:00Z"
        });
        let parsed: Subject = serde_json::from_value(v0_1_0_json).unwrap();
        assert_eq!(parsed.native_status, None);
        assert!(parsed.status_metadata.is_null());
        assert!(parsed.attachments.is_empty());
        // Re-serializing also yields no new fields.
        let reserialized = serde_json::to_value(&parsed).unwrap();
        assert!(reserialized.get("native_status").is_none());
        assert!(reserialized.get("status_metadata").is_none());
        assert!(reserialized.get("attachments").is_none());
    }

    #[test]
    fn subject_attachment_round_trips_json() {
        let attachment = SubjectAttachment {
            id: "att-7".into(),
            kind: "document".into(),
            uri: "linear://issue/ENG-1/doc/handoff".into(),
            title: Some("Handoff doc".into()),
            mime_type: Some("text/markdown".into()),
            metadata: serde_json::json!({"author": "alice"}),
        };
        let v = serde_json::to_value(&attachment).unwrap();
        assert_eq!(v.get("kind"), Some(&Value::String("document".into())));
        let back: SubjectAttachment = serde_json::from_value(v).unwrap();
        assert_eq!(back, attachment);

        // Default metadata is omitted on serialization.
        let bare = SubjectAttachment {
            id: "att-8".into(),
            kind: "url".into(),
            uri: "https://example.com/x".into(),
            title: None,
            mime_type: None,
            metadata: Value::Null,
        };
        let bare_v = serde_json::to_value(&bare).unwrap();
        assert!(bare_v.get("metadata").is_none());
        assert!(bare_v.get("title").is_none());
        assert!(bare_v.get("mime_type").is_none());
    }

    #[test]
    fn status_dispatch_hint_round_trips_json() {
        let hint = StatusDispatchHint {
            native_status: "In Review".into(),
            maps_to: SubjectStatus::InProgress,
            dispatch_label: Some("code-review".into()),
            description: Some("Awaiting peer review".into()),
        };
        let v = serde_json::to_value(&hint).unwrap();
        assert_eq!(
            v.get("native_status"),
            Some(&Value::String("In Review".into()))
        );
        assert_eq!(v.get("maps_to"), Some(&Value::String("in-progress".into())));
        assert_eq!(
            v.get("dispatch_label"),
            Some(&Value::String("code-review".into()))
        );
        let back: StatusDispatchHint = serde_json::from_value(v).unwrap();
        assert_eq!(back, hint);

        // Schema embedding round-trips through SubjectSchema.
        let schema = SubjectSchema {
            kinds: vec!["issue".into()],
            status_values: vec![SubjectStatus::InProgress, SubjectStatus::Done],
            supports_watch: false,
            supports_create: false,
            supports_pagination: true,
            native_status_values: vec!["In Review".into(), "Shipped".into()],
            status_dispatch_hints: vec![hint.clone()],
            custom_fields: vec![],
        };
        let sv = serde_json::to_value(&schema).unwrap();
        let back: SubjectSchema = serde_json::from_value(sv).unwrap();
        assert_eq!(back, schema);
    }

    #[test]
    fn subject_filter_native_status_round_trips() {
        let filter = SubjectFilter {
            native_status: Some("In Review".into()),
            dispatch_label: Some("code-review".into()),
            has_attachment_kind: Some("document".into()),
            ..Default::default()
        };
        let v = serde_json::to_value(&filter).unwrap();
        assert_eq!(
            v.get("native_status"),
            Some(&Value::String("In Review".into()))
        );
        assert_eq!(
            v.get("dispatch_label"),
            Some(&Value::String("code-review".into()))
        );
        assert_eq!(
            v.get("has_attachment_kind"),
            Some(&Value::String("document".into()))
        );
        let back: SubjectFilter = serde_json::from_value(v).unwrap();
        assert_eq!(back, filter);

        // Default filter omits all the new keys for v0.1.0 wire compat.
        let default_v = serde_json::to_value(SubjectFilter::default()).unwrap();
        assert!(default_v.get("native_status").is_none());
        assert!(default_v.get("dispatch_label").is_none());
        assert!(default_v.get("has_attachment_kind").is_none());
    }

    #[test]
    fn delete_subject_request_round_trips_json() {
        let req = DeleteSubjectRequest {
            id: SubjectId::new("linear:ENG-123"),
        };
        let v = serde_json::to_value(&req).unwrap();
        assert_eq!(v, serde_json::json!({ "id": "linear:ENG-123" }));
        let back: DeleteSubjectRequest = serde_json::from_value(v).unwrap();
        assert_eq!(back, req);
    }

    #[test]
    fn delete_subject_response_round_trips_json() {
        let resp = DeleteSubjectResponse { ok: true };
        let v = serde_json::to_value(&resp).unwrap();
        assert_eq!(v, serde_json::json!({ "ok": true }));
        let back: DeleteSubjectResponse = serde_json::from_value(v).unwrap();
        assert_eq!(back, resp);
    }

    #[test]
    fn backend_error_unsupported_maps_to_method_not_supported() {
        let rpc: RpcError = BackendError::Unsupported("delete not supported".into()).into();
        assert_eq!(rpc.code, error_codes::METHOD_NOT_SUPPORTED);
        assert_eq!(
            rpc.data
                .as_ref()
                .and_then(|d| d.get("category"))
                .and_then(|c| c.as_str()),
            Some("unsupported")
        );
    }

    #[test]
    fn method_subject_delete_constant_matches_wire_verb() {
        assert_eq!(METHOD_SUBJECT_DELETE, "subject/delete");
    }

    #[test]
    fn change_kind_dispatch_label_changed_round_trips() {
        let kind = ChangeKind::DispatchLabelChanged;
        let v = serde_json::to_value(kind).unwrap();
        assert_eq!(v, serde_json::json!("dispatch-label-changed"));
        let back: ChangeKind = serde_json::from_value(v).unwrap();
        assert_eq!(back, kind);

        // Sibling additions also serialize kebab-case.
        assert_eq!(
            serde_json::to_value(ChangeKind::AttachmentAdded).unwrap(),
            serde_json::json!("attachment-added")
        );
        assert_eq!(
            serde_json::to_value(ChangeKind::AttachmentRemoved).unwrap(),
            serde_json::json!("attachment-removed")
        );

        // Full SubjectChangedEvent with new previous_* fields round-trips.
        let event = SubjectChangedEvent {
            id: SubjectId::new("linear:ENG-123"),
            change_kind: ChangeKind::DispatchLabelChanged,
            subject: sample_subject_with_extras(),
            previous_native_status: Some("Todo".into()),
            previous_dispatch_label: Some("triage".into()),
        };
        let ev_v = serde_json::to_value(&event).unwrap();
        assert_eq!(
            ev_v.get("previous_native_status"),
            Some(&Value::String("Todo".into()))
        );
        assert_eq!(
            ev_v.get("previous_dispatch_label"),
            Some(&Value::String("triage".into()))
        );
        let back: SubjectChangedEvent = serde_json::from_value(ev_v).unwrap();
        assert_eq!(back, event);

        // Defaults omit the new fields entirely.
        let default_event = SubjectChangedEvent {
            id: SubjectId::new("linear:ENG-123"),
            change_kind: ChangeKind::Updated,
            subject: sample_subject_v0_1_0_shape(),
            previous_native_status: None,
            previous_dispatch_label: None,
        };
        let de_v = serde_json::to_value(&default_event).unwrap();
        assert!(de_v.get("previous_native_status").is_none());
        assert!(de_v.get("previous_dispatch_label").is_none());
    }
}
