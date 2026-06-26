//! # Animus workflow-journal plugin protocol
//!
//! The kernel's [`WorkflowStateManager`] persists workflow RUN STATE (the
//! orchestrator workflow blob), CHECKPOINTS, and a stream of lifecycle EVENTS.
//! Historically this lived in a local SQLite file (`workflow.db`), which is
//! ephemeral on disposable hosts (a Railway container loses run history on every
//! redeploy). This crate defines the `workflow_journal` plugin role so that
//! persistence becomes pluggable — an `InTree` SQLite backend by default, or an
//! out-of-tree backend (e.g. `animus-journal-postgres`) for durable, queryable
//! run history.
//!
//! The protocol treats the run state as an OPAQUE JSON blob (`run.blob`) plus a
//! few indexed SUMMARY columns (`workflow_id`, `workflow_ref`, `status`,
//! `kind`, timestamps). The plugin never has to understand the kernel's
//! `OrchestratorWorkflow` shape — it stores the blob and indexes the summary —
//! so the kernel can evolve the workflow model without a protocol bump.
//!
//! ## Methods
//! - [`METHOD_JOURNAL_SAVE`] — upsert a run's state.
//! - [`METHOD_JOURNAL_LOAD`] — load one run by id.
//! - [`METHOD_JOURNAL_LIST`] — list runs (filtered).
//! - [`METHOD_JOURNAL_QUERY_IDS`] — list matching run ids only.
//! - [`METHOD_JOURNAL_DELETE`] — delete a run (and its checkpoints/events).
//! - [`METHOD_JOURNAL_CHECKPOINT_SAVE`] / [`METHOD_JOURNAL_CHECKPOINT_LOAD`] /
//!   [`METHOD_JOURNAL_CHECKPOINT_LIST`] / [`METHOD_JOURNAL_CHECKPOINT_PRUNE`].
//! - [`METHOD_JOURNAL_RECORD`] — append a lifecycle event.
//! - [`METHOD_JOURNAL_EVENTS`] — query the event stream.
//! - [`METHOD_JOURNAL_SCHEMA`] — declare backend capabilities.

use animus_plugin_protocol::error_codes;
use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The plugin kind string a `workflow_journal` backend declares in its manifest.
pub const PLUGIN_KIND_WORKFLOW_JOURNAL: &str = "workflow_journal";

pub const METHOD_JOURNAL_SAVE: &str = "journal/save";
pub const METHOD_JOURNAL_LOAD: &str = "journal/load";
pub const METHOD_JOURNAL_LIST: &str = "journal/list";
pub const METHOD_JOURNAL_QUERY_IDS: &str = "journal/query_ids";
pub const METHOD_JOURNAL_DELETE: &str = "journal/delete";
pub const METHOD_JOURNAL_CHECKPOINT_SAVE: &str = "journal/checkpoint_save";
pub const METHOD_JOURNAL_CHECKPOINT_LOAD: &str = "journal/checkpoint_load";
pub const METHOD_JOURNAL_CHECKPOINT_LIST: &str = "journal/checkpoint_list";
pub const METHOD_JOURNAL_CHECKPOINT_PRUNE: &str = "journal/checkpoint_prune";
pub const METHOD_JOURNAL_RECORD: &str = "journal/record";
pub const METHOD_JOURNAL_EVENTS: &str = "journal/events";
pub const METHOD_JOURNAL_SCHEMA: &str = "journal/schema";

/// One persisted workflow run: an opaque state `blob` plus indexed summary
/// fields the backend stores in queryable columns.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct JournalRun {
    /// Unique workflow run id (the kernel's workflow id).
    pub workflow_id: String,
    /// The workflow definition this run is an instance of (e.g. `task-default`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_ref: Option<String>,
    /// Run status (e.g. `running`, `completed`, `failed`, `paused`). The kernel
    /// owns the vocabulary; the backend stores it verbatim for filtering.
    pub status: String,
    /// Optional workflow kind (e.g. `task`, `requirement`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// The opaque serialized run state (the kernel's `OrchestratorWorkflow`).
    pub blob: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<DateTime<Utc>>,
}

/// Params for [`METHOD_JOURNAL_SAVE`].
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SaveParams {
    pub run: JournalRun,
}

/// Params for [`METHOD_JOURNAL_LOAD`] / [`METHOD_JOURNAL_DELETE`].
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct WorkflowIdParams {
    pub workflow_id: String,
}

/// Result of [`METHOD_JOURNAL_LOAD`]: the run, or `None` if absent.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct LoadResult {
    pub run: Option<JournalRun>,
}

/// Filter for [`METHOD_JOURNAL_LIST`] / [`METHOD_JOURNAL_QUERY_IDS`]. All fields
/// are optional; an empty query lists everything (bounded by `limit`).
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct JournalQuery {
    /// Restrict to these statuses (any-of).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub status: Vec<String>,
    /// Restrict to one workflow definition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_ref: Option<String>,
    /// Only runs updated at/after this instant.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_since: Option<DateTime<Utc>>,
    /// Max rows to return. The backend MAY cap this lower and set `truncated`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

/// Result of [`METHOD_JOURNAL_LIST`].
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ListResult {
    pub runs: Vec<JournalRun>,
    /// True when the result was capped below the full match set.
    #[serde(default)]
    pub truncated: bool,
}

/// Result of [`METHOD_JOURNAL_QUERY_IDS`].
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct QueryIdsResult {
    pub ids: Vec<String>,
    #[serde(default)]
    pub truncated: bool,
}

/// Params for [`METHOD_JOURNAL_CHECKPOINT_SAVE`].
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CheckpointSaveParams {
    pub workflow_id: String,
    pub checkpoint_num: u32,
    pub blob: serde_json::Value,
}

/// Params for [`METHOD_JOURNAL_CHECKPOINT_LOAD`].
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CheckpointLoadParams {
    pub workflow_id: String,
    pub checkpoint_num: u32,
}

/// Result of [`METHOD_JOURNAL_CHECKPOINT_LOAD`].
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CheckpointLoadResult {
    pub blob: Option<serde_json::Value>,
}

/// Result of [`METHOD_JOURNAL_CHECKPOINT_LIST`].
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CheckpointListResult {
    pub checkpoint_nums: Vec<u32>,
}

/// Params for [`METHOD_JOURNAL_CHECKPOINT_PRUNE`]: keep the most recent `keep`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CheckpointPruneParams {
    pub workflow_id: String,
    pub keep: u32,
}

/// A workflow lifecycle event recorded into the journal.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct JournalEvent {
    /// The run this event belongs to (the workflow id).
    pub run_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_ref: Option<String>,
    pub kind: JournalEventKind,
    /// Phase name, for phase-scoped events.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    /// Agent/tool, for phase-scoped events.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
    /// Phase/run outcome, when terminal (e.g. `success`, `failed`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    pub ts: DateTime<Utc>,
    /// Arbitrary structured detail (timings, error message, decision, ...).
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    pub detail: serde_json::Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum JournalEventKind {
    RunStarted,
    PhaseStarted,
    PhaseCompleted,
    RunCompleted,
    RunFailed,
}

/// Params for [`METHOD_JOURNAL_RECORD`].
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RecordParams {
    pub event: JournalEvent,
}

/// Filter for [`METHOD_JOURNAL_EVENTS`].
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct EventQuery {
    /// Restrict to one run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub since: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

/// Result of [`METHOD_JOURNAL_EVENTS`].
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct EventsResult {
    pub events: Vec<JournalEvent>,
    #[serde(default)]
    pub truncated: bool,
}

/// Capability declaration returned by [`METHOD_JOURNAL_SCHEMA`]. A minimal
/// backend can persist runs but skip events/checkpoints; the kernel reads this
/// to decide what to delegate vs. keep local.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct JournalSchema {
    pub supports_checkpoints: bool,
    pub supports_events: bool,
    /// True if `journal/list` honors the `status`/`workflow_ref` filters
    /// server-side (vs. the kernel filtering client-side).
    pub supports_filtering: bool,
}

/// A backend error surfaced over RPC. Maps to a plugin-protocol error code.
#[derive(Debug, Clone, thiserror::Error, Serialize, Deserialize, JsonSchema)]
pub enum JournalError {
    #[error("workflow run not found: {0}")]
    NotFound(String),
    #[error("journal backend method not supported: {0}")]
    MethodNotSupported(String),
    #[error("journal backend internal error: {0}")]
    Internal(String),
}

impl JournalError {
    /// The plugin-protocol error code this maps to over the wire.
    #[must_use]
    pub fn code(&self) -> i32 {
        match self {
            JournalError::NotFound(_) => error_codes::INVALID_PARAMS,
            JournalError::MethodNotSupported(_) => error_codes::METHOD_NOT_SUPPORTED,
            JournalError::Internal(_) => error_codes::INTERNAL_ERROR,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_blob_round_trips() {
        let run = JournalRun {
            workflow_id: "wf-1".into(),
            workflow_ref: Some("task-default".into()),
            status: "running".into(),
            kind: Some("task".into()),
            blob: serde_json::json!({ "phases": [], "id": "wf-1" }),
            created_at: None,
            updated_at: None,
        };
        let json = serde_json::to_string(&run).unwrap();
        let back: JournalRun = serde_json::from_str(&json).unwrap();
        assert_eq!(back.workflow_id, "wf-1");
        assert_eq!(back.blob["id"], "wf-1");
    }

    #[test]
    fn event_kind_serializes_snake_case() {
        let v = serde_json::to_string(&JournalEventKind::PhaseCompleted).unwrap();
        assert_eq!(v, "\"phase_completed\"");
    }

    #[test]
    fn error_codes_map() {
        assert_eq!(JournalError::NotFound("x".into()).code(), error_codes::INVALID_PARAMS);
        assert_eq!(
            JournalError::MethodNotSupported("journal/events".into()).code(),
            error_codes::METHOD_NOT_SUPPORTED
        );
    }
}
