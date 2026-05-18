//! `LogStorageBackend` trait and log schema for Animus log storage plugins.
//!
//! Log storage is the fourth plugin kind alongside
//! [`SubjectBackend`](animus_plugin_protocol::PLUGIN_KIND_SUBJECT_BACKEND),
//! [`ProviderBackend`](animus_plugin_protocol::PLUGIN_KIND_PROVIDER), and
//! [`TriggerBackend`](animus_plugin_protocol::PLUGIN_KIND_TRIGGER_BACKEND). A
//! log storage backend persists structured log entries emitted by the Animus
//! daemon, its plugins, the CLI, and individual workflow runs, and lets
//! operators query and tail those entries — locally as a flat `events.jsonl`
//! file, or via an external aggregator like Grafana Loki, Splunk, or
//! ClickHouse.
//!
//! This crate defines:
//!
//! - The [`LogEntry`] payload shape — one structured log line.
//! - The [`LogLevel`] and [`LogSource`] enums.
//! - The [`LogQuery`] filter passed to [`LogStorageBackend::query`] and
//!   [`LogStorageBackend::tail`].
//! - The [`LogQueryResult`] paginated response shape.
//! - The [`LogStorageSchema`] capability declaration returned by
//!   [`METHOD_LOG_STORAGE_SCHEMA`].
//! - The [`LogStream`] alias used by [`LogStorageBackend::tail`].
//! - The Rust-side [`LogStorageBackend`] trait that plugin authors implement.
//! - The JSON-RPC method-name constants used on the wire (e.g.
//!   [`METHOD_LOG_STORAGE_STORE`]).
//! - [`BackendError`] mapping backend failures to JSON-RPC error responses.
//!
//! Plugin authors typically depend on this crate alongside
//! [`animus-plugin-runtime`], implement [`LogStorageBackend`] for their type,
//! and call
//! `animus_plugin_runtime::log_storage_backend_main(info, backend).await`
//! from `main`.

#![warn(missing_docs)]

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

/// `log_storage/store` — persist a batch of [`LogEntry`] records.
///
/// Backends MAY deduplicate by [`LogEntry::id`] to keep
/// at-least-once delivery idempotent.
pub const METHOD_LOG_STORAGE_STORE: &str = "log_storage/store";

/// `log_storage/query` — non-streaming query for historical log entries.
pub const METHOD_LOG_STORAGE_QUERY: &str = "log_storage/query";

/// `log_storage/tail` — open a streaming query. Returns immediately and then
/// emits [`NOTIFICATION_LOG_STORAGE_EVENT`] notifications carrying the
/// originating request id in `params.id`.
pub const METHOD_LOG_STORAGE_TAIL: &str = "log_storage/tail";

/// `log_storage/event` — notification method emitted by
/// [`METHOD_LOG_STORAGE_TAIL`] streams.
pub const NOTIFICATION_LOG_STORAGE_EVENT: &str = "log_storage/event";

/// `log_storage/schema` — capability declaration; returns [`LogStorageSchema`].
pub const METHOD_LOG_STORAGE_SCHEMA: &str = "log_storage/schema";

/// Plugin kind constant for log storage backend plugins.
pub const PLUGIN_KIND_LOG_STORAGE_BACKEND: &str = "log_storage_backend";

// =====================================================================
// Log entry payload
// =====================================================================

/// One structured log record persisted by a log storage backend.
///
/// `LogEntry` is the unit of work for `log_storage/*` methods — the daemon
/// produces them, the backend persists them, and operators query them. The
/// `fields` value is opaque to the host and lets emitters attach structured
/// key/value context (`request_id`, `subject_id`, `workflow_id`, ...) without
/// stretching the fixed schema.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogEntry {
    /// Backend-assigned unique id. UUID, ULID, monotonic timestamp,
    /// `(source, ts, hash(message))` — whatever the backend can dedupe on.
    pub id: String,

    /// When the upstream emitter produced the entry. Backends MUST preserve
    /// the original timestamp rather than overwriting with arrival time.
    pub ts: DateTime<Utc>,

    /// Severity level. Used both for filtering and for surfacing in operator
    /// UIs.
    pub level: LogLevel,

    /// What produced this entry — the daemon process, a plugin, the CLI, or
    /// an in-flight workflow run.
    pub source: LogSource,

    /// Free-form name disambiguating multiple emitters within a [`LogSource`]:
    /// plugin name (`"animus-subject-linear"`), workflow id, CLI command,
    /// etc. `None` is acceptable for source kinds with only one emitter.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_name: Option<String>,

    /// Hierarchical target identifier matching the
    /// [`tracing`](https://docs.rs/tracing) module convention, e.g.
    /// `"plugin.animus-subject-linear.client"`. Backends MAY index this for
    /// faster glob matches.
    pub target: String,

    /// Human-readable log message. Multi-line content is allowed; emitters
    /// SHOULD avoid stuffing entire JSON blobs here and use `fields` instead.
    pub message: String,

    /// Structured key/value context. Defaults to `null` (omitted on the
    /// wire) when the emitter has no structured context to attach.
    #[serde(default, skip_serializing_if = "is_null")]
    pub fields: Value,
}

fn is_null(value: &Value) -> bool {
    value.is_null()
}

// =====================================================================
// Log level
// =====================================================================

/// Severity level for a [`LogEntry`].
///
/// Levels follow the [`tracing`](https://docs.rs/tracing) convention.
/// Backends MAY drop entries below a configured floor; the daemon does not
/// require backends to persist every level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    /// Highest-volume, lowest-priority. Used for fine-grained instrumentation.
    Trace,
    /// Debug-level diagnostic detail. Off by default in production.
    Debug,
    /// Routine informational messages.
    Info,
    /// Recoverable problem the operator should know about.
    Warn,
    /// Failure that prevented the requested work from completing.
    Error,
}

// =====================================================================
// Log source
// =====================================================================

/// What produced a [`LogEntry`].
///
/// The set is closed — every emitter in the Animus protocol maps to one of
/// these four buckets. Disambiguate finer-grained emitters via
/// [`LogEntry::source_name`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogSource {
    /// The Animus daemon process itself.
    Daemon,
    /// A plugin process (subject backend, provider, trigger, log storage, ...).
    /// Use [`LogEntry::source_name`] to name the specific plugin.
    Plugin,
    /// A short-lived CLI invocation. Use [`LogEntry::source_name`] to name
    /// the command (`"queue list"`, `"workflow run"`, ...).
    Cli,
    /// An in-flight workflow run. Use [`LogEntry::source_name`] to carry the
    /// workflow id.
    Workflow,
}

// =====================================================================
// Query
// =====================================================================

/// Filter applied to [`LogStorageBackend::query`] and
/// [`LogStorageBackend::tail`].
///
/// All fields are optional and combined with AND semantics. Backends MUST
/// honor filters they advertise via [`LogStorageSchema::supports_filtering`]
/// and MAY return [`error_codes::METHOD_NOT_SUPPORTED`] for filters they
/// can't apply — the daemon then filters in-process.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct LogQuery {
    /// Minimum severity. Entries below this level are excluded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_level: Option<LogLevel>,

    /// Match on [`LogEntry::source`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<LogSource>,

    /// Exact-match against [`LogEntry::source_name`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_name: Option<String>,

    /// Glob pattern matched against [`LogEntry::target`]. Conventional glob
    /// semantics: `*` matches any run of non-`.` characters, `**` matches
    /// any run including `.`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_glob: Option<String>,

    /// Lower bound (inclusive) on [`LogEntry::ts`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub since: Option<DateTime<Utc>>,

    /// Upper bound (exclusive) on [`LogEntry::ts`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub until: Option<DateTime<Utc>>,

    /// Maximum number of entries to return from a single
    /// [`LogStorageBackend::query`] call. `None` means no caller-imposed
    /// limit; the backend may still enforce its own.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,

    /// Opaque resume cursor returned by a prior
    /// [`LogQueryResult::next_cursor`]. Backends define the encoding.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,

    /// `true` = continue streaming new entries as they arrive (only
    /// meaningful for [`LogStorageBackend::tail`]). Default `false`.
    #[serde(default)]
    pub follow: bool,
}

// =====================================================================
// Query result
// =====================================================================

/// One page of [`LogEntry`] records returned by
/// [`LogStorageBackend::query`].
///
/// Backends that paginate set `next_cursor` to an opaque string the caller
/// passes back as [`LogQuery::cursor`]. Backends that return everything
/// in one shot leave it `None`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogQueryResult {
    /// Matched entries in chronological order (oldest first). Backends MAY
    /// reverse this for performance; if so, document it.
    pub entries: Vec<LogEntry>,

    /// Opaque cursor for the next page, or `None` if exhausted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

// =====================================================================
// Schema / capability declaration
// =====================================================================

/// Capability declaration returned by [`METHOD_LOG_STORAGE_SCHEMA`].
///
/// The daemon uses this to adapt behavior without runtime guessing — for
/// example, to skip [`LogStorageBackend::tail`] for batch-only backends, or
/// to know whether to apply filters in-process before/after sending the
/// query.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogStorageSchema {
    /// Backend implements [`LogStorageBackend::query`] for historical reads.
    /// If `false`, the backend is write-only (a sink); hosts MUST NOT call
    /// `query`.
    pub supports_query: bool,

    /// Backend implements [`LogStorageBackend::tail`] for streaming reads.
    pub supports_tail: bool,

    /// Backend deduplicates by [`LogEntry::id`]. Hosts MAY skip their own
    /// dedup table when this is `true`.
    pub supports_dedup: bool,

    /// Which [`LogQuery`] filters the backend can evaluate server-side.
    pub supports_filtering: SupportsFiltering,

    /// Maximum query window the backend will honor. Loki caps at 30d by
    /// default; ClickHouse has no hard cap. `None` means the backend
    /// declines to advertise a limit. Stored as a millisecond count on the
    /// wire to keep the schema serde-friendly.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "duration_ms_opt")]
    pub max_query_window: Option<chrono::Duration>,

    /// Typical retention period after which entries are evicted. Surfaced to
    /// operators so they understand how far back queries can reach.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "duration_ms_opt")]
    pub retention_hint: Option<chrono::Duration>,
}

/// Server-side filter support advertised by [`LogStorageSchema`].
///
/// Fields default to `false`; backends opt into each filter they can apply
/// natively. Filters the backend doesn't support are evaluated in-process
/// by the daemon.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct SupportsFiltering {
    /// Backend honors [`LogQuery::min_level`].
    #[serde(default)]
    pub by_level: bool,
    /// Backend honors [`LogQuery::source`].
    #[serde(default)]
    pub by_source: bool,
    /// Backend honors [`LogQuery::target_glob`] as an exact-target match
    /// (without the glob expansion).
    #[serde(default)]
    pub by_target: bool,
    /// Backend honors [`LogQuery::since`] and [`LogQuery::until`].
    #[serde(default)]
    pub by_time_range: bool,
    /// Backend evaluates [`LogQuery::target_glob`] with full glob semantics.
    /// Implies (but does not require) [`SupportsFiltering::by_target`].
    #[serde(default)]
    pub by_glob: bool,
}

// =====================================================================
// Tail streams
// =====================================================================

/// Stream of [`LogEntry`] items delivered by [`LogStorageBackend::tail`].
///
/// Each item is sent on the wire as a [`NOTIFICATION_LOG_STORAGE_EVENT`]
/// notification carrying the original tail-request id in `params.id`.
/// Errors yielded by the stream are forwarded as
/// [`NOTIFICATION_LOG_STORAGE_EVENT`] notifications with the error payload
/// in `params.error`; fatal stream-level failures terminate the tail.
pub type LogStream = Pin<Box<dyn Stream<Item = Result<LogEntry, BackendError>> + Send>>;

// =====================================================================
// Errors
// =====================================================================

/// Errors a log storage backend may return.
///
/// These map to JSON-RPC error responses via the [`From`] impl below.
/// Backend authors typically produce these directly from their trait
/// implementation; the runtime translates to wire-level [`RpcError`].
///
/// The spec refers to this conceptually as `ProtocolError`; the in-crate
/// name `BackendError` matches the convention used by the sibling
/// `animus-subject-protocol` and `animus-trigger-protocol` crates.
#[derive(Debug, thiserror::Error)]
pub enum BackendError {
    /// Caller asked for a cursor/id the backend doesn't recognize.
    #[error("not found: {0}")]
    NotFound(String),

    /// Backend recognized the call but does not implement it (e.g.
    /// `log_storage/tail` on a batch-only backend).
    #[error("not supported: {0}")]
    NotSupported(String),

    /// Request was malformed at the domain level (e.g. `since` after
    /// `until`).
    #[error("invalid request: {0}")]
    InvalidRequest(String),

    /// Backend (or its upstream) is temporarily unavailable.
    #[error("backend unavailable: {0}")]
    Unavailable(String),

    /// Caller exceeded a quota or rate limit imposed by the backend or its
    /// upstream.
    #[error("quota exceeded: {0}")]
    QuotaExceeded(String),

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
            BackendError::NotSupported(message) => RpcError {
                code: error_codes::METHOD_NOT_SUPPORTED,
                message,
                data: Some(serde_json::json!({"category": "not_supported"})),
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
            BackendError::QuotaExceeded(message) => RpcError {
                code: error_codes::INTERNAL_ERROR,
                message: format!("quota exceeded: {message}"),
                data: Some(serde_json::json!({"category": "quota_exceeded"})),
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

/// What a log storage backend plugin implements.
///
/// Log storage backends own a piece of durable storage — a `events.jsonl`
/// file, a Loki tenant, a Splunk index, a ClickHouse table, an S3 prefix,
/// whatever — and expose three operations on top of it: persist new entries
/// ([`LogStorageBackend::store`]), query historical entries
/// ([`LogStorageBackend::query`]), and tail entries as they arrive
/// ([`LogStorageBackend::tail`]).
///
/// Backends that cannot read (write-only sinks) advertise
/// `supports_query = false` and `supports_tail = false` in [`schema`] and
/// return [`BackendError::NotSupported`] from the corresponding methods.
/// The default in-tree `animus-log-storage-file` backend implements all
/// three.
///
/// # Example
///
/// ```ignore
/// use animus_log_storage_protocol::{
///     BackendError, LogEntry, LogQuery, LogQueryResult, LogStorageBackend,
///     LogStorageSchema, LogStream, SupportsFiltering,
/// };
/// use animus_plugin_protocol::{HealthCheckResult, HealthStatus};
/// use async_trait::async_trait;
/// use futures_core::stream;
///
/// pub struct InMemoryBackend {
///     entries: tokio::sync::Mutex<Vec<LogEntry>>,
/// }
///
/// #[async_trait]
/// impl LogStorageBackend for InMemoryBackend {
///     async fn store(&self, entries: Vec<LogEntry>) -> Result<(), BackendError> {
///         self.entries.lock().await.extend(entries);
///         Ok(())
///     }
///
///     async fn query(&self, _filter: LogQuery) -> Result<LogQueryResult, BackendError> {
///         let entries = self.entries.lock().await.clone();
///         Ok(LogQueryResult { entries, next_cursor: None })
///     }
///
///     async fn tail(&self, _filter: LogQuery) -> Result<LogStream, BackendError> {
///         Ok(Box::pin(stream::iter(Vec::<Result<LogEntry, BackendError>>::new())))
///     }
///
///     fn schema(&self) -> LogStorageSchema {
///         LogStorageSchema {
///             supports_query: true,
///             supports_tail: true,
///             supports_dedup: false,
///             supports_filtering: SupportsFiltering::default(),
///             max_query_window: None,
///             retention_hint: None,
///         }
///     }
///
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
pub trait LogStorageBackend: Send + Sync + 'static {
    /// Persist a batch of log entries. Backends MAY dedupe by
    /// [`LogEntry::id`]; hosts treat this as an at-least-once write.
    ///
    /// Implementations SHOULD be transactional within a single call — either
    /// all entries land or none do — so callers can retry on partial
    /// failure without producing duplicates.
    async fn store(&self, entries: Vec<LogEntry>) -> Result<(), BackendError>;

    /// Query historical log entries matching `filter`. Non-streaming.
    ///
    /// Backends that don't support reads return
    /// [`BackendError::NotSupported`] and advertise `supports_query = false`
    /// in [`schema`].
    async fn query(&self, filter: LogQuery) -> Result<LogQueryResult, BackendError>;

    /// Stream log entries matching `filter`. If `filter.follow = true`, the
    /// stream remains open and emits new entries as they arrive.
    ///
    /// Backends that don't support streaming return
    /// [`BackendError::NotSupported`] and advertise `supports_tail = false`
    /// in [`schema`].
    async fn tail(&self, filter: LogQuery) -> Result<LogStream, BackendError>;

    /// Capability declaration. Should be cheap (preferably a constant).
    fn schema(&self) -> LogStorageSchema;

    /// Backend health. The daemon polls this on a schedule.
    async fn health(&self) -> Result<HealthCheckResult, BackendError>;
}

// =====================================================================
// Helpers
// =====================================================================

/// Serde helper for `Option<chrono::Duration>` ↔ millisecond integer.
///
/// `chrono::Duration` does not implement `Serialize`, so [`LogStorageSchema`]
/// stores its durations as a signed millisecond count on the wire. `None`
/// is serialized as a missing field (see `skip_serializing_if`).
mod duration_ms_opt {
    use chrono::Duration;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(value: &Option<Duration>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match value {
            Some(duration) => duration.num_milliseconds().serialize(serializer),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt: Option<i64> = Option::deserialize(deserializer)?;
        Ok(opt.map(Duration::milliseconds))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixed_ts() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-05-17T18:20:34Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    #[test]
    fn log_entry_round_trips_json() {
        let entry = LogEntry {
            id: "evt-001".into(),
            ts: fixed_ts(),
            level: LogLevel::Info,
            source: LogSource::Plugin,
            source_name: Some("animus-subject-linear".into()),
            target: "plugin.animus-subject-linear.client".into(),
            message: "fetched 14 issues".into(),
            fields: serde_json::json!({"count": 14}),
        };
        let value = serde_json::to_value(&entry).unwrap();
        let back: LogEntry = serde_json::from_value(value).unwrap();
        assert_eq!(back, entry);
    }

    #[test]
    fn log_query_round_trips_with_all_filters_set() {
        let query = LogQuery {
            min_level: Some(LogLevel::Warn),
            source: Some(LogSource::Daemon),
            source_name: Some("scheduler".into()),
            target_glob: Some("daemon.scheduler.*".into()),
            since: Some(fixed_ts()),
            until: Some(fixed_ts()),
            limit: Some(100),
            cursor: Some("page-2".into()),
            follow: true,
        };
        let value = serde_json::to_value(&query).unwrap();
        let back: LogQuery = serde_json::from_value(value).unwrap();
        assert_eq!(back, query);
    }

    #[test]
    fn log_storage_schema_round_trips() {
        let schema = LogStorageSchema {
            supports_query: true,
            supports_tail: true,
            supports_dedup: true,
            supports_filtering: SupportsFiltering {
                by_level: true,
                by_source: true,
                by_target: true,
                by_time_range: true,
                by_glob: false,
            },
            max_query_window: Some(chrono::Duration::days(30)),
            retention_hint: Some(chrono::Duration::days(7)),
        };
        let value = serde_json::to_value(&schema).unwrap();
        let back: LogStorageSchema = serde_json::from_value(value).unwrap();
        assert_eq!(back, schema);
    }

    #[test]
    fn log_entry_omits_empty_fields_in_serialization() {
        let entry = LogEntry {
            id: "evt-002".into(),
            ts: fixed_ts(),
            level: LogLevel::Error,
            source: LogSource::Daemon,
            source_name: None,
            target: "daemon".into(),
            message: "panic".into(),
            fields: Value::Null,
        };
        let value = serde_json::to_value(&entry).unwrap();
        assert!(
            value.get("source_name").is_none(),
            "source_name should be omitted when None"
        );
        assert!(
            value.get("fields").is_none(),
            "fields should be omitted when Null"
        );
    }

    #[test]
    fn log_level_serializes_lowercase() {
        for (level, expected) in [
            (LogLevel::Trace, "trace"),
            (LogLevel::Debug, "debug"),
            (LogLevel::Info, "info"),
            (LogLevel::Warn, "warn"),
            (LogLevel::Error, "error"),
        ] {
            assert_eq!(
                serde_json::to_value(level).unwrap(),
                serde_json::json!(expected)
            );
        }
    }

    #[test]
    fn log_source_serializes_snake_case() {
        for (source, expected) in [
            (LogSource::Daemon, "daemon"),
            (LogSource::Plugin, "plugin"),
            (LogSource::Cli, "cli"),
            (LogSource::Workflow, "workflow"),
        ] {
            assert_eq!(
                serde_json::to_value(source).unwrap(),
                serde_json::json!(expected)
            );
        }
    }

    #[test]
    fn backend_error_not_supported_maps_to_method_not_supported() {
        let rpc: RpcError = BackendError::NotSupported("log_storage/tail".into()).into();
        assert_eq!(rpc.code, error_codes::METHOD_NOT_SUPPORTED);
    }

    #[test]
    fn log_storage_schema_default_durations_omitted_on_wire() {
        let schema = LogStorageSchema {
            supports_query: true,
            supports_tail: false,
            supports_dedup: false,
            supports_filtering: SupportsFiltering::default(),
            max_query_window: None,
            retention_hint: None,
        };
        let value = serde_json::to_value(&schema).unwrap();
        assert!(value.get("max_query_window").is_none());
        assert!(value.get("retention_hint").is_none());
    }
}
