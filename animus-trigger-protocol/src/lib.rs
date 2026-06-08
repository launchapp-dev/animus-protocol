//! `TriggerBackend` trait and event schema for Animus trigger plugins.
//!
//! Triggers are the third plugin kind alongside
//! [`SubjectBackend`](animus_plugin_protocol::PLUGIN_KIND_SUBJECT_BACKEND) and
//! [`ProviderBackend`](animus_plugin_protocol::PLUGIN_KIND_PROVIDER). They are
//! push-driven event sources: a trigger plugin listens for activity in some
//! external system — Slack mentions, generic webhooks, file system changes,
//! cron schedules, GitHub events — and emits [`TriggerEvent`]s that the
//! Animus daemon consumes and turns into work (queueing a workflow,
//! creating a task, kicking off a review, ...).
//!
//! This crate defines:
//!
//! - The [`TriggerEvent`] payload shape.
//! - The [`TriggerSchema`] capability declaration returned by
//!   [`METHOD_TRIGGER_SCHEMA`].
//! - The [`TriggerStream`] alias used by [`TriggerBackend::watch`].
//! - The Rust-side [`TriggerBackend`] trait that plugin authors implement.
//! - The JSON-RPC method-name constants used on the wire (e.g.
//!   [`METHOD_TRIGGER_WATCH`]).
//! - [`BackendError`] mapping backend failures to JSON-RPC error responses.
//!
//! Plugin authors typically depend on this crate alongside
//! [`animus-plugin-runtime`], implement [`TriggerBackend`] for their type, and
//! call `animus_plugin_runtime::trigger_backend_main(info, backend).await`
//! from `main`.

#![warn(missing_docs)]

use std::pin::Pin;

use animus_plugin_protocol::{error_codes, HealthCheckResult, RpcError};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures_core::Stream;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

// =====================================================================
// Method-name constants (the JSON-RPC wire methods)
// =====================================================================

/// `trigger/watch` — open the event stream. The plugin acknowledges
/// immediately and then emits [`NOTIFICATION_TRIGGER_EVENT`] notifications
/// carrying the original watch-request id.
pub const METHOD_TRIGGER_WATCH: &str = "trigger/watch";

/// `trigger/ack` — acknowledge an event id so the backend does not redeliver
/// it. No-op for backends that don't track delivery state.
pub const METHOD_TRIGGER_ACK: &str = "trigger/ack";

/// `trigger/schema` — capability declaration; returns [`TriggerSchema`].
pub const METHOD_TRIGGER_SCHEMA: &str = "trigger/schema";

/// `trigger/event` — notification method emitted by [`METHOD_TRIGGER_WATCH`]
/// streams.
pub const NOTIFICATION_TRIGGER_EVENT: &str = "trigger/event";

// =====================================================================
// Event payload
// =====================================================================

/// One event emitted by a trigger backend.
///
/// Trigger backends produce these in response to external activity (a Slack
/// mention, a webhook POST, a file change, a cron tick, ...). The daemon
/// consumes the stream returned by [`TriggerBackend::watch`] and decides
/// what to do with each event using configuration the workflow YAML
/// declares — typically "enqueue this workflow" or "create this task".
///
/// Backend-specific payload data lives in [`TriggerEvent::payload`] and is
/// addressable from workflow YAML via templating (e.g.
/// `{{trigger.payload.user}}`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct TriggerEvent {
    /// Stable identifier for this event. Used for deduplication if the
    /// daemon restarts and the backend redelivers from its journal, and as
    /// the argument to [`TriggerBackend::ack`].
    ///
    /// Backends that cannot produce a stable id (e.g. timer-based triggers)
    /// SHOULD synthesize one from `(kind, occurred_at, payload-hash)` so
    /// duplicates collapse.
    pub id: String,

    /// When the upstream event occurred. Falls back to `Utc::now()` if the
    /// source does not carry a timestamp.
    pub occurred_at: DateTime<Utc>,

    /// What kind of trigger fired. Convention is
    /// `"<backend>_<event>"`, e.g. `"slack_mention"`,
    /// `"slack_channel_message"`, `"github_webhook_push"`,
    /// `"file_changed"`, `"cron_tick"`. The daemon treats the value as
    /// opaque; workflows match on it.
    pub kind: String,

    /// Free-form payload describing the event. Schema is trigger-specific
    /// and documented by each backend's plugin.
    pub payload: Value,

    /// Optional subject id (e.g. `"linear:ENG-123"`) this event is about,
    /// if the backend can resolve one. Lets workflows correlate triggers
    /// with subject backends without re-querying.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject_id: Option<String>,

    /// Optional hint about what the daemon should do (e.g.
    /// `"run-workflow:review"`, `"create-task"`). Workflows MAY ignore
    /// this — it is an advisory field, not a routing decision.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action_hint: Option<String>,
}

// =====================================================================
// Schema / capability declaration
// =====================================================================

/// Capability declaration returned by [`METHOD_TRIGGER_SCHEMA`].
///
/// The daemon uses this to adapt behavior without runtime guessing — for
/// example, to skip [`TriggerBackend::ack`] calls for fire-and-forget
/// backends, or to expose the set of event kinds to workflow authors so
/// they can match on `kind`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct TriggerSchema {
    /// Event kinds this backend can emit. Examples:
    /// `["slack_mention", "slack_channel_message"]`,
    /// `["github_webhook_push", "github_webhook_pull_request"]`,
    /// `["cron_tick"]`.
    pub kinds: Vec<String>,

    /// Whether the backend can resume a subscription after a host restart
    /// (i.e. honors a delivery cursor). Backends without resume support
    /// re-emit events from the moment [`TriggerBackend::watch`] is called.
    pub supports_resume: bool,

    /// Whether the backend deduplicates events across restarts by emitting
    /// the same [`TriggerEvent::id`] for a re-seen event. Hosts use this to
    /// decide whether to maintain their own dedup table.
    pub supports_dedup: bool,

    /// Whether the backend honors [`METHOD_TRIGGER_ACK`]. Backends without
    /// ack support return [`error_codes::METHOD_NOT_SUPPORTED`] from
    /// `trigger/ack`; hosts then skip the call.
    pub supports_ack: bool,
}

// =====================================================================
// Watch streams
// =====================================================================

/// Stream of trigger events delivered by [`TriggerBackend::watch`].
///
/// Each item is sent on the wire as a [`NOTIFICATION_TRIGGER_EVENT`]
/// notification carrying the original watch-request id in `params.id`.
/// Errors yielded by the stream are forwarded as
/// [`NOTIFICATION_TRIGGER_EVENT`] notifications with the error payload in
/// `params.error`; fatal stream-level failures terminate the watch.
pub type TriggerStream = Pin<Box<dyn Stream<Item = Result<TriggerEvent, BackendError>> + Send>>;

// =====================================================================
// Errors
// =====================================================================

/// Errors a trigger backend may return.
///
/// These map to JSON-RPC error responses via the [`From`] impl below.
/// Backend authors typically produce these directly from their trait
/// implementation; the runtime translates to wire-level [`RpcError`].
#[derive(Debug, thiserror::Error)]
pub enum BackendError {
    /// Caller asked the backend to ack an event id the backend doesn't know
    /// about (e.g. already-acked or expired from the delivery journal).
    #[error("event not found: {0}")]
    NotFound(String),

    /// Backend recognized the call but does not implement it (e.g.
    /// `trigger/ack` on a fire-and-forget backend).
    #[error("not supported: {0}")]
    NotSupported(String),

    /// Request was malformed at the domain level (distinct from
    /// JSON-RPC `invalid_params` which catches wire-shape problems).
    #[error("invalid request: {0}")]
    InvalidRequest(String),

    /// Backend (or its upstream) is temporarily unavailable.
    #[error("backend unavailable: {0}")]
    Unavailable(String),

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

/// What a trigger backend plugin implements.
///
/// Trigger backends are push-driven: the runtime calls
/// [`TriggerBackend::watch`] once to open the event stream and then
/// forwards every item the stream yields as a
/// [`NOTIFICATION_TRIGGER_EVENT`] notification on the wire. Backends that
/// need delivery confirmation implement [`TriggerBackend::ack`]; backends
/// that don't can rely on the default no-op.
///
/// Unlike subject backends (which are pull-driven and read-modify-write
/// against a system-of-record) trigger backends typically maintain a live
/// connection to their upstream (a Slack socket, an HTTP listener, an
/// inotify handle, a `tokio::time::interval`, ...). The runtime spawns
/// `watch` once and drains the stream until it returns `None` or the
/// plugin process exits.
///
/// # Example
///
/// ```ignore
/// use animus_plugin_protocol::{HealthCheckResult, HealthStatus};
/// use animus_trigger_protocol::{
///     BackendError, TriggerBackend, TriggerEvent, TriggerSchema, TriggerStream,
/// };
/// use async_trait::async_trait;
/// use chrono::Utc;
/// use futures_core::stream;
///
/// pub struct CronBackend;
///
/// #[async_trait]
/// impl TriggerBackend for CronBackend {
///     fn schema(&self) -> TriggerSchema {
///         TriggerSchema {
///             kinds: vec!["cron_tick".into()],
///             supports_resume: false,
///             supports_dedup: false,
///             supports_ack: false,
///         }
///     }
///
///     async fn watch(&self) -> Result<TriggerStream, BackendError> {
///         let event = TriggerEvent {
///             id: "tick-0".into(),
///             occurred_at: Utc::now(),
///             kind: "cron_tick".into(),
///             payload: serde_json::json!({}),
///             subject_id: None,
///             action_hint: None,
///         };
///         let s = stream::iter(vec![Ok(event)]);
///         Ok(Box::pin(s))
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
pub trait TriggerBackend: Send + Sync + 'static {
    /// Capability declaration. Should be cheap to compute (preferably a
    /// constant).
    fn schema(&self) -> TriggerSchema;

    /// Open the event stream. The runtime calls this once after the
    /// `initialize` handshake and drains the returned stream for the life
    /// of the plugin connection.
    async fn watch(&self) -> Result<TriggerStream, BackendError>;

    /// Acknowledge an event id so the backend does not redeliver it.
    /// Default implementation is a no-op for backends that don't track
    /// delivery state — those backends should also set
    /// [`TriggerSchema::supports_ack`] to `false`.
    async fn ack(&self, event_id: &str) -> Result<(), BackendError> {
        let _ = event_id;
        Ok(())
    }

    /// Backend health. The daemon polls this on a schedule.
    async fn health(&self) -> Result<HealthCheckResult, BackendError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trigger_event_round_trips() {
        let event = TriggerEvent {
            id: "slack:T123/C456/1715701234.000100".into(),
            occurred_at: DateTime::parse_from_rfc3339("2026-05-14T18:20:34Z")
                .unwrap()
                .with_timezone(&Utc),
            kind: "slack_mention".into(),
            payload: serde_json::json!({"user": "U1", "text": "hi"}),
            subject_id: Some("linear:ENG-123".into()),
            action_hint: Some("run-workflow:review".into()),
        };
        let v = serde_json::to_value(&event).unwrap();
        let back: TriggerEvent = serde_json::from_value(v).unwrap();
        assert_eq!(back, event);
    }

    #[test]
    fn trigger_schema_round_trips() {
        let schema = TriggerSchema {
            kinds: vec!["cron_tick".into()],
            supports_resume: false,
            supports_dedup: false,
            supports_ack: false,
        };
        let v = serde_json::to_value(&schema).unwrap();
        let back: TriggerSchema = serde_json::from_value(v).unwrap();
        assert_eq!(back, schema);
    }

    #[test]
    fn backend_error_not_supported_maps_to_method_not_supported() {
        let rpc: RpcError = BackendError::NotSupported("trigger/ack".into()).into();
        assert_eq!(rpc.code, error_codes::METHOD_NOT_SUPPORTED);
    }
}
