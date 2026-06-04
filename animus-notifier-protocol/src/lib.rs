//! Wire types for Animus notifier plugins.
//!
//! Notifiers are the outbound counterpart to triggers: a trigger plugin
//! converts an external event into a daemon event; a notifier plugin
//! takes a daemon event record and forwards it to an external system
//! (HTTP webhook, Slack, email, PagerDuty, ...).
//!
//! The kernel publishes daemon events; this crate defines the wire shape
//! the daemon uses to hand each event to every installed notifier plugin.
//! Notifier plugins are advisory: the daemon does not block on them, and
//! the daemon refuses to start without a notifier plugin only if an
//! operator explicitly wires that policy. The default daemon policy
//! treats `notifier` as an optional role.
//!
//! Plugin authors typically depend on this crate alongside
//! [`animus-plugin-runtime`], register the [`METHOD_NOTIFIER_NOTIFY`]
//! handler (and optionally [`METHOD_NOTIFIER_FLUSH`]) on a `Plugin`
//! builder, and run it.

#![warn(missing_docs)]

use animus_plugin_protocol::{error_codes, RpcError};
use serde::{Deserialize, Serialize};
use serde_json::Value;

// =====================================================================
// Plugin-kind wire literal
// =====================================================================

/// Plugin-kind wire literal for notifier plugins.
///
/// Plugin manifests (`plugin.toml`) and discovery filters compare against
/// this exact string.
pub const PLUGIN_KIND_NOTIFIER: &str = "notifier";

// =====================================================================
// Method-name constants
// =====================================================================

/// `notifier/notify` — hand one [`NotifierNotifyParams`] payload to the
/// plugin. The plugin SHOULD enqueue and best-effort flush; backends
/// that need to retry MUST persist their outbox internally.
pub const METHOD_NOTIFIER_NOTIFY: &str = "notifier/notify";

/// `notifier/flush` — request that the plugin drain any pending
/// deliveries from its internal outbox. Optional: backends without
/// background retry MAY return [`error_codes::METHOD_NOT_SUPPORTED`].
pub const METHOD_NOTIFIER_FLUSH: &str = "notifier/flush";

/// `notifier/schema` — capability declaration; returns
/// [`NotifierSchema`].
pub const METHOD_NOTIFIER_SCHEMA: &str = "notifier/schema";

// =====================================================================
// Request / response shapes
// =====================================================================

/// Wire shape of one daemon event record forwarded to notifiers.
///
/// Mirrors `protocol::DaemonEventRecord` from `animus-cli` so the daemon
/// can hand its native event record over the wire without translation.
/// Kept in this crate (rather than imported from the main protocol
/// crate) so notifier plugin authors only need to depend on this crate
/// + `animus-plugin-runtime`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonEventRecord {
    /// Schema URI for the event payload (e.g. `"animus.daemon-event.v1"`).
    pub schema: String,
    /// Globally-unique event id.
    pub id: String,
    /// Monotonic sequence number assigned by the daemon for this run.
    #[serde(default)]
    pub seq: u64,
    /// RFC3339 timestamp the daemon stamped at emission.
    pub timestamp: String,
    /// Event kind (e.g. `"workflow_completed"`, `"task-state-change"`).
    pub event_type: String,
    /// Optional project root path this event is about.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_root: Option<String>,
    /// Free-form event payload.
    pub data: Value,
}

/// Parameters for [`METHOD_NOTIFIER_NOTIFY`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotifierNotifyParams {
    /// One daemon event record to forward.
    pub event: DaemonEventRecord,
}

/// Result for [`METHOD_NOTIFIER_NOTIFY`].
///
/// `accepted` reports whether the plugin took ownership of the event for
/// at-least-one configured connector. `delivered` reports the number of
/// successful synchronous deliveries (useful for telemetry; backends that
/// only enqueue MUST set this to `0`). `lifecycle_events` carries
/// best-effort lifecycle reporting (enqueued / sent / failed /
/// dead-lettered) that the daemon can fan out into `events.jsonl` for
/// operator visibility.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct NotifierNotifyResult {
    /// `true` iff at least one connector accepted the event for delivery.
    pub accepted: bool,
    /// Number of deliveries the plugin completed synchronously.
    #[serde(default)]
    pub delivered: u32,
    /// Optional lifecycle records (delivery-enqueued / sent / failed /
    /// dead-lettered) the daemon can fan out into `events.jsonl`.
    #[serde(default)]
    pub lifecycle_events: Vec<NotifierLifecycleEvent>,
}

/// Lifecycle record emitted by a notifier plugin so the daemon can mirror
/// it into operator-visible logs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotifierLifecycleEvent {
    /// Event-type label, e.g. `"notification-delivery-enqueued"`.
    pub event_type: String,
    /// Project root the underlying event belonged to, if known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_root: Option<String>,
    /// Free-form payload mirrored verbatim into `DaemonEventRecord.data`.
    pub data: Value,
}

/// Parameters for [`METHOD_NOTIFIER_FLUSH`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct NotifierFlushParams {
    /// Optional project-root scoping. `None` means flush every project
    /// the plugin tracks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_root: Option<String>,
}

/// Result for [`METHOD_NOTIFIER_FLUSH`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct NotifierFlushResult {
    /// Lifecycle records produced by the flush. Same shape as
    /// [`NotifierNotifyResult::lifecycle_events`].
    #[serde(default)]
    pub lifecycle_events: Vec<NotifierLifecycleEvent>,
}

/// Capability declaration returned by [`METHOD_NOTIFIER_SCHEMA`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct NotifierSchema {
    /// Free-form connector kinds this plugin can route to (e.g.
    /// `["webhook", "slack_webhook"]`). Workflows may use this to
    /// surface which transports are available.
    pub connector_kinds: Vec<String>,
    /// Whether the plugin maintains its own outbox + background retry
    /// loop. When `true`, the daemon SHOULD call [`METHOD_NOTIFIER_FLUSH`]
    /// on its tick boundary; when `false`, the daemon should skip flush.
    pub supports_flush: bool,
}

// =====================================================================
// Errors
// =====================================================================

/// Errors a notifier backend may return.
#[derive(Debug, thiserror::Error)]
pub enum NotifierBackendError {
    /// Backend recognized the call but does not implement it (e.g.
    /// `notifier/flush` on a fire-and-forget plugin).
    #[error("not supported: {0}")]
    NotSupported(String),

    /// Request was malformed at the domain level.
    #[error("invalid request: {0}")]
    InvalidRequest(String),

    /// Backend (or its upstream) is temporarily unavailable.
    #[error("backend unavailable: {0}")]
    Unavailable(String),

    /// Anything else.
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl From<NotifierBackendError> for RpcError {
    fn from(error: NotifierBackendError) -> Self {
        match error {
            NotifierBackendError::NotSupported(message) => RpcError {
                code: error_codes::METHOD_NOT_SUPPORTED,
                message,
                data: Some(serde_json::json!({"category": "not_supported"})),
            },
            NotifierBackendError::InvalidRequest(message) => RpcError {
                code: error_codes::INVALID_PARAMS,
                message,
                data: Some(serde_json::json!({"category": "invalid_request"})),
            },
            NotifierBackendError::Unavailable(message) => RpcError {
                code: error_codes::INTERNAL_ERROR,
                message: format!("backend unavailable: {message}"),
                data: Some(serde_json::json!({"category": "unavailable"})),
            },
            NotifierBackendError::Other(error) => RpcError {
                code: error_codes::INTERNAL_ERROR,
                message: error.to_string(),
                data: Some(serde_json::json!({"category": "other"})),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn notify_params_round_trip() {
        let params = NotifierNotifyParams {
            event: DaemonEventRecord {
                schema: "animus.daemon-event.v1".into(),
                id: "evt-1".into(),
                seq: 7,
                timestamp: "2026-05-31T00:00:00Z".into(),
                event_type: "workflow_completed".into(),
                project_root: Some("/repo".into()),
                data: serde_json::json!({"workflow_id": "wf-1"}),
            },
        };
        let v = serde_json::to_value(&params).unwrap();
        let back: NotifierNotifyParams = serde_json::from_value(v).unwrap();
        assert_eq!(back, params);
    }

    #[test]
    fn notify_result_defaults_are_empty() {
        let v = serde_json::to_value(NotifierNotifyResult::default()).unwrap();
        let back: NotifierNotifyResult = serde_json::from_value(v).unwrap();
        assert!(!back.accepted);
        assert_eq!(back.delivered, 0);
        assert!(back.lifecycle_events.is_empty());
    }

    #[test]
    fn schema_round_trip() {
        let s = NotifierSchema {
            connector_kinds: vec!["webhook".into(), "slack_webhook".into()],
            supports_flush: true,
        };
        let v = serde_json::to_value(&s).unwrap();
        let back: NotifierSchema = serde_json::from_value(v).unwrap();
        assert_eq!(back, s);
    }

    #[test]
    fn backend_error_not_supported_maps_to_method_not_supported() {
        let rpc: RpcError = NotifierBackendError::NotSupported("notifier/flush".into()).into();
        assert_eq!(rpc.code, error_codes::METHOD_NOT_SUPPORTED);
    }
}
