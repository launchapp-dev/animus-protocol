//! `TransportBackend` trait and wire types for Animus transport plugins.
//!
//! Transport is the sixth plugin kind alongside
//! [`SubjectBackend`](animus_plugin_protocol::PLUGIN_KIND_SUBJECT_BACKEND),
//! [`ProviderBackend`](animus_plugin_protocol::PLUGIN_KIND_PROVIDER),
//! [`TriggerBackend`](animus_plugin_protocol::PLUGIN_KIND_TRIGGER_BACKEND),
//! [`LogStorageBackend`](animus_plugin_protocol::PLUGIN_KIND_CUSTOM) (log
//! storage), and the in-process control surface. A transport plugin exposes
//! an *external surface* — an HTTP server, a GraphQL endpoint, a gRPC
//! listener, a WebSocket gateway, MQTT bridge, whatever — and translates
//! inbound requests on that surface into control RPCs against the daemon's
//! Unix socket. They are the controller-as-plugin endgame: the daemon stays
//! a small JSON-RPC core, and every external surface ships as a separate,
//! independently versioned process.
//!
//! This crate defines:
//!
//! - The [`TransportConfig`] payload the daemon hands to the plugin when it
//!   spawns it (control-socket path, project root, optional bind address,
//!   transport-specific config blob).
//! - The [`TransportInfo`] reply describing the live listener
//!   (`bound_addr`, `started_at`).
//! - The [`TransportSchema`] capability declaration returned by
//!   [`TRANSPORT_METHOD_SCHEMA`].
//! - The Rust-side [`TransportBackend`] trait that plugin authors implement.
//! - The JSON-RPC method-name constants used on the wire (e.g.
//!   [`TRANSPORT_METHOD_START`]).
//! - [`BackendError`] mapping backend failures to JSON-RPC error responses.
//!
//! Plugin authors typically depend on this crate alongside
//! [`animus-plugin-runtime`], implement [`TransportBackend`] for their type,
//! and call
//! `animus_plugin_runtime::transport_backend_main(info, backend).await`
//! from `main`.

#![warn(missing_docs)]

use std::path::PathBuf;

use animus_plugin_protocol::{error_codes, HealthCheckResult, RpcError};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

// =====================================================================
// Method-name constants (the JSON-RPC wire methods)
// =====================================================================

/// `transport/start` — bind the listener and begin accepting connections.
///
/// The plugin returns [`TransportInfo`] once the listener is live; the
/// transport then runs in the background until [`TRANSPORT_METHOD_SHUTDOWN`]
/// (or stdin EOF) terminates it.
pub const TRANSPORT_METHOD_START: &str = "transport/start";

/// `transport/shutdown` — graceful shutdown: stop accepting new connections,
/// drain in-flight ones, and release the bound address.
///
/// Backends MUST be safe to call this more than once; subsequent calls
/// after a successful shutdown SHOULD be no-ops.
pub const TRANSPORT_METHOD_SHUTDOWN: &str = "transport/shutdown";

/// `transport/schema` — capability declaration; returns [`TransportSchema`].
pub const TRANSPORT_METHOD_SCHEMA: &str = "transport/schema";

/// Plugin kind constant for transport backend plugins.
pub const PLUGIN_KIND_TRANSPORT_BACKEND: &str = "transport_backend";

// =====================================================================
// Config
// =====================================================================

/// Configuration handed to a transport plugin on [`TRANSPORT_METHOD_START`].
///
/// The daemon owns the control socket and the project root; the transport
/// owns translation of its protocol (HTTP, GraphQL, gRPC, ...) into control
/// RPCs against that socket. `bind_addr` is optional so the daemon can let
/// the transport pick its own default (e.g. `127.0.0.1:8080` for HTTP,
/// `127.0.0.1:8090` for GraphQL); operators that want a specific port set
/// it explicitly in workflow YAML.
///
/// `config` is a free-form JSON blob carrying transport-specific knobs that
/// the daemon does not parse. Examples:
///
/// - HTTP transport: `{ "cors": {"allowed_origins": ["*"]}, "auth_token":
///   "..." }`
/// - GraphQL transport: `{ "introspection": false, "subscriptions": true }`
/// - gRPC transport: `{ "tls": {"cert": "...", "key": "..."} }`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TransportConfig {
    /// Absolute path to the daemon's control Unix socket. The transport
    /// connects to this socket to issue control RPCs on behalf of inbound
    /// requests. POSIX hosts only; Windows named-pipe naming is reserved
    /// for a future revision.
    pub control_socket_path: PathBuf,

    /// Absolute path to the project root the daemon is serving. Transports
    /// surface this in metadata responses (e.g. HTTP `/healthz`) and use it
    /// to scope filesystem access if they expose static-file routes.
    pub project_root: PathBuf,

    /// Address (and port) the transport should bind. Format is
    /// transport-specific; HTTP/GraphQL/gRPC use `host:port` (e.g.
    /// `"127.0.0.1:8080"`, `"[::1]:8080"`). `None` lets the plugin pick a
    /// sensible default — usually the value reported in
    /// [`TransportSchema::default_port`] on `localhost`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bind_addr: Option<String>,

    /// Transport-specific configuration. Defaults to `null` (omitted on the
    /// wire) when the operator did not supply any.
    #[serde(default, skip_serializing_if = "is_null")]
    pub config: Value,
}

fn is_null(value: &Value) -> bool {
    value.is_null()
}

// =====================================================================
// Info (start reply)
// =====================================================================

/// Reply returned by [`TransportBackend::start`] once the listener is bound.
///
/// `bound_addr` is the *actual* address the listener accepted on (after any
/// `0` port resolution) and is what the daemon advertises to operators.
/// `started_at` lets dashboards display "transport up for N minutes" without
/// the daemon tracking it separately.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TransportInfo {
    /// Address the listener actually bound to. For TCP transports this is
    /// `host:port`. For Unix-socket transports (future) this is the socket
    /// path. Daemons MAY surface this verbatim in `daemon/status` output.
    pub bound_addr: String,

    /// When the listener became ready to accept connections. UTC, RFC 3339.
    pub started_at: DateTime<Utc>,
}

// =====================================================================
// Schema / capability declaration
// =====================================================================

/// Capability declaration returned by [`TRANSPORT_METHOD_SCHEMA`].
///
/// The daemon uses this to adapt behavior without runtime guessing — for
/// example, to skip WebSocket-bound features for transports that don't
/// support them, or to pick a default port when the operator omits one.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TransportSchema {
    /// Protocol kinds this transport exposes. Convention is a single
    /// lowercase token per kind, e.g. `["http", "rest"]` for an HTTP/REST
    /// transport, `["graphql"]` for a GraphQL transport, `["grpc"]`,
    /// `["mqtt"]`, `["websocket"]`. Multi-protocol transports (e.g. one
    /// process that serves HTTP and gRPC) list every kind they expose.
    pub kinds: Vec<String>,

    /// Whether the transport supports server-streaming responses
    /// (HTTP/2 server push, gRPC server streaming, GraphQL subscriptions
    /// over SSE, ...). Hosts use this to decide whether to route streaming
    /// control methods (`daemon/events`, `daemon/logs`, `subject/watch`)
    /// through this transport.
    pub supports_streaming: bool,

    /// Whether the transport accepts WebSocket upgrades. Distinct from
    /// [`supports_streaming`] because HTTP transports may stream without
    /// supporting WebSocket and vice versa.
    pub supports_websocket: bool,

    /// Default port the transport binds to when [`TransportConfig::bind_addr`]
    /// is `None`. Hosts surface this to operators so they know where to
    /// point clients. `None` means the transport refuses to start without
    /// an explicit `bind_addr`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_port: Option<u16>,
}

// =====================================================================
// Errors
// =====================================================================

/// Errors a transport backend may return.
///
/// These map to JSON-RPC error responses via the [`From`] impl below.
/// Backend authors typically produce these directly from their trait
/// implementation; the runtime translates to wire-level [`RpcError`].
///
/// The spec refers to this conceptually as `ProtocolError`; the in-crate
/// name `BackendError` matches the convention used by the sibling
/// `animus-subject-protocol`, `animus-trigger-protocol`, and
/// `animus-log-storage-protocol` crates.
#[derive(Debug, thiserror::Error)]
pub enum BackendError {
    /// Backend recognized the call but does not implement it.
    #[error("not supported: {0}")]
    NotSupported(String),

    /// Request was malformed at the domain level — e.g. an unparseable
    /// `bind_addr`, contradictory `config` keys.
    #[error("invalid request: {0}")]
    InvalidRequest(String),

    /// The requested bind address is already in use (e.g. another process
    /// holds the port). Hosts MAY surface this directly to operators so
    /// they can pick a different port.
    #[error("address in use: {0}")]
    AddressInUse(String),

    /// The transport asked the OS for a privileged port (< 1024) without
    /// the necessary capability, or asked for a path the process cannot
    /// write to.
    #[error("permission denied: {0}")]
    PermissionDenied(String),

    /// Backend (or its upstream — the OS, a TLS provider, an external
    /// auth service) is temporarily unavailable.
    #[error("backend unavailable: {0}")]
    Unavailable(String),

    /// Anything else.
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl From<BackendError> for RpcError {
    fn from(error: BackendError) -> Self {
        match error {
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
            BackendError::AddressInUse(message) => RpcError {
                code: error_codes::INTERNAL_ERROR,
                message: format!("address in use: {message}"),
                data: Some(serde_json::json!({"category": "address_in_use"})),
            },
            BackendError::PermissionDenied(message) => RpcError {
                code: error_codes::INTERNAL_ERROR,
                message: format!("permission denied: {message}"),
                data: Some(serde_json::json!({"category": "permission_denied"})),
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

/// What a transport backend plugin implements.
///
/// Transport backends own the *external* protocol surface — HTTP, GraphQL,
/// gRPC, WebSocket, MQTT — and translate inbound requests into control
/// RPCs against the daemon's Unix socket. The runtime drives them with a
/// short, deliberately rigid lifecycle:
///
/// 1. Spawn the plugin process.
/// 2. Issue [`TRANSPORT_METHOD_START`] with the [`TransportConfig`] the
///    daemon prepared.
/// 3. Drain notifications and forward arbitrary `control/*` calls that the
///    transport translates inbound requests into (the wire shape for
///    plugin-to-daemon control calls is defined in
///    [`animus-control-protocol`](https://crates.io/crates/animus-control-protocol)).
/// 4. Issue [`TRANSPORT_METHOD_SHUTDOWN`] before terminating the process.
///
/// Unlike subject backends (which are pull-driven RPCs) and triggers
/// (which are push-driven event sources owned by the plugin), transport
/// backends are bidirectional: they accept external calls AND issue
/// internal calls. Each transport plugin owns one listener thread (or
/// async task) per protocol it exposes.
///
/// # Example
///
/// ```ignore
/// use animus_plugin_protocol::{HealthCheckResult, HealthStatus, ProtocolError};
/// use animus_transport_protocol::{
///     BackendError, TransportBackend, TransportConfig, TransportInfo, TransportSchema,
/// };
/// use async_trait::async_trait;
/// use chrono::Utc;
///
/// pub struct HttpTransport;
///
/// #[async_trait]
/// impl TransportBackend for HttpTransport {
///     async fn start(&self, config: TransportConfig) -> Result<TransportInfo, BackendError> {
///         let bind = config.bind_addr.unwrap_or_else(|| "127.0.0.1:8080".into());
///         // ...bind the listener, spawn the accept loop in the background...
///         Ok(TransportInfo {
///             bound_addr: bind,
///             started_at: Utc::now(),
///         })
///     }
///
///     async fn shutdown(&self) -> Result<(), BackendError> {
///         // ...drain in-flight requests, release the bound address...
///         Ok(())
///     }
///
///     fn schema(&self) -> TransportSchema {
///         TransportSchema {
///             kinds: vec!["http".into(), "rest".into()],
///             supports_streaming: true,
///             supports_websocket: false,
///             default_port: Some(8080),
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
pub trait TransportBackend: Send + Sync + 'static {
    /// Start the transport listener. Returns once the listener is bound and
    /// ready to accept connections; the actual accept loop runs in the
    /// background (typically a `tokio::spawn`).
    async fn start(&self, config: TransportConfig) -> Result<TransportInfo, BackendError>;

    /// Graceful shutdown. Stop accepting new connections, drain in-flight
    /// ones, and release the bound address. Backends MUST be safe to call
    /// this more than once.
    async fn shutdown(&self) -> Result<(), BackendError>;

    /// Capability declaration. Should be cheap to compute (preferably a
    /// constant).
    fn schema(&self) -> TransportSchema;

    /// Backend health. The daemon polls this on a schedule.
    async fn health(&self) -> Result<HealthCheckResult, BackendError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixed_ts() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-05-23T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    #[test]
    fn transport_config_round_trips_json_full() {
        let config = TransportConfig {
            control_socket_path: PathBuf::from("/Users/op/.animus/scope/control.sock"),
            project_root: PathBuf::from("/Users/op/code/animus"),
            bind_addr: Some("127.0.0.1:8080".into()),
            config: serde_json::json!({
                "cors": {"allowed_origins": ["*"]},
                "auth_token": "redacted"
            }),
        };
        let value = serde_json::to_value(&config).unwrap();
        let back: TransportConfig = serde_json::from_value(value).unwrap();
        assert_eq!(back, config);
    }

    #[test]
    fn transport_config_omits_defaults_on_wire() {
        let config = TransportConfig {
            control_socket_path: PathBuf::from("/tmp/control.sock"),
            project_root: PathBuf::from("/tmp/proj"),
            bind_addr: None,
            config: Value::Null,
        };
        let value = serde_json::to_value(&config).unwrap();
        assert!(
            value.get("bind_addr").is_none(),
            "bind_addr should be omitted when None"
        );
        assert!(
            value.get("config").is_none(),
            "config should be omitted when Null"
        );
    }

    #[test]
    fn transport_info_round_trips() {
        let info = TransportInfo {
            bound_addr: "127.0.0.1:8080".into(),
            started_at: fixed_ts(),
        };
        let value = serde_json::to_value(&info).unwrap();
        let back: TransportInfo = serde_json::from_value(value).unwrap();
        assert_eq!(back, info);
    }

    #[test]
    fn transport_schema_round_trips() {
        let schema = TransportSchema {
            kinds: vec!["http".into(), "rest".into()],
            supports_streaming: true,
            supports_websocket: false,
            default_port: Some(8080),
        };
        let value = serde_json::to_value(&schema).unwrap();
        let back: TransportSchema = serde_json::from_value(value).unwrap();
        assert_eq!(back, schema);
    }

    #[test]
    fn transport_schema_omits_default_port_when_none() {
        let schema = TransportSchema {
            kinds: vec!["grpc".into()],
            supports_streaming: true,
            supports_websocket: false,
            default_port: None,
        };
        let value = serde_json::to_value(&schema).unwrap();
        assert!(value.get("default_port").is_none());
    }

    #[test]
    fn backend_error_not_supported_maps_to_method_not_supported() {
        let rpc: RpcError = BackendError::NotSupported("transport/start".into()).into();
        assert_eq!(rpc.code, error_codes::METHOD_NOT_SUPPORTED);
    }

    #[test]
    fn backend_error_address_in_use_carries_category() {
        let rpc: RpcError = BackendError::AddressInUse("127.0.0.1:8080".into()).into();
        assert_eq!(
            rpc.data.unwrap()["category"],
            serde_json::json!("address_in_use")
        );
    }
}
