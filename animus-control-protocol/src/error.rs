//! Errors returned by the daemon control surface.
//!
//! [`ControlError`] is the in-process Rust representation of a failure from
//! any [`ControlSurface`](crate::ControlSurface) method. On the JSON-RPC wire
//! it maps to a standard JSON-RPC error response via the [`From`] impl below,
//! using the same `error_codes` namespace defined by
//! [`animus_plugin_protocol`].
//!
//! The shape mirrors `BackendError` in `animus-subject-protocol` and
//! `animus-trigger-protocol` so transports (CLI, MCP, WebAPI) can categorize
//! failures uniformly across the daemon's outbound *and* inbound surfaces.

use animus_plugin_protocol::{error_codes, RpcError};
use serde::{Deserialize, Serialize};

/// A typed error returned by any [`ControlSurface`](crate::ControlSurface)
/// method.
///
/// Unlike subject/trigger/log_storage `BackendError`, this enum implements
/// [`Serialize`] and [`Deserialize`] so clients can decode JSON-RPC error
/// responses and recover the categorical kind without resorting to string
/// matching on `error.message`. The discriminant is carried in
/// `error.data.category`.
#[derive(Debug, Clone, thiserror::Error, Serialize, Deserialize)]
#[serde(tag = "category", content = "message", rename_all = "snake_case")]
pub enum ControlError {
    /// The resource referenced by the request does not exist.
    #[error("not found: {0}")]
    NotFound(String),

    /// The request was malformed at the domain level (distinct from
    /// JSON-RPC `invalid_params` which catches wire-shape problems).
    #[error("invalid request: {0}")]
    InvalidRequest(String),

    /// The caller lacks permission for the requested action.
    #[error("permission denied: {0}")]
    PermissionDenied(String),

    /// The daemon (or one of its dependencies) is temporarily unavailable —
    /// e.g. a plugin process is down, the dispatch queue is paused, an
    /// upstream backend timed out.
    #[error("unavailable: {0}")]
    Unavailable(String),

    /// The control surface recognized the method but does not implement it
    /// — used during incremental rollout when an operation exists in the
    /// protocol but not yet in the daemon.
    #[error("not supported: {0}")]
    NotSupported(String),

    /// A conflict prevented the operation — e.g. attempting to cancel a
    /// workflow that's already completed.
    #[error("conflict: {0}")]
    Conflict(String),

    /// Catch-all for failures that don't fit the other categories.
    #[error("internal: {0}")]
    Internal(String),
}

impl ControlError {
    /// The stable category string used on the wire in `error.data.category`.
    ///
    /// Provided as a free function so callers can branch on the category
    /// without re-implementing the serde tag mapping.
    pub fn category(&self) -> &'static str {
        match self {
            ControlError::NotFound(_) => "not_found",
            ControlError::InvalidRequest(_) => "invalid_request",
            ControlError::PermissionDenied(_) => "permission_denied",
            ControlError::Unavailable(_) => "unavailable",
            ControlError::NotSupported(_) => "not_supported",
            ControlError::Conflict(_) => "conflict",
            ControlError::Internal(_) => "internal",
        }
    }
}

impl From<ControlError> for RpcError {
    fn from(error: ControlError) -> Self {
        let (code, data_category) = match &error {
            ControlError::NotFound(_) => (error_codes::INVALID_PARAMS, "not_found"),
            ControlError::InvalidRequest(_) => (error_codes::INVALID_PARAMS, "invalid_request"),
            ControlError::PermissionDenied(_) => {
                (error_codes::INVALID_REQUEST, "permission_denied")
            }
            ControlError::Unavailable(_) => (error_codes::INTERNAL_ERROR, "unavailable"),
            ControlError::NotSupported(_) => (error_codes::METHOD_NOT_SUPPORTED, "not_supported"),
            ControlError::Conflict(_) => (error_codes::INVALID_REQUEST, "conflict"),
            ControlError::Internal(_) => (error_codes::INTERNAL_ERROR, "internal"),
        };
        RpcError {
            code,
            message: error.to_string(),
            data: Some(serde_json::json!({ "category": data_category })),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_error_maps_categories_to_rpc_codes() {
        let cases = [
            (
                ControlError::NotFound("x".into()),
                error_codes::INVALID_PARAMS,
                "not_found",
            ),
            (
                ControlError::InvalidRequest("x".into()),
                error_codes::INVALID_PARAMS,
                "invalid_request",
            ),
            (
                ControlError::PermissionDenied("x".into()),
                error_codes::INVALID_REQUEST,
                "permission_denied",
            ),
            (
                ControlError::Unavailable("x".into()),
                error_codes::INTERNAL_ERROR,
                "unavailable",
            ),
            (
                ControlError::NotSupported("x".into()),
                error_codes::METHOD_NOT_SUPPORTED,
                "not_supported",
            ),
            (
                ControlError::Conflict("x".into()),
                error_codes::INVALID_REQUEST,
                "conflict",
            ),
            (
                ControlError::Internal("x".into()),
                error_codes::INTERNAL_ERROR,
                "internal",
            ),
        ];
        for (err, expected_code, expected_category) in cases {
            let category = err.category();
            let rpc: RpcError = err.into();
            assert_eq!(rpc.code, expected_code, "code for {category}");
            assert_eq!(
                rpc.data.unwrap().get("category").unwrap().as_str().unwrap(),
                expected_category,
            );
        }
    }

    #[test]
    fn control_error_serializes_compactly() {
        let err = ControlError::NotFound("subject linear:ENG-1".into());
        let v = serde_json::to_value(&err).unwrap();
        assert_eq!(
            v,
            serde_json::json!({
                "category": "not_found",
                "message": "subject linear:ENG-1"
            })
        );
        let back: ControlError = serde_json::from_value(v).unwrap();
        assert!(matches!(back, ControlError::NotFound(s) if s == "subject linear:ENG-1"));
    }
}
