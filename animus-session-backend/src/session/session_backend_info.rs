use super::{session_backend_kind::SessionBackendKind, session_stability::SessionStability};

/// Static metadata describing a session backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionBackendInfo {
    /// Discriminator identifying which backend variant.
    pub kind: SessionBackendKind,
    /// Canonical tool name (`"claude"`, `"codex"`, ...).
    pub provider_tool: String,
    /// Stability tier (stable vs experimental).
    pub stability: SessionStability,
    /// Human-readable display name.
    pub display_name: String,
}
