use async_trait::async_trait;

use crate::error::Result;

use super::transport::{start_claude_session, terminate_claude_session};
use crate::session::{
    session_backend::SessionBackend, session_backend_info::SessionBackendInfo,
    session_backend_kind::SessionBackendKind, session_capabilities::SessionCapabilities,
    session_request::SessionRequest, session_run::SessionRun, session_stability::SessionStability,
};

/// Native Claude Code (`claude`) session backend.
///
/// Spawns the `claude` CLI with `--print --verbose --output-format stream-json`
/// and parses its JSON event stream into [`crate::SessionEvent`]s.
pub struct ClaudeSessionBackend;

impl ClaudeSessionBackend {
    /// Construct a fresh backend instance.
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl SessionBackend for ClaudeSessionBackend {
    fn info(&self) -> SessionBackendInfo {
        SessionBackendInfo {
            kind: SessionBackendKind::ClaudeSdk,
            provider_tool: "claude".to_string(),
            stability: SessionStability::Experimental,
            display_name: "Claude Native Backend".to_string(),
        }
    }

    fn capabilities(&self) -> SessionCapabilities {
        SessionCapabilities {
            supports_resume: true,
            supports_terminate: true,
            supports_permissions: true,
            supports_mcp: true,
            supports_tool_events: true,
            supports_thinking_events: true,
            supports_artifact_events: false,
            supports_usage_metadata: true,
        }
    }

    async fn start_session(&self, request: SessionRequest) -> Result<SessionRun> {
        start_claude_session(request, None).await
    }

    async fn resume_session(
        &self,
        request: SessionRequest,
        session_id: &str,
    ) -> Result<SessionRun> {
        start_claude_session(request, Some(session_id.to_string())).await
    }

    async fn terminate_session(&self, session_id: &str) -> Result<()> {
        terminate_claude_session(session_id).await
    }
}

impl Default for ClaudeSessionBackend {
    fn default() -> Self {
        Self::new()
    }
}
