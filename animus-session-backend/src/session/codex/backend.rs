use async_trait::async_trait;

use crate::error::Result;

use super::transport::{start_codex_session, terminate_codex_session};
use crate::session::{
    session_backend::SessionBackend, session_backend_info::SessionBackendInfo,
    session_backend_kind::SessionBackendKind, session_capabilities::SessionCapabilities,
    session_request::SessionRequest, session_run::SessionRun, session_stability::SessionStability,
};

/// Native OpenAI Codex (`codex`) session backend.
pub struct CodexSessionBackend;

impl CodexSessionBackend {
    /// Construct a fresh backend instance.
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl SessionBackend for CodexSessionBackend {
    fn info(&self) -> SessionBackendInfo {
        SessionBackendInfo {
            kind: SessionBackendKind::CodexSdk,
            provider_tool: "codex".to_string(),
            stability: SessionStability::Experimental,
            display_name: "Codex Native Backend".to_string(),
        }
    }

    fn capabilities(&self) -> SessionCapabilities {
        SessionCapabilities {
            supports_resume: true,
            supports_terminate: true,
            supports_permissions: true,
            supports_mcp: true,
            supports_tool_events: false,
            supports_thinking_events: true,
            supports_artifact_events: false,
            supports_usage_metadata: true,
        }
    }

    async fn start_session(&self, request: SessionRequest) -> Result<SessionRun> {
        start_codex_session(request, None).await
    }

    async fn resume_session(&self, request: SessionRequest, session_id: &str) -> Result<SessionRun> {
        start_codex_session(request, Some(session_id)).await
    }

    async fn terminate_session(&self, session_id: &str) -> Result<()> {
        terminate_codex_session(session_id).await
    }
}

impl Default for CodexSessionBackend {
    fn default() -> Self {
        Self::new()
    }
}
