use async_trait::async_trait;

use crate::error::Result;
use crate::session::{
    session_backend::SessionBackend, session_backend_info::SessionBackendInfo,
    session_backend_kind::SessionBackendKind, session_capabilities::SessionCapabilities,
    session_request::SessionRequest, session_run::SessionRun, session_stability::SessionStability,
};

use super::transport::{start_gemini_session, terminate_gemini_session};

/// Native Google Gemini (`gemini`) session backend.
pub struct GeminiSessionBackend;

impl GeminiSessionBackend {
    /// Construct a fresh backend instance.
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl SessionBackend for GeminiSessionBackend {
    fn info(&self) -> SessionBackendInfo {
        SessionBackendInfo {
            kind: SessionBackendKind::GeminiSdk,
            provider_tool: "gemini".to_string(),
            stability: SessionStability::Experimental,
            display_name: "Gemini Native Backend".to_string(),
        }
    }

    fn capabilities(&self) -> SessionCapabilities {
        SessionCapabilities {
            supports_resume: true,
            supports_terminate: true,
            supports_permissions: true,
            supports_mcp: true,
            supports_tool_events: false,
            supports_thinking_events: false,
            supports_artifact_events: false,
            supports_usage_metadata: true,
        }
    }

    async fn start_session(&self, request: SessionRequest) -> Result<SessionRun> {
        start_gemini_session(request, None).await
    }

    async fn resume_session(&self, request: SessionRequest, session_id: &str) -> Result<SessionRun> {
        start_gemini_session(request, Some(session_id.to_string())).await
    }

    async fn terminate_session(&self, session_id: &str) -> Result<()> {
        terminate_gemini_session(session_id).await
    }
}

impl Default for GeminiSessionBackend {
    fn default() -> Self {
        Self::new()
    }
}
