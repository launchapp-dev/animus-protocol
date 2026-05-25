use std::path::PathBuf;

use async_trait::async_trait;

use crate::error::Result;
use crate::session::{
    session_backend::SessionBackend, session_backend_info::SessionBackendInfo,
    session_backend_kind::SessionBackendKind, session_capabilities::SessionCapabilities,
    session_request::SessionRequest, session_run::SessionRun, session_stability::SessionStability,
};

use super::transport::{start_oai_runner_session, terminate_oai_runner_session};

/// Native Animus OAI-compatible runner session backend.
#[derive(Debug, Clone, Default)]
pub struct OaiRunnerSessionBackend {
    runner_binary_path: Option<PathBuf>,
}

impl OaiRunnerSessionBackend {
    /// Construct a fresh backend instance that resolves the runner via
    /// the `ANIMUS_OAI_RUNNER_BIN` environment variable (when set) and
    /// falls back to `animus-oai-runner` on `PATH`.
    pub fn new() -> Self {
        Self {
            runner_binary_path: None,
        }
    }

    /// Construct a backend pinned to a specific runner binary on disk.
    ///
    /// When set, this path takes precedence over both the
    /// `ANIMUS_OAI_RUNNER_BIN` environment variable and the default
    /// `animus-oai-runner` PATH lookup. Pass `None` to clear the override
    /// and restore env/PATH resolution.
    pub fn with_runner_binary_path(mut self, path: Option<PathBuf>) -> Self {
        self.runner_binary_path = path;
        self
    }

    /// Currently configured runner binary override, if any.
    pub fn runner_binary_path(&self) -> Option<&PathBuf> {
        self.runner_binary_path.as_ref()
    }
}

#[async_trait]
impl SessionBackend for OaiRunnerSessionBackend {
    fn info(&self) -> SessionBackendInfo {
        SessionBackendInfo {
            kind: SessionBackendKind::OaiRunnerSdk,
            provider_tool: "oai-runner".to_string(),
            stability: SessionStability::Experimental,
            display_name: "Animus OAI Runner Native Backend".to_string(),
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
            supports_usage_metadata: false,
        }
    }

    async fn start_session(&self, request: SessionRequest) -> Result<SessionRun> {
        start_oai_runner_session(request, None, self.runner_binary_path.clone()).await
    }

    async fn resume_session(
        &self,
        request: SessionRequest,
        session_id: &str,
    ) -> Result<SessionRun> {
        start_oai_runner_session(
            request,
            Some(session_id.to_string()),
            self.runner_binary_path.clone(),
        )
        .await
    }

    async fn terminate_session(&self, session_id: &str) -> Result<()> {
        terminate_oai_runner_session(session_id).await
    }
}
