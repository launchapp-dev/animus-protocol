use async_trait::async_trait;

use crate::error::Result;

use super::{
    session_backend_info::SessionBackendInfo, session_capabilities::SessionCapabilities,
    session_request::SessionRequest, session_run::SessionRun,
};

/// The contract every CLI-wrapping session backend implements.
///
/// A provider plugin typically holds one or more `SessionBackend` instances
/// behind a resolver, and forwards `agent/run` / `agent/resume` /
/// `agent/cancel` calls to the matching backend.
#[async_trait]
pub trait SessionBackend: Send + Sync {
    /// Static metadata describing this backend.
    fn info(&self) -> SessionBackendInfo;

    /// Capability flags the backend honors.
    fn capabilities(&self) -> SessionCapabilities;

    /// Start a fresh session.
    async fn start_session(&self, request: SessionRequest) -> Result<SessionRun>;

    /// Resume a previously-started session by id.
    async fn resume_session(&self, request: SessionRequest, session_id: &str)
        -> Result<SessionRun>;

    /// Cancel an in-flight session by id.
    async fn terminate_session(&self, session_id: &str) -> Result<()>;
}
