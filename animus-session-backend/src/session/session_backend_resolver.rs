use std::sync::Arc;

use super::{
    claude::ClaudeSessionBackend, codex::CodexSessionBackend, gemini::GeminiSessionBackend,
    oai_runner::OaiRunnerSessionBackend, opencode::OpenCodeSessionBackend, session_backend::SessionBackend,
    session_request::SessionRequest, session_run::SessionRun, subprocess_session_backend::SubprocessSessionBackend,
};
use crate::error::Result;

/// Picks the right [`SessionBackend`] for an incoming [`SessionRequest`].
///
/// The resolver knows about the five native CLI backends shipped with this
/// crate (claude, codex, gemini, opencode, oai-runner) and falls back to
/// [`SubprocessSessionBackend`] for any other tool name.
pub struct SessionBackendResolver {
    claude: Arc<ClaudeSessionBackend>,
    codex: Arc<CodexSessionBackend>,
    gemini: Arc<GeminiSessionBackend>,
    opencode: Arc<OpenCodeSessionBackend>,
    oai_runner: Arc<OaiRunnerSessionBackend>,
    subprocess: Arc<SubprocessSessionBackend>,
}

impl SessionBackendResolver {
    /// Construct a resolver with every native backend pre-instantiated.
    pub fn new() -> Self {
        Self {
            claude: Arc::new(ClaudeSessionBackend::new()),
            codex: Arc::new(CodexSessionBackend::new()),
            gemini: Arc::new(GeminiSessionBackend::new()),
            opencode: Arc::new(OpenCodeSessionBackend::new()),
            oai_runner: Arc::new(OaiRunnerSessionBackend::new()),
            subprocess: Arc::new(SubprocessSessionBackend::new()),
        }
    }

    /// Human-readable explanation when the resolver falls back to the
    /// subprocess backend for an unrecognized tool. `None` if a native
    /// backend was chosen.
    pub fn fallback_reason(&self, request: &SessionRequest) -> Option<String> {
        if request.tool.eq_ignore_ascii_case("claude")
            || request.tool.eq_ignore_ascii_case("codex")
            || request.tool.eq_ignore_ascii_case("gemini")
            || request.tool.eq_ignore_ascii_case("opencode")
            || request.tool.eq_ignore_ascii_case("oai-runner")
            || request.tool.eq_ignore_ascii_case("animus-oai-runner")
        {
            return None;
        }

        Some(format!("native backend not implemented for tool '{}'; using subprocess backend", request.tool))
    }

    /// Pick the backend that will service `request`.
    pub fn resolve(&self, request: &SessionRequest) -> Arc<dyn SessionBackend> {
        if request.tool.eq_ignore_ascii_case("claude") {
            return self.claude.clone();
        }
        if request.tool.eq_ignore_ascii_case("codex") {
            return self.codex.clone();
        }
        if request.tool.eq_ignore_ascii_case("gemini") {
            return self.gemini.clone();
        }
        if request.tool.eq_ignore_ascii_case("opencode") {
            return self.opencode.clone();
        }
        if request.tool.eq_ignore_ascii_case("oai-runner") || request.tool.eq_ignore_ascii_case("animus-oai-runner") {
            return self.oai_runner.clone();
        }

        self.subprocess.clone()
    }

    /// Convenience: resolve + start in one call. Stamps the resolver-derived
    /// fallback reason into `request.extras` so the subprocess backend can
    /// surface it as `SessionRun::fallback_reason`.
    pub async fn start_session(&self, mut request: SessionRequest) -> Result<SessionRun> {
        if let Some(reason) = self.fallback_reason(&request) {
            if let Some(extras) = request.extras.as_object_mut() {
                extras.insert("fallback_reason".to_string(), serde_json::Value::String(reason));
            }
        }

        self.resolve(&request).start_session(request).await
    }
}

impl Default for SessionBackendResolver {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use std::path::PathBuf;

    use super::SessionBackendResolver;
    use crate::session::SessionRequest;

    fn req(tool: &str) -> SessionRequest {
        SessionRequest {
            tool: tool.to_string(),
            model: String::new(),
            prompt: "hello".to_string(),
            cwd: PathBuf::from("."),
            project_root: None,
            mcp_endpoint: None,
            permission_mode: None,
            timeout_secs: None,
            env_vars: Vec::new(),
            extras: json!({}),
        }
    }

    #[test]
    fn resolver_reports_subprocess_fallback_reason() {
        let resolver = SessionBackendResolver::new();
        let reason = resolver.fallback_reason(&req("sh")).expect("fallback reason should exist");
        assert!(reason.contains("using subprocess backend"));
    }

    #[test]
    fn resolver_selects_native_backends_without_fallback() {
        let resolver = SessionBackendResolver::new();
        for tool in ["claude", "codex", "gemini", "opencode", "oai-runner", "animus-oai-runner"] {
            assert!(resolver.fallback_reason(&req(tool)).is_none(), "tool {tool} should not need fallback");
        }
    }
}
