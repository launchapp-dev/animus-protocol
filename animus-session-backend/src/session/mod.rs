//! Session backends and the resolver that picks between them.

pub mod claude;
pub mod codex;
pub mod gemini;
pub mod oai_runner;
pub mod opencode;
pub mod session_backend;
pub mod session_backend_info;
pub mod session_backend_kind;
pub mod session_backend_resolver;
pub mod session_capabilities;
pub mod session_event;
pub mod session_request;
pub mod session_run;
pub mod session_stability;
pub mod subprocess_session_backend;

/// Instruction block prepended to the prompt for transports without a
/// native permission-prompt hook (codex / gemini / opencode) when the
/// request carries `extras.approvals == true`. Directs the agent to route
/// sensitive actions and blocking questions through the kernel-hosted
/// Animus MCP tools. Compliance is voluntary for these CLIs — unlike the
/// claude transport, which enforces approvals through
/// `--permission-prompt-tool` instead of this preamble.
pub const APPROVALS_PROMPT_PREAMBLE: &str = "\
Human-in-the-loop approvals are ENFORCED for this session.
- Before any destructive or irreversible action (deleting data, force-pushing,
  rewriting history, publishing, spending money), call the
  `animus.agent.request_approval` MCP tool and proceed only on an \"allow\" decision.
- For blocking questions you cannot resolve yourself, call `animus.agent.ask`.
- Decisions and answers come from a human operator and may take time; wait for
  the tool result instead of assuming an outcome.";

/// True when the request opts into human-in-the-loop approvals via
/// `extras.approvals == true`. Any other shape (absent key, `false`,
/// non-boolean) leaves the transport behavior byte-identical to a request
/// without the key.
pub(crate) fn approvals_enabled(request: &session_request::SessionRequest) -> bool {
    request
        .extras
        .get("approvals")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
}

/// Prepend [`APPROVALS_PROMPT_PREAMBLE`] to the request prompt when
/// approvals are enabled. Mutating `request.prompt` covers both prompt
/// delivery paths — the argv positional prompt and `prompt_via_stdin`
/// runtime-contract launches. A runtime contract that embeds its prompt
/// directly in `args` (leaving `request.prompt` unused) is not rewritten.
/// No-op when approvals are not enabled or the prompt is empty.
pub(crate) fn apply_approvals_prompt_preamble(request: &mut session_request::SessionRequest) {
    if !approvals_enabled(request) || request.prompt.is_empty() {
        return;
    }
    request.prompt = format!("{APPROVALS_PROMPT_PREAMBLE}\n\n{}", request.prompt);
}

/// Kill and reap a child process, attempting to terminate the entire process
/// group on Unix so detached descendants are cleaned up too.
pub(crate) async fn kill_and_reap_child(child: &mut tokio::process::Child) {
    #[cfg(unix)]
    if let Some(pid) = child.id() {
        let _ = std::process::Command::new("kill")
            .args(["-9", &format!("-{}", pid)])
            .output();
    }
    #[cfg(not(unix))]
    let _ = child.kill().await;
    let _ = child.wait().await;
}

/// Write `contents` to a fresh file readable only by the current user
/// (mode `0600` on Unix) so secret-bearing per-run config is not exposed
/// to other local users via the shared temp dir.
pub(crate) fn write_private_file(path: &std::path::Path, contents: &str) -> std::io::Result<()> {
    use std::io::Write;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)?.write_all(contents.as_bytes())
}

/// Owns a secret-bearing temp file and removes it on drop, so every exit
/// path (spawn failure, invocation-parse error, normal completion) cleans
/// the file up without per-path bookkeeping.
pub(crate) struct PrivateFileGuard {
    path: std::path::PathBuf,
}

impl PrivateFileGuard {
    pub(crate) fn new(path: std::path::PathBuf) -> Self {
        Self { path }
    }

    pub(crate) fn path(&self) -> &std::path::Path {
        &self.path
    }
}

impl Drop for PrivateFileGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

pub use claude::{ClaudeSessionBackend, CLAUDE_PERMISSION_PROMPT_TOOL};
pub use codex::CodexSessionBackend;
pub use gemini::GeminiSessionBackend;
pub use oai_runner::OaiRunnerSessionBackend;
pub use opencode::OpenCodeSessionBackend;
pub use session_backend::SessionBackend;
pub use session_backend_info::SessionBackendInfo;
pub use session_backend_kind::SessionBackendKind;
pub use session_backend_resolver::SessionBackendResolver;
pub use session_capabilities::SessionCapabilities;
pub use session_event::SessionEvent;
pub use session_request::SessionRequest;
pub use session_run::SessionRun;
pub use session_stability::SessionStability;
pub use subprocess_session_backend::SubprocessSessionBackend;

#[cfg(test)]
mod approvals_tests {
    use super::*;
    use serde_json::json;
    use std::path::PathBuf;

    fn request_with_extras(extras: serde_json::Value) -> SessionRequest {
        SessionRequest {
            tool: "codex".into(),
            model: "test-model".into(),
            prompt: "say hi".into(),
            cwd: PathBuf::from("."),
            project_root: None,
            mcp_endpoint: None,
            mcp_servers: None,
            permission_mode: None,
            timeout_secs: None,
            env_vars: Vec::new(),
            extras,
            actor: None,
        }
    }

    #[test]
    fn approvals_enabled_only_for_boolean_true() {
        assert!(approvals_enabled(&request_with_extras(
            json!({ "approvals": true })
        )));
        assert!(!approvals_enabled(&request_with_extras(json!({}))));
        assert!(!approvals_enabled(&request_with_extras(
            json!({ "approvals": false })
        )));
        assert!(!approvals_enabled(&request_with_extras(
            json!({ "approvals": "true" })
        )));
    }

    #[test]
    fn preamble_is_prepended_when_approvals_enabled() {
        let mut request = request_with_extras(json!({ "approvals": true }));
        apply_approvals_prompt_preamble(&mut request);
        assert_eq!(
            request.prompt,
            format!("{APPROVALS_PROMPT_PREAMBLE}\n\nsay hi"),
            "the preamble must come first so it reads as session-level instruction"
        );
    }

    #[test]
    fn absent_or_false_approvals_leave_prompt_byte_identical() {
        for extras in [json!({}), json!({ "approvals": false })] {
            let mut request = request_with_extras(extras);
            apply_approvals_prompt_preamble(&mut request);
            assert_eq!(request.prompt, "say hi");
        }
    }

    #[test]
    fn empty_prompt_is_not_rewritten() {
        let mut request = request_with_extras(json!({ "approvals": true }));
        request.prompt = String::new();
        apply_approvals_prompt_preamble(&mut request);
        assert!(
            request.prompt.is_empty(),
            "a contract-embedded prompt (empty request.prompt) must not grow a dangling preamble"
        );
    }
}
