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

pub use claude::ClaudeSessionBackend;
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
