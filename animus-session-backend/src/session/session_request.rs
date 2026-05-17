use serde_json::Value;
use std::path::PathBuf;

/// Inputs for starting (or resuming) a session.
#[derive(Debug, Clone, PartialEq)]
pub struct SessionRequest {
    /// Tool name (e.g. `"claude"`, `"codex"`, `"gemini"`, `"opencode"`).
    pub tool: String,
    /// Model identifier passed through to the wrapped CLI.
    pub model: String,
    /// User prompt for this turn.
    pub prompt: String,
    /// Working directory for the child process.
    pub cwd: PathBuf,
    /// Project root, if distinct from `cwd`.
    pub project_root: Option<PathBuf>,
    /// MCP endpoint URL when the wrapped CLI bridges MCP.
    pub mcp_endpoint: Option<String>,
    /// Permission-mode hint passed to the wrapped CLI.
    pub permission_mode: Option<String>,
    /// Hard timeout in seconds.
    pub timeout_secs: Option<u64>,
    /// Environment variables to inject into the child.
    pub env_vars: Vec<(String, String)>,
    /// Backend-specific extras — including the `runtime_contract` envelope that
    /// can override the entire launch invocation.
    pub extras: Value,
}
