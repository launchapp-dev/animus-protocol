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
    /// MCP server configs keyed by server name. Each value is an
    /// `.mcp.json`-style entry — stdio: `{"command", "args", "env"}`;
    /// remote: `{"type": "http"|"sse", "url", "headers"}`.
    pub mcp_servers: Option<Value>,
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

impl SessionRequest {
    /// The `mcp_servers` payload as a non-empty JSON object, if present.
    pub(crate) fn mcp_servers_object(&self) -> Option<&serde_json::Map<String, Value>> {
        self.mcp_servers
            .as_ref()
            .and_then(Value::as_object)
            .filter(|servers| !servers.is_empty())
    }
}
