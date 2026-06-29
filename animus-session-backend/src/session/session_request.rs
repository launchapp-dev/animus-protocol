use animus_actor::Actor;
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
    /// Transport-asserted caller identity, relayed verbatim from the workflow
    /// runner so the provider session (and any MCP tools it bridges) can scope
    /// to the user. `None` for system-initiated sessions with no actor.
    ///
    /// NOTE: [`SessionRequest`] is an in-process type with no serde derives, so
    /// this is a plain field (no `#[serde]` back-compat attribute). The actor
    /// reaches the wire via the serde-derived request types upstream
    /// (`animus-control-protocol`, `animus-workflow-runner-protocol`).
    pub actor: Option<Actor>,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> SessionRequest {
        SessionRequest {
            tool: "claude".into(),
            model: "m".into(),
            prompt: "p".into(),
            cwd: PathBuf::from("."),
            project_root: None,
            mcp_endpoint: None,
            mcp_servers: None,
            permission_mode: None,
            timeout_secs: None,
            env_vars: Vec::new(),
            extras: Value::Null,
            actor: None,
        }
    }

    #[test]
    fn actor_defaults_to_none_and_carries_when_set() {
        // SessionRequest is an in-process type (no serde derives); this proves
        // the additive `actor` field threads through construction + clone + eq.
        let none = base();
        assert!(none.actor.is_none());

        let with_actor = SessionRequest {
            actor: Some(Actor::new("u-1")),
            ..base()
        };
        assert_eq!(
            with_actor.actor.as_ref().map(|a| a.user_id.as_str()),
            Some("u-1")
        );
        assert_eq!(with_actor.clone(), with_actor);
        assert_ne!(with_actor, none);
    }
}
