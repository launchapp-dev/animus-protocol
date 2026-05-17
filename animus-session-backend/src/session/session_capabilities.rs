/// Capability flags a backend advertises.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionCapabilities {
    /// Backend supports `resume_session`.
    pub supports_resume: bool,
    /// Backend supports `terminate_session`.
    pub supports_terminate: bool,
    /// Backend honors `permission_mode` on the request.
    pub supports_permissions: bool,
    /// Backend bridges MCP servers.
    pub supports_mcp: bool,
    /// Backend emits `ToolCall` / `ToolResult` events.
    pub supports_tool_events: bool,
    /// Backend emits `Thinking` events.
    pub supports_thinking_events: bool,
    /// Backend emits `Artifact` events.
    pub supports_artifact_events: bool,
    /// Backend emits token-usage metadata.
    pub supports_usage_metadata: bool,
}
