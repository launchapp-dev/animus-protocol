use serde_json::Value;

/// Stream of normalized events a session emits while running.
#[derive(Debug, Clone, PartialEq)]
pub enum SessionEvent {
    /// Backend acknowledged the request and the child process is starting.
    Started {
        /// Label identifying the selected backend (e.g. `"claude-native"`).
        backend: String,
        /// Session id, if assigned synchronously.
        session_id: Option<String>,
        /// Child PID, if known.
        pid: Option<u32>,
    },
    /// Incremental text delta from the model.
    TextDelta {
        /// Text content.
        text: String,
    },
    /// Final aggregated assistant text.
    FinalText {
        /// Text content.
        text: String,
    },
    /// Agent invoked a tool.
    ToolCall {
        /// Tool name.
        tool_name: String,
        /// Tool arguments.
        arguments: Value,
        /// MCP server that hosts the tool, if known.
        server: Option<String>,
    },
    /// Result returned from a tool.
    ToolResult {
        /// Tool name.
        tool_name: String,
        /// Tool output.
        output: Value,
        /// True if the tool reported success.
        success: bool,
    },
    /// Visible reasoning trace from the model.
    Thinking {
        /// Reasoning text.
        text: String,
    },
    /// Generated artifact reference.
    Artifact {
        /// Artifact id.
        artifact_id: String,
        /// Free-form metadata.
        metadata: Value,
    },
    /// Free-form metadata frame (usage stats, session info, ...).
    Metadata {
        /// Metadata payload.
        metadata: Value,
    },
    /// Agent requested a human-in-the-loop interaction (approval or
    /// question) and is waiting on a decision. Added in v0.1.13.5.
    InteractionRequested {
        /// Interaction id in the kernel interactions store.
        id: String,
        /// Interaction kind: `"approval"` or `"question"`.
        kind: String,
    },
    /// A previously requested interaction was resolved. Added in v0.1.13.5.
    InteractionResolved {
        /// Interaction id in the kernel interactions store.
        id: String,
        /// Resolution: `"allow"` / `"deny"` for approvals, or a short
        /// answer summary for questions.
        decision: String,
    },
    /// Error encountered mid-run.
    Error {
        /// Error message.
        message: String,
        /// True if the run continues after this event.
        recoverable: bool,
    },
    /// Terminal event — child process exited.
    Finished {
        /// Process exit code.
        exit_code: Option<i32>,
    },
}
