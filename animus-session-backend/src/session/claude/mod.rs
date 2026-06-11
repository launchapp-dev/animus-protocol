//! Claude Code native session backend.

mod backend;
mod parser;
mod transport;

pub use backend::ClaudeSessionBackend;
pub use transport::CLAUDE_PERMISSION_PROMPT_TOOL;
