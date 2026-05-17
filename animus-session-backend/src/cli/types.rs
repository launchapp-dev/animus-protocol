//! Canonical CLI tool identifiers recognized by the resolver.

use serde::{Deserialize, Serialize};

/// One of the CLI tools this crate knows how to drive natively.
///
/// Tools not in this enum fall through to [`crate::SubprocessSessionBackend`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CliType {
    /// Anthropic Claude Code (`claude`).
    Claude,
    /// OpenAI Codex (`codex`).
    Codex,
    /// Google Gemini CLI (`gemini`).
    Gemini,
    /// SST opencode (`opencode`).
    OpenCode,
    /// Animus OAI-compatible runner (`animus-oai-runner`).
    OaiRunner,
    /// Aider — recognized for tool-name parsing but no native backend.
    Aider,
    /// Cursor CLI — recognized for tool-name parsing but no native backend.
    Cursor,
    /// Cline — recognized for tool-name parsing but no native backend.
    Cline,
    /// Anything else — recognized for tool-name parsing but no native backend.
    Custom,
}

impl CliType {
    /// Default executable name to spawn for this tool.
    pub fn executable_name(&self) -> &str {
        match self {
            CliType::Claude => "claude",
            CliType::Codex => "codex",
            CliType::Gemini => "gemini",
            CliType::OpenCode => "opencode",
            CliType::OaiRunner => "animus-oai-runner",
            CliType::Aider => "aider",
            CliType::Cursor => "cursor",
            CliType::Cline => "cline",
            CliType::Custom => "custom",
        }
    }

    /// Human-readable display name.
    pub fn display_name(&self) -> &str {
        match self {
            CliType::Claude => "Claude Code",
            CliType::Codex => "OpenAI Codex",
            CliType::Gemini => "Google Gemini CLI",
            CliType::OpenCode => "OpenCode",
            CliType::OaiRunner => "Animus OAI Runner",
            CliType::Aider => "Aider",
            CliType::Cursor => "Cursor CLI",
            CliType::Cline => "Cline",
            CliType::Custom => "Custom CLI",
        }
    }
}
