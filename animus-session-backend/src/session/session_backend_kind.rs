/// Discriminator identifying which session backend variant is in use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionBackendKind {
    /// Claude Code native backend.
    ClaudeSdk,
    /// Codex native backend.
    CodexSdk,
    /// Gemini native backend.
    GeminiSdk,
    /// OpenCode native backend.
    OpenCodeSdk,
    /// Animus OAI-compatible runner native backend.
    OaiRunnerSdk,
    /// Generic subprocess fallback.
    Subprocess,
}
