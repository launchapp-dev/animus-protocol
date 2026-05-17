//! Error types used by every session backend in this crate.

use thiserror::Error;

/// Convenience alias for `Result<T, Error>`.
pub type Result<T> = std::result::Result<T, Error>;

/// All failure modes a session backend can surface.
#[derive(Debug, Error)]
pub enum Error {
    /// The requested CLI binary was not found on PATH.
    #[error("CLI not found: {0}")]
    CliNotFound(String),

    /// The wrapped CLI failed to start or exited unexpectedly.
    #[error("CLI execution failed: {0}")]
    ExecutionFailed(String),

    /// Caller supplied invalid parameters (e.g. empty resume session id).
    #[error("CLI validation failed: {0}")]
    ValidationFailed(String),

    /// IO failure while talking to the child process.
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    /// JSON (de)serialization failure.
    #[error("Serialization error: {0}")]
    SerializationError(String),

    /// Wrapped anyhow chain.
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::SerializationError(e.to_string())
    }
}
