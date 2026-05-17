/// Normalized text-event shape emitted by [`super::extract_text_from_line`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NormalizedTextEvent {
    /// Incremental text delta from a streaming response.
    TextChunk {
        /// Text content.
        text: String,
    },
    /// Final aggregated result text.
    FinalResult {
        /// Text content.
        text: String,
    },
    /// Line did not contain extractable text.
    Ignored,
}
