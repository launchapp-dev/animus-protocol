use tokio::sync::mpsc;

use super::session_event::SessionEvent;

/// Handle to an in-flight session run. The caller drains `events` until the
/// terminal [`SessionEvent::Finished`] arrives.
#[derive(Debug)]
pub struct SessionRun {
    /// Backend-assigned session id (may differ from any caller-supplied id).
    pub session_id: Option<String>,
    /// Stream of events.
    pub events: mpsc::Receiver<SessionEvent>,
    /// Label identifying which backend was selected.
    pub selected_backend: String,
    /// If the resolver picked a fallback backend, the human-readable reason.
    pub fallback_reason: Option<String>,
    /// Child PID, if known.
    pub pid: Option<u32>,
}
