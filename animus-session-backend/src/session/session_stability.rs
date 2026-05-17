/// Stability tier a backend declares.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionStability {
    /// Production-ready backend.
    Stable,
    /// Behavior may change as the wrapped CLI evolves.
    Experimental,
}
