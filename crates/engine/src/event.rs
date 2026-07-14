//! The sink seam events flow through. The event vocabulary itself is
//! `proto`'s — the engine emits wire types directly, so the log a sink
//! writes is already the published format (ADR 0005 streams it
//! verbatim).

use async_trait::async_trait;

pub use proto::event::{IterationOutcome, OutputStream, RunEvent};

/// Where a kernel's events go — an append-only log in production, a
/// vector in tests. Serves exactly one run.
#[async_trait]
pub trait EventSink: Send + Sync {
    /// Appends one event. Order is meaning: events must land in the
    /// order they were emitted.
    async fn emit(&self, event: RunEvent) -> Result<(), EventSinkError>;
}

/// An event that could not be recorded. Fatal to a run — a loop whose
/// audit trail has holes must not keep going.
#[derive(Debug, thiserror::Error)]
#[error("event sink failure: {0}")]
pub struct EventSinkError(pub String);
