//! The published language — the vocabulary the engine and its clients
//! both speak: the run state machine, the event log's payloads, and
//! the agent's progress report. JSON via serde, not protobuf, despite
//! the crate name's lineage.
//!
//! One definition serves every side. The engine emits these types, the
//! daemon records and streams them verbatim (ADR 0005), and clients
//! deserialize the same shapes — so a change here is a wire-contract
//! change by construction, never by accident. Shapes only the daemon
//! and clients speak live in `api`: the envelope each logged event is
//! wrapped in (sequence, timestamp) and the REST bodies. The `openapi`
//! feature adds `utoipa` schema derives so `api` can generate the
//! OpenAPI document; the engine builds without it.

pub mod budget;
pub mod event;
pub mod progress;
pub mod run;

pub use budget::{BudgetKind, TokenUsage};
pub use event::{IterationOutcome, OutputStream, RunEvent};
pub use progress::{ProgressReport, ProgressStatus, Question};
pub use run::{PauseReason, RunState};
