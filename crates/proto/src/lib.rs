//! The published language — every type that crosses a process
//! boundary: the run event log on disk, the SSE stream, and the
//! agent's progress report. JSON via serde, not protobuf, despite the
//! crate name's lineage.
//!
//! One definition serves every side. The engine writes these types
//! into its event log, the daemon streams that log verbatim (ADR
//! 0005), and clients deserialize the same shapes — so a change here
//! is a wire-contract change by construction, never by accident. The
//! `openapi` feature adds `utoipa` schema derives so `api` can
//! generate the OpenAPI document; the engine builds without it.

pub mod budget;
pub mod event;
pub mod progress;
pub mod run;

pub use budget::{BudgetKind, TokenUsage};
pub use event::{IterationOutcome, OutputStream, RunEvent};
pub use progress::{ProgressReport, ProgressStatus, Question};
pub use run::{PauseReason, RunState};
