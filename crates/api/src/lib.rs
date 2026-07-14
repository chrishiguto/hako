//! The wire contract between the daemon and its clients: request/response
//! types and the SSE event vocabulary. Shared by `server` and every
//! client, starting with `cli`.
//!
//! This crate mirrors engine concepts without depending on the engine:
//! clients must be able to consume the contract without linking it.

pub mod dto;
pub mod event;
pub mod openapi;
pub mod progress;
pub mod run;

pub use dto::{
    Answer, AnswerRequest, ApiError, BudgetExtension, ListRunsResponse, ResumeRequest,
    RunStatusResponse, RunSummary, SubmitRunRequest, SubmitRunResponse,
};
pub use event::{BudgetKind, EventEnvelope, IterationOutcome, OutputStream, RunEvent, TokenUsage};
pub use openapi::document;
pub use progress::{ProgressReport, ProgressStatus, Question};
pub use run::{PauseReason, RunState};
