//! The wire contract between the daemon and its clients: request and
//! response bodies, the SSE envelope, and the OpenAPI document. Shared
//! by `server` and every client, starting with `cli`.
//!
//! The vocabulary the engine also speaks — run states, events, the
//! progress report — is defined once in `proto` and re-exported here,
//! so clients depend on this crate alone and never link the engine.

pub mod dto;
pub mod event;
pub mod openapi;

pub use dto::{
    Answer, AnswerRequest, ApiError, BudgetExtension, ListRunsResponse, ResumeRequest,
    RunStatusResponse, RunSummary, SubmitRunRequest, SubmitRunResponse,
};
pub use event::EventEnvelope;
pub use openapi::document;
pub use proto::{
    BudgetKind, IterationOutcome, OutputStream, PauseReason, ProgressReport, ProgressStatus,
    Question, RunEvent, RunState, TokenUsage,
};
