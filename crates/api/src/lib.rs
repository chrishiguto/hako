//! The wire contract between the daemon and its clients: request and
//! response bodies and the OpenAPI document. Shared by `server` and
//! every client, starting with `cli`.
//!
//! The vocabulary the engine also speaks — run states, events and the
//! envelope they stream in, the shared report vocabulary — is defined
//! once in `proto` and re-exported here, so clients depend on this
//! crate alone and never link the engine.

pub mod dto;
pub mod openapi;

pub use dto::{
    AnswerRequest, ApiError, BudgetExtension, ListRunsResponse, ResumeRequest, RunStatusResponse,
    RunSummary, SubmitRunRequest, SubmitRunResponse,
};
pub use openapi::document;
/// The full published language, so a `proto` type missing from the
/// flat re-exports below stays nameable without a direct `proto` dep.
pub use proto;
pub use proto::{
    Answer, BudgetKind, EventEnvelope, IterationOutcome, OutputStream, PauseReason, Question,
    ReportStatus, RunEvent, RunState, TokenUsage,
};
