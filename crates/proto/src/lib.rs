//! The published language — the vocabulary the engine and its clients
//! both speak: the run state machine, the event log's payloads, the
//! agent's progress report, and the flow file format. JSON via serde,
//! not protobuf, despite the crate name's lineage.
//!
//! One definition serves every side. The engine emits these types, the
//! daemon records and streams them verbatim, and clients deserialize
//! the same shapes — so a change here is a wire-contract change by
//! construction, never by accident. Shapes only the daemon
//! and clients speak live in `api`: the envelope each logged event is
//! wrapped in (sequence, timestamp) and the REST bodies. The `openapi`
//! feature adds `utoipa` schema derives so `api` can generate the
//! OpenAPI document; the `schema` feature adds `schemars` derives so
//! xtask can generate the committed flow schema. Product crates build
//! without either.

pub mod budget;
pub mod event;
pub mod flow;
pub mod progress;
pub mod run;
pub mod secrets;

pub use budget::{BudgetKind, TokenUsage};
pub use event::{IterationOutcome, OutputStream, RunEvent};
pub use flow::{FlowConfig, FlowError};
pub use progress::{ProgressReport, ProgressStatus, Question};
pub use run::{PauseReason, RunState};
pub use secrets::SecretName;
