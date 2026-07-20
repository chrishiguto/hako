//! The published language — the vocabulary the engine and its clients
//! both speak: the run state machine, the event log's payloads, the
//! report vocabulary kernels share, and the flow file format. JSON via
//! serde, not protobuf, despite the crate name's lineage.
//!
//! The language has two layers (ADR 0010). The core modules — `run`,
//! `event`, `flow`, `report`, `budget`, `secrets` — are shared
//! vocabulary every kernel speaks. Each kernel's own additions — its
//! report shapes, later its prompt slots — form a *dialect*, one
//! top-level module named after the kernel (`pipeline`), building on
//! the core and never redefining it. Dialect types are reached through
//! their module, not re-exported at the crate root: the path is the
//! label that says whose vocabulary you are speaking.
//!
//! One definition serves every side. The engine emits these types, the
//! daemon records and streams them verbatim, and clients deserialize
//! the same shapes — so a change here is a wire-contract change by
//! construction, never by accident. Shapes only the daemon and clients
//! speak live in `api`: the REST bodies. The `openapi` feature adds
//! `utoipa` schema derives so `api` can generate the OpenAPI document;
//! the `schema` feature adds `schemars` derives so xtask can generate
//! the committed schemas. Product crates build without either.

pub mod budget;
pub mod event;
pub mod flow;
pub mod pipeline;
pub mod report;
pub mod run;
#[cfg(feature = "schema")]
pub(crate) mod schema;
pub mod secrets;

pub use budget::{BudgetKind, TokenUsage};
pub use event::{EventEnvelope, IterationOutcome, OutputStream, RunEvent};
pub use flow::{FlowConfig, FlowError};
pub use report::{Answer, Question, ReportStatus};
pub use run::{PauseReason, RunState};
pub use secrets::SecretName;
