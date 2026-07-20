//! The agent-loop engine: kernels, runs, and the six trait seams. A
//! library with no knowledge of its hosts — it must never depend on
//! `server` or `api`.
//!
//! All engine I/O flows through six seams — [`Kernel`], [`Sandbox`],
//! [`AgentAdapter`], [`EventSink`], [`Notifier`], [`SecretsProvider`] —
//! handed to a kernel via [`KernelContext`], never reached globally.
//! That is what makes an entire loop testable in-process with fakes.

pub mod agent;
pub mod agents;
pub mod budget;
pub mod event;
pub mod invocation;
pub mod kernel;
pub mod notify;
pub mod pipeline;
pub mod preamble;
pub mod report;
pub mod run;
pub mod sandbox;
pub mod secrets;
pub mod store;
pub mod verify;
pub mod workspace;

// The flow language lives in proto; re-exported so a flow remains
// part of the engine's own vocabulary — it is what a kernel runs.
pub use proto::flow;
pub use proto::flow::{FailAction, OnFail, PromptsConfig, VerifyConfig};

pub use agent::AgentAdapter;
pub use budget::{BudgetKind, Budgets, TokenUsage};
pub use event::{
    EventEnvelope, EventSink, EventSinkError, IterationOutcome, OutputStream, RunEvent,
};
pub use kernel::{Kernel, KernelContext, KernelError};
pub use notify::{Notification, Notifier, NotifierError};
pub use pipeline::PipelineKernel;
pub use report::{Answer, Question, ReportStatus};
pub use run::{PauseReason, RunId, RunOutcome, RunState};
pub use sandbox::{
    ExecEvent, ExecSpec, ExecStream, ExitStatus, Sandbox, SandboxError, SandboxHandle, SandboxSpec,
    WorkspaceMount,
};
pub use secrets::{SecretName, SecretValue, SecretsError, SecretsProvider};
pub use store::{FileEventSink, RunDir, RunMeta, StoreError};
pub use workspace::{Workspace, WorkspaceError};
