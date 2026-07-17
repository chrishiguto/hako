//! The agent-loop engine: kernels, runs, and the six trait seams. A
//! library with no knowledge of its hosts — it must never depend on
//! `server` or `api`.
//!
//! All engine I/O flows through six seams — [`Kernel`], [`Sandbox`],
//! [`AgentAdapter`], [`EventSink`], [`Notifier`], [`SecretsProvider`] —
//! handed to a kernel via [`KernelContext`], never reached globally.
//! That is what makes an entire loop testable in-process with fakes.

pub mod agent;
pub mod budget;
pub mod event;
pub mod kernel;
pub mod notify;
mod preamble;
pub mod progress;
pub mod ralph;
pub mod run;
pub mod sandbox;
pub mod secrets;
pub mod workspace;

// The flow language lives in proto; re-exported so a flow remains
// part of the engine's own vocabulary — it is what a kernel runs.
pub use proto::flow;

pub use agent::AgentAdapter;
pub use budget::{BudgetKind, Budgets, TokenUsage};
pub use event::{EventSink, EventSinkError, IterationOutcome, OutputStream, RunEvent};
pub use kernel::{Kernel, KernelContext, KernelError};
pub use notify::{Notification, Notifier, NotifierError};
pub use progress::{ProgressReport, ProgressStatus, Question};
pub use ralph::RalphKernel;
pub use run::{Answer, PauseReason, Resume, RunId, RunOutcome, RunState};
pub use sandbox::{
    ExecEvent, ExecSpec, ExecStream, ExitStatus, Sandbox, SandboxError, SandboxHandle, SandboxSpec,
    WorkspaceMount,
};
pub use secrets::{SecretName, SecretValue, SecretsError, SecretsProvider};
pub use workspace::{Workspace, WorkspaceError};
