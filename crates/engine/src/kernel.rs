//! The kernel seam and the context a kernel works through.

use std::sync::Arc;

use async_trait::async_trait;

use crate::agent::AgentAdapter;
use crate::budget::Budgets;
use crate::event::{EventSink, EventSinkError};
use crate::notify::{Notifier, NotifierError};
use crate::run::{RunId, RunOutcome};
use crate::sandbox::{Sandbox, SandboxError};
use crate::secrets::{SecretsError, SecretsProvider};
use crate::workspace::{Workspace, WorkspaceError};

/// A loop pattern. Kernels own all control flow — iterate, verify,
/// retry, stop — leaving flow files nothing to program.
#[async_trait]
pub trait Kernel: Send + Sync {
    /// The name flows select the kernel by, e.g. `ralph`.
    fn name(&self) -> &str;

    /// Drives the run until it can go no further and reports how it
    /// ended. Resuming a paused run is another `run` call on the same
    /// context.
    async fn run(&self, ctx: KernelContext) -> Result<RunOutcome, KernelError>;
}

/// Everything a kernel may touch. Collaborators are handed in here and
/// reached nowhere else, so a test swaps in fakes wholesale and the
/// whole loop runs in-process — no VMs, no LLMs, no network.
#[derive(Clone)]
pub struct KernelContext {
    pub run_id: RunId,
    pub budgets: Budgets,
    /// Prepared before the kernel starts; the kernel mounts it,
    /// checkpoints it, and reads the domain prompt from it.
    pub workspace: Workspace,
    pub sandbox: Arc<dyn Sandbox>,
    pub agent: Arc<dyn AgentAdapter>,
    pub events: Arc<dyn EventSink>,
    pub notifier: Arc<dyn Notifier>,
    pub secrets: Arc<dyn SecretsProvider>,
}

/// An infrastructure failure the kernel cannot recover from — distinct
/// from a run that *ends* badly, which is `RunOutcome::Failed`.
#[derive(Debug, thiserror::Error)]
pub enum KernelError {
    #[error(transparent)]
    Sandbox(#[from] SandboxError),
    #[error(transparent)]
    Events(#[from] EventSinkError),
    #[error(transparent)]
    Notifier(#[from] NotifierError),
    #[error(transparent)]
    Secrets(#[from] SecretsError),
    /// Host-side workspace work (clone, branch, checkpoint) is kernel
    /// logic rather than a seam; its failures land here.
    #[error(transparent)]
    Workspace(#[from] WorkspaceError),
}
