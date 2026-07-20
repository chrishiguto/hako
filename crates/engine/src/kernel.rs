//! The kernel seam, the context a kernel works through, and the
//! resolution from a flow's `[loop]` table.

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
use proto::flow::{KernelName, VerifyConfig};

/// A loop pattern. Kernels own all control flow — iterate, verify,
/// retry, stop — leaving flow files nothing to program. A kernel
/// carries no name of its own: flows select one by [`KernelName`],
/// and [`resolve`] is the only door in.
#[async_trait]
pub trait Kernel: Send + Sync {
    /// Drives the run until it can go no further and reports how it
    /// ended. Resuming a paused run is another `run` call on the same
    /// context.
    async fn run(&self, ctx: KernelContext) -> Result<RunOutcome, KernelError>;
}

/// Builds the kernel a flow's `[loop]` table selects — the loop-side
/// twin of [`crate::agents::resolve`], run at submit. A name may be
/// declared in the flow language ahead of its kernel, so the language
/// never has zero kernels; resolving such a name is where a submit
/// learns the flow cannot run yet.
pub fn resolve(name: KernelName) -> Result<Arc<dyn Kernel>, KernelConfigError> {
    match name {
        KernelName::Pipeline => Err(KernelConfigError::NotImplemented(name)),
    }
}

/// A `[loop]` table naming a kernel the engine cannot serve yet.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum KernelConfigError {
    #[error("kernel `{}` is not implemented yet", .0.as_str())]
    NotImplemented(KernelName),
}

/// Everything a kernel may touch. Collaborators are handed in here and
/// reached nowhere else, so a test swaps in fakes wholesale and the
/// whole loop runs in-process — no VMs, no LLMs, no network.
#[derive(Clone)]
pub struct KernelContext {
    pub run_id: RunId,
    pub budgets: Budgets,
    /// The verify checks an iteration must pass to count as progress,
    /// and what to do when they keep failing. Empty checks means every
    /// iteration counts. Lifted straight from the flow — no engine-side
    /// lowering, unlike [`Budgets`], because nothing here needs
    /// resolving.
    pub verify: VerifyConfig,
    /// Prepared before the kernel starts; the kernel mounts it and
    /// checkpoints it.
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

#[cfg(test)]
mod tests {
    use super::*;

    /// `pipeline` is declared ahead of its kernel so the flow language
    /// never has zero kernels; submitting one must say so instead of
    /// running.
    #[test]
    fn the_declared_but_unimplemented_kernel_resolves_to_a_refusal() {
        let error = resolve(KernelName::Pipeline)
            .err()
            .expect("pipeline has no kernel yet");
        assert_eq!(
            error,
            KernelConfigError::NotImplemented(KernelName::Pipeline)
        );
        let message = error.to_string();
        assert!(message.contains("pipeline"), "{message}");
        assert!(message.contains("not implemented"), "{message}");
    }
}
