//! The kernel seam, the context a kernel works through, and the
//! resolution from a flow's `[loop]` table.

use std::sync::Arc;

use async_trait::async_trait;

use crate::agent::AgentAdapter;
use crate::budget::Budgets;
use crate::event::{EventSink, EventSinkError};
use crate::notify::{Notifier, NotifierError};
use crate::pipeline::PipelineKernel;
use crate::run::{RunId, RunOutcome};
use crate::sandbox::{Sandbox, SandboxError};
use crate::secrets::{SecretsError, SecretsProvider};
use crate::workspace::{Workspace, WorkspaceError};
use proto::flow::{KernelName, PromptsConfig, VerifyConfig};

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
/// twin of [`crate::agents::resolve`], run at submit. Infallible: the
/// kernel set is closed and every name it admits has a kernel, so the
/// exhaustive match cannot fall through. A future kernel declared in
/// the flow language ahead of its implementation would reintroduce the
/// fallible shape [`crate::agents::resolve`] carries for its open set.
pub fn resolve(name: KernelName) -> Arc<dyn Kernel> {
    match name {
        KernelName::Pipeline => Arc::new(PipelineKernel),
    }
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
    /// The flow's per-slot prompt overrides — slot name → workspace-
    /// relative file. A staged kernel resolves each stage's prompt
    /// through this, falling back to its kernel-shipped default when a
    /// slot is absent. Lifted straight from the flow, like [`verify`]:
    /// which slots are legal is the kernel's, checked at validation.
    pub prompts: PromptsConfig,
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

    /// Every name the flow language admits resolves to a live kernel —
    /// the closed set has no gaps, so a submit never learns here that
    /// its flow cannot run.
    #[test]
    fn every_kernel_name_resolves_to_a_live_kernel() {
        for name in KernelName::ALL {
            let _: Arc<dyn Kernel> = resolve(name);
        }
    }
}
