//! The kernel seam, the context a kernel works through, and the
//! resolution from a flow's `[loop]` table.

use std::sync::Arc;

use async_trait::async_trait;

use crate::agent::AgentAdapter;
use crate::budget::{BudgetUsage, Budgets};
use crate::cancel::CancelToken;
use crate::event::{EventEnvelope, EventSink, EventSinkError};
use crate::notify::{Notifier, NotifierError};
use crate::pipeline::PipelineKernel;
use crate::run::{RunId, RunOutcome};
use crate::sandbox::{Sandbox, SandboxError};
use crate::secrets::{SecretEnv, SecretsError};
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
    /// Consumption retained across pause/resume launches. Like
    /// [`budgets`](Self::budgets), this is run data rather than a seam.
    pub budget_usage: BudgetUsage,
    /// The replayed Event Log when the host relaunches a paused run.
    /// It stays in the shared wire vocabulary; the owning kernel
    /// derives its typed resume point from it.
    pub replay: Option<Vec<EventEnvelope>>,
    /// The run's cooperative cancel flag — a value like [`budgets`],
    /// not a seventh seam: nothing fakes it, the host fires the same
    /// token a test would. The sandbox bracket is its single
    /// observation point, so a kernel never checks it; it only answers
    /// a cancelled bracket with [`RunOutcome::Cancelled`] through its
    /// normal exit, and teardown and the terminal event happen exactly
    /// as for any other ending.
    ///
    /// [`budgets`]: Self::budgets
    pub cancel: CancelToken,
    /// The verify checks an iteration must pass to count as progress,
    /// and what to do when they keep failing. Empty checks means every
    /// iteration counts. Lifted straight from the flow — no engine-side
    /// lowering, unlike [`Budgets`], because nothing here needs
    /// resolving.
    pub verify: VerifyConfig,
    /// The flow's configured prompt slots — slot name → workspace-
    /// relative file. Core slots override shipped defaults; optional
    /// slots enable their stage. Lifted straight from the flow, like
    /// [`verify`]: which slots are legal is the kernel's, checked at
    /// validation.
    pub prompts: PromptsConfig,
    /// Prepared before the kernel starts; the kernel mounts it and
    /// checkpoints it.
    pub workspace: Workspace,
    pub sandbox: Arc<dyn Sandbox>,
    pub agent: Arc<dyn AgentAdapter>,
    pub events: Arc<dyn EventSink>,
    pub notifier: Arc<dyn Notifier>,
    /// The run's secrets, already resolved — a value like [`budgets`],
    /// not a seam. The host resolves them once at submit, where a gap
    /// still fails the submission that could be fixed; a kernel only
    /// spends them, injecting them into every sandbox it boots and
    /// scrubbing them back out of what comes off the agent.
    ///
    /// [`budgets`]: Self::budgets
    pub secrets: SecretEnv,
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
    /// The replayed Event Log does not describe a valid resume point
    /// for the selected kernel. The detail arrives flattened to a
    /// string on purpose: each kernel owns its resume dialect, and
    /// this shared vocabulary imports none of them.
    #[error("cannot resume run: {0}")]
    Resume(String),
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
