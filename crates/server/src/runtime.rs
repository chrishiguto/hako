use std::sync::Arc;

use api::proto::flow::FlowConfig;
use async_trait::async_trait;
use engine::agents::{self, AgentConfigError};
use engine::workspace;
use engine::{
    AgentAdapter, Budgets, CancelToken, EventSink, Kernel, KernelContext, Notification, Notifier,
    NotifierError, RunDir, RunEvent, RunResume, RunState, Sandbox, SandboxError, SecretEnv,
    SecretsError, SecretsProvider,
};
use futures_util::FutureExt;
use sandbox::SmolvmConfig;

/// The engine collaborators shared by every run. Each launched run
/// gets its own kernel, workspace, file sink, and context.
#[derive(Clone)]
pub struct EngineRuntime {
    sandbox: Arc<dyn Sandbox>,
    notifier: Arc<dyn Notifier>,
    secrets: Arc<dyn SecretsProvider>,
}

impl EngineRuntime {
    /// The host-side collaborators used by the daemon binary, over the
    /// microVM configuration the host was started with. The notifier is
    /// an inert stub: no kernel notifies yet, so a real implementation
    /// would go unobserved.
    pub fn production(sandbox: SmolvmConfig, secrets: Arc<dyn SecretsProvider>) -> Self {
        Self::new(
            Arc::new(sandbox::SmolvmSandbox::new(sandbox)),
            Arc::new(QuietNotifier),
            secrets,
        )
    }

    pub fn new(
        sandbox: Arc<dyn Sandbox>,
        notifier: Arc<dyn Notifier>,
        secrets: Arc<dyn SecretsProvider>,
    ) -> Self {
        Self {
            sandbox,
            notifier,
            secrets,
        }
    }

    pub(crate) async fn preflight(&self) -> Result<(), SandboxError> {
        self.sandbox.preflight().await
    }

    /// Everything a flow needs settled before a run is accepted: the
    /// kernel and adapter it names, and the secrets both it and the
    /// adapter require — resolved here, at submit, so a provisioning
    /// gap is the answer to the submission that could still be fixed
    /// rather than a failed run discovered later. Resolved once, too:
    /// what comes back is the environment every sandbox of the run is
    /// built with, so no iteration depends on the store still being
    /// reachable.
    pub(crate) async fn resolve(&self, flow: &FlowConfig) -> Result<ResolvedRun, ResolveError> {
        let agent = agents::resolve(&flow.agent)?;
        let secrets = engine::secrets::resolve(
            self.secrets.as_ref(),
            &flow.secrets.env,
            &agent.required_secrets(),
        )
        .await?;
        Ok(ResolvedRun {
            kernel: engine::kernel::resolve(flow.r#loop.kernel),
            agent,
            secrets,
        })
    }

    pub(crate) fn launch(&self, launch: RunLaunch) -> tokio::task::JoinHandle<()> {
        let runtime = self.clone();
        tokio::spawn(async move {
            let dir = launch.dir.clone();
            let events = launch.events.clone();
            let result = std::panic::AssertUnwindSafe(drive_run(&runtime, launch))
                .catch_unwind()
                .await;
            let failure = match result {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(error.to_string()),
                Err(_) => Some("engine task panicked".to_owned()),
            };
            if let Some(failure) = failure {
                tracing::error!(run_id = %dir.meta().run_id, %failure, "run failed");
                let _ = events
                    .emit(RunEvent::StateChanged {
                        state: RunState::Failed,
                    })
                    .await;
            }
        })
    }
}

/// Everything one kernel launch needs, fresh or resumed. A carrier,
/// not an abstraction: the registry owns the values — budgets
/// included, derived exactly once where the record keeps them — and
/// the runtime only drives.
pub(crate) struct RunLaunch {
    pub(crate) dir: RunDir,
    pub(crate) flow: FlowConfig,
    pub(crate) resolved: ResolvedRun,
    pub(crate) events: Arc<dyn EventSink>,
    pub(crate) cancel: CancelToken,
    pub(crate) budgets: Budgets,
    pub(crate) resume: Option<RunResume>,
}

struct QuietNotifier;

#[async_trait]
impl Notifier for QuietNotifier {
    async fn notify(&self, _notification: &Notification) -> Result<(), NotifierError> {
        Ok(())
    }
}

/// A flow the daemon cannot start, as the submit route answers for
/// it: a `[agent]` table no adapter accepts, or a secret the store
/// does not hold.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ResolveError {
    #[error(transparent)]
    Agent(#[from] AgentConfigError),
    #[error(transparent)]
    Secrets(#[from] SecretsError),
}

/// What a submitted flow resolved to, carried from the submit route to
/// the launched run.
#[derive(Clone)]
pub(crate) struct ResolvedRun {
    kernel: Arc<dyn Kernel>,
    agent: Arc<dyn AgentAdapter>,
    /// The run's secrets, resolved at submit — the environment every
    /// sandbox is built with, and what the run's events are scrubbed
    /// against.
    pub(crate) secrets: SecretEnv,
}

async fn drive_run(runtime: &EngineRuntime, launch: RunLaunch) -> Result<(), engine::KernelError> {
    let run_id = &launch.dir.meta().run_id;
    let clone_dest = launch.dir.path().join("workspace");
    // Which mode means what — and mount mode's one-active-run lock,
    // which a resume must take back — is the workspace module's
    // knowledge, not this crate's.
    let workspace = if launch.resume.is_some() {
        workspace::reattach(&launch.flow.workspace, run_id, &clone_dest).await?
    } else {
        workspace::prepare(&launch.flow.workspace, run_id, &clone_dest).await?
    };
    let context = KernelContext {
        run_id: launch.dir.meta().run_id.clone(),
        budgets: launch.budgets,
        resume: launch.resume,
        cancel: launch.cancel,
        verify: launch.flow.verify,
        prompts: launch.flow.prompts,
        workspace,
        sandbox: runtime.sandbox.clone(),
        agent: launch.resolved.agent,
        events: launch.events,
        notifier: runtime.notifier.clone(),
        secrets: launch.resolved.secrets,
    };
    launch.resolved.kernel.run(context).await.map(|_| ())
}
