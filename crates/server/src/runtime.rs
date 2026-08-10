use std::sync::Arc;

use api::proto::flow::FlowConfig;
use async_trait::async_trait;
use engine::agents::{self, AgentConfigError};
use engine::workspace;
use engine::{
    AgentAdapter, Budgets, CancelToken, EventSink, Kernel, KernelContext, Notification, Notifier,
    NotifierError, RunDir, RunEvent, RunState, Sandbox, SandboxError, SecretEnv, SecretsError,
    SecretsProvider,
};
use futures_util::FutureExt;

/// The engine collaborators shared by every run. Each launched run
/// gets its own kernel, workspace, file sink, and context.
#[derive(Clone)]
pub struct EngineRuntime {
    sandbox: Arc<dyn Sandbox>,
    notifier: Arc<dyn Notifier>,
    secrets: Arc<dyn SecretsProvider>,
}

impl EngineRuntime {
    /// The host-side collaborators used by the daemon binary. The
    /// notifier is an inert stub: no kernel notifies yet, so a real
    /// implementation would go unobserved.
    pub fn production(secrets: Arc<dyn SecretsProvider>) -> Self {
        Self::new(
            Arc::new(sandbox::SmolvmSandbox::new(sandbox::SmolvmConfig::default())),
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

    pub(crate) fn launch(
        &self,
        dir: RunDir,
        flow: FlowConfig,
        resolved: ResolvedRun,
        events: Arc<dyn EventSink>,
        cancel: CancelToken,
    ) -> tokio::task::JoinHandle<()> {
        let runtime = self.clone();
        tokio::spawn(async move {
            let result = std::panic::AssertUnwindSafe(drive_run(
                &runtime,
                &dir,
                flow,
                resolved,
                events.clone(),
                cancel,
            ))
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
pub(crate) struct ResolvedRun {
    kernel: Arc<dyn Kernel>,
    agent: Arc<dyn AgentAdapter>,
    /// The run's secrets, resolved at submit — the environment every
    /// sandbox is built with, and what the run's events are scrubbed
    /// against.
    pub(crate) secrets: SecretEnv,
}

async fn drive_run(
    runtime: &EngineRuntime,
    dir: &RunDir,
    flow: FlowConfig,
    resolved: ResolvedRun,
    events: Arc<dyn EventSink>,
    cancel: CancelToken,
) -> Result<(), engine::KernelError> {
    let workspace = workspace::prepare(
        &flow.workspace,
        &dir.meta().run_id,
        &dir.path().join("workspace"),
    )
    .await?;
    let context = KernelContext {
        run_id: dir.meta().run_id.clone(),
        budgets: Budgets::from(&flow.budget),
        cancel,
        verify: flow.verify,
        prompts: flow.prompts,
        workspace,
        sandbox: runtime.sandbox.clone(),
        agent: resolved.agent,
        events,
        notifier: runtime.notifier.clone(),
        secrets: resolved.secrets,
    };
    resolved.kernel.run(context).await.map(|_| ())
}
