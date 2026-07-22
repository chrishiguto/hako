use std::sync::Arc;

use api::proto::flow::FlowConfig;
use async_trait::async_trait;
use engine::agents::{self, AgentConfigError};
use engine::workspace;
use engine::{
    AgentAdapter, Budgets, EventSink, Kernel, KernelContext, Notification, Notifier, NotifierError,
    RunDir, RunEvent, RunState, Sandbox, SandboxError, SecretName, SecretValue, SecretsError,
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
    /// notifier and secrets store gain their real implementations in
    /// their dedicated slices; neither is exercised by today's kernel.
    pub fn production() -> Self {
        Self::new(
            Arc::new(sandbox::SmolvmSandbox::new(sandbox::SmolvmConfig::default())),
            Arc::new(QuietNotifier),
            Arc::new(EmptySecrets),
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

    pub(crate) fn resolve(&self, flow: &FlowConfig) -> Result<ResolvedRun, AgentConfigError> {
        Ok(ResolvedRun {
            kernel: engine::kernel::resolve(flow.r#loop.kernel),
            agent: agents::resolve(&flow.agent)?,
        })
    }

    pub(crate) async fn launch(
        &self,
        dir: RunDir,
        flow: FlowConfig,
        resolved: ResolvedRun,
    ) -> Result<tokio::task::JoinHandle<()>, engine::StoreError> {
        let events: Arc<dyn EventSink> = Arc::new(dir.event_sink().await?);
        let runtime = self.clone();
        Ok(tokio::spawn(async move {
            let result = std::panic::AssertUnwindSafe(drive_run(
                &runtime,
                &dir,
                flow,
                resolved,
                events.clone(),
            ))
            .catch_unwind()
            .await;
            let failure = match result {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(error.to_string()),
                Err(_) => Some("engine task panicked".to_owned()),
            };
            if let Some(failure) = failure {
                eprintln!("run {} failed: {failure}", dir.meta().run_id);
                let _ = events
                    .emit(RunEvent::StateChanged {
                        state: RunState::Failed,
                    })
                    .await;
            }
        }))
    }
}

struct QuietNotifier;

#[async_trait]
impl Notifier for QuietNotifier {
    async fn notify(&self, _notification: &Notification) -> Result<(), NotifierError> {
        Ok(())
    }
}

struct EmptySecrets;

#[async_trait]
impl SecretsProvider for EmptySecrets {
    async fn resolve(&self, name: &SecretName) -> Result<SecretValue, SecretsError> {
        Err(SecretsError::NotFound(name.clone()))
    }
}

pub(crate) struct ResolvedRun {
    kernel: Arc<dyn Kernel>,
    agent: Arc<dyn AgentAdapter>,
}

async fn drive_run(
    runtime: &EngineRuntime,
    dir: &RunDir,
    flow: FlowConfig,
    resolved: ResolvedRun,
    events: Arc<dyn EventSink>,
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
        verify: flow.verify,
        prompts: flow.prompts,
        workspace,
        sandbox: runtime.sandbox.clone(),
        agent: resolved.agent,
        events,
        notifier: runtime.notifier.clone(),
        secrets: runtime.secrets.clone(),
    };
    resolved.kernel.run(context).await.map(|_| ())
}
