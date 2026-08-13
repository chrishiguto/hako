use std::sync::Arc;

use api::proto::flow::FlowConfig;
use engine::agents::{self, AgentConfigError};
use engine::workspace;
use engine::{
    AgentAdapter, Budgets, CancelToken, EventSink, Kernel, KernelContext, Notifier, RunDir,
    RunEvent, RunState, Sandbox, SandboxError, SecretEnv, SecretsError, SecretsProvider,
};
use futures_util::FutureExt;
use sandbox::SmolvmConfig;

use crate::notifier;

/// The engine collaborators shared by every run. Each launched run
/// gets its own kernel, workspace, file sink, and context.
#[derive(Clone)]
pub struct EngineRuntime {
    sandbox: Arc<dyn Sandbox>,
    notifier_source: NotifierSource,
    secrets: Arc<dyn SecretsProvider>,
}

#[derive(Clone)]
enum NotifierSource {
    Flow,
    Fixed(Arc<dyn Notifier>),
}

impl EngineRuntime {
    /// The host-side collaborators used by the daemon binary, over the
    /// microVM configuration the host was started with. Each flow
    /// resolves its own notifier because the webhook target belongs to
    /// the run, not the daemon process.
    pub fn production(sandbox: SmolvmConfig, secrets: Arc<dyn SecretsProvider>) -> Self {
        Self {
            sandbox: Arc::new(sandbox::SmolvmSandbox::new(sandbox)),
            notifier_source: NotifierSource::Flow,
            secrets,
        }
    }

    /// Builds a host with a fixed notifier for tests embedding the
    /// engine. Production uses [`Self::production`] so flow webhooks
    /// are resolved independently.
    pub fn new(
        sandbox: Arc<dyn Sandbox>,
        notifier: Arc<dyn Notifier>,
        secrets: Arc<dyn SecretsProvider>,
    ) -> Self {
        Self {
            sandbox,
            notifier_source: NotifierSource::Fixed(notifier),
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
        let notifier: Arc<dyn Notifier> = match &self.notifier_source {
            NotifierSource::Flow => notifier::resolve(flow.notify.as_ref())?,
            NotifierSource::Fixed(notifier) => notifier.clone(),
        };
        Ok(ResolvedRun {
            kernel: engine::kernel::resolve(flow.r#loop.kernel),
            agent,
            notifier,
            secrets,
        })
    }

    pub(crate) fn launch(&self, launch: RunLaunch) -> LaunchedRun {
        let runtime = self.clone();
        let (settle, settled) = tokio::sync::watch::channel(false);
        let task = tokio::spawn(async move {
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
            // Unconditional and last: a watcher learns the task is
            // gone even when the terminal event write above failed,
            // and judges the run by its log.
            let _ = settle.send(true);
        });
        LaunchedRun { task, settled }
    }
}

/// A launched execution: the driving task and its settle signal.
pub(crate) struct LaunchedRun {
    pub(crate) task: tokio::task::JoinHandle<()>,
    /// Flips true when the driving task has fully wound down — after
    /// every event the run will ever write, whether or not the last
    /// write succeeded.
    pub(crate) settled: tokio::sync::watch::Receiver<bool>,
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
    pub(crate) budget_usage: engine::BudgetUsage,
    pub(crate) replay: Option<Vec<engine::EventEnvelope>>,
    pub(crate) scope: Option<String>,
    pub(crate) run_spawner: Arc<dyn engine::RunSpawner>,
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
    #[error(transparent)]
    Notifier(#[from] notifier::NotifierConfigError),
}

/// What a submitted flow resolved to, carried from the submit route to
/// the launched run.
#[derive(Clone)]
pub(crate) struct ResolvedRun {
    kernel: Arc<dyn Kernel>,
    agent: Arc<dyn AgentAdapter>,
    notifier: Arc<dyn Notifier>,
    /// The run's secrets, resolved at submit — the environment every
    /// sandbox is built with, and what the run's events are scrubbed
    /// against.
    pub(crate) secrets: SecretEnv,
}

impl ResolvedRun {
    pub(crate) fn for_kernel(&self, kernel: api::proto::flow::KernelName) -> Self {
        Self {
            kernel: engine::kernel::resolve(kernel),
            agent: self.agent.clone(),
            notifier: self.notifier.clone(),
            secrets: self.secrets.clone(),
        }
    }
}

async fn drive_run(runtime: &EngineRuntime, launch: RunLaunch) -> Result<(), engine::KernelError> {
    let run_id = &launch.dir.meta().run_id;
    let clone_dest = launch.dir.path().join("workspace");
    // Which mode means what — and mount mode's one-active-run lock,
    // which a resume must take back — is the workspace module's
    // knowledge, not this crate's.
    let workspace = if launch.replay.is_some() {
        workspace::reattach(&launch.flow.workspace, run_id, &clone_dest).await?
    } else {
        workspace::prepare(&launch.flow.workspace, run_id, &clone_dest).await?
    };
    let context = KernelContext {
        run_id: launch.dir.meta().run_id.clone(),
        budgets: launch.budgets,
        budget_usage: launch.budget_usage,
        replay: launch.replay,
        scope: launch.scope,
        cancel: launch.cancel,
        verify: launch.flow.verify,
        prompts: launch.flow.prompts,
        workspace,
        sandbox: runtime.sandbox.clone(),
        agent: launch.resolved.agent,
        events: launch.events,
        notifier: launch.resolved.notifier,
        run_spawner: launch.run_spawner,
        secrets: launch.resolved.secrets,
    };
    launch.resolved.kernel.run(context).await.map(|_| ())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::{
        Router,
        body::Bytes,
        http::{HeaderMap, StatusCode},
        routing::post,
    };
    use engine::{Notification, testkit::NoSecrets};
    use serde_json::json;

    use super::*;

    /// A recording webhook endpoint: every request's content type and
    /// body land on the channel, and every request succeeds.
    async fn webhook_server() -> (
        std::net::SocketAddr,
        tokio::sync::mpsc::UnboundedReceiver<serde_json::Value>,
        tokio::task::JoinHandle<()>,
    ) {
        let (sent, received) = tokio::sync::mpsc::unbounded_channel();
        let app = Router::new().route(
            "/hook",
            post(move |headers: HeaderMap, body: Bytes| {
                let sent = sent.clone();
                async move {
                    let content_type = headers
                        .get(reqwest::header::CONTENT_TYPE)
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or_default()
                        .to_owned();
                    let _ = sent.send(json!({
                        "content_type": content_type,
                        "body": String::from_utf8(body.to_vec()).unwrap(),
                    }));
                    StatusCode::OK
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (address, received, server)
    }

    async fn notify_via(
        notify_section: &str,
        reason: engine::PauseReason,
        summary: Option<&str>,
    ) -> serde_json::Value {
        let (address, mut received, server) = webhook_server().await;
        let flow = FlowConfig::from_toml(&format!(
            r#"[loop]
kernel = "pipeline"

[agent]
engine = "cmd"
command = ["agent", "{{prompt}}"]

[workspace]
repo = "."

[notify]
webhook = "http://{address}/hook"
{notify_section}"#
        ))
        .unwrap();
        let runtime = EngineRuntime::production(SmolvmConfig::default(), Arc::new(NoSecrets));

        let resolved = runtime.resolve(&flow).await.unwrap();
        resolved
            .notifier
            .notify(&Notification {
                run_id: engine::RunId::new("run-8"),
                reason,
                summary: summary.map(str::to_owned),
            })
            .await
            .unwrap();

        let request = received.recv().await.unwrap();
        assert!(
            received.try_recv().is_err(),
            "one pause, one request — never a probing retry"
        );
        server.abort();
        request
    }

    #[tokio::test]
    async fn a_flow_webhook_posts_the_pause_as_plain_text_by_default() {
        assert_eq!(
            notify_via(
                "",
                engine::PauseReason::Drift,
                Some("three passes produced no commits"),
            )
            .await,
            json!({
                "content_type": "text/plain; charset=utf-8",
                "body": "hako run run-8 paused (drift): three passes produced no commits",
            })
        );
    }

    #[tokio::test]
    async fn a_slack_format_flow_posts_the_pause_as_json_text() {
        assert_eq!(
            notify_via(
                "format = \"slack\"",
                engine::PauseReason::Drift,
                Some("three passes produced no commits"),
            )
            .await,
            json!({
                "content_type": "application/json",
                "body": serde_json::to_string(&json!({
                    "text": "hako run run-8 paused (drift): three passes produced no commits",
                })).unwrap(),
            })
        );
    }

    #[tokio::test]
    async fn a_flow_webhook_omits_a_missing_summary() {
        assert_eq!(
            notify_via("", engine::PauseReason::Budget, None).await,
            json!({
                "content_type": "text/plain; charset=utf-8",
                "body": "hako run run-8 paused (budget)",
            })
        );
    }
}
