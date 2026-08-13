use std::sync::Arc;

use api::proto::flow::{BudgetConfig, FlowConfig, KernelName, PromptsConfig};
use engine::{ChildRunSpec, EventEnvelope, RunId, RunSpawner, RunSpawnerError, RunState};

use crate::registry::RunRegistry;
use crate::runtime::{EngineRuntime, ResolvedRun};

pub(super) fn for_parent(
    registry: RunRegistry,
    flow: &FlowConfig,
    resolved: &ResolvedRun,
    runtime: &EngineRuntime,
) -> Arc<dyn RunSpawner> {
    let mut child_flow = flow.clone();
    child_flow.r#loop.kernel = KernelName::Pipeline;
    child_flow.prompts = PromptsConfig::default();
    child_flow.budget = BudgetConfig {
        iteration_timeout: flow.budget.iteration_timeout,
        ..BudgetConfig::default()
    };
    child_flow.workspace.branch = None;
    Arc::new(RegistryRunSpawner {
        registry,
        runtime: runtime.clone(),
        flow: child_flow,
        resolved: resolved.for_kernel(KernelName::Pipeline),
    })
}

struct RegistryRunSpawner {
    registry: RunRegistry,
    runtime: EngineRuntime,
    flow: FlowConfig,
    resolved: ResolvedRun,
}

#[async_trait::async_trait]
impl RunSpawner for RegistryRunSpawner {
    async fn spawn(&self, child: ChildRunSpec) -> Result<RunId, RunSpawnerError> {
        self.registry
            .submit_scoped(
                self.flow.clone(),
                self.resolved.clone(),
                &self.runtime,
                Some(child.scope),
            )
            .await
            .map_err(|error| RunSpawnerError(error.to_string()))
    }

    async fn watch(&self, run_id: &RunId) -> Result<Vec<EventEnvelope>, RunSpawnerError> {
        let dir = self
            .registry
            .get(run_id)
            .await
            .ok_or_else(|| RunSpawnerError(format!("child run {run_id} vanished")))?;
        if let Some(mut settled) = self.registry.settled(run_id).await {
            // An Err means the signal's sender dropped without firing;
            // the task is gone either way, and the log below judges.
            let _ = settled.wait_for(|done| *done).await;
        }
        // One projection read, after the signal: `Running` here means
        // the task died without landing its terminal event — answered
        // loudly, never by waiting on a log that will not change.
        let projection = dir
            .project()
            .await
            .map_err(|error| RunSpawnerError(error.to_string()))?;
        if projection.state == RunState::Running {
            return Err(RunSpawnerError(format!(
                "child run {run_id} settled without a terminal event"
            )));
        }
        dir.events()
            .await
            .map_err(|error| RunSpawnerError(error.to_string()))
    }
}
