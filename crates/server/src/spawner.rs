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
    Arc::new(RegistryRunSpawner {
        registry,
        runtime: runtime.clone(),
        flow: child_flow(flow),
        resolved: resolved.for_kernel(KernelName::Pipeline),
    })
}

/// The flow a child pipeline runs under — the parent's flow with each
/// fanout-specific choice stripped, every line for a reason:
///
/// - The kernel becomes `pipeline`: fanout children are pipeline runs
///   by design, never nested fanouts.
/// - The prompts table is cleared, not re-keyed: both kernels publish
///   a `plan` slot, so an inherited table would feed the parent's
///   fanout planning prompt to every child's plan stage. Children run
///   the shipped defaults; their objective arrives as the scope.
/// - Budgets reset to the uncapped defaults, keeping only the
///   parent's iteration timeout: the parent enforces the aggregate
///   across the batch, and a local cap would end a child early
///   without the parent's say.
/// - The pinned branch is dropped: a child starts from the repo's
///   default branch, and any branch it works on is its own agent's
///   workflow.
fn child_flow(parent: &FlowConfig) -> FlowConfig {
    let mut flow = parent.clone();
    flow.r#loop.kernel = KernelName::Pipeline;
    flow.prompts = PromptsConfig::default();
    flow.budget = BudgetConfig {
        iteration_timeout: parent.budget.iteration_timeout,
        ..BudgetConfig::default()
    };
    flow.workspace.branch = None;
    flow
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
            .submit(
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
