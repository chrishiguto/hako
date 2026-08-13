//! The pipeline run harness the integration suites share: a
//! [`KernelContext`] over the staged fakes, the real kernel driven to
//! its outcome, and everything a test asserts on collected in one
//! place — so each suite scripts agents and states expectations, never
//! re-assembling the run machinery.

use std::path::Path;
use std::sync::Arc;

use proto::flow::{PromptsConfig, VerifyConfig};

use super::fakes::{RecordingSink, ScriptedAgent};
use super::sandbox::StagedSandbox;
use crate::event::RunEvent;
use crate::kernel::{Kernel, KernelContext};
use crate::pipeline::PipelineKernel;
use crate::run::RunOutcome;
use crate::workspace::Workspace;

/// What one pipeline run left behind for the assertions.
pub struct Ran {
    pub outcome: RunOutcome,
    pub events: Vec<RunEvent>,
    pub prompts: Vec<String>,
    pub sandbox: Arc<StagedSandbox>,
    pub workspace: tempfile::TempDir,
}

/// A [`KernelContext`] over the staged fakes with a recording sink —
/// for tests that touch the context before the run or assert on an
/// error the kernel returns. Runs that go the distance use
/// [`drive_pipeline`].
pub fn pipeline_context(
    workspace: &Path,
    sandbox: Arc<StagedSandbox>,
    verify: VerifyConfig,
    prompts: PromptsConfig,
) -> (KernelContext, Arc<RecordingSink>) {
    let sink = Arc::new(RecordingSink::default());
    let ctx = KernelContext {
        verify,
        prompts,
        workspace: Workspace::at(workspace),
        sandbox,
        agent: Arc::new(ScriptedAgent::new()),
        events: sink.clone(),
        ..super::context()
    };
    (ctx, sink)
}

/// Runs the pipeline kernel over a prepared workspace and sandbox and
/// collects the [`Ran`]. The caller builds the sandbox, so seeding
/// guest files or scripting check exits happens before the run without
/// a second assembly path.
pub async fn drive_pipeline(
    workspace: tempfile::TempDir,
    sandbox: Arc<StagedSandbox>,
    verify: VerifyConfig,
    prompts: PromptsConfig,
) -> Ran {
    let (ctx, sink) = pipeline_context(workspace.path(), sandbox.clone(), verify, prompts);
    let outcome = PipelineKernel.run(ctx).await.unwrap();
    Ran {
        outcome,
        events: sink.events(),
        prompts: sandbox.agent_prompts(),
        sandbox,
        workspace,
    }
}

/// The stage-scoped events in order, as `(kind, stage)` pairs.
pub fn stage_events(events: &[RunEvent]) -> Vec<(String, String)> {
    events
        .iter()
        .filter_map(|event| match event {
            RunEvent::StageStarted { stage, .. } => Some(("stage_started".into(), stage.clone())),
            RunEvent::StageReported { stage, .. } => Some(("stage_reported".into(), stage.clone())),
            _ => None,
        })
        .collect()
}
