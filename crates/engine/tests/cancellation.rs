//! Cooperative cancellation driven over the testkit fakes: however
//! the cancel lands — mid-exec, between stages, or before anything
//! boots — the run ends `Cancelled` through the same terminal
//! `state_changed` door as every other ending, and no sandbox
//! outlives it. House pattern: assert the emitted events, the
//! outcome, and the seam effects, never internal call patterns.

use std::sync::Arc;

use async_trait::async_trait;
use engine::testkit::{
    self, RecordingSink, ScriptedAgent, ScriptedSandbox, StagedSandbox, kinds, reports, seeded_repo,
};
use engine::{
    CancelToken, EventSink, EventSinkError, Kernel, KernelContext, PipelineKernel, RunEvent,
    RunOutcome, RunState, Workspace,
};
use tokio::sync::Barrier;

fn assert_ended_cancelled(events: &[RunEvent]) {
    assert!(
        matches!(
            events.last().unwrap(),
            RunEvent::StateChanged {
                state: RunState::Cancelled
            }
        ),
        "{events:?}"
    );
}

/// A cancel that lands inside the minutes-long agent exec: the exec is
/// abandoned mid-stream, but the sandbox bracket still runs its
/// destroy — the leak `JoinHandle::abort` would have caused.
#[tokio::test]
async fn a_cancel_mid_exec_destroys_the_sandbox_and_ends_the_run_cancelled() {
    let workspace = seeded_repo();
    let barrier = Arc::new(Barrier::new(2));
    let sandbox = Arc::new(ScriptedSandbox::hanging().with_barrier(barrier.clone()));
    let sink = Arc::new(RecordingSink::default());
    let ctx = KernelContext {
        workspace: Workspace::at(workspace.path()),
        sandbox: sandbox.clone(),
        agent: Arc::new(ScriptedAgent::new()),
        events: sink.clone(),
        ..testkit::context()
    };

    // Fire the cancel only once the exec is provably in flight.
    let cancel = ctx.cancel.clone();
    let firing = tokio::spawn(async move {
        barrier.wait().await;
        cancel.cancel();
    });
    let outcome = PipelineKernel.run(ctx).await.unwrap();
    firing.await.unwrap();

    assert_eq!(outcome, RunOutcome::Cancelled);
    assert_eq!(
        kinds(&sink.events()),
        [
            "run_started",
            "iteration_started",
            "stage_started",
            "state_changed"
        ]
    );
    assert_ended_cancelled(&sink.events());
    assert_eq!(sandbox.created(), 1);
    assert_eq!(sandbox.destroyed(), 1);
}

/// A token that fired before the kernel reaches a stage boundary stops
/// the run there: no sandbox boots, and the ending is the same
/// terminal event as any other. The default context's `NoSandbox`
/// makes any boot a loud panic.
#[tokio::test]
async fn a_cancelled_token_ends_the_run_before_the_next_stage_boots() {
    let workspace = seeded_repo();
    let sink = Arc::new(RecordingSink::default());
    let ctx = KernelContext {
        workspace: Workspace::at(workspace.path()),
        events: sink.clone(),
        ..testkit::context()
    };
    ctx.cancel.cancel();

    let outcome = PipelineKernel.run(ctx).await.unwrap();

    assert_eq!(outcome, RunOutcome::Cancelled);
    assert_eq!(
        kinds(&sink.events()),
        ["run_started", "iteration_started", "state_changed"]
    );
    assert_ended_cancelled(&sink.events());
}

/// Fires the run's cancel token the moment a stage reports — the
/// deterministic stand-in for a cancel arriving while one stage winds
/// down and before the next begins.
struct CancelOnStageReported {
    inner: Arc<RecordingSink>,
    cancel: CancelToken,
}

#[async_trait]
impl EventSink for CancelOnStageReported {
    async fn emit(&self, event: RunEvent) -> Result<(), EventSinkError> {
        let fire = matches!(&event, RunEvent::StageReported { .. });
        self.inner.emit(event).await?;
        if fire {
            self.cancel.cancel();
        }
        Ok(())
    }
}

/// A cancel between stages: the finished stage's report stands, the
/// next stage never starts, and every sandbox that booted was torn
/// down.
#[tokio::test]
async fn a_cancel_between_stages_keeps_the_finished_stage_and_starts_no_other() {
    let workspace = seeded_repo();
    let sandbox = Arc::new(StagedSandbox::new(
        workspace.path().to_path_buf(),
        vec![reports("continue", "planned")],
    ));
    let cancel = CancelToken::new();
    let sink = Arc::new(RecordingSink::default());
    let ctx = KernelContext {
        cancel: cancel.clone(),
        workspace: Workspace::at(workspace.path()),
        sandbox: sandbox.clone(),
        agent: Arc::new(ScriptedAgent::new()),
        events: Arc::new(CancelOnStageReported {
            inner: sink.clone(),
            cancel,
        }),
        ..testkit::context()
    };

    let outcome = PipelineKernel.run(ctx).await.unwrap();

    assert_eq!(outcome, RunOutcome::Cancelled);
    // Plan ran to its report; implement never started.
    let kinds = kinds(&sink.events());
    assert!(kinds.contains(&"stage_reported".to_string()), "{kinds:?}");
    assert_eq!(
        kinds.iter().filter(|kind| *kind == "stage_started").count(),
        1,
        "{kinds:?}"
    );
    assert_ended_cancelled(&sink.events());
    assert_eq!(sandbox.created(), 1);
    assert_eq!(sandbox.destroyed(), 1);
}
