//! The run store under a kernel: `FileEventSink` serves a kernel
//! through the same seam the in-memory sink serves elsewhere, and the
//! event sequence reads back from the file. Then the daemon-restart
//! story: everything the run was comes back from the directory alone.
//!
//! The kernel here is a test-local fake that replays a scripted event
//! sequence — the store's contract is with the seam, not with any
//! particular loop. Every other collaborator is the testkit's inert
//! default: this kernel touches no sandbox and invokes no agent, so
//! any such call is a test bug the defaults turn into a panic.

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use engine::testkit;
use engine::{
    EventSink, Kernel, KernelContext, KernelError, PauseReason, RunDir, RunEvent, RunId,
    RunOutcome, RunState, Workspace,
};
use proto::event::{IterationOutcome, OutputStream};

/// Replays a scripted event sequence through the sink and ends with
/// the scripted outcome — a kernel-shaped probe for the store.
struct ScriptedKernel {
    events: Vec<RunEvent>,
    outcome: RunOutcome,
}

#[async_trait]
impl Kernel for ScriptedKernel {
    async fn run(&self, ctx: KernelContext) -> Result<RunOutcome, KernelError> {
        for event in &self.events {
            ctx.events.emit(event.clone()).await?;
        }
        Ok(self.outcome)
    }
}

/// Runs a scripted kernel over the file sink of a freshly created run
/// directory — the exact wiring a daemon host will do.
async fn run_scripted(runs_root: &Path, events: Vec<RunEvent>, outcome: RunOutcome) -> RunOutcome {
    let run_dir = RunDir::create(runs_root, RunId::new("r1"), "pipeline", "scripted")
        .await
        .unwrap();
    let sink: Arc<dyn EventSink> = Arc::new(run_dir.event_sink().await.unwrap());
    let workspace_dir = tempfile::tempdir().unwrap();
    let ctx = KernelContext {
        workspace: Workspace::at(workspace_dir.path()),
        events: sink,
        ..testkit::context()
    };
    ScriptedKernel { events, outcome }.run(ctx).await.unwrap()
}

fn kinds(events: &[engine::EventEnvelope]) -> Vec<String> {
    events
        .iter()
        .map(|envelope| {
            serde_json::to_value(&envelope.event).unwrap()["type"]
                .as_str()
                .unwrap()
                .to_owned()
        })
        .collect()
}

fn started() -> RunEvent {
    RunEvent::RunStarted {
        kernel: "scripted".into(),
        agent: "scripted".into(),
    }
}

/// The drop-in criterion: a full iteration's event sequence driven
/// through the file sink via the kernel seam — same seam as the
/// in-memory sinks, read back from the log file with stable ids.
#[tokio::test]
async fn the_file_sink_serves_a_kernel_as_a_drop_in() {
    let runs_root = tempfile::tempdir().unwrap();
    let script = vec![
        started(),
        RunEvent::IterationStarted { iteration: 1 },
        RunEvent::AgentOutput {
            iteration: 1,
            stream: OutputStream::Stdout,
            chunk: "working\n".into(),
        },
        RunEvent::WorkspaceCheckpointed {
            iteration: 1,
            commit: "a1b2c3d".into(),
        },
        RunEvent::IterationFinished {
            iteration: 1,
            outcome: IterationOutcome::Completed,
        },
        RunEvent::StateChanged {
            state: RunState::Done,
        },
    ];
    let outcome = run_scripted(runs_root.path(), script, RunOutcome::Done).await;

    assert_eq!(outcome, RunOutcome::Done);

    let run_dir = RunDir::open(runs_root.path(), &RunId::new("r1"))
        .await
        .unwrap();
    let events = run_dir.events().await.unwrap();
    assert_eq!(
        kinds(&events),
        [
            "run_started",
            "iteration_started",
            "agent_output",
            "workspace_checkpointed",
            "iteration_finished",
            "state_changed",
        ]
    );
    let seqs: Vec<u64> = events.iter().map(|envelope| envelope.seq).collect();
    assert_eq!(seqs, (0..6).collect::<Vec<u64>>());
}

/// The daemon-restart story: nothing survives but the run directory,
/// and the run's identity, state, and full history all come back —
/// with the sink continuing the sequence where it stopped.
#[tokio::test]
async fn a_restarted_host_reconstructs_the_run_from_disk_alone() {
    let runs_root = tempfile::tempdir().unwrap();
    let script = vec![
        started(),
        RunEvent::StateChanged {
            state: RunState::Paused {
                reason: PauseReason::Blocked,
            },
        },
    ];
    let outcome = run_scripted(
        runs_root.path(),
        script,
        RunOutcome::Paused(PauseReason::Blocked),
    )
    .await;
    assert_eq!(outcome, RunOutcome::Paused(PauseReason::Blocked));

    // Everything in memory is gone; the directory is all there is.
    let run_dir = RunDir::open(runs_root.path(), &RunId::new("r1"))
        .await
        .unwrap();
    assert_eq!(run_dir.meta().kernel, "pipeline");
    assert_eq!(run_dir.meta().agent, "scripted");
    assert_eq!(
        run_dir.project().await.unwrap().state,
        RunState::Paused {
            reason: PauseReason::Blocked
        }
    );

    let events = run_dir.events().await.unwrap();
    assert_eq!(*kinds(&events).last().unwrap(), "state_changed");

    // A resume appends to the same log; ids stay stable across the
    // restart, which is what SSE Last-Event-ID replay will lean on.
    let sink = run_dir.event_sink().await.unwrap();
    sink.emit(RunEvent::RunResumed { note: None })
        .await
        .unwrap();
    let resumed = run_dir.events().await.unwrap();
    assert_eq!(resumed.len(), events.len() + 1);
    assert_eq!(resumed.last().unwrap().seq, events.len() as u64);
}
