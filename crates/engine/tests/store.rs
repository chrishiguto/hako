//! The run store under a kernel: `FileEventSink` serves a kernel
//! through the same seam the in-memory sink serves elsewhere, and the
//! event sequence reads back from the file. Then the daemon-restart
//! story: everything the run was comes back from the directory alone.
//!
//! The kernel here is a test-local fake that replays a scripted event
//! sequence — the store's contract is with the seam, not with any
//! particular loop.

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use engine::{
    AgentAdapter, Budgets, EventSink, ExecSpec, ExecStream, Kernel, KernelContext, KernelError,
    Notification, Notifier, NotifierError, PauseReason, PromptsConfig, RunDir, RunEvent, RunId,
    RunOutcome, RunState, Sandbox, SandboxError, SandboxHandle, SandboxSpec, SecretName,
    SecretValue, SecretsError, SecretsProvider, TokenUsage, VerifyConfig, Workspace,
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

/// The scripted kernel touches no sandbox; every call is a test bug.
struct NoSandbox;

#[async_trait]
impl Sandbox for NoSandbox {
    async fn create(&self, _spec: &SandboxSpec) -> Result<SandboxHandle, SandboxError> {
        unreachable!("the scripted kernel boots no sandbox");
    }

    async fn exec_stream(
        &self,
        _sandbox: &SandboxHandle,
        _command: &ExecSpec,
    ) -> Result<ExecStream, SandboxError> {
        unreachable!("the scripted kernel execs nothing");
    }

    async fn put_file(
        &self,
        _sandbox: &SandboxHandle,
        _path: &Path,
        _contents: &[u8],
    ) -> Result<(), SandboxError> {
        unreachable!("the scripted kernel uploads nothing");
    }

    async fn get_file(
        &self,
        _sandbox: &SandboxHandle,
        _path: &Path,
    ) -> Result<Vec<u8>, SandboxError> {
        unreachable!("the scripted kernel reads nothing");
    }

    async fn destroy(&self, _sandbox: SandboxHandle) -> Result<(), SandboxError> {
        unreachable!("the scripted kernel boots no sandbox");
    }

    async fn preflight(&self) -> Result<(), SandboxError> {
        Ok(())
    }
}

struct NoAgent;

impl AgentAdapter for NoAgent {
    fn name(&self) -> &str {
        "scripted"
    }

    fn required_secrets(&self) -> Vec<SecretName> {
        vec![]
    }

    fn invocation(&self, _prompt: &str) -> ExecSpec {
        unreachable!("the scripted kernel invokes no agent");
    }

    fn token_usage(&self, _stdout: &str) -> Option<TokenUsage> {
        None
    }
}

struct StubNotifier;

#[async_trait]
impl Notifier for StubNotifier {
    async fn notify(&self, _notification: &Notification) -> Result<(), NotifierError> {
        Ok(())
    }
}

struct NoSecrets;

#[async_trait]
impl SecretsProvider for NoSecrets {
    async fn resolve(&self, _name: &SecretName) -> Result<SecretValue, SecretsError> {
        Err(SecretsError::Provider("no secrets in this loop".into()))
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
        run_id: RunId::new("r1"),
        budgets: Budgets::default(),
        verify: VerifyConfig::default(),
        prompts: PromptsConfig::default(),
        workspace: Workspace::at(workspace_dir.path()),
        sandbox: Arc::new(NoSandbox),
        agent: Arc::new(NoAgent),
        events: sink,
        notifier: Arc::new(StubNotifier),
        secrets: Arc::new(NoSecrets),
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
        run_dir.state().await.unwrap(),
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
