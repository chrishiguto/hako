//! The run store under the real kernel: `FileEventSink` serves the
//! ralph loop through the same seam the in-memory sink serves in the
//! ralph suite, and asserts the same event sequence — from the file.
//! Then the daemon-restart story: everything the run was comes back
//! from the directory alone.

use std::collections::VecDeque;
use std::path::Path;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use engine::workspace::{DOMAIN_PROMPT_FILE, PROGRESS_FILE};
use engine::{
    AgentAdapter, Budgets, EventSink, ExecEvent, ExecSpec, ExecStream, ExitStatus, Kernel,
    KernelContext, Notification, Notifier, NotifierError, PauseReason, RalphKernel, RunDir,
    RunEvent, RunId, RunOutcome, RunState, Sandbox, SandboxError, SandboxHandle, SandboxSpec,
    SecretName, SecretValue, SecretsError, SecretsProvider, TokenUsage, VerifyConfig, Workspace,
    WorkspaceMount,
};
use futures_util::{StreamExt, stream};
use serde_json::json;

/// What the scripted agent leaves behind in one sandbox: optionally a
/// workspace edit, always a progress report.
struct ScriptedIteration {
    edit: Option<(&'static str, &'static str)>,
    report: serde_json::Value,
}

/// Boots nothing; exec writes the next scripted edit and report
/// through the mount, like a real sandbox's rw mount would.
struct FakeSandbox {
    script: Mutex<VecDeque<ScriptedIteration>>,
    mount: Mutex<Option<WorkspaceMount>>,
}

impl FakeSandbox {
    fn scripted(script: Vec<ScriptedIteration>) -> Arc<Self> {
        Arc::new(Self {
            script: Mutex::new(script.into()),
            mount: Mutex::new(None),
        })
    }
}

#[async_trait]
impl Sandbox for FakeSandbox {
    async fn create(&self, spec: &SandboxSpec) -> Result<SandboxHandle, SandboxError> {
        *self.mount.lock().unwrap() = Some(spec.workspace.clone());
        Ok(SandboxHandle::new("vm"))
    }

    async fn exec_stream(
        &self,
        _sandbox: &SandboxHandle,
        _command: &ExecSpec,
    ) -> Result<ExecStream, SandboxError> {
        let step = self
            .script
            .lock()
            .unwrap()
            .pop_front()
            .expect("the agent was invoked beyond its script");
        let host = self.mount.lock().unwrap().as_ref().unwrap().host.clone();
        if let Some((path, contents)) = step.edit {
            std::fs::write(host.join(path), contents).unwrap();
        }
        let report_path = host.join(PROGRESS_FILE);
        std::fs::create_dir_all(report_path.parent().unwrap()).unwrap();
        std::fs::write(report_path, step.report.to_string()).unwrap();

        let transcript: Vec<Result<ExecEvent, SandboxError>> = vec![
            Ok(ExecEvent::Stdout(b"working\n".to_vec())),
            Ok(ExecEvent::Exited(ExitStatus { code: Some(0) })),
        ];
        Ok(stream::iter(transcript).boxed())
    }

    async fn put_file(
        &self,
        _sandbox: &SandboxHandle,
        _path: &Path,
        _contents: &[u8],
    ) -> Result<(), SandboxError> {
        unreachable!("the ralph kernel never uploads files");
    }

    async fn get_file(
        &self,
        _sandbox: &SandboxHandle,
        path: &Path,
    ) -> Result<Vec<u8>, SandboxError> {
        let mount = self.mount.lock().unwrap().clone().unwrap();
        let relative = path
            .strip_prefix(&mount.guest)
            .map_err(|_| SandboxError(format!("outside the mount: {}", path.display())))?;
        std::fs::read(mount.host.join(relative)).map_err(|error| SandboxError(error.to_string()))
    }

    async fn destroy(&self, _sandbox: SandboxHandle) -> Result<(), SandboxError> {
        Ok(())
    }

    async fn preflight(&self) -> Result<(), SandboxError> {
        Ok(())
    }
}

struct ScriptedAgent;

impl AgentAdapter for ScriptedAgent {
    fn name(&self) -> &str {
        "scripted"
    }

    fn required_secrets(&self) -> Vec<SecretName> {
        vec![]
    }

    fn invocation(&self, prompt: &str) -> ExecSpec {
        ExecSpec {
            argv: vec!["scripted-agent".into(), prompt.into()],
            cwd: None,
        }
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

fn seeded_workspace() -> (tempfile::TempDir, Workspace) {
    let dir = tempfile::tempdir().unwrap();
    git(dir.path(), &["init", "-q", "-b", "main"]);
    std::fs::write(
        dir.path().join(DOMAIN_PROMPT_FILE),
        "keep the build green\n",
    )
    .unwrap();
    git(dir.path(), &["add", "-A"]);
    git(
        dir.path(),
        &[
            "-c",
            "user.name=test",
            "-c",
            "user.email=test@localhost",
            "commit",
            "-qm",
            "seed",
        ],
    );
    let workspace = Workspace::at(dir.path());
    (dir, workspace)
}

fn git(dir: &Path, args: &[&str]) {
    let status = std::process::Command::new("git")
        .args(args)
        .current_dir(dir)
        .status()
        .unwrap();
    assert!(status.success(), "git {args:?}");
}

/// Runs the ralph kernel over the file sink of a freshly created run
/// directory — the exact wiring a daemon host will do.
async fn run_ralph_over(
    runs_root: &Path,
    script: Vec<ScriptedIteration>,
) -> (tempfile::TempDir, RunOutcome) {
    let (workspace_dir, workspace) = seeded_workspace();
    let run_dir = RunDir::create(runs_root, RunId::new("r1"), "ralph", "scripted")
        .await
        .unwrap();
    let sink: Arc<dyn EventSink> = Arc::new(run_dir.event_sink().await.unwrap());
    let ctx = KernelContext {
        run_id: RunId::new("r1"),
        budgets: Budgets::default(),
        verify: VerifyConfig::default(),
        workspace,
        resume: None,
        sandbox: FakeSandbox::scripted(script),
        agent: Arc::new(ScriptedAgent),
        events: sink,
        notifier: Arc::new(StubNotifier),
        secrets: Arc::new(NoSecrets),
    };
    let outcome = RalphKernel.run(ctx).await.unwrap();
    (workspace_dir, outcome)
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

/// The drop-in criterion: the same continue→done loop the ralph suite
/// drives through the in-memory sink, driven through the file sink —
/// same seam, same event sequence, read back from the log file.
#[tokio::test]
async fn the_file_sink_serves_the_ralph_kernel_as_a_drop_in() {
    let runs_root = tempfile::tempdir().unwrap();
    let (_workspace, outcome) = run_ralph_over(
        runs_root.path(),
        vec![
            ScriptedIteration {
                edit: Some(("store.rs", "store v1")),
                report: json!({
                    "status": "continue",
                    "summary": "wired the store",
                    "remaining": ["tests"],
                }),
            },
            ScriptedIteration {
                edit: None,
                report: json!({"status": "done", "summary": "all green"}),
            },
        ],
    )
    .await;

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
            "progress_reported",
            "iteration_finished",
            "iteration_started",
            "agent_output",
            "progress_reported",
            "iteration_finished",
            "state_changed",
        ]
    );
    let seqs: Vec<u64> = events.iter().map(|envelope| envelope.seq).collect();
    assert_eq!(seqs, (0..11).collect::<Vec<u64>>());
}

/// The daemon-restart story: nothing survives but the run directory,
/// and the run's identity, state, and full history all come back —
/// with the sink continuing the sequence where it stopped.
#[tokio::test]
async fn a_restarted_host_reconstructs_the_run_from_disk_alone() {
    let runs_root = tempfile::tempdir().unwrap();
    let (_workspace, outcome) = run_ralph_over(
        runs_root.path(),
        vec![ScriptedIteration {
            edit: None,
            report: json!({
                "status": "blocked",
                "summary": "the schema decision is not mine",
                "blockers": ["storage layout undecided"],
            }),
        }],
    )
    .await;
    assert_eq!(outcome, RunOutcome::Paused(PauseReason::Blocked));

    // Everything in memory is gone; the directory is all there is.
    let run_dir = RunDir::open(runs_root.path(), &RunId::new("r1"))
        .await
        .unwrap();
    assert_eq!(run_dir.meta().kernel, "ralph");
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
