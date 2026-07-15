//! The Ralph kernel driven end-to-end over fakes: a scripted agent, a
//! sandbox that boots nothing, an in-memory event sink — and a real
//! tempdir git repository as the workspace, because git effects are
//! asserted behavior, not infrastructure. House pattern: assert the
//! emitted event sequence, the run outcome, and the workspace — never
//! internal call patterns.

use std::collections::{BTreeMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use engine::workspace::{DOMAIN_PROMPT_FILE, PROGRESS_FILE};
use engine::{
    AgentAdapter, Budgets, EventSink, EventSinkError, ExecEvent, ExecSpec, ExecStream, ExitStatus,
    Kernel, KernelContext, Notification, Notifier, NotifierError, PauseReason, RalphKernel,
    RunEvent, RunId, RunOutcome, RunState, Sandbox, SandboxError, SandboxHandle, SandboxSpec,
    SecretName, SecretValue, SecretsError, SecretsProvider, TokenUsage, Workspace, WorkspaceMount,
};
use futures_util::{StreamExt, stream};
use proto::budget::BudgetKind;
use proto::event::IterationOutcome;
use serde_json::json;

const GOAL: &str = "close every open issue";
const DOMAIN_PROMPT: &str = "## Domain rules\n\nkeep the build green\n";

/// What the scripted agent does inside one sandbox: edit workspace
/// files, print output, leave (or not) a progress report, exit.
struct ScriptedIteration {
    files: Vec<(String, String)>,
    stdout: String,
    report: Option<String>,
    exit: ExitStatus,
}

fn reporting(report: serde_json::Value) -> ScriptedIteration {
    ScriptedIteration {
        files: vec![],
        stdout: "working\n".into(),
        report: Some(report.to_string()),
        exit: ExitStatus { code: Some(0) },
    }
}

fn continuing(summary: &str, remaining: &[&str]) -> ScriptedIteration {
    reporting(json!({"status": "continue", "summary": summary, "remaining": remaining}))
}

fn finishing(summary: &str) -> ScriptedIteration {
    reporting(json!({"status": "done", "summary": summary}))
}

impl ScriptedIteration {
    fn editing(mut self, path: &str, contents: &str) -> Self {
        self.files.push((path.into(), contents.into()));
        self
    }

    fn printing(mut self, stdout: &str) -> Self {
        self.stdout = stdout.into();
        self
    }
}

/// Boots nothing. Exec replays the next scripted iteration, writing
/// its files through the mount onto the host — exactly what a real
/// sandbox's rw mount does — and file reads resolve back through the
/// same mount.
#[derive(Default)]
struct FakeSandbox {
    script: Mutex<VecDeque<ScriptedIteration>>,
    mounts: Mutex<BTreeMap<String, WorkspaceMount>>,
    execs: Mutex<Vec<(String, ExecSpec)>>,
    created: AtomicU32,
    destroyed: AtomicU32,
}

impl FakeSandbox {
    fn scripted(iterations: Vec<ScriptedIteration>) -> Arc<Self> {
        Arc::new(Self {
            script: Mutex::new(iterations.into()),
            ..Self::default()
        })
    }

    fn prompt_of(&self, exec: usize) -> String {
        let execs = self.execs.lock().unwrap();
        let (_, spec) = &execs[exec];
        spec.argv.last().unwrap().clone()
    }

    fn host_path(
        &self,
        sandbox: &SandboxHandle,
        guest_path: &Path,
    ) -> Result<PathBuf, SandboxError> {
        let mounts = self.mounts.lock().unwrap();
        let mount = mounts
            .get(sandbox.as_str())
            .ok_or_else(|| SandboxError(format!("no such sandbox: {}", sandbox.as_str())))?;
        let relative = guest_path
            .strip_prefix(&mount.guest)
            .map_err(|_| SandboxError(format!("outside the mount: {}", guest_path.display())))?;
        Ok(mount.host.join(relative))
    }
}

#[async_trait]
impl Sandbox for FakeSandbox {
    async fn create(&self, spec: &SandboxSpec) -> Result<SandboxHandle, SandboxError> {
        assert!(spec.env.is_empty(), "the minimal loop injects no secrets");
        let n = self.created.fetch_add(1, Ordering::SeqCst);
        let handle = format!("vm-{n}");
        self.mounts
            .lock()
            .unwrap()
            .insert(handle.clone(), spec.workspace.clone());
        Ok(SandboxHandle::new(handle))
    }

    async fn exec_stream(
        &self,
        sandbox: &SandboxHandle,
        command: &ExecSpec,
    ) -> Result<ExecStream, SandboxError> {
        let step = self
            .script
            .lock()
            .unwrap()
            .pop_front()
            .expect("the agent was invoked beyond its script");
        let host = {
            let mounts = self.mounts.lock().unwrap();
            mounts[sandbox.as_str()].host.clone()
        };
        for (path, contents) in &step.files {
            let target = host.join(path);
            std::fs::create_dir_all(target.parent().unwrap()).unwrap();
            std::fs::write(target, contents).unwrap();
        }
        if let Some(report) = &step.report {
            let target = host.join(PROGRESS_FILE);
            std::fs::create_dir_all(target.parent().unwrap()).unwrap();
            std::fs::write(target, report).unwrap();
        }
        self.execs
            .lock()
            .unwrap()
            .push((sandbox.as_str().to_owned(), command.clone()));

        let transcript: Vec<Result<ExecEvent, SandboxError>> = vec![
            Ok(ExecEvent::Stdout(step.stdout.into_bytes())),
            Ok(ExecEvent::Exited(step.exit)),
        ];
        Ok(stream::iter(transcript).boxed())
    }

    async fn put_file(
        &self,
        _sandbox: &SandboxHandle,
        _path: &Path,
        _contents: &[u8],
    ) -> Result<(), SandboxError> {
        unreachable!("the ralph kernel never uploads files — work reaches it through the mount");
    }

    async fn get_file(
        &self,
        sandbox: &SandboxHandle,
        path: &Path,
    ) -> Result<Vec<u8>, SandboxError> {
        let source = self.host_path(sandbox, path)?;
        std::fs::read(source).map_err(|error| SandboxError(error.to_string()))
    }

    async fn destroy(&self, sandbox: SandboxHandle) -> Result<(), SandboxError> {
        self.destroyed.fetch_add(1, Ordering::SeqCst);
        self.mounts
            .lock()
            .unwrap()
            .remove(sandbox.as_str())
            .map(|_| ())
            .ok_or_else(|| SandboxError(format!("no such sandbox: {}", sandbox.as_str())))
    }

    async fn preflight(&self) -> Result<(), SandboxError> {
        Ok(())
    }
}

/// A pure translator, like every real adapter: prompt in, argv out.
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
            argv: vec!["scripted-agent".into(), "--prompt".into(), prompt.into()],
            cwd: None,
        }
    }

    fn token_usage(&self, stdout: &str) -> Option<TokenUsage> {
        stdout.contains("tokens used").then_some(TokenUsage {
            input: 12,
            output: 3,
        })
    }
}

#[derive(Default)]
struct RecordingSink {
    events: Mutex<Vec<RunEvent>>,
}

impl RecordingSink {
    fn events(&self) -> Vec<RunEvent> {
        self.events.lock().unwrap().clone()
    }

    /// Kinds come off the wire tag, so they cannot drift from the
    /// serde mapping `proto` owns.
    fn kinds(&self) -> Vec<String> {
        self.events()
            .iter()
            .map(|event| {
                serde_json::to_value(event).unwrap()["type"]
                    .as_str()
                    .unwrap()
                    .to_owned()
            })
            .collect()
    }
}

#[async_trait]
impl EventSink for RecordingSink {
    async fn emit(&self, event: RunEvent) -> Result<(), EventSinkError> {
        self.events.lock().unwrap().push(event);
        Ok(())
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
        Err(SecretsError::Provider(
            "the minimal loop resolves no secrets".into(),
        ))
    }
}

fn seeded_workspace() -> (tempfile::TempDir, Workspace) {
    let dir = tempfile::tempdir().unwrap();
    git(dir.path(), &["init", "-q", "-b", "main"]);
    std::fs::write(dir.path().join(DOMAIN_PROMPT_FILE), DOMAIN_PROMPT).unwrap();
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

/// Commit hashes oldest-first, excluding the seed commit.
fn checkpoints_in_history(dir: &Path) -> Vec<String> {
    let output = std::process::Command::new("git")
        .args(["log", "--reverse", "--format=%H"])
        .current_dir(dir)
        .output()
        .unwrap();
    String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .skip(1)
        .map(str::to_owned)
        .collect()
}

fn context(
    workspace: Workspace,
    sandbox: Arc<FakeSandbox>,
    sink: Arc<RecordingSink>,
    budgets: Budgets,
) -> KernelContext {
    KernelContext {
        run_id: RunId::new("r1"),
        goal: GOAL.into(),
        budgets,
        workspace,
        sandbox,
        agent: Arc::new(ScriptedAgent),
        events: sink,
        notifier: Arc::new(StubNotifier),
        secrets: Arc::new(NoSecrets),
    }
}

async fn run_ralph(
    script: Vec<ScriptedIteration>,
    budgets: Budgets,
) -> (
    tempfile::TempDir,
    Arc<FakeSandbox>,
    Arc<RecordingSink>,
    RunOutcome,
) {
    let (dir, workspace) = seeded_workspace();
    let sandbox = FakeSandbox::scripted(script);
    let sink = Arc::new(RecordingSink::default());
    let ctx = context(workspace, sandbox.clone(), sink.clone(), budgets);
    let outcome = RalphKernel.run(ctx).await.unwrap();
    (dir, sandbox, sink, outcome)
}

fn last_state(sink: &RecordingSink) -> RunState {
    match sink.events().last().unwrap() {
        RunEvent::StateChanged { state } => *state,
        other => panic!("the run must end with state_changed, not {other:?}"),
    }
}

#[tokio::test]
async fn a_continue_continue_done_run_completes_and_checkpoints_each_step() {
    let (dir, sandbox, sink, outcome) = run_ralph(
        vec![
            continuing("wired the store", &["docs", "tests"]).editing("store.rs", "store v1"),
            continuing("wrote the docs", &["tests"]).editing("docs.md", "how it works"),
            finishing("all green"),
        ],
        Budgets::default(),
    )
    .await;

    assert_eq!(outcome, RunOutcome::Done);
    assert_eq!(last_state(&sink), RunState::Done);
    assert_eq!(
        sink.kinds(),
        [
            "run_started",
            "iteration_started",
            "agent_output",
            "workspace_checkpointed",
            "progress_reported",
            "iteration_finished",
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

    let events = sink.events();
    assert_eq!(
        events[0],
        RunEvent::RunStarted {
            goal: GOAL.into(),
            kernel: "ralph".into(),
            agent: "scripted".into(),
        }
    );

    // The workspace carries the loop's work, committed iteration by
    // iteration, and the events reference exactly those commits.
    assert!(dir.path().join("store.rs").exists());
    assert!(dir.path().join("docs.md").exists());
    let referenced: Vec<(u32, String)> = events
        .iter()
        .filter_map(|event| match event {
            RunEvent::WorkspaceCheckpointed { iteration, commit } => {
                Some((*iteration, commit.clone()))
            }
            _ => None,
        })
        .collect();
    let history = checkpoints_in_history(dir.path());
    assert_eq!(history.len(), 2);
    assert_eq!(
        referenced,
        [(1, history[0].clone()), (2, history[1].clone())]
    );

    // Nothing survives between iterations except the workspace: every
    // iteration got its own sandbox, each invoked exactly once, and
    // every sandbox was destroyed.
    assert_eq!(sandbox.created.load(Ordering::SeqCst), 3);
    assert_eq!(sandbox.destroyed.load(Ordering::SeqCst), 3);
    let execs = sandbox.execs.lock().unwrap();
    let handles: Vec<&str> = execs.iter().map(|(handle, _)| handle.as_str()).collect();
    assert_eq!(handles, ["vm-0", "vm-1", "vm-2"]);
}

#[tokio::test]
async fn a_blocked_report_pauses_the_run_with_reason_blocked() {
    let (_dir, _sandbox, sink, outcome) = run_ralph(
        vec![reporting(json!({
            "status": "blocked",
            "summary": "the API key has expired",
            "blockers": ["no valid credentials"],
        }))],
        Budgets::default(),
    )
    .await;

    assert_eq!(outcome, RunOutcome::Paused(PauseReason::Blocked));
    assert_eq!(
        last_state(&sink),
        RunState::Paused {
            reason: PauseReason::Blocked
        }
    );
}

#[tokio::test]
async fn a_needs_input_report_pauses_the_run_for_the_human() {
    let (_dir, _sandbox, sink, outcome) = run_ralph(
        vec![reporting(json!({
            "status": "needs_input",
            "summary": "two storage designs are viable",
            "questions": [{"id": "q1", "text": "sqlite or plain files?", "options": []}],
        }))],
        Budgets::default(),
    )
    .await;

    assert_eq!(outcome, RunOutcome::Paused(PauseReason::AwaitingHuman));
    assert_eq!(
        last_state(&sink),
        RunState::Paused {
            reason: PauseReason::AwaitingHuman
        }
    );
}

/// Budgets come from a real flow file here, covering the submit path:
/// flow TOML in, kernel behavior out.
#[tokio::test]
async fn exhausting_max_iterations_finishes_the_iteration_then_pauses_resumably() {
    let flow = proto::flow::FlowConfig::from_toml(
        r#"
        [loop]
        kernel = "ralph"
        goal = "close every open issue"

        [agent]
        engine = "scripted"

        [budget]
        max_iterations = 2

        [workspace]
        repo = "."
        "#,
    )
    .unwrap();

    let (_dir, sandbox, sink, outcome) = run_ralph(
        vec![
            continuing("first step", &["more"]),
            continuing("second step", &["more still"]),
            continuing("never runs", &[]),
        ],
        Budgets::from(&flow.budget),
    )
    .await;

    assert_eq!(outcome, RunOutcome::Paused(PauseReason::Budget));
    assert_eq!(
        sink.kinds(),
        [
            "run_started",
            "iteration_started",
            "agent_output",
            "progress_reported",
            "iteration_finished",
            "iteration_started",
            "agent_output",
            "progress_reported",
            "iteration_finished",
            "budget_exhausted",
            "state_changed",
        ]
    );
    assert!(sink.events().contains(&RunEvent::BudgetExhausted {
        budget: BudgetKind::Iterations
    }));
    assert_eq!(
        last_state(&sink),
        RunState::Paused {
            reason: PauseReason::Budget
        }
    );
    // The second iteration ran to completion before the pause, and no
    // sandbox outlived it.
    assert_eq!(sandbox.created.load(Ordering::SeqCst), 2);
    assert_eq!(sandbox.destroyed.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn the_preamble_frames_the_domain_prompt_with_goal_position_history_and_contract() {
    let budgets = Budgets {
        max_iterations: Some(5),
        ..Budgets::default()
    };
    let (_dir, sandbox, _sink, outcome) = run_ralph(
        vec![
            continuing("wired the store", &["docs", "tests"]),
            finishing("all done"),
        ],
        budgets,
    )
    .await;
    assert_eq!(outcome, RunOutcome::Done);

    let first = sandbox.prompt_of(0);
    assert!(first.contains(GOAL), "{first}");
    assert!(first.contains("iteration 1 of 5"), "{first}");
    assert!(first.contains(PROGRESS_FILE), "{first}");
    assert!(!first.contains("Previous iteration"), "{first}");
    // The domain prompt closes the prompt; the frame precedes it.
    assert!(
        first.trim_end().ends_with("keep the build green"),
        "{first}"
    );
    assert!(first.find(GOAL).unwrap() < first.find("Domain rules").unwrap());

    let second = sandbox.prompt_of(1);
    assert!(second.contains("iteration 2 of 5"), "{second}");
    assert!(second.contains("wired the store"), "{second}");
    assert!(second.contains("- docs\n- tests"), "{second}");
}

#[tokio::test]
async fn a_crashed_agent_fails_the_iteration_and_the_run() {
    let crash = ScriptedIteration {
        files: vec![("junk.txt".into(), "half-finished".into())],
        stdout: "panic!\n".into(),
        report: None,
        exit: ExitStatus { code: Some(1) },
    };
    let (dir, sandbox, sink, outcome) = run_ralph(vec![crash], Budgets::default()).await;

    assert_eq!(outcome, RunOutcome::Failed);
    assert_eq!(
        sink.kinds(),
        [
            "run_started",
            "iteration_started",
            "agent_output",
            "iteration_finished",
            "state_changed",
        ]
    );
    assert!(sink.events().contains(&RunEvent::IterationFinished {
        iteration: 1,
        outcome: IterationOutcome::Failed,
    }));
    assert_eq!(last_state(&sink), RunState::Failed);
    // A crashed iteration is not progress: nothing was committed, and
    // the sandbox still came down.
    assert!(checkpoints_in_history(dir.path()).is_empty());
    assert_eq!(sandbox.destroyed.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_malformed_progress_report_is_rejected_and_fails_the_run() {
    let garbled = reporting(json!({}));
    let garbled = ScriptedIteration {
        report: Some("this is not json".into()),
        ..garbled
    };
    let (_dir, sandbox, sink, outcome) = run_ralph(vec![garbled], Budgets::default()).await;

    assert_eq!(outcome, RunOutcome::Failed);
    assert_eq!(
        sink.kinds(),
        [
            "run_started",
            "iteration_started",
            "agent_output",
            "progress_rejected",
            "iteration_finished",
            "state_changed",
        ]
    );
    let events = sink.events();
    let errors = events
        .iter()
        .find_map(|event| match event {
            RunEvent::ProgressRejected { errors, .. } => Some(errors.clone()),
            _ => None,
        })
        .unwrap();
    assert!(!errors.is_empty());
    assert_eq!(sandbox.destroyed.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_missing_progress_report_is_rejected_and_fails_the_run() {
    let silent = ScriptedIteration {
        files: vec![],
        stdout: "did things, reported nothing\n".into(),
        report: None,
        exit: ExitStatus { code: Some(0) },
    };
    let (_dir, _sandbox, sink, outcome) = run_ralph(vec![silent], Budgets::default()).await;

    assert_eq!(outcome, RunOutcome::Failed);
    let events = sink.events();
    let errors = events
        .iter()
        .find_map(|event| match event {
            RunEvent::ProgressRejected { errors, .. } => Some(errors.clone()),
            _ => None,
        })
        .unwrap();
    assert!(errors[0].contains("missing"), "{errors:?}");
    assert_eq!(last_state(&sink), RunState::Failed);
}

#[tokio::test]
async fn token_usage_reported_by_the_adapter_lands_in_the_log() {
    let (_dir, _sandbox, sink, outcome) = run_ralph(
        vec![finishing("all done").printing("tokens used: some\n")],
        Budgets::default(),
    )
    .await;

    assert_eq!(outcome, RunOutcome::Done);
    assert_eq!(
        sink.kinds(),
        [
            "run_started",
            "iteration_started",
            "agent_output",
            "tokens_used",
            "progress_reported",
            "iteration_finished",
            "state_changed",
        ]
    );
    assert!(sink.events().contains(&RunEvent::TokensUsed {
        iteration: 1,
        usage: TokenUsage {
            input: 12,
            output: 3
        },
    }));
}
