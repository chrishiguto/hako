//! The pipeline kernel driven entirely over fakes: scripted per-stage
//! agents, a fake sandbox, a real tempdir git repo. House pattern —
//! assert the emitted events, the run outcome, and the git effects,
//! never internal call patterns.
//!
//! The fake sandbox writes to the real workspace on disk: each agent
//! exec drops a unique work file (so a mutating stage's checkpoint has
//! something to commit) and the stage's report under `.hako/` (fetched
//! back through `get_file`, and excluded from history by the
//! checkpoint). Verify checks are separate execs, distinguished from
//! the agent by their argv.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use engine::{
    AgentAdapter, Budgets, EventSink, EventSinkError, ExecEvent, ExecSpec, ExecStream, ExitStatus,
    FailAction, IterationOutcome, Kernel, KernelContext, Notification, Notifier, NotifierError,
    OnFail, PauseReason, PipelineKernel, PromptsConfig, RunEvent, RunId, RunOutcome, RunState,
    Sandbox, SandboxError, SandboxHandle, SandboxSpec, SecretName, SecretValue, SecretsError,
    SecretsProvider, TokenUsage, VerifyConfig, Workspace,
};
use futures_util::{StreamExt, stream};

/// The agent adapter's binary name — argv[0] of an agent invocation,
/// which is how the fake tells an agent exec from a verify check.
const AGENT_BIN: &str = "scripted-agent";

/// The guest mount point every workspace lands at; the fake strips it
/// to reach the real host path.
const GUEST_ROOT: &str = "/workspace";

// ---------- one scripted stage attempt ----------

/// One agent exec the fake will serve: what it prints, how it exits,
/// and the report it leaves (or `None` to leave none, so the kernel
/// sees a missing report).
struct AgentStep {
    stdout: String,
    code: i32,
    report: Option<String>,
}

/// A clean attempt: the agent works, exits zero, and leaves this
/// report.
fn reports(status: &str, summary: &str) -> AgentStep {
    AgentStep {
        stdout: "working\n".into(),
        code: 0,
        report: Some(report_json(status, summary)),
    }
}

/// An attempt that leaves a report serde will reject — an unknown field
/// — so the kernel offers its one repair.
fn malformed() -> AgentStep {
    AgentStep {
        stdout: "working\n".into(),
        code: 0,
        report: Some(r#"{"status": "continue", "summary": "x", "mystery": 1}"#.into()),
    }
}

/// An attempt that crashes: non-zero exit, nothing to trust.
fn crashes() -> AgentStep {
    AgentStep {
        stdout: "boom\n".into(),
        code: 1,
        report: None,
    }
}

/// A clean agent exit that does not touch the shared report path.
fn omits_report() -> AgentStep {
    AgentStep {
        stdout: "forgot to report\n".into(),
        code: 0,
        report: None,
    }
}

fn report_json(status: &str, summary: &str) -> String {
    serde_json::json!({"status": status, "summary": summary}).to_string()
}

// ---------- the fakes ----------

/// A fake sandbox over a real workspace. Serves a queue of agent steps
/// and a queue of verify-check exit codes; an exhausted check queue
/// passes by default, so only failures need scripting.
struct FakeSandbox {
    workspace_root: PathBuf,
    agent_steps: Mutex<VecDeque<AgentStep>>,
    checks: Mutex<VecDeque<i32>>,
    agent_prompts: Mutex<Vec<String>>,
    guest_files: Mutex<BTreeMap<PathBuf, Vec<u8>>>,
    created: AtomicU32,
    destroyed: AtomicU32,
    work_files: AtomicU32,
}

impl FakeSandbox {
    fn new(workspace_root: PathBuf, agent_steps: Vec<AgentStep>, checks: Vec<i32>) -> Arc<Self> {
        Arc::new(Self {
            workspace_root,
            agent_steps: Mutex::new(agent_steps.into()),
            checks: Mutex::new(checks.into()),
            agent_prompts: Mutex::new(Vec::new()),
            guest_files: Mutex::new(BTreeMap::new()),
            created: AtomicU32::new(0),
            destroyed: AtomicU32::new(0),
            work_files: AtomicU32::new(0),
        })
    }

    fn agent_prompts(&self) -> Vec<String> {
        self.agent_prompts.lock().unwrap().clone()
    }

    fn seed_guest_file(&self, path: impl Into<PathBuf>, contents: impl Into<Vec<u8>>) {
        self.guest_files
            .lock()
            .unwrap()
            .insert(path.into(), contents.into());
    }

    /// Maps a guest path back to the real host workspace — the fake's
    /// stand-in for the mount every real sandbox shares.
    fn host_path(&self, guest: &Path) -> PathBuf {
        let relative = guest.strip_prefix(GUEST_ROOT).unwrap_or(guest);
        self.workspace_root.join(relative)
    }

    /// Serves one agent exec: record its prompt, drop a unique work
    /// file (real change for the checkpoint), and lay down or clear the
    /// report the kernel will fetch.
    fn run_agent(&self, command: &ExecSpec) -> Transcript {
        let step = self
            .agent_steps
            .lock()
            .unwrap()
            .pop_front()
            .expect("an agent exec ran beyond its script");
        self.agent_prompts
            .lock()
            .unwrap()
            .push(command.argv.get(2).cloned().unwrap_or_default());

        let n = self.work_files.fetch_add(1, Ordering::SeqCst);
        std::fs::write(self.workspace_root.join(format!("work-{n}.txt")), "work\n").unwrap();

        let report_path = self.workspace_root.join(".hako/report.json");
        // Leave a previous report untouched when the agent omits one:
        // freshness belongs to the invocation executor, not this fake.
        if let Some(report) = &step.report {
            std::fs::create_dir_all(report_path.parent().unwrap()).unwrap();
            std::fs::write(&report_path, report).unwrap();
        }
        vec![
            Ok(ExecEvent::Stdout(step.stdout.into_bytes())),
            Ok(ExecEvent::Exited(ExitStatus {
                code: Some(step.code),
            })),
        ]
    }

    /// Serves one verify check: its scripted exit code, or a pass when
    /// the queue is empty.
    fn run_check(&self) -> Transcript {
        let code = self.checks.lock().unwrap().pop_front().unwrap_or(0);
        let line = if code == 0 {
            "ok\n"
        } else {
            "assertion failed: boom\n"
        };
        vec![
            Ok(ExecEvent::Stdout(line.as_bytes().to_vec())),
            Ok(ExecEvent::Exited(ExitStatus { code: Some(code) })),
        ]
    }
}

type Transcript = Vec<Result<ExecEvent, SandboxError>>;

#[async_trait]
impl Sandbox for FakeSandbox {
    async fn create(&self, _spec: &SandboxSpec) -> Result<SandboxHandle, SandboxError> {
        let n = self.created.fetch_add(1, Ordering::SeqCst);
        Ok(SandboxHandle::new(format!("vm-{n}")))
    }

    async fn exec_stream(
        &self,
        _sandbox: &SandboxHandle,
        command: &ExecSpec,
    ) -> Result<ExecStream, SandboxError> {
        let transcript = if command.argv.first().is_some_and(|arg| arg == AGENT_BIN) {
            self.run_agent(command)
        } else {
            self.run_check()
        };
        Ok(stream::iter(transcript).boxed())
    }

    async fn put_file(
        &self,
        _sandbox: &SandboxHandle,
        _path: &Path,
        _contents: &[u8],
    ) -> Result<(), SandboxError> {
        unreachable!("the pipeline kernel passes prompts as argv, never as files");
    }

    async fn get_file(
        &self,
        _sandbox: &SandboxHandle,
        path: &Path,
    ) -> Result<Vec<u8>, SandboxError> {
        if let Some(contents) = self.guest_files.lock().unwrap().get(path).cloned() {
            return Ok(contents);
        }
        std::fs::read(self.host_path(path))
            .map_err(|error| SandboxError(format!("no such file {}: {error}", path.display())))
    }

    async fn remove_file(&self, _sandbox: &SandboxHandle, path: &Path) -> Result<(), SandboxError> {
        match std::fs::remove_file(self.host_path(path)) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(SandboxError(format!(
                "cannot remove {}: {error}",
                path.display()
            ))),
        }
    }

    async fn destroy(&self, _sandbox: SandboxHandle) -> Result<(), SandboxError> {
        self.destroyed.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn preflight(&self) -> Result<(), SandboxError> {
        Ok(())
    }
}

/// A pure translator: prompt in, argv out, argv[0] the marker the fake
/// keys on.
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
            argv: vec![AGENT_BIN.into(), "--prompt".into(), prompt.into()],
            cwd: None,
        }
    }

    fn token_usage(&self, _stdout: &str) -> Option<TokenUsage> {
        None
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
        Err(SecretsError::Provider("no secrets in this loop".into()))
    }
}

// ---------- git fixtures ----------

fn git(dir: &Path, args: &[&str]) {
    let status = std::process::Command::new("git")
        .args(args)
        .current_dir(dir)
        .status()
        .unwrap();
    assert!(status.success(), "git {args:?}");
}

/// A repository on branch `main` with one committed file — what a
/// prepared workspace looks like before the loop runs.
fn seeded_repo() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    git(dir.path(), &["init", "-q", "-b", "main"]);
    std::fs::write(dir.path().join("README.md"), "seed\n").unwrap();
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
    dir
}

fn tracked_files(dir: &Path) -> Vec<String> {
    let out = std::process::Command::new("git")
        .args(["ls-files"])
        .current_dir(dir)
        .output()
        .unwrap();
    String::from_utf8(out.stdout)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect()
}

// ---------- the harness ----------

/// What one pipeline run left behind for the assertions.
struct Ran {
    outcome: RunOutcome,
    events: Vec<RunEvent>,
    prompts: Vec<String>,
    sandbox: Arc<FakeSandbox>,
    workspace: tempfile::TempDir,
}

fn verifying(checks: &[&str], retries: u32, then: FailAction) -> VerifyConfig {
    VerifyConfig {
        checks: checks.iter().map(|c| (*c).to_string()).collect(),
        on_fail: OnFail { retries, then },
    }
}

fn context(
    workspace: &Path,
    sandbox: Arc<FakeSandbox>,
    verify: VerifyConfig,
    prompts: PromptsConfig,
) -> (KernelContext, Arc<RecordingSink>) {
    let sink = Arc::new(RecordingSink::default());
    let ctx = KernelContext {
        run_id: RunId::new("r1"),
        budgets: Budgets::default(),
        verify,
        prompts,
        workspace: Workspace::at(workspace),
        sandbox,
        agent: Arc::new(ScriptedAgent),
        events: sink.clone(),
        notifier: Arc::new(StubNotifier),
        secrets: Arc::new(NoSecrets),
    };
    (ctx, sink)
}

/// Runs the pipeline kernel over a fresh seeded repo with the given
/// flow verify config and prompt overrides, serving `agent_steps` and
/// `checks` from the fake.
async fn run_pipeline(
    verify: VerifyConfig,
    prompts: PromptsConfig,
    agent_steps: Vec<AgentStep>,
    checks: Vec<i32>,
) -> Ran {
    let workspace = seeded_repo();
    let sandbox = FakeSandbox::new(workspace.path().to_path_buf(), agent_steps, checks);
    let (ctx, sink) = context(workspace.path(), sandbox.clone(), verify, prompts);
    let outcome = PipelineKernel.run(ctx).await.unwrap();
    Ran {
        outcome,
        events: sink.events(),
        prompts: sandbox.agent_prompts(),
        sandbox,
        workspace,
    }
}

/// The default flow: one verify check, pause on exhausted retries, no
/// prompt overrides. Most tests only vary the scripted agent.
async fn run_default(agent_steps: Vec<AgentStep>, checks: Vec<i32>) -> Ran {
    run_pipeline(
        verifying(&["check"], 1, FailAction::Pause),
        PromptsConfig::default(),
        agent_steps,
        checks,
    )
    .await
}

fn kinds(events: &[RunEvent]) -> Vec<String> {
    events.iter().map(kind).collect()
}

fn kind(event: &RunEvent) -> String {
    serde_json::to_value(event).unwrap()["type"]
        .as_str()
        .unwrap()
        .to_owned()
}

/// The stage-scoped events in order, as `(kind, stage)` pairs.
fn stage_events(events: &[RunEvent]) -> Vec<(String, String)> {
    events
        .iter()
        .filter_map(|event| match event {
            RunEvent::StageStarted { stage, .. } => Some(("stage_started".into(), stage.clone())),
            RunEvent::StageReported { stage, .. } => Some(("stage_reported".into(), stage.clone())),
            _ => None,
        })
        .collect()
}

// ---------- AC 1: the stage event sequence, one sandbox per stage ----------

#[tokio::test]
async fn a_full_iteration_emits_the_staged_event_sequence_one_sandbox_per_stage() {
    let ran = run_default(
        vec![
            reports("continue", "planned"),
            reports("continue", "built"),
            reports("continue", "reviewed"),
            reports("done", "simplified and complete"),
        ],
        vec![],
    )
    .await;

    assert_eq!(ran.outcome, RunOutcome::Done);
    assert_eq!(
        kinds(&ran.events),
        [
            "run_started",
            "iteration_started",
            "stage_started", // plan — no checkpoint, no verify
            "agent_output",
            "stage_reported",
            "stage_started", // implement — mutating
            "agent_output",
            "workspace_checkpointed",
            "stage_reported",
            "verify_check_finished",
            "stage_started", // review
            "agent_output",
            "workspace_checkpointed",
            "stage_reported",
            "verify_check_finished",
            "stage_started", // simplify — claims done
            "agent_output",
            "workspace_checkpointed",
            "stage_reported",
            "verify_check_finished",
            "state_changed", // done — no iteration_finished, the run ended
        ]
    );
    // The stages ran in kernel order, each announced and each reported.
    assert_eq!(
        stage_events(&ran.events),
        [
            ("stage_started".into(), "plan".into()),
            ("stage_reported".into(), "plan".into()),
            ("stage_started".into(), "implement".into()),
            ("stage_reported".into(), "implement".into()),
            ("stage_started".into(), "review".into()),
            ("stage_reported".into(), "review".into()),
            ("stage_started".into(), "simplify".into()),
            ("stage_reported".into(), "simplify".into()),
        ]
    );
    // One fresh sandbox per stage, every one torn down.
    assert_eq!(ran.sandbox.created.load(Ordering::SeqCst), 4);
    assert_eq!(
        ran.sandbox.created.load(Ordering::SeqCst),
        ran.sandbox.destroyed.load(Ordering::SeqCst)
    );
}

// ---------- AC 2: preamble carries prior reports and the stage's schema ----------

#[tokio::test]
async fn each_stage_preamble_carries_prior_reports_and_its_own_schema() {
    let ran = run_default(
        vec![
            reports("continue", "PLAN-MARKER"),
            reports("continue", "IMPL-MARKER"),
            reports("continue", "reviewed"),
            reports("done", "done"),
        ],
        vec![],
    )
    .await;

    let [plan, implement, review, _simplify] = ran.prompts.as_slice() else {
        panic!("expected four stage prompts, got {}", ran.prompts.len());
    };

    // The first plan has no hand-off — nothing came before it — but
    // still quotes its own report contract.
    assert!(!plan.contains("## Reports so far"), "{plan}");
    assert!(plan.contains("\"title\": \"PlanReport\""), "{plan}");

    // Implement reads the plan's report; review reads plan and
    // implement. Each quotes its own schema, never another stage's.
    assert!(implement.contains("## Reports so far"), "{implement}");
    assert!(implement.contains("### plan report"), "{implement}");
    assert!(implement.contains("PLAN-MARKER"), "{implement}");
    assert!(
        implement.contains("\"title\": \"ImplementReport\""),
        "{implement}"
    );

    assert!(review.contains("### plan report"), "{review}");
    assert!(review.contains("### implement report"), "{review}");
    assert!(review.contains("IMPL-MARKER"), "{review}");
    assert!(review.contains("\"title\": \"ReviewReport\""), "{review}");
}

#[tokio::test]
async fn a_prompt_override_replaces_the_shipped_default() {
    let workspace = seeded_repo();
    std::fs::create_dir(workspace.path().join("prompts")).unwrap();
    std::fs::write(
        workspace.path().join("prompts/plan.md"),
        "CUSTOM PLAN RULES\n",
    )
    .unwrap();
    let sandbox = FakeSandbox::new(
        workspace.path().to_path_buf(),
        vec![reports("done", "done")],
        vec![],
    );
    let prompts: PromptsConfig =
        serde_json::from_value(serde_json::json!({"plan": "prompts/plan.md"})).unwrap();
    let (ctx, _) = context(
        workspace.path(),
        sandbox.clone(),
        VerifyConfig::default(),
        prompts,
    );
    let outcome = PipelineKernel.run(ctx).await.unwrap();

    assert_eq!(outcome, RunOutcome::Done);
    let plan = &sandbox.agent_prompts()[0];
    assert!(plan.contains("CUSTOM PLAN RULES"), "{plan}");
}

#[cfg(unix)]
#[tokio::test]
async fn a_prompt_symlink_is_dereferenced_inside_the_sandbox() {
    use std::os::unix::fs::symlink;

    let workspace = seeded_repo();
    let outside = tempfile::tempdir().unwrap();
    let host_secret = outside.path().join("host-secret");
    std::fs::write(&host_secret, "HOST SECRET\n").unwrap();
    std::fs::create_dir(workspace.path().join("prompts")).unwrap();
    symlink(&host_secret, workspace.path().join("prompts/plan.md")).unwrap();

    let sandbox = FakeSandbox::new(
        workspace.path().to_path_buf(),
        vec![reports("done", "done")],
        vec![],
    );
    sandbox.seed_guest_file(
        "/workspace/prompts/plan.md",
        b"GUEST PROMPT CONTENT\n".to_vec(),
    );
    let prompts: PromptsConfig =
        serde_json::from_value(serde_json::json!({"plan": "prompts/plan.md"})).unwrap();
    let (ctx, _) = context(
        workspace.path(),
        sandbox.clone(),
        VerifyConfig::default(),
        prompts,
    );

    assert_eq!(PipelineKernel.run(ctx).await.unwrap(), RunOutcome::Done);
    let prompt = &sandbox.agent_prompts()[0];
    assert!(prompt.contains("GUEST PROMPT CONTENT"), "{prompt}");
    assert!(!prompt.contains("HOST SECRET"), "{prompt}");
}

#[tokio::test]
async fn a_non_utf8_override_prompt_is_run_fatal() {
    let workspace = seeded_repo();
    let sandbox = FakeSandbox::new(workspace.path().to_path_buf(), vec![], vec![]);
    sandbox.seed_guest_file("/workspace/prompts/plan.md", vec![0xff]);
    let prompts: PromptsConfig =
        serde_json::from_value(serde_json::json!({"plan": "prompts/plan.md"})).unwrap();
    let (ctx, _) = context(workspace.path(), sandbox, VerifyConfig::default(), prompts);

    let error = PipelineKernel.run(ctx).await.unwrap_err();
    assert!(error.to_string().contains("not UTF-8"), "{error}");
}

#[tokio::test]
async fn a_missing_override_prompt_is_run_fatal() {
    let workspace = seeded_repo();
    let sandbox = FakeSandbox::new(
        workspace.path().to_path_buf(),
        vec![reports("continue", "planned")],
        vec![],
    );
    let prompts: PromptsConfig =
        serde_json::from_value(serde_json::json!({"plan": "prompts/absent.md"})).unwrap();
    let (ctx, _) = context(workspace.path(), sandbox, VerifyConfig::default(), prompts);
    let error = PipelineKernel.run(ctx).await.unwrap_err();
    assert!(error.to_string().contains("absent.md"), "{error}");
}

// ---------- AC 3: a red verify re-runs the stage, then pauses ----------

#[tokio::test]
async fn a_red_verify_reruns_the_stage_then_pauses_verify_failed() {
    // Plan passes, then implement fails its check on both the first try
    // and the one retry the flow allows.
    let ran = run_default(
        vec![
            reports("continue", "planned"),
            reports("continue", "first implement try"),
            reports("continue", "second implement try"),
        ],
        vec![1, 1],
    )
    .await;

    assert_eq!(ran.outcome, RunOutcome::Paused(PauseReason::VerifyFailed));
    assert!(matches!(
        ran.events.last().unwrap(),
        RunEvent::StateChanged {
            state: RunState::Paused {
                reason: PauseReason::VerifyFailed
            }
        }
    ));
    // Implement ran twice; the run never reached review.
    assert_eq!(
        stage_events(&ran.events),
        [
            ("stage_started".into(), "plan".into()),
            ("stage_reported".into(), "plan".into()),
            ("stage_started".into(), "implement".into()),
            ("stage_reported".into(), "implement".into()),
            ("stage_started".into(), "implement".into()),
            ("stage_reported".into(), "implement".into()),
        ]
    );
    // The re-run carried the verify failure into the agent's preamble.
    let second_implement = &ran.prompts[2];
    assert!(
        second_implement.contains("## Verify checks failed"),
        "{second_implement}"
    );
    assert!(
        second_implement.contains("assertion failed: boom"),
        "{second_implement}"
    );
    // One sandbox for plan, one per implement attempt.
    assert_eq!(ran.sandbox.created.load(Ordering::SeqCst), 3);
    assert_eq!(
        ran.sandbox.created.load(Ordering::SeqCst),
        ran.sandbox.destroyed.load(Ordering::SeqCst)
    );
}

#[tokio::test]
async fn exhausted_retries_can_fail_the_run_instead_of_pausing() {
    let ran = run_pipeline(
        verifying(&["check"], 0, FailAction::Fail),
        PromptsConfig::default(),
        vec![reports("continue", "planned"), reports("continue", "built")],
        vec![1],
    )
    .await;
    assert_eq!(ran.outcome, RunOutcome::Failed);
}

// ---------- AC 4: blocked / needs_input pause mid-pipeline ----------

#[tokio::test]
async fn a_blocked_stage_pauses_the_run_mid_pipeline() {
    let ran = run_default(
        vec![
            reports("continue", "planned"),
            reports("blocked", "cannot reach the registry"),
        ],
        vec![],
    )
    .await;

    assert_eq!(ran.outcome, RunOutcome::Paused(PauseReason::Blocked));
    // Review and simplify never ran — the pause is immediate.
    assert_eq!(
        stage_events(&ran.events),
        [
            ("stage_started".into(), "plan".into()),
            ("stage_reported".into(), "plan".into()),
            ("stage_started".into(), "implement".into()),
            ("stage_reported".into(), "implement".into()),
        ]
    );
    // Implement is mutating, so its work was checkpointed before the
    // pause — but a blocked report skips its verify checks.
    assert!(kinds(&ran.events).contains(&"workspace_checkpointed".to_string()));
    assert!(!kinds(&ran.events).contains(&"verify_check_finished".to_string()));
    assert_eq!(ran.sandbox.created.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn a_needs_input_stage_pauses_awaiting_the_human() {
    let ran = run_default(
        vec![AgentStep {
            stdout: "working\n".into(),
            code: 0,
            report: Some(
                serde_json::json!({
                    "status": "needs_input",
                    "summary": "which database?",
                    "questions": [{"id": "q1", "text": "sqlite or postgres?"}],
                })
                .to_string(),
            ),
        }],
        vec![],
    )
    .await;

    assert_eq!(ran.outcome, RunOutcome::Paused(PauseReason::AwaitingHuman));
    // Only plan ran; the questions ride out on its stage report.
    assert_eq!(
        stage_events(&ran.events),
        [
            ("stage_started".into(), "plan".into()),
            ("stage_reported".into(), "plan".into()),
        ]
    );
    let reported = ran
        .events
        .iter()
        .find_map(|event| match event {
            RunEvent::StageReported { report, .. } => Some(report),
            _ => None,
        })
        .unwrap();
    assert_eq!(reported["questions"][0]["id"], "q1");
    assert_eq!(ran.sandbox.created.load(Ordering::SeqCst), 1);
}

// ---------- AC 5: checkpoints after mutating stages; scratch excluded ----------

#[tokio::test]
async fn checkpoints_land_after_mutating_stages_and_scratch_stays_out_of_history() {
    let ran = run_default(
        vec![
            reports("continue", "planned"),
            reports("continue", "built"),
            reports("continue", "reviewed"),
            reports("done", "done"),
        ],
        vec![],
    )
    .await;

    // Exactly one checkpoint per mutating stage — implement, review,
    // simplify — each a distinct commit.
    let commits: Vec<&String> = ran
        .events
        .iter()
        .filter_map(|event| match event {
            RunEvent::WorkspaceCheckpointed { commit, .. } => Some(commit),
            _ => None,
        })
        .collect();
    assert_eq!(commits.len(), 3, "one checkpoint per mutating stage");
    assert_eq!(
        commits.iter().collect::<BTreeSet<_>>().len(),
        3,
        "each checkpoint is its own commit"
    );

    // The agent's work is committed; its report under `.hako/` never is.
    let tracked = tracked_files(ran.workspace.path());
    assert!(
        tracked.iter().any(|path| path.starts_with("work-")),
        "the agent's work was committed: {tracked:?}"
    );
    assert!(
        !tracked.iter().any(|path| path.contains(".hako")),
        "scratch entered history: {tracked:?}"
    );
    // The report is on disk, just never tracked.
    assert!(ran.workspace.path().join(".hako/report.json").exists());
}

// ---------- the loop across iterations, repair, and hard failure ----------

#[tokio::test]
async fn a_full_pass_starts_a_fresh_iteration_that_reads_the_last() {
    // Iteration 1 completes a full pass; iteration 2's plan claims done.
    let ran = run_default(
        vec![
            reports("continue", "planned"),
            reports("continue", "built"),
            reports("continue", "reviewed"),
            reports("continue", "ITER1-SIMPLIFY"),
            reports("done", "nothing left"),
        ],
        vec![],
    )
    .await;

    assert_eq!(ran.outcome, RunOutcome::Done);
    // The first iteration closed with a completed outcome before the
    // second began.
    let iteration_events: Vec<String> = ran
        .events
        .iter()
        .filter_map(|event| match event {
            RunEvent::IterationStarted { iteration } => Some(format!("started {iteration}")),
            RunEvent::IterationFinished { iteration, outcome } => {
                Some(format!("finished {iteration} {outcome:?}"))
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        iteration_events,
        ["started 1", "finished 1 Completed", "started 2"]
    );
    // The second iteration's plan (the fifth prompt) reads the first
    // iteration's reports.
    let second_plan = &ran.prompts[4];
    assert!(second_plan.contains("## Reports so far"), "{second_plan}");
    assert!(second_plan.contains("ITER1-SIMPLIFY"), "{second_plan}");
}

#[tokio::test]
async fn a_malformed_report_earns_one_repair_then_advances() {
    // Plan's first report is rejected; its repair is accepted, and the
    // run goes on.
    let ran = run_default(vec![malformed(), reports("done", "recovered")], vec![]).await;

    assert_eq!(ran.outcome, RunOutcome::Done);
    let kinds = kinds(&ran.events);
    assert!(kinds.contains(&"report_rejected".to_string()));
    // The rejection named the offending field, and the repair re-prompt
    // quoted the plan schema back.
    let rejected = ran
        .events
        .iter()
        .find_map(|event| match event {
            RunEvent::ReportRejected { errors, .. } => Some(errors),
            _ => None,
        })
        .unwrap();
    assert!(
        rejected.iter().any(|e| e.contains("mystery")),
        "{rejected:?}"
    );
    let repair = &ran.prompts[1];
    assert!(repair.contains("PlanReport"), "{repair}");
    // Both attempts shared the one plan sandbox.
    assert_eq!(ran.sandbox.created.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_crashed_agent_fails_the_iteration_and_the_run() {
    let ran = run_default(vec![reports("continue", "planned"), crashes()], vec![]).await;

    assert_eq!(ran.outcome, RunOutcome::Failed);
    // The failing iteration is marked before the run concludes.
    let tail = kinds(&ran.events);
    let tail = &tail[tail.len() - 2..];
    assert_eq!(tail, ["iteration_finished", "state_changed"]);
    assert!(matches!(
        ran.events.iter().rev().nth(1).unwrap(),
        RunEvent::IterationFinished {
            outcome: IterationOutcome::Failed,
            ..
        }
    ));
}

#[tokio::test]
async fn a_stage_cannot_reuse_the_previous_stages_report() {
    let ran = run_default(
        vec![
            reports("continue", "planned"),
            omits_report(),
            omits_report(),
        ],
        vec![],
    )
    .await;

    assert_eq!(ran.outcome, RunOutcome::Failed);
    assert_eq!(
        kinds(&ran.events)
            .into_iter()
            .filter(|kind| kind == "report_rejected")
            .count(),
        2
    );
    assert_eq!(
        stage_events(&ran.events),
        [
            ("stage_started".into(), "plan".into()),
            ("stage_reported".into(), "plan".into()),
            ("stage_started".into(), "implement".into()),
        ]
    );
}

/// The run's very first event names the kernel and the agent, so a
/// listing and the metadata agree on what is running.
#[tokio::test]
async fn the_run_opens_by_naming_the_kernel_and_agent() {
    let ran = run_default(vec![reports("done", "done")], vec![]).await;
    assert!(matches!(
        &ran.events[0],
        RunEvent::RunStarted { kernel, agent } if kernel == "pipeline" && agent == "scripted"
    ));
}
