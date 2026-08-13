//! Safety rails exercised through the engine library boundary: a real
//! pipeline kernel over sandbox, event, and notifier fakes.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use engine::testkit::{
    self, AgentStep, RecordingNotifier, RecordingSink, ScriptedAgent, ScriptedSandbox,
    StagedSandbox, event_log, exec, reports, seeded_repo,
};
use engine::{
    Budgets, FailAction, IterationOutcome, Kernel, KernelContext, OnFail, PauseReason,
    PipelineKernel, RunEvent, RunOutcome, RunState, TokenUsage, VerifyConfig, Workspace,
};
use proto::budget::BudgetKind;

struct AdvancingSink {
    inner: RecordingSink,
    advanced: AtomicBool,
    by: Duration,
}

impl AdvancingSink {
    fn new(by: Duration) -> Self {
        Self {
            inner: RecordingSink::default(),
            advanced: AtomicBool::new(false),
            by,
        }
    }

    fn events(&self) -> Vec<RunEvent> {
        self.inner.events()
    }
}

#[async_trait]
impl engine::EventSink for AdvancingSink {
    async fn emit(&self, event: RunEvent) -> Result<(), engine::EventSinkError> {
        let starts_first_stage = matches!(event, RunEvent::StageStarted { .. })
            && !self.advanced.swap(true, Ordering::SeqCst);
        self.inner.emit(event).await?;
        if starts_first_stage {
            tokio::time::advance(self.by).await;
        }
        Ok(())
    }
}

fn context(
    workspace: &Path,
    sandbox: Arc<dyn engine::Sandbox>,
    agent: Arc<dyn engine::AgentAdapter>,
    budgets: Budgets,
    verify: VerifyConfig,
) -> (KernelContext, Arc<RecordingSink>, Arc<RecordingNotifier>) {
    let events = Arc::new(RecordingSink::default());
    let notifier = Arc::new(RecordingNotifier::default());
    let ctx = KernelContext {
        budgets,
        verify,
        workspace: Workspace::at(workspace),
        sandbox,
        agent,
        events: events.clone(),
        notifier: notifier.clone(),
        ..testkit::context()
    };
    (ctx, events, notifier)
}

fn continuing_steps() -> Vec<AgentStep> {
    vec![
        reports("continue", "planned the work"),
        reports("continue", "implemented the work"),
        reports("continue", "reviewed the work"),
        reports("continue", "simplified the work"),
    ]
}

fn no_change_sandbox() -> Arc<ScriptedSandbox> {
    let sandbox = Arc::new(ScriptedSandbox::repeating(exec("working\n", 0)));
    sandbox.write_report_on_exec(br#"{"status":"continue","summary":"no change"}"#);
    sandbox
}

fn resumed_after_completed_iteration(note: Option<&str>) -> Vec<engine::EventEnvelope> {
    event_log([
        RunEvent::IterationStarted { iteration: 1 },
        RunEvent::IterationFinished {
            iteration: 1,
            outcome: IterationOutcome::Completed,
        },
        RunEvent::StateChanged {
            state: RunState::Paused {
                reason: PauseReason::Budget,
            },
        },
        RunEvent::RunResumed {
            note: note.map(str::to_owned),
            extend: None,
        },
    ])
}

#[tokio::test]
async fn iteration_budget_finishes_the_pass_then_pauses_and_notifies() {
    let workspace = seeded_repo();
    let sandbox = Arc::new(StagedSandbox::new(
        workspace.path().to_path_buf(),
        continuing_steps(),
    ));
    let budgets = Budgets {
        max_iterations: Some(1),
        ..Budgets::default()
    };
    let (ctx, events, notifier) = context(
        workspace.path(),
        sandbox.clone(),
        Arc::new(ScriptedAgent::new()),
        budgets,
        VerifyConfig::default(),
    );

    let outcome = PipelineKernel.run(ctx).await.unwrap();

    assert_eq!(outcome, RunOutcome::Paused(PauseReason::Budget));
    assert_eq!(sandbox.created(), 4, "the whole pass finished");
    assert!(events.events().contains(&RunEvent::BudgetExhausted {
        budget: BudgetKind::Iterations,
    }));
    assert_eq!(
        notifier.notifications(),
        [engine::Notification {
            run_id: engine::RunId::new("r1"),
            reason: PauseReason::Budget,
            summary: Some("simplified the work".into()),
        }]
    );
}

#[tokio::test]
async fn a_budget_pause_before_any_report_notifies_without_a_summary() {
    let workspace = seeded_repo();
    let sandbox = Arc::new(ScriptedSandbox::repeating(exec("unused\n", 0)));
    let budgets = Budgets {
        max_iterations: Some(0),
        ..Budgets::default()
    };
    let (ctx, _, notifier) = context(
        workspace.path(),
        sandbox.clone(),
        Arc::new(ScriptedAgent::new()),
        budgets,
        VerifyConfig::default(),
    );

    let outcome = PipelineKernel.run(ctx).await.unwrap();

    assert_eq!(outcome, RunOutcome::Paused(PauseReason::Budget));
    assert_eq!(sandbox.created(), 0);
    assert_eq!(notifier.notifications()[0].summary, None);
}

#[tokio::test(start_paused = true)]
async fn wall_clock_budget_is_checked_after_the_in_flight_pass() {
    // A paused-clock runtime auto-advances time whenever every task is
    // idle, which would leap straight to the iteration deadline. This
    // never-idle task pins the clock so only the sink's explicit
    // advance moves it.
    let keep_clock_manual = tokio::spawn(async {
        loop {
            tokio::task::yield_now().await;
        }
    });
    let workspace = seeded_repo();
    let sandbox = Arc::new(ScriptedSandbox::repeating(exec("working\n", 0)));
    sandbox.write_report_on_exec(br#"{"status":"continue","summary":"slow pass"}"#);
    let budgets = Budgets {
        max_wall_clock: Some(Duration::from_secs(60 * 60)),
        iteration_timeout: Duration::from_secs(3 * 60 * 60),
        ..Budgets::default()
    };
    let events = Arc::new(AdvancingSink::new(Duration::from_secs(2 * 60 * 60)));
    let ctx = KernelContext {
        budgets,
        workspace: Workspace::at(workspace.path()),
        sandbox: sandbox.clone(),
        agent: Arc::new(ScriptedAgent::new()),
        events: events.clone(),
        ..testkit::context()
    };

    let outcome = PipelineKernel.run(ctx).await.unwrap();
    keep_clock_manual.abort();

    assert_eq!(outcome, RunOutcome::Paused(PauseReason::Budget));
    assert_eq!(sandbox.created(), 4, "the whole pass finished");
    assert!(events.events().contains(&RunEvent::BudgetExhausted {
        budget: BudgetKind::WallClock,
    }));
}

#[tokio::test]
async fn reported_tokens_pause_only_after_the_in_flight_pass() {
    let workspace = seeded_repo();
    let steps = continuing_steps()
        .into_iter()
        .map(|mut step| {
            step.stdout = "tokens used\n".into();
            step
        })
        .collect();
    let sandbox = Arc::new(StagedSandbox::new(workspace.path().to_path_buf(), steps));
    let budgets = Budgets {
        max_tokens: Some(10),
        ..Budgets::default()
    };
    let (ctx, events, _) = context(
        workspace.path(),
        sandbox.clone(),
        Arc::new(ScriptedAgent::new().reporting(TokenUsage {
            input: 2,
            output: 1,
        })),
        budgets,
        VerifyConfig::default(),
    );

    let outcome = PipelineKernel.run(ctx).await.unwrap();

    assert_eq!(outcome, RunOutcome::Paused(PauseReason::Budget));
    assert_eq!(sandbox.created(), 4, "the whole pass finished");
    assert!(events.events().contains(&RunEvent::BudgetExhausted {
        budget: BudgetKind::Tokens,
    }));
}

#[tokio::test]
async fn an_adapter_without_usage_is_not_token_budgeted() {
    let workspace = seeded_repo();
    let mut steps = continuing_steps();
    steps.push(reports("done", "all done"));
    steps.push(AgentStep {
        stdout: "checked\n".into(),
        code: 0,
        report: Some(r#"{"refuted":false,"findings":[]}"#.into()),
    });
    let sandbox = Arc::new(StagedSandbox::new(workspace.path().to_path_buf(), steps));
    let budgets = Budgets {
        max_tokens: Some(0),
        ..Budgets::default()
    };
    let (ctx, events, _) = context(
        workspace.path(),
        sandbox,
        Arc::new(ScriptedAgent::new()),
        budgets,
        VerifyConfig::default(),
    );

    assert_eq!(PipelineKernel.run(ctx).await.unwrap(), RunOutcome::Done);
    assert!(
        !events
            .events()
            .iter()
            .any(|event| matches!(event, RunEvent::BudgetExhausted { .. }))
    );
}

#[tokio::test]
async fn an_extended_iteration_budget_continues_on_resume() {
    let workspace = seeded_repo();
    let mut steps = continuing_steps();
    steps.push(reports("done", "finished after the extension"));
    steps.push(AgentStep {
        stdout: "checked\n".into(),
        code: 0,
        report: Some(r#"{"refuted":false,"findings":[]}"#.into()),
    });
    let sandbox = Arc::new(StagedSandbox::new(workspace.path().to_path_buf(), steps));
    let (first, _, _) = context(
        workspace.path(),
        sandbox.clone(),
        Arc::new(ScriptedAgent::new()),
        Budgets {
            max_iterations: Some(1),
            ..Budgets::default()
        },
        VerifyConfig::default(),
    );
    assert_eq!(
        PipelineKernel.run(first).await.unwrap(),
        RunOutcome::Paused(PauseReason::Budget)
    );

    let (mut resumed, _, _) = context(
        workspace.path(),
        sandbox,
        Arc::new(ScriptedAgent::new()),
        Budgets {
            max_iterations: Some(2),
            ..Budgets::default()
        },
        VerifyConfig::default(),
    );
    resumed.replay = Some(resumed_after_completed_iteration(Some(
        "one more iteration",
    )));

    assert_eq!(PipelineKernel.run(resumed).await.unwrap(), RunOutcome::Done);
}

#[tokio::test]
async fn a_token_extension_keeps_what_the_run_spent_before_resume() {
    let workspace = seeded_repo();
    let steps = continuing_steps()
        .into_iter()
        .chain(continuing_steps())
        .map(|mut step| {
            step.stdout = "tokens used\n".into();
            step
        })
        .collect();
    let sandbox = Arc::new(StagedSandbox::new(workspace.path().to_path_buf(), steps));
    let usage = engine::BudgetUsage::default();
    let agent = || {
        Arc::new(ScriptedAgent::new().reporting(TokenUsage {
            input: 2,
            output: 1,
        })) as Arc<dyn engine::AgentAdapter>
    };
    let (mut first, _, _) = context(
        workspace.path(),
        sandbox.clone(),
        agent(),
        Budgets {
            max_tokens: Some(10),
            ..Budgets::default()
        },
        VerifyConfig::default(),
    );
    first.budget_usage = usage.clone();
    assert_eq!(
        PipelineKernel.run(first).await.unwrap(),
        RunOutcome::Paused(PauseReason::Budget)
    );

    let (mut resumed, events, _) = context(
        workspace.path(),
        sandbox.clone(),
        agent(),
        Budgets {
            max_tokens: Some(20),
            ..Budgets::default()
        },
        VerifyConfig::default(),
    );
    resumed.budget_usage = usage;
    resumed.replay = Some(resumed_after_completed_iteration(None));

    assert_eq!(
        PipelineKernel.run(resumed).await.unwrap(),
        RunOutcome::Paused(PauseReason::Budget)
    );
    assert_eq!(sandbox.created(), 8, "one pass ran on each launch");
    assert!(events.events().contains(&RunEvent::BudgetExhausted {
        budget: BudgetKind::Tokens,
    }));
}

#[tokio::test]
async fn a_token_paused_run_resumed_without_extension_pauses_before_booting() {
    let workspace = seeded_repo();
    let steps = continuing_steps()
        .into_iter()
        .map(|mut step| {
            step.stdout = "tokens used\n".into();
            step
        })
        .collect();
    let sandbox = Arc::new(StagedSandbox::new(workspace.path().to_path_buf(), steps));
    let usage = engine::BudgetUsage::default();
    let budgets = || Budgets {
        max_tokens: Some(10),
        ..Budgets::default()
    };
    let agent = || {
        Arc::new(ScriptedAgent::new().reporting(TokenUsage {
            input: 2,
            output: 1,
        })) as Arc<dyn engine::AgentAdapter>
    };
    let (mut first, _, _) = context(
        workspace.path(),
        sandbox.clone(),
        agent(),
        budgets(),
        VerifyConfig::default(),
    );
    first.budget_usage = usage.clone();
    assert_eq!(
        PipelineKernel.run(first).await.unwrap(),
        RunOutcome::Paused(PauseReason::Budget)
    );
    assert_eq!(sandbox.created(), 4, "the first launch ran a full pass");

    let (mut resumed, events, notifier) = context(
        workspace.path(),
        sandbox.clone(),
        agent(),
        budgets(),
        VerifyConfig::default(),
    );
    resumed.budget_usage = usage;
    resumed.replay = Some(resumed_after_completed_iteration(None));

    assert_eq!(
        PipelineKernel.run(resumed).await.unwrap(),
        RunOutcome::Paused(PauseReason::Budget)
    );
    assert_eq!(
        sandbox.created(),
        4,
        "the still-exhausted budget pauses the resumed run before it boots anything"
    );
    assert!(events.events().contains(&RunEvent::BudgetExhausted {
        budget: BudgetKind::Tokens,
    }));
    assert_eq!(notifier.notifications().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn iteration_timeout_destroys_the_sandbox_and_uses_on_fail() {
    let workspace = seeded_repo();
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let sandbox = Arc::new(ScriptedSandbox::hanging().with_barrier(barrier.clone()));
    let budgets = Budgets {
        iteration_timeout: Duration::from_secs(30),
        ..Budgets::default()
    };
    let verify = VerifyConfig {
        checks: vec![],
        on_fail: OnFail {
            retries: 0,
            then: FailAction::Pause,
        },
    };
    let (ctx, events, notifier) = context(
        workspace.path(),
        sandbox.clone(),
        Arc::new(ScriptedAgent::new()),
        budgets,
        verify,
    );
    let task = tokio::spawn(PipelineKernel.run(ctx));
    barrier.wait().await;
    tokio::time::advance(Duration::from_secs(31)).await;
    // Git checkpointing uses a real OS child; wall time must run so
    // Tokio does not outrun the child while the test clock is paused.
    tokio::time::resume();

    let outcome = tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("the iteration timeout did not fire")
        .unwrap()
        .unwrap();

    assert_eq!(outcome, RunOutcome::Paused(PauseReason::Timeout));
    assert_eq!(sandbox.created(), 1);
    assert_eq!(sandbox.destroyed(), 1);
    assert!(events.events().contains(&RunEvent::IterationFinished {
        iteration: 1,
        outcome: IterationOutcome::TimedOut,
    }));
    assert_eq!(notifier.notifications()[0].reason, PauseReason::Timeout);
}

/// The scripted sandbox with an agent that commits its own work:
/// every exec lands a commit in the workspace before the scripted
/// "no change" report — the shape `checkpoint` cannot see, because
/// nothing is left uncommitted for it to stage.
struct SelfCommittingSandbox {
    inner: Arc<ScriptedSandbox>,
    workspace: PathBuf,
    commits: AtomicU32,
}

impl SelfCommittingSandbox {
    async fn commit(&self) {
        let n = self.commits.fetch_add(1, Ordering::SeqCst);
        tokio::fs::write(self.workspace.join("agent-work.txt"), format!("work {n}\n"))
            .await
            .unwrap();
        for args in [
            &["add", "agent-work.txt"][..],
            &[
                "-c",
                "user.name=agent",
                "-c",
                "user.email=agent@localhost",
                "commit",
                "--quiet",
                "--no-verify",
                "--no-gpg-sign",
                "-m",
                "agent work",
            ],
        ] {
            let status = tokio::process::Command::new("git")
                .args(args)
                .current_dir(&self.workspace)
                .status()
                .await
                .unwrap();
            assert!(status.success());
        }
    }
}

#[async_trait]
impl engine::Sandbox for SelfCommittingSandbox {
    async fn create(
        &self,
        spec: &engine::SandboxSpec,
    ) -> Result<engine::SandboxHandle, engine::SandboxError> {
        self.inner.create(spec).await
    }

    async fn exec_stream(
        &self,
        sandbox: &engine::SandboxHandle,
        command: &engine::ExecSpec,
    ) -> Result<engine::ExecStream, engine::SandboxError> {
        self.commit().await;
        self.inner.exec_stream(sandbox, command).await
    }

    async fn put_file(
        &self,
        sandbox: &engine::SandboxHandle,
        path: &Path,
        contents: &[u8],
    ) -> Result<(), engine::SandboxError> {
        self.inner.put_file(sandbox, path, contents).await
    }

    async fn get_file(
        &self,
        sandbox: &engine::SandboxHandle,
        path: &Path,
    ) -> Result<Vec<u8>, engine::SandboxError> {
        self.inner.get_file(sandbox, path).await
    }

    async fn remove_file(
        &self,
        sandbox: &engine::SandboxHandle,
        path: &Path,
    ) -> Result<(), engine::SandboxError> {
        self.inner.remove_file(sandbox, path).await
    }

    async fn destroy(&self, sandbox: engine::SandboxHandle) -> Result<(), engine::SandboxError> {
        self.inner.destroy(sandbox).await
    }

    async fn preflight(&self) -> Result<(), engine::SandboxError> {
        self.inner.preflight().await
    }
}

/// An agent that commits for itself leaves `checkpoint` nothing to
/// stage, but the moved HEAD is still durable progress — the run must
/// reach its iteration budget, never a drift pause.
#[tokio::test]
async fn an_agent_committing_its_own_work_is_not_drifting() {
    let workspace = seeded_repo();
    let sandbox = Arc::new(SelfCommittingSandbox {
        inner: no_change_sandbox(),
        workspace: workspace.path().to_path_buf(),
        commits: AtomicU32::new(0),
    });
    let budgets = Budgets {
        max_iterations: Some(4),
        ..Budgets::default()
    };
    let (ctx, events, _) = context(
        workspace.path(),
        sandbox.clone(),
        Arc::new(ScriptedAgent::new()),
        budgets,
        VerifyConfig::default(),
    );

    let outcome = PipelineKernel.run(ctx).await.unwrap();

    assert_eq!(outcome, RunOutcome::Paused(PauseReason::Budget));
    assert_eq!(
        sandbox.inner.created(),
        16,
        "four full passes ran — drift never tripped at three"
    );
    assert!(events.events().contains(&RunEvent::BudgetExhausted {
        budget: BudgetKind::Iterations,
    }));
}

#[tokio::test]
async fn three_no_commit_iterations_pause_for_drift() {
    let workspace = seeded_repo();
    let sandbox = no_change_sandbox();
    let (ctx, events, notifier) = context(
        workspace.path(),
        sandbox.clone(),
        Arc::new(ScriptedAgent::new()),
        Budgets::default(),
        VerifyConfig::default(),
    );

    let outcome = PipelineKernel.run(ctx).await.unwrap();

    assert_eq!(outcome, RunOutcome::Paused(PauseReason::Drift));
    assert_eq!(sandbox.created(), 12, "three complete four-stage passes");
    assert_eq!(sandbox.created(), sandbox.destroyed());
    assert_eq!(
        events
            .events()
            .iter()
            .filter(|event| matches!(event, RunEvent::IterationFinished { .. }))
            .count(),
        3
    );
    assert_eq!(
        notifier.notifications()[0].summary.as_deref(),
        Some("no change")
    );
}
