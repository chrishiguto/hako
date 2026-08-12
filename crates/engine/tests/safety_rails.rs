//! Safety rails exercised through the engine library boundary: a real
//! pipeline kernel over sandbox, event, and notifier fakes.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use engine::testkit::{
    self, AgentStep, RecordingNotifier, RecordingSink, ScriptedAgent, ScriptedSandbox,
    StagedSandbox, exec, reports, seeded_repo,
};
use engine::{
    Budgets, FailAction, IterationOutcome, Kernel, KernelContext, OnFail, PauseReason,
    PipelineKernel, RunEvent, RunOutcome, RunResume, TokenUsage, VerifyConfig, Workspace,
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
            summary: "simplified the work".into(),
        }]
    );
}

#[tokio::test(start_paused = true)]
async fn wall_clock_budget_is_checked_after_the_in_flight_pass() {
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
    resumed.resume = Some(RunResume {
        next_iteration: 2,
        human: engine::HumanInput {
            answers: vec![],
            questions: vec![],
            note: Some("one more iteration".into()),
        },
    });

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
    resumed.resume = Some(RunResume {
        next_iteration: 2,
        human: engine::HumanInput {
            answers: vec![],
            questions: vec![],
            note: None,
        },
    });

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
    resumed.resume = Some(RunResume {
        next_iteration: 2,
        human: engine::HumanInput {
            answers: vec![],
            questions: vec![],
            note: None,
        },
    });

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

    let outcome = tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("the iteration timeout did not fire")
        .unwrap()
        .unwrap();

    assert_eq!(outcome, RunOutcome::Paused(PauseReason::VerifyFailed));
    assert_eq!(sandbox.created(), 1);
    assert_eq!(sandbox.destroyed(), 1);
    assert!(events.events().contains(&RunEvent::IterationFinished {
        iteration: 1,
        outcome: IterationOutcome::TimedOut,
    }));
    assert_eq!(
        notifier.notifications()[0].reason,
        PauseReason::VerifyFailed
    );
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
    assert_eq!(notifier.notifications()[0].summary, "no change");
}
