use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use engine::testkit::{
    RecordingNotifier, RecordingSink, ScriptedAgent, ScriptedSandbox, UNREFUTED_SKEPTIC_REPORT,
    context, event_log, exec, seeded_repo,
};
use engine::{
    Budgets, ChildRunSpec, EventEnvelope, FanoutKernel, Kernel, RunEvent, RunId, RunOutcome,
    RunSpawner, RunSpawnerError, RunState, TokenUsage, Workspace,
};

#[derive(Default)]
struct FakeSpawner {
    scopes: Mutex<Vec<String>>,
    histories: Mutex<BTreeMap<RunId, Vec<EventEnvelope>>>,
    watched: Mutex<Vec<RunId>>,
}

impl FakeSpawner {
    fn with_children(histories: impl IntoIterator<Item = Vec<RunEvent>>) -> Self {
        let histories = histories
            .into_iter()
            .enumerate()
            .map(|(index, events)| {
                let id = RunId::new(format!("child-{}", index + 1));
                let wire_id = id.to_string();
                let history = event_log(events).into_iter().map(|mut envelope| {
                    envelope.run_id = wire_id.clone();
                    envelope
                });
                (id, history.collect())
            })
            .collect();
        Self {
            histories: Mutex::new(histories),
            ..Self::default()
        }
    }

    fn scopes(&self) -> Vec<String> {
        self.scopes.lock().unwrap().clone()
    }

    fn watched(&self) -> Vec<RunId> {
        self.watched.lock().unwrap().clone()
    }
}

#[async_trait]
impl RunSpawner for FakeSpawner {
    async fn spawn(&self, child: ChildRunSpec) -> Result<RunId, RunSpawnerError> {
        let mut scopes = self.scopes.lock().unwrap();
        scopes.push(child.scope);
        Ok(RunId::new(format!("child-{}", scopes.len())))
    }

    async fn watch(&self, run_id: &RunId) -> Result<Vec<EventEnvelope>, RunSpawnerError> {
        self.watched.lock().unwrap().push(run_id.clone());
        self.histories
            .lock()
            .unwrap()
            .get(run_id)
            .cloned()
            .ok_or_else(|| RunSpawnerError(format!("unknown fake child {run_id}")))
    }
}

fn terminal(state: RunState, usage: TokenUsage) -> Vec<RunEvent> {
    vec![
        RunEvent::IterationStarted { iteration: 1 },
        RunEvent::TokensUsed {
            iteration: 1,
            usage,
        },
        RunEvent::IterationFinished {
            iteration: 1,
            outcome: engine::IterationOutcome::Completed,
        },
        RunEvent::StateChanged { state },
    ]
}

fn fanout_context(
    reports: &[&[u8]],
    spawner: Arc<FakeSpawner>,
) -> (
    engine::KernelContext,
    Arc<RecordingSink>,
    Arc<RecordingNotifier>,
) {
    let workspace = seeded_repo();
    let path = workspace.keep();
    let sandbox = Arc::new(ScriptedSandbox::repeating(exec("planned\n", 0)));
    for report in reports {
        sandbox.write_report_on_exec(report.to_vec());
    }
    let events = Arc::new(RecordingSink::default());
    let notifier = Arc::new(RecordingNotifier::default());
    let ctx = engine::KernelContext {
        workspace: Workspace::at(path),
        sandbox,
        agent: Arc::new(ScriptedAgent::new()),
        events: events.clone(),
        notifier: notifier.clone(),
        run_spawner: spawner,
        ..context()
    };
    (ctx, events, notifier)
}

#[tokio::test]
async fn a_plan_spawns_one_scoped_child_per_unit_and_records_each_ending() {
    let spawner = Arc::new(FakeSpawner::with_children([
        terminal(RunState::Done, TokenUsage::default()),
        terminal(RunState::Failed, TokenUsage::default()),
    ]));
    let (ctx, events, _) = fanout_context(
        &[
            br#"{"status":"continue","summary":"two ready","units":["issue #31","issue #32"]}"#,
            br#"{"status":"done","summary":"frontier drained","units":[]}"#,
            UNREFUTED_SKEPTIC_REPORT,
        ],
        spawner.clone(),
    );

    let outcome = FanoutKernel.run(ctx).await.unwrap();

    assert_eq!(outcome, RunOutcome::Done);
    assert!(
        events
            .events()
            .iter()
            .any(|event| matches!(event, RunEvent::SkepticVerdict { refuted: false, .. }))
    );
    assert_eq!(spawner.scopes(), ["issue #31", "issue #32"]);
    assert!(events.events().contains(&RunEvent::ChildRunFinished {
        child_run_id: "child-1".into(),
        state: RunState::Done,
        iterations: 1,
        usage: Some(TokenUsage::default()),
    }));
    assert!(events.events().contains(&RunEvent::ChildRunFinished {
        child_run_id: "child-2".into(),
        state: RunState::Failed,
        iterations: 1,
        usage: Some(TokenUsage::default()),
    }));
}

#[tokio::test]
async fn a_budget_paused_fanout_resumes_after_an_extension() {
    let spawner = Arc::new(FakeSpawner::with_children([terminal(
        RunState::Done,
        TokenUsage {
            input: 6,
            output: 4,
        },
    )]));
    let (mut first, events, _) = fanout_context(
        &[br#"{"status":"continue","summary":"one ready","units":["a"]}"#],
        spawner,
    );
    first.budgets = Budgets {
        max_tokens: Some(10),
        ..Budgets::default()
    };
    let usage = first.budget_usage.clone();
    assert_eq!(
        FanoutKernel.run(first).await.unwrap(),
        RunOutcome::Paused(engine::PauseReason::Budget)
    );

    let mut replay = event_log(events.events());
    replay.extend(event_log([RunEvent::RunResumed {
        note: None,
        extend: Some(proto::budget::BudgetExtension {
            max_iterations: None,
            max_wall_clock_seconds: None,
            max_tokens: Some(20),
        }),
    }]));
    let (mut resumed, resumed_events, _) = fanout_context(
        &[
            br#"{"status":"done","summary":"frontier drained","units":[]}"#,
            UNREFUTED_SKEPTIC_REPORT,
        ],
        Arc::new(FakeSpawner::default()),
    );
    resumed.replay = Some(replay);
    resumed.budget_usage = usage;
    resumed.budgets = Budgets {
        max_tokens: Some(20),
        ..Budgets::default()
    };

    assert_eq!(FanoutKernel.run(resumed).await.unwrap(), RunOutcome::Done);
    assert!(
        !resumed_events
            .events()
            .iter()
            .any(|event| matches!(event, RunEvent::RunStarted { .. }))
    );
    assert!(
        resumed_events
            .events()
            .contains(&RunEvent::IterationStarted { iteration: 2 })
    );
}

#[tokio::test]
async fn child_usage_exhausts_the_parent_only_after_all_children_settle() {
    let spawner = Arc::new(FakeSpawner::with_children([
        terminal(
            RunState::Done,
            TokenUsage {
                input: 6,
                output: 4,
            },
        ),
        terminal(
            RunState::Done,
            TokenUsage {
                input: 6,
                output: 4,
            },
        ),
    ]));
    let (mut ctx, events, _) = fanout_context(
        &[br#"{"status":"continue","summary":"two ready","units":["a","b"]}"#],
        spawner.clone(),
    );
    ctx.budgets = Budgets {
        max_tokens: Some(15),
        ..Budgets::default()
    };

    let outcome = FanoutKernel.run(ctx).await.unwrap();

    assert_eq!(outcome, RunOutcome::Paused(engine::PauseReason::Budget));
    assert_eq!(spawner.watched().len(), 2, "every in-flight child settled");
    assert!(events.events().iter().any(|event| matches!(
        event,
        RunEvent::BudgetExhausted {
            budget: engine::BudgetKind::Tokens
        }
    )));
}

#[tokio::test]
async fn a_failed_watch_still_drains_and_records_its_siblings() {
    // Two units planned, but the fake only knows child-1: watching
    // child-2 errors while child-1 settles normally. The settled
    // sibling's outcome must still land in the parent's log before
    // the failure surfaces.
    let spawner = Arc::new(FakeSpawner::with_children([terminal(
        RunState::Done,
        TokenUsage::default(),
    )]));
    let (ctx, events, _) = fanout_context(
        &[br#"{"status":"continue","summary":"two ready","units":["a","b"]}"#],
        spawner.clone(),
    );

    let error = FanoutKernel.run(ctx).await.unwrap_err();

    assert!(error.to_string().contains("unknown fake child"), "{error}");
    assert_eq!(spawner.watched().len(), 2, "both children were watched");
    assert!(events.events().contains(&RunEvent::ChildRunFinished {
        child_run_id: "child-1".into(),
        state: RunState::Done,
        iterations: 1,
        usage: Some(TokenUsage::default()),
    }));
}

#[tokio::test]
async fn an_empty_blocked_frontier_pauses_and_notifies() {
    let spawner = Arc::new(FakeSpawner::default());
    let (ctx, _, notifier) = fanout_context(
        &[br#"{"status":"blocked","summary":"children still running","units":[]}"#],
        spawner,
    );

    let outcome = FanoutKernel.run(ctx).await.unwrap();

    assert_eq!(outcome, RunOutcome::Paused(engine::PauseReason::Blocked));
    assert_eq!(notifier.notifications().len(), 1);
    assert_eq!(
        notifier.notifications()[0].summary,
        "children still running"
    );
}

#[tokio::test]
async fn a_refuted_done_claim_returns_to_planning() {
    let spawner = Arc::new(FakeSpawner::default());
    let (ctx, events, _) = fanout_context(
        &[
            br#"{"status":"done","summary":"frontier drained","units":[]}"#,
            br#"{"refuted":true,"findings":["issue #32 is still ready"]}"#,
            br#"{"status":"blocked","summary":"waiting","units":[]}"#,
        ],
        spawner,
    );

    let outcome = FanoutKernel.run(ctx).await.unwrap();

    assert_eq!(outcome, RunOutcome::Paused(engine::PauseReason::Blocked));
    assert!(events.events().contains(&RunEvent::SkepticVerdict {
        iteration: 1,
        refuted: true,
        findings: vec!["issue #32 is still ready".into()],
    }));
    assert!(
        events
            .events()
            .contains(&RunEvent::IterationStarted { iteration: 2 })
    );
}
