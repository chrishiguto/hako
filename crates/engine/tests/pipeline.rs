//! The pipeline kernel driven entirely over the testkit fakes:
//! scripted per-stage agents, a fake sandbox, a real tempdir git repo.
//! House pattern — assert the emitted events, the run outcome, and the
//! git effects, never internal call patterns.

use std::collections::BTreeSet;
use std::sync::Arc;

use engine::testkit::{
    AgentStep, Ran, StagedSandbox, carries_handoff, carries_report_from, carries_verify_feedback,
    crashes, drive_pipeline, event_log, kinds, malformed, omits_report, pipeline_context, reports,
    seeded_repo, skeptic, stage_events, tracked_files, unrefuted,
};
use engine::{
    FailAction, IterationOutcome, Kernel, KernelError, OnFail, PauseReason, PipelineKernel,
    PromptsConfig, RunEvent, RunOutcome, RunState, VerifyConfig,
};
use proto::pipeline::Stage;

// ---------- the harness ----------

fn verifying(checks: &[&str], retries: u32, then: FailAction) -> VerifyConfig {
    VerifyConfig {
        checks: checks.iter().map(|c| (*c).to_string()).collect(),
        on_fail: OnFail { retries, then },
    }
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
    let sandbox = Arc::new(
        StagedSandbox::new(workspace.path().to_path_buf(), agent_steps).with_checks(checks),
    );
    drive_pipeline(workspace, sandbox, verify, prompts).await
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

// ---------- AC 1: the stage event sequence and fresh sandboxes ----------

#[tokio::test]
async fn a_full_iteration_and_its_skeptic_each_get_a_fresh_sandbox() {
    let ran = run_default(
        vec![
            reports("continue", "planned"),
            reports("continue", "built"),
            reports("continue", "reviewed"),
            reports("done", "simplified and complete"),
            unrefuted(),
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
            "agent_output",
            "skeptic_verdict",
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
    // One fresh sandbox per stage plus one for the skeptic, every one
    // torn down.
    assert_eq!(ran.sandbox.created(), 5);
    assert_eq!(ran.sandbox.created(), ran.sandbox.destroyed());
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
            unrefuted(),
        ],
        vec![],
    )
    .await;

    let [plan, implement, review, _simplify, _skeptic] = ran.prompts.as_slice() else {
        panic!(
            "expected four stage prompts and a skeptic, got {}",
            ran.prompts.len()
        );
    };

    // The first plan has no hand-off — nothing came before it — but
    // still quotes its own report contract.
    assert!(!carries_handoff(plan), "{plan}");
    assert!(plan.contains("\"title\": \"PlanReport\""), "{plan}");

    // Implement reads the plan's report; review reads plan and
    // implement. Each quotes its own schema, never another stage's.
    assert!(carries_handoff(implement), "{implement}");
    assert!(carries_report_from(implement, Stage::Plan), "{implement}");
    assert!(implement.contains("PLAN-MARKER"), "{implement}");
    assert!(
        implement.contains("\"title\": \"ImplementReport\""),
        "{implement}"
    );

    assert!(carries_report_from(review, Stage::Plan), "{review}");
    assert!(carries_report_from(review, Stage::Implement), "{review}");
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
    let sandbox = Arc::new(StagedSandbox::new(
        workspace.path().to_path_buf(),
        vec![reports("done", "done"), unrefuted()],
    ));
    let prompts: PromptsConfig =
        serde_json::from_value(serde_json::json!({"plan": "prompts/plan.md"})).unwrap();
    let (ctx, _) = pipeline_context(
        workspace.path(),
        sandbox.clone(),
        VerifyConfig::default(),
        prompts,
    );
    let outcome = PipelineKernel.run(ctx).await.unwrap();

    assert_eq!(outcome, RunOutcome::Done);
    let plan = &sandbox.agent_prompts()[0];
    assert!(plan.contains("CUSTOM PLAN RULES"), "{plan}");
    let skeptic = &sandbox.agent_prompts()[1];
    assert!(skeptic.contains("CUSTOM PLAN RULES"), "{skeptic}");
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

    let sandbox = Arc::new(StagedSandbox::new(
        workspace.path().to_path_buf(),
        vec![reports("done", "done"), unrefuted()],
    ));
    sandbox.seed_guest_file(
        "/workspace/prompts/plan.md",
        b"GUEST PROMPT CONTENT\n".to_vec(),
    );
    let prompts: PromptsConfig =
        serde_json::from_value(serde_json::json!({"plan": "prompts/plan.md"})).unwrap();
    let (ctx, _) = pipeline_context(
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
    let sandbox = Arc::new(StagedSandbox::new(workspace.path().to_path_buf(), vec![]));
    sandbox.seed_guest_file("/workspace/prompts/plan.md", vec![0xff]);
    let prompts: PromptsConfig =
        serde_json::from_value(serde_json::json!({"plan": "prompts/plan.md"})).unwrap();
    let (ctx, _) = pipeline_context(workspace.path(), sandbox, VerifyConfig::default(), prompts);

    let error = PipelineKernel.run(ctx).await.unwrap_err();
    assert!(error.to_string().contains("not UTF-8"), "{error}");
}

#[tokio::test]
async fn a_missing_override_prompt_is_run_fatal() {
    let workspace = seeded_repo();
    let sandbox = Arc::new(StagedSandbox::new(
        workspace.path().to_path_buf(),
        vec![reports("continue", "planned")],
    ));
    let prompts: PromptsConfig =
        serde_json::from_value(serde_json::json!({"plan": "prompts/absent.md"})).unwrap();
    let (ctx, _) = pipeline_context(workspace.path(), sandbox, VerifyConfig::default(), prompts);
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
        carries_verify_feedback(second_implement),
        "{second_implement}"
    );
    assert!(
        second_implement.contains("assertion failed: boom"),
        "{second_implement}"
    );
    // One sandbox for plan, one per implement attempt.
    assert_eq!(ran.sandbox.created(), 3);
    assert_eq!(ran.sandbox.created(), ran.sandbox.destroyed());
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
    assert_eq!(ran.sandbox.created(), 2);
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
    assert_eq!(ran.sandbox.created(), 1);
    assert_eq!(
        &kinds(&ran.events)[ran.events.len() - 2..],
        ["workspace_checkpointed", "state_changed"]
    );
    assert!(
        tracked_files(ran.workspace.path())
            .iter()
            .any(|path| path.starts_with("work-")),
        "the workspace was checkpointed before the pause"
    );
}

#[tokio::test]
async fn a_replayed_mid_pipeline_pause_resumes_at_the_interrupted_stage() {
    let workspace = seeded_repo();
    let sandbox = Arc::new(StagedSandbox::new(
        workspace.path().to_path_buf(),
        vec![
            reports("continue", "built after the answer"),
            reports("continue", "reviewed"),
            reports("done", "complete"),
            unrefuted(),
        ],
    ));
    let (mut ctx, sink) = pipeline_context(
        workspace.path(),
        sandbox.clone(),
        VerifyConfig::default(),
        PromptsConfig::default(),
    );
    ctx.replay = Some(event_log(vec![
        RunEvent::RunStarted {
            kernel: "pipeline".into(),
            agent: "scripted".into(),
        },
        RunEvent::IterationStarted { iteration: 7 },
        RunEvent::StageStarted {
            iteration: 7,
            stage: "plan".into(),
        },
        RunEvent::StageReported {
            iteration: 7,
            stage: "plan".into(),
            report: serde_json::json!({
                "status": "continue",
                "summary": "planned issue #28",
                "work_unit": "issue #28"
            }),
        },
        RunEvent::StageStarted {
            iteration: 7,
            stage: "implement".into(),
        },
        RunEvent::StageReported {
            iteration: 7,
            stage: "implement".into(),
            report: serde_json::json!({
                "status": "needs_input",
                "summary": "need a storage choice",
                "questions": [{"id": "q1", "text": "sqlite or files?"}]
            }),
        },
        RunEvent::StateChanged {
            state: RunState::Paused {
                reason: PauseReason::AwaitingHuman,
            },
        },
        RunEvent::QuestionAnswered {
            question_id: "q1".into(),
            answer: "sqlite".into(),
        },
        RunEvent::RunResumed {
            note: Some("keep the schema narrow".into()),
            extend: None,
        },
    ]));

    assert_eq!(PipelineKernel.run(ctx).await.unwrap(), RunOutcome::Done);
    assert!(!sink.events().iter().any(|event| matches!(
        event,
        RunEvent::RunStarted { .. } | RunEvent::IterationStarted { iteration: 7 }
    )));
    assert_eq!(
        stage_events(&sink.events()),
        [
            ("stage_started".into(), "implement".into()),
            ("stage_reported".into(), "implement".into()),
            ("stage_started".into(), "review".into()),
            ("stage_reported".into(), "review".into()),
            ("stage_started".into(), "simplify".into()),
            ("stage_reported".into(), "simplify".into()),
        ]
    );
    let resumed = &sandbox.agent_prompts()[0];
    assert!(resumed.contains("planned issue #28"), "{resumed}");
    assert!(
        resumed.contains("Q: sqlite or files?\n  A: sqlite"),
        "{resumed}"
    );
    assert!(
        resumed.contains("Note: keep the schema narrow"),
        "{resumed}"
    );
}

/// A replay aimed at a stage this flow never runs — deliver without a
/// configured prompt — fails the resume loudly before anything boots,
/// naming the stage instead of silently running an empty pass.
#[tokio::test]
async fn a_resume_aimed_at_an_inactive_stage_fails_loudly() {
    let workspace = seeded_repo();
    let sandbox = Arc::new(StagedSandbox::new(workspace.path().to_path_buf(), vec![]));
    let (mut ctx, _sink) = pipeline_context(
        workspace.path(),
        sandbox.clone(),
        VerifyConfig::default(),
        PromptsConfig::default(),
    );
    ctx.replay = Some(event_log(vec![
        RunEvent::IterationStarted { iteration: 1 },
        RunEvent::StageStarted {
            iteration: 1,
            stage: "deliver".into(),
        },
        RunEvent::StateChanged {
            state: RunState::Paused {
                reason: PauseReason::AwaitingHuman,
            },
        },
        RunEvent::RunResumed {
            note: None,
            extend: None,
        },
    ]));

    let error = PipelineKernel.run(ctx).await.unwrap_err();
    assert!(matches!(error, KernelError::Resume(_)), "{error}");
    assert!(error.to_string().contains("deliver"), "{error}");
    assert_eq!(sandbox.created(), 0);
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
            unrefuted(),
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
            unrefuted(),
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
    assert!(carries_handoff(second_plan), "{second_plan}");
    assert!(second_plan.contains("ITER1-SIMPLIFY"), "{second_plan}");
}

#[tokio::test]
async fn a_malformed_report_earns_one_repair_then_advances() {
    // Plan's first report is rejected; its repair is accepted, and the
    // run goes on.
    let ran = run_default(
        vec![malformed(), reports("done", "recovered"), unrefuted()],
        vec![],
    )
    .await;

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
    // Both report attempts shared the plan sandbox; the skeptic got a
    // second, fresh one after the repaired done claim.
    assert_eq!(ran.sandbox.created(), 2);
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
    let ran = run_default(vec![reports("done", "done"), unrefuted()], vec![]).await;
    assert!(matches!(
        &ran.events[0],
        RunEvent::RunStarted { kernel, agent } if kernel == "pipeline" && agent == "scripted"
    ));
}

// ---------- Verified Done: green checks and an unrefuted skeptic ----------

#[tokio::test]
async fn done_requires_green_checks_and_an_unrefuted_fresh_skeptic() {
    let ran = run_default(
        vec![reports("done", "the objective is complete"), unrefuted()],
        vec![],
    )
    .await;

    assert_eq!(ran.outcome, RunOutcome::Done);
    assert_eq!(
        kinds(&ran.events),
        [
            "run_started",
            "iteration_started",
            "stage_started",
            "agent_output",
            "stage_reported",
            "verify_check_finished",
            "agent_output",
            "skeptic_verdict",
            "state_changed",
        ]
    );
    assert!(matches!(
        ran.events.iter().find(|event| matches!(event, RunEvent::SkepticVerdict { .. })),
        Some(RunEvent::SkepticVerdict {
            iteration: 1,
            refuted: false,
            findings,
        }) if findings.is_empty()
    ));
    assert_eq!(ran.sandbox.created(), 2);
    assert_eq!(ran.sandbox.created(), ran.sandbox.destroyed());

    let skeptic_prompt = &ran.prompts[1];
    assert!(
        skeptic_prompt.contains("the objective is complete"),
        "{skeptic_prompt}"
    );
    assert!(skeptic_prompt.contains("SkepticReport"), "{skeptic_prompt}");
    for stage in ["plan", "implement", "review", "simplify"] {
        assert!(
            skeptic_prompt.contains(&format!("## {stage} domain prompt")),
            "{stage}: {skeptic_prompt}"
        );
    }
}

#[tokio::test]
async fn a_refuting_skeptic_feeds_the_next_plan_and_the_loop_continues() {
    let ran = run_default(
        vec![
            reports("done", "the first claim"),
            skeptic(true, &["TODO.md still lists the API as unfinished"]),
            reports("done", "the finding is resolved"),
            unrefuted(),
        ],
        vec![],
    )
    .await;

    assert_eq!(ran.outcome, RunOutcome::Done);
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

    let second_plan = &ran.prompts[2];
    assert!(
        second_plan.contains("## Completion claim refuted"),
        "{second_plan}"
    );
    assert!(
        second_plan.contains("TODO.md still lists the API as unfinished"),
        "{second_plan}"
    );
    assert_eq!(
        ran.events
            .iter()
            .filter(|event| matches!(event, RunEvent::SkepticVerdict { .. }))
            .count(),
        2
    );
    assert_eq!(ran.sandbox.created(), 4);
    assert_eq!(ran.sandbox.created(), ran.sandbox.destroyed());
}

#[tokio::test]
async fn a_done_claim_with_red_checks_never_reaches_the_skeptic() {
    let ran = run_default(
        vec![
            reports("done", "first premature claim"),
            reports("done", "second premature claim"),
        ],
        vec![1, 1],
    )
    .await;

    assert_eq!(ran.outcome, RunOutcome::Paused(PauseReason::VerifyFailed));
    assert!(carries_verify_feedback(&ran.prompts[1]));
    assert!(
        !ran.events
            .iter()
            .any(|event| matches!(event, RunEvent::SkepticVerdict { .. }))
    );
    assert_eq!(ran.sandbox.created(), 2);
    assert_eq!(ran.sandbox.created(), ran.sandbox.destroyed());
}

#[tokio::test]
async fn a_malformed_skeptic_report_earns_one_repair_in_the_same_sandbox() {
    let ran = run_default(
        vec![
            reports("done", "the objective is complete"),
            malformed(),
            unrefuted(),
        ],
        vec![],
    )
    .await;

    assert_eq!(ran.outcome, RunOutcome::Done);
    assert!(
        ran.events
            .iter()
            .any(|event| matches!(event, RunEvent::ReportRejected { .. }))
    );
    assert!(
        ran.prompts[2].contains("SkepticReport"),
        "{}",
        ran.prompts[2]
    );
    // Plan and skeptic get fresh sandboxes; the skeptic's report
    // repair stays in the skeptic sandbox because no new work runs.
    assert_eq!(ran.sandbox.created(), 2);
    assert_eq!(ran.sandbox.created(), ran.sandbox.destroyed());
}

#[tokio::test]
async fn a_refutation_without_findings_is_repaired_before_it_can_branch() {
    let ran = run_default(
        vec![
            reports("done", "the objective is complete"),
            skeptic(true, &[]),
            unrefuted(),
        ],
        vec![],
    )
    .await;

    assert_eq!(ran.outcome, RunOutcome::Done);
    let rejected = ran
        .events
        .iter()
        .find_map(|event| match event {
            RunEvent::ReportRejected { errors, .. } => Some(errors),
            _ => None,
        })
        .expect("the contradictory verdict is rejected");
    assert!(
        rejected
            .iter()
            .any(|error| error.contains("at least one finding")),
        "{rejected:?}"
    );
    assert!(
        ran.prompts[2].contains("SkepticReport"),
        "{}",
        ran.prompts[2]
    );
    assert_eq!(ran.sandbox.created(), 2);
}
