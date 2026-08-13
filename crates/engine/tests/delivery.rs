//! The optional deliver stage driven over the testkit fakes: a
//! configured `deliver` prompt slot runs delivery last with the pass's
//! reports in its preamble; an absent slot skips the stage entirely.
//! House pattern — assert the emitted events, the run outcome, and the
//! sandbox counters, never internal call patterns.

use std::sync::Arc;

use engine::testkit::{
    AgentStep, Ran, StagedSandbox, drive_pipeline, pipeline_context, reports, seeded_repo,
    stage_events, unrefuted,
};
use engine::{
    Budgets, Kernel, PauseReason, PipelineKernel, PromptsConfig, RunOutcome, VerifyConfig,
};

async fn run_with_delivery(agent_steps: Vec<AgentStep>) -> Ran {
    let workspace = seeded_repo();
    let sandbox = Arc::new(StagedSandbox::new(
        workspace.path().to_path_buf(),
        agent_steps,
    ));
    sandbox.seed_guest_file("/workspace/prompts/deliver.md", b"DELIVER-RULES\n".to_vec());
    let prompts = serde_json::from_value(serde_json::json!({
        "deliver": "prompts/deliver.md"
    }))
    .unwrap();
    drive_pipeline(workspace, sandbox, VerifyConfig::default(), prompts).await
}

fn through_core(tail: Vec<AgentStep>) -> Vec<AgentStep> {
    [
        reports("continue", "planned"),
        reports("continue", "built"),
        reports("continue", "reviewed"),
        reports("continue", "simplified"),
    ]
    .into_iter()
    .chain(tail)
    .collect()
}

#[tokio::test]
async fn a_configured_deliver_stage_runs_last_with_the_iteration_reports() {
    let ran = run_with_delivery(vec![
        reports("continue", "PLAN-MARKER"),
        reports("continue", "IMPLEMENT-MARKER"),
        reports("continue", "REVIEW-MARKER"),
        reports("continue", "SIMPLIFY-MARKER"),
        reports("done", "delivered"),
        unrefuted(),
    ])
    .await;

    assert_eq!(ran.outcome, RunOutcome::Done);
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
            ("stage_started".into(), "deliver".into()),
            ("stage_reported".into(), "deliver".into()),
        ]
    );
    let deliver = &ran.prompts[4];
    for marker in [
        "PLAN-MARKER",
        "IMPLEMENT-MARKER",
        "REVIEW-MARKER",
        "SIMPLIFY-MARKER",
    ] {
        assert!(deliver.contains(marker), "{marker}: {deliver}");
    }
    assert!(deliver.contains("DELIVER-RULES"), "{deliver}");
    assert!(
        deliver.contains("\"title\": \"DeliverReport\""),
        "{deliver}"
    );
    assert!(ran.prompts[5].contains("### deliver domain prompt"));
    assert!(ran.prompts[5].contains("DELIVER-RULES"));
    assert_eq!(ran.sandbox.created(), 6);
    assert_eq!(ran.sandbox.created(), ran.sandbox.destroyed());
}

/// `done` is a claim from any stage and ends the run mid-pass, so a
/// configured delivery is not a completion epilogue: the pass that
/// finishes the work is not delivered — the previous full pass was.
#[tokio::test]
async fn a_mid_pass_done_claim_ends_the_run_without_reaching_deliver() {
    let ran = run_with_delivery(vec![
        reports("continue", "planned"),
        reports("continue", "built"),
        reports("continue", "reviewed"),
        reports("done", "the work is complete"),
        unrefuted(),
    ])
    .await;

    assert_eq!(ran.outcome, RunOutcome::Done);
    // Four stages plus the skeptic; deliver never booted a sandbox.
    assert_eq!(ran.sandbox.created(), 5);
    assert_eq!(ran.sandbox.created(), ran.sandbox.destroyed());
    assert!(
        stage_events(&ran.events)
            .iter()
            .all(|(_, stage)| stage != "deliver")
    );
}

#[tokio::test]
async fn an_absent_deliver_prompt_skips_the_stage_without_a_sandbox() {
    let workspace = seeded_repo();
    let sandbox = Arc::new(StagedSandbox::new(
        workspace.path().to_path_buf(),
        through_core(vec![]),
    ));
    let (mut ctx, sink) = pipeline_context(
        workspace.path(),
        sandbox.clone(),
        VerifyConfig::default(),
        PromptsConfig::default(),
    );
    ctx.budgets = Budgets {
        max_iterations: Some(1),
        ..Budgets::default()
    };

    assert_eq!(
        PipelineKernel.run(ctx).await.unwrap(),
        RunOutcome::Paused(PauseReason::Budget)
    );
    assert_eq!(
        stage_events(&sink.events()),
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
    assert_eq!(sandbox.created(), 4);
    assert_eq!(sandbox.created(), sandbox.destroyed());
}

#[tokio::test]
async fn deliver_pause_statuses_stop_the_run() {
    for (status, reason) in [
        ("blocked", PauseReason::Blocked),
        ("needs_input", PauseReason::AwaitingHuman),
    ] {
        let ran = run_with_delivery(through_core(vec![reports(status, "cannot publish")])).await;

        assert_eq!(ran.outcome, RunOutcome::Paused(reason), "{status}");
        assert_eq!(ran.sandbox.created(), 5, "{status}");
        assert_eq!(ran.sandbox.created(), ran.sandbox.destroyed(), "{status}");
        assert_eq!(stage_events(&ran.events).last().unwrap().1, "deliver");
    }
}
