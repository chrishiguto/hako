//! Locks every event wire shape against the golden fixture.
//!
//! The fixture is regression armor for the published language: a serde
//! attribute edit or a variant rename that would break deployed
//! clients and recorded event logs fails here before it ships.

use std::collections::BTreeSet;

use proto::RunEvent;

const FIXTURE: &str = include_str!("fixtures/run_events.jsonl");

/// Growing `RunEvent` breaks this match at compile time; add the new
/// arm together with a fixture line and an entry in the coverage
/// test's `all` list. The coverage test catches a missing fixture
/// line or a missing list entry, but not both omitted at once — the
/// compile error here is the only prompt.
fn variant(event: &RunEvent) -> &'static str {
    match event {
        RunEvent::RunStarted { .. } => "run_started",
        RunEvent::StateChanged { .. } => "state_changed",
        RunEvent::IterationStarted { .. } => "iteration_started",
        RunEvent::IterationFinished { .. } => "iteration_finished",
        RunEvent::AgentOutput { .. } => "agent_output",
        RunEvent::VerifyCheckFinished { .. } => "verify_check_finished",
        RunEvent::WorkspaceCheckpointed { .. } => "workspace_checkpointed",
        RunEvent::ProgressReported { .. } => "progress_reported",
        RunEvent::ProgressRejected { .. } => "progress_rejected",
        RunEvent::SkepticVerdict { .. } => "skeptic_verdict",
        RunEvent::TokensUsed { .. } => "tokens_used",
        RunEvent::BudgetExhausted { .. } => "budget_exhausted",
        RunEvent::QuestionAnswered { .. } => "question_answered",
        RunEvent::RunResumed { .. } => "run_resumed",
    }
}

fn fixture_events() -> Vec<(serde_json::Value, RunEvent)> {
    FIXTURE
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            let wire: serde_json::Value = serde_json::from_str(line).unwrap();
            let event: RunEvent = serde_json::from_value(wire.clone())
                .unwrap_or_else(|e| panic!("fixture line does not deserialize: {line}\n{e}"));
            (wire, event)
        })
        .collect()
}

#[test]
fn every_fixture_line_reserializes_identically() {
    for (wire, event) in fixture_events() {
        assert_eq!(
            serde_json::to_value(&event).unwrap(),
            wire,
            "wire shape drifted for: {wire}"
        );
    }
}

#[test]
fn the_fixture_covers_every_event_variant() {
    let covered: BTreeSet<&str> = fixture_events()
        .iter()
        .map(|(_, event)| variant(event))
        .collect();
    let all: BTreeSet<&str> = [
        "run_started",
        "state_changed",
        "iteration_started",
        "iteration_finished",
        "agent_output",
        "verify_check_finished",
        "workspace_checkpointed",
        "progress_reported",
        "progress_rejected",
        "skeptic_verdict",
        "tokens_used",
        "budget_exhausted",
        "question_answered",
        "run_resumed",
    ]
    .into();
    assert_eq!(covered, all);
}
