//! Locks every event wire shape against the golden fixture.
//!
//! The fixture is regression armor for the published language: a serde
//! attribute edit or a variant rename that would break deployed
//! clients and recorded event logs fails here before it ships.

use std::collections::BTreeSet;

use proto::RunEvent;

const FIXTURE: &str = include_str!("fixtures/run_events.jsonl");

/// Growing `RunEvent` breaks this match at compile time; extend it
/// together with a fixture line and an entry in the coverage test's
/// `all` list. The coverage test catches a missing fixture line or a
/// missing list entry, but not both omitted at once — the compile
/// error here is the only prompt.
fn variant_tripwire(event: &RunEvent) {
    match event {
        RunEvent::RunStarted { .. }
        | RunEvent::StateChanged { .. }
        | RunEvent::IterationStarted { .. }
        | RunEvent::IterationFinished { .. }
        | RunEvent::AgentOutput { .. }
        | RunEvent::VerifyCheckFinished { .. }
        | RunEvent::WorkspaceCheckpointed { .. }
        | RunEvent::ProgressReported { .. }
        | RunEvent::ProgressRejected { .. }
        | RunEvent::SkepticVerdict { .. }
        | RunEvent::TokensUsed { .. }
        | RunEvent::BudgetExhausted { .. }
        | RunEvent::QuestionAnswered { .. }
        | RunEvent::RunResumed { .. } => {}
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
    let events = fixture_events();
    let covered: BTreeSet<&str> = events
        .iter()
        .map(|(wire, event)| {
            variant_tripwire(event);
            wire["type"].as_str().expect("events are tagged by `type`")
        })
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
