//! Pins the fanout plan schema quoted to agents to the strict serde
//! parser the kernel applies to their reports.

mod common;

use std::sync::LazyLock;

use proto::fanout::{PlanReport, plan_schema};
use serde_json::json;

static SCHEMA: LazyLock<serde_json::Value> =
    LazyLock::new(|| serde_json::to_value(plan_schema()).expect("schema serializes"));
static VALIDATOR: LazyLock<jsonschema::Validator> =
    LazyLock::new(|| jsonschema::validator_for(&SCHEMA).expect("schema compiles"));

fn schema_accepts(report: &serde_json::Value) -> bool {
    VALIDATOR.is_valid(report)
}

fn serde_accepts(report: &serde_json::Value) -> bool {
    serde_json::from_value::<PlanReport>(report.clone()).is_ok()
}

#[test]
fn schema_and_serde_agree_on_fanout_reports() {
    let corpus = [
        (
            "ready units",
            json!({
                "status": "continue",
                "summary": "two ready",
                "units": ["issue #31", "issue #32"]
            }),
            true,
        ),
        (
            "blocked empty frontier",
            json!({
                "status": "blocked",
                "summary": "children still running",
                "units": [],
                "blockers": ["in-flight work"]
            }),
            true,
        ),
        (
            "question",
            json!({
                "status": "needs_input",
                "summary": "need priority",
                "questions": [{"id": "q1", "text": "which first?", "options": []}]
            }),
            true,
        ),
        (
            "tracker vocabulary leaked into the shape",
            json!({
                "status": "continue",
                "summary": "ready",
                "units": [],
                "frontier": [31]
            }),
            false,
        ),
        (
            "misspelled status",
            json!({"status": "running", "summary": "ready"}),
            false,
        ),
        ("missing summary", json!({"status": "done"}), false),
    ];
    for (name, report, expected) in corpus {
        assert_eq!(schema_accepts(&report), expected, "schema: {name}");
        assert_eq!(serde_accepts(&report), expected, "serde: {name}");
    }
}

#[test]
fn every_object_rejects_unknown_keys() {
    common::assert_every_object_rejects_unknown_keys(&SCHEMA);
}

#[test]
fn the_schema_introduces_no_integer_formats() {
    assert_eq!(common::assert_integer_formats_carry_bounds(&SCHEMA), 0);
}
