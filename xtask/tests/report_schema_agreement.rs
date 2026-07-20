//! Pins the generated stage-report schemas — the contract each
//! stage's preamble quotes to its agent — to the strict serde parse
//! the kernel runs on what comes back. A report one accepts and the
//! other rejects would send the repair loop chasing a contract the
//! parser contradicts. These tests live with the generator: xtask is
//! the one crate that always enables proto's `schema` feature, so
//! they run on every `cargo test --workspace`.

mod common;

use std::sync::LazyLock;

use proto::report::{Stage, StageReport, stage_schema};
use serde_json::json;

/// Generated once for the whole binary: every test reads the same
/// schemas, and compiling their validators is the expensive step.
static SCHEMAS: LazyLock<Vec<(Stage, serde_json::Value, jsonschema::Validator)>> =
    LazyLock::new(|| {
        Stage::ALL
            .into_iter()
            .map(|stage| {
                let schema = serde_json::to_value(stage_schema(stage)).expect("schema serializes");
                let validator = jsonschema::validator_for(&schema).expect("schema compiles");
                (stage, schema, validator)
            })
            .collect()
    });

fn schema_of(stage: Stage) -> &'static serde_json::Value {
    let (_, schema, _) = SCHEMAS.iter().find(|(s, _, _)| *s == stage).unwrap();
    schema
}

fn schema_accepts(stage: Stage, report: &serde_json::Value) -> bool {
    let (_, _, validator) = SCHEMAS.iter().find(|(s, _, _)| *s == stage).unwrap();
    validator.is_valid(report)
}

fn serde_accepts(stage: Stage, report: &serde_json::Value) -> bool {
    StageReport::from_stage_json(stage, &report.to_string()).is_ok()
}

/// The smallest report every stage accepts: the uniform status and
/// the summary.
fn minimal() -> serde_json::Value {
    json!({"status": "continue", "summary": "did the thing"})
}

/// `base` with `extra`'s keys merged over it.
fn with(base: serde_json::Value, extra: serde_json::Value) -> serde_json::Value {
    let mut merged = base;
    let entries = merged.as_object_mut().unwrap();
    for (key, value) in extra.as_object().unwrap() {
        entries.insert(key.clone(), value.clone());
    }
    merged
}

/// Each stage's own payload — the fields that exist on no other
/// stage's shape (simplify has none: its report is the common core).
fn payload(stage: Stage) -> serde_json::Value {
    match stage {
        Stage::Plan => json!({"work_unit": "issue #7", "steps": ["add the type"]}),
        Stage::Implement => json!({"remaining": ["wire the schema"]}),
        Stage::Review => json!({"findings": ["error paths untested"]}),
        Stage::Simplify => json!({}),
        Stage::Deliver => json!({"links": ["https://example.com/pr/1"]}),
    }
}

/// The corpus every stage must judge identically to strict serde —
/// shape-agnostic entries exercising the uniform core.
#[test]
fn schema_and_serde_agree_on_the_shared_corpus() {
    let corpus: &[(&str, serde_json::Value, bool)] = &[
        ("minimal report", minimal(), true),
        (
            "blocked with blockers",
            with(
                minimal(),
                json!({"status": "blocked", "blockers": ["no push credential"]}),
            ),
            true,
        ),
        (
            "needs_input with a full question",
            with(
                minimal(),
                json!({
                    "status": "needs_input",
                    "questions": [{"id": "q1", "text": "which way?", "options": ["a", "b"]}],
                }),
            ),
            true,
        ),
        (
            "unknown field",
            with(minimal(), json!({"mood": "optimistic"})),
            false,
        ),
        (
            "unknown field inside a question",
            with(
                minimal(),
                json!({"questions": [{"id": "q1", "text": "t", "urgency": "high"}]}),
            ),
            false,
        ),
        (
            "question missing its text",
            with(minimal(), json!({"questions": [{"id": "q1"}]})),
            false,
        ),
        (
            "misspelled status",
            with(minimal(), json!({"status": "paused"})),
            false,
        ),
        (
            "status not a string",
            with(minimal(), json!({"status": 2})),
            false,
        ),
        ("missing status", json!({"summary": "s"}), false),
        ("missing summary", json!({"status": "done"}), false),
    ];
    for stage in Stage::ALL {
        for (name, report, accepted) in corpus {
            assert_eq!(
                schema_accepts(stage, report),
                *accepted,
                "schema on {stage:?}: {name}"
            );
            assert_eq!(
                serde_accepts(stage, report),
                *accepted,
                "serde on {stage:?}: {name}"
            );
        }
    }
}

#[test]
fn each_stage_accepts_its_own_payload() {
    for stage in Stage::ALL {
        let report = with(minimal(), payload(stage));
        assert!(schema_accepts(stage, &report), "schema on {stage:?}");
        assert!(serde_accepts(stage, &report), "serde on {stage:?}");
    }
}

/// A payload field belongs to exactly one stage; carried anywhere
/// else it is an unknown key, and both judges must say so.
#[test]
fn a_payload_field_from_another_stage_is_rejected() {
    for stage in Stage::ALL {
        let own = payload(stage);
        let own = own.as_object().unwrap();
        for other in Stage::ALL {
            for (key, value) in payload(other).as_object().unwrap() {
                if own.contains_key(key) {
                    continue;
                }
                let report = with(minimal(), json!({key.as_str(): value}));
                assert!(
                    !schema_accepts(stage, &report),
                    "schema on {stage:?}: {key}"
                );
                assert!(!serde_accepts(stage, &report), "serde on {stage:?}: {key}");
            }
        }
    }
}

/// Plan's work unit is nullable — a blocked plan selected nothing —
/// and both judges must accept the explicit null.
#[test]
fn a_plan_without_a_work_unit_is_accepted() {
    let report = with(
        minimal(),
        json!({"status": "blocked", "work_unit": null, "blockers": ["empty frontier"]}),
    );
    assert!(schema_accepts(Stage::Plan, &report));
    assert!(serde_accepts(Stage::Plan, &report));
}

/// Every stage's schema demands the uniform status, spelled with the
/// shared four-value vocabulary — the artifact-level pin of the
/// contract's one invariant.
#[test]
fn every_stage_schema_requires_the_uniform_status() {
    for stage in Stage::ALL {
        let schema = schema_of(stage);
        let required = schema["required"].as_array().unwrap();
        assert!(required.contains(&json!("status")), "{stage:?}");
        assert!(required.contains(&json!("summary")), "{stage:?}");
        assert_eq!(
            schema["$defs"]["ReportStatus"]["enum"],
            json!(["continue", "done", "blocked", "needs_input"]),
            "{stage:?}"
        );
    }
}

#[test]
fn every_object_in_every_stage_schema_rejects_unknown_keys() {
    for stage in Stage::ALL {
        common::assert_every_object_rejects_unknown_keys(schema_of(stage));
    }
}

/// No report carries an integer today; the guard arms the generator's
/// bounds transform the moment one does.
#[test]
fn every_integer_format_in_every_stage_schema_carries_bounds() {
    for stage in Stage::ALL {
        common::assert_integer_formats_carry_bounds(schema_of(stage));
    }
}
