//! The pipeline kernel's shipped assets: a default prompt for every
//! active stage, and the report schema each dialect stage quotes.
//!
//! Both are compiled in. The default prompts fill any active slot a
//! flow leaves unset, so a minimal pipeline flow needs no prompt files.
//! `deliver` has a schema but no default prompt because its execution
//! remains boarded up until #29. The schemas are the committed
//! artifacts under `schemas/report/pipeline/`
//! — generated from proto's report types and drift-checked in CI — so
//! quoting them here cannot disagree with what the strict parse
//! enforces, and the engine reads the published contract without
//! carrying schemars (a product crate never does).

use proto::pipeline::Stage;

/// The kernel-shipped default prompt for an active stage. A dialect
/// stage may be published before its execution policy and therefore
/// have no default yet.
pub fn default_prompt(stage: Stage) -> Option<&'static str> {
    match stage {
        Stage::Plan => Some(include_str!("prompts/plan.md")),
        Stage::Implement => Some(include_str!("prompts/implement.md")),
        Stage::Review => Some(include_str!("prompts/review.md")),
        Stage::Simplify => Some(include_str!("prompts/simplify.md")),
        Stage::Deliver => None,
    }
}

/// The stage's committed report schema, quoted verbatim in its preamble
/// so the agent's output is constrained and self-repairable.
pub fn report_schema(stage: Stage) -> &'static str {
    match stage {
        Stage::Plan => include_str!("../../../../schemas/report/pipeline/plan.schema.json"),
        Stage::Implement => {
            include_str!("../../../../schemas/report/pipeline/implement.schema.json")
        }
        Stage::Review => include_str!("../../../../schemas/report/pipeline/review.schema.json"),
        Stage::Simplify => {
            include_str!("../../../../schemas/report/pipeline/simplify.schema.json")
        }
        Stage::Deliver => include_str!("../../../../schemas/report/pipeline/deliver.schema.json"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Active stages have defaults; every dialect stage has its own
    /// report schema, including the reserved deliver stage.
    #[test]
    fn active_stages_ship_defaults_and_every_stage_ships_a_schema() {
        for stage in Stage::ALL {
            match stage {
                Stage::Deliver => assert_eq!(default_prompt(stage), None),
                _ => assert!(
                    !default_prompt(stage).unwrap().trim().is_empty(),
                    "{stage:?}"
                ),
            }
            let schema = report_schema(stage);
            assert!(schema.contains("\"type\": \"object\""), "{stage:?}");
            let name = stage.as_str();
            let title = format!("\"{}{}Report\"", name[..1].to_uppercase(), &name[1..]);
            assert!(schema.contains(&title), "{stage:?}: want title {title}");
        }
    }

    /// The embedded schema is the committed artifact, so it matches what
    /// the strict parse enforces — a `deny_unknown_fields` object with
    /// the uniform status.
    #[test]
    fn the_embedded_schema_is_the_committed_contract() {
        let plan = report_schema(Stage::Plan);
        assert!(plan.contains("\"title\": \"PlanReport\""), "{plan}");
        assert!(plan.contains("\"additionalProperties\": false"), "{plan}");
        assert!(plan.contains("ReportStatus"), "{plan}");
    }
}
