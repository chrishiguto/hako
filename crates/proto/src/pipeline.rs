//! The pipeline kernel's dialect — the vocabulary one kernel adds to
//! the published language: its stages and the report shape each stage
//! writes. Dialects build on the shared report vocabulary in
//! [`crate::report`] — the uniform status, questions, answers — and
//! never redefine it; one module per kernel, so the line between core
//! language and kernel dialect stays visible in the crate tree.
//!
//! The shapes mirror the flow format's pattern: Rust types as the
//! source of truth, generated JSON Schemas (committed under
//! `schemas/report/pipeline/`, drift-checked in CI) for the preamble
//! to quote, and a repair re-prompt fed by the strict parse's errors.
//!
//! Doc comments on the report types become the schemas' descriptions,
//! so they are written to the agent filling the report in.

use serde::{Deserialize, Serialize};

use crate::report::{Question, ReportStatus};

/// The pipeline kernel's stages, in the order one iteration drives a
/// work unit through them. Deliver is optional — a flow without a
/// deliver prompt skips the stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Stage {
    Plan,
    Implement,
    Review,
    Simplify,
    Deliver,
}

impl Stage {
    /// Every stage in kernel order — what the schema generator and any
    /// per-stage table iterate. The one enumeration of the stages the
    /// compiler cannot check: a new variant must be added here too, or
    /// it ships with no schema. The ordinal test below keeps the order
    /// honest; completeness rests on review.
    pub const ALL: [Self; 5] = [
        Self::Plan,
        Self::Implement,
        Self::Review,
        Self::Simplify,
        Self::Deliver,
    ];

    /// The wire string naming the stage — the same string serde reads,
    /// spelled once for schema file names, events, and error messages.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Plan => "plan",
            Self::Implement => "implement",
            Self::Review => "review",
            Self::Simplify => "simplify",
            Self::Deliver => "deliver",
        }
    }
}

/// What the plan stage leaves behind: the work unit this iteration
/// drives and the intended route through it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct PlanReport {
    pub status: ReportStatus,
    /// The plan in the agent's words: what this iteration will change
    /// and why.
    pub summary: String,
    /// The work unit this iteration drives, named so a human can trace
    /// it to its source — a ticket handle, a one-line description.
    /// Absent when no unit could be selected.
    pub work_unit: Option<String>,
    /// The intended steps in order; the implement stage follows them.
    #[serde(default)]
    pub steps: Vec<String>,
    /// What prevents progress, one entry per blocker; expected when
    /// the status is `blocked`.
    #[serde(default)]
    pub blockers: Vec<String>,
    /// Questions only a human can answer; expected when the status is
    /// `needs_input`.
    #[serde(default)]
    pub questions: Vec<Question>,
}

/// What the implement stage leaves behind: the change it made and
/// what it knowingly left open.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ImplementReport {
    pub status: ReportStatus,
    /// What was built, in the agent's words.
    pub summary: String,
    /// Work the agent believes is still open on this unit; feeds the
    /// next iteration's plan.
    #[serde(default)]
    pub remaining: Vec<String>,
    /// What prevents progress, one entry per blocker; expected when
    /// the status is `blocked`.
    #[serde(default)]
    pub blockers: Vec<String>,
    /// Questions only a human can answer; expected when the status is
    /// `needs_input`.
    #[serde(default)]
    pub questions: Vec<Question>,
}

/// What the review stage leaves behind: its verdict, plus the
/// findings it could not fix in place.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ReviewReport {
    pub status: ReportStatus,
    /// The verdict in the agent's words: what was checked, what was
    /// patched in place.
    pub summary: String,
    /// Findings the review could not patch itself; they feed the next
    /// iteration's plan.
    #[serde(default)]
    pub findings: Vec<String>,
    /// What prevents progress, one entry per blocker; expected when
    /// the status is `blocked`.
    #[serde(default)]
    pub blockers: Vec<String>,
    /// Questions only a human can answer; expected when the status is
    /// `needs_input`.
    #[serde(default)]
    pub questions: Vec<Question>,
}

/// What the simplify stage leaves behind.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct SimplifyReport {
    pub status: ReportStatus,
    /// What was simplified, in the agent's words — or why nothing
    /// needed it.
    pub summary: String,
    /// What prevents progress, one entry per blocker; expected when
    /// the status is `blocked`.
    #[serde(default)]
    pub blockers: Vec<String>,
    /// Questions only a human can answer; expected when the status is
    /// `needs_input`.
    #[serde(default)]
    pub questions: Vec<Question>,
}

/// What the deliver stage leaves behind: where the published work
/// lives.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct DeliverReport {
    pub status: ReportStatus,
    /// What was published, in the agent's words.
    pub summary: String,
    /// URLs for everything the delivery touched — the pull request,
    /// closed issues, comments.
    #[serde(default)]
    pub links: Vec<String>,
    /// What prevents progress, one entry per blocker; expected when
    /// the status is `blocked`.
    #[serde(default)]
    pub blockers: Vec<String>,
    /// Questions only a human can answer; expected when the status is
    /// `needs_input`.
    #[serde(default)]
    pub questions: Vec<Question>,
}

/// One stage's report, whichever stage wrote it — what the kernel
/// holds when it carries reports across stages. Parsing dispatches on
/// the stage the kernel just ran: the report file is not
/// self-describing, the pipeline's position is what names it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StageReport {
    Plan(PlanReport),
    Implement(ImplementReport),
    Review(ReviewReport),
    Simplify(SimplifyReport),
    Deliver(DeliverReport),
}

impl StageReport {
    /// The strict agent-boundary parse of `stage`'s report: unknown
    /// fields are rejected, so a mistyped key fails the report and
    /// feeds the repair re-prompt instead of being half-understood.
    pub fn from_stage_json(stage: Stage, json: &str) -> serde_json::Result<Self> {
        Ok(match stage {
            Stage::Plan => Self::Plan(serde_json::from_str(json)?),
            Stage::Implement => Self::Implement(serde_json::from_str(json)?),
            Stage::Review => Self::Review(serde_json::from_str(json)?),
            Stage::Simplify => Self::Simplify(serde_json::from_str(json)?),
            Stage::Deliver => Self::Deliver(serde_json::from_str(json)?),
        })
    }

    /// Which stage wrote this report.
    pub fn stage(&self) -> Stage {
        match self {
            Self::Plan(_) => Stage::Plan,
            Self::Implement(_) => Stage::Implement,
            Self::Review(_) => Stage::Review,
            Self::Simplify(_) => Stage::Simplify,
            Self::Deliver(_) => Stage::Deliver,
        }
    }

    /// The uniform status every shape carries — what the kernel gates
    /// on, never the payload.
    pub fn status(&self) -> ReportStatus {
        match self {
            Self::Plan(report) => report.status,
            Self::Implement(report) => report.status,
            Self::Review(report) => report.status,
            Self::Simplify(report) => report.status,
            Self::Deliver(report) => report.status,
        }
    }

    /// The questions a `needs_input` report asks — uniform across
    /// stages, so pausing surfaces them the same wherever they arose.
    pub fn questions(&self) -> &[Question] {
        match self {
            Self::Plan(report) => &report.questions,
            Self::Implement(report) => &report.questions,
            Self::Review(report) => &report.questions,
            Self::Simplify(report) => &report.questions,
            Self::Deliver(report) => &report.questions,
        }
    }
}

/// The stage's report schema, generated from its type so the two
/// cannot disagree. Committed under `schemas/report/pipeline/`,
/// drift-checked in CI, and quoted verbatim in the stage's preamble.
#[cfg(feature = "schema")]
pub fn stage_schema(stage: Stage) -> schemars::Schema {
    match stage {
        Stage::Plan => crate::schema::root_schema_for::<PlanReport>(),
        Stage::Implement => crate::schema::root_schema_for::<ImplementReport>(),
        Stage::Review => crate::schema::root_schema_for::<ReviewReport>(),
        Stage::Simplify => crate::schema::root_schema_for::<SimplifyReport>(),
        Stage::Deliver => crate::schema::root_schema_for::<DeliverReport>(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    /// The smallest report every stage accepts: the uniform status
    /// plus the summary; all payloads default.
    fn minimal(status: &str) -> String {
        json!({"status": status, "summary": "did the thing"}).to_string()
    }

    #[test]
    fn every_stage_matches_its_wire_string() {
        let wires = ["plan", "implement", "review", "simplify", "deliver"];
        assert_eq!(Stage::ALL.len(), wires.len());
        for (stage, wire) in Stage::ALL.into_iter().zip(wires) {
            assert_eq!(stage.as_str(), wire);
            assert_eq!(serde_json::to_value(stage).unwrap(), json!(wire));
            let parsed: Stage = serde_json::from_value(json!(wire)).unwrap();
            assert_eq!(parsed, stage);
        }
    }

    /// Pins `ALL` to kernel order: the exhaustive match forces every
    /// variant to declare its ordinal, and the loop demands `ALL`
    /// carry the declared ordinals in order, with no gaps or
    /// duplicates. An insertion that renumbers the stages but forgets
    /// `ALL` fails here; a forgotten append only review can catch.
    #[test]
    fn all_lists_the_stages_in_kernel_order() {
        fn ordinal(stage: Stage) -> usize {
            match stage {
                Stage::Plan => 0,
                Stage::Implement => 1,
                Stage::Review => 2,
                Stage::Simplify => 3,
                Stage::Deliver => 4,
            }
        }
        for (index, stage) in Stage::ALL.into_iter().enumerate() {
            assert_eq!(ordinal(stage), index, "{stage:?}");
        }
    }

    #[test]
    fn each_stage_report_round_trips_with_its_payload() {
        let question = Question {
            id: "q1".into(),
            text: "which way?".into(),
            options: vec![],
        };
        let reports = [
            StageReport::Plan(PlanReport {
                status: ReportStatus::Continue,
                summary: "drive issue #7".into(),
                work_unit: Some("issue #7".into()),
                steps: vec!["add the type".into(), "wire the schema".into()],
                blockers: vec![],
                questions: vec![question.clone()],
            }),
            StageReport::Implement(ImplementReport {
                status: ReportStatus::Continue,
                summary: "added the type".into(),
                remaining: vec!["wire the schema".into()],
                blockers: vec![],
                questions: vec![],
            }),
            StageReport::Review(ReviewReport {
                status: ReportStatus::Continue,
                summary: "patched naming".into(),
                findings: vec!["error paths untested".into()],
                blockers: vec![],
                questions: vec![],
            }),
            StageReport::Simplify(SimplifyReport {
                status: ReportStatus::Done,
                summary: "folded the twins".into(),
                blockers: vec![],
                questions: vec![],
            }),
            StageReport::Deliver(DeliverReport {
                status: ReportStatus::Blocked,
                summary: "push rejected".into(),
                links: vec!["https://example.com/pr/1".into()],
                blockers: vec!["no push credential".into()],
                questions: vec![],
            }),
        ];
        for report in reports {
            let wire = match &report {
                StageReport::Plan(r) => serde_json::to_string(r).unwrap(),
                StageReport::Implement(r) => serde_json::to_string(r).unwrap(),
                StageReport::Review(r) => serde_json::to_string(r).unwrap(),
                StageReport::Simplify(r) => serde_json::to_string(r).unwrap(),
                StageReport::Deliver(r) => serde_json::to_string(r).unwrap(),
            };
            let parsed = StageReport::from_stage_json(report.stage(), &wire).unwrap();
            assert_eq!(parsed, report);
        }
    }

    #[test]
    fn every_stage_accepts_the_minimal_report_and_defaults_its_payload() {
        for stage in Stage::ALL {
            let report = StageReport::from_stage_json(stage, &minimal("continue")).unwrap();
            assert_eq!(report.stage(), stage);
            assert_eq!(report.status(), ReportStatus::Continue, "{stage:?}");
            assert!(report.questions().is_empty(), "{stage:?}");
        }
    }

    #[test]
    fn every_stage_carries_the_uniform_status_vocabulary() {
        for stage in Stage::ALL {
            for (wire, status) in [
                ("continue", ReportStatus::Continue),
                ("done", ReportStatus::Done),
                ("blocked", ReportStatus::Blocked),
                ("needs_input", ReportStatus::NeedsInput),
            ] {
                let report = StageReport::from_stage_json(stage, &minimal(wire)).unwrap();
                assert_eq!(report.status(), status, "{stage:?}");
            }
            let err = StageReport::from_stage_json(stage, &minimal("paused")).unwrap_err();
            assert!(err.to_string().contains("paused"), "{stage:?}: {err}");
        }
    }

    #[test]
    fn an_unknown_field_is_rejected_naming_it() {
        for stage in Stage::ALL {
            let report =
                json!({"status": "continue", "summary": "s", "mood": "optimistic"}).to_string();
            let err = StageReport::from_stage_json(stage, &report).unwrap_err();
            assert!(err.to_string().contains("mood"), "{stage:?}: {err}");
        }
    }

    #[test]
    fn an_unknown_field_inside_a_question_is_rejected() {
        for stage in Stage::ALL {
            let report = json!({
                "status": "needs_input",
                "summary": "s",
                "questions": [{"id": "q1", "text": "t", "urgency": "high"}],
            })
            .to_string();
            let err = StageReport::from_stage_json(stage, &report).unwrap_err();
            assert!(err.to_string().contains("urgency"), "{stage:?}: {err}");
        }
    }

    #[test]
    fn a_report_missing_the_uniform_fields_is_rejected_naming_them() {
        for stage in Stage::ALL {
            let err = StageReport::from_stage_json(stage, r#"{"summary": "s"}"#).unwrap_err();
            assert!(err.to_string().contains("status"), "{stage:?}: {err}");
            let err = StageReport::from_stage_json(stage, r#"{"status": "done"}"#).unwrap_err();
            assert!(err.to_string().contains("summary"), "{stage:?}: {err}");
        }
    }

    #[test]
    fn every_needs_input_report_surfaces_its_questions_uniformly() {
        for stage in Stage::ALL {
            let report = json!({
                "status": "needs_input",
                "summary": "s",
                "questions": [{"id": "q1", "text": "which way?", "options": ["a", "b"]}],
            })
            .to_string();
            let report = StageReport::from_stage_json(stage, &report).unwrap();
            assert_eq!(report.status(), ReportStatus::NeedsInput);
            let [question] = report.questions() else {
                panic!("{stage:?}: expected one question");
            };
            assert_eq!(question.id, "q1");
            assert_eq!(question.options, ["a", "b"]);
        }
    }
}
