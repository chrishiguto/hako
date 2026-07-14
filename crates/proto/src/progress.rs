//! The progress report — the only structured channel from agent to
//! engine, written by the agent at the end of every iteration.
//!
//! One wire shape, two parse postures. Deserializing these types is
//! lenient about unknown fields so older clients survive newer
//! daemons; the agent-facing ingest is
//! [`ProgressReport::from_agent_json`], which rejects unknown fields
//! so a malformed report fails loudly and feeds the one repair
//! re-prompt (ADR 0007) instead of being half-understood.

use serde::{Deserialize, Serialize};

/// What the agent declares when an iteration ends.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ProgressReport {
    pub status: ProgressStatus,
    /// What happened this iteration, in the agent's words.
    pub summary: String,
    /// Work the agent believes is still open. Comparing this across
    /// iterations is one half of drift detection.
    #[serde(default)]
    pub remaining: Vec<String>,
    #[serde(default)]
    pub blockers: Vec<String>,
    /// Present when the status is `needs_input`; answered by a human
    /// through the API.
    #[serde(default)]
    pub questions: Vec<Question>,
}

impl ProgressReport {
    /// The agent-boundary parse: unknown fields are rejected, so a
    /// mistyped key fails the report instead of being ignored.
    pub fn from_agent_json(json: &str) -> serde_json::Result<Self> {
        serde_json::from_str::<strict::ProgressReport>(json).map(Into::into)
    }
}

/// The agent's claim about where the loop stands. `done` is a claim,
/// not a verdict — the engine accepts it only after verify checks pass
/// and a skeptic iteration fails to refute it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum ProgressStatus {
    Continue,
    Done,
    Blocked,
    NeedsInput,
}

/// A question the agent needs a human to answer before it can proceed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct Question {
    /// The handle an answer is addressed to (`hako answer <run> <id>`).
    pub id: String,
    pub text: String,
    /// Suggested answers; free-text is always allowed.
    #[serde(default)]
    pub options: Vec<String>,
}

/// Strict twins of the public types: serde cannot hang two postures
/// off one derive. Exhaustive destructuring in the `From` impls keeps
/// the twins honest — a field added to either side breaks the
/// conversion at compile time.
mod strict {
    use serde::Deserialize;

    use super::ProgressStatus;

    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct ProgressReport {
        pub status: ProgressStatus,
        pub summary: String,
        #[serde(default)]
        pub remaining: Vec<String>,
        #[serde(default)]
        pub blockers: Vec<String>,
        #[serde(default)]
        pub questions: Vec<Question>,
    }

    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct Question {
        pub id: String,
        pub text: String,
        #[serde(default)]
        pub options: Vec<String>,
    }

    impl From<ProgressReport> for super::ProgressReport {
        fn from(report: ProgressReport) -> Self {
            let ProgressReport {
                status,
                summary,
                remaining,
                blockers,
                questions,
            } = report;
            Self {
                status,
                summary,
                remaining,
                blockers,
                questions: questions.into_iter().map(Into::into).collect(),
            }
        }
    }

    impl From<Question> for super::Question {
        fn from(question: Question) -> Self {
            let Question { id, text, options } = question;
            Self { id, text, options }
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    /// The exact shape locked in the v1 design session — if this test
    /// breaks, the agent-facing contract broke.
    #[test]
    fn the_locked_shape_parses_at_the_agent_boundary() {
        let report = ProgressReport::from_agent_json(
            &json!({
                "status": "continue",
                "summary": "what happened this iteration",
                "remaining": ["..."],
                "blockers": ["..."],
                "questions": [{ "id": "q1", "text": "...", "options": ["..."] }]
            })
            .to_string(),
        )
        .unwrap();
        assert_eq!(report.status, ProgressStatus::Continue);
        assert_eq!(report.summary, "what happened this iteration");
        assert_eq!(report.remaining, vec!["..."]);
        assert_eq!(report.blockers, vec!["..."]);
        assert_eq!(
            report.questions,
            vec![Question {
                id: "q1".into(),
                text: "...".into(),
                options: vec!["...".into()],
            }]
        );
    }

    #[test]
    fn every_status_matches_its_wire_string() {
        let statuses = [
            (ProgressStatus::Continue, "continue"),
            (ProgressStatus::Done, "done"),
            (ProgressStatus::Blocked, "blocked"),
            (ProgressStatus::NeedsInput, "needs_input"),
        ];
        for (status, wire) in statuses {
            assert_eq!(serde_json::to_value(status).unwrap(), json!(wire));
        }
    }

    #[test]
    fn lists_may_be_omitted() {
        let report = ProgressReport::from_agent_json(
            &json!({
                "status": "done",
                "summary": "all acceptance criteria met"
            })
            .to_string(),
        )
        .unwrap();
        assert!(report.remaining.is_empty());
        assert!(report.blockers.is_empty());
        assert!(report.questions.is_empty());
    }

    #[test]
    fn unknown_fields_fail_the_agent_parse() {
        let result = ProgressReport::from_agent_json(
            &json!({
                "status": "continue",
                "summary": "…",
                "remaining_work": ["a mistyped key must fail loudly"]
            })
            .to_string(),
        );
        assert!(result.is_err());
    }

    /// The client-side posture: a field this version doesn't know is
    /// skipped, so older clients survive newer daemons.
    #[test]
    fn unknown_fields_are_tolerated_on_the_wire() {
        let report: ProgressReport = serde_json::from_value(json!({
            "status": "continue",
            "summary": "…",
            "invented_by_a_future_daemon": true
        }))
        .unwrap();
        assert_eq!(report.status, ProgressStatus::Continue);
    }

    #[test]
    fn reports_round_trip() {
        let report = ProgressReport {
            status: ProgressStatus::NeedsInput,
            summary: "need a decision on the storage layer".into(),
            remaining: vec!["wire the store".into()],
            blockers: vec![],
            questions: vec![Question {
                id: "q1".into(),
                text: "sqlite or plain files?".into(),
                options: vec!["sqlite".into(), "files".into()],
            }],
        };
        let wire = serde_json::to_string(&report).unwrap();
        assert_eq!(
            serde_json::from_str::<ProgressReport>(&wire).unwrap(),
            report
        );
    }
}
