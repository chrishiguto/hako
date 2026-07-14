//! The progress report — the only structured channel from agent to
//! engine, written by the agent at the end of every iteration.

use serde::{Deserialize, Serialize};

/// What the agent declares when an iteration ends.
///
/// Unknown fields are rejected so that a malformed report fails loudly
/// and feeds the one repair re-prompt instead of being half-understood.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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

/// The agent's claim about where the loop stands. `done` is a claim,
/// not a verdict — the engine accepts it only after verify checks pass
/// and a skeptic iteration fails to refute it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProgressStatus {
    Continue,
    Done,
    Blocked,
    NeedsInput,
}

/// A question the agent needs a human to answer before it can proceed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Question {
    /// The handle an answer is addressed to (`hako answer <run> <id>`).
    pub id: String,
    pub text: String,
    /// Suggested answers; free-text is always allowed.
    #[serde(default)]
    pub options: Vec<String>,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    /// The exact shape locked in the v1 design session — if this test
    /// breaks, the agent-facing contract broke.
    #[test]
    fn the_locked_shape_deserializes() {
        let report: ProgressReport = serde_json::from_value(json!({
            "status": "continue",
            "summary": "what happened this iteration",
            "remaining": ["..."],
            "blockers": ["..."],
            "questions": [{ "id": "q1", "text": "...", "options": ["..."] }]
        }))
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
        let report: ProgressReport = serde_json::from_value(json!({
            "status": "done",
            "summary": "all acceptance criteria met"
        }))
        .unwrap();
        assert!(report.remaining.is_empty());
        assert!(report.blockers.is_empty());
        assert!(report.questions.is_empty());
    }

    #[test]
    fn unknown_fields_are_rejected() {
        let result = serde_json::from_value::<ProgressReport>(json!({
            "status": "continue",
            "summary": "…",
            "remaining_work": ["a mistyped key must fail loudly"]
        }));
        assert!(result.is_err());
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
