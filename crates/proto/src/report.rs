//! The shared report vocabulary — the only structured channel from
//! agent to engine, written to end every invocation. The status
//! semantics, the questions a paused run asks, and the answers a
//! human sends back are uniform across kernels, so HITL behaves the
//! same whatever loop is running. Each kernel's own report shapes
//! build on this vocabulary in its dialect module — the pipeline's in
//! [`crate::pipeline`] — never redefining it (ADR 0010).
//!
//! The whole vocabulary deserializes strictly — questions and answers
//! included, wherever they travel. The agent boundary demands it (a
//! mistyped key must fail the report and feed the repair re-prompt,
//! and the schemas must promise exactly what serde enforces), and
//! both ends of the wire ship from this workspace in lockstep, so
//! leniency would only let a contract drift land silently.

use serde::{Deserialize, Serialize};

/// The agent's claim about where the loop stands. `done` is a claim,
/// not a verdict — the engine accepts it only after verify checks pass
/// and a skeptic iteration fails to refute it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum ReportStatus {
    Continue,
    Done,
    Blocked,
    NeedsInput,
}

/// A question the agent needs a human to answer before it can proceed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct Question {
    /// The handle an answer is addressed to (`hako answer <run> <id>`).
    pub id: String,
    pub text: String,
    /// Suggested answers; free-text is always allowed.
    #[serde(default)]
    pub options: Vec<String>,
}

/// A human's answer to one question a paused run asked, addressed by
/// the question's id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct Answer {
    /// Matches a [`Question::id`] from the run's last report.
    pub question_id: String,
    pub answer: String,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn every_status_matches_its_wire_string() {
        let statuses = [
            (ReportStatus::Continue, "continue"),
            (ReportStatus::Done, "done"),
            (ReportStatus::Blocked, "blocked"),
            (ReportStatus::NeedsInput, "needs_input"),
        ];
        for (status, wire) in statuses {
            assert_eq!(serde_json::to_value(status).unwrap(), json!(wire));
        }
    }

    #[test]
    fn questions_and_answers_round_trip() {
        let question = Question {
            id: "q1".into(),
            text: "sqlite or plain files?".into(),
            options: vec!["sqlite".into(), "files".into()],
        };
        let answer = Answer {
            question_id: "q1".into(),
            answer: "sqlite".into(),
        };
        let wire = serde_json::to_string(&question).unwrap();
        assert_eq!(serde_json::from_str::<Question>(&wire).unwrap(), question);
        let wire = serde_json::to_string(&answer).unwrap();
        assert_eq!(serde_json::from_str::<Answer>(&wire).unwrap(), answer);
    }

    #[test]
    fn question_options_may_be_omitted() {
        let question: Question =
            serde_json::from_value(json!({"id": "q1", "text": "which way?"})).unwrap();
        assert!(question.options.is_empty());
    }
}
