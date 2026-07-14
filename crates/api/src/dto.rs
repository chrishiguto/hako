//! Request and response bodies for the daemon's REST commands.

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use proto::progress::Question;
use proto::run::RunState;

/// Submit a flow for execution as a new run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct SubmitRunRequest {
    /// The flow file, verbatim TOML. Sent as text so the daemon is the
    /// single validator; clients may pre-validate against the
    /// published JSON Schema but never re-encode.
    pub flow: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct SubmitRunResponse {
    pub run_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ListRunsResponse {
    pub runs: Vec<RunSummary>,
}

/// One line of `hako list`: enough to see what a run is and where it
/// stands, nothing more.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct RunSummary {
    pub run_id: String,
    /// Flattened: `"state": "paused", "reason": "budget"` sit directly
    /// on the summary object.
    #[serde(flatten)]
    pub state: RunState,
    pub goal: String,
    pub kernel: String,
    pub agent: String,
    /// RFC 3339 UTC timestamp.
    pub created_at: String,
    /// RFC 3339 UTC timestamp of the last event.
    pub updated_at: String,
}

/// The full picture of one run, also returned by every command that
/// changes a run so clients see the effect without a second request.
/// Extends the list line structurally — on the wire it reads as one
/// flat object, a summary with depth.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct RunStatusResponse {
    #[serde(flatten)]
    pub run: RunSummary,
    pub iterations_completed: u32,
    /// The agent's most recent progress summary.
    pub last_summary: Option<String>,
    /// Open questions when paused `awaiting_human`; empty otherwise.
    #[serde(default)]
    pub pending_questions: Vec<Question>,
}

/// Answer one or more of a paused run's questions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct AnswerRequest {
    pub answers: Vec<Answer>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct Answer {
    /// Matches a `Question::id` from the run's progress report.
    pub question_id: String,
    pub answer: String,
}

/// Resume a paused run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ResumeRequest {
    /// Guidance injected into the next iteration's prompt preamble.
    pub note: Option<String>,
    /// New caps for a run paused on `budget`; absent fields keep their
    /// current cap.
    pub extend: Option<BudgetExtension>,
}

/// Integer seconds rather than fractional hours: a float on a frozen
/// wire admits negatives and precision junk that every consumer would
/// have to police forever.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct BudgetExtension {
    pub max_iterations: Option<u32>,
    pub max_wall_clock_seconds: Option<u64>,
    pub max_tokens: Option<u64>,
}

/// Every non-2xx response carries this body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ApiError {
    /// Machine-readable and stable, e.g. `run_not_found`,
    /// `secret_missing`, `not_paused`.
    pub code: String,
    /// Human-readable; never parse this.
    pub message: String,
}

#[cfg(test)]
mod tests {
    use proto::run::PauseReason;
    use serde::de::DeserializeOwned;
    use serde_json::json;

    use super::*;

    fn round_trips<T>(value: &T)
    where
        T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug,
    {
        let wire = serde_json::to_string(value).unwrap();
        assert_eq!(&serde_json::from_str::<T>(&wire).unwrap(), value);
    }

    #[test]
    fn a_paused_summary_reads_flat() {
        let summary = RunSummary {
            run_id: "r1".into(),
            state: RunState::Paused {
                reason: PauseReason::Budget,
            },
            goal: "implement all open issues".into(),
            kernel: "ralph".into(),
            agent: "claude".into(),
            created_at: "2026-07-13T08:00:00Z".into(),
            updated_at: "2026-07-13T09:30:00Z".into(),
        };
        assert_eq!(
            serde_json::to_value(&summary).unwrap(),
            json!({
                "run_id": "r1",
                "state": "paused",
                "reason": "budget",
                "goal": "implement all open issues",
                "kernel": "ralph",
                "agent": "claude",
                "created_at": "2026-07-13T08:00:00Z",
                "updated_at": "2026-07-13T09:30:00Z"
            })
        );
    }

    #[test]
    fn commands_round_trip() {
        let submit = SubmitRunRequest {
            flow: "[loop]\nkernel = \"ralph\"\n".into(),
        };
        let answer = AnswerRequest {
            answers: vec![Answer {
                question_id: "q1".into(),
                answer: "plain files".into(),
            }],
        };
        let resume = ResumeRequest {
            note: Some("skip the flaky suite".into()),
            extend: Some(BudgetExtension {
                max_iterations: Some(30),
                max_wall_clock_seconds: None,
                max_tokens: None,
            }),
        };
        round_trips(&submit);
        round_trips(&answer);
        round_trips(&resume);
    }

    /// The nested `RunSummary` must be invisible on the wire — a
    /// status is a flat object, a summary with depth.
    #[test]
    fn a_status_reads_flat_like_a_summary() {
        let status = RunStatusResponse {
            run: RunSummary {
                run_id: "r9".into(),
                state: RunState::Running,
                goal: "port the parser".into(),
                kernel: "ralph".into(),
                agent: "codex".into(),
                created_at: "2026-07-13T08:00:00Z".into(),
                updated_at: "2026-07-13T09:30:00Z".into(),
            },
            iterations_completed: 4,
            last_summary: Some("parser ported, printer next".into()),
            pending_questions: vec![],
        };
        assert_eq!(
            serde_json::to_value(&status).unwrap(),
            json!({
                "run_id": "r9",
                "state": "running",
                "goal": "port the parser",
                "kernel": "ralph",
                "agent": "codex",
                "created_at": "2026-07-13T08:00:00Z",
                "updated_at": "2026-07-13T09:30:00Z",
                "iterations_completed": 4,
                "last_summary": "parser ported, printer next",
                "pending_questions": []
            })
        );
        round_trips(&status);
    }
}
