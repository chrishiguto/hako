//! Run identity and the run state machine. The states themselves are
//! wire vocabulary and live in `proto`; the identity and the kernel's
//! view of an ending are engine-only.

use serde::{Deserialize, Serialize};

use crate::progress::ProgressReport;

pub use proto::run::{PauseReason, RunState};

/// Names one run for its whole life — directory name, API path segment,
/// event-log subject.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RunId(String);

impl RunId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for RunId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// A human's answer to one question a paused run asked, addressed by
/// the question's id (`hako answer <run> <id>`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Answer {
    pub question_id: String,
    pub answer: String,
}

/// What a resume call carries back into a paused run: where the loop
/// stood and what the human said. The host derives it from the run's
/// records — the kernel itself keeps nothing between `run` calls.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resume {
    /// The last iteration that finished before the pause; the resumed
    /// loop continues after it.
    pub iteration: u32,
    /// The report that iteration filed — the summary and remaining
    /// list the next preamble carries, and the questions the answers
    /// are attributed to.
    pub report: ProgressReport,
    pub answers: Vec<Answer>,
    /// The free-form note from `hako resume --note`.
    pub note: Option<String>,
}

/// How a kernel invocation ended — every run state except `Running`,
/// which would mean the kernel returned while still owing work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunOutcome {
    Done,
    Failed,
    Paused(PauseReason),
    Cancelled,
}

/// The wire state an outcome lands the run in — what a kernel's final
/// `state_changed` event carries.
impl From<RunOutcome> for RunState {
    fn from(outcome: RunOutcome) -> Self {
        match outcome {
            RunOutcome::Done => RunState::Done,
            RunOutcome::Failed => RunState::Failed,
            RunOutcome::Paused(reason) => RunState::Paused { reason },
            RunOutcome::Cancelled => RunState::Cancelled,
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn run_id_serializes_as_a_bare_string() {
        let id = RunId::new("run-7");
        assert_eq!(serde_json::to_value(&id).unwrap(), json!("run-7"));
    }

    #[test]
    fn every_outcome_lands_in_the_matching_state() {
        let outcomes = [
            (RunOutcome::Done, RunState::Done),
            (RunOutcome::Failed, RunState::Failed),
            (
                RunOutcome::Paused(PauseReason::Budget),
                RunState::Paused {
                    reason: PauseReason::Budget,
                },
            ),
            (RunOutcome::Cancelled, RunState::Cancelled),
        ];
        for (outcome, state) in outcomes {
            assert_eq!(RunState::from(outcome), state);
        }
    }
}
