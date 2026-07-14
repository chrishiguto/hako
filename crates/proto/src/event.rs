//! The event vocabulary — everything a run appends to its event log,
//! and therefore everything a client can see.

use serde::{Deserialize, Serialize};

use crate::budget::{BudgetKind, TokenUsage};
use crate::progress::ProgressReport;
use crate::run::RunState;

/// One thing a run did. The append-only sequence of these is the run's
/// source of truth: state, audit trail, and everything a client sees.
///
/// Events carry no run id or sequence number — a sink serves exactly
/// one run and owns the envelope (ordering, timestamps) it writes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RunEvent {
    RunStarted {
        goal: String,
        kernel: String,
        agent: String,
    },
    StateChanged {
        #[serde(flatten)]
        state: RunState,
    },
    IterationStarted {
        iteration: u32,
    },
    IterationFinished {
        iteration: u32,
        outcome: IterationOutcome,
    },
    /// A chunk of the agent's live output, replayed verbatim to
    /// attached clients.
    AgentOutput {
        iteration: u32,
        stream: OutputStream,
        chunk: String,
    },
    VerifyCheckFinished {
        iteration: u32,
        command: String,
        passed: bool,
    },
    /// The workspace was git-checkpointed, so the loop's work can be
    /// inspected, bisected, or rolled back iteration by iteration.
    WorkspaceCheckpointed {
        iteration: u32,
        commit: String,
    },
    ProgressReported {
        iteration: u32,
        report: ProgressReport,
    },
    /// The agent's report failed schema validation; the errors are
    /// what the repair re-prompt carries back.
    ProgressRejected {
        iteration: u32,
        errors: Vec<String>,
    },
    /// The outcome of a skeptic iteration judging a done claim. A
    /// refutation's findings feed the next iteration's preamble.
    SkepticVerdict {
        iteration: u32,
        refuted: bool,
        findings: Vec<String>,
    },
    TokensUsed {
        iteration: u32,
        usage: TokenUsage,
    },
    BudgetExhausted {
        budget: BudgetKind,
    },
    QuestionAnswered {
        question_id: String,
        answer: String,
    },
    /// A human resumed a paused run; the note becomes part of the next
    /// iteration's preamble.
    RunResumed {
        note: Option<String>,
    },
}

/// How one iteration ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum IterationOutcome {
    Completed,
    Failed,
    TimedOut,
}

/// Which of the two output pipes a chunk came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum OutputStream {
    Stdout,
    Stderr,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::run::PauseReason;

    #[test]
    fn events_are_tagged_by_type() {
        let event = RunEvent::IterationStarted { iteration: 3 };
        assert_eq!(
            serde_json::to_value(&event).unwrap(),
            json!({"type": "iteration_started", "iteration": 3})
        );
    }

    /// The run state flattens into the event so the wire shape stays
    /// one level deep: `{"type": "state_changed", "state": "paused",
    /// "reason": "drift"}`.
    #[test]
    fn state_changes_flatten_the_state() {
        let event = RunEvent::StateChanged {
            state: RunState::Paused {
                reason: PauseReason::Drift,
            },
        };
        assert_eq!(
            serde_json::to_value(&event).unwrap(),
            json!({"type": "state_changed", "state": "paused", "reason": "drift"})
        );
    }
}
