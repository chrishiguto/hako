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
        /// What a failing check printed, tail-capped like the preamble
        /// feedback — the log must be able to say why a run stopped
        /// verifying, because the sandbox that could reproduce it is
        /// already gone. Empty for a passing check: there, `passed` is
        /// the whole story.
        #[serde(default)]
        output: String,
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

/// One event as it lands in the run's JSONL log and crosses the wire —
/// the same shape, because the daemon streams log lines verbatim.
/// `seq` doubles as the SSE event id, so a client reconnecting with
/// `Last-Event-ID` resumes exactly where it dropped — no losses, no
/// duplicates.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct EventEnvelope {
    /// Position in the run's append-only event log, starting at 0.
    pub seq: u64,
    pub run_id: String,
    /// RFC 3339 UTC timestamp of when the event was recorded.
    pub at: String,
    #[serde(flatten)]
    pub event: RunEvent,
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

    /// The envelope flattens the event so a logged line reads as one
    /// flat object — and stays a verbatim copy of what SSE delivers.
    #[test]
    fn the_envelope_is_flat_on_the_wire() {
        let envelope = EventEnvelope {
            seq: 42,
            run_id: "r1".into(),
            at: "2026-07-13T09:00:00Z".into(),
            event: RunEvent::IterationStarted { iteration: 5 },
        };
        assert_eq!(
            serde_json::to_value(&envelope).unwrap(),
            json!({
                "seq": 42,
                "run_id": "r1",
                "at": "2026-07-13T09:00:00Z",
                "type": "iteration_started",
                "iteration": 5
            })
        );
    }

    #[test]
    fn envelopes_round_trip() {
        let envelope = EventEnvelope {
            seq: 7,
            run_id: "r2".into(),
            at: "2026-07-13T09:00:00Z".into(),
            event: RunEvent::StateChanged {
                state: RunState::Done,
            },
        };
        let wire = serde_json::to_string(&envelope).unwrap();
        assert_eq!(
            serde_json::from_str::<EventEnvelope>(&wire).unwrap(),
            envelope
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
