//! The typed event stream — what SSE delivers, one envelope per event.

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::progress::ProgressReport;
use crate::run::RunState;

/// One event as it crosses the wire. `seq` doubles as the SSE event
/// id, so a client reconnecting with `Last-Event-ID` resumes exactly
/// where it dropped — no losses, no duplicates.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct EventEnvelope {
    /// Position in the run's append-only event log, starting at 0.
    pub seq: u64,
    pub run_id: String,
    /// RFC 3339 UTC timestamp of when the event was recorded.
    pub at: String,
    #[serde(flatten)]
    pub event: RunEvent,
}

/// One thing a run did. Mirrors the engine's event vocabulary without
/// depending on it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
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
    /// A chunk of the agent's live output, verbatim.
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
    /// The workspace was git-checkpointed; `commit` is inspectable in
    /// the run's branch.
    WorkspaceCheckpointed {
        iteration: u32,
        commit: String,
    },
    ProgressReported {
        iteration: u32,
        report: ProgressReport,
    },
    /// The agent's report failed schema validation.
    ProgressRejected {
        iteration: u32,
        errors: Vec<String>,
    },
    /// The outcome of a skeptic iteration judging a done claim.
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
    /// A human resumed a paused run.
    RunResumed {
        note: Option<String>,
    },
}

/// How one iteration ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum IterationOutcome {
    Completed,
    Failed,
    TimedOut,
}

/// Which of the two output pipes a chunk came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum OutputStream {
    Stdout,
    Stderr,
}

/// Tokens one agent invocation consumed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct TokenUsage {
    pub input: u64,
    pub output: u64,
}

/// Which budget ran out.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum BudgetKind {
    Iterations,
    WallClock,
    Tokens,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::run::PauseReason;

    /// The envelope flattens the event so a streamed line reads as one
    /// flat object — and stays a verbatim copy of the daemon's log.
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

    /// Same wire shape the engine's event tests lock — the two crates
    /// never link, so the JSON literals are the shared truth.
    #[test]
    fn state_changes_serialize_exactly_like_the_engines() {
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
}
