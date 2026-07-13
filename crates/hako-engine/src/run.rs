//! Run identity and the run state machine.

use serde::{Deserialize, Serialize};

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

/// Where a run stands: `running → paused(reason) | done | failed |
/// cancelled`. Paused is the only state a run can leave again.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum RunState {
    Running,
    Paused { reason: PauseReason },
    Done,
    Failed,
    Cancelled,
}

/// Why a run paused. Every pause is resumable and notifies the user —
/// pausing exists so an unattended loop asks instead of guessing or
/// burning budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PauseReason {
    /// The agent reported it cannot make progress.
    Blocked,
    /// Verify checks failed and the configured retries are exhausted.
    VerifyFailed,
    /// Consecutive iterations produced no commits and an unchanged
    /// remaining list — the loop is spinning, not progressing.
    Drift,
    /// A budget ran out; the current iteration was finished first.
    Budget,
    /// The agent asked structured questions a human must answer.
    AwaitingHuman,
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
    fn running_state_carries_no_reason() {
        assert_eq!(
            serde_json::to_value(RunState::Running).unwrap(),
            json!({"state": "running"})
        );
    }

    #[test]
    fn paused_state_carries_its_reason() {
        let paused = RunState::Paused {
            reason: PauseReason::AwaitingHuman,
        };
        assert_eq!(
            serde_json::to_value(paused).unwrap(),
            json!({"state": "paused", "reason": "awaiting_human"})
        );
    }

    #[test]
    fn every_pause_reason_is_snake_case_on_the_wire() {
        let reasons = [
            (PauseReason::Blocked, "blocked"),
            (PauseReason::VerifyFailed, "verify_failed"),
            (PauseReason::Drift, "drift"),
            (PauseReason::Budget, "budget"),
            (PauseReason::AwaitingHuman, "awaiting_human"),
        ];
        for (reason, wire) in reasons {
            assert_eq!(serde_json::to_value(reason).unwrap(), json!(wire));
        }
    }

    #[test]
    fn states_round_trip() {
        for state in [
            RunState::Running,
            RunState::Paused {
                reason: PauseReason::Drift,
            },
            RunState::Done,
            RunState::Failed,
            RunState::Cancelled,
        ] {
            let wire = serde_json::to_string(&state).unwrap();
            assert_eq!(serde_json::from_str::<RunState>(&wire).unwrap(), state);
        }
    }
}
