//! Run states as clients see them. Mirrors the engine's state machine
//! without depending on it — the wire contract must stay consumable by
//! clients that will never link the engine.

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Where a run stands: `running → paused(reason) | done | failed |
/// cancelled`. Paused is the only state a run can leave again.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum RunState {
    Running,
    Paused { reason: PauseReason },
    Done,
    Failed,
    Cancelled,
}

/// Why a run paused. Every pause is resumable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum PauseReason {
    /// The agent reported it cannot make progress.
    Blocked,
    /// Verify checks failed and the configured retries are exhausted.
    VerifyFailed,
    /// Consecutive iterations produced no commits and an unchanged
    /// remaining list.
    Drift,
    /// A budget ran out; the current iteration was finished first.
    Budget,
    /// The agent asked structured questions a human must answer.
    AwaitingHuman,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    /// The wire shape must match the engine's serialization of the
    /// same states byte for byte — the daemon streams its event log,
    /// it does not translate it.
    #[test]
    fn states_serialize_exactly_like_the_engines() {
        assert_eq!(
            serde_json::to_value(RunState::Running).unwrap(),
            json!({"state": "running"})
        );
        assert_eq!(
            serde_json::to_value(RunState::Paused {
                reason: PauseReason::VerifyFailed
            })
            .unwrap(),
            json!({"state": "paused", "reason": "verify_failed"})
        );
    }
}
