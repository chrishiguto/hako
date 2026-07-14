//! The typed event stream — what SSE delivers, one envelope per event.

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use proto::RunEvent;

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

#[cfg(test)]
mod tests {
    use proto::RunState;
    use serde_json::json;

    use super::*;

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
