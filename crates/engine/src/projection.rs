//! The run projection — the single definition of how a run's Event
//! Log reads back. One pure pass reduces the whole history to where
//! the run stands now: state, counters, the last report's shared
//! core, and the resume cursor. Every consumer — the store's state
//! read, the daemon's status endpoint, a future SSE resume — projects
//! through this fold, so no two can disagree about the same log.
//! Events in, value out: no I/O, testable with fixture logs.
//!
//! The projection is dialect-blind — the engine's shared machinery
//! never imports a kernel's dialect — so it reads a stage report only
//! through [`ReportCore`], the uniform slice every kernel's dialect
//! flattens into. A typed dialect read is the owning kernel's
//! business, done by replaying the raw events itself.

use serde::Deserialize;

use proto::event::{EventEnvelope, RunEvent};
use proto::report::{Question, ReportCore};

use crate::run::{PauseReason, RunState};

/// The state a run is born in — what an empty log projects to, and
/// what the store seeds its state mirror with. One definition, so the
/// two can never disagree about a fresh run.
pub(crate) const INITIAL_STATE: RunState = RunState::Running;

/// Where a run stands, reduced from its full event history.
#[derive(Debug, Clone, PartialEq)]
pub struct RunProjection {
    /// The last `state_changed`, or `running` while none has landed.
    /// A run that projects `running` after a restart simply never got
    /// further — what to do about its dead kernel is the host's call.
    pub state: RunState,
    /// The last event's timestamp; `None` while the log is empty — a
    /// host falls back to the run's `created_at` from its metadata.
    pub updated_at: Option<String>,
    /// How many iterations have finished, whatever their outcome.
    pub iterations_completed: u32,
    /// The shared core of the most recent stage report, whichever
    /// kernel and stage wrote it.
    pub last_report: Option<ReportCore>,
    /// The last event's sequence number — the cursor a resuming
    /// reader continues from; `None` while the log is empty.
    pub last_seq: Option<u64>,
}

impl RunProjection {
    /// Reduces an event history in one pass. Fails only on a stage
    /// report that cannot yield the shared core — corruption, since
    /// every logged report was strict-parsed against its dialect
    /// before it was appended.
    pub fn of(events: &[EventEnvelope]) -> Result<Self, ProjectionError> {
        let mut state = INITIAL_STATE;
        let mut iterations_completed: u32 = 0;
        let mut last_report: Option<(u64, &serde_json::Value)> = None;
        for envelope in events {
            match &envelope.event {
                RunEvent::StateChanged { state: changed } => state = *changed,
                RunEvent::IterationFinished { .. } => {
                    iterations_completed = iterations_completed.saturating_add(1);
                }
                RunEvent::StageReported { report, .. } => {
                    last_report = Some((envelope.seq, report));
                }
                _ => {}
            }
        }
        let last_report = last_report
            .map(|(seq, raw)| {
                ReportCore::deserialize(raw).map_err(|error| ProjectionError {
                    seq,
                    detail: error.to_string(),
                })
            })
            .transpose()?;
        let last = events.last();
        Ok(Self {
            state,
            updated_at: last.map(|envelope| envelope.at.clone()),
            iterations_completed,
            last_report,
            last_seq: last.map(|envelope| envelope.seq),
        })
    }

    /// The questions the run is waiting on right now: the last
    /// report's, while the run is paused awaiting a human. Outside
    /// that pause a report's questions are history, not an ask —
    /// uniform across kernels, so every host surfaces the same
    /// pending questions from the same log.
    pub fn pending_questions(&self) -> &[Question] {
        match (&self.state, &self.last_report) {
            (
                RunState::Paused {
                    reason: PauseReason::AwaitingHuman,
                },
                Some(core),
            ) => &core.questions,
            _ => &[],
        }
    }
}

/// The one way a projection can fail: a logged stage report does not
/// carry the shared report core every dialect promises.
#[derive(Debug, thiserror::Error)]
#[error("stage report at seq {seq} does not carry the report core: {detail}")]
pub struct ProjectionError {
    pub seq: u64,
    pub detail: String,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::run::PauseReason;
    use proto::event::IterationOutcome;
    use proto::report::ReportStatus;

    /// A fixture log: the events enveloped in order, ids and
    /// timestamps laid down the way the file sink would.
    fn log(events: Vec<RunEvent>) -> Vec<EventEnvelope> {
        events
            .into_iter()
            .enumerate()
            .map(|(seq, event)| EventEnvelope {
                seq: seq as u64,
                run_id: "r1".into(),
                at: format!("2026-07-13T09:{:02}:00Z", seq),
                event,
            })
            .collect()
    }

    fn reported(stage: &str, report: serde_json::Value) -> RunEvent {
        RunEvent::StageReported {
            iteration: 1,
            stage: stage.into(),
            report,
        }
    }

    fn finished(iteration: u32) -> RunEvent {
        RunEvent::IterationFinished {
            iteration,
            outcome: IterationOutcome::Completed,
        }
    }

    fn paused(reason: PauseReason) -> RunEvent {
        RunEvent::StateChanged {
            state: RunState::Paused { reason },
        }
    }

    #[test]
    fn an_empty_log_projects_a_fresh_run() {
        let projection = RunProjection::of(&[]).unwrap();
        assert_eq!(
            projection,
            RunProjection {
                state: RunState::Running,
                updated_at: None,
                iterations_completed: 0,
                last_report: None,
                last_seq: None,
            }
        );
    }

    #[test]
    fn a_full_history_projects_in_one_value() {
        let events = log(vec![
            RunEvent::RunStarted {
                kernel: "pipeline".into(),
                agent: "claude".into(),
            },
            RunEvent::IterationStarted { iteration: 1 },
            reported(
                "plan",
                json!({"status": "continue", "summary": "picked issue #7", "steps": ["a", "b"]}),
            ),
            finished(1),
            paused(PauseReason::Drift),
        ]);
        let projection = RunProjection::of(&events).unwrap();
        assert_eq!(
            projection.state,
            RunState::Paused {
                reason: PauseReason::Drift
            }
        );
        assert_eq!(
            projection.updated_at.as_deref(),
            Some("2026-07-13T09:04:00Z")
        );
        assert_eq!(projection.iterations_completed, 1);
        assert_eq!(projection.last_seq, Some(4));
        let core = projection.last_report.unwrap();
        assert_eq!(core.status, ReportStatus::Continue);
        assert_eq!(core.summary, "picked issue #7");
        assert!(core.questions.is_empty());
    }

    #[test]
    fn the_last_state_change_and_the_last_report_win() {
        let events = log(vec![
            paused(PauseReason::Budget),
            reported("plan", json!({"status": "continue", "summary": "first"})),
            finished(1),
            reported("review", json!({"status": "done", "summary": "second"})),
            finished(2),
            RunEvent::StateChanged {
                state: RunState::Done,
            },
        ]);
        let projection = RunProjection::of(&events).unwrap();
        assert_eq!(projection.state, RunState::Done);
        assert_eq!(projection.iterations_completed, 2);
        let core = projection.last_report.unwrap();
        assert_eq!(core.summary, "second");
        assert_eq!(core.status, ReportStatus::Done);
    }

    /// The core keeps the questions and skips the dialect payload the
    /// report carried alongside them.
    #[test]
    fn a_pausing_reports_questions_ride_the_core() {
        let events = log(vec![
            reported(
                "plan",
                json!({
                    "status": "needs_input",
                    "summary": "need a decision",
                    "work_unit": "issue #7",
                    "questions": [{"id": "q1", "text": "which shape?", "options": ["a", "b"]}],
                }),
            ),
            paused(PauseReason::AwaitingHuman),
        ]);
        let core = RunProjection::of(&events).unwrap().last_report.unwrap();
        assert_eq!(core.status, ReportStatus::NeedsInput);
        assert_eq!(core.questions.len(), 1);
        assert_eq!(core.questions[0].id, "q1");
        assert_eq!(core.questions[0].options, ["a", "b"]);
    }

    #[test]
    fn questions_are_pending_only_while_awaiting_a_human() {
        let asked = vec![
            reported(
                "plan",
                json!({
                    "status": "needs_input",
                    "summary": "need a decision",
                    "questions": [{"id": "q1", "text": "which shape?"}],
                }),
            ),
            paused(PauseReason::AwaitingHuman),
        ];
        let waiting = RunProjection::of(&log(asked.clone())).unwrap();
        assert_eq!(waiting.pending_questions().len(), 1);
        assert_eq!(waiting.pending_questions()[0].id, "q1");

        let mut answered = asked;
        answered.push(RunEvent::StateChanged {
            state: RunState::Running,
        });
        let resumed = RunProjection::of(&log(answered)).unwrap();
        assert!(resumed.pending_questions().is_empty());
    }

    #[test]
    fn a_report_without_the_core_is_an_error_naming_its_seq() {
        let events = log(vec![
            RunEvent::IterationStarted { iteration: 1 },
            reported("plan", json!({"weird": true})),
        ]);
        let error = RunProjection::of(&events).unwrap_err();
        assert_eq!(error.seq, 1);
        assert!(error.to_string().contains("seq 1"), "{error}");
    }
}
