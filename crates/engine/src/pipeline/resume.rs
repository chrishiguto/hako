//! Pipeline-owned replay of the dialect-free Event Log into the exact
//! stage, typed reports, and human input needed to resume in place.
//! One forward fold reduces the whole history — the same shape as the
//! shared [`RunProjection`](crate::projection::RunProjection), applied
//! to this kernel's dialect.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use proto::event::{EventEnvelope, RunEvent};
use proto::pipeline::{Stage, StageReport};
use proto::report::{Answer, Question};

use crate::preamble::HumanInput;

/// Where a resumed run re-enters its loop. [`from_events`]
/// (Self::from_events) is the only constructor, and it upholds the
/// shape's one invariant: a point that starts a fresh iteration always
/// re-enters at plan with nothing completed.
pub(super) struct ResumePoint {
    pub iteration: u32,
    /// Whether the loop must announce this iteration — a fresh one
    /// opening after a boundary pause. Re-entering the interrupted
    /// iteration announces nothing: its `iteration_started` is
    /// already on the log.
    pub starts_iteration: bool,
    /// The stage the pass re-enters at — plan when starting fresh.
    pub stage: Stage,
    /// The interrupted iteration's reports from before the pause, in
    /// kernel order; empty when starting fresh.
    pub completed: Vec<StageReport>,
    pub human: Option<HumanInput>,
}

impl ResumePoint {
    pub fn from_events(events: &[EventEnvelope]) -> Result<Self, ResumeError> {
        let mut replay = Replay::default();
        let mut resumed = None;
        for envelope in events {
            replay.apply(envelope)?;
            // The point is read at the last resume command; whatever a
            // log carries after it never rewrites what the command saw.
            if matches!(envelope.event, RunEvent::RunResumed { .. }) {
                resumed = Some(replay.clone());
            }
        }
        resumed.ok_or(ResumeError::MissingResume)?.into_point()
    }
}

/// The fold's state: what one pass over the log knows at each event.
/// Reports stay raw here — only the window the final resume actually
/// reads is parsed, so an old iteration's corrupt report cannot fail a
/// resume that never touches it.
#[derive(Default, Clone)]
struct Replay<'a> {
    /// The last announced iteration, and whether an
    /// `iteration_finished` closed it.
    iteration: Option<u32>,
    finished: bool,
    /// The last stage the current iteration started.
    interrupted: Option<Stage>,
    /// The latest report each stage of the current iteration emitted —
    /// a verify retry's report supersedes its predecessor's.
    reports: BTreeMap<Stage, (u64, &'a serde_json::Value)>,
    /// Human input no stage has read yet. An answer or note is
    /// delivered by the first stage that starts after it, so whatever
    /// is still pending at the resume belongs to the stage that
    /// re-runs — across as many back-to-back pauses as it takes.
    answers: BTreeMap<String, String>,
    note: Option<String>,
    /// Whether any pause preceded the resume — a log without one has
    /// nothing to resume from.
    paused: bool,
}

impl<'a> Replay<'a> {
    fn apply(&mut self, envelope: &'a EventEnvelope) -> Result<(), ResumeError> {
        match &envelope.event {
            RunEvent::IterationStarted { iteration } => {
                self.iteration = Some(*iteration);
                self.finished = false;
                self.interrupted = None;
                self.reports.clear();
            }
            RunEvent::IterationFinished { iteration, .. } if self.iteration == Some(*iteration) => {
                self.finished = true;
            }
            RunEvent::StageStarted { iteration, stage } if self.iteration == Some(*iteration) => {
                self.interrupted = Some(stage_named(envelope.seq, stage)?);
                // The stage that just started read every pending
                // answer and note; nothing stays pending behind it.
                self.answers.clear();
                self.note = None;
            }
            RunEvent::StageReported {
                iteration,
                stage,
                report,
            } if self.iteration == Some(*iteration) => {
                self.reports
                    .insert(stage_named(envelope.seq, stage)?, (envelope.seq, report));
            }
            RunEvent::StateChanged {
                state: proto::run::RunState::Paused { .. },
            } => self.paused = true,
            RunEvent::QuestionAnswered {
                question_id,
                answer,
            } => {
                self.answers.insert(question_id.clone(), answer.clone());
            }
            // The latest words win, but a silent resume does not erase
            // an earlier note still awaiting its stage.
            RunEvent::RunResumed {
                note: Some(note), ..
            } => self.note = Some(note.clone()),
            _ => {}
        }
        Ok(())
    }

    fn into_point(self) -> Result<ResumePoint, ResumeError> {
        if !self.paused {
            return Err(ResumeError::MissingPause);
        }
        let Some(iteration) = self.iteration else {
            return Err(ResumeError::MissingIteration);
        };
        if self.finished {
            // The pause closed its pass; the resume opens the next
            // iteration at the top.
            return Ok(ResumePoint {
                iteration: iteration.saturating_add(1),
                starts_iteration: true,
                stage: Stage::Plan,
                completed: Vec::new(),
                human: human_input(self.answers, self.note, Vec::new()),
            });
        }
        // Every mid-pipeline pause lands after its stage started; plan
        // is the can't-happen fallback that keeps the pass whole
        // rather than a panic.
        let interrupted = self.interrupted.unwrap_or(Stage::Plan);
        let mut completed = Vec::new();
        let mut questions = Vec::new();
        for (&stage, &(seq, report)) in &self.reports {
            match stage.cmp(&interrupted) {
                Ordering::Less => completed.push(parse_report(seq, stage, report)?),
                // The interrupted stage's own report is not part of
                // the hand-off — the stage re-runs — but its
                // questions are what the answers attribute to.
                Ordering::Equal => {
                    questions = parse_report(seq, stage, report)?.questions().to_vec();
                }
                // A stage past the interrupted one cannot have
                // reported in a well-formed log; a stray is noise,
                // not hand-off material.
                Ordering::Greater => {}
            }
        }
        Ok(ResumePoint {
            iteration,
            starts_iteration: false,
            stage: interrupted,
            completed,
            human: human_input(self.answers, self.note, questions),
        })
    }
}

/// `None` when the human said nothing — the preamble's contract is
/// that no human input means no section, never an empty one.
fn human_input(
    answers: BTreeMap<String, String>,
    note: Option<String>,
    questions: Vec<Question>,
) -> Option<HumanInput> {
    (!answers.is_empty() || note.is_some()).then(|| HumanInput {
        answers: answers
            .into_iter()
            .map(|(question_id, answer)| Answer {
                question_id,
                answer,
            })
            .collect(),
        questions,
        note,
    })
}

fn parse_report(
    seq: u64,
    stage: Stage,
    report: &serde_json::Value,
) -> Result<StageReport, ResumeError> {
    StageReport::from_stage_json(stage, &report.to_string()).map_err(|error| {
        ResumeError::InvalidReport {
            seq,
            detail: error.to_string(),
        }
    })
}

fn stage_named(seq: u64, name: &str) -> Result<Stage, ResumeError> {
    Stage::from_wire(name).ok_or_else(|| ResumeError::UnknownStage {
        seq,
        name: name.to_owned(),
    })
}

#[derive(Debug, thiserror::Error)]
pub(super) enum ResumeError {
    #[error("replayed log has no resume command")]
    MissingResume,
    #[error("replayed log has no pause before its resume command")]
    MissingPause,
    #[error("replayed log has no iteration before its pause")]
    MissingIteration,
    #[error("stage event at seq {seq} names unknown pipeline stage `{name}`")]
    UnknownStage { seq: u64, name: String },
    #[error("stage report at seq {seq} is invalid: {detail}")]
    InvalidReport { seq: u64, detail: String },
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::run::{PauseReason, RunState};
    use crate::testkit::event_log;

    fn started(iteration: u32, stage: &str) -> RunEvent {
        RunEvent::StageStarted {
            iteration,
            stage: stage.into(),
        }
    }

    fn reported(iteration: u32, stage: &str, report: serde_json::Value) -> RunEvent {
        RunEvent::StageReported {
            iteration,
            stage: stage.into(),
            report,
        }
    }

    fn paused(reason: PauseReason) -> RunEvent {
        RunEvent::StateChanged {
            state: RunState::Paused { reason },
        }
    }

    fn answered(question_id: &str, answer: &str) -> RunEvent {
        RunEvent::QuestionAnswered {
            question_id: question_id.into(),
            answer: answer.into(),
        }
    }

    fn resumed(note: Option<&str>) -> RunEvent {
        RunEvent::RunResumed {
            note: note.map(str::to_owned),
            extend: None,
        }
    }

    /// One iteration interrupted at implement: plan's report done, a
    /// question asked, the human's words on the log.
    fn interrupted_at_implement() -> Vec<RunEvent> {
        vec![
            RunEvent::IterationStarted { iteration: 7 },
            started(7, "plan"),
            reported(
                7,
                "plan",
                json!({"status": "continue", "summary": "planned", "work_unit": "issue #28"}),
            ),
            started(7, "implement"),
            reported(
                7,
                "implement",
                json!({
                    "status": "needs_input",
                    "summary": "need a storage choice",
                    "questions": [{"id": "q1", "text": "sqlite or files?"}]
                }),
            ),
            paused(PauseReason::AwaitingHuman),
        ]
    }

    #[track_caller]
    fn point(events: Vec<RunEvent>) -> ResumePoint {
        ResumePoint::from_events(&event_log(events)).unwrap()
    }

    /// The acceptance shape: answers and the note reach the
    /// interrupted stage, attributed to its questions, with the
    /// completed reports carried in kernel order.
    #[test]
    fn an_interrupted_pause_carries_reports_answers_and_note() {
        let mut events = interrupted_at_implement();
        events.extend([answered("q1", "sqlite"), resumed(Some("keep it narrow"))]);
        let point = point(events);
        assert_eq!(point.iteration, 7);
        assert!(!point.starts_iteration);
        assert_eq!(point.stage, Stage::Implement);
        assert_eq!(point.completed.len(), 1);
        assert_eq!(point.completed[0].stage(), Stage::Plan);
        let human = point.human.unwrap();
        assert_eq!(human.answers.len(), 1);
        assert_eq!(human.answers[0].question_id, "q1");
        assert_eq!(human.questions.len(), 1);
        assert_eq!(human.note.as_deref(), Some("keep it narrow"));
    }

    /// A resume that never reached its stage — an exhausted budget
    /// pausing the run again at the loop top — must not swallow the
    /// answers or the note; they are still pending for the stage that
    /// finally re-runs.
    #[test]
    fn answers_survive_a_pause_that_lands_before_the_stage_reruns() {
        let mut events = interrupted_at_implement();
        events.extend([
            answered("q1", "sqlite"),
            resumed(Some("keep it narrow")),
            paused(PauseReason::Budget),
            resumed(None),
        ]);
        let point = point(events);
        assert_eq!(point.stage, Stage::Implement);
        let human = point.human.unwrap();
        assert_eq!(human.answers[0].answer, "sqlite");
        assert_eq!(human.note.as_deref(), Some("keep it narrow"));
    }

    /// Answers a stage already read are history: only input given
    /// since that stage started rides the next resume.
    #[test]
    fn a_stage_that_ran_consumes_the_answers_behind_it() {
        let mut events = interrupted_at_implement();
        events.extend([
            answered("q1", "sqlite"),
            resumed(None),
            // The interrupted stage re-ran with q1's answer and asked
            // something new.
            started(7, "implement"),
            reported(
                7,
                "implement",
                json!({
                    "status": "needs_input",
                    "summary": "need a branch name",
                    "questions": [{"id": "q2", "text": "branch?"}]
                }),
            ),
            paused(PauseReason::AwaitingHuman),
            answered("q2", "run/7"),
            resumed(None),
        ]);
        let human = point(events).human.unwrap();
        assert_eq!(human.answers.len(), 1);
        assert_eq!(human.answers[0].question_id, "q2");
        assert_eq!(human.questions.len(), 1);
        assert_eq!(human.questions[0].id, "q2");
    }

    /// Re-answering before the resume is a correction: the latest
    /// answer wins and the question rides exactly once.
    #[test]
    fn the_latest_answer_to_a_question_wins() {
        let mut events = interrupted_at_implement();
        events.extend([
            answered("q1", "sqlite"),
            answered("q1", "plain files"),
            resumed(None),
        ]);
        let human = point(events).human.unwrap();
        assert_eq!(human.answers.len(), 1);
        assert_eq!(human.answers[0].answer, "plain files");
    }

    /// A verify retry's report supersedes its predecessor's, so the
    /// hand-off carries each completed stage exactly once, latest
    /// words first-class.
    #[test]
    fn the_latest_report_per_stage_rides_the_handoff() {
        let events = vec![
            RunEvent::IterationStarted { iteration: 1 },
            started(1, "plan"),
            reported(1, "plan", json!({"status": "continue", "summary": "first"})),
            started(1, "plan"),
            reported(
                1,
                "plan",
                json!({"status": "continue", "summary": "second"}),
            ),
            started(1, "implement"),
            paused(PauseReason::VerifyFailed),
            resumed(None),
        ];
        let completed = point(events).completed;
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].summary(), "second");
    }

    /// A pause after `iteration_finished` resumes at the next
    /// iteration's top, nothing completed and nothing interrupted.
    #[test]
    fn a_finished_iteration_resumes_at_the_next_one() {
        let events = vec![
            RunEvent::IterationStarted { iteration: 3 },
            RunEvent::IterationFinished {
                iteration: 3,
                outcome: proto::event::IterationOutcome::Completed,
            },
            paused(PauseReason::Budget),
            resumed(Some("one more")),
        ];
        let point = point(events);
        assert_eq!(point.iteration, 4);
        assert!(point.starts_iteration);
        assert_eq!(point.stage, Stage::Plan);
        assert!(point.completed.is_empty());
        assert_eq!(point.human.unwrap().note.as_deref(), Some("one more"));
    }

    #[test]
    fn a_log_without_a_resume_or_pause_or_iteration_cannot_seed_a_point() {
        let no_resume = event_log(interrupted_at_implement());
        assert!(matches!(
            ResumePoint::from_events(&no_resume),
            Err(ResumeError::MissingResume)
        ));
        let no_pause = event_log(vec![
            RunEvent::IterationStarted { iteration: 1 },
            resumed(None),
        ]);
        assert!(matches!(
            ResumePoint::from_events(&no_pause),
            Err(ResumeError::MissingPause)
        ));
        let no_iteration = event_log(vec![paused(PauseReason::Budget), resumed(None)]);
        assert!(matches!(
            ResumePoint::from_events(&no_iteration),
            Err(ResumeError::MissingIteration)
        ));
    }

    /// The fold fails loudly, naming the offending line: an unknown
    /// stage or an unparsable report in the resumed window is
    /// corruption, not something to guess around.
    #[test]
    fn a_corrupt_window_fails_naming_its_seq() {
        let unknown = event_log(vec![
            RunEvent::IterationStarted { iteration: 1 },
            started(1, "ship"),
            paused(PauseReason::AwaitingHuman),
            resumed(None),
        ]);
        assert!(matches!(
            ResumePoint::from_events(&unknown),
            Err(ResumeError::UnknownStage { seq: 1, .. })
        ));
        let invalid = event_log(vec![
            RunEvent::IterationStarted { iteration: 1 },
            started(1, "plan"),
            reported(1, "plan", json!({"weird": true})),
            started(1, "implement"),
            paused(PauseReason::AwaitingHuman),
            resumed(None),
        ]);
        assert!(matches!(
            ResumePoint::from_events(&invalid),
            Err(ResumeError::InvalidReport { seq: 2, .. })
        ));
    }
}
