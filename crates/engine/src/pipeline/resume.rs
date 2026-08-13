//! Pipeline-owned replay of the dialect-free Event Log into the exact
//! stage, typed reports, and human input needed to resume in place.

use std::collections::BTreeMap;

use proto::event::{EventEnvelope, RunEvent};
use proto::pipeline::{Stage, StageReport};
use proto::report::{Answer, Question};

use crate::preamble::HumanInput;

pub(super) struct ResumePoint {
    pub iteration: u32,
    pub position: Position,
    pub human: Option<HumanInput>,
}

pub(super) enum Position {
    NewIteration,
    Interrupted {
        stage: Stage,
        completed: Vec<StageReport>,
    },
}

impl ResumePoint {
    pub fn from_events(events: &[EventEnvelope]) -> Result<Self, ResumeError> {
        let resumed_at = events
            .iter()
            .rposition(|envelope| matches!(envelope.event, RunEvent::RunResumed { .. }))
            .ok_or(ResumeError::MissingResume)?;
        let paused_at = events[..resumed_at]
            .iter()
            .rposition(|envelope| {
                matches!(
                    envelope.event,
                    RunEvent::StateChanged {
                        state: proto::run::RunState::Paused { .. }
                    }
                )
            })
            .ok_or(ResumeError::MissingPause)?;
        let started_at = events[..paused_at]
            .iter()
            .rposition(|envelope| matches!(envelope.event, RunEvent::IterationStarted { .. }))
            .ok_or(ResumeError::MissingIteration)?;
        let RunEvent::IterationStarted { iteration } = events[started_at].event else {
            unreachable!("the search selected an iteration_started event")
        };

        let iteration_finished = events[started_at + 1..=paused_at].iter().any(|envelope| {
            matches!(
                envelope.event,
                RunEvent::IterationFinished {
                    iteration: finished,
                    ..
                } if finished == iteration
            )
        });
        let (iteration, position, questions) = if iteration_finished {
            (
                iteration.saturating_add(1),
                Position::NewIteration,
                Vec::new(),
            )
        } else {
            let interrupted = last_started_stage(&events[started_at + 1..=paused_at], iteration)?
                .unwrap_or(Stage::Plan);
            let completed =
                completed_reports(&events[started_at + 1..=paused_at], iteration, interrupted)?;
            let questions =
                interrupted_questions(&events[started_at + 1..=paused_at], iteration, interrupted)?;
            (
                iteration,
                Position::Interrupted {
                    stage: interrupted,
                    completed,
                },
                questions,
            )
        };
        let human = human_input(&events[paused_at + 1..=resumed_at], questions);

        Ok(Self {
            iteration,
            position,
            human,
        })
    }
}

fn last_started_stage(
    events: &[EventEnvelope],
    iteration: u32,
) -> Result<Option<Stage>, ResumeError> {
    events
        .iter()
        .rev()
        .find_map(|envelope| match &envelope.event {
            RunEvent::StageStarted {
                iteration: event_iteration,
                stage,
            } if *event_iteration == iteration => Some(stage_named(envelope.seq, stage)),
            _ => None,
        })
        .transpose()
}

fn completed_reports(
    events: &[EventEnvelope],
    iteration: u32,
    interrupted: Stage,
) -> Result<Vec<StageReport>, ResumeError> {
    let interrupted_position = stage_position(interrupted);
    let mut reports = BTreeMap::new();
    for envelope in events {
        let RunEvent::StageReported {
            iteration: event_iteration,
            stage,
            report,
        } = &envelope.event
        else {
            continue;
        };
        if *event_iteration != iteration {
            continue;
        }
        let stage = stage_named(envelope.seq, stage)?;
        if stage_position(stage) >= interrupted_position {
            continue;
        }
        reports.insert(
            stage_position(stage),
            parse_report(envelope.seq, stage, report)?,
        );
    }
    Ok(reports.into_values().collect())
}

fn interrupted_questions(
    events: &[EventEnvelope],
    iteration: u32,
    interrupted: Stage,
) -> Result<Vec<Question>, ResumeError> {
    for envelope in events.iter().rev() {
        let RunEvent::StageReported {
            iteration: event_iteration,
            stage,
            report,
        } = &envelope.event
        else {
            continue;
        };
        if *event_iteration == iteration && stage_named(envelope.seq, stage)? == interrupted {
            return Ok(parse_report(envelope.seq, interrupted, report)?
                .questions()
                .to_vec());
        }
    }
    Ok(Vec::new())
}

fn human_input(events: &[EventEnvelope], questions: Vec<Question>) -> Option<HumanInput> {
    let mut answers = BTreeMap::new();
    let mut note = None;
    for envelope in events {
        match &envelope.event {
            RunEvent::QuestionAnswered {
                question_id,
                answer,
            } => {
                answers.insert(question_id.clone(), answer.clone());
            }
            RunEvent::RunResumed {
                note: resumed_note, ..
            } => note.clone_from(resumed_note),
            _ => {}
        }
    }
    if answers.is_empty() && note.is_none() {
        return None;
    }
    Some(HumanInput {
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
    Stage::ALL
        .into_iter()
        .find(|stage| stage.as_str() == name)
        .ok_or_else(|| ResumeError::UnknownStage {
            seq,
            name: name.to_owned(),
        })
}

fn stage_position(stage: Stage) -> usize {
    Stage::ALL
        .iter()
        .position(|candidate| *candidate == stage)
        .expect("every stage is in Stage::ALL")
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
