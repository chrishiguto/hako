//! The preamble — the engine-composed frame wrapped around the domain
//! prompt every iteration. The frame carries the loop mechanics
//! (position, last progress, human input, the report contract) so
//! editing the domain prompt can never break the loop's contract with
//! the agent. The objective itself is the domain prompt's alone.

use std::fmt::Write;

use crate::progress::ProgressReport;
use crate::run::Answer;
use crate::workspace::PROGRESS_FILE;

/// Quoted verbatim so the shape the agent copies is exactly the shape
/// the strict parse accepts.
const REPORT_SHAPE: &str = r#"{
  "status": "continue | done | blocked | needs_input",
  "summary": "what happened this iteration",
  "remaining": ["work you believe is still open"],
  "blockers": ["what stops you, when blocked"],
  "questions": [{ "id": "q1", "text": "what a human must decide, when needs_input", "options": ["..."] }]
}"#;

/// Everything the engine frames one iteration's prompt with. One
/// field per concern, so the next input (the skeptic's findings, per
/// ADR 0007) adds a field instead of another positional argument.
pub(crate) struct Preamble<'a> {
    pub iteration: u32,
    pub max_iterations: Option<u32>,
    pub previous: Option<&'a ProgressReport>,
    /// The human's answers on resume, attributed to the previous
    /// report's questions — where a paused run's questions live by
    /// construction.
    pub answers: &'a [Answer],
    /// The free-form resume note.
    pub note: Option<&'a str>,
}

/// Composes the full prompt for one iteration: preamble first, domain
/// prompt verbatim at the end.
pub(crate) fn compose(frame: &Preamble, domain_prompt: &str) -> String {
    let mut text = String::new();
    let _ = write!(
        text,
        "# hako iteration\n\n\
         You are one iteration of an unattended loop; your objective is the \
         domain prompt at the end of this message.\n\n\
         This is iteration {}. Your context is fresh: nothing survives between \
         iterations except the workspace you are in, so read it before acting \
         and leave it consistent when you stop.\n",
        counter(frame.iteration, frame.max_iterations),
    );
    if let Some(report) = frame.previous {
        let _ = write!(text, "\n## Previous iteration\n\n{}\n", report.summary);
        if !report.remaining.is_empty() {
            let _ = write!(text, "\nRemaining work:\n");
            for item in &report.remaining {
                let _ = writeln!(text, "- {item}");
            }
        }
    }
    write_human_input(&mut text, frame);
    let _ = write!(
        text,
        "\n## Progress report\n\n\
         End this iteration by writing `{PROGRESS_FILE}` inside the workspace:\n\n\
         ```json\n{REPORT_SHAPE}\n```\n\n\
         Report `continue` to hand the remaining work to the next iteration, \
         `done` only when the domain prompt is fully satisfied, `blocked` when \
         you cannot proceed, `needs_input` when a human must decide something \
         first.\n\n\
         ---\n\n\
         {domain_prompt}",
    );
    text
}

/// The repair re-prompt — the one second chance a rejected report
/// earns (ADR 0007). Deliberately bare: the iteration's work is done
/// and stays done; only the report needs writing, so this carries the
/// validation errors and the contract and nothing else.
pub(crate) fn repair(errors: &[String]) -> String {
    let mut text = String::from(
        "# hako progress report repair\n\n\
         The progress report you wrote this iteration was rejected:\n\n",
    );
    for error in errors {
        let _ = writeln!(text, "- {error}");
    }
    let _ = write!(
        text,
        "\nWrite a corrected `{PROGRESS_FILE}` in the workspace and do \
         nothing else:\n\n\
         ```json\n{REPORT_SHAPE}\n```\n",
    );
    text
}

fn counter(iteration: u32, max_iterations: Option<u32>) -> String {
    match max_iterations {
        Some(max) => format!("{iteration} of {max}"),
        None => iteration.to_string(),
    }
}

/// How a human's words become loop memory: answers attributed to the
/// questions they were addressed to, then the resume note. The agent
/// reads them here once — its own summary carries them forward.
fn write_human_input(text: &mut String, frame: &Preamble) {
    if frame.answers.is_empty() && frame.note.is_none() {
        return;
    }
    let _ = write!(
        text,
        "\n## Human input\n\n\
         The run paused and a human responded; treat their words as \
         authoritative.\n",
    );
    for answer in frame.answers {
        let question = frame
            .previous
            .and_then(|report| {
                report
                    .questions
                    .iter()
                    .find(|question| question.id == answer.question_id)
            })
            .map_or(answer.question_id.as_str(), |question| &question.text);
        let _ = write!(text, "\n- Q: {question}\n  A: {}\n", answer.answer);
    }
    if let Some(note) = frame.note {
        let _ = write!(text, "\nNote: {note}\n");
    }
}

#[cfg(test)]
mod tests {
    use crate::progress::{ProgressStatus, Question};

    use super::*;

    fn report(summary: &str, remaining: &[&str]) -> ProgressReport {
        ProgressReport {
            status: ProgressStatus::Continue,
            summary: summary.into(),
            remaining: remaining.iter().map(|item| item.to_string()).collect(),
            blockers: vec![],
            questions: vec![],
        }
    }

    /// A report that paused the run asking the given questions.
    fn questioned(questions: &[(&str, &str)]) -> ProgressReport {
        let mut report = report("paused for a decision", &[]);
        report.status = ProgressStatus::NeedsInput;
        report.questions = questions
            .iter()
            .map(|(id, text)| Question {
                id: (*id).into(),
                text: (*text).into(),
                options: vec![],
            })
            .collect();
        report
    }

    fn answers(answers: &[(&str, &str)]) -> Vec<Answer> {
        answers
            .iter()
            .map(|(question_id, answer)| Answer {
                question_id: (*question_id).into(),
                answer: (*answer).into(),
            })
            .collect()
    }

    fn bare(iteration: u32, max_iterations: Option<u32>) -> Preamble<'static> {
        Preamble {
            iteration,
            max_iterations,
            previous: None,
            answers: &[],
            note: None,
        }
    }

    #[test]
    fn a_bounded_run_counts_against_the_ceiling() {
        let text = compose(&bare(3, Some(20)), "domain");
        assert!(text.contains("iteration 3 of 20"), "{text}");
    }

    #[test]
    fn an_unbounded_run_counts_without_a_ceiling() {
        let text = compose(&bare(3, None), "domain");
        assert!(text.contains("This is iteration 3."), "{text}");
    }

    #[test]
    fn the_first_iteration_carries_no_history() {
        let text = compose(&bare(1, None), "domain");
        assert!(!text.contains("Previous iteration"), "{text}");
    }

    #[test]
    fn later_iterations_carry_the_last_summary_and_remaining_list() {
        let last = report("wired the store", &["docs", "tests"]);
        let frame = Preamble {
            previous: Some(&last),
            ..bare(2, None)
        };
        let text = compose(&frame, "domain");
        assert!(text.contains("wired the store"), "{text}");
        assert!(text.contains("- docs\n- tests\n"), "{text}");
    }

    #[test]
    fn an_empty_remaining_list_is_omitted() {
        let last = report("did things", &[]);
        let frame = Preamble {
            previous: Some(&last),
            ..bare(2, None)
        };
        let text = compose(&frame, "domain");
        assert!(!text.contains("Remaining work"), "{text}");
    }

    #[test]
    fn the_contract_names_the_file_and_every_status() {
        let text = compose(&bare(1, None), "domain");
        assert!(text.contains(PROGRESS_FILE), "{text}");
        for status in ["continue", "done", "blocked", "needs_input"] {
            assert!(text.contains(status), "missing {status}: {text}");
        }
    }

    #[test]
    fn the_quoted_shape_is_one_the_strict_parse_accepts() {
        let example =
            REPORT_SHAPE.replace("continue | done | blocked | needs_input", "needs_input");
        assert!(ProgressReport::from_agent_json(&example).is_ok());
    }

    #[test]
    fn answers_are_attributed_to_their_questions() {
        let last = questioned(&[("q1", "sqlite or plain files?"), ("q2", "branch name?")]);
        let answers = answers(&[("q1", "sqlite"), ("q2", "run/1")]);
        let frame = Preamble {
            previous: Some(&last),
            answers: &answers,
            ..bare(2, None)
        };
        let text = compose(&frame, "domain");
        assert!(
            text.contains("- Q: sqlite or plain files?\n  A: sqlite\n"),
            "{text}"
        );
        assert!(text.contains("- Q: branch name?\n  A: run/1\n"), "{text}");
    }

    #[test]
    fn an_answer_to_an_unknown_question_keeps_its_id_as_the_handle() {
        let answers = answers(&[("q9", "yes")]);
        let frame = Preamble {
            answers: &answers,
            ..bare(2, None)
        };
        let text = compose(&frame, "domain");
        assert!(text.contains("- Q: q9\n  A: yes\n"), "{text}");
    }

    #[test]
    fn a_note_alone_still_forms_the_human_input_section() {
        let frame = Preamble {
            note: Some("go with the simplest thing"),
            ..bare(2, None)
        };
        let text = compose(&frame, "domain");
        assert!(text.contains("## Human input"), "{text}");
        assert!(text.contains("Note: go with the simplest thing"), "{text}");
    }

    #[test]
    fn a_resume_with_nothing_to_say_adds_no_section() {
        let last = questioned(&[("q1", "sqlite or plain files?")]);
        let frame = Preamble {
            previous: Some(&last),
            ..bare(2, None)
        };
        let text = compose(&frame, "domain");
        assert!(!text.contains("## Human input"), "{text}");
    }

    #[test]
    fn human_input_sits_between_history_and_the_contract() {
        let last = report("waiting on the human", &[]);
        let frame = Preamble {
            previous: Some(&last),
            note: Some("carry on"),
            ..bare(2, None)
        };
        let text = compose(&frame, "domain");
        let history = text.find("## Previous iteration").unwrap();
        let human = text.find("## Human input").unwrap();
        let contract = text.find("## Progress report").unwrap();
        assert!(history < human && human < contract, "{text}");
    }

    #[test]
    fn the_repair_prompt_carries_the_errors_and_the_contract() {
        let text = repair(&["missing field `summary`".into(), "not UTF-8".into()]);
        assert!(text.contains("- missing field `summary`\n"), "{text}");
        assert!(text.contains("- not UTF-8\n"), "{text}");
        assert!(text.contains(PROGRESS_FILE), "{text}");
        assert!(text.contains(REPORT_SHAPE), "{text}");
    }

    #[test]
    fn the_domain_prompt_closes_the_prompt_verbatim() {
        let text = compose(&bare(1, None), "## My rules\n\nkeep tests green\n");
        assert!(
            text.ends_with("## My rules\n\nkeep tests green\n"),
            "{text}"
        );
        let frame = text.find("hako iteration").unwrap();
        let domain = text.find("## My rules").unwrap();
        assert!(frame < domain);
    }
}
