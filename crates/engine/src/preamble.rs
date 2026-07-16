//! The preamble — the engine-composed frame wrapped around the domain
//! prompt every iteration. The frame carries the loop mechanics
//! (position, last progress, the report contract) so editing the
//! domain prompt can never break the loop's contract with the agent.
//! The objective itself is the domain prompt's alone.

use std::fmt::Write;

use crate::progress::ProgressReport;
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

/// Composes the full prompt for one iteration: preamble first, domain
/// prompt verbatim at the end.
pub(crate) fn compose(
    iteration: u32,
    max_iterations: Option<u32>,
    previous: Option<&ProgressReport>,
    domain_prompt: &str,
) -> String {
    let mut text = String::new();
    let _ = write!(
        text,
        "# hako iteration\n\n\
         You are one iteration of an unattended loop; your objective is the \
         domain prompt at the end of this message.\n\n\
         This is iteration {}. Your context is fresh: nothing survives between \
         iterations except the workspace you are in, so read it before acting \
         and leave it consistent when you stop.\n",
        counter(iteration, max_iterations),
    );
    if let Some(report) = previous {
        let _ = write!(text, "\n## Previous iteration\n\n{}\n", report.summary);
        if !report.remaining.is_empty() {
            let _ = write!(text, "\nRemaining work:\n");
            for item in &report.remaining {
                let _ = writeln!(text, "- {item}");
            }
        }
    }
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

fn counter(iteration: u32, max_iterations: Option<u32>) -> String {
    match max_iterations {
        Some(max) => format!("{iteration} of {max}"),
        None => iteration.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use crate::progress::ProgressStatus;

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

    #[test]
    fn a_bounded_run_counts_against_the_ceiling() {
        let text = compose(3, Some(20), None, "domain");
        assert!(text.contains("iteration 3 of 20"), "{text}");
    }

    #[test]
    fn an_unbounded_run_counts_without_a_ceiling() {
        let text = compose(3, None, None, "domain");
        assert!(text.contains("This is iteration 3."), "{text}");
    }

    #[test]
    fn the_first_iteration_carries_no_history() {
        let text = compose(1, None, None, "domain");
        assert!(!text.contains("Previous iteration"), "{text}");
    }

    #[test]
    fn later_iterations_carry_the_last_summary_and_remaining_list() {
        let last = report("wired the store", &["docs", "tests"]);
        let text = compose(2, None, Some(&last), "domain");
        assert!(text.contains("wired the store"), "{text}");
        assert!(text.contains("- docs\n- tests\n"), "{text}");
    }

    #[test]
    fn an_empty_remaining_list_is_omitted() {
        let last = report("did things", &[]);
        let text = compose(2, None, Some(&last), "domain");
        assert!(!text.contains("Remaining work"), "{text}");
    }

    #[test]
    fn the_contract_names_the_file_and_every_status() {
        let text = compose(1, None, None, "domain");
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
    fn the_domain_prompt_closes_the_prompt_verbatim() {
        let text = compose(1, None, None, "## My rules\n\nkeep tests green\n");
        assert!(
            text.ends_with("## My rules\n\nkeep tests green\n"),
            "{text}"
        );
        let frame = text.find("hako iteration").unwrap();
        let domain = text.find("## My rules").unwrap();
        assert!(frame < domain);
    }
}
