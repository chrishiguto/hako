use std::fmt::Write;

use proto::fanout::PlanReport;

use super::contract;
use crate::preamble;
use crate::skeptic;
use crate::workspace::REPORT_FILE;

pub(super) fn plan(domain: &str, feedback: &[String]) -> String {
    let mut prompt = format!("# hako fanout — plan stage\n\n{}\n", domain.trim());
    if !feedback.is_empty() {
        prompt.push_str("\n## Skeptic findings\n\nThe previous completion claim was refuted:\n");
        for finding in feedback {
            let _ = writeln!(prompt, "- {finding}");
        }
    }
    let _ = write!(
        prompt,
        "\n## Your report\n\nEnd by writing `{REPORT_FILE}`. Unit strings are opaque to the engine; each one scopes exactly one child pipeline. Use `continue` with ready units, `done` only when the whole objective is complete, `blocked` when progress is externally stopped, or `needs_input` for human questions. A `done` claim must pass verification and an independent skeptic.\n\nThe report must match this schema exactly:\n\n```json\n{}\n```",
        contract::report_schema().trim_end()
    );
    prompt
}

pub(super) fn skeptic(claim: &PlanReport, domain: &str) -> String {
    let claim = serde_json::to_string_pretty(claim).expect("fanout reports serialize");
    format!(
        "# hako fanout — skeptic iteration\n\nIndependently test the completion claim against the workspace and domain prompt. Look for concrete evidence that ready or unfinished work remains. Do not change the workspace.\n\n## Done claim\n\n{}\n\n## Domain prompt\n\n{}\n\n## Your report\n\nWrite `{REPORT_FILE}` and do nothing else. Set `refuted` to true only when concrete evidence disproves completion, and list that evidence in `findings`. Otherwise set it to false and leave `findings` empty.\n\n```json\n{}\n```",
        preamble::fenced(&claim),
        domain.trim(),
        skeptic::REPORT_SCHEMA,
    )
}
