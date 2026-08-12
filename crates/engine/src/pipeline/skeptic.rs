//! The Verified Done gate for the pipeline: a fresh invocation reads
//! the claiming report, the domain prompts, and the workspace, then
//! reports whether concrete evidence refutes completion.

use std::fmt::Write;

use serde::Deserialize;

use super::{active_stages, resolve_prompt};
use crate::event::RunEvent;
use crate::invocation::{self, Bracketed, ReportContract};
use crate::kernel::{KernelContext, KernelError};
use crate::preamble;
use crate::workspace::REPORT_FILE;
use proto::pipeline::{Stage, StageReport};

/// The heading that opens the skeptic's prompt. Published (through the
/// testkit) so a fake telling the skeptic invocation apart from a
/// stage's shares one definition with [`compose`] rather than
/// respelling its wording.
pub(crate) const PROMPT_HEADING: &str = "# hako pipeline — skeptic iteration";

const REPORT_SCHEMA: &str = r#"{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "SkepticReport",
  "type": "object",
  "properties": {
    "refuted": {
      "description": "Whether concrete evidence shows the domain prompts are not yet satisfied.",
      "type": "boolean"
    },
    "findings": {
      "description": "Concrete unmet requirements; empty when the claim survives scrutiny.",
      "type": "array",
      "items": { "type": "string" }
    }
  },
  "required": ["refuted", "findings"],
  "additionalProperties": false,
  "if": {
    "properties": { "refuted": { "const": true } }
  },
  "then": {
    "properties": { "findings": { "minItems": 1 } }
  },
  "else": {
    "properties": { "findings": { "maxItems": 0 } }
  }
}"#;

struct SkepticContract;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SkepticReport {
    refuted: bool,
    findings: Vec<String>,
}

impl ReportContract for SkepticContract {
    type Report = SkepticReport;

    fn schema(&self) -> &str {
        REPORT_SCHEMA
    }

    fn parse(&self, text: &str) -> Result<Self::Report, String> {
        let report: SkepticReport =
            serde_json::from_str(text).map_err(|error| error.to_string())?;
        match (report.refuted, report.findings.is_empty()) {
            (true, true) => Err("a refutation requires at least one finding".into()),
            (false, false) => Err("an unrefuted claim must have no findings".into()),
            _ => Ok(report),
        }
    }
}

pub(super) enum SkepticEnd {
    Unrefuted,
    Refuted(Vec<String>),
    Failed,
}

pub(super) async fn judge(
    ctx: &KernelContext,
    iteration: u32,
    claim: &StageReport,
    deadline: tokio::time::Instant,
) -> Result<Bracketed<SkepticEnd>, KernelError> {
    invocation::in_fresh_sandbox_until(ctx, Some(deadline), async |sandbox| {
        let mut domain_prompts = Vec::with_capacity(Stage::ALL.len());
        for stage in active_stages(&ctx.prompts) {
            domain_prompts.push((stage, resolve_prompt(ctx, sandbox, stage).await?));
        }
        let prompt = compose(claim, &domain_prompts);
        let Some(report) =
            invocation::invoke_to_report(ctx, iteration, sandbox, &prompt, &SkepticContract)
                .await?
        else {
            return Ok(SkepticEnd::Failed);
        };

        ctx.events
            .emit(RunEvent::SkepticVerdict {
                iteration,
                refuted: report.refuted,
                findings: report.findings.clone(),
            })
            .await?;
        Ok(if report.refuted {
            SkepticEnd::Refuted(report.findings)
        } else {
            SkepticEnd::Unrefuted
        })
    })
    .await
}

fn compose(claim: &StageReport, domain_prompts: &[(Stage, String)]) -> String {
    let mut text = format!(
        "{PROMPT_HEADING}\n\n\
         Independently test the `done` claim against the workspace and every \
         domain prompt. Look for concrete evidence that any requirement remains \
         unsatisfied. Do not change the workspace.\n\n\
         ## Done claim\n\n\
         ### {} report\n\n{}\n\n\
         ## Domain prompts\n",
        claim.stage().as_str(),
        preamble::fenced(&claim.to_pretty_json()),
    );
    for (stage, prompt) in domain_prompts {
        let _ = write!(
            text,
            "\n### {} domain prompt\n\n{}\n",
            stage.as_str(),
            prompt.trim(),
        );
    }
    let _ = write!(
        text,
        "\n## Your report\n\n\
         Write `{REPORT_FILE}` and do nothing else. Set `refuted` to true only \
         when the workspace contains concrete evidence that a domain prompt is \
         unsatisfied, and list each piece of evidence in `findings`. Otherwise \
         set `refuted` to false and leave `findings` empty.\n\n\
         The report must match this schema exactly:\n\n\
         ```json\n{REPORT_SCHEMA}\n```",
    );
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(text: &str) -> Result<SkepticReport, String> {
        SkepticContract.parse(text)
    }

    /// The cross-field rule the schema states in `if`/`then`/`else`,
    /// pinned against the strict parse so the two hand-written
    /// spellings cannot drift apart.
    #[test]
    fn refuted_and_findings_must_agree() {
        let unrefuted = parse(r#"{"refuted": false, "findings": []}"#).unwrap();
        assert!(!unrefuted.refuted);
        let refuted = parse(r#"{"refuted": true, "findings": ["a TODO remains"]}"#).unwrap();
        assert_eq!(refuted.findings, ["a TODO remains"]);

        let error = parse(r#"{"refuted": true, "findings": []}"#).unwrap_err();
        assert!(error.contains("at least one finding"), "{error}");
        let error = parse(r#"{"refuted": false, "findings": ["stray"]}"#).unwrap_err();
        assert!(error.contains("no findings"), "{error}");
    }

    /// The strict parse matches the schema's `additionalProperties:
    /// false` — a stage report mistakenly served to the skeptic is
    /// rejected, not half-read.
    #[test]
    fn unknown_fields_are_rejected() {
        let error = parse(r#"{"refuted": false, "findings": [], "status": "done"}"#).unwrap_err();
        assert!(error.contains("unknown field"), "{error}");
    }

    /// The schema is hand-written, not a committed artifact like the
    /// stage schemas — so at least its JSON validity and title are
    /// pinned here.
    #[test]
    fn the_schema_is_valid_json_naming_the_contract() {
        let schema: serde_json::Value = serde_json::from_str(REPORT_SCHEMA).unwrap();
        assert_eq!(schema["title"], "SkepticReport");
        assert_eq!(schema["additionalProperties"], false);
    }
}
