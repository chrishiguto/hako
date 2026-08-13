//! Shared enforcement for Verified Done. Kernels compose their own
//! evidence prompt; this module owns the strict verdict contract,
//! repair loop, and event.

use serde::Deserialize;

use crate::event::RunEvent;
use crate::invocation::{self, ReportContract};
use crate::kernel::{KernelContext, KernelError};
use crate::sandbox::SandboxHandle;

pub(crate) const REPORT_SCHEMA: &str = r#"{
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

pub(crate) enum SkepticEnd {
    Unrefuted,
    Refuted(Vec<String>),
    Failed,
}

pub(crate) async fn evaluate(
    ctx: &KernelContext,
    iteration: u32,
    sandbox: &SandboxHandle,
    prompt: &str,
) -> Result<SkepticEnd, KernelError> {
    let Some(report) =
        invocation::invoke_to_report(ctx, iteration, sandbox, prompt, &SkepticContract).await?
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(text: &str) -> Result<SkepticReport, String> {
        SkepticContract.parse(text)
    }

    #[test]
    fn refuted_and_findings_must_agree() {
        let unrefuted = parse(r#"{"refuted": false, "findings": []}"#).unwrap();
        assert!(!unrefuted.refuted);
        let refuted = parse(r#"{"refuted": true, "findings": ["a TODO remains"]}"#).unwrap();
        assert_eq!(refuted.findings, ["a TODO remains"]);

        assert!(
            parse(r#"{"refuted": true, "findings": []}"#)
                .unwrap_err()
                .contains("at least one finding")
        );
        assert!(
            parse(r#"{"refuted": false, "findings": ["stray"]}"#)
                .unwrap_err()
                .contains("no findings")
        );
    }

    #[test]
    fn unknown_fields_are_rejected() {
        let error = parse(r#"{"refuted": false, "findings": [], "status": "done"}"#).unwrap_err();
        assert!(error.contains("unknown field"), "{error}");
    }

    #[test]
    fn the_schema_is_valid_json_naming_the_contract() {
        let schema: serde_json::Value = serde_json::from_str(REPORT_SCHEMA).unwrap();
        assert_eq!(schema["title"], "SkepticReport");
        assert_eq!(schema["additionalProperties"], false);
    }
}
