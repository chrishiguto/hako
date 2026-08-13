//! The Verified Done gate for the pipeline: a fresh invocation reads
//! the claiming report, the domain prompts, and the workspace, then
//! reports whether concrete evidence refutes completion.

use std::fmt::Write;

use super::{active_stages, resolve_prompt};
use crate::invocation::{self, Bracketed};
use crate::kernel::{KernelContext, KernelError};
use crate::preamble;
use crate::skeptic;
pub(super) use crate::skeptic::SkepticEnd;
use crate::workspace::REPORT_FILE;
use proto::pipeline::{Stage, StageReport};

/// The heading that opens the skeptic's prompt. Published (through the
/// testkit) so a fake telling the skeptic invocation apart from a
/// stage's shares one definition with [`compose`] rather than
/// respelling its wording.
pub(crate) const PROMPT_HEADING: &str = "# hako pipeline — skeptic iteration";

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
        skeptic::evaluate(ctx, iteration, sandbox, &prompt).await
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
         ```json\n{}\n```",
        skeptic::REPORT_SCHEMA,
    );
    text
}
