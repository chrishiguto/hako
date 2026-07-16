//! The Ralph kernel — a single-prompt loop: every iteration runs the
//! same domain prompt, which alone carries the objective. Each gets a
//! fresh sandbox and a fresh agent context; the workspace is the only
//! memory. The loop trusts nothing it cannot see: the agent speaks
//! back only through the progress report, and every step lands in the
//! event log.

use std::collections::BTreeMap;

use async_trait::async_trait;
use futures_util::StreamExt;

use crate::event::{IterationOutcome, OutputStream, RunEvent};
use crate::kernel::{Kernel, KernelContext, KernelError};
use crate::preamble;
use crate::progress::{ProgressReport, ProgressStatus};
use crate::run::{PauseReason, RunOutcome};
use crate::sandbox::{ExecEvent, SandboxHandle, SandboxSpec};
use proto::budget::BudgetKind;

/// The v1 kernel. Stateless — everything a run needs arrives in its
/// [`KernelContext`].
#[derive(Debug, Clone, Copy, Default)]
pub struct RalphKernel;

/// What one iteration left behind, once its sandbox is gone.
enum IterationEnd {
    /// The agent exited non-zero or was killed; there is no report to
    /// trust.
    Crashed,
    /// The report is missing or malformed; the errors are what a
    /// repair re-prompt would carry back.
    Rejected(Vec<String>),
    Reported(ProgressReport),
}

#[async_trait]
impl Kernel for RalphKernel {
    fn name(&self) -> &str {
        "ralph"
    }

    async fn run(&self, ctx: KernelContext) -> Result<RunOutcome, KernelError> {
        ctx.events
            .emit(RunEvent::RunStarted {
                kernel: self.name().into(),
                agent: ctx.agent.name().into(),
            })
            .await?;

        let mut previous: Option<ProgressReport> = None;
        let mut iteration: u32 = 1;
        loop {
            if ctx
                .budgets
                .max_iterations
                .is_some_and(|max| iteration > max)
            {
                ctx.events
                    .emit(RunEvent::BudgetExhausted {
                        budget: BudgetKind::Iterations,
                    })
                    .await?;
                return conclude(&ctx, RunOutcome::Paused(PauseReason::Budget)).await;
            }

            ctx.events
                .emit(RunEvent::IterationStarted { iteration })
                .await?;

            let report = match iterate(&ctx, iteration, previous.as_ref()).await? {
                IterationEnd::Crashed => {
                    return fail_iteration(&ctx, iteration).await;
                }
                IterationEnd::Rejected(errors) => {
                    ctx.events
                        .emit(RunEvent::ProgressRejected { iteration, errors })
                        .await?;
                    return fail_iteration(&ctx, iteration).await;
                }
                IterationEnd::Reported(report) => report,
            };

            ctx.events
                .emit(RunEvent::ProgressReported {
                    iteration,
                    report: report.clone(),
                })
                .await?;
            ctx.events
                .emit(RunEvent::IterationFinished {
                    iteration,
                    outcome: IterationOutcome::Completed,
                })
                .await?;

            match report.status {
                ProgressStatus::Continue => {
                    previous = Some(report);
                    iteration += 1;
                }
                // A done claim is accepted as-is here; the Verified
                // Done gate (verify checks plus a skeptic iteration)
                // guards it in the full loop.
                ProgressStatus::Done => return conclude(&ctx, RunOutcome::Done).await,
                ProgressStatus::Blocked => {
                    return conclude(&ctx, RunOutcome::Paused(PauseReason::Blocked)).await;
                }
                ProgressStatus::NeedsInput => {
                    return conclude(&ctx, RunOutcome::Paused(PauseReason::AwaitingHuman)).await;
                }
            }
        }
    }
}

/// One full iteration: fresh sandbox in, destroyed sandbox out — on
/// every path, so an error can never leak a live sandbox.
async fn iterate(
    ctx: &KernelContext,
    iteration: u32,
    previous: Option<&ProgressReport>,
) -> Result<IterationEnd, KernelError> {
    // Re-read every iteration: the domain prompt is agent-editable.
    // A prompt that cannot be read — including one the agent deleted —
    // is run-fatal by design: the loop must not iterate without its
    // domain rules.
    let domain_prompt = ctx.workspace.domain_prompt().await?;
    let prompt = preamble::compose(
        iteration,
        ctx.budgets.max_iterations,
        previous,
        &domain_prompt,
    );

    let spec = SandboxSpec {
        workspace: ctx.workspace.mount(),
        env: BTreeMap::new(),
    };
    let sandbox = ctx.sandbox.create(&spec).await?;
    let end = drive_agent(ctx, iteration, &sandbox, &prompt).await;
    let destroyed = ctx.sandbox.destroy(sandbox).await;
    let end = end?;
    destroyed?;
    Ok(end)
}

/// The part of an iteration that needs the sandbox alive: run the
/// agent, checkpoint what it changed, collect its report.
async fn drive_agent(
    ctx: &KernelContext,
    iteration: u32,
    sandbox: &SandboxHandle,
    prompt: &str,
) -> Result<IterationEnd, KernelError> {
    let invocation = ctx.agent.invocation(prompt);
    let mut output = ctx.sandbox.exec_stream(sandbox, &invocation).await?;
    let mut stdout = String::new();
    let mut exit = None;
    while let Some(event) = output.next().await {
        match event? {
            ExecEvent::Stdout(bytes) => {
                let chunk = into_text(bytes);
                stdout.push_str(&chunk);
                ctx.events
                    .emit(RunEvent::AgentOutput {
                        iteration,
                        stream: OutputStream::Stdout,
                        chunk,
                    })
                    .await?;
            }
            ExecEvent::Stderr(bytes) => {
                ctx.events
                    .emit(RunEvent::AgentOutput {
                        iteration,
                        stream: OutputStream::Stderr,
                        chunk: into_text(bytes),
                    })
                    .await?;
            }
            ExecEvent::Exited(status) => exit = Some(status),
        }
    }

    if let Some(usage) = ctx.agent.token_usage(&stdout) {
        ctx.events
            .emit(RunEvent::TokensUsed { iteration, usage })
            .await?;
    }

    if !exit.is_some_and(|status| status.success()) {
        return Ok(IterationEnd::Crashed);
    }

    if let Some(commit) = ctx
        .workspace
        .checkpoint(&format!("hako: iteration {iteration}"))
        .await?
    {
        ctx.events
            .emit(RunEvent::WorkspaceCheckpointed { iteration, commit })
            .await?;
    }

    let report_path = ctx.workspace.guest_progress_path();
    let raw = match ctx.sandbox.get_file(sandbox, &report_path).await {
        Ok(raw) => raw,
        Err(error) => {
            return Ok(IterationEnd::Rejected(vec![format!(
                "progress report missing: {error}"
            )]));
        }
    };
    Ok(parse_report(&raw))
}

/// Exec output decoded for the event log. Valid UTF-8 — the common
/// case — moves without a copy; invalid bytes fall back to the lossy
/// copy, byte for byte what `from_utf8_lossy` would produce.
fn into_text(bytes: Vec<u8>) -> String {
    String::from_utf8(bytes)
        .unwrap_or_else(|error| String::from_utf8_lossy(error.as_bytes()).into_owned())
}

fn parse_report(raw: &[u8]) -> IterationEnd {
    let text = match std::str::from_utf8(raw) {
        Ok(text) => text,
        Err(error) => {
            return IterationEnd::Rejected(vec![format!("progress report is not UTF-8: {error}")]);
        }
    };
    match ProgressReport::from_agent_json(text) {
        Ok(report) => IterationEnd::Reported(report),
        Err(error) => IterationEnd::Rejected(vec![error.to_string()]),
    }
}

async fn fail_iteration(ctx: &KernelContext, iteration: u32) -> Result<RunOutcome, KernelError> {
    ctx.events
        .emit(RunEvent::IterationFinished {
            iteration,
            outcome: IterationOutcome::Failed,
        })
        .await?;
    conclude(ctx, RunOutcome::Failed).await
}

/// Every ending goes out the same door: the terminal `state_changed`
/// event, then the outcome to the caller.
async fn conclude(ctx: &KernelContext, outcome: RunOutcome) -> Result<RunOutcome, KernelError> {
    ctx.events
        .emit(RunEvent::StateChanged {
            state: outcome.into(),
        })
        .await?;
    Ok(outcome)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    /// The name flows select and the name this kernel answers to are
    /// the same wire string.
    #[test]
    fn the_kernel_answers_to_the_name_flows_select() {
        let selected: proto::flow::KernelName =
            serde_json::from_value(json!(RalphKernel.name())).unwrap();
        assert_eq!(selected, proto::flow::KernelName::Ralph);
    }
}
