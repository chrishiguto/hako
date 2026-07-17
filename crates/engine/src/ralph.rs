//! The Ralph kernel — a single-prompt loop: every iteration runs the
//! same domain prompt, which alone carries the objective. Each gets a
//! fresh sandbox and a fresh agent context; the workspace is the only
//! memory. The loop trusts nothing it cannot see: the agent speaks
//! back only through the progress report — with one repair re-prompt
//! when a report fails validation — and every step lands in the event
//! log. A human speaks back through resume: answers and notes ride the
//! next iteration's preamble.

use std::collections::BTreeMap;

use async_trait::async_trait;
use futures_util::StreamExt;

use crate::event::{IterationOutcome, OutputStream, RunEvent};
use crate::kernel::{Kernel, KernelContext, KernelError};
use crate::preamble;
use crate::progress::{ProgressReport, ProgressStatus};
use crate::run::{PauseReason, Resume, RunOutcome};
use crate::sandbox::{ExecEvent, SandboxHandle, SandboxSpec};
use proto::budget::BudgetKind;

/// The v1 kernel. Stateless — everything a run needs arrives in its
/// [`KernelContext`].
#[derive(Debug, Clone, Copy, Default)]
pub struct RalphKernel;

/// What one agent attempt left behind.
enum AttemptEnd {
    /// The agent exited non-zero or was killed; there is no report to
    /// trust.
    Crashed,
    /// The report is missing or malformed; the errors are what the
    /// repair re-prompt carries back.
    Rejected(Vec<String>),
    Reported(ProgressReport),
}

#[async_trait]
impl Kernel for RalphKernel {
    fn name(&self) -> &str {
        "ralph"
    }

    async fn run(&self, mut ctx: KernelContext) -> Result<RunOutcome, KernelError> {
        // A resume picks the loop up where the host says it paused; the
        // human's words ride the first preamble, then the loop is on
        // its own again.
        let mut human = ctx.resume.take();
        let (mut previous, mut iteration): (Option<ProgressReport>, u32) = match &human {
            Some(resume) => {
                ctx.events
                    .emit(RunEvent::RunResumed {
                        note: resume.note.clone(),
                    })
                    .await?;
                (Some(resume.report.clone()), resume.iteration + 1)
            }
            None => {
                ctx.events
                    .emit(RunEvent::RunStarted {
                        kernel: self.name().into(),
                        agent: ctx.agent.name().into(),
                    })
                    .await?;
                (None, 1)
            }
        };
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

            let resume = human.take();
            let Some(report) = iterate(&ctx, iteration, previous.as_ref(), resume.as_ref()).await?
            else {
                return fail_iteration(&ctx, iteration).await;
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
/// every path, so an error can never leak a live sandbox. `None` means
/// the iteration failed — a crashed agent or a twice-rejected report —
/// with the details already in the log.
async fn iterate(
    ctx: &KernelContext,
    iteration: u32,
    previous: Option<&ProgressReport>,
    human: Option<&Resume>,
) -> Result<Option<ProgressReport>, KernelError> {
    // Re-read every iteration: the domain prompt is agent-editable.
    // A prompt that cannot be read — including one the agent deleted —
    // is run-fatal by design: the loop must not iterate without its
    // domain rules.
    let domain_prompt = ctx.workspace.domain_prompt().await?;
    let prompt = preamble::compose(
        &preamble::Preamble {
            iteration,
            max_iterations: ctx.budgets.max_iterations,
            previous,
            answers: human.map_or(&[], |resume| &resume.answers),
            note: human.and_then(|resume| resume.note.as_deref()),
        },
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

/// The part of an iteration that needs the sandbox alive: one full
/// agent attempt, and — when its report fails validation — the one
/// repair re-prompt ADR 0007 grants. The repair runs in the same
/// sandbox: the workspace already holds the iteration's work, and only
/// the report is being repaired. Every rejection is emitted here;
/// `None` means the iteration is out of chances.
async fn drive_agent(
    ctx: &KernelContext,
    iteration: u32,
    sandbox: &SandboxHandle,
    prompt: &str,
) -> Result<Option<ProgressReport>, KernelError> {
    let errors = match attempt(ctx, iteration, sandbox, prompt).await? {
        AttemptEnd::Reported(report) => return Ok(Some(report)),
        AttemptEnd::Crashed => return Ok(None),
        AttemptEnd::Rejected(errors) => errors,
    };
    let repair = preamble::repair(&errors);
    ctx.events
        .emit(RunEvent::ProgressRejected { iteration, errors })
        .await?;
    match attempt(ctx, iteration, sandbox, &repair).await? {
        AttemptEnd::Reported(report) => Ok(Some(report)),
        AttemptEnd::Crashed => Ok(None),
        AttemptEnd::Rejected(errors) => {
            ctx.events
                .emit(RunEvent::ProgressRejected { iteration, errors })
                .await?;
            Ok(None)
        }
    }
}

/// One agent attempt: invoke it headless, checkpoint what it changed,
/// collect its report.
async fn attempt(
    ctx: &KernelContext,
    iteration: u32,
    sandbox: &SandboxHandle,
    prompt: &str,
) -> Result<AttemptEnd, KernelError> {
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
        return Ok(AttemptEnd::Crashed);
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
            return Ok(AttemptEnd::Rejected(vec![format!(
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

fn parse_report(raw: &[u8]) -> AttemptEnd {
    let text = match std::str::from_utf8(raw) {
        Ok(text) => text,
        Err(error) => {
            return AttemptEnd::Rejected(vec![format!("progress report is not UTF-8: {error}")]);
        }
    };
    match ProgressReport::from_agent_json(text) {
        Ok(report) => AttemptEnd::Reported(report),
        Err(error) => AttemptEnd::Rejected(vec![error.to_string()]),
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
