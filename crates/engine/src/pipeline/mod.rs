//! The pipeline kernel — a staged loop. One iteration drives one work
//! unit through a fixed sequence of stages: plan → implement → review →
//! simplify. The kernel owns the order and the gating in Rust; a flow
//! customizes each stage only through its prompt (the `[prompts]`
//! table, or the shipped default). Stages hand off solely through
//! schema-validated reports, the same pattern as the flow format:
//! strict parse, one repair re-prompt, the report quoted back to the
//! next stage.
//!
//! Every stage runs in its own fresh sandbox and fresh agent context —
//! a reviewer never inherits the implementer's environment. Mutating
//! stages (implement, review, simplify) are checkpointed and then
//! verified; a red check re-runs that stage with the failure in its
//! preamble, and exhausted retries pause the run. A `done` claim ends
//! the run, `blocked`/`needs_input` pause it immediately, mid-pipeline.
//!
//! What lands here later: the Verified Done skeptic gate (#7), the
//! safety rails — budgets, iteration timeout, drift, pause
//! notifications (#8), resume-in-place (#28), and the deliver stage
//! (#29). This slice is the loop those build on.

mod contract;
mod frame;

use async_trait::async_trait;

use crate::event::{IterationOutcome, RunEvent};
use crate::invocation::{self, InvocationEnd};
use crate::kernel::{Kernel, KernelContext, KernelError};
use crate::preamble::{Feedback, repair};
use crate::run::{PauseReason, RunOutcome};
use crate::sandbox::SandboxHandle;
use crate::verify::{self, VerifyOutcome};
use crate::workspace::WorkspaceError;
use proto::flow::{FailAction, KernelName};
use proto::pipeline::{Stage, StageReport};
use proto::report::ReportStatus;

/// The stages one iteration drives a work unit through, in order.
/// Forward-only — a stage never bounces back; what it cannot patch it
/// reports for the next iteration's plan. Deliver is absent until #29
/// wires it in.
const STAGES: [Stage; 4] = [
    Stage::Plan,
    Stage::Implement,
    Stage::Review,
    Stage::Simplify,
];

/// The staged kernel. Stateless — everything a run needs arrives in its
/// [`KernelContext`].
#[derive(Debug, Clone, Copy, Default)]
pub struct PipelineKernel;

#[async_trait]
impl Kernel for PipelineKernel {
    async fn run(&self, ctx: KernelContext) -> Result<RunOutcome, KernelError> {
        ctx.events
            .emit(RunEvent::RunStarted {
                kernel: KernelName::Pipeline.as_str().into(),
                agent: ctx.agent.name().into(),
            })
            .await?;

        // The reports the plan stage of the next iteration reads —
        // remaining work and unfixed findings carrying forward. Empty
        // for the first iteration; nothing came before it.
        let mut prior: Vec<StageReport> = Vec::new();
        let mut iteration: u32 = 1;
        loop {
            ctx.events
                .emit(RunEvent::IterationStarted { iteration })
                .await?;

            let mut pass: Vec<StageReport> = Vec::new();
            for (index, &stage) in STAGES.iter().enumerate() {
                // Plan opens a fresh unit, so it reads the previous
                // iteration; every later stage reads what this
                // iteration produced before it.
                let handoff = if index == 0 { &prior } else { &pass };
                match execute_stage(&ctx, iteration, stage, handoff).await? {
                    StageEnd::Advance(report) => pass.push(report),
                    // Done and pause leave the iteration mid-pipeline —
                    // the run ends or suspends on the agent's own word —
                    // so no `IterationFinished` closes a pass that never
                    // finished. Only a full pass (below) or a hard
                    // failure emits one.
                    StageEnd::Done => return conclude(&ctx, RunOutcome::Done).await,
                    StageEnd::Pause(reason) => {
                        return conclude(&ctx, RunOutcome::Paused(reason)).await;
                    }
                    StageEnd::Fail => {
                        ctx.events
                            .emit(RunEvent::IterationFinished {
                                iteration,
                                outcome: IterationOutcome::Failed,
                            })
                            .await?;
                        return conclude(&ctx, RunOutcome::Failed).await;
                    }
                }
            }

            ctx.events
                .emit(RunEvent::IterationFinished {
                    iteration,
                    outcome: IterationOutcome::Completed,
                })
                .await?;
            prior = pass;
            iteration += 1;
        }
    }
}

/// How one stage ended, as the loop reads it.
enum StageEnd {
    /// The stage reported `continue`; its report joins the hand-off to
    /// the next stage.
    Advance(StageReport),
    /// A stage claimed `done` and cleared its verify gate — the run is
    /// complete. (The Verified Done skeptic lands on top of this in #7.)
    Done,
    /// The run pauses now, mid-pipeline — a `blocked`/`needs_input`
    /// report, or verify failures that outran the retry budget.
    Pause(PauseReason),
    /// The stage produced no trustworthy report — a crashed agent or a
    /// report still malformed after its one repair.
    Fail,
}

/// Runs one stage to a verdict, re-running it in a fresh sandbox for as
/// many verify failures as the flow's `on_fail` allows before it pauses
/// or fails. Each pass re-reads the domain prompt (it is agent-editable)
/// and re-frames it — carrying the last verify failure so the agent
/// fixes the cause rather than repeating it.
async fn execute_stage(
    ctx: &KernelContext,
    iteration: u32,
    stage: Stage,
    handoff: &[StageReport],
) -> Result<StageEnd, KernelError> {
    let mut feedback: Option<Feedback> = None;
    let mut verify_failures: u32 = 0;
    loop {
        let StageDrive::Reported { report, verify } =
            drive_stage(ctx, iteration, stage, handoff, feedback.as_ref()).await?
        else {
            return Ok(StageEnd::Fail);
        };

        if let VerifyOutcome::Failed { command, output } = verify {
            verify_failures += 1;
            if verify_failures > ctx.verify.on_fail.retries {
                return Ok(match ctx.verify.on_fail.then {
                    FailAction::Pause => StageEnd::Pause(PauseReason::VerifyFailed),
                    FailAction::Fail => StageEnd::Fail,
                });
            }
            feedback = Some(Feedback::VerifyFailed { command, output });
            continue;
        }

        // Verify passed or was skipped: the report's own status decides
        // where the run goes.
        return Ok(match report.status() {
            ReportStatus::Continue => StageEnd::Advance(report),
            ReportStatus::Done => StageEnd::Done,
            ReportStatus::Blocked => StageEnd::Pause(PauseReason::Blocked),
            ReportStatus::NeedsInput => StageEnd::Pause(PauseReason::AwaitingHuman),
        });
    }
}

/// What one pass through a stage left behind.
enum StageDrive {
    /// A parsed report and how the verify checks gating it came out.
    Reported {
        report: StageReport,
        verify: VerifyOutcome,
    },
    /// No trustworthy report — the details are already in the log.
    Failed,
}

/// The sandbox-alive part of a stage: fresh sandbox in, destroyed
/// sandbox out on every path. Drive the agent to a report, checkpoint a
/// mutating stage's work, emit the report, then verify what it left —
/// the repair re-prompt and the checks share the invocation's sandbox,
/// because both judge the work it just did.
async fn drive_stage(
    ctx: &KernelContext,
    iteration: u32,
    stage: Stage,
    handoff: &[StageReport],
    feedback: Option<&Feedback>,
) -> Result<StageDrive, KernelError> {
    invocation::in_fresh_sandbox(ctx, async |sandbox| {
        let domain_prompt = resolve_prompt(ctx, sandbox, stage).await?;
        let prompt = frame::compose(stage, handoff, feedback, &domain_prompt);
        ctx.events
            .emit(RunEvent::StageStarted {
                iteration,
                stage: stage.as_str().into(),
            })
            .await?;

        let Some(report) = invoke_and_parse(ctx, iteration, stage, sandbox, &prompt).await? else {
            return Ok(StageDrive::Failed);
        };

        if is_mutating(stage)
            && let Some(commit) = ctx
                .workspace
                .checkpoint(&format!("hako: iteration {iteration} {}", stage.as_str()))
                .await?
        {
            ctx.events
                .emit(RunEvent::WorkspaceCheckpointed { iteration, commit })
                .await?;
        }

        // The report is emitted before its checks, so the log reads
        // claim first, verdict second.
        ctx.events
            .emit(RunEvent::StageReported {
                iteration,
                stage: stage.as_str().into(),
                report: report.to_json_value(),
            })
            .await?;

        let verify = if runs_verify(stage, report.status()) {
            verify::run_checks(ctx, sandbox, iteration).await?
        } else {
            VerifyOutcome::Skipped
        };
        Ok(StageDrive::Reported { report, verify })
    })
    .await
}

/// Drives the agent to a parsed report, spending the one repair
/// re-prompt a rejected report earns in the same sandbox — the work is
/// done, only the report needs fixing. `None` means the stage is out of
/// chances: a crash (no report to trust) or a report still malformed
/// after repair. Every rejection is logged for the repair to answer.
async fn invoke_and_parse(
    ctx: &KernelContext,
    iteration: u32,
    stage: Stage,
    sandbox: &SandboxHandle,
    prompt: &str,
) -> Result<Option<StageReport>, KernelError> {
    let errors = match parse(
        stage,
        invocation::invoke(ctx, iteration, sandbox, prompt).await?,
    ) {
        Parsed::Report(report) => return Ok(Some(report)),
        Parsed::Crashed => return Ok(None),
        Parsed::Rejected(errors) => errors,
    };
    let repair_prompt = repair(&errors, contract::report_schema(stage));
    ctx.events
        .emit(RunEvent::ReportRejected { iteration, errors })
        .await?;
    match parse(
        stage,
        invocation::invoke(ctx, iteration, sandbox, &repair_prompt).await?,
    ) {
        Parsed::Report(report) => Ok(Some(report)),
        Parsed::Crashed => Ok(None),
        Parsed::Rejected(errors) => {
            ctx.events
                .emit(RunEvent::ReportRejected { iteration, errors })
                .await?;
            Ok(None)
        }
    }
}

/// The outcome of reading one invocation's report against `stage`'s
/// schema.
enum Parsed {
    Report(StageReport),
    /// The agent exited badly — nothing it left can be trusted, so no
    /// repair is offered.
    Crashed,
    /// The report is missing or malformed; the errors feed the repair
    /// re-prompt.
    Rejected(Vec<String>),
}

fn parse(stage: Stage, end: InvocationEnd) -> Parsed {
    match end {
        InvocationEnd::Crashed => Parsed::Crashed,
        InvocationEnd::MissingReport(message) => Parsed::Rejected(vec![message]),
        InvocationEnd::Reported(raw) => match std::str::from_utf8(&raw) {
            Err(error) => Parsed::Rejected(vec![format!("report is not UTF-8: {error}")]),
            Ok(text) => match StageReport::from_stage_json(stage, text) {
                Ok(report) => Parsed::Report(report),
                Err(error) => Parsed::Rejected(vec![error.to_string()]),
            },
        },
    }
}

/// The stage's domain prompt: the flow's override for the slot, read
/// fresh from the workspace, or the kernel-shipped default when the
/// slot is unset.
async fn resolve_prompt(
    ctx: &KernelContext,
    sandbox: &SandboxHandle,
    stage: Stage,
) -> Result<String, KernelError> {
    match ctx.prompts.get(stage.as_str()) {
        Some(path) => {
            let guest_path = ctx.workspace.guest_path(path)?;
            let raw = ctx.sandbox.get_file(sandbox, &guest_path).await?;
            String::from_utf8(raw).map_err(|error| {
                WorkspaceError(format!("prompt `{path}` is not UTF-8: {error}")).into()
            })
        }
        None => contract::default_prompt(stage)
            .map(str::to_owned)
            .ok_or_else(|| {
                WorkspaceError(format!(
                    "stage `{}` has no default prompt and must be explicitly enabled",
                    stage.as_str()
                ))
                .into()
            }),
    }
}

/// The stages that change the workspace — implement, review, simplify.
/// Only these are checkpointed and verified. Plan selects the unit and
/// deliver publishes (once #29 wires it); neither is gated here.
fn is_mutating(stage: Stage) -> bool {
    matches!(stage, Stage::Implement | Stage::Review | Stage::Simplify)
}

/// Whether a stage's report faces the verify checks: only a mutating
/// stage with fresh work to judge. A pausing status stops the run on
/// the agent's own word, so there is nothing to verify.
fn runs_verify(stage: Stage, status: ReportStatus) -> bool {
    is_mutating(stage) && matches!(status, ReportStatus::Continue | ReportStatus::Done)
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
    use super::*;

    /// The kernel answers to the name flows select it by — the same
    /// wire string, so run metadata and `[loop] kernel` cannot drift.
    #[test]
    fn the_kernel_names_itself_by_the_flow_name() {
        assert_eq!(KernelName::Pipeline.as_str(), "pipeline");
    }

    /// Plan and deliver are not checkpointed or verified; the three
    /// stages that edit the workspace are.
    #[test]
    fn only_the_workspace_editing_stages_mutate() {
        assert!(!is_mutating(Stage::Plan));
        assert!(is_mutating(Stage::Implement));
        assert!(is_mutating(Stage::Review));
        assert!(is_mutating(Stage::Simplify));
        assert!(!is_mutating(Stage::Deliver));
    }

    /// Verify judges fresh work only: a mutating stage that claims
    /// progress. A pausing status skips its checks, whatever the stage.
    #[test]
    fn verify_runs_only_on_mutating_stages_that_claim_progress() {
        assert!(runs_verify(Stage::Implement, ReportStatus::Continue));
        assert!(runs_verify(Stage::Implement, ReportStatus::Done));
        assert!(!runs_verify(Stage::Implement, ReportStatus::Blocked));
        assert!(!runs_verify(Stage::Implement, ReportStatus::NeedsInput));
        assert!(!runs_verify(Stage::Plan, ReportStatus::Continue));
    }
}
