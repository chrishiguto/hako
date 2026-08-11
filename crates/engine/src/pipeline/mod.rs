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
//! preamble, and exhausted retries pause the run. A `done` claim starts
//! a fresh skeptic invocation; only an unrefuted claim ends the run.
//! `blocked`/`needs_input` pause it immediately, mid-pipeline.

mod contract;
pub(crate) mod frame;
pub(crate) mod skeptic;

use async_trait::async_trait;

use crate::event::{IterationOutcome, RunEvent};
use crate::invocation::{self, Bracketed};
use crate::kernel::{Kernel, KernelContext, KernelError};
use crate::preamble::Feedback;
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
        let mut plan_feedback: Vec<Feedback> = Vec::new();
        let mut iteration: u32 = 1;
        loop {
            ctx.events
                .emit(RunEvent::IterationStarted { iteration })
                .await?;
            match run_iteration(&ctx, iteration, &prior, std::mem::take(&mut plan_feedback)).await?
            {
                IterationEnd::Continue { pass, feedback } => {
                    ctx.events
                        .emit(RunEvent::IterationFinished {
                            iteration,
                            outcome: IterationOutcome::Completed,
                        })
                        .await?;
                    prior = pass;
                    plan_feedback = feedback;
                    iteration += 1;
                }
                IterationEnd::Done => return conclude(&ctx, RunOutcome::Done).await,
                IterationEnd::Pause(reason) => {
                    return conclude(&ctx, RunOutcome::Paused(reason)).await;
                }
                IterationEnd::Fail => {
                    ctx.events
                        .emit(RunEvent::IterationFinished {
                            iteration,
                            outcome: IterationOutcome::Failed,
                        })
                        .await?;
                    return conclude(&ctx, RunOutcome::Failed).await;
                }
                IterationEnd::Cancelled => return conclude(&ctx, RunOutcome::Cancelled).await,
            }
        }
    }
}

/// How one iteration ended, as the run loop reads it. Every way an
/// iteration can end — a full pass, a skeptic's verdict, a pause, a
/// failure — lands here, so the loop emits each closing event and
/// keeps its books in exactly one place.
enum IterationEnd {
    /// The iteration counts as verified progress and the run goes on:
    /// a full pass, or a `done` claim the skeptic refuted. Carries the
    /// reports the next plan reads and the feedback it must answer —
    /// the skeptic's findings, or nothing.
    Continue {
        pass: Vec<StageReport>,
        feedback: Vec<Feedback>,
    },
    /// A `done` claim cleared its verify gate and survived the skeptic
    /// — the run is complete.
    Done,
    /// The run pauses now, mid-pipeline — a `blocked`/`needs_input`
    /// report, or verify failures that outran the retry budget.
    Pause(PauseReason),
    /// A stage or the skeptic produced no trustworthy report; the
    /// iteration counts as failed.
    Fail,
    /// The run's cancel token fired; the iteration stops where it
    /// stands.
    Cancelled,
}

/// Drives one iteration through the stages. Plan opens a fresh unit,
/// so it reads the previous iteration's reports and the feedback the
/// loop carried in; every later stage reads what this iteration
/// produced before it. A `done` claim that cleared its verify gate
/// meets the skeptic here — unrefuted ends the run, refuted ends the
/// iteration as progress with the findings as the next plan's
/// feedback.
async fn run_iteration(
    ctx: &KernelContext,
    iteration: u32,
    prior: &[StageReport],
    mut plan_feedback: Vec<Feedback>,
) -> Result<IterationEnd, KernelError> {
    let mut pass: Vec<StageReport> = Vec::new();
    for (index, &stage) in STAGES.iter().enumerate() {
        let handoff = if index == 0 { prior } else { pass.as_slice() };
        let feedback = if index == 0 {
            std::mem::take(&mut plan_feedback)
        } else {
            Vec::new()
        };
        match execute_stage(ctx, iteration, stage, handoff, feedback).await? {
            StageEnd::Advance(report) => pass.push(report),
            StageEnd::Done(claim) => {
                return Ok(match skeptic::judge(ctx, iteration, &claim).await? {
                    Bracketed::Finished(skeptic::SkepticEnd::Unrefuted) => IterationEnd::Done,
                    Bracketed::Finished(skeptic::SkepticEnd::Refuted(findings)) => {
                        // The refuted claim still advanced the work:
                        // its report joins the hand-off, its findings
                        // become the next plan's feedback.
                        pass.push(claim);
                        IterationEnd::Continue {
                            pass,
                            feedback: vec![Feedback::SkepticRefuted { findings }],
                        }
                    }
                    Bracketed::Finished(skeptic::SkepticEnd::Failed) => IterationEnd::Fail,
                    Bracketed::Cancelled => IterationEnd::Cancelled,
                });
            }
            StageEnd::Pause(reason) => return Ok(IterationEnd::Pause(reason)),
            StageEnd::Fail => return Ok(IterationEnd::Fail),
            StageEnd::Cancelled => return Ok(IterationEnd::Cancelled),
        }
    }
    Ok(IterationEnd::Continue {
        pass,
        feedback: Vec::new(),
    })
}

/// How one stage ended, as the loop reads it.
enum StageEnd {
    /// The stage reported `continue`; its report joins the hand-off to
    /// the next stage.
    Advance(StageReport),
    /// A stage claimed `done` and cleared its verify gate. Carries the
    /// report the skeptic must interrogate before completion is real.
    Done(StageReport),
    /// The run pauses now, mid-pipeline — a `blocked`/`needs_input`
    /// report, or verify failures that outran the retry budget.
    Pause(PauseReason),
    /// The stage produced no trustworthy report — a crashed agent or a
    /// report still malformed after its one repair.
    Fail,
    /// The run's cancel token fired, mid-stage or at the boundary
    /// before this stage booted anything: a finished stage's work
    /// stands, no further stage starts, and the run ends `Cancelled` —
    /// terminal, unlike a pause.
    Cancelled,
}

/// Runs one stage to a verdict, re-running it in a fresh sandbox for as
/// many verify failures as the flow's `on_fail` allows before it pauses
/// or fails. Each pass re-reads the agent-editable domain prompt. The
/// first attempt may carry prior-loop feedback; a verify retry replaces
/// that with the latest check failure.
async fn execute_stage(
    ctx: &KernelContext,
    iteration: u32,
    stage: Stage,
    handoff: &[StageReport],
    mut feedback: Vec<Feedback>,
) -> Result<StageEnd, KernelError> {
    let mut verify_failures: u32 = 0;
    loop {
        let drive = match drive_stage(ctx, iteration, stage, handoff, &feedback).await? {
            Bracketed::Finished(drive) => drive,
            Bracketed::Cancelled => return Ok(StageEnd::Cancelled),
        };
        let StageDrive::Reported { report, verify } = drive else {
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
            // Replaced, not accumulated: the re-run answers the latest
            // failure, not a history of them.
            feedback = vec![Feedback::VerifyFailed { command, output }];
            continue;
        }

        // Verify passed or was skipped: the report's own status decides
        // where the run goes.
        return Ok(match report.status() {
            ReportStatus::Continue => StageEnd::Advance(report),
            ReportStatus::Done => StageEnd::Done(report),
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
    feedback: &[Feedback],
) -> Result<Bracketed<StageDrive>, KernelError> {
    invocation::in_fresh_sandbox(ctx, async |sandbox| {
        let domain_prompt = resolve_prompt(ctx, sandbox, stage).await?;
        let prompt = frame::compose(&frame::Frame {
            stage,
            handoff,
            feedback,
            // No human on this path: only a resume carries a paused
            // run's answers back into a frame.
            human: None,
            domain_prompt: &domain_prompt,
        });
        ctx.events
            .emit(RunEvent::StageStarted {
                iteration,
                stage: stage.as_str().into(),
            })
            .await?;

        let Some(report) =
            invocation::invoke_to_report(ctx, iteration, sandbox, &prompt, &stage).await?
        else {
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

/// Mutating progress is always checked; a `done` claim from any stage
/// also needs a current green verdict before it can reach the skeptic.
fn runs_verify(stage: Stage, status: ReportStatus) -> bool {
    status == ReportStatus::Done || (is_mutating(stage) && status == ReportStatus::Continue)
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
        assert!(runs_verify(Stage::Plan, ReportStatus::Done));
    }
}
