//! The pipeline kernel — a staged loop. One iteration drives one work
//! unit through a fixed sequence of stages: plan → implement → review →
//! simplify, then deliver when the flow enables it. The kernel owns the
//! order and gating; a flow customizes each stage only through its
//! prompt (the `[prompts]` table, or the shipped default). Stages hand
//! off solely through schema-validated reports, the same pattern as
//! the flow format:
//! strict parse, one repair re-prompt, the report quoted back to the
//! next stage.
//!
//! Every stage runs in its own fresh sandbox and fresh agent context —
//! a reviewer never inherits the implementer's environment. Mutating
//! stages (implement, review, simplify) are checkpointed and then
//! verified; a red check re-runs that stage with the failure in its
//! preamble, and exhausted retries pause the run. A `done` claim starts
//! a fresh skeptic invocation; only an unrefuted claim ends the run —
//! mid-pass, so no later stage runs, a configured deliver included.
//! `blocked`/`needs_input` pause it immediately, mid-pipeline.

mod contract;
mod ending;
pub(crate) mod frame;
mod resume;
pub(crate) mod skeptic;

use async_trait::async_trait;

use self::ending::{conclude, last_summary, pause_for_budget};
use crate::event::{IterationOutcome, RunEvent};
use crate::invocation::{self, Bracketed};
use crate::kernel::{Kernel, KernelContext, KernelError};
use crate::preamble::{Feedback, HumanInput};
use crate::run::{PauseReason, RunOutcome};
use crate::sandbox::SandboxHandle;
use crate::verify::{self, VerifyOutcome};
use crate::workspace::WorkspaceError;
use proto::flow::{FailAction, KernelName, PromptsConfig};
use proto::pipeline::{Stage, StageReport};
use proto::report::ReportStatus;

/// A shipped default makes a stage part of every flow; a configured
/// prompt opts into stages whose policy is too project-specific for a
/// default, such as delivery.
fn active_stages(prompts: &PromptsConfig) -> impl Iterator<Item = Stage> + '_ {
    Stage::ALL.into_iter().filter(|stage| {
        contract::default_prompt(*stage).is_some() || prompts.get(stage.as_str()).is_some()
    })
}

/// The `K` in "K consecutive no-commit iterations pause as drift".
/// Fixed rather than a flow knob for v1: forgiving enough that one
/// stray empty pass never pauses a run, tight enough that a spinning
/// loop stops within three.
const DRIFT_LIMIT: u32 = 3;

/// The staged kernel. Stateless — everything a run needs arrives in its
/// [`KernelContext`].
#[derive(Debug, Clone, Copy, Default)]
pub struct PipelineKernel;

#[async_trait]
impl Kernel for PipelineKernel {
    async fn run(&self, ctx: KernelContext) -> Result<RunOutcome, KernelError> {
        let _active = ctx.budget_usage.activate();
        let (mut iteration, mut resumed) = match &ctx.replay {
            Some(events) => {
                let point = resume::ResumePoint::from_events(events)
                    .map_err(|error| KernelError::Resume(error.to_string()))?;
                (point.iteration, Some(point))
            }
            None => {
                ctx.events
                    .emit(RunEvent::RunStarted {
                        kernel: KernelName::Pipeline.as_str().into(),
                        agent: ctx.agent.name().into(),
                    })
                    .await?;
                (1, None)
            }
        };

        // The reports the plan stage of the next iteration reads —
        // remaining work and unfixed findings carrying forward. Empty
        // for the first iteration; nothing came before it.
        let mut prior: Vec<StageReport> = Vec::new();
        let mut plan_feedback: Vec<Feedback> = Vec::new();
        // The commit drift detection measures against. Sampled at the
        // loop boundary rather than threaded up from the stages: any
        // new commit — the engine's checkpoint or one the agent made
        // itself — is durable progress.
        let mut head = ctx.workspace.head().await?;
        let mut no_commit_iterations: u32 = 0;
        let mut timeout_failures: u32 = 0;
        loop {
            if let Some(budget) = ctx
                .budgets
                .exhausted(&ctx.budget_usage, iteration.saturating_sub(1))
            {
                return pause_for_budget(&ctx, iteration, budget, last_summary(&prior)).await;
            }
            let resume = resumed.take();
            if resume
                .as_ref()
                .is_none_or(|point| matches!(&point.position, resume::Position::NewIteration))
            {
                ctx.events
                    .emit(RunEvent::IterationStarted { iteration })
                    .await?;
            }
            let deadline = tokio::time::Instant::now() + ctx.budgets.iteration_timeout;
            let (completed, interrupted, human) = match resume {
                Some(resume::ResumePoint {
                    position: resume::Position::Interrupted { stage, completed },
                    human,
                    ..
                }) => (completed, stage, human),
                Some(resume::ResumePoint {
                    position: resume::Position::NewIteration,
                    human,
                    ..
                }) => (Vec::new(), Stage::Plan, human),
                None => (Vec::new(), Stage::Plan, None),
            };
            match run_iteration(
                &ctx,
                iteration,
                IterationStart {
                    prior: &prior,
                    completed,
                    interrupted,
                    plan_feedback: std::mem::take(&mut plan_feedback),
                    human,
                },
                deadline,
            )
            .await?
            {
                IterationEnd::Continue { pass, feedback } => {
                    ctx.events
                        .emit(RunEvent::IterationFinished {
                            iteration,
                            outcome: IterationOutcome::Completed,
                        })
                        .await?;
                    timeout_failures = 0;
                    let new_head = ctx.workspace.head().await?;
                    no_commit_iterations = if new_head == head {
                        no_commit_iterations + 1
                    } else {
                        0
                    };
                    head = new_head;
                    if let Some(budget) = ctx.budgets.exhausted(&ctx.budget_usage, iteration) {
                        return pause_for_budget(&ctx, iteration, budget, last_summary(&pass))
                            .await;
                    }
                    if no_commit_iterations >= DRIFT_LIMIT {
                        return conclude(
                            &ctx,
                            iteration,
                            RunOutcome::Paused(PauseReason::Drift),
                            Some(last_summary(&pass)),
                        )
                        .await;
                    }
                    prior = pass;
                    plan_feedback = feedback;
                    iteration += 1;
                }
                // Done, pause, and cancel stop mid-pipeline, so no
                // `IterationFinished` closes a pass that never
                // finished. Only a full pass or a hard failure emits
                // one.
                IterationEnd::Done => {
                    return conclude(&ctx, iteration, RunOutcome::Done, None).await;
                }
                IterationEnd::Pause { reason, summary } => {
                    return conclude(&ctx, iteration, RunOutcome::Paused(reason), Some(&summary))
                        .await;
                }
                IterationEnd::Fail => {
                    ctx.events
                        .emit(RunEvent::IterationFinished {
                            iteration,
                            outcome: IterationOutcome::Failed,
                        })
                        .await?;
                    return conclude(&ctx, iteration, RunOutcome::Failed, None).await;
                }
                IterationEnd::TimedOut => {
                    let timed_out_iteration = iteration;
                    ctx.events
                        .emit(RunEvent::IterationFinished {
                            iteration: timed_out_iteration,
                            outcome: IterationOutcome::TimedOut,
                        })
                        .await?;
                    timeout_failures += 1;
                    let summary = format!(
                        "iteration {iteration} timed out after {} seconds",
                        ctx.budgets.iteration_timeout.as_secs()
                    );
                    // Deliberately verify's knobs: `on_fail` is the
                    // flow's one word on how much failure to tolerate
                    // and what to do then. Only the label is its own —
                    // the human reading the pause must see a timeout,
                    // not a failed check.
                    if timeout_failures > ctx.verify.on_fail.retries {
                        let outcome = match ctx.verify.on_fail.then {
                            FailAction::Pause => RunOutcome::Paused(PauseReason::Timeout),
                            FailAction::Fail => RunOutcome::Failed,
                        };
                        return conclude(&ctx, timed_out_iteration, outcome, Some(&summary)).await;
                    }
                    iteration += 1;
                    plan_feedback = vec![Feedback::IterationTimedOut {
                        timeout: ctx.budgets.iteration_timeout,
                    }];
                }
                IterationEnd::Cancelled => {
                    return conclude(&ctx, iteration, RunOutcome::Cancelled, None).await;
                }
            }
        }
    }
}

struct IterationStart<'a> {
    prior: &'a [StageReport],
    completed: Vec<StageReport>,
    interrupted: Stage,
    plan_feedback: Vec<Feedback>,
    human: Option<HumanInput>,
}

/// How one iteration ended, as the run loop reads it. Every ending
/// lands here, so the loop emits each closing event and keeps its
/// books in exactly one place.
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
    Pause {
        reason: PauseReason,
        summary: String,
    },
    /// A stage or the skeptic produced no trustworthy report; the
    /// iteration counts as failed.
    Fail,
    /// The hard iteration deadline fired; the live sandbox has already
    /// been destroyed by its bracket.
    TimedOut,
    /// The run's cancel token fired; the iteration stops where it
    /// stands.
    Cancelled,
}

/// Drives one iteration through the stages. Plan opens a fresh unit,
/// so it reads the previous iteration's reports and the feedback the
/// loop carried in; every later stage reads what this iteration
/// produced before it. A resumed run's human input reaches the first
/// stage of this iteration and no other.
async fn run_iteration(
    ctx: &KernelContext,
    iteration: u32,
    start: IterationStart<'_>,
    deadline: tokio::time::Instant,
) -> Result<IterationEnd, KernelError> {
    let IterationStart {
        prior,
        mut completed,
        interrupted,
        mut plan_feedback,
        mut human,
    } = start;
    let mut stages = active_stages(&ctx.prompts).skip_while(|stage| *stage != interrupted);
    let Some(first) = stages.next() else {
        return Err(KernelError::Resume(format!(
            "stage `{}` is not active in this flow",
            interrupted.as_str()
        )));
    };
    for (index, stage) in std::iter::once(first).chain(stages).enumerate() {
        let handoff = if stage == Stage::Plan && completed.is_empty() {
            prior
        } else {
            completed.as_slice()
        };
        let feedback = if index == 0 && stage == Stage::Plan {
            std::mem::take(&mut plan_feedback)
        } else {
            Vec::new()
        };
        let stage_human = human.take();
        match execute_stage(
            ctx,
            iteration,
            stage,
            handoff,
            feedback,
            stage_human.as_ref(),
            deadline,
        )
        .await?
        {
            StageEnd::Advance(report) => completed.push(report),
            StageEnd::Done(claim) => {
                return Ok(
                    match skeptic::judge(ctx, iteration, &claim, deadline).await? {
                        Bracketed::Finished(skeptic::SkepticEnd::Unrefuted) => IterationEnd::Done,
                        Bracketed::Finished(skeptic::SkepticEnd::Refuted(findings)) => {
                            // A refuted claim still advanced the work, so
                            // the next plan reads it alongside the
                            // findings.
                            completed.push(claim);
                            IterationEnd::Continue {
                                pass: completed,
                                feedback: vec![Feedback::SkepticRefuted { findings }],
                            }
                        }
                        Bracketed::Finished(skeptic::SkepticEnd::Failed) => IterationEnd::Fail,
                        Bracketed::Cancelled => IterationEnd::Cancelled,
                        Bracketed::TimedOut => IterationEnd::TimedOut,
                    },
                );
            }
            StageEnd::Pause { reason, summary } => {
                return Ok(IterationEnd::Pause { reason, summary });
            }
            StageEnd::Fail => return Ok(IterationEnd::Fail),
            StageEnd::TimedOut => return Ok(IterationEnd::TimedOut),
            StageEnd::Cancelled => return Ok(IterationEnd::Cancelled),
        }
    }
    Ok(IterationEnd::Continue {
        pass: completed,
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
    Pause {
        reason: PauseReason,
        summary: String,
    },
    /// The stage produced no trustworthy report — a crashed agent or a
    /// report still malformed after its one repair.
    Fail,
    /// The hard iteration deadline fired mid-stage; the bracket has
    /// already destroyed the sandbox it interrupted.
    TimedOut,
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
    human: Option<&HumanInput>,
    deadline: tokio::time::Instant,
) -> Result<StageEnd, KernelError> {
    let mut verify_failures: u32 = 0;
    loop {
        let drive =
            match drive_stage(ctx, iteration, stage, handoff, &feedback, human, deadline).await? {
                Bracketed::Finished(drive) => drive,
                Bracketed::Cancelled => return Ok(StageEnd::Cancelled),
                Bracketed::TimedOut => return Ok(StageEnd::TimedOut),
            };
        let StageDrive::Reported { report, verify } = drive else {
            return Ok(StageEnd::Fail);
        };

        if let VerifyOutcome::Failed { command, output } = verify {
            verify_failures += 1;
            if verify_failures > ctx.verify.on_fail.retries {
                return Ok(match ctx.verify.on_fail.then {
                    FailAction::Pause => StageEnd::Pause {
                        reason: PauseReason::VerifyFailed,
                        summary: report.summary().into(),
                    },
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
            ReportStatus::Blocked => StageEnd::Pause {
                reason: PauseReason::Blocked,
                summary: report.summary().into(),
            },
            ReportStatus::NeedsInput => StageEnd::Pause {
                reason: PauseReason::AwaitingHuman,
                summary: report.summary().into(),
            },
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
    human: Option<&HumanInput>,
    deadline: tokio::time::Instant,
) -> Result<Bracketed<StageDrive>, KernelError> {
    invocation::in_fresh_sandbox_until(ctx, Some(deadline), async |sandbox| {
        let domain_prompt = resolve_prompt(ctx, sandbox, stage).await?;
        let prompt = frame::compose(&frame::Frame {
            stage,
            handoff,
            feedback,
            human,
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
/// deliver publishes; neither mutates the workspace by kernel policy.
fn is_mutating(stage: Stage) -> bool {
    matches!(stage, Stage::Implement | Stage::Review | Stage::Simplify)
}

/// Mutating progress is always checked; a `done` claim from any stage
/// also needs a current green verdict before it can reach the skeptic.
fn runs_verify(stage: Stage, status: ReportStatus) -> bool {
    status == ReportStatus::Done || (is_mutating(stage) && status == ReportStatus::Continue)
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

    #[test]
    fn only_the_workspace_editing_stages_mutate() {
        assert!(!is_mutating(Stage::Plan));
        assert!(is_mutating(Stage::Implement));
        assert!(is_mutating(Stage::Review));
        assert!(is_mutating(Stage::Simplify));
        assert!(!is_mutating(Stage::Deliver));
    }

    /// Verify judges mutating progress and every completion claim: a
    /// mutating stage claiming `continue`, or a `done` claim from any
    /// stage. A pausing status skips its checks, whatever the stage.
    #[test]
    fn verify_gates_mutating_progress_and_every_done_claim() {
        assert!(runs_verify(Stage::Implement, ReportStatus::Continue));
        assert!(runs_verify(Stage::Implement, ReportStatus::Done));
        assert!(!runs_verify(Stage::Implement, ReportStatus::Blocked));
        assert!(!runs_verify(Stage::Implement, ReportStatus::NeedsInput));
        assert!(!runs_verify(Stage::Plan, ReportStatus::Continue));
        assert!(runs_verify(Stage::Plan, ReportStatus::Done));
    }
}
