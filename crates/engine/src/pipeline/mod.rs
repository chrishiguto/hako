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
pub(crate) mod frame;
mod resume;
pub(crate) mod skeptic;

use async_trait::async_trait;

use crate::ending::{Ending, conclude, pause_for_budget};
use crate::event::{IterationOutcome, RunEvent};
use crate::invocation::{self, Bracketed};
use crate::kernel::{Kernel, KernelContext, KernelError};
use crate::preamble::{Feedback, HumanInput};
use crate::report::{self, Disposition};
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
        let (mut iteration, mut resumed, mut notification_summary) = match &ctx.replay {
            Some(events) => {
                let mut point = resume::ResumePoint::from_events(events)
                    .map_err(|error| KernelError::Resume(error.to_string()))?;
                // Checked here, where the point is born, so the loop
                // below can trust every entry stage it is handed.
                if !active_stages(&ctx.prompts).any(|stage| stage == point.stage) {
                    return Err(KernelError::Resume(format!(
                        "stage `{}` is not active in this flow",
                        point.stage.as_str()
                    )));
                }
                let notification_summary = point.notification_summary.take();
                (point.iteration, Some(point), notification_summary)
            }
            None => {
                ctx.events
                    .emit(RunEvent::RunStarted {
                        kernel: KernelName::Pipeline.as_str().into(),
                        agent: ctx.agent.name().into(),
                    })
                    .await?;
                (1, None, None)
            }
        };

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
                // Work a loop-top pause parks belongs to the last
                // iteration the log announced: the interrupted one
                // when re-entering mid-pass, otherwise the one that
                // came before this.
                let announced = match &resumed {
                    Some(point) if !point.starts_iteration => iteration,
                    _ => iteration.saturating_sub(1),
                };
                return pause_for_budget(&ctx, announced, budget, notification_summary.as_deref())
                    .await;
            }
            let resume = resumed.take();
            if resume.as_ref().is_none_or(|point| point.starts_iteration) {
                ctx.events
                    .emit(RunEvent::IterationStarted { iteration })
                    .await?;
            }
            let deadline = tokio::time::Instant::now() + ctx.budgets.iteration_timeout;
            match run_iteration(
                &ctx,
                iteration,
                resume,
                std::mem::take(&mut plan_feedback),
                deadline,
            )
            .await?
            {
                IterationEnd::Continue {
                    notification_summary: summary,
                    feedback,
                } => {
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
                    notification_summary = summary;
                    if let Some(budget) = ctx.budgets.exhausted(&ctx.budget_usage, iteration) {
                        return pause_for_budget(
                            &ctx,
                            iteration,
                            budget,
                            notification_summary.as_deref(),
                        )
                        .await;
                    }
                    if no_commit_iterations >= DRIFT_LIMIT {
                        return conclude(
                            &ctx,
                            Ending::Paused {
                                iteration,
                                reason: PauseReason::Drift,
                                summary: notification_summary.as_deref(),
                            },
                        )
                        .await;
                    }
                    plan_feedback = feedback;
                    iteration += 1;
                }
                // Done, pause, and cancel stop mid-pipeline, so no
                // `IterationFinished` closes a pass that never
                // finished. Only a full pass or a hard failure emits
                // one.
                IterationEnd::Done => {
                    return conclude(&ctx, Ending::Done).await;
                }
                IterationEnd::Pause { reason, summary } => {
                    return conclude(
                        &ctx,
                        Ending::Paused {
                            iteration,
                            reason,
                            summary: Some(&summary),
                        },
                    )
                    .await;
                }
                IterationEnd::Fail => {
                    ctx.events
                        .emit(RunEvent::IterationFinished {
                            iteration,
                            outcome: IterationOutcome::Failed,
                        })
                        .await?;
                    return conclude(&ctx, Ending::Failed).await;
                }
                IterationEnd::TimedOut {
                    notification_summary: latest,
                } => {
                    ctx.events
                        .emit(RunEvent::IterationFinished {
                            iteration,
                            outcome: IterationOutcome::TimedOut,
                        })
                        .await?;
                    // A pass that reported nothing leaves the summary
                    // alone: the log's latest eligible Report is still
                    // the one already held.
                    if let Some(latest) = latest {
                        notification_summary = Some(latest);
                    }
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
                        let ending = match ctx.verify.on_fail.then {
                            FailAction::Pause => Ending::Paused {
                                iteration,
                                reason: PauseReason::Timeout,
                                summary: Some(&summary),
                            },
                            FailAction::Fail => Ending::Failed,
                        };
                        return conclude(&ctx, ending).await;
                    }
                    iteration += 1;
                    plan_feedback = vec![Feedback::IterationTimedOut {
                        timeout: ctx.budgets.iteration_timeout,
                    }];
                }
                IterationEnd::Cancelled => {
                    return conclude(&ctx, Ending::Cancelled).await;
                }
            }
        }
    }
}

/// How one iteration ended, as the run loop reads it. Every ending
/// lands here, so the loop emits each closing event and keeps its
/// books in exactly one place.
enum IterationEnd {
    /// The iteration counts as verified progress and the run goes on:
    /// a full pass, or a `done` claim the skeptic refuted. Carries the
    /// latest eligible summary for operator-facing pauses and any
    /// explicitly scoped feedback the next plan must answer. A
    /// refuted done claim carries no notification summary.
    Continue {
        notification_summary: Option<String>,
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
    /// been destroyed by its bracket. Carries the latest Report the
    /// interrupted pass put on the log — `None` when it never reported
    /// — so the loop's summary keeps matching what a replay of this
    /// log would rebuild.
    TimedOut {
        notification_summary: Option<String>,
    },
    /// The run's cancel token fired; the iteration stops where it
    /// stands.
    Cancelled,
}

/// Drives one iteration through the stages, entering fresh at plan or
/// exactly where the replayed pause interrupted it — the run loop has
/// already checked that stage is active in this flow. Plan opens a
/// fresh unit from the Workspace and the explicitly scoped feedback
/// the loop carried in; every later stage reads what this iteration
/// produced before it. A resumed run's human input reaches the first
/// stage of this iteration and no other.
async fn run_iteration(
    ctx: &KernelContext,
    iteration: u32,
    resume: Option<resume::ResumePoint>,
    mut plan_feedback: Vec<Feedback>,
    deadline: tokio::time::Instant,
) -> Result<IterationEnd, KernelError> {
    let (interrupted, mut completed, mut human) = resume
        .map_or((Stage::Plan, Vec::new(), None), |point| {
            (point.stage, point.completed, point.human)
        });
    // What this pass has put on the log so far — not `completed`,
    // whose resume-carried reports predate the summary the loop
    // already holds. A timeout hands this back so the loop's summary
    // keeps matching the log.
    let mut latest_emitted: Option<String> = None;
    for stage in active_stages(&ctx.prompts).skip_while(|stage| *stage != interrupted) {
        let feedback = if stage == Stage::Plan {
            std::mem::take(&mut plan_feedback)
        } else {
            Vec::new()
        };
        let stage_human = human.take();
        match execute_stage(
            ctx,
            iteration,
            stage,
            &completed,
            feedback,
            stage_human.as_ref(),
            deadline,
        )
        .await?
        {
            StageEnd::Advance(report) => {
                latest_emitted = Some(report.summary().to_owned());
                completed.push(report);
            }
            StageEnd::Done(claim) => {
                return Ok(
                    match skeptic::judge(ctx, iteration, &claim, deadline).await? {
                        Bracketed::Finished(skeptic::SkepticEnd::Unrefuted) => IterationEnd::Done,
                        Bracketed::Finished(skeptic::SkepticEnd::Refuted(findings)) => {
                            // The claim is already on the log with the
                            // rest of the pass; only the skeptic's
                            // findings reach the next plan.
                            IterationEnd::Continue {
                                notification_summary: None,
                                feedback: vec![Feedback::SkepticRefuted { findings }],
                            }
                        }
                        Bracketed::Finished(skeptic::SkepticEnd::Failed) => IterationEnd::Fail,
                        Bracketed::Cancelled => IterationEnd::Cancelled,
                        // The unjudged claim stays eligible — only a
                        // refutation retires a Report.
                        Bracketed::TimedOut => IterationEnd::TimedOut {
                            notification_summary: Some(claim.summary().to_owned()),
                        },
                    },
                );
            }
            StageEnd::Pause { reason, summary } => {
                return Ok(IterationEnd::Pause { reason, summary });
            }
            StageEnd::Fail => return Ok(IterationEnd::Fail),
            StageEnd::TimedOut { last_reported } => {
                return Ok(IterationEnd::TimedOut {
                    notification_summary: last_reported.or(latest_emitted),
                });
            }
            StageEnd::Cancelled => return Ok(IterationEnd::Cancelled),
        }
    }
    // `None` is the refuted-claim case above; on this path every stage
    // advanced, so `completed` always holds a last report.
    Ok(IterationEnd::Continue {
        notification_summary: completed.last().map(|report| report.summary().to_owned()),
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
    /// already destroyed the sandbox it interrupted. Carries the
    /// summary of the last report this stage's attempts emitted — a
    /// verify retry's failed predecessor is still on the log, and the
    /// loop's summary must keep matching the log.
    TimedOut { last_reported: Option<String> },
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
    let mut last_reported: Option<String> = None;
    loop {
        let drive =
            match drive_stage(ctx, iteration, stage, handoff, &feedback, human, deadline).await? {
                Bracketed::Finished(drive) => drive,
                Bracketed::Cancelled => return Ok(StageEnd::Cancelled),
                Bracketed::TimedOut => return Ok(StageEnd::TimedOut { last_reported }),
            };
        let StageDrive::Reported { report, verify } = drive else {
            return Ok(StageEnd::Fail);
        };
        last_reported = Some(report.summary().to_owned());

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
        return Ok(match report::disposition(report.status()) {
            Disposition::Advance => StageEnd::Advance(report),
            Disposition::Claimed => StageEnd::Done(report),
            Disposition::Pause(reason) => StageEnd::Pause {
                reason,
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
        // A parent's assignment reaches the unit-choosing stage only.
        let scope = if stage == Stage::Plan {
            ctx.scope.as_deref()
        } else {
            None
        };
        let prompt = frame::compose(&frame::Frame {
            stage,
            scope,
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

/// The stage's domain prompt: the flow's override for the slot, or
/// the kernel-shipped default when the slot is unset.
async fn resolve_prompt(
    ctx: &KernelContext,
    sandbox: &SandboxHandle,
    stage: Stage,
) -> Result<String, KernelError> {
    match invocation::read_prompt_override(ctx, sandbox, stage.as_str()).await? {
        Some(prompt) => Ok(prompt),
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
