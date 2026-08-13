//! The fanout kernel: plan opaque independent units, launch one child
//! pipeline per unit, and account for the whole batch before planning
//! again. Parallelism stays at the run boundary.

mod contract;
mod frame;

use async_trait::async_trait;
use futures_util::stream::{FuturesUnordered, StreamExt};
use proto::fanout::PlanReport;
use proto::flow::KernelName;
use proto::report::ReportStatus;

use crate::budget::TokenUsage;
use crate::ending::{Ending, conclude, pause_for_budget};
use crate::event::{EventEnvelope, IterationOutcome, RunEvent};
use crate::invocation::{self, Bracketed};
use crate::kernel::{Kernel, KernelContext, KernelError};
use crate::report::{self, Disposition};
use crate::run::{PauseReason, RunId, RunOutcome, RunState};
use crate::run_spawner::{ChildRunSpec, RunSpawnerError};
use crate::sandbox::SandboxHandle;
use crate::skeptic::{self, SkepticEnd};
use crate::verify::{self, VerifyOutcome};

#[derive(Debug, Clone, Copy, Default)]
pub struct FanoutKernel;

#[async_trait]
impl Kernel for FanoutKernel {
    async fn run(&self, ctx: KernelContext) -> Result<RunOutcome, KernelError> {
        let _active = ctx.budget_usage.activate();
        let (mut iteration, mut child_iterations) = match &ctx.replay {
            Some(events) => resume_progress(events)?,
            None => {
                ctx.events
                    .emit(RunEvent::RunStarted {
                        kernel: KernelName::Fanout.as_str().into(),
                        agent: ctx.agent.name().into(),
                    })
                    .await?;
                (1, 0)
            }
        };
        let mut feedback = Vec::new();
        loop {
            if let Some(budget) = ctx.budgets.exhausted(&ctx.budget_usage, child_iterations) {
                return pause_for_budget(&ctx, iteration.saturating_sub(1), budget, None).await;
            }
            ctx.events
                .emit(RunEvent::IterationStarted { iteration })
                .await?;
            match run_iteration(&ctx, iteration, &feedback).await? {
                IterationEnd::Continue { summary, children } => {
                    ctx.events
                        .emit(RunEvent::IterationFinished {
                            iteration,
                            outcome: IterationOutcome::Completed,
                        })
                        .await?;
                    child_iterations += children;
                    if let Some(budget) = ctx.budgets.exhausted(&ctx.budget_usage, child_iterations)
                    {
                        return pause_for_budget(&ctx, iteration, budget, Some(&summary)).await;
                    }
                    feedback.clear();
                    iteration += 1;
                }
                IterationEnd::Refuted { findings } => {
                    ctx.events
                        .emit(RunEvent::IterationFinished {
                            iteration,
                            outcome: IterationOutcome::Completed,
                        })
                        .await?;
                    feedback = findings;
                    iteration += 1;
                }
                IterationEnd::Done => {
                    return conclude(&ctx, iteration, Ending::Done).await;
                }
                IterationEnd::Pause { reason, summary } => {
                    return conclude(
                        &ctx,
                        iteration,
                        Ending::Paused {
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
                    return conclude(&ctx, iteration, Ending::Failed).await;
                }
                IterationEnd::PlanTimedOut => {
                    ctx.events
                        .emit(RunEvent::IterationFinished {
                            iteration,
                            outcome: IterationOutcome::TimedOut,
                        })
                        .await?;
                    return conclude(
                        &ctx,
                        iteration,
                        Ending::Paused {
                            reason: PauseReason::Timeout,
                            summary: Some("fanout plan timed out"),
                        },
                    )
                    .await;
                }
                IterationEnd::Cancelled => {
                    return conclude(&ctx, iteration, Ending::Cancelled).await;
                }
            }
        }
    }
}

/// How one planning iteration ended, as the run loop reads it. Every
/// ending lands here, so the loop emits each closing event and keeps
/// its books in exactly one place.
enum IterationEnd {
    /// The plan named ready units, every spawned child settled, and
    /// the run goes on. Carries the batch's completed child
    /// iterations and the plan's summary for a budget pause landing
    /// right after.
    Continue { summary: String, children: u32 },
    /// A `done` claim the skeptic refuted; its findings feed the next
    /// plan.
    Refuted { findings: Vec<String> },
    /// A `done` claim cleared its verify gate and survived the
    /// skeptic — the run is complete.
    Done,
    /// The run pauses now — a pausing status, a failed verify gate on
    /// a claim, or a timed-out skeptic.
    Pause {
        reason: PauseReason,
        summary: String,
    },
    /// The plan or the skeptic produced no trustworthy report; the
    /// iteration counts as failed.
    Fail,
    /// The iteration deadline fired before the plan reported; the
    /// sandbox has already been destroyed by its bracket.
    PlanTimedOut,
    /// The run's cancel token fired; the run ends where it stands.
    Cancelled,
}

/// Drives one planning pass: plan in a fresh sandbox, then either
/// spawn and settle the planned batch or put a completion claim to
/// the skeptic.
async fn run_iteration(
    ctx: &KernelContext,
    iteration: u32,
    feedback: &[String],
) -> Result<IterationEnd, KernelError> {
    let deadline = tokio::time::Instant::now() + ctx.budgets.iteration_timeout;
    let Planned { report, verify } = match plan(ctx, iteration, feedback, deadline).await? {
        Bracketed::Finished(Some(planned)) => planned,
        Bracketed::Finished(None) => return Ok(IterationEnd::Fail),
        Bracketed::Cancelled => return Ok(IterationEnd::Cancelled),
        Bracketed::TimedOut => return Ok(IterationEnd::PlanTimedOut),
    };
    match report::disposition(report.status) {
        Disposition::Claimed => {
            if matches!(verify, VerifyOutcome::Failed { .. }) {
                return Ok(IterationEnd::Pause {
                    reason: PauseReason::VerifyFailed,
                    summary: report.summary,
                });
            }
            Ok(match judge_done(ctx, iteration, &report, deadline).await? {
                Bracketed::Finished(SkepticEnd::Unrefuted) => IterationEnd::Done,
                Bracketed::Finished(SkepticEnd::Refuted(findings)) => {
                    IterationEnd::Refuted { findings }
                }
                Bracketed::Finished(SkepticEnd::Failed) => IterationEnd::Fail,
                Bracketed::Cancelled => IterationEnd::Cancelled,
                Bracketed::TimedOut => IterationEnd::Pause {
                    reason: PauseReason::Timeout,
                    summary: "fanout skeptic timed out".into(),
                },
            })
        }
        Disposition::Pause(reason) => Ok(IterationEnd::Pause {
            reason,
            summary: report.summary,
        }),
        Disposition::Advance => {
            let children = spawn_batch(ctx, report.units).await?;
            let settled = watch_batch(ctx, children).await?;
            Ok(IterationEnd::Continue {
                summary: report.summary,
                children: settled,
            })
        }
    }
}

async fn plan(
    ctx: &KernelContext,
    iteration: u32,
    feedback: &[String],
    deadline: tokio::time::Instant,
) -> Result<Bracketed<Option<Planned>>, KernelError> {
    invocation::in_fresh_sandbox_until(ctx, Some(deadline), async |sandbox| {
        let domain = resolve_prompt(ctx, sandbox).await?;
        let prompt = frame::plan(&domain, feedback);
        ctx.events
            .emit(RunEvent::StageStarted {
                iteration,
                stage: "plan".into(),
            })
            .await?;
        let report =
            invocation::invoke_to_report(ctx, iteration, sandbox, &prompt, &contract::PlanContract)
                .await?;
        let Some(report) = report else {
            return Ok(None);
        };
        ctx.events
            .emit(RunEvent::StageReported {
                iteration,
                stage: "plan".into(),
                report: serde_json::to_value(&report).expect("fanout reports serialize"),
            })
            .await?;
        let verify = if report.status == ReportStatus::Done {
            verify::run_checks(ctx, sandbox, iteration).await?
        } else {
            VerifyOutcome::Skipped
        };
        Ok(Some(Planned { report, verify }))
    })
    .await
}

struct Planned {
    report: PlanReport,
    verify: VerifyOutcome,
}

/// The domain prompt: the flow's `plan` override, or the shipped
/// default.
async fn resolve_prompt(
    ctx: &KernelContext,
    sandbox: &SandboxHandle,
) -> Result<String, KernelError> {
    Ok(invocation::read_prompt_override(ctx, sandbox, "plan")
        .await?
        .unwrap_or_else(|| contract::default_prompt().to_owned()))
}

async fn judge_done(
    ctx: &KernelContext,
    iteration: u32,
    claim: &PlanReport,
    deadline: tokio::time::Instant,
) -> Result<Bracketed<SkepticEnd>, KernelError> {
    invocation::in_fresh_sandbox_until(ctx, Some(deadline), async |sandbox| {
        let domain = resolve_prompt(ctx, sandbox).await?;
        let prompt = frame::skeptic(claim, &domain);
        skeptic::evaluate(ctx, iteration, sandbox, &prompt).await
    })
    .await
}

async fn spawn_batch(ctx: &KernelContext, units: Vec<String>) -> Result<Vec<RunId>, KernelError> {
    let mut children = Vec::with_capacity(units.len());
    for scope in units {
        let run_id = ctx
            .run_spawner
            .spawn(ChildRunSpec {
                scope: scope.clone(),
            })
            .await?;
        ctx.events
            .emit(RunEvent::ChildRunStarted {
                child_run_id: run_id.to_string(),
                scope,
            })
            .await?;
        children.push(run_id);
    }
    Ok(children)
}

async fn watch_batch(ctx: &KernelContext, children: Vec<RunId>) -> Result<u32, KernelError> {
    let mut watches = FuturesUnordered::new();
    for run_id in children {
        let spawner = ctx.run_spawner.clone();
        watches.push(async move {
            let history = spawner.watch(&run_id).await?;
            Ok::<_, RunSpawnerError>((run_id, history))
        });
    }

    // One failed watch must not abandon its siblings: they keep
    // running either way, and only draining them lands their
    // outcomes and token usage in the parent's account. The first
    // failure is kept and raised once the whole batch has settled.
    let mut failure: Option<RunSpawnerError> = None;
    let mut completed_iterations: u32 = 0;
    while let Some(watched) = watches.next().await {
        let (run_id, history) = match watched {
            Ok(watched) => watched,
            Err(error) => {
                failure.get_or_insert(error);
                continue;
            }
        };
        let mut child_usage = None;
        let mut child_iterations: u32 = 0;
        for envelope in &history {
            match envelope.event {
                RunEvent::TokensUsed { usage, .. } => {
                    ctx.budget_usage.record_tokens(usage);
                    *child_usage.get_or_insert_with(TokenUsage::default) += usage;
                }
                RunEvent::IterationFinished { .. } => {
                    child_iterations = child_iterations.saturating_add(1);
                    completed_iterations = completed_iterations.saturating_add(1);
                }
                _ => {}
            }
        }
        let settled_state = history
            .iter()
            .rev()
            .find_map(|envelope| match envelope.event {
                RunEvent::StateChanged { state } if state != RunState::Running => Some(state),
                _ => None,
            });
        let Some(state) = settled_state else {
            failure.get_or_insert(RunSpawnerError(format!(
                "child {run_id} settled without a state event"
            )));
            continue;
        };
        ctx.events
            .emit(RunEvent::ChildRunFinished {
                child_run_id: run_id.to_string(),
                state,
                iterations: child_iterations,
                usage: child_usage,
            })
            .await?;
    }
    match failure {
        Some(error) => Err(error.into()),
        None => Ok(completed_iterations),
    }
}

/// Rebuilds `(next iteration, settled child iterations)` from a
/// replayed log. Only settled children are counted, which is sound
/// because every pause this kernel can reach happens after
/// `watch_batch` drained its batch: a cleanly paused log never holds
/// a `child_run_started` without its `child_run_finished`. A log
/// torn by a crash can — but a detached record refuses resume today,
/// so such a log never replays; the day detached resume lands, this
/// must learn to re-watch those orphans instead of replanning over
/// them.
fn resume_progress(events: &[EventEnvelope]) -> Result<(u32, u32), KernelError> {
    if !events
        .iter()
        .any(|envelope| matches!(envelope.event, RunEvent::RunResumed { .. }))
    {
        return Err(KernelError::Resume(
            "fanout replay has no resume command".into(),
        ));
    }
    let last_finished = events.iter().filter_map(|envelope| match envelope.event {
        RunEvent::IterationFinished { iteration, .. } => Some(iteration),
        _ => None,
    });
    let iteration = last_finished.max().unwrap_or(0).saturating_add(1);
    let child_iterations = events
        .iter()
        .filter_map(|envelope| match envelope.event {
            RunEvent::ChildRunFinished { iterations, .. } => Some(iterations),
            _ => None,
        })
        .fold(0_u32, u32::saturating_add);
    Ok((iteration, child_iterations))
}
