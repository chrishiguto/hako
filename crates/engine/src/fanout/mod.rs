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

use crate::ending::{conclude, pause_for_budget};
use crate::event::{IterationOutcome, RunEvent};
use crate::invocation::{self, Bracketed};
use crate::kernel::{Kernel, KernelContext, KernelError};
use crate::run::{PauseReason, RunOutcome, RunState};
use crate::run_spawner::ChildRunSpec;
use crate::sandbox::SandboxHandle;
use crate::skeptic::{self, SkepticEnd};
use crate::verify::{self, VerifyOutcome};
use crate::workspace::WorkspaceError;

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
                return pause_for_budget(
                    &ctx,
                    iteration.saturating_sub(1),
                    budget,
                    "fanout budget exhausted",
                )
                .await;
            }
            ctx.events
                .emit(RunEvent::IterationStarted { iteration })
                .await?;
            let deadline = tokio::time::Instant::now() + ctx.budgets.iteration_timeout;
            let planned = match plan(&ctx, iteration, &feedback, deadline).await? {
                Bracketed::Finished(Some(planned)) => planned,
                Bracketed::Finished(None) => {
                    ctx.events
                        .emit(RunEvent::IterationFinished {
                            iteration,
                            outcome: IterationOutcome::Failed,
                        })
                        .await?;
                    return conclude(&ctx, iteration, RunOutcome::Failed, None).await;
                }
                Bracketed::Cancelled => {
                    return conclude(&ctx, iteration, RunOutcome::Cancelled, None).await;
                }
                Bracketed::TimedOut => {
                    ctx.events
                        .emit(RunEvent::IterationFinished {
                            iteration,
                            outcome: IterationOutcome::TimedOut,
                        })
                        .await?;
                    return conclude(
                        &ctx,
                        iteration,
                        RunOutcome::Paused(PauseReason::Timeout),
                        Some("fanout plan timed out"),
                    )
                    .await;
                }
            };
            let Planned { report, verify } = planned;

            match report.status {
                ReportStatus::Done => {
                    if matches!(verify, VerifyOutcome::Failed { .. }) {
                        return conclude(
                            &ctx,
                            iteration,
                            RunOutcome::Paused(PauseReason::VerifyFailed),
                            Some(&report.summary),
                        )
                        .await;
                    }
                    match judge_done(&ctx, iteration, &report, deadline).await? {
                        Bracketed::Finished(SkepticEnd::Unrefuted) => {
                            return conclude(
                                &ctx,
                                iteration,
                                RunOutcome::Done,
                                Some(&report.summary),
                            )
                            .await;
                        }
                        Bracketed::Finished(SkepticEnd::Refuted(findings)) => {
                            feedback = findings;
                            ctx.events
                                .emit(RunEvent::IterationFinished {
                                    iteration,
                                    outcome: IterationOutcome::Completed,
                                })
                                .await?;
                            iteration += 1;
                            continue;
                        }
                        Bracketed::Finished(SkepticEnd::Failed) => {
                            ctx.events
                                .emit(RunEvent::IterationFinished {
                                    iteration,
                                    outcome: IterationOutcome::Failed,
                                })
                                .await?;
                            return conclude(&ctx, iteration, RunOutcome::Failed, None).await;
                        }
                        Bracketed::Cancelled => {
                            return conclude(&ctx, iteration, RunOutcome::Cancelled, None).await;
                        }
                        Bracketed::TimedOut => {
                            return conclude(
                                &ctx,
                                iteration,
                                RunOutcome::Paused(PauseReason::Timeout),
                                Some("fanout skeptic timed out"),
                            )
                            .await;
                        }
                    }
                }
                ReportStatus::Blocked => {
                    return conclude(
                        &ctx,
                        iteration,
                        RunOutcome::Paused(PauseReason::Blocked),
                        Some(&report.summary),
                    )
                    .await;
                }
                ReportStatus::NeedsInput => {
                    return conclude(
                        &ctx,
                        iteration,
                        RunOutcome::Paused(PauseReason::AwaitingHuman),
                        Some(&report.summary),
                    )
                    .await;
                }
                ReportStatus::Continue => {}
            }

            let children = spawn_batch(&ctx, report.units).await?;
            child_iterations += watch_batch(&ctx, children).await?;
            ctx.events
                .emit(RunEvent::IterationFinished {
                    iteration,
                    outcome: IterationOutcome::Completed,
                })
                .await?;
            if let Some(budget) = ctx.budgets.exhausted(&ctx.budget_usage, child_iterations) {
                return pause_for_budget(&ctx, iteration, budget, &report.summary).await;
            }
            feedback.clear();
            iteration += 1;
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

async fn resolve_prompt(
    ctx: &KernelContext,
    sandbox: &SandboxHandle,
) -> Result<String, KernelError> {
    match ctx.prompts.get("plan") {
        Some(path) => {
            let guest_path = ctx.workspace.guest_path(path)?;
            let raw = ctx.sandbox.get_file(sandbox, &guest_path).await?;
            String::from_utf8(raw).map_err(|error| {
                KernelError::Workspace(WorkspaceError(format!(
                    "prompt `{path}` is not UTF-8: {error}"
                )))
            })
        }
        None => Ok(contract::default_prompt().to_owned()),
    }
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

async fn spawn_batch(
    ctx: &KernelContext,
    units: Vec<String>,
) -> Result<Vec<crate::run::RunId>, KernelError> {
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

async fn watch_batch(
    ctx: &KernelContext,
    children: Vec<crate::run::RunId>,
) -> Result<u32, KernelError> {
    let mut watches = FuturesUnordered::new();
    for run_id in children {
        let spawner = ctx.run_spawner.clone();
        watches.push(async move {
            let history = spawner.watch(&run_id).await?;
            Ok::<_, crate::run_spawner::RunSpawnerError>((run_id, history))
        });
    }

    let mut completed_iterations: u32 = 0;
    while let Some(watched) = watches.next().await {
        let (run_id, history) = watched?;
        let mut child_usage = None;
        let mut child_iterations: u32 = 0;
        for envelope in &history {
            match envelope.event {
                RunEvent::TokensUsed { usage, .. } => {
                    ctx.budget_usage.record_tokens(usage);
                    let total = child_usage.get_or_insert_with(crate::budget::TokenUsage::default);
                    total.input = total.input.saturating_add(usage.input);
                    total.output = total.output.saturating_add(usage.output);
                }
                RunEvent::IterationFinished { .. } => {
                    child_iterations = child_iterations.saturating_add(1);
                    completed_iterations = completed_iterations.saturating_add(1);
                }
                _ => {}
            }
        }
        let state = history
            .iter()
            .rev()
            .find_map(|envelope| match envelope.event {
                RunEvent::StateChanged { state } if state != RunState::Running => Some(state),
                _ => None,
            })
            .ok_or_else(|| {
                crate::run_spawner::RunSpawnerError(format!(
                    "child {run_id} settled without a state event"
                ))
            })?;
        ctx.events
            .emit(RunEvent::ChildRunFinished {
                child_run_id: run_id.to_string(),
                state,
                iterations: child_iterations,
                usage: child_usage,
            })
            .await?;
    }
    Ok(completed_iterations)
}

fn resume_progress(events: &[crate::event::EventEnvelope]) -> Result<(u32, u32), KernelError> {
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
