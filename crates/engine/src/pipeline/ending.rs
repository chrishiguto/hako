//! The pipeline's one exit path. A pause becomes publishable only
//! after the workspace is durable; every ending then records its state,
//! and pauses make a best-effort notification.

use proto::BudgetKind;
use proto::pipeline::StageReport;

use crate::event::RunEvent;
use crate::kernel::{KernelContext, KernelError};
use crate::notify::Notification;
use crate::run::{PauseReason, RunOutcome};

pub(super) fn last_summary(pass: &[StageReport]) -> &str {
    pass.last()
        .map_or("budget exhausted", |report| report.summary())
}

pub(super) async fn pause_for_budget(
    ctx: &KernelContext,
    iteration: u32,
    budget: BudgetKind,
    summary: &str,
) -> Result<RunOutcome, KernelError> {
    ctx.events
        .emit(RunEvent::BudgetExhausted { budget })
        .await?;
    conclude(
        ctx,
        iteration,
        RunOutcome::Paused(PauseReason::Budget),
        Some(summary),
    )
    .await
}

/// Lands every run ending through the same event path. Pauses sweep
/// the workspace first, so publishing the parked state proves there
/// is no uncommitted work left behind.
pub(super) async fn conclude(
    ctx: &KernelContext,
    iteration: u32,
    outcome: RunOutcome,
    summary: Option<&str>,
) -> Result<RunOutcome, KernelError> {
    if matches!(outcome, RunOutcome::Paused(_))
        && let Some(commit) = ctx.workspace.checkpoint("hako: pause").await?
    {
        ctx.events
            .emit(RunEvent::WorkspaceCheckpointed { iteration, commit })
            .await?;
    }
    ctx.events
        .emit(RunEvent::StateChanged {
            state: outcome.into(),
        })
        .await?;
    if let RunOutcome::Paused(reason) = outcome {
        // An unreachable webhook must not turn a clean pause into a
        // failed run: the human loses the ping, never the work.
        let _ = ctx
            .notifier
            .notify(&Notification {
                run_id: ctx.run_id.clone(),
                reason,
                summary: summary.unwrap_or("run paused").into(),
            })
            .await;
    }
    Ok(outcome)
}
