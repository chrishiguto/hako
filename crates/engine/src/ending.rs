//! The shared run-ending path. Pauses become visible only after the
//! workspace is durable, and every pause makes a best-effort
//! notification.

use proto::BudgetKind;

use crate::event::RunEvent;
use crate::kernel::{KernelContext, KernelError};
use crate::notify::Notification;
use crate::run::{PauseReason, RunOutcome};

pub(crate) async fn pause_for_budget(
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

pub(crate) async fn conclude(
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
        // Notification delivery cannot invalidate a pause already
        // made durable; losing the ping must never lose the work.
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
