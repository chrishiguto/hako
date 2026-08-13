//! The shared run-ending path. Pauses become visible only after the
//! workspace is durable, and every pause makes a best-effort
//! notification.

use proto::BudgetKind;

use crate::event::RunEvent;
use crate::kernel::{KernelContext, KernelError};
use crate::notify::Notification;
use crate::run::{PauseReason, RunOutcome};

#[derive(Debug, Clone, Copy)]
pub(crate) enum Ending<'a> {
    Done,
    Failed,
    Paused {
        /// The iteration the pause parks — the checkpoint commit is
        /// recorded against it. Only pauses checkpoint, so only they
        /// carry one.
        iteration: u32,
        reason: PauseReason,
        summary: Option<&'a str>,
    },
    Cancelled,
}

impl Ending<'_> {
    fn outcome(self) -> RunOutcome {
        match self {
            Self::Done => RunOutcome::Done,
            Self::Failed => RunOutcome::Failed,
            Self::Paused { reason, .. } => RunOutcome::Paused(reason),
            Self::Cancelled => RunOutcome::Cancelled,
        }
    }
}

pub(crate) async fn pause_for_budget(
    ctx: &KernelContext,
    iteration: u32,
    budget: BudgetKind,
    summary: Option<&str>,
) -> Result<RunOutcome, KernelError> {
    ctx.events
        .emit(RunEvent::BudgetExhausted { budget })
        .await?;
    conclude(
        ctx,
        Ending::Paused {
            iteration,
            reason: PauseReason::Budget,
            summary,
        },
    )
    .await
}

pub(crate) async fn conclude(
    ctx: &KernelContext,
    ending: Ending<'_>,
) -> Result<RunOutcome, KernelError> {
    if let Ending::Paused { iteration, .. } = ending
        && let Some(commit) = ctx.workspace.checkpoint("hako: pause").await?
    {
        ctx.events
            .emit(RunEvent::WorkspaceCheckpointed { iteration, commit })
            .await?;
    }
    let outcome = ending.outcome();
    ctx.events
        .emit(RunEvent::StateChanged {
            state: outcome.into(),
        })
        .await?;
    if let Ending::Paused {
        reason, summary, ..
    } = ending
    {
        // Notification delivery cannot invalidate a pause already
        // made durable; losing the ping must never lose the work.
        let _ = ctx
            .notifier
            .notify(&Notification {
                run_id: ctx.run_id.clone(),
                reason,
                summary: summary.map(str::to_owned),
            })
            .await;
    }
    Ok(outcome)
}
