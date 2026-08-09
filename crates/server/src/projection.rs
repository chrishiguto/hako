//! The event-log → API view: how a run directory's durable history
//! becomes the status the daemon publishes. A DTO mapping over the
//! engine's [`engine::RunProjection`] — the daemon adds no fold of
//! its own, it only names the projection's fields the wire way. Owns
//! no state: disk is the source of truth and every read projects the
//! log afresh.

use api::{RunStatusResponse, RunSummary};
use engine::{PauseReason, RunDir, RunState};

pub(crate) async fn status(dir: &RunDir) -> Result<RunStatusResponse, engine::StoreError> {
    let meta = dir.meta();
    let projection = dir.project().await?;
    // Questions are pending only while the run actually waits on a
    // human; outside that pause the last report's questions are
    // history, not an ask.
    let pending_questions = match projection.state {
        RunState::Paused {
            reason: PauseReason::AwaitingHuman,
        } => projection
            .last_report
            .as_ref()
            .map(|core| core.questions.clone())
            .unwrap_or_default(),
        _ => Vec::new(),
    };
    Ok(RunStatusResponse {
        run: RunSummary {
            run_id: meta.run_id.as_str().to_owned(),
            state: projection.state,
            kernel: meta.kernel.clone(),
            agent: meta.agent.clone(),
            created_at: meta.created_at.clone(),
            updated_at: projection
                .updated_at
                .unwrap_or_else(|| meta.created_at.clone()),
        },
        iterations_completed: projection.iterations_completed,
        last_summary: projection.last_report.map(|core| core.summary),
        pending_questions,
    })
}
