//! The event-log → API view: how a run directory's durable history
//! becomes the status the daemon publishes. Owns no state — disk is
//! the source of truth and every read reduces the log afresh.

use api::{Question, RunStatusResponse, RunSummary};
use engine::{RunDir, RunEvent, RunState};

/// The stage-report fields the daemon surfaces, as one typed view.
/// Lenient by design: every kernel dialect's report carries `summary`
/// and `questions`, and each stage's own fields are ignored here.
#[derive(serde::Deserialize)]
struct ReportView {
    summary: Option<String>,
    #[serde(default)]
    questions: Vec<Question>,
}

pub(crate) async fn status(dir: &RunDir) -> Result<RunStatusResponse, engine::StoreError> {
    let meta = dir.meta();
    let events = dir.events().await?;
    let state = engine::reduce_state(&events);
    let updated_at = events
        .last()
        .map_or_else(|| meta.created_at.clone(), |event| event.at.clone());
    let last_report = events
        .iter()
        .rev()
        .find_map(|envelope| match &envelope.event {
            RunEvent::StageReported { report, .. } => Some(report),
            _ => None,
        })
        .map(|report| serde_json::from_value::<ReportView>(report.clone()))
        .transpose()
        .map_err(|error| engine::StoreError::Corrupt {
            path: dir.path().to_path_buf(),
            detail: format!("last stage report is malformed: {error}"),
        })?;
    let last_summary = last_report.as_ref().and_then(|view| view.summary.clone());
    let pending_questions = match state {
        RunState::Paused {
            reason: engine::PauseReason::AwaitingHuman,
        } => last_report.map(|view| view.questions).unwrap_or_default(),
        _ => Vec::new(),
    };
    let iterations_completed = events
        .iter()
        .filter(|envelope| matches!(envelope.event, RunEvent::IterationFinished { .. }))
        .count()
        .try_into()
        .unwrap_or(u32::MAX);

    Ok(RunStatusResponse {
        run: RunSummary {
            run_id: meta.run_id.as_str().to_owned(),
            state,
            kernel: meta.kernel.clone(),
            agent: meta.agent.clone(),
            created_at: meta.created_at.clone(),
            updated_at,
        },
        iterations_completed,
        last_summary,
        pending_questions,
    })
}
