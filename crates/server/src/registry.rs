use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use api::{Question, RunStatusResponse, RunSummary};
use engine::{RunDir, RunEvent, RunId, RunState};
use tokio::sync::RwLock;

use crate::ServerError;

/// The live index over durable run directories. The map owns no run
/// state: status is reduced from each event log whenever it is read.
pub(crate) struct RunRegistry {
    runs_root: PathBuf,
    runs: Arc<RwLock<BTreeMap<RunId, RunRecord>>>,
}

/// One registry entry. The task handle is retained so #14 can cancel
/// or resume the run through it; nothing reads it yet — published
/// status is reduced from the event log, never from the handle — but
/// dropping it would detach its task beyond recall, so it is kept.
struct RunRecord {
    dir: RunDir,
    #[allow(dead_code)]
    execution: Option<tokio::task::JoinHandle<()>>,
}

impl RunRecord {
    fn persisted(dir: RunDir) -> Self {
        Self {
            dir,
            execution: None,
        }
    }

    fn live(dir: RunDir, execution: tokio::task::JoinHandle<()>) -> Self {
        Self {
            dir,
            execution: Some(execution),
        }
    }
}

impl RunRegistry {
    pub(crate) async fn load(runs_root: PathBuf) -> Result<Self, ServerError> {
        tokio::fs::create_dir_all(&runs_root)
            .await
            .map_err(|source| ServerError::registry_io(&runs_root, source))?;
        let mut entries = tokio::fs::read_dir(&runs_root)
            .await
            .map_err(|source| ServerError::registry_io(&runs_root, source))?;
        let mut runs = BTreeMap::new();
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|source| ServerError::registry_io(&runs_root, source))?
        {
            let file_type = entry
                .file_type()
                .await
                .map_err(|source| ServerError::registry_io(entry.path(), source))?;
            if !file_type.is_dir() {
                continue;
            }
            let name = entry.file_name().into_string().map_err(|_| {
                ServerError::registry_io(
                    entry.path(),
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "run directory name is not UTF-8",
                    ),
                )
            })?;
            let run_id = RunId::new(name);
            let dir = RunDir::open(&runs_root, &run_id).await?;
            runs.insert(run_id, RunRecord::persisted(dir));
        }
        Ok(Self {
            runs_root,
            runs: Arc::new(RwLock::new(runs)),
        })
    }

    /// Lays down the on-disk directory for a new run. The registry
    /// owns where runs live, so creation sits beside `load`; the caller
    /// attaches the live task with [`insert_live`] once the engine is
    /// driving it.
    pub(crate) async fn create_dir(
        &self,
        run_id: RunId,
        kernel: &str,
        agent: &str,
    ) -> Result<RunDir, engine::StoreError> {
        RunDir::create(&self.runs_root, run_id, kernel, agent).await
    }

    pub(crate) async fn insert_live(
        &self,
        run_id: RunId,
        dir: RunDir,
        execution: tokio::task::JoinHandle<()>,
    ) {
        self.runs
            .write()
            .await
            .insert(run_id, RunRecord::live(dir, execution));
    }

    pub(crate) async fn get(&self, run_id: &RunId) -> Option<RunDir> {
        self.runs
            .read()
            .await
            .get(run_id)
            .map(|record| record.dir.clone())
    }

    pub(crate) async fn list(&self) -> Result<Vec<RunSummary>, engine::StoreError> {
        let dirs: Vec<RunDir> = self
            .runs
            .read()
            .await
            .values()
            .map(|record| record.dir.clone())
            .collect();
        let mut summaries = Vec::with_capacity(dirs.len());
        for dir in dirs {
            summaries.push(status(&dir).await?.run);
        }
        summaries.sort_by(|left, right| {
            right
                .created_at
                .cmp(&left.created_at)
                .then_with(|| right.run_id.cmp(&left.run_id))
        });
        Ok(summaries)
    }
}

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
