use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use api::{RunStatusResponse, RunSummary};
use engine::{RunDir, RunEvent, RunId, RunState};
use tokio::sync::RwLock;

use crate::ServerError;

/// The live index over durable run directories. The map owns no run
/// state: status is reduced from each event log whenever it is read.
pub(crate) struct RunRegistry {
    runs_root: PathBuf,
    runs: Arc<RwLock<BTreeMap<RunId, RunRecord>>>,
}

/// One registry entry. Reloaded runs have no execution; newly
/// submitted runs keep their task here until it finishes, giving the
/// daemon one ownership point for later cancellation and resumption.
struct RunRecord {
    dir: RunDir,
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

    /// Finished tasks have already translated their outcome into the
    /// event log. Reaping their handles makes the in-memory lifecycle
    /// match the durable one: no execution remains attached.
    fn dir(&mut self) -> RunDir {
        if self
            .execution
            .as_ref()
            .is_some_and(tokio::task::JoinHandle::is_finished)
        {
            self.execution = None;
        }
        self.dir.clone()
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

    pub(crate) fn runs_root(&self) -> &std::path::Path {
        &self.runs_root
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
        self.runs.write().await.get_mut(run_id).map(RunRecord::dir)
    }

    pub(crate) async fn list(&self) -> Result<Vec<RunSummary>, engine::StoreError> {
        let dirs: Vec<RunDir> = self
            .runs
            .write()
            .await
            .values_mut()
            .map(RunRecord::dir)
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

pub(crate) async fn status(dir: &RunDir) -> Result<RunStatusResponse, engine::StoreError> {
    let meta = dir.meta();
    let events = dir.events().await?;
    let state = events
        .iter()
        .rev()
        .find_map(|envelope| match envelope.event {
            RunEvent::StateChanged { state } => Some(state),
            _ => None,
        })
        .unwrap_or(RunState::Running);
    let updated_at = events
        .last()
        .map_or_else(|| meta.created_at.clone(), |event| event.at.clone());
    let last_report = events
        .iter()
        .rev()
        .find_map(|envelope| match &envelope.event {
            RunEvent::StageReported { report, .. } => Some(report),
            _ => None,
        });
    let last_summary = last_report
        .and_then(|report| report.get("summary"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned);
    let pending_questions = if matches!(
        state,
        RunState::Paused {
            reason: engine::PauseReason::AwaitingHuman
        }
    ) {
        last_report
            .and_then(|report| report.get("questions"))
            .cloned()
            .map(serde_json::from_value)
            .transpose()
            .map_err(|error| engine::StoreError::Corrupt {
                path: dir.path().to_path_buf(),
                detail: format!("last stage report carries invalid questions: {error}"),
            })?
            .unwrap_or_default()
    } else {
        Vec::new()
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
