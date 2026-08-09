use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use api::proto::flow::FlowConfig;
use api::{Question, RunStatusResponse, RunSummary};
use engine::{EventSink, RunDir, RunEvent, RunId, RunState};
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::ServerError;
use crate::runtime::{EngineRuntime, ResolvedRun};

/// The live index over durable run directories. The map owns no run
/// state: status is reduced from each event log whenever it is read.
pub(crate) struct RunRegistry {
    runs_root: PathBuf,
    runs: RwLock<BTreeMap<RunId, RunRecord>>,
}

/// One registry entry. The task handle is retained so a later
/// cancel/resume slice can drive the run through it; nothing reads it
/// yet — published status is reduced from the event log, never from
/// the handle — but dropping it would detach its task beyond recall,
/// so it is kept.
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
            runs: RwLock::new(runs),
        })
    }

    /// Brings a new run into existence — directory on disk, kernel
    /// driving it, live record in the index — or nothing at all. The
    /// event sink is opened before anything is published so the one
    /// fallible step can still unwind the directory; a failure here
    /// must not leave a run a restarted daemon would misreport as
    /// running forever.
    pub(crate) async fn submit(
        &self,
        flow: FlowConfig,
        resolved: ResolvedRun,
        runtime: &EngineRuntime,
    ) -> Result<RunId, engine::StoreError> {
        let run_id = RunId::new(Uuid::now_v7().to_string());
        let dir = RunDir::create(
            &self.runs_root,
            run_id.clone(),
            flow.r#loop.kernel.as_str(),
            &flow.agent.engine,
        )
        .await?;
        let events: Arc<dyn EventSink> = match dir.event_sink().await {
            Ok(sink) => Arc::new(sink),
            Err(error) => {
                let _ = tokio::fs::remove_dir_all(dir.path()).await;
                return Err(error);
            }
        };
        // Spawning under the write lock keeps the record's insertion
        // atomic with the launch: no reader can observe the run on
        // disk and running but absent from the index. `launch` never
        // awaits, so the lock is not held across a suspension point.
        let mut runs = self.runs.write().await;
        let execution = runtime.launch(dir.clone(), flow, resolved, events);
        runs.insert(run_id.clone(), RunRecord::live(dir, execution));
        Ok(run_id)
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
