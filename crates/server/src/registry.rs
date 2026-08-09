use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use api::RunSummary;
use api::proto::flow::FlowConfig;
use engine::{EventSink, RunDir, RunId};
use futures_util::future::try_join_all;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::ServerError;
use crate::projection;
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
        let io_error = |source| ServerError::registry_io(&runs_root, source);
        tokio::fs::create_dir_all(&runs_root)
            .await
            .map_err(io_error)?;
        let mut entries = tokio::fs::read_dir(&runs_root).await.map_err(io_error)?;
        let mut runs = BTreeMap::new();
        while let Some(entry) = entries.next_entry().await.map_err(io_error)? {
            let file_type = entry
                .file_type()
                .await
                .map_err(|source| ServerError::registry_io(entry.path(), source))?;
            if !file_type.is_dir() {
                continue;
            }
            // A directory the store never wrote — a non-UTF-8 name can
            // name no run, one without metadata holds none — is not
            // ours to interpret, and skipping it beats refusing to
            // start. Real runs stay strict: corrupt metadata or a
            // lying log still fails the load.
            // TODO: the two eprintln!s below await the in-flight
            // rework of this load loop before converting to tracing.
            let Ok(name) = entry.file_name().into_string() else {
                eprintln!("ignoring {}: not a run directory", entry.path().display());
                continue;
            };
            let run_id = RunId::new(name);
            let dir = match RunDir::open(&runs_root, &run_id).await {
                Ok(dir) => dir,
                Err(engine::StoreError::NotFound(_)) => {
                    eprintln!("ignoring {}: not a run directory", entry.path().display());
                    continue;
                }
                Err(error) => return Err(error.into()),
            };
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
        let statuses = try_join_all(dirs.iter().map(projection::status)).await?;
        let mut summaries: Vec<RunSummary> =
            statuses.into_iter().map(|status| status.run).collect();
        summaries.sort_by(|left, right| {
            right
                .created_at
                .cmp(&left.created_at)
                .then_with(|| right.run_id.cmp(&left.run_id))
        });
        Ok(summaries)
    }
}
