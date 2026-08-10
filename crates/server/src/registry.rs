use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use api::RunSummary;
use api::proto::flow::FlowConfig;
use engine::{CancelToken, EventSink, RunDir, RunId};
use futures_util::future::join_all;
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

/// One registry entry. Published status is reduced from the event
/// log, never from the execution — the live half exists only so a
/// cancel can reach the run.
struct RunRecord {
    dir: RunDir,
    execution: Option<Execution>,
}

/// The live half of a record: the cancel token the kernel watches and
/// the task driving the run. Cancelling means firing the token and
/// draining the task — never `JoinHandle::abort`, which would drop
/// the future mid-await inside the agent exec, skip the sandbox
/// bracket's destroy, and leak the microVM (ADR 0003: teardown is the
/// isolation guarantee).
struct Execution {
    cancel: CancelToken,
    task: tokio::task::JoinHandle<()>,
}

/// How a cancel request landed against the registry's live view.
#[allow(dead_code, reason = "#14 wires the HTTP cancel route to this")]
pub(crate) enum CancelOutcome {
    /// The token fired and the task drained: the terminal event is on
    /// disk and every sandbox torn down before this comes back.
    Cancelled,
    /// The run exists but nothing is executing — it already reached a
    /// terminal state, or the daemon restarted and only the directory
    /// remains.
    NotRunning,
    /// No such run.
    UnknownRun,
}

impl RunRecord {
    fn persisted(dir: RunDir) -> Self {
        Self {
            dir,
            execution: None,
        }
    }

    fn live(dir: RunDir, execution: Execution) -> Self {
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
        let cancel = CancelToken::new();
        let task = runtime.launch(dir.clone(), flow, resolved, events, cancel.clone());
        runs.insert(
            run_id.clone(),
            RunRecord::live(dir, Execution { cancel, task }),
        );
        Ok(run_id)
    }

    /// Cancels a run cooperatively: fire the token the kernel watches,
    /// then drain its task, so the run unwinds through the sandbox
    /// bracket — teardown on every exit path — and writes its terminal
    /// `state_changed` before the caller answers. The execution is
    /// taken out of the record first, outside any await: a second
    /// cancel finds `NotRunning`, and the lock is never held while the
    /// run winds down.
    #[allow(dead_code, reason = "#14 wires the HTTP cancel route to this")]
    pub(crate) async fn cancel(&self, run_id: &RunId) -> CancelOutcome {
        let execution = {
            let mut runs = self.runs.write().await;
            match runs.get_mut(run_id) {
                None => return CancelOutcome::UnknownRun,
                Some(record) => record.execution.take(),
            }
        };
        let Some(execution) = execution else {
            return CancelOutcome::NotRunning;
        };
        execution.cancel.cancel();
        // A run that beat the token to a terminal state stays what it
        // was — the event log holds whichever ending won. A join error
        // is a panic `launch` already logged and published as failed.
        let _ = execution.task.await;
        CancelOutcome::Cancelled
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
        let statuses = join_all(dirs.iter().map(projection::status)).await;
        let mut summaries = Vec::with_capacity(statuses.len());
        for status in statuses {
            match status {
                Ok(status) => summaries.push(status.run),
                // A run dir deleted under a live entry is a run that no
                // longer exists: skipping it serves the same list a
                // restarted daemon would, where `load` would not index
                // it at all.
                Err(engine::StoreError::NotFound(_)) => {}
                Err(error) => return Err(error),
            }
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

#[cfg(test)]
mod tests {
    use engine::RunState;
    use engine::testkit::{NoSecrets, ScriptedSandbox, StubNotifier, seeded_repo};
    use tokio::sync::Barrier;

    use super::*;

    /// Cancel rides the token through the kernel's sandbox bracket:
    /// the hung agent exec is abandoned, its sandbox destroyed, and
    /// the terminal `cancelled` event is on disk before `cancel`
    /// answers — the run `JoinHandle::abort` would have left as a
    /// leaked VM and a log stuck on `running`.
    #[tokio::test]
    async fn cancel_drains_the_run_destroys_the_sandbox_and_ends_the_log_cancelled() {
        let runs_root = tempfile::tempdir().unwrap();
        let repo = seeded_repo();
        let barrier = Arc::new(Barrier::new(2));
        let sandbox = Arc::new(ScriptedSandbox::hanging().with_barrier(barrier.clone()));
        let runtime =
            EngineRuntime::new(sandbox.clone(), Arc::new(StubNotifier), Arc::new(NoSecrets));
        let registry = RunRegistry::load(runs_root.path().to_path_buf())
            .await
            .unwrap();

        let flow = FlowConfig::from_toml(&format!(
            r#"[loop]
kernel = "pipeline"

[agent]
engine = "cmd"
command = ["fake-agent", "{{prompt}}"]

[workspace]
repo = {:?}
"#,
            repo.path().to_str().unwrap()
        ))
        .unwrap();
        let resolved = runtime.resolve(&flow).unwrap();
        let run_id = registry.submit(flow, resolved, &runtime).await.unwrap();

        // Fire the cancel only once the agent exec is provably in
        // flight — the mid-exec case, where an abort would leak.
        barrier.wait().await;
        assert!(matches!(
            registry.cancel(&run_id).await,
            CancelOutcome::Cancelled
        ));

        let dir = registry.get(&run_id).await.unwrap();
        let status = projection::status(&dir).await.unwrap();
        assert_eq!(status.run.state, RunState::Cancelled);
        assert_eq!(sandbox.created(), 1);
        assert_eq!(sandbox.destroyed(), 1);

        // The execution is spent: a second cancel finds nothing live,
        // and a made-up run finds nothing at all.
        assert!(matches!(
            registry.cancel(&run_id).await,
            CancelOutcome::NotRunning
        ));
        assert!(matches!(
            registry.cancel(&RunId::new("no-such-run")).await,
            CancelOutcome::UnknownRun
        ));
    }
}
