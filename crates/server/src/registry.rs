use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use api::proto::flow::FlowConfig;
use api::{BudgetExtension, RunListEntry};
use engine::{BudgetUsage, Budgets, CancelToken, EventSink, RunDir, RunId, SecretEnv};
use futures_util::future::join_all;
use tokio::sync::{Mutex, RwLock};
use uuid::Uuid;

use crate::ServerError;
use crate::projection;
use crate::runtime::{EngineRuntime, ResolvedRun, RunLaunch};

/// The live index over durable run directories. The map owns no run
/// state: status is reduced from each event log whenever it is read.
pub(crate) struct RunRegistry {
    runs_root: PathBuf,
    runs: RwLock<BTreeMap<RunId, RunRecord>>,
}

/// One registry entry. Published status is reduced from the event
/// log, never from the live half; it only lets commands reach the
/// execution and the relaunch material.
struct RunRecord {
    dir: RunDir,
    commands: Arc<Mutex<()>>,
    liveness: Liveness,
}

/// What the daemon holds for a run beyond its directory. A record
/// loaded from disk after a restart is detached: status still reduces
/// from the log, but the relaunch material — resolved agent, secrets,
/// flow — died with the old process and cannot be reconstructed from
/// the run directory yet, so the commands that need it answer that
/// honestly instead of misreporting the run's state.
enum Liveness {
    Live(Box<LiveRun>),
    Detached,
}

/// The live half of a record: the relaunch material and, while one
/// runs, the execution.
struct LiveRun {
    execution: Option<Execution>,
    flow: FlowConfig,
    resolved: ResolvedRun,
    budgets: Budgets,
    budget_usage: BudgetUsage,
}

/// A running execution: the cancel token the kernel watches and the
/// task driving the run. Cancelling means firing the token and
/// draining the task — never `JoinHandle::abort`, which would drop
/// the future mid-await inside the agent exec, skip the sandbox
/// bracket's destroy, and leak the microVM that teardown is there to
/// reclaim.
struct Execution {
    cancel: CancelToken,
    task: tokio::task::JoinHandle<()>,
}

/// How a cancel landed. The registry never mints a state — the log
/// holds whichever ending won — but it does read the verdict after
/// the drain, so the policy for a run that beat the token to its own
/// ending lives here, in one place, not half in the HTTP layer.
pub(crate) enum CancelOutcome {
    /// The run is terminal at `cancelled`: any execution has fully
    /// wound down, every sandbox is torn down, and the terminal event
    /// is on disk — reflected in this status — before this comes back.
    Cancelled(Box<api::RunStatusResponse>),
    /// Nothing ended `cancelled`: the run had already earned its own
    /// ending, a prior cancel drained it, or the daemon restarted and
    /// only the directory remains.
    NotRunning,
    /// No such run.
    UnknownRun,
}

/// A failure inside a run command — the daemon's fault or a vanished
/// run, never the client's request. Typed, so the HTTP layer keeps
/// its one rule for missing runs: a directory that is gone answers
/// `run_not_found` from a command exactly as it does from a status
/// read, instead of masquerading as an internal fault.
#[derive(Debug, thiserror::Error)]
pub(crate) enum CommandError {
    #[error(transparent)]
    Store(#[from] engine::StoreError),
    #[error(transparent)]
    Sink(#[from] engine::EventSinkError),
}

pub(crate) enum ResumeOutcome {
    Resumed(RunDir),
    NotPaused,
    /// The run predates a daemon restart; see [`Liveness::Detached`].
    Detached,
    UnknownRun,
}

pub(crate) enum AnswerOutcome {
    Recorded(RunDir),
    NotAwaitingInput,
    UnknownQuestion(String),
    /// The run predates a daemon restart. Its answers exist to feed a
    /// resume that can never happen, so they are refused up front.
    Detached,
    UnknownRun,
}

impl RunRecord {
    fn persisted(dir: RunDir) -> Self {
        Self {
            dir,
            commands: Arc::new(Mutex::new(())),
            liveness: Liveness::Detached,
        }
    }

    fn live(
        dir: RunDir,
        execution: Execution,
        flow: FlowConfig,
        resolved: ResolvedRun,
        budgets: Budgets,
        budget_usage: BudgetUsage,
    ) -> Self {
        Self {
            dir,
            commands: Arc::new(Mutex::new(())),
            liveness: Liveness::Live(Box::new(LiveRun {
                execution: Some(execution),
                flow,
                resolved,
                budgets,
                budget_usage,
            })),
        }
    }
}

/// One command's grip on a run: the per-run serialization lock, the
/// run's directory, and a projection read under that lock. Answer,
/// resume, and cancel each hold one for their whole span, so commands
/// on one run never interleave and each judges state no other command
/// can shift beneath it.
struct CommandGuard {
    dir: RunDir,
    /// The run's secret values while the record is live; `None` once
    /// detached — a restarted daemon has nothing left to scrub.
    secrets: Option<SecretEnv>,
    projected: engine::RunProjection,
    _serial: tokio::sync::OwnedMutexGuard<()>,
}

impl CommandGuard {
    fn is_detached(&self) -> bool {
        self.secrets.is_none()
    }

    /// The sink a command writes through — scrubbing, like every sink
    /// the daemon hands a run: what the run was given must never
    /// appear in the record of it. A detached record scrubs nothing,
    /// which is an empty scrub, not a bare sink.
    async fn scrubbed_sink(&self) -> Result<Arc<dyn EventSink>, CommandError> {
        Ok(scrubbed(
            self.dir.event_sink().await?,
            &self.secrets.clone().unwrap_or_default(),
        ))
    }
}

/// Wraps a fresh sink in the scrub every daemon-held sink carries.
fn scrubbed(sink: impl EventSink + 'static, secrets: &SecretEnv) -> Arc<dyn EventSink> {
    Arc::new(engine::ScrubbingSink::new(Arc::new(sink), secrets.clone()))
}

impl RunRegistry {
    /// Opens a command against a run: takes the run's command lock,
    /// then reads where the run stands under it. `None` is no such
    /// run.
    async fn command(&self, run_id: &RunId) -> Result<Option<CommandGuard>, CommandError> {
        let (dir, commands, secrets) = {
            let runs = self.runs.read().await;
            let Some(record) = runs.get(run_id) else {
                return Ok(None);
            };
            let secrets = match &record.liveness {
                Liveness::Live(live) => Some(live.resolved.secrets.clone()),
                Liveness::Detached => None,
            };
            (record.dir.clone(), record.commands.clone(), secrets)
        };
        let serial = commands.lock_owned().await;
        let projected = dir.project().await?;
        Ok(Some(CommandGuard {
            dir,
            secrets,
            projected,
            _serial: serial,
        }))
    }

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
            // Every event the run emits passes the scrub on its way to
            // the log: the values the run was given are exactly what
            // must not appear in the record of it, and an agent that
            // echoes its environment is the ordinary case, not the
            // adversarial one.
            Ok(sink) => Arc::new(engine::ScrubbingSink::new(
                Arc::new(sink),
                resolved.secrets.clone(),
            )),
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
        let budgets = Budgets::from(&flow.budget);
        let budget_usage = BudgetUsage::default();
        let task = runtime.launch(RunLaunch {
            dir: dir.clone(),
            flow: flow.clone(),
            resolved: resolved.clone(),
            events,
            cancel: cancel.clone(),
            budgets: budgets.clone(),
            budget_usage: budget_usage.clone(),
            replay: None,
        });
        runs.insert(
            run_id.clone(),
            RunRecord::live(
                dir,
                Execution { cancel, task },
                flow,
                resolved,
                budgets,
                budget_usage,
            ),
        );
        Ok(run_id)
    }

    pub(crate) async fn resume(
        &self,
        run_id: &RunId,
        note: Option<String>,
        extend: Option<BudgetExtension>,
        runtime: &EngineRuntime,
    ) -> Result<ResumeOutcome, CommandError> {
        let Some(guard) = self.command(run_id).await? else {
            return Ok(ResumeOutcome::UnknownRun);
        };
        if !matches!(guard.projected.state, engine::RunState::Paused { .. }) {
            return Ok(ResumeOutcome::NotPaused);
        }

        let (execution, flow, resolved, mut budgets, budget_usage) = {
            let mut runs = self.runs.write().await;
            let record = runs.get_mut(run_id).expect("run remained indexed");
            match &mut record.liveness {
                Liveness::Live(live) => (
                    live.execution.take(),
                    live.flow.clone(),
                    live.resolved.clone(),
                    live.budgets.clone(),
                    live.budget_usage.clone(),
                ),
                Liveness::Detached => return Ok(ResumeOutcome::Detached),
            }
        };
        if let Some(execution) = execution {
            let _ = execution.task.await;
        }
        apply_extension(&mut budgets, extend.clone());

        let sink = guard.scrubbed_sink().await?;
        // The command lands in the log like everything else — the
        // granted extension included, so the record of how far this
        // run may go survives the daemon that granted it.
        sink.emit(engine::RunEvent::RunResumed { note, extend })
            .await?;
        let replay = guard.dir.events().await?;
        let cancel = CancelToken::new();
        let task = runtime.launch(RunLaunch {
            dir: guard.dir.clone(),
            flow,
            resolved,
            events: sink,
            cancel: cancel.clone(),
            budgets: budgets.clone(),
            budget_usage,
            replay: Some(replay),
        });
        let mut runs = self.runs.write().await;
        let record = runs.get_mut(run_id).expect("run remained indexed");
        // Still live: nothing detaches a record while its command
        // lock — held right here — serializes every mutation.
        if let Liveness::Live(live) = &mut record.liveness {
            live.execution = Some(Execution { cancel, task });
            live.budgets = budgets;
        }
        Ok(ResumeOutcome::Resumed(guard.dir))
    }

    pub(crate) async fn answer(
        &self,
        run_id: &RunId,
        answers: Vec<engine::Answer>,
    ) -> Result<AnswerOutcome, CommandError> {
        let Some(guard) = self.command(run_id).await? else {
            return Ok(AnswerOutcome::UnknownRun);
        };
        if guard.is_detached() {
            return Ok(AnswerOutcome::Detached);
        }
        if guard.projected.state
            != (engine::RunState::Paused {
                reason: engine::PauseReason::AwaitingHuman,
            })
        {
            return Ok(AnswerOutcome::NotAwaitingInput);
        }
        for answer in &answers {
            if !guard
                .projected
                .pending_questions()
                .iter()
                .any(|question| question.id == answer.question_id)
            {
                return Ok(AnswerOutcome::UnknownQuestion(answer.question_id.clone()));
            }
        }
        let sink = guard.scrubbed_sink().await?;
        for answer in answers {
            sink.emit(engine::RunEvent::QuestionAnswered {
                question_id: answer.question_id,
                answer: answer.answer,
            })
            .await?;
        }
        Ok(AnswerOutcome::Recorded(guard.dir))
    }

    /// Cancels a run cooperatively: fire the token the kernel watches,
    /// then drain its task, so the run unwinds through the sandbox
    /// bracket — teardown on every exit path — and its terminal
    /// `state_changed` is on disk before the caller answers. A paused
    /// run holds no execution to signal; its ending is written
    /// directly. Either way the verdict is read back from the log —
    /// a run that beat the token to its own ending keeps what it
    /// earned and reads as [`CancelOutcome::NotRunning`]. The
    /// execution is taken out of the record outside any await: a
    /// second cancel finds `NotRunning`, and the registry index is
    /// never held while the run winds down.
    pub(crate) async fn cancel(&self, run_id: &RunId) -> Result<CancelOutcome, CommandError> {
        let Some(guard) = self.command(run_id).await? else {
            return Ok(CancelOutcome::UnknownRun);
        };
        let execution = {
            let mut runs = self.runs.write().await;
            match runs.get_mut(run_id) {
                None => return Ok(CancelOutcome::UnknownRun),
                Some(record) => match &mut record.liveness {
                    Liveness::Live(live) => live.execution.take(),
                    Liveness::Detached => None,
                },
            }
        };
        if matches!(guard.projected.state, engine::RunState::Paused { .. }) {
            if let Some(execution) = execution {
                let _ = execution.task.await;
            }
            let sink = guard.scrubbed_sink().await?;
            sink.emit(engine::RunEvent::StateChanged {
                state: engine::RunState::Cancelled,
            })
            .await?;
        } else {
            let Some(execution) = execution else {
                return Ok(CancelOutcome::NotRunning);
            };
            execution.cancel.cancel();
            // A join error is a panic `launch` already logged and
            // published as failed; the log still holds the verdict.
            let _ = execution.task.await;
        }
        let status = projection::status(&guard.dir).await?;
        if status.run.state == engine::RunState::Cancelled {
            Ok(CancelOutcome::Cancelled(Box::new(status)))
        } else {
            Ok(CancelOutcome::NotRunning)
        }
    }

    pub(crate) async fn get(&self, run_id: &RunId) -> Option<RunDir> {
        self.runs
            .read()
            .await
            .get(run_id)
            .map(|record| record.dir.clone())
    }

    /// Every indexed run, newest first. Infallible by design: one
    /// damaged run directory must not empty the fleet view — the list
    /// is how an operator finds the broken run, so its failure rides
    /// in-band as an unreadable entry instead of failing the whole
    /// read. The run's own status endpoint still fails loudly.
    pub(crate) async fn list(&self) -> Vec<RunListEntry> {
        let mut dirs: Vec<RunDir> = self
            .runs
            .read()
            .await
            .values()
            .map(|record| record.dir.clone())
            .collect();
        // Ordered before projecting, on the registry metadata every
        // entry — readable or not — carries into its list line.
        dirs.sort_by(|left, right| {
            let (left, right) = (left.meta(), right.meta());
            (&right.created_at, right.run_id.as_str())
                .cmp(&(&left.created_at, left.run_id.as_str()))
        });
        join_all(dirs.iter().map(projection::list_entry))
            .await
            .into_iter()
            .flatten()
            .collect()
    }
}

fn apply_extension(budgets: &mut Budgets, extend: Option<BudgetExtension>) {
    let Some(extend) = extend else {
        return;
    };
    if let Some(max_iterations) = extend.max_iterations {
        budgets.max_iterations = Some(max_iterations);
    }
    if let Some(seconds) = extend.max_wall_clock_seconds {
        budgets.max_wall_clock = Some(std::time::Duration::from_secs(seconds));
    }
    if let Some(max_tokens) = extend.max_tokens {
        budgets.max_tokens = Some(max_tokens);
    }
}

#[cfg(test)]
mod tests {
    use engine::RunState;
    use engine::testkit::{
        NoSecrets, ScriptedSandbox, StubNotifier, UNREFUTED_SKEPTIC_REPORT, exec, seeded_repo,
    };
    use tokio::sync::Barrier;

    use super::*;

    /// The one-stage cmd-agent flow every registry test submits, over
    /// the given seeded repo.
    fn flow_over(repo: &std::path::Path) -> FlowConfig {
        FlowConfig::from_toml(&format!(
            r#"[loop]
kernel = "pipeline"

[agent]
engine = "cmd"
command = ["fake-agent", "{{prompt}}"]

[workspace]
repo = {:?}
"#,
            repo.to_str().unwrap()
        ))
        .unwrap()
    }

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

        let flow = flow_over(repo.path());
        let resolved = runtime.resolve(&flow).await.unwrap();
        let run_id = registry.submit(flow, resolved, &runtime).await.unwrap();

        // Fire the cancel only once the agent exec is provably in
        // flight — the mid-exec case, where an abort would leak.
        barrier.wait().await;
        let CancelOutcome::Cancelled(status) = registry.cancel(&run_id).await.unwrap() else {
            panic!("the drained run did not read back cancelled");
        };
        assert_eq!(status.run.state, RunState::Cancelled);

        let dir = registry.get(&run_id).await.unwrap();
        let status = projection::status(&dir).await.unwrap();
        assert_eq!(status.run.state, RunState::Cancelled);
        assert_eq!(sandbox.created(), 1);
        assert_eq!(sandbox.destroyed(), 1);

        // The execution is spent — a cancel can only reap it once.
        assert!(matches!(
            registry.cancel(&run_id).await.unwrap(),
            CancelOutcome::NotRunning
        ));
        assert!(matches!(
            registry.cancel(&RunId::new("no-such-run")).await.unwrap(),
            CancelOutcome::UnknownRun
        ));
    }

    /// A run that already ended keeps its ending: cancelling it drains
    /// the spent execution, reads `done` back from the log, and
    /// reports `NotRunning` — nothing ended `cancelled`, and the
    /// registry, not its callers, owns that verdict.
    #[tokio::test]
    async fn cancelling_a_finished_run_drains_it_but_the_log_keeps_its_ending() {
        let runs_root = tempfile::tempdir().unwrap();
        let repo = seeded_repo();
        // The plan claims done, then a fresh skeptic lets the claim
        // stand. No checks are configured, so neither path needs a
        // check transcript.
        let sandbox = Arc::new(ScriptedSandbox::scripted(vec![
            exec("planned\n", 0),
            exec("checked\n", 0),
        ]));
        sandbox.write_report_on_exec(r#"{"status": "done", "summary": "nothing left"}"#);
        sandbox.write_report_on_exec(UNREFUTED_SKEPTIC_REPORT);
        let runtime =
            EngineRuntime::new(sandbox.clone(), Arc::new(StubNotifier), Arc::new(NoSecrets));
        let registry = RunRegistry::load(runs_root.path().to_path_buf())
            .await
            .unwrap();

        let flow = flow_over(repo.path());
        let resolved = runtime.resolve(&flow).await.unwrap();
        let run_id = registry.submit(flow, resolved, &runtime).await.unwrap();

        // Let the run reach its own ending before any cancel exists.
        let dir = registry.get(&run_id).await.unwrap();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        while projection::status(&dir).await.unwrap().run.state != RunState::Done {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the run never reached done"
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }

        assert!(matches!(
            registry.cancel(&run_id).await.unwrap(),
            CancelOutcome::NotRunning
        ));
        assert_eq!(
            projection::status(&dir).await.unwrap().run.state,
            RunState::Done
        );
    }
}
