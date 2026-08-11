use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use api::proto::flow::FlowConfig;
use api::{BudgetExtension, RunSummary};
use engine::{Budgets, CancelToken, EventSink, HumanInput, RunDir, RunId, RunResume};
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

/// How a cancel request landed against the registry's live view. The
/// registry reports only what it knows — whether an execution was
/// drained — and never mints a state: a drained run may have beaten
/// the token to its own ending, and a completed run's execution stays
/// in its record until a cancel reaps it. The run's actual ending is
/// read from the log, like all published status.
pub(crate) enum CancelOutcome {
    /// An execution existed and has fully wound down: its terminal
    /// event is on disk and every sandbox torn down before this comes
    /// back. The log holds whichever ending won the race — usually
    /// `cancelled`, but a run already finished keeps what it earned.
    Drained,
    /// Nothing was executing — a prior cancel already drained the run,
    /// or the daemon restarted and only the directory remains.
    NotRunning,
    /// No such run.
    UnknownRun,
    Failed(String),
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
    ) -> Self {
        Self {
            dir,
            commands: Arc::new(Mutex::new(())),
            liveness: Liveness::Live(Box::new(LiveRun {
                execution: Some(execution),
                flow,
                resolved,
                budgets,
            })),
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
        let task = runtime.launch(RunLaunch::fresh(
            dir.clone(),
            flow.clone(),
            resolved.clone(),
            events,
            cancel.clone(),
        ));
        runs.insert(
            run_id.clone(),
            RunRecord::live(dir, Execution { cancel, task }, flow, resolved, budgets),
        );
        Ok(run_id)
    }

    pub(crate) async fn resume(
        &self,
        run_id: &RunId,
        note: Option<String>,
        extend: Option<BudgetExtension>,
        runtime: &EngineRuntime,
    ) -> Result<ResumeOutcome, String> {
        let (dir, commands) = {
            let runs = self.runs.read().await;
            let Some(record) = runs.get(run_id) else {
                return Ok(ResumeOutcome::UnknownRun);
            };
            (record.dir.clone(), record.commands.clone())
        };
        let _command = commands.lock().await;
        let history = dir.events().await.map_err(|error| error.to_string())?;
        let projected = dir.project().await.map_err(|error| error.to_string())?;
        if !matches!(projected.state, engine::RunState::Paused { .. }) {
            return Ok(ResumeOutcome::NotPaused);
        }

        let (execution, flow, resolved, mut budgets) = {
            let mut runs = self.runs.write().await;
            let record = runs.get_mut(run_id).expect("run remained indexed");
            match &mut record.liveness {
                Liveness::Live(live) => (
                    live.execution.take(),
                    live.flow.clone(),
                    live.resolved.clone(),
                    live.budgets.clone(),
                ),
                Liveness::Detached => return Ok(ResumeOutcome::Detached),
            }
        };
        if let Some(execution) = execution {
            let _ = execution.task.await;
        }
        apply_extension(&mut budgets, extend);

        let pause_at = history
            .iter()
            .rposition(|event| {
                matches!(
                    event.event,
                    engine::RunEvent::StateChanged {
                        state: engine::RunState::Paused { .. }
                    }
                )
            })
            .unwrap_or(0);
        let answers = history[pause_at..]
            .iter()
            .filter_map(|event| match &event.event {
                engine::RunEvent::QuestionAnswered {
                    question_id,
                    answer,
                } => Some(engine::Answer {
                    question_id: question_id.clone(),
                    answer: answer.clone(),
                }),
                _ => None,
            })
            .collect();
        let next_iteration = history
            .iter()
            .filter_map(|event| match event.event {
                engine::RunEvent::IterationStarted { iteration } => Some(iteration),
                _ => None,
            })
            .max()
            .unwrap_or(0)
            .saturating_add(1);
        let resume = RunResume {
            next_iteration,
            human: HumanInput {
                answers,
                questions: projected.pending_questions().to_vec(),
                note: note.clone(),
            },
        };
        let sink: Arc<dyn EventSink> = Arc::new(engine::ScrubbingSink::new(
            Arc::new(dir.event_sink().await.map_err(|error| error.to_string())?),
            resolved.secrets.clone(),
        ));
        sink.emit(engine::RunEvent::RunResumed { note })
            .await
            .map_err(|error| error.to_string())?;
        let cancel = CancelToken::new();
        let task = runtime.launch(RunLaunch::resumed(
            dir.clone(),
            flow,
            resolved,
            sink,
            cancel.clone(),
            budgets.clone(),
            resume,
        ));
        let mut runs = self.runs.write().await;
        let record = runs.get_mut(run_id).expect("run remained indexed");
        // Still live: nothing detaches a record while its command
        // lock — held right here — serializes every mutation.
        if let Liveness::Live(live) = &mut record.liveness {
            live.execution = Some(Execution { cancel, task });
            live.budgets = budgets;
        }
        Ok(ResumeOutcome::Resumed(dir))
    }

    pub(crate) async fn answer(
        &self,
        run_id: &RunId,
        answers: Vec<engine::Answer>,
    ) -> Result<AnswerOutcome, String> {
        let (dir, commands, secrets) = {
            let runs = self.runs.read().await;
            let Some(record) = runs.get(run_id) else {
                return Ok(AnswerOutcome::UnknownRun);
            };
            let Liveness::Live(live) = &record.liveness else {
                return Ok(AnswerOutcome::Detached);
            };
            (
                record.dir.clone(),
                record.commands.clone(),
                live.resolved.secrets.clone(),
            )
        };
        let _command = commands.lock().await;
        let projected = dir.project().await.map_err(|error| error.to_string())?;
        if projected.state
            != (engine::RunState::Paused {
                reason: engine::PauseReason::AwaitingHuman,
            })
        {
            return Ok(AnswerOutcome::NotAwaitingInput);
        }
        for answer in &answers {
            if !projected
                .pending_questions()
                .iter()
                .any(|question| question.id == answer.question_id)
            {
                return Ok(AnswerOutcome::UnknownQuestion(answer.question_id.clone()));
            }
        }
        let sink = engine::ScrubbingSink::new(
            Arc::new(dir.event_sink().await.map_err(|error| error.to_string())?),
            secrets,
        );
        for answer in answers {
            sink.emit(engine::RunEvent::QuestionAnswered {
                question_id: answer.question_id,
                answer: answer.answer,
            })
            .await
            .map_err(|error| error.to_string())?;
        }
        Ok(AnswerOutcome::Recorded(dir))
    }

    /// Cancels a run cooperatively: fire the token the kernel watches,
    /// then drain its task, so the run unwinds through the sandbox
    /// bracket — teardown on every exit path — and its terminal
    /// `state_changed` is on disk before the caller answers. What the
    /// run ended as is the log's to say, per [`CancelOutcome`]. The
    /// execution is taken out of the record first, outside any await:
    /// a second cancel finds `NotRunning`, and the registry index is
    /// never held while the run winds down.
    pub(crate) async fn cancel(&self, run_id: &RunId) -> CancelOutcome {
        let (dir, commands) = {
            let runs = self.runs.read().await;
            let Some(record) = runs.get(run_id) else {
                return CancelOutcome::UnknownRun;
            };
            (record.dir.clone(), record.commands.clone())
        };
        let _command = commands.lock().await;
        let state = match dir.project().await {
            Ok(projected) => projected.state,
            Err(error) => return CancelOutcome::Failed(error.to_string()),
        };
        let execution = {
            let mut runs = self.runs.write().await;
            match runs.get_mut(run_id) {
                None => return CancelOutcome::UnknownRun,
                Some(record) => match &mut record.liveness {
                    Liveness::Live(live) => live.execution.take(),
                    Liveness::Detached => None,
                },
            }
        };
        if matches!(state, engine::RunState::Paused { .. }) {
            if let Some(execution) = execution {
                let _ = execution.task.await;
            }
            let sink = match dir.event_sink().await {
                Ok(sink) => sink,
                Err(error) => return CancelOutcome::Failed(error.to_string()),
            };
            if let Err(error) = sink
                .emit(engine::RunEvent::StateChanged {
                    state: engine::RunState::Cancelled,
                })
                .await
            {
                return CancelOutcome::Failed(error.to_string());
            }
            return CancelOutcome::Drained;
        }
        let Some(execution) = execution else {
            return CancelOutcome::NotRunning;
        };
        execution.cancel.cancel();
        // A run that beat the token to a terminal state stays what it
        // was — the event log holds whichever ending won. A join error
        // is a panic `launch` already logged and published as failed.
        let _ = execution.task.await;
        CancelOutcome::Drained
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
        assert!(matches!(
            registry.cancel(&run_id).await,
            CancelOutcome::Drained
        ));

        let dir = registry.get(&run_id).await.unwrap();
        let status = projection::status(&dir).await.unwrap();
        assert_eq!(status.run.state, RunState::Cancelled);
        assert_eq!(sandbox.created(), 1);
        assert_eq!(sandbox.destroyed(), 1);

        // The execution is spent — a cancel can only reap it once.
        assert!(matches!(
            registry.cancel(&run_id).await,
            CancelOutcome::NotRunning
        ));
        assert!(matches!(
            registry.cancel(&RunId::new("no-such-run")).await,
            CancelOutcome::UnknownRun
        ));
    }

    /// A run that already ended keeps its ending: cancelling it drains
    /// the spent execution — `Drained`, because the registry reports
    /// only what it drained, never a state — while the log still says
    /// `done`. The route reads status for the truth, like every
    /// caller.
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
            registry.cancel(&run_id).await,
            CancelOutcome::Drained
        ));
        assert_eq!(
            projection::status(&dir).await.unwrap().run.state,
            RunState::Done
        );
    }
}
