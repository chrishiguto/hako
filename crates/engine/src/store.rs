//! The run store — a run persisted as a directory of plain files.
//! Deliberately not a seam: like the workspace, it is a real directory
//! the engine reaches with real files, in production and in tests
//! alike. Observability without tooling: everything in it reads with
//! cat/jq.
//!
//! The JSONL Event Log is the source of truth. `state.json` is a
//! mirror the sink refreshes on every state change so a human can cat
//! the current state without replaying; code always derives state from
//! the log. `meta.json` is fixed at creation. Every append is flushed
//! before the emit returns, so a daemon restart loses nothing a kernel
//! saw acknowledged. An event exists once its whole line is on disk,
//! and not before: an append cut short — crash, full disk — leaves a
//! torn tail that readers ignore and the sink truncates before
//! continuing.

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

use crate::event::{EventSink, EventSinkError};
use crate::run::{RunId, RunState};
use proto::event::{EventEnvelope, RunEvent};

const EVENT_LOG_FILE: &str = "events.jsonl";
const STATE_FILE: &str = "state.json";
const META_FILE: &str = "meta.json";

/// The state a run is born in — what `create` seeds the mirror with
/// and what `state()` reports while the log holds no `state_changed`
/// yet. One definition, so the two can never disagree about a fresh
/// run.
const INITIAL_STATE: RunState = RunState::Running;

/// What identifies a run beyond its log: written once at creation,
/// immutable after. The one file a registry can read without touching
/// the log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunMeta {
    pub run_id: RunId,
    /// The kernel name the flow selected.
    pub kernel: String,
    /// The agent adapter name the flow selected.
    pub agent: String,
    /// RFC 3339 UTC timestamp of when the run was created.
    pub created_at: String,
}

/// One run's directory: the event log, the state mirror, and the
/// metadata beside it. Creating one is what brings a run into
/// existence on disk; opening one is how a restarted daemon gets the
/// run back.
#[derive(Debug, Clone)]
pub struct RunDir {
    root: PathBuf,
    meta: RunMeta,
}

impl RunDir {
    /// Creates `<runs_root>/<run_id>/` with its metadata, an empty
    /// event log, and a `running` state mirror. Rejects a run id that
    /// already has a directory — run ids name exactly one run ever.
    pub async fn create(
        runs_root: &Path,
        run_id: RunId,
        kernel: &str,
        agent: &str,
    ) -> Result<Self, StoreError> {
        let root = runs_root.join(run_id.as_str());
        fs::create_dir_all(runs_root)
            .await
            .map_err(|source| StoreError::io(runs_root, source))?;
        fs::create_dir(&root).await.map_err(|source| {
            if source.kind() == std::io::ErrorKind::AlreadyExists {
                StoreError::AlreadyExists(run_id.clone())
            } else {
                StoreError::io(&root, source)
            }
        })?;

        let meta = RunMeta {
            run_id,
            kernel: kernel.to_owned(),
            agent: agent.to_owned(),
            created_at: now_rfc3339(),
        };
        let dir = Self { root, meta };
        write_json(&dir.meta_path(), &dir.meta).await?;
        write_json(&dir.state_path(), &INITIAL_STATE).await?;
        fs::write(&dir.log_path(), b"")
            .await
            .map_err(|source| StoreError::io(&dir.log_path(), source))?;
        Ok(dir)
    }

    /// Opens an existing run directory — after a daemon restart, this
    /// plus the files inside is all there is.
    pub async fn open(runs_root: &Path, run_id: &RunId) -> Result<Self, StoreError> {
        let root = runs_root.join(run_id.as_str());
        let meta_path = root.join(META_FILE);
        let raw = match fs::read_to_string(&meta_path).await {
            Ok(raw) => raw,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                return Err(StoreError::NotFound(run_id.clone()));
            }
            Err(source) => return Err(StoreError::io(&meta_path, source)),
        };
        let meta: RunMeta =
            serde_json::from_str(&raw).map_err(|error| StoreError::corrupt(&meta_path, error))?;
        if meta.run_id != *run_id {
            return Err(StoreError::corrupt(
                &meta_path,
                format!("metadata names run `{}`", meta.run_id),
            ));
        }
        Ok(Self { root, meta })
    }

    pub fn path(&self) -> &Path {
        &self.root
    }

    pub fn meta(&self) -> &RunMeta {
        &self.meta
    }

    /// The full event history, replayed strictly: every line must
    /// parse, carry this run's id, and sit at the sequence position it
    /// claims — a log that lies about its ids cannot back SSE resume.
    /// The one tolerated flaw is a torn tail, which was never
    /// acknowledged and so is not history.
    pub async fn events(&self) -> Result<Vec<EventEnvelope>, StoreError> {
        Ok(self.read_log().await?.0)
    }

    /// The log's whole-line events, plus the byte length of the
    /// whole-lines prefix — anything beyond it is a torn tail.
    async fn read_log(&self) -> Result<(Vec<EventEnvelope>, u64), StoreError> {
        let path = self.log_path();
        let raw = fs::read(&path)
            .await
            .map_err(|source| StoreError::io(&path, source))?;
        let terminated = raw
            .iter()
            .rposition(|&byte| byte == b'\n')
            .map_or(0, |newline| newline + 1);
        let text = std::str::from_utf8(&raw[..terminated])
            .map_err(|error| StoreError::corrupt(&path, error))?;
        let mut events = Vec::new();
        for (position, line) in text.lines().enumerate() {
            let envelope: EventEnvelope = serde_json::from_str(line).map_err(|error| {
                StoreError::corrupt(&path, format!("line {}: {error}", position + 1))
            })?;
            if envelope.seq != position as u64 {
                return Err(StoreError::corrupt(
                    &path,
                    format!(
                        "line {}: seq {} out of sequence",
                        position + 1,
                        envelope.seq
                    ),
                ));
            }
            if envelope.run_id != self.meta.run_id.as_str() {
                return Err(StoreError::corrupt(
                    &path,
                    format!(
                        "line {}: event belongs to run `{}`",
                        position + 1,
                        envelope.run_id
                    ),
                ));
            }
            events.push(envelope);
        }
        Ok((events, terminated as u64))
    }

    /// Where the run stands according to the log: the last
    /// `state_changed`, or `running` while none has landed. A run that
    /// reads `running` after a restart simply never got further — what
    /// to do about its dead kernel is the host's call.
    pub async fn state(&self) -> Result<RunState, StoreError> {
        Ok(reduce_state(&self.events().await?))
    }

    /// The sink a kernel appends this run's events through. Continues
    /// after the last logged sequence number, so ids stay stable across
    /// any number of restarts and resumes. A torn tail is truncated
    /// away first — the next event must not land glued onto an
    /// unfinished line.
    pub async fn event_sink(&self) -> Result<FileEventSink, StoreError> {
        let (events, whole_lines) = self.read_log().await?;
        let next_seq = events.len() as u64;
        let path = self.log_path();
        let log = fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .await
            .map_err(|source| StoreError::io(&path, source))?;
        // Shrinking to the whole-lines length heals any torn tail; on
        // a healthy log it changes nothing.
        log.set_len(whole_lines)
            .await
            .map_err(|source| StoreError::io(&path, source))?;
        Ok(FileEventSink {
            run_id: self.meta.run_id.clone(),
            log_path: path,
            state_path: self.state_path(),
            inner: Mutex::new(SinkInner { log, next_seq }),
        })
    }

    fn log_path(&self) -> PathBuf {
        self.root.join(EVENT_LOG_FILE)
    }

    fn state_path(&self) -> PathBuf {
        self.root.join(STATE_FILE)
    }

    fn meta_path(&self) -> PathBuf {
        self.root.join(META_FILE)
    }
}

/// The state a run's log reduces to: the last `state_changed` it
/// carries, or [`INITIAL_STATE`] while none has landed. The single
/// definition of how a run's state is read back — shared by
/// [`RunDir::state`] and the daemon's status projection, so a run
/// cannot read one state through the store and another through the API.
pub fn reduce_state(events: &[EventEnvelope]) -> RunState {
    events
        .iter()
        .rev()
        .find_map(|envelope| match envelope.event {
            RunEvent::StateChanged { state } => Some(state),
            _ => None,
        })
        .unwrap_or(INITIAL_STATE)
}

/// The file-backed [`EventSink`]: each event becomes one enveloped
/// JSON line — sequence number, run id, timestamp — appended and
/// flushed before the emit returns. The envelope written here is the
/// exact shape the daemon later streams, so serving a client is
/// tailing this file.
pub struct FileEventSink {
    run_id: RunId,
    log_path: PathBuf,
    state_path: PathBuf,
    inner: Mutex<SinkInner>,
}

struct SinkInner {
    log: fs::File,
    next_seq: u64,
}

#[async_trait]
impl EventSink for FileEventSink {
    async fn emit(&self, event: RunEvent) -> Result<(), EventSinkError> {
        let mut inner = self.inner.lock().await;
        let envelope = EventEnvelope {
            seq: inner.next_seq,
            run_id: self.run_id.as_str().to_owned(),
            at: now_rfc3339(),
            event,
        };
        let mut line = serde_json::to_string(&envelope)
            .map_err(|error| StoreError::encode(&self.log_path, error))?;
        line.push('\n');
        inner
            .log
            .write_all(line.as_bytes())
            .await
            .map_err(|source| StoreError::io(&self.log_path, source))?;
        inner
            .log
            .flush()
            .await
            .map_err(|source| StoreError::io(&self.log_path, source))?;
        inner.next_seq += 1;

        // Refresh the human-readable mirror; the log line above stays
        // the authoritative record.
        if let RunEvent::StateChanged { state } = &envelope.event {
            write_json(&self.state_path, state).await?;
        }
        Ok(())
    }
}

/// Sink failures speak `StoreError` internally; the seam's error is a
/// plain string, so the store's vocabulary crosses it once, here.
impl From<StoreError> for EventSinkError {
    fn from(error: StoreError) -> Self {
        Self(error.to_string())
    }
}

/// A store failure. `NotFound` and `AlreadyExists` are the two a host
/// answers for (no such run; duplicate run id) — everything else is
/// run-fatal infrastructure.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("no such run: {0}")]
    NotFound(RunId),
    #[error("run already exists: {0}")]
    AlreadyExists(RunId),
    #[error("run store I/O on {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("corrupt run record {path}: {detail}")]
    Corrupt { path: PathBuf, detail: String },
    #[error("cannot encode {path}: {detail}")]
    Encode { path: PathBuf, detail: String },
}

impl StoreError {
    fn io(path: &Path, source: std::io::Error) -> Self {
        Self::Io {
            path: path.to_path_buf(),
            source,
        }
    }

    fn corrupt(path: &Path, detail: impl ToString) -> Self {
        Self::Corrupt {
            path: path.to_path_buf(),
            detail: detail.to_string(),
        }
    }

    fn encode(path: &Path, detail: impl ToString) -> Self {
        Self::Encode {
            path: path.to_path_buf(),
            detail: detail.to_string(),
        }
    }
}

fn now_rfc3339() -> String {
    jiff::Timestamp::now().to_string()
}

/// Pretty-printed with a trailing newline — these files exist to be
/// cat'ed. Written to a sibling and renamed so a reader never sees a
/// half-written mirror.
async fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<(), StoreError> {
    let mut contents =
        serde_json::to_string_pretty(value).map_err(|error| StoreError::encode(path, error))?;
    contents.push('\n');
    let tmp = path.with_extension("json.tmp");
    fs::write(&tmp, contents)
        .await
        .map_err(|source| StoreError::io(&tmp, source))?;
    fs::rename(&tmp, path)
        .await
        .map_err(|source| StoreError::io(path, source))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::run::PauseReason;

    async fn created(runs_root: &Path) -> RunDir {
        RunDir::create(runs_root, RunId::new("r1"), "pipeline", "claude")
            .await
            .unwrap()
    }

    fn started() -> RunEvent {
        RunEvent::RunStarted {
            kernel: "pipeline".into(),
            agent: "claude".into(),
        }
    }

    fn paused() -> RunEvent {
        RunEvent::StateChanged {
            state: RunState::Paused {
                reason: PauseReason::Drift,
            },
        }
    }

    #[tokio::test]
    async fn creating_a_run_lays_down_the_directory_of_plain_files() {
        let root = tempfile::tempdir().unwrap();
        let dir = created(root.path()).await;

        let meta_raw = std::fs::read_to_string(dir.path().join(META_FILE)).unwrap();
        let meta: RunMeta = serde_json::from_str(&meta_raw).unwrap();
        assert_eq!(meta, *dir.meta());
        assert_eq!(meta.run_id, RunId::new("r1"));
        assert_eq!(meta.kernel, "pipeline");
        assert_eq!(meta.agent, "claude");
        meta.created_at
            .parse::<jiff::Timestamp>()
            .expect("created_at is RFC 3339");

        let state_raw = std::fs::read_to_string(dir.path().join(STATE_FILE)).unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&state_raw).unwrap(),
            json!({"state": "running"})
        );

        let log = std::fs::read_to_string(dir.path().join(EVENT_LOG_FILE)).unwrap();
        assert_eq!(log, "");
    }

    #[tokio::test]
    async fn a_duplicate_run_id_is_rejected() {
        let root = tempfile::tempdir().unwrap();
        created(root.path()).await;
        let error = RunDir::create(root.path(), RunId::new("r1"), "pipeline", "claude")
            .await
            .unwrap_err();
        assert!(matches!(error, StoreError::AlreadyExists(id) if id == RunId::new("r1")));
    }

    #[tokio::test]
    async fn opening_a_missing_run_is_not_found() {
        let root = tempfile::tempdir().unwrap();
        let error = RunDir::open(root.path(), &RunId::new("ghost"))
            .await
            .unwrap_err();
        assert!(matches!(error, StoreError::NotFound(id) if id == RunId::new("ghost")));
    }

    #[tokio::test]
    async fn events_append_as_one_json_object_per_line_with_monotonic_ids() {
        let root = tempfile::tempdir().unwrap();
        let dir = created(root.path()).await;
        let sink = dir.event_sink().await.unwrap();

        sink.emit(started()).await.unwrap();
        sink.emit(RunEvent::IterationStarted { iteration: 1 })
            .await
            .unwrap();
        sink.emit(paused()).await.unwrap();

        let raw = std::fs::read_to_string(dir.path().join(EVENT_LOG_FILE)).unwrap();
        let lines: Vec<&str> = raw.lines().collect();
        assert_eq!(lines.len(), 3);
        for (position, line) in lines.iter().enumerate() {
            let envelope: EventEnvelope = serde_json::from_str(line).unwrap();
            assert_eq!(envelope.seq, position as u64);
            assert_eq!(envelope.run_id, "r1");
            envelope
                .at
                .parse::<jiff::Timestamp>()
                .expect("timestamp is RFC 3339");
        }
    }

    #[tokio::test]
    async fn a_state_change_refreshes_the_state_mirror() {
        let root = tempfile::tempdir().unwrap();
        let dir = created(root.path()).await;
        let sink = dir.event_sink().await.unwrap();

        sink.emit(paused()).await.unwrap();

        let raw = std::fs::read_to_string(dir.path().join(STATE_FILE)).unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&raw).unwrap(),
            json!({"state": "paused", "reason": "drift"})
        );
    }

    #[tokio::test]
    async fn a_restart_reconstructs_state_and_history_from_disk_alone() {
        let root = tempfile::tempdir().unwrap();
        let run_id = RunId::new("r1");
        {
            let dir = created(root.path()).await;
            let sink = dir.event_sink().await.unwrap();
            sink.emit(started()).await.unwrap();
            sink.emit(RunEvent::IterationStarted { iteration: 1 })
                .await
                .unwrap();
            sink.emit(paused()).await.unwrap();
        }

        let dir = RunDir::open(root.path(), &run_id).await.unwrap();
        assert_eq!(dir.meta().kernel, "pipeline");

        let events = dir.events().await.unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].event, started());
        assert_eq!(events[2].event, paused());

        assert_eq!(
            dir.state().await.unwrap(),
            RunState::Paused {
                reason: PauseReason::Drift
            }
        );
    }

    #[tokio::test]
    async fn a_reopened_sink_continues_the_sequence() {
        let root = tempfile::tempdir().unwrap();
        let run_id = RunId::new("r1");
        {
            let dir = created(root.path()).await;
            let sink = dir.event_sink().await.unwrap();
            sink.emit(started()).await.unwrap();
            sink.emit(paused()).await.unwrap();
        }

        let dir = RunDir::open(root.path(), &run_id).await.unwrap();
        let sink = dir.event_sink().await.unwrap();
        sink.emit(RunEvent::RunResumed { note: None })
            .await
            .unwrap();

        let events = dir.events().await.unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[2].seq, 2);
        assert_eq!(events[2].event, RunEvent::RunResumed { note: None });
    }

    #[tokio::test]
    async fn a_run_with_no_state_change_yet_is_running() {
        let root = tempfile::tempdir().unwrap();
        let dir = created(root.path()).await;
        let sink = dir.event_sink().await.unwrap();
        sink.emit(started()).await.unwrap();

        assert_eq!(dir.state().await.unwrap(), RunState::Running);
    }

    /// An append cut short by a crash or a full disk leaves a torn
    /// tail. The event it would have carried was never acknowledged to
    /// the kernel, so it never happened — history is every whole line.
    #[tokio::test]
    async fn a_torn_final_line_is_not_history() {
        let root = tempfile::tempdir().unwrap();
        let dir = created(root.path()).await;
        let sink = dir.event_sink().await.unwrap();
        sink.emit(started()).await.unwrap();
        drop(sink);
        let log_path = dir.path().join(EVENT_LOG_FILE);
        let mut raw = std::fs::read(&log_path).unwrap();
        raw.extend_from_slice(br#"{"seq":1,"run_id":"r1","at":"20"#);
        std::fs::write(&log_path, raw).unwrap();

        let events = dir.events().await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event, started());
        assert_eq!(dir.state().await.unwrap(), RunState::Running);
    }

    #[tokio::test]
    async fn the_sink_heals_a_torn_tail_before_appending() {
        let root = tempfile::tempdir().unwrap();
        let dir = created(root.path()).await;
        let sink = dir.event_sink().await.unwrap();
        sink.emit(started()).await.unwrap();
        drop(sink);
        let log_path = dir.path().join(EVENT_LOG_FILE);
        let mut raw = std::fs::read(&log_path).unwrap();
        // A torn tail need not even be text — the crash can cut a
        // multi-byte character in half.
        raw.extend_from_slice(&[0xff, 0xfe]);
        std::fs::write(&log_path, raw).unwrap();

        let sink = dir.event_sink().await.unwrap();
        sink.emit(paused()).await.unwrap();

        let events = dir.events().await.unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[1].seq, 1);
        assert_eq!(events[1].event, paused());
        // Strictly whole lines again: nothing of the torn tail remains.
        let raw = std::fs::read_to_string(&log_path).unwrap();
        assert_eq!(raw.lines().count(), 2);
    }

    /// The extreme torn tail: the very first append was cut short, so
    /// the whole file is one unfinished line and history is empty.
    #[tokio::test]
    async fn a_log_that_is_all_torn_tail_heals_to_empty() {
        let root = tempfile::tempdir().unwrap();
        let dir = created(root.path()).await;
        let log_path = dir.path().join(EVENT_LOG_FILE);
        std::fs::write(&log_path, br#"{"seq":0,"run_"#).unwrap();

        assert_eq!(dir.events().await.unwrap(), vec![]);

        let sink = dir.event_sink().await.unwrap();
        sink.emit(started()).await.unwrap();
        let events = dir.events().await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].seq, 0);
    }

    #[tokio::test]
    async fn a_corrupt_log_line_is_an_error_naming_the_file() {
        let root = tempfile::tempdir().unwrap();
        let dir = created(root.path()).await;
        std::fs::write(dir.path().join(EVENT_LOG_FILE), "not json\n").unwrap();

        let error = dir.events().await.unwrap_err();
        assert!(matches!(error, StoreError::Corrupt { .. }));
        assert!(error.to_string().contains(EVENT_LOG_FILE), "{error}");
        assert!(error.to_string().contains("line 1"), "{error}");
    }

    #[tokio::test]
    async fn a_log_lying_about_its_ids_is_corrupt() {
        let root = tempfile::tempdir().unwrap();
        let dir = created(root.path()).await;
        let forged = serde_json::to_string(&EventEnvelope {
            seq: 7,
            run_id: "r1".into(),
            at: "2026-07-13T09:00:00Z".into(),
            event: started(),
        })
        .unwrap();
        std::fs::write(dir.path().join(EVENT_LOG_FILE), forged + "\n").unwrap();

        let error = dir.events().await.unwrap_err();
        assert!(error.to_string().contains("out of sequence"), "{error}");
    }

    #[tokio::test]
    async fn an_event_from_another_run_is_corrupt() {
        let root = tempfile::tempdir().unwrap();
        let dir = created(root.path()).await;
        let stray = serde_json::to_string(&EventEnvelope {
            seq: 0,
            run_id: "r2".into(),
            at: "2026-07-13T09:00:00Z".into(),
            event: started(),
        })
        .unwrap();
        std::fs::write(dir.path().join(EVENT_LOG_FILE), stray + "\n").unwrap();

        let error = dir.events().await.unwrap_err();
        assert!(error.to_string().contains("belongs to run `r2`"), "{error}");
    }

    #[tokio::test]
    async fn metadata_naming_another_run_is_corrupt() {
        let root = tempfile::tempdir().unwrap();
        let dir = created(root.path()).await;
        let meta_path = dir.path().join(META_FILE);
        let stray = std::fs::read_to_string(&meta_path)
            .unwrap()
            .replace("\"r1\"", "\"r2\"");
        std::fs::write(&meta_path, stray).unwrap();

        let error = RunDir::open(root.path(), &RunId::new("r1"))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("names run `r2`"), "{error}");
    }
}
