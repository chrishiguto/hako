//! Fakes for the sandbox seam. Three, because tests need three
//! relationships to a sandbox: none at all ([`NoSandbox`]), scripted
//! transcripts over in-memory files ([`ScriptedSandbox`]), and a
//! staged agent working a real on-disk workspace ([`StagedSandbox`]).

use std::collections::{BTreeMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, Ordering};

use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use tokio::sync::Barrier;

use super::fakes::{AGENT_BIN, Transcript, exec, prompt_from};
use crate::sandbox::{ExecSpec, ExecStream, Sandbox, SandboxError, SandboxHandle, SandboxSpec};
use crate::workspace::{GUEST_ROOT, REPORT_FILE};

/// A sandbox that must never be touched — for tests whose kernel boots
/// nothing, where any call is a test bug. Only `preflight` answers:
/// hosts probe it before any run exists.
pub struct NoSandbox;

#[async_trait]
impl Sandbox for NoSandbox {
    async fn create(&self, _spec: &SandboxSpec) -> Result<SandboxHandle, SandboxError> {
        unreachable!("this test boots no sandbox");
    }

    async fn exec_stream(
        &self,
        _sandbox: &SandboxHandle,
        _command: &ExecSpec,
    ) -> Result<ExecStream, SandboxError> {
        unreachable!("this test execs nothing");
    }

    async fn put_file(
        &self,
        _sandbox: &SandboxHandle,
        _path: &Path,
        _contents: &[u8],
    ) -> Result<(), SandboxError> {
        unreachable!("this test uploads nothing");
    }

    async fn get_file(
        &self,
        _sandbox: &SandboxHandle,
        _path: &Path,
    ) -> Result<Vec<u8>, SandboxError> {
        unreachable!("this test reads nothing");
    }

    async fn remove_file(
        &self,
        _sandbox: &SandboxHandle,
        _path: &Path,
    ) -> Result<(), SandboxError> {
        unreachable!("this test removes nothing");
    }

    async fn destroy(&self, _sandbox: SandboxHandle) -> Result<(), SandboxError> {
        unreachable!("this test boots no sandbox");
    }

    async fn preflight(&self) -> Result<(), SandboxError> {
        Ok(())
    }
}

/// Boots nothing: hands out handles, replays scripted transcripts in
/// order, records every exec, and serves files from one in-memory map.
///
/// Files planted with [`Self::seed_file`] exist up front; files
/// registered with [`Self::write_on_exec`] land in the same map the
/// moment an exec streams — output that exec "wrote", so a test proves
/// a stale-file clear ran before the exec, not merely at some point.
#[derive(Default)]
pub struct ScriptedSandbox {
    script: Mutex<VecDeque<Transcript>>,
    /// Served when the script runs dry; `None` makes an unscripted
    /// exec a loud panic instead.
    fallback: Option<Transcript>,
    files: Mutex<BTreeMap<PathBuf, Vec<u8>>>,
    /// Per path, the contents successive execs write; the last entry
    /// repeats once the queue is down to it.
    on_exec: Mutex<BTreeMap<PathBuf, VecDeque<Vec<u8>>>>,
    execs: Mutex<Vec<ExecSpec>>,
    /// Every exec waits here before streaming — how a test holds two
    /// runs in flight at once to observe their overlap.
    barrier: Option<Arc<Barrier>>,
    /// Execs begin and never finish — see [`Self::hanging`].
    hang: bool,
    panic_on_create: bool,
    created: AtomicU32,
    destroyed: AtomicU32,
    active: AtomicU32,
    max_active: AtomicU32,
}

impl ScriptedSandbox {
    /// Replays `script` in order; an exec beyond its end panics.
    pub fn scripted(script: Vec<Transcript>) -> Self {
        Self {
            script: Mutex::new(script.into()),
            ..Self::default()
        }
    }

    /// Serves the same transcript on every exec, forever.
    pub fn repeating(transcript: Transcript) -> Self {
        Self {
            fallback: Some(transcript),
            ..Self::default()
        }
    }

    /// Serves execs that begin and never finish — the minutes-long
    /// agent exec in miniature, for cancellation tests. Pair with
    /// [`Self::with_barrier`] so the test knows the exec is in flight
    /// before it fires the cancel.
    pub fn hanging() -> Self {
        Self {
            hang: true,
            ..Self::default()
        }
    }

    /// A sandbox that panics on `create` — for proving a host survives
    /// an engine task blowing up under it.
    pub fn panicking(mut self) -> Self {
        self.panic_on_create = true;
        self
    }

    pub fn with_barrier(mut self, barrier: Arc<Barrier>) -> Self {
        self.barrier = Some(barrier);
        self
    }

    /// Plants a mutable guest file — visible to `get_file`, gone after
    /// `remove_file`.
    pub fn seed_file(&self, path: impl Into<PathBuf>, contents: impl Into<Vec<u8>>) {
        self.files
            .lock()
            .unwrap()
            .insert(path.into(), contents.into());
    }

    /// Plants a guest file the moment any exec streams — output that
    /// exec "wrote". Successive calls for the same path queue up for
    /// successive execs, and the last registered contents repeat
    /// forever — so one call scripts an agent that always writes the
    /// same file, two script one that writes differently after a
    /// re-prompt. The write lands in the same removable map as
    /// [`Self::seed_file`], so a stale-file clear before the exec is
    /// observable and a clear after it is too. The map is shared
    /// across every handle: concurrent runs removing and fetching the
    /// same path must synchronize their execs (a barrier) or their
    /// interleavings race.
    pub fn write_on_exec(&self, path: impl Into<PathBuf>, contents: impl Into<Vec<u8>>) {
        self.on_exec
            .lock()
            .unwrap()
            .entry(path.into())
            .or_default()
            .push_back(contents.into());
    }

    /// Registers what the next exec "writes" at the guest report path
    /// — the report a real agent would leave for the kernel to fetch.
    /// Queued like [`Self::write_on_exec`]: the last registered report
    /// repeats.
    pub fn write_report_on_exec(&self, report: impl Into<Vec<u8>>) {
        self.write_on_exec(Path::new(GUEST_ROOT).join(REPORT_FILE), report);
    }

    /// Every exec that ran, argv-exact, in order.
    pub fn execs(&self) -> Vec<ExecSpec> {
        self.execs.lock().unwrap().clone()
    }

    pub fn created(&self) -> u32 {
        self.created.load(Ordering::SeqCst)
    }

    pub fn destroyed(&self) -> u32 {
        self.destroyed.load(Ordering::SeqCst)
    }

    /// The most sandboxes ever alive at once.
    pub fn max_active(&self) -> u32 {
        self.max_active.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl Sandbox for ScriptedSandbox {
    async fn create(&self, _spec: &SandboxSpec) -> Result<SandboxHandle, SandboxError> {
        assert!(!self.panic_on_create, "scripted sandbox panic");
        let n = self.created.fetch_add(1, Ordering::SeqCst);
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_active.fetch_max(active, Ordering::SeqCst);
        Ok(SandboxHandle::new(format!("vm-{n}")))
    }

    async fn exec_stream(
        &self,
        _sandbox: &SandboxHandle,
        command: &ExecSpec,
    ) -> Result<ExecStream, SandboxError> {
        self.execs.lock().unwrap().push(command.clone());
        // A hanging exec writes nothing and never exits: the stream
        // stays pending until the sandbox is torn down around it.
        if self.hang {
            if let Some(barrier) = &self.barrier {
                barrier.wait().await;
            }
            return Ok(stream::pending().boxed());
        }
        for (path, queue) in self.on_exec.lock().unwrap().iter_mut() {
            // Advance the queue only while another entry waits behind;
            // the last one serves every exec from then on.
            let contents = if queue.len() > 1 {
                queue.pop_front().unwrap()
            } else {
                queue.front().cloned().unwrap()
            };
            self.seed_file(path.clone(), contents);
        }
        let transcript = {
            let mut script = self.script.lock().unwrap();
            script
                .pop_front()
                .or_else(|| self.fallback.clone())
                .expect("an exec ran beyond its script")
        };
        if let Some(barrier) = &self.barrier {
            barrier.wait().await;
        }
        Ok(stream::iter(transcript).boxed())
    }

    async fn put_file(
        &self,
        _sandbox: &SandboxHandle,
        path: &Path,
        contents: &[u8],
    ) -> Result<(), SandboxError> {
        self.seed_file(path, contents);
        Ok(())
    }

    async fn get_file(
        &self,
        _sandbox: &SandboxHandle,
        path: &Path,
    ) -> Result<Vec<u8>, SandboxError> {
        self.files
            .lock()
            .unwrap()
            .get(path)
            .cloned()
            .ok_or_else(|| SandboxError(format!("no such file: {}", path.display())))
    }

    async fn remove_file(&self, _sandbox: &SandboxHandle, path: &Path) -> Result<(), SandboxError> {
        self.files.lock().unwrap().remove(path);
        Ok(())
    }

    async fn destroy(&self, _sandbox: SandboxHandle) -> Result<(), SandboxError> {
        self.destroyed.fetch_add(1, Ordering::SeqCst);
        self.active.fetch_sub(1, Ordering::SeqCst);
        Ok(())
    }

    async fn preflight(&self) -> Result<(), SandboxError> {
        Ok(())
    }
}

/// One agent exec [`StagedSandbox`] will serve: what it prints, how it
/// exits, and the report it leaves (or `None` to leave none, so the
/// kernel sees a missing report).
pub struct AgentStep {
    pub stdout: String,
    pub code: i32,
    pub report: Option<String>,
}

/// A clean attempt: the agent works, exits zero, and leaves this
/// report.
pub fn reports(status: &str, summary: &str) -> AgentStep {
    AgentStep {
        stdout: "working\n".into(),
        code: 0,
        report: Some(serde_json::json!({"status": status, "summary": summary}).to_string()),
    }
}

/// An attempt that leaves a report serde will reject — an unknown field
/// — so the kernel offers its one repair.
pub fn malformed() -> AgentStep {
    AgentStep {
        stdout: "working\n".into(),
        code: 0,
        report: Some(r#"{"status": "continue", "summary": "x", "mystery": 1}"#.into()),
    }
}

/// An attempt that crashes: non-zero exit, nothing to trust.
pub fn crashes() -> AgentStep {
    AgentStep {
        stdout: "boom\n".into(),
        code: 1,
        report: None,
    }
}

/// A clean agent exit that does not touch the shared report path.
pub fn omits_report() -> AgentStep {
    AgentStep {
        stdout: "forgot to report\n".into(),
        code: 0,
        report: None,
    }
}

/// A fake sandbox over a real workspace. Serves a queue of agent steps
/// and a queue of verify-check exit codes; an exhausted check queue
/// passes by default, so only failures need scripting. Each agent exec
/// drops a unique work file into the workspace — so a mutating stage's
/// checkpoint has something to commit — and lays down the step's
/// report under the scratch dir. Verify checks are separate execs,
/// told apart from the agent by their argv.
pub struct StagedSandbox {
    workspace_root: PathBuf,
    agent_steps: Mutex<VecDeque<AgentStep>>,
    checks: Mutex<VecDeque<i32>>,
    agent_prompts: Mutex<Vec<String>>,
    guest_files: Mutex<BTreeMap<PathBuf, Vec<u8>>>,
    created: AtomicU32,
    destroyed: AtomicU32,
    work_files: AtomicU32,
}

impl StagedSandbox {
    pub fn new(workspace_root: PathBuf, agent_steps: Vec<AgentStep>) -> Self {
        Self {
            workspace_root,
            agent_steps: Mutex::new(agent_steps.into()),
            checks: Mutex::new(VecDeque::new()),
            agent_prompts: Mutex::new(Vec::new()),
            guest_files: Mutex::new(BTreeMap::new()),
            created: AtomicU32::new(0),
            destroyed: AtomicU32::new(0),
            work_files: AtomicU32::new(0),
        }
    }

    /// Scripts the verify-check exit codes; without this every check
    /// passes.
    pub fn with_checks(self, checks: Vec<i32>) -> Self {
        *self.checks.lock().unwrap() = checks.into();
        self
    }

    /// Every prompt the agent was invoked with, in order.
    pub fn agent_prompts(&self) -> Vec<String> {
        self.agent_prompts.lock().unwrap().clone()
    }

    /// Plants a guest file served ahead of the host filesystem — how a
    /// test makes the guest's view diverge from the host's.
    pub fn seed_guest_file(&self, path: impl Into<PathBuf>, contents: impl Into<Vec<u8>>) {
        self.guest_files
            .lock()
            .unwrap()
            .insert(path.into(), contents.into());
    }

    pub fn created(&self) -> u32 {
        self.created.load(Ordering::SeqCst)
    }

    pub fn destroyed(&self) -> u32 {
        self.destroyed.load(Ordering::SeqCst)
    }

    /// Maps a guest path back to the real host workspace — the fake's
    /// stand-in for the mount every real sandbox shares.
    fn host_path(&self, guest: &Path) -> PathBuf {
        let relative = guest.strip_prefix(GUEST_ROOT).unwrap_or(guest);
        self.workspace_root.join(relative)
    }

    /// Serves one agent exec: record its prompt, drop a unique work
    /// file (real change for the checkpoint), and lay down or clear the
    /// report the kernel will fetch.
    fn run_agent(&self, command: &ExecSpec) -> Transcript {
        let step = self
            .agent_steps
            .lock()
            .unwrap()
            .pop_front()
            .expect("an agent exec ran beyond its script");
        self.agent_prompts.lock().unwrap().push(
            prompt_from(&command.argv)
                .expect("agent exec argv does not match ScriptedAgent's encoding")
                .to_owned(),
        );

        let n = self.work_files.fetch_add(1, Ordering::SeqCst);
        std::fs::write(self.workspace_root.join(format!("work-{n}.txt")), "work\n").unwrap();

        let report_path = self.workspace_root.join(REPORT_FILE);
        // Leave a previous report untouched when the agent omits one:
        // freshness belongs to the invocation executor, not this fake.
        if let Some(report) = &step.report {
            std::fs::create_dir_all(report_path.parent().unwrap()).unwrap();
            std::fs::write(&report_path, report).unwrap();
        }
        exec(&step.stdout, step.code)
    }

    /// Serves one verify check: its scripted exit code, or a pass when
    /// the queue is empty.
    fn run_check(&self) -> Transcript {
        let code = self.checks.lock().unwrap().pop_front().unwrap_or(0);
        let line = if code == 0 {
            "ok\n"
        } else {
            "assertion failed: boom\n"
        };
        exec(line, code)
    }
}

#[async_trait]
impl Sandbox for StagedSandbox {
    async fn create(&self, _spec: &SandboxSpec) -> Result<SandboxHandle, SandboxError> {
        let n = self.created.fetch_add(1, Ordering::SeqCst);
        Ok(SandboxHandle::new(format!("vm-{n}")))
    }

    async fn exec_stream(
        &self,
        _sandbox: &SandboxHandle,
        command: &ExecSpec,
    ) -> Result<ExecStream, SandboxError> {
        let transcript = if command.argv.first().is_some_and(|arg| arg == AGENT_BIN) {
            self.run_agent(command)
        } else {
            self.run_check()
        };
        Ok(stream::iter(transcript).boxed())
    }

    async fn put_file(
        &self,
        _sandbox: &SandboxHandle,
        _path: &Path,
        _contents: &[u8],
    ) -> Result<(), SandboxError> {
        unreachable!("staged kernels pass prompts as argv, never as files");
    }

    async fn get_file(
        &self,
        _sandbox: &SandboxHandle,
        path: &Path,
    ) -> Result<Vec<u8>, SandboxError> {
        if let Some(contents) = self.guest_files.lock().unwrap().get(path).cloned() {
            return Ok(contents);
        }
        std::fs::read(self.host_path(path))
            .map_err(|error| SandboxError(format!("no such file {}: {error}", path.display())))
    }

    async fn remove_file(&self, _sandbox: &SandboxHandle, path: &Path) -> Result<(), SandboxError> {
        match std::fs::remove_file(self.host_path(path)) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(SandboxError(format!(
                "cannot remove {}: {error}",
                path.display()
            ))),
        }
    }

    async fn destroy(&self, _sandbox: SandboxHandle) -> Result<(), SandboxError> {
        self.destroyed.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn preflight(&self) -> Result<(), SandboxError> {
        Ok(())
    }
}
