//! The smolvm-backed implementation of the engine's `Sandbox` seam:
//! ephemeral microVMs, one per iteration.
//!
//! smolvm ships no Rust SDK, so the adapter drives its CLI (ADR-0004):
//! one persistent named machine per sandbox, created with the
//! workspace mounted read-write, executed into with `machine exec
//! --stream`, and force-deleted on destroy. The version is pinned —
//! [`preflight`](Sandbox::preflight) refuses to run against any other
//! smolvm, because a young upstream's behavior changes (mount
//! semantics, exit-code propagation) would otherwise corrupt runs
//! silently.
//!
//! Dropping an [`ExecStream`] mid-command abandons the host-side exec
//! client but not the guest command, which runs until it finishes or
//! `destroy` tears the machine down — cancellation is the
//! fresh-VM-per-iteration teardown (ADR-0003), not a kill signal.

mod command;

pub use command::PINNED_SMOLVM_VERSION;

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use engine::{
    ExecEvent, ExecSpec, ExecStream, ExitStatus, Sandbox, SandboxError, SandboxHandle, SandboxSpec,
    SecretValue,
};
use futures_util::StreamExt;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::sync::mpsc;

/// How the adapter reaches smolvm and what its machines boot.
#[derive(Debug, Clone)]
pub struct SmolvmConfig {
    /// The smolvm binary; a bare name resolves through `PATH`.
    pub binary: PathBuf,
    /// OCI image the machines boot; `None` boots smolvm's bare
    /// Alpine-based rootfs, which needs no registry pull.
    pub image: Option<String>,
    /// Outbound network access for the guest. Also required to pull
    /// `image` on first boot — smolvm pulls from inside the VM.
    pub net: bool,
}

impl Default for SmolvmConfig {
    fn default() -> Self {
        Self {
            binary: PathBuf::from("smolvm"),
            image: None,
            net: false,
        }
    }
}

/// What the adapter must remember about a live machine between trait
/// calls, keyed by handle: the env to inject into every exec and the
/// workspace guest path that is the default working directory.
struct Machine {
    env: BTreeMap<String, SecretValue>,
    workspace_guest: PathBuf,
}

/// The production [`Sandbox`]: every handle is a named smolvm machine.
pub struct SmolvmSandbox {
    config: SmolvmConfig,
    /// Baked into machine names — with the PID — so they cannot
    /// collide with a machine leaked by an earlier daemon that died
    /// before destroying it, nor with a sibling started the same
    /// millisecond.
    born_ms: u128,
    counter: AtomicU64,
    machines: Mutex<HashMap<String, Arc<Machine>>>,
}

impl SmolvmSandbox {
    pub fn new(config: SmolvmConfig) -> Self {
        let born_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        Self {
            config,
            born_ms,
            counter: AtomicU64::new(0),
            machines: Mutex::new(HashMap::new()),
        }
    }

    fn command(&self, args: &[String]) -> Command {
        let mut command = Command::new(&self.config.binary);
        command.args(args).stdin(Stdio::null());
        command
    }

    /// Runs one smolvm invocation to completion, failing loudly — with
    /// the full command line and smolvm's stderr — on anything but
    /// exit 0.
    async fn run_checked(&self, args: &[String]) -> Result<std::process::Output, SandboxError> {
        let output = self
            .command(args)
            .output()
            .await
            .map_err(|error| self.spawn_error(args, &error))?;
        if !output.status.success() {
            return Err(self.command_failed(args, &output));
        }
        Ok(output)
    }

    /// The one fail-loudly shape for "smolvm ran and exited non-zero":
    /// the full command line plus smolvm's stderr, where the real
    /// diagnosis lives.
    fn command_failed(&self, args: &[String], output: &std::process::Output) -> SandboxError {
        SandboxError(format!(
            "`{} {}` failed: {}",
            self.config.binary.display(),
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        ))
    }

    fn spawn_error(&self, args: &[String], error: &std::io::Error) -> SandboxError {
        // A missing binary means the same thing at every spawn site, so
        // the install hint is not preflight-only.
        if error.kind() == std::io::ErrorKind::NotFound {
            return SandboxError(format!(
                "smolvm not found at `{}` — hako pins smolvm {PINNED_SMOLVM_VERSION}; \
                 install it and make sure it is on PATH (https://smolmachines.com)",
                self.config.binary.display()
            ));
        }
        SandboxError(format!(
            "cannot spawn `{} {}`: {error}",
            self.config.binary.display(),
            args.join(" ")
        ))
    }

    /// The live record behind a handle — an `Arc` bump, not a deep
    /// copy, so callers that only need existence (put/get) don't
    /// duplicate the secret env on every call.
    fn machine(&self, sandbox: &SandboxHandle) -> Result<Arc<Machine>, SandboxError> {
        self.machines
            .lock()
            .unwrap()
            .get(sandbox.as_str())
            .cloned()
            .ok_or_else(|| SandboxError(format!("unknown sandbox `{}`", sandbox.as_str())))
    }
}

#[async_trait]
impl Sandbox for SmolvmSandbox {
    async fn create(&self, spec: &SandboxSpec) -> Result<SandboxHandle, SandboxError> {
        for name in spec.env.keys() {
            command::validate_env_name(name)?;
        }
        command::validate_mount_host(&spec.workspace.host)?;

        let name = format!(
            "hako-{:x}-{}-{}",
            self.born_ms,
            std::process::id(),
            self.counter.fetch_add(1, Ordering::Relaxed)
        );
        self.run_checked(&command::create_args(
            &name,
            self.config.image.as_deref(),
            self.config.net,
            &spec.workspace,
        ))
        .await?;
        if let Err(error) = self.run_checked(&command::start_args(&name)).await {
            // A machine that never started must not leak its config.
            let _ = self.run_checked(&command::delete_args(&name)).await;
            return Err(error);
        }

        self.machines.lock().unwrap().insert(
            name.clone(),
            Arc::new(Machine {
                env: spec.env.clone(),
                workspace_guest: spec.workspace.guest.clone(),
            }),
        );
        Ok(SandboxHandle::new(name))
    }

    async fn exec_stream(
        &self,
        sandbox: &SandboxHandle,
        command: &ExecSpec,
    ) -> Result<ExecStream, SandboxError> {
        let machine = self.machine(sandbox)?;
        let args = command::exec_args(
            sandbox.as_str(),
            command,
            &machine.workspace_guest,
            machine.env.keys().map(String::as_str),
        );

        let mut client = self.command(&args);
        client.stdout(Stdio::piped()).stderr(Stdio::piped());
        for (name, value) in &machine.env {
            client.env(command::secret_env_host_var(name), value.expose());
        }
        let mut child = client
            .spawn()
            .map_err(|error| self.spawn_error(&args, &error))?;
        let stdout = child.stdout.take().expect("stdout was piped");
        let stderr = child.stderr.take().expect("stderr was piped");

        // Both pipes drain concurrently into one channel; `Exited` is
        // sent only after both hit EOF, so it is always the last event.
        let (tx, rx) = mpsc::channel(16);
        tokio::spawn(async move {
            tokio::join!(
                pump(stdout, ExecEvent::Stdout, tx.clone()),
                pump(stderr, ExecEvent::Stderr, tx.clone()),
            );
            // smolvm exits with the guest command's code, so the
            // child's own status is the guest status.
            let last = match child.wait().await {
                Ok(status) => Ok(ExecEvent::Exited(ExitStatus {
                    code: status.code(),
                })),
                Err(error) => Err(wait_error(error)),
            };
            let _ = tx.send(last).await;
        });

        Ok(futures_util::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|event| (event, rx))
        })
        .boxed())
    }

    async fn put_file(
        &self,
        sandbox: &SandboxHandle,
        path: &Path,
        contents: &[u8],
    ) -> Result<(), SandboxError> {
        self.machine(sandbox)?;
        command::validate_guest_path(path)?;
        let args = command::put_args(sandbox.as_str(), path);
        let mut client = self.command(&args);
        client
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        let mut child = client
            .spawn()
            .map_err(|error| self.spawn_error(&args, &error))?;

        let mut stdin = child.stdin.take().expect("stdin was piped");
        // A fast-failing `tee` breaks this pipe mid-write; the real
        // diagnosis is in smolvm's stderr, so hold the write error and
        // let the exit status speak first.
        let written = stdin.write_all(contents).await;
        // Dropping stdin is the EOF that lets `tee` finish.
        drop(stdin);

        let output = child.wait_with_output().await.map_err(wait_error)?;
        if !output.status.success() {
            return Err(self.command_failed(&args, &output));
        }
        written.map_err(|error| {
            SandboxError(format!(
                "writing {} into the sandbox: {error}",
                path.display()
            ))
        })
    }

    async fn get_file(
        &self,
        sandbox: &SandboxHandle,
        path: &Path,
    ) -> Result<Vec<u8>, SandboxError> {
        self.machine(sandbox)?;
        command::validate_guest_path(path)?;
        let output = self
            .run_checked(&command::get_args(sandbox.as_str(), path))
            .await?;
        Ok(output.stdout)
    }

    async fn destroy(&self, sandbox: SandboxHandle) -> Result<(), SandboxError> {
        // Forget the machine before deleting: the handle is consumed
        // either way, so nothing may address it again even if smolvm
        // fails here.
        self.machines.lock().unwrap().remove(sandbox.as_str());
        self.run_checked(&command::delete_args(sandbox.as_str()))
            .await?;
        Ok(())
    }

    async fn preflight(&self) -> Result<(), SandboxError> {
        let output = self.run_checked(&command::version_args()).await?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        let found = command::parse_version(&stdout).ok_or_else(|| {
            SandboxError(format!(
                "`{} --version` printed {:?}, which is not smolvm version output",
                self.config.binary.display(),
                stdout.trim()
            ))
        })?;
        if found != PINNED_SMOLVM_VERSION {
            return Err(SandboxError(format!(
                "smolvm {found} does not match the pinned {PINNED_SMOLVM_VERSION} — \
                 upstream drift must fail here, not corrupt runs; \
                 install smolvm {PINNED_SMOLVM_VERSION}"
            )));
        }
        Ok(())
    }
}

/// The wait itself failing is host-side plumbing breakage, distinct
/// from the guest command exiting non-zero.
fn wait_error(error: std::io::Error) -> SandboxError {
    SandboxError(format!("waiting for smolvm exec: {error}"))
}

/// Forwards one pipe into the event channel in raw chunks. Chunk
/// boundaries are arbitrary by contract — decoding is the kernel's
/// job, not ours.
async fn pump(
    mut reader: impl AsyncRead + Unpin,
    wrap: fn(Vec<u8>) -> ExecEvent,
    tx: mpsc::Sender<Result<ExecEvent, SandboxError>>,
) {
    let mut buffer = vec![0u8; 8192];
    loop {
        match reader.read(&mut buffer).await {
            Ok(0) => break,
            Ok(n) => {
                if tx.send(Ok(wrap(buffer[..n].to_vec()))).await.is_err() {
                    break;
                }
            }
            Err(error) => {
                let _ = tx
                    .send(Err(SandboxError(format!(
                        "reading smolvm exec output: {error}"
                    ))))
                    .await;
                break;
            }
        }
    }
}
