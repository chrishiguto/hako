//! The sandbox seam — the hardware-isolated environment an iteration
//! executes in, created fresh and destroyed with it. Only the mounted
//! workspace survives.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use futures_core::stream::BoxStream;

use crate::secrets::SecretValue;

/// Creates, drives, and destroys isolated execution environments.
/// Backends: a microVM in production, a scripted fake in tests.
#[async_trait]
pub trait Sandbox: Send + Sync {
    /// Boots one environment described by `spec`.
    async fn create(&self, spec: &SandboxSpec) -> Result<SandboxHandle, SandboxError>;

    /// Starts `command` inside the sandbox and returns its output as
    /// it happens. The stream yields stdout/stderr chunks and ends
    /// with exactly one `Exited`.
    async fn exec_stream(
        &self,
        sandbox: &SandboxHandle,
        command: &ExecSpec,
    ) -> Result<ExecStream, SandboxError>;

    /// Writes a file inside the sandbox.
    async fn put_file(
        &self,
        sandbox: &SandboxHandle,
        path: &Path,
        contents: &[u8],
    ) -> Result<(), SandboxError>;

    /// Reads a file from inside the sandbox.
    async fn get_file(&self, sandbox: &SandboxHandle, path: &Path)
    -> Result<Vec<u8>, SandboxError>;

    /// Removes a file inside the sandbox. Missing files are accepted so
    /// callers can establish a clean boundary without probing first.
    async fn remove_file(&self, sandbox: &SandboxHandle, path: &Path) -> Result<(), SandboxError>;

    /// Tears the sandbox down. Takes the handle by value: nothing can
    /// address a destroyed sandbox.
    async fn destroy(&self, sandbox: SandboxHandle) -> Result<(), SandboxError>;

    /// Confirms the backend is present and its version matches the
    /// engine's pin. Runs before any run starts — upstream drift must
    /// fail loudly here, not corrupt iterations later.
    async fn preflight(&self) -> Result<(), SandboxError>;
}

/// What a sandbox is built from: the workspace mount and the
/// environment injected at boot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxSpec {
    pub workspace: WorkspaceMount,
    /// Injected as environment variables. Values are wrapped as
    /// secrets because per-iteration env is the secret-injection
    /// channel — anything here may be sensitive, so nothing here may
    /// be printed.
    pub env: BTreeMap<String, SecretValue>,
}

/// The persistent workspace, mounted read-write into the sandbox —
/// the only channel through which an iteration's work survives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceMount {
    pub host: PathBuf,
    pub guest: PathBuf,
}

/// Names one live sandbox. Opaque to kernels; only the backend that
/// issued it can interpret it.
#[derive(Debug, PartialEq, Eq)]
pub struct SandboxHandle(String);

impl SandboxHandle {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A command to run inside a sandbox, argv-style — no shell in the
/// middle to expand, split, or inject.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecSpec {
    pub argv: Vec<String>,
    /// Defaults to the workspace mount point when `None`.
    pub cwd: Option<PathBuf>,
}

/// Live output of one command. Ends with exactly one `Exited`; an
/// `Err` item means the sandbox broke mid-command.
pub type ExecStream = BoxStream<'static, Result<ExecEvent, SandboxError>>;

/// One piece of a running command's life. Output chunks are raw bytes
/// arriving at arbitrary boundaries — they can split multibyte
/// codepoints and need not be UTF-8 at all — so decoding to text
/// happens once, in the kernel, not in every backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecEvent {
    Stdout(Vec<u8>),
    Stderr(Vec<u8>),
    Exited(ExitStatus),
}

/// How a command ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExitStatus {
    /// `None` when the process was killed before it could exit — the
    /// iteration-timeout path.
    pub code: Option<i32>,
}

impl ExitStatus {
    pub fn success(&self) -> bool {
        self.code == Some(0)
    }
}

/// Decodes one raw exec-output chunk to text — the decode [`ExecEvent`]
/// defers to its consumers. Valid UTF-8, the common case, moves without
/// a copy; invalid bytes fall back to the lossy copy, byte for byte what
/// `from_utf8_lossy` would produce.
pub(crate) fn into_text(bytes: Vec<u8>) -> String {
    String::from_utf8(bytes)
        .unwrap_or_else(|error| String::from_utf8_lossy(error.as_bytes()).into_owned())
}

/// A sandbox operation that failed. Opaque by design: kernels react to
/// sandbox failure uniformly (fail the iteration), never to its cause.
#[derive(Debug, thiserror::Error)]
#[error("sandbox failure: {0}")]
pub struct SandboxError(pub String);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_zero_exit_is_success() {
        assert!(ExitStatus { code: Some(0) }.success());
        assert!(!ExitStatus { code: Some(1) }.success());
        assert!(!ExitStatus { code: None }.success());
    }
}
