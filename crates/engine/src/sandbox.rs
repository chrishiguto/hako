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

/// Decodes a stream of raw chunks whose boundaries can split a
/// multibyte codepoint: the incomplete sequence a chunk ends with is
/// held — at most three bytes — and completed by the next chunk, so a
/// split codepoint decodes whole instead of as two replacement
/// characters. Truly invalid bytes still fall through to the lossy
/// replacement, exactly as [`into_text`] leaves them.
#[derive(Default)]
pub(crate) struct StreamDecoder {
    held: Vec<u8>,
}

impl StreamDecoder {
    /// Decodes `bytes` in the context of the previous chunk's held
    /// tail and returns the text that is complete so far.
    pub(crate) fn push(&mut self, bytes: Vec<u8>) -> String {
        let mut bytes = if self.held.is_empty() {
            bytes
        } else {
            let mut joined = std::mem::take(&mut self.held);
            joined.extend_from_slice(&bytes);
            joined
        };
        self.held = bytes.split_off(bytes.len() - incomplete_utf8_suffix(&bytes));
        into_text(bytes)
    }

    /// The stream is over: an incomplete sequence can no longer
    /// complete, so it decodes lossily — output is never dropped.
    pub(crate) fn finish(self) -> String {
        into_text(self.held)
    }
}

/// The length of the longest suffix of `bytes` that is an incomplete —
/// but so far valid — UTF-8 sequence a next chunk could complete. Zero
/// when the tail is complete or already invalid; a next chunk fixes
/// neither.
fn incomplete_utf8_suffix(bytes: &[u8]) -> usize {
    for back in 1..=bytes.len().min(3) {
        let byte = bytes[bytes.len() - back];
        // A continuation byte: the sequence's lead sits further back.
        if byte & 0b1100_0000 == 0b1000_0000 {
            continue;
        }
        let need = match byte {
            0b1100_0000..=0b1101_1111 => 2,
            0b1110_0000..=0b1110_1111 => 3,
            0b1111_0000..=0b1111_0111 => 4,
            // ASCII, or a lead no valid sequence starts with.
            _ => return 0,
        };
        return if need > back { back } else { 0 };
    }
    0
}

/// A sandbox operation that failed. Opaque by design: kernels react to
/// sandbox failure uniformly (fail the iteration), never to its cause.
#[derive(Debug, Clone, thiserror::Error)]
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

    /// A chunk boundary can land inside a codepoint; the held bytes
    /// complete on the next chunk instead of decoding as two
    /// replacement characters.
    #[test]
    fn a_codepoint_split_across_chunks_decodes_whole() {
        let mut decoder = StreamDecoder::default();
        let mut out = String::new();
        // "naïve" chopped inside the two-byte 'ï'.
        for chunk in [&b"na\xC3"[..], &b"\xAFve"[..]] {
            out.push_str(&decoder.push(chunk.to_vec()));
        }
        out.push_str(&decoder.finish());
        assert_eq!(out, "na\u{ef}ve");
    }

    /// Bytes no next chunk can complete — a bad lead, a lone
    /// continuation — are not held: they decode lossily right away,
    /// byte for byte what [`into_text`] produces.
    #[test]
    fn invalid_bytes_are_not_held() {
        let mut decoder = StreamDecoder::default();
        assert_eq!(decoder.push(b"\xFFa".to_vec()), "\u{fffd}a");
        assert_eq!(decoder.push(b"\x80b".to_vec()), "\u{fffd}b");
        assert_eq!(decoder.finish(), "");
    }

    /// The stream ending settles a held tail: incomplete for good, it
    /// decodes lossily rather than vanishing.
    #[test]
    fn an_incomplete_tail_flushes_lossily_at_stream_end() {
        let mut decoder = StreamDecoder::default();
        assert_eq!(decoder.push(b"a\xC3".to_vec()), "a");
        assert_eq!(decoder.finish(), "\u{fffd}");
    }

    /// A four-byte sequence can straddle three chunks; the held tail
    /// carries across every boundary until the codepoint completes.
    #[test]
    fn a_four_byte_codepoint_survives_two_boundaries() {
        let mut decoder = StreamDecoder::default();
        let mut out = String::new();
        // "🦀" (F0 9F A6 80) one byte at a time.
        for chunk in [&b"\xF0"[..], &b"\x9F"[..], &b"\xA6"[..], &b"\x80"[..]] {
            out.push_str(&decoder.push(chunk.to_vec()));
        }
        out.push_str(&decoder.finish());
        assert_eq!(out, "\u{1f980}");
    }
}
