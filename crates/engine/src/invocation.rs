//! One agent invocation — the engine mechanism every kernel drives
//! its agent through: a fresh sandbox, the agent exec-streamed into
//! the event log, token usage accounted, and the report it left
//! fetched back and read against the kernel's [`ReportContract`].
//! What shape a report takes is kernel policy; the loop that enforces
//! it — strict parse, exactly one repair re-prompt, every rejection
//! logged — is engine mechanism and lives here, so every kernel's
//! agent-boundary read behaves identically. When to checkpoint the
//! workspace remains kernel policy.

use std::fmt::Write;

use futures_util::StreamExt;

use crate::event::{OutputStream, RunEvent};
use crate::kernel::{KernelContext, KernelError};
use crate::sandbox::{ExecEvent, SandboxHandle, SandboxSpec, StreamDecoder, into_text};
use crate::secrets::{SecretEnv, StreamScrubber};
use crate::workspace::REPORT_FILE;

/// What one agent invocation left behind.
#[derive(Debug)]
pub enum InvocationEnd {
    /// The agent exited non-zero or was killed; nothing it left can
    /// be trusted, the report included.
    Crashed,
    /// The agent exited cleanly but its report cannot be read; the
    /// message is what a repair re-prompt carries back.
    MissingReport(String),
    /// The raw report, exactly as the agent wrote it.
    /// [`invoke_to_report`] reads it against the kernel's contract.
    Reported(Vec<u8>),
}

/// The kernel-supplied half of the report boundary: one report shape —
/// its schema and its strict parse. [`invoke_to_report`] is generic
/// over this, so dialect types stay with the kernel that owns them
/// while the engine owns the repair loop.
pub trait ReportContract {
    /// What a report parses into — a kernel dialect type.
    type Report;

    /// The report's JSON Schema, quoted verbatim in the repair
    /// re-prompt so the agent corrects itself against the published
    /// contract.
    fn schema(&self) -> &str;

    /// Strict-parses the report text against the shape. The error is
    /// the human-readable rejection the repair re-prompt carries back.
    fn parse(&self, text: &str) -> Result<Self::Report, String>;
}

/// How a sandbox bracket ended: the work ran to its value, or the
/// run's cancel token fired mid-work and the rest was abandoned.
/// Cancellation changes what comes back, never whether teardown runs.
#[derive(Debug)]
pub enum Bracketed<T> {
    Finished(T),
    Cancelled,
}

/// Boots a fresh sandbox over the workspace, runs `work` inside it,
/// and destroys the sandbox on every path — neither an error nor a
/// cancellation can leak a live VM. One bracket may span several
/// execs: a repair re-prompt and the verify checks belong in the same
/// sandbox as the invocation whose work they judge. The cancel token
/// is raced against the whole bracket body, so a cancel lands even
/// mid-exec — dropping the work abandons the guest process with its
/// VM, which is the whole cancellation model: teardown, not a kill
/// signal.
pub async fn in_fresh_sandbox<T>(
    ctx: &KernelContext,
    work: impl AsyncFnOnce(&SandboxHandle) -> Result<T, KernelError>,
) -> Result<Bracketed<T>, KernelError> {
    if ctx.cancel.is_cancelled() {
        return Ok(Bracketed::Cancelled);
    }
    // Every sandbox gets the same env: the run's secrets resolved
    // once, at submit. Copied rather than re-resolved because a store
    // read here would put a run four iterations deep at the mercy of
    // the store still being up.
    let spec = SandboxSpec {
        workspace: ctx.workspace.mount(),
        env: ctx.secrets.vars().clone(),
    };
    let sandbox = ctx.sandbox.create(&spec).await?;
    // The select sits inside the bracket: however the race lands, the
    // destroy below still runs.
    let result = tokio::select! {
        result = work(&sandbox) => result.map(Bracketed::Finished),
        () = ctx.cancel.cancelled() => Ok(Bracketed::Cancelled),
    };
    let destroyed = ctx.sandbox.destroy(sandbox).await;
    let result = result?;
    destroyed?;
    Ok(result)
}

/// One output stream's path from raw bytes to loggable text. Chunks
/// arrive at arbitrary byte boundaries, and both things a boundary can
/// bisect settle here before anything is emitted: a codepoint split
/// mid-sequence reassembles, then a secret value split mid-token
/// redacts whole.
struct ScrubbedStream<'env> {
    stream: OutputStream,
    decoder: StreamDecoder,
    scrubber: StreamScrubber<'env>,
}

impl<'env> ScrubbedStream<'env> {
    fn new(stream: OutputStream, secrets: &'env SecretEnv) -> Self {
        Self {
            stream,
            decoder: StreamDecoder::default(),
            scrubber: secrets.stream_scrubber(),
        }
    }

    /// What of this chunk is safe to emit — possibly empty while a
    /// split codepoint or a would-be value sits on the boundary.
    fn push(&mut self, bytes: Vec<u8>) -> String {
        self.scrubber.push(self.decoder.push(bytes))
    }

    /// The stream is over: both held tails settle, the decoder's
    /// through the scrubber.
    fn finish(self) -> (OutputStream, String) {
        let Self {
            stream,
            decoder,
            mut scrubber,
        } = self;
        let mut tail = scrubber.push(decoder.finish());
        tail.push_str(&scrubber.finish());
        (stream, tail)
    }
}

/// Runs the agent once in the sandbox: exec-stream the invocation,
/// emit every output chunk and the token usage as events, then fetch
/// the report the agent wrote — through the sandbox seam, because
/// scratch is read through the guest's view, never the host's.
/// One exec, no enforcement — a kernel reads the agent boundary
/// through [`invoke_to_report`], which drives this.
pub async fn invoke(
    ctx: &KernelContext,
    iteration: u32,
    sandbox: &SandboxHandle,
    prompt: &str,
) -> Result<InvocationEnd, KernelError> {
    // The report path survives sandbox lifetimes with the workspace. Clear
    // it before every exec so a successful agent that forgets to report
    // cannot inherit another invocation's valid bytes.
    let report_path = ctx.workspace.guest_report_path();
    ctx.sandbox.remove_file(sandbox, &report_path).await?;

    let invocation = ctx.agent.invocation(prompt);
    let mut output = ctx.sandbox.exec_stream(sandbox, &invocation).await?;
    // Raw stdout, decoded whole once the stream ends: the adapter's
    // token-usage parse must not read a codepoint the chunk boundary
    // split as two replacement characters.
    let mut stdout = Vec::new();
    // One seam-holder per stream: the sink scrubs each event whole,
    // but chunks split at arbitrary byte boundaries — a codepoint or
    // a value bisected by one must still land whole.
    let mut stdout_seam = ScrubbedStream::new(OutputStream::Stdout, &ctx.secrets);
    let mut stderr_seam = ScrubbedStream::new(OutputStream::Stderr, &ctx.secrets);
    let mut exit = None;
    while let Some(event) = output.next().await {
        let (stream, chunk) = match event? {
            ExecEvent::Stdout(bytes) => {
                stdout.extend_from_slice(&bytes);
                (OutputStream::Stdout, stdout_seam.push(bytes))
            }
            ExecEvent::Stderr(bytes) => (OutputStream::Stderr, stderr_seam.push(bytes)),
            ExecEvent::Exited(status) => {
                exit = Some(status);
                continue;
            }
        };
        if !chunk.is_empty() {
            ctx.events
                .emit(RunEvent::AgentOutput {
                    iteration,
                    stream,
                    chunk,
                })
                .await?;
        }
    }
    // The streams are over: whatever sat on a boundary flushes now.
    for (stream, tail) in [stdout_seam.finish(), stderr_seam.finish()] {
        if !tail.is_empty() {
            ctx.events
                .emit(RunEvent::AgentOutput {
                    iteration,
                    stream,
                    chunk: tail,
                })
                .await?;
        }
    }

    let stdout = into_text(stdout);
    if let Some(usage) = ctx.agent.token_usage(&stdout) {
        ctx.events
            .emit(RunEvent::TokensUsed { iteration, usage })
            .await?;
    }

    if !exit.is_some_and(|status| status.success()) {
        return Ok(InvocationEnd::Crashed);
    }

    match ctx.sandbox.get_file(sandbox, &report_path).await {
        Ok(raw) => Ok(InvocationEnd::Reported(raw)),
        Err(error) => Ok(InvocationEnd::MissingReport(format!(
            "report missing: {error}"
        ))),
    }
}

/// Drives the agent to a parsed report, spending the one repair
/// re-prompt a rejected report earns in the same sandbox — the work is
/// done, only the report needs fixing. `None` means the invocation is
/// out of chances: a crash (no report to trust) or a report still
/// malformed after repair. Every rejection is logged for the repair to
/// answer.
pub async fn invoke_to_report<C: ReportContract>(
    ctx: &KernelContext,
    iteration: u32,
    sandbox: &SandboxHandle,
    prompt: &str,
    contract: &C,
) -> Result<Option<C::Report>, KernelError> {
    let errors = match parse(contract, invoke(ctx, iteration, sandbox, prompt).await?) {
        Parsed::Report(report) => return Ok(Some(report)),
        Parsed::Crashed => return Ok(None),
        Parsed::Rejected(errors) => errors,
    };
    let repair_prompt = repair(&errors, contract.schema());
    ctx.events
        .emit(RunEvent::ReportRejected { iteration, errors })
        .await?;
    match parse(
        contract,
        invoke(ctx, iteration, sandbox, &repair_prompt).await?,
    ) {
        Parsed::Report(report) => Ok(Some(report)),
        Parsed::Crashed => Ok(None),
        Parsed::Rejected(errors) => {
            ctx.events
                .emit(RunEvent::ReportRejected { iteration, errors })
                .await?;
            Ok(None)
        }
    }
}

/// The repair re-prompt — the one second chance a rejected report
/// earns. Deliberately bare: the work already done stays done; only
/// the report needs writing, so this carries the validation errors
/// and the report contract — the fixed scratch path and the shape the
/// kernel demands — and nothing else. The path is the same one
/// [`invoke`] fetches, by construction.
fn repair(errors: &[String], shape: &str) -> String {
    let mut text = String::from(
        "# hako report repair\n\n\
         The report you wrote was rejected:\n\n",
    );
    for error in errors {
        let _ = writeln!(text, "- {error}");
    }
    let _ = write!(
        text,
        "\nWrite a corrected `{REPORT_FILE}` in the workspace and do \
         nothing else:\n\n\
         ```json\n{shape}\n```\n",
    );
    text
}

/// The outcome of reading one invocation's report against the
/// contract.
enum Parsed<R> {
    Report(R),
    /// The agent exited badly — nothing it left can be trusted, so no
    /// repair is offered.
    Crashed,
    /// The report is missing or malformed; the errors feed the repair
    /// re-prompt.
    Rejected(Vec<String>),
}

fn parse<C: ReportContract>(contract: &C, end: InvocationEnd) -> Parsed<C::Report> {
    match end {
        InvocationEnd::Crashed => Parsed::Crashed,
        InvocationEnd::MissingReport(message) => Parsed::Rejected(vec![message]),
        InvocationEnd::Reported(raw) => match std::str::from_utf8(&raw) {
            Err(error) => Parsed::Rejected(vec![format!("report is not UTF-8: {error}")]),
            Ok(text) => match contract.parse(text) {
                Ok(report) => Parsed::Report(report),
                Err(error) => Parsed::Rejected(vec![error]),
            },
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_repair_prompt_carries_the_errors_and_the_contract() {
        let text = repair(
            &["missing field `summary`".into(), "not UTF-8".into()],
            r#"{ "status": "..." }"#,
        );
        assert!(text.contains("- missing field `summary`\n"), "{text}");
        assert!(text.contains("- not UTF-8\n"), "{text}");
        assert!(text.contains(&format!("`{REPORT_FILE}`")), "{text}");
        assert!(
            text.contains("```json\n{ \"status\": \"...\" }\n```"),
            "{text}"
        );
    }
}
