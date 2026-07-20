//! Verify checks — the flow's configured commands, run in the
//! iteration's sandbox once the agent has stopped. They are the loop's
//! definition of progress: an iteration counts only when they pass. A
//! failure is not fatal — it feeds the next preamble and the on_fail
//! retry budget — so the run corrects course instead of advancing on
//! confidently broken work.

use futures_util::StreamExt;

use crate::event::RunEvent;
use crate::kernel::{KernelContext, KernelError};
use crate::sandbox::{ExecEvent, ExecSpec, SandboxHandle, into_text};

/// How much of a failing check's output the next preamble carries.
/// Capped because it is agent-influenced text re-entering a prompt
/// every retry — an unbounded test log would crowd out the domain
/// rules and burn context. The tail is kept: the error a build or test
/// run stops on is the last thing it prints.
const OUTPUT_TAIL_CHARS: usize = 4000;

/// Bounds the raw bytes retained while a check streams — a check that
/// prints without end must not balloon the host, whose memory no
/// sandbox limit protects. Wide enough to always yield
/// [`OUTPUT_TAIL_CHARS`] characters, with a margin that keeps [`tail`]
/// marking the cut whenever bytes were dropped.
const MAX_OUTPUT_BYTES: usize = OUTPUT_TAIL_CHARS * 4 + 4;

/// How an iteration came out of the verify gate. `Skipped` is distinct
/// from `Passed` so nothing downstream can mistake an unverified
/// iteration for a verified one.
#[derive(Debug)]
pub enum VerifyOutcome {
    Passed,
    /// No checks ran: the report's status pauses the run on the
    /// agent's own word, so there is no progress to verify.
    Skipped,
    /// The failing command and its output for the next preamble.
    Failed {
        command: String,
        output: String,
    },
}

/// Runs the flow's checks in order and stops at the first failure —
/// later checks assume earlier ones passed (build before test), so
/// running them past a failure only buries the real error. Each check
/// that runs emits a [`RunEvent::VerifyCheckFinished`]; no checks
/// configured means every iteration passes.
pub async fn run_checks(
    ctx: &KernelContext,
    sandbox: &SandboxHandle,
    iteration: u32,
) -> Result<VerifyOutcome, KernelError> {
    for command in &ctx.verify.checks {
        let (passed, output) = run_check(ctx, sandbox, command).await?;
        // Only a failure's output is worth carrying: it is what the
        // log and the next preamble need to say what went wrong.
        let output = if passed { String::new() } else { tail(&output) };
        ctx.events
            .emit(RunEvent::VerifyCheckFinished {
                iteration,
                command: command.clone(),
                passed,
                output: output.clone(),
            })
            .await?;
        if !passed {
            return Ok(VerifyOutcome::Failed {
                command: command.clone(),
                output,
            });
        }
    }
    Ok(VerifyOutcome::Passed)
}

/// Runs one check to completion in the sandbox and returns whether it
/// passed and its combined output. A check is a user-authored command
/// line, so it runs through a shell — the one place the engine wants
/// the shell's splitting and expansion, unlike the argv-exact agent
/// invocation. Stdout and stderr are merged in arrival order: the
/// preamble shows the failure as the terminal did.
async fn run_check(
    ctx: &KernelContext,
    sandbox: &SandboxHandle,
    command: &str,
) -> Result<(bool, String), KernelError> {
    let spec = ExecSpec {
        argv: vec!["sh".into(), "-c".into(), command.into()],
        cwd: None,
    };
    let mut stream = ctx.sandbox.exec_stream(sandbox, &spec).await?;
    let mut output = Vec::new();
    let mut exit = None;
    while let Some(event) = stream.next().await {
        match event? {
            ExecEvent::Stdout(bytes) | ExecEvent::Stderr(bytes) => {
                push_bounded(&mut output, &bytes);
            }
            ExecEvent::Exited(status) => exit = Some(status),
        }
    }
    Ok((
        exit.is_some_and(|status| status.success()),
        decode_tail(output),
    ))
}

/// Appends a chunk to the bounded raw-byte tail, dropping from the
/// front once the cap is passed — the newest output is the output that
/// matters.
fn push_bounded(output: &mut Vec<u8>, bytes: &[u8]) {
    output.extend_from_slice(bytes);
    if output.len() > MAX_OUTPUT_BYTES {
        output.drain(..output.len() - MAX_OUTPUT_BYTES);
    }
}

/// One decode for the whole stream — chunks arrive at arbitrary byte
/// boundaries and can split codepoints (the [`ExecEvent`] contract),
/// so decoding per chunk would mangle them. The front may sit inside a
/// codepoint the byte cap cut; the orphaned continuation bytes are
/// shed so the decode stays clean.
fn decode_tail(mut output: Vec<u8>) -> String {
    while output.first().is_some_and(|byte| byte & 0xC0 == 0x80) {
        output.remove(0);
    }
    into_text(output)
}

/// Keeps the last [`OUTPUT_TAIL_CHARS`] characters, marking the cut so
/// the agent knows output was dropped. One reverse walk over at most
/// the kept tail; a cut landing at the very start means nothing was
/// dropped.
fn tail(output: &str) -> String {
    match output.char_indices().rev().nth(OUTPUT_TAIL_CHARS - 1) {
        Some((cut, _)) if cut > 0 => {
            format!("…(earlier output truncated)…\n{}", &output[cut..])
        }
        _ => output.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_output_is_kept_whole() {
        assert_eq!(tail("build failed: E0001"), "build failed: E0001");
    }

    #[test]
    fn output_at_exactly_the_cap_is_kept_whole() {
        let output: String = std::iter::repeat_n('x', OUTPUT_TAIL_CHARS).collect();
        assert_eq!(tail(&output), output);
    }

    #[test]
    fn long_output_keeps_the_tail_and_marks_the_cut() {
        let output: String = std::iter::repeat_n('x', OUTPUT_TAIL_CHARS + 500).collect();
        let tailed = tail(&output);
        assert!(
            tailed.starts_with("…(earlier output truncated)…\n"),
            "{tailed}"
        );
        assert_eq!(
            tailed.chars().filter(|c| *c == 'x').count(),
            OUTPUT_TAIL_CHARS
        );
    }

    /// The cut lands on a character boundary — a multibyte tail must not
    /// panic or split a codepoint.
    #[test]
    fn truncation_respects_character_boundaries() {
        let output: String = std::iter::repeat_n('é', OUTPUT_TAIL_CHARS + 10).collect();
        let tailed = tail(&output);
        assert_eq!(
            tailed.chars().filter(|c| *c == 'é').count(),
            OUTPUT_TAIL_CHARS
        );
    }

    #[test]
    fn the_byte_cap_keeps_the_newest_output() {
        let mut output = Vec::new();
        push_bounded(&mut output, &[b'a'; MAX_OUTPUT_BYTES]);
        push_bounded(&mut output, b"end");
        assert_eq!(output.len(), MAX_OUTPUT_BYTES);
        assert!(output.ends_with(b"end"));
    }

    /// A codepoint split across two chunks decodes whole, and one cut
    /// open by the byte cap is shed rather than turned into
    /// replacement characters.
    #[test]
    fn decode_joins_chunks_and_sheds_a_codepoint_the_cap_cut() {
        let bytes = "é".as_bytes();
        let mut output = Vec::new();
        push_bounded(&mut output, &bytes[..1]);
        push_bounded(&mut output, &bytes[1..]);
        assert_eq!(decode_tail(output), "é");
        assert_eq!(decode_tail(vec![bytes[1], b'o', b'k']), "ok");
    }
}
