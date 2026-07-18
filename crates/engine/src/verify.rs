//! Verify checks — the flow's configured commands, run in the
//! iteration's sandbox once the agent has stopped. They are the loop's
//! definition of progress: an iteration counts only when they pass. A
//! failure is not fatal — it feeds the next preamble and the on_fail
//! retry budget — so the run corrects course instead of committing
//! confidently broken work.

use futures_util::StreamExt;

use crate::kernel::{KernelContext, KernelError};
use crate::sandbox::{ExecEvent, ExecSpec, SandboxHandle, into_text};
use proto::event::RunEvent;

/// How much of a failing check's output the next preamble carries.
/// Capped because it is agent-influenced text re-entering a prompt
/// every retry — an unbounded test log would crowd out the domain
/// rules and burn context. The tail is kept: the error a build or test
/// run stops on is the last thing it prints.
const OUTPUT_TAIL_CHARS: usize = 4000;

/// Whether an iteration's verify checks passed, and — when they did
/// not — the failing command and its output for the next preamble.
pub(crate) enum VerifyOutcome {
    Passed,
    Failed { command: String, output: String },
}

/// Runs the flow's checks in order and stops at the first failure —
/// later checks assume earlier ones passed (build before test), so
/// running them past a failure only buries the real error. Each check
/// that runs emits a [`RunEvent::VerifyCheckFinished`]; no checks
/// configured means every iteration passes.
pub(crate) async fn run_checks(
    ctx: &KernelContext,
    sandbox: &SandboxHandle,
    iteration: u32,
) -> Result<VerifyOutcome, KernelError> {
    for command in &ctx.verify.checks {
        let (passed, output) = run_check(ctx, sandbox, command).await?;
        ctx.events
            .emit(RunEvent::VerifyCheckFinished {
                iteration,
                command: command.clone(),
                passed,
            })
            .await?;
        if !passed {
            return Ok(VerifyOutcome::Failed {
                command: command.clone(),
                output: tail(&output),
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
    let mut output = String::new();
    let mut exit = None;
    while let Some(event) = stream.next().await {
        match event? {
            ExecEvent::Stdout(bytes) | ExecEvent::Stderr(bytes) => {
                output.push_str(&into_text(bytes));
            }
            ExecEvent::Exited(status) => exit = Some(status),
        }
    }
    Ok((exit.is_some_and(|status| status.success()), output))
}

/// Keeps the last [`OUTPUT_TAIL_CHARS`] characters, marking the cut so
/// the agent knows output was dropped.
fn tail(output: &str) -> String {
    let total = output.chars().count();
    if total <= OUTPUT_TAIL_CHARS {
        return output.to_owned();
    }
    let kept: String = output.chars().skip(total - OUTPUT_TAIL_CHARS).collect();
    format!("…(earlier output truncated)…\n{kept}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_output_is_kept_whole() {
        assert_eq!(tail("build failed: E0001"), "build failed: E0001");
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
}
