//! One agent invocation — the engine mechanism every kernel drives
//! its agent through: a fresh sandbox, the agent exec-streamed into
//! the event log, token usage accounted, and the report it left
//! fetched back. What the report means — its shape, its parse, its
//! repair — is kernel policy, as is when to checkpoint the workspace.

use std::collections::BTreeMap;

use futures_util::StreamExt;

use crate::event::{OutputStream, RunEvent};
use crate::kernel::{KernelContext, KernelError};
use crate::sandbox::{ExecEvent, SandboxHandle, SandboxSpec, into_text};

/// What one agent invocation left behind.
#[derive(Debug)]
pub enum InvocationEnd {
    /// The agent exited non-zero or was killed; nothing it left can
    /// be trusted, the report included.
    Crashed,
    /// The agent exited cleanly but its report cannot be read; the
    /// message is what a repair re-prompt carries back.
    MissingReport(String),
    /// The raw report, exactly as the agent wrote it. Parsing it is
    /// the kernel's: each kernel owns its report shapes.
    Reported(Vec<u8>),
}

/// Boots a fresh sandbox over the workspace, runs `work` inside it,
/// and destroys the sandbox on every path — an error can never leak a
/// live VM. One bracket may span several execs: a repair re-prompt
/// and the verify checks belong in the same sandbox as the invocation
/// whose work they judge.
pub async fn in_fresh_sandbox<T>(
    ctx: &KernelContext,
    work: impl AsyncFnOnce(&SandboxHandle) -> Result<T, KernelError>,
) -> Result<T, KernelError> {
    let spec = SandboxSpec {
        workspace: ctx.workspace.mount(),
        env: BTreeMap::new(),
    };
    let sandbox = ctx.sandbox.create(&spec).await?;
    let result = work(&sandbox).await;
    let destroyed = ctx.sandbox.destroy(sandbox).await;
    let result = result?;
    destroyed?;
    Ok(result)
}

/// Runs the agent once in the sandbox: exec-stream the invocation,
/// emit every output chunk and the token usage as events, then fetch
/// the report the agent wrote — through the sandbox seam, because
/// scratch is read through the guest's view, never the host's.
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
    let mut stdout = String::new();
    let mut exit = None;
    while let Some(event) = output.next().await {
        match event? {
            ExecEvent::Stdout(bytes) => {
                let chunk = into_text(bytes);
                stdout.push_str(&chunk);
                ctx.events
                    .emit(RunEvent::AgentOutput {
                        iteration,
                        stream: OutputStream::Stdout,
                        chunk,
                    })
                    .await?;
            }
            ExecEvent::Stderr(bytes) => {
                ctx.events
                    .emit(RunEvent::AgentOutput {
                        iteration,
                        stream: OutputStream::Stderr,
                        chunk: into_text(bytes),
                    })
                    .await?;
            }
            ExecEvent::Exited(status) => exit = Some(status),
        }
    }

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
