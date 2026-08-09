//! The shared kernel machinery driven over fakes: one agent
//! invocation exec-streamed into the event log, the fresh-sandbox
//! bracket that can never leak a VM, and the verify checks that gate
//! progress. House pattern: assert the emitted events, the outcomes,
//! and the seam effects — never internal call patterns.

use std::sync::Arc;

use engine::invocation::{self, InvocationEnd};
use engine::testkit::{self, RecordingSink, ScriptedAgent, ScriptedSandbox, exec};
use engine::verify::{self, VerifyOutcome};
use engine::{
    ExecEvent, ExitStatus, KernelContext, KernelError, OnFail, OutputStream, RunEvent,
    SandboxError, SandboxHandle, TokenUsage, VerifyConfig,
};

fn context(
    sandbox: Arc<ScriptedSandbox>,
    sink: Arc<RecordingSink>,
    verify: VerifyConfig,
) -> KernelContext {
    testkit::context()
        .verify(verify)
        .sandbox(sandbox)
        .agent(Arc::new(ScriptedAgent::new().reporting(TokenUsage {
            input: 12,
            output: 3,
        })))
        .events(sink)
        .build()
}

fn seed_report(ctx: &KernelContext, sandbox: &ScriptedSandbox, raw: &[u8]) {
    sandbox.seed_file(ctx.workspace.guest_report_path(), raw);
}

#[tokio::test]
async fn an_invocation_streams_output_accounts_tokens_and_fetches_the_report() {
    let sandbox = Arc::new(ScriptedSandbox::scripted(vec![vec![
        Ok(ExecEvent::Stdout(b"working\n".to_vec())),
        Ok(ExecEvent::Stderr(b"warning: unused\n".to_vec())),
        Ok(ExecEvent::Stdout(b"tokens used: some\n".to_vec())),
        Ok(ExecEvent::Exited(ExitStatus { code: Some(0) })),
    ]]));
    let sink = Arc::new(RecordingSink::default());
    let ctx = context(sandbox.clone(), sink.clone(), VerifyConfig::default());
    sandbox.serve_report(br#"{"status": "done"}"#.as_slice());
    let handle = SandboxHandle::new("vm-0");

    let end = invocation::invoke(&ctx, 3, &handle, "do the work")
        .await
        .unwrap();

    let InvocationEnd::Reported(raw) = end else {
        panic!("expected a report, got {end:?}");
    };
    assert_eq!(raw, br#"{"status": "done"}"#);
    // The agent was invoked argv-exact with the prompt.
    assert_eq!(
        sandbox.execs()[0].argv,
        ["scripted-agent", "--prompt", "do the work"]
    );
    // Every chunk lands in the log in arrival order, tagged by stream,
    // and the adapter-reported usage follows.
    assert_eq!(
        sink.events(),
        [
            RunEvent::AgentOutput {
                iteration: 3,
                stream: OutputStream::Stdout,
                chunk: "working\n".into(),
            },
            RunEvent::AgentOutput {
                iteration: 3,
                stream: OutputStream::Stderr,
                chunk: "warning: unused\n".into(),
            },
            RunEvent::AgentOutput {
                iteration: 3,
                stream: OutputStream::Stdout,
                chunk: "tokens used: some\n".into(),
            },
            RunEvent::TokensUsed {
                iteration: 3,
                usage: TokenUsage {
                    input: 12,
                    output: 3,
                },
            },
        ]
    );
}

/// A crashed agent yields nothing — even a report file on disk is not
/// to be trusted from an invocation that did not exit cleanly.
#[tokio::test]
async fn a_crashed_agent_leaves_no_trustworthy_report() {
    let sandbox = Arc::new(ScriptedSandbox::scripted(vec![exec("panic!\n", 1)]));
    let sink = Arc::new(RecordingSink::default());
    let ctx = context(sandbox.clone(), sink.clone(), VerifyConfig::default());
    seed_report(&ctx, &sandbox, br#"{"status": "done"}"#);
    let handle = SandboxHandle::new("vm-0");

    let end = invocation::invoke(&ctx, 1, &handle, "work").await.unwrap();

    assert!(matches!(end, InvocationEnd::Crashed), "{end:?}");
    // The crash still left its output in the log — that is how a host
    // explains what happened.
    assert_eq!(
        sink.events(),
        [RunEvent::AgentOutput {
            iteration: 1,
            stream: OutputStream::Stdout,
            chunk: "panic!\n".into(),
        }]
    );
}

#[tokio::test]
async fn a_missing_report_names_the_gap_for_the_repair_re_prompt() {
    let sandbox = Arc::new(ScriptedSandbox::scripted(vec![exec(
        "did things, reported nothing\n",
        0,
    )]));
    let sink = Arc::new(RecordingSink::default());
    let ctx = context(sandbox, sink, VerifyConfig::default());
    let handle = SandboxHandle::new("vm-0");

    let end = invocation::invoke(&ctx, 1, &handle, "work").await.unwrap();

    let InvocationEnd::MissingReport(error) = end else {
        panic!("expected a missing report, got {end:?}");
    };
    assert!(error.contains("report missing"), "{error}");
}

#[tokio::test]
async fn a_clean_invocation_cannot_reuse_a_previous_report() {
    let sandbox = Arc::new(ScriptedSandbox::scripted(vec![exec(
        "forgot the report\n",
        0,
    )]));
    let sink = Arc::new(RecordingSink::default());
    let ctx = context(sandbox.clone(), sink, VerifyConfig::default());
    seed_report(
        &ctx,
        &sandbox,
        br#"{"status":"continue","summary":"stale"}"#,
    );
    let handle = SandboxHandle::new("vm-0");

    let end = invocation::invoke(&ctx, 1, &handle, "work").await.unwrap();

    let InvocationEnd::MissingReport(error) = end else {
        panic!("expected the stale report to be cleared, got {end:?}");
    };
    assert!(error.contains("report missing"), "{error}");
}

#[tokio::test]
async fn the_bracket_destroys_the_sandbox_on_success() {
    let sandbox = Arc::new(ScriptedSandbox::scripted(vec![]));
    let sink = Arc::new(RecordingSink::default());
    let ctx = context(sandbox.clone(), sink, VerifyConfig::default());

    let out = invocation::in_fresh_sandbox(&ctx, async |handle| Ok(handle.as_str().to_owned()))
        .await
        .unwrap();

    assert_eq!(out, "vm-0");
    assert_eq!(sandbox.created(), 1);
    assert_eq!(sandbox.destroyed(), 1);
}

/// The bracket's whole reason to exist: an error inside it still tears
/// the sandbox down before propagating.
#[tokio::test]
async fn the_bracket_destroys_the_sandbox_when_the_work_fails() {
    let sandbox = Arc::new(ScriptedSandbox::scripted(vec![]));
    let sink = Arc::new(RecordingSink::default());
    let ctx = context(sandbox.clone(), sink, VerifyConfig::default());

    let error = invocation::in_fresh_sandbox(&ctx, async |_handle| -> Result<(), KernelError> {
        Err(SandboxError("the work blew up".into()).into())
    })
    .await
    .expect_err("the error must propagate");

    assert!(error.to_string().contains("the work blew up"), "{error}");
    assert_eq!(sandbox.created(), 1);
    assert_eq!(sandbox.destroyed(), 1);
}

/// A verify section with the given checks; retries and on_fail stay
/// out of scope — they are kernel policy, not check mechanism.
fn verifying(checks: &[&str]) -> VerifyConfig {
    VerifyConfig {
        checks: checks.iter().map(|check| (*check).to_string()).collect(),
        on_fail: OnFail::default(),
    }
}

#[tokio::test]
async fn green_checks_run_in_order_through_the_shell_and_pass() {
    let sandbox = Arc::new(ScriptedSandbox::scripted(vec![
        exec("compiled", 0),
        exec("42 passed", 0),
    ]));
    let sink = Arc::new(RecordingSink::default());
    let ctx = context(
        sandbox.clone(),
        sink.clone(),
        verifying(&["cargo build", "cargo test"]),
    );
    let handle = SandboxHandle::new("vm-0");

    let outcome = verify::run_checks(&ctx, &handle, 2).await.unwrap();

    assert!(matches!(outcome, VerifyOutcome::Passed), "{outcome:?}");
    // A check is a user-authored command line, so it runs through the
    // shell — unlike the argv-exact agent invocation.
    let argvs: Vec<Vec<String>> = sandbox.execs().into_iter().map(|spec| spec.argv).collect();
    assert_eq!(
        argvs,
        [["sh", "-c", "cargo build"], ["sh", "-c", "cargo test"],]
    );
    // A passing check's output is not worth carrying; `passed` is the
    // whole story.
    assert_eq!(
        sink.events(),
        [
            RunEvent::VerifyCheckFinished {
                iteration: 2,
                command: "cargo build".into(),
                passed: true,
                output: String::new(),
            },
            RunEvent::VerifyCheckFinished {
                iteration: 2,
                command: "cargo test".into(),
                passed: true,
                output: String::new(),
            },
        ]
    );
}

#[tokio::test]
async fn a_red_check_stops_the_list_and_carries_its_output() {
    let sandbox = Arc::new(ScriptedSandbox::scripted(vec![exec(
        "error[E0433]: cannot find `Parser`",
        1,
    )]));
    let sink = Arc::new(RecordingSink::default());
    let ctx = context(
        sandbox.clone(),
        sink.clone(),
        verifying(&["cargo build", "cargo test"]),
    );
    let handle = SandboxHandle::new("vm-0");

    let outcome = verify::run_checks(&ctx, &handle, 1).await.unwrap();

    // Fail-fast: cargo test never ran — running past the failure only
    // buries the real error.
    let VerifyOutcome::Failed { command, output } = outcome else {
        panic!("expected a failure, got {outcome:?}");
    };
    assert_eq!(command, "cargo build");
    assert!(output.contains("error[E0433]"), "{output}");
    assert_eq!(sandbox.execs().len(), 1);
    assert_eq!(
        sink.events(),
        [RunEvent::VerifyCheckFinished {
            iteration: 1,
            command: "cargo build".into(),
            passed: false,
            output: "error[E0433]: cannot find `Parser`".into(),
        }]
    );
}

#[tokio::test]
async fn no_checks_means_every_iteration_passes() {
    let sandbox = Arc::new(ScriptedSandbox::scripted(vec![]));
    let sink = Arc::new(RecordingSink::default());
    let ctx = context(sandbox, sink.clone(), VerifyConfig::default());
    let handle = SandboxHandle::new("vm-0");

    let outcome = verify::run_checks(&ctx, &handle, 1).await.unwrap();

    assert!(matches!(outcome, VerifyOutcome::Passed), "{outcome:?}");
    assert!(sink.events().is_empty());
}
