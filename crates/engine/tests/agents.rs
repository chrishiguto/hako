//! The built-in adapters proven against scripted stand-ins for the
//! real agent CLIs: each invocation actually spawns, delivers the
//! composed prompt byte-for-byte through argv, and the stdout a
//! scripted CLI prints round-trips into token usage. No real agent
//! CLIs, no tokens spent.

use std::io::Write;
use std::path::Path;
use std::process::Output;

use engine::agents::{ClaudeAdapter, CmdAdapter, CodexAdapter};
use engine::{AgentAdapter, ExecSpec, TokenUsage};

/// Writes an executable shell script posing as an agent binary. It
/// records its argv NUL-separated — the only unambiguous separator
/// once prompts carry newlines — then prints the scripted stdout.
///
/// The write happens in a child process: a write fd held here could be
/// inherited by another test's concurrently forked child and fail this
/// script's exec with ETXTBSY.
fn fake_agent(dir: &Path, name: &str, stdout: &str) {
    let path = dir.join(name);
    let body = format!(
        "#!/bin/sh\nprintf '%s\\0' \"$@\" > \"$(dirname \"$0\")/argv\"\ncat <<'HAKO_EOF'\n{stdout}\nHAKO_EOF\n"
    );
    let mut writer = std::process::Command::new("sh")
        .args(["-c", r#"cat > "$1" && chmod 755 "$1""#, "sh"])
        .arg(&path)
        .stdin(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    writer
        .stdin
        .take()
        .unwrap()
        .write_all(body.as_bytes())
        .unwrap();
    assert!(writer.wait().unwrap().success());
}

/// Runs the adapter-built invocation resolving the binary via PATH,
/// exactly how the sandbox would. The fake's directory goes first so a
/// real agent CLI on this machine can never shadow it.
fn run(dir: &Path, spec: &ExecSpec) -> Output {
    let path = format!("{}:{}", dir.display(), std::env::var("PATH").unwrap());
    let output = std::process::Command::new(&spec.argv[0])
        .args(&spec.argv[1..])
        .env("PATH", path)
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    output
}

/// The argv the fake received, minus the program name it dispatched
/// on.
fn recorded_args(dir: &Path) -> Vec<String> {
    let raw = std::fs::read_to_string(dir.join("argv")).unwrap();
    raw.split_terminator('\0')
        .map(ToString::to_string)
        .collect()
}

/// A prompt with the shapes that break naive quoting: newlines,
/// spaces, and shell metacharacters must all survive argv intact.
const PROMPT: &str = "Iteration 3 of 20.\n\nFix `build.rs` — don't touch \"$HOME\".";

#[test]
fn claude_runs_headless_and_reports_usage_from_its_result_json() {
    let dir = tempfile::tempdir().unwrap();
    fake_agent(
        dir.path(),
        "claude",
        r#"{"type":"result","subtype":"success","result":"done","usage":{"input_tokens":1200,"cache_creation_input_tokens":80,"cache_read_input_tokens":9000,"output_tokens":340},"total_cost_usd":0.31}"#,
    );

    let spec = ClaudeAdapter.invocation(PROMPT);
    let output = run(dir.path(), &spec);

    assert_eq!(
        recorded_args(dir.path()),
        [
            "-p",
            PROMPT,
            "--output-format",
            "json",
            "--dangerously-skip-permissions",
        ]
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert_eq!(
        ClaudeAdapter.token_usage(&stdout),
        Some(TokenUsage {
            input: 10280,
            output: 340,
        })
    );
}

#[test]
fn codex_runs_headless_and_reports_usage_from_its_event_stream() {
    let dir = tempfile::tempdir().unwrap();
    fake_agent(
        dir.path(),
        "codex",
        concat!(
            r#"{"type":"thread.started","thread_id":"t1"}"#,
            "\n",
            r#"{"type":"item.completed","item":{"type":"agent_message","text":"done"}}"#,
            "\n",
            r#"{"type":"turn.completed","usage":{"input_tokens":24763,"cached_input_tokens":24448,"output_tokens":122}}"#,
        ),
    );

    let spec = CodexAdapter.invocation(PROMPT);
    let output = run(dir.path(), &spec);

    assert_eq!(
        recorded_args(dir.path()),
        [
            "exec",
            "--json",
            "--sandbox",
            "danger-full-access",
            "--skip-git-repo-check",
            PROMPT,
        ]
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert_eq!(
        CodexAdapter.token_usage(&stdout),
        Some(TokenUsage {
            input: 24763,
            output: 122,
        })
    );
}

#[test]
fn a_command_template_delivers_the_prompt_to_an_arbitrary_cli() {
    let dir = tempfile::tempdir().unwrap();
    fake_agent(dir.path(), "my-agent", "did some work");

    let adapter = CmdAdapter::new(vec![
        "my-agent".into(),
        "--message".into(),
        "{prompt}".into(),
        "--yes".into(),
    ])
    .unwrap();
    let output = run(dir.path(), &adapter.invocation(PROMPT));

    assert_eq!(recorded_args(dir.path()), ["--message", PROMPT, "--yes"]);
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert_eq!(adapter.token_usage(&stdout), None);
}
