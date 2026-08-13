use std::collections::BTreeSet;
use std::env;
use std::fs::{self, File};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use api::{EventEnvelope, RunEvent, RunState};
use serde_json::Value;

const DAEMON_TOKEN: &str = "hako-e2e-local-token";
const RESULT: &str = "hako end-to-end smoke passed\n";
// One source for the flow's verify checks and the assertion that every
// one of them gated the done claim.
const CHECKS: [&str; 2] = [
    r"printf 'hako end-to-end smoke passed\n' | cmp -s - SMOKE_RESULT.txt",
    "git diff --check",
];

pub fn run() {
    let image = required("HAKO_E2E_IMAGE");
    let secrets = PathBuf::from(required("HAKO_E2E_SECRETS_DIR"));
    let secret = claude_secret(&secrets);
    let root = tempfile::tempdir().unwrap();
    let source = seed_repo(root.path());
    let source_head = git(&source, &["rev-parse", "HEAD"]);
    let machines_before = hako_machines();
    let address = unused_address();
    let mut daemon = Daemon::start(root.path(), address, &image, &secrets);
    daemon.wait_until_ready(address);

    let flow = root.path().join("smoke.toml");
    fs::write(&flow, flow_for(&source)).unwrap();
    let submitted = hako(address, &["run", utf8_path(&flow)]);
    assert_success("hako run", &submitted);
    let run_id = String::from_utf8(submitted.stdout).unwrap();
    let run_id = run_id.trim();
    assert!(!run_id.is_empty(), "hako run returned no run id");

    let attached = hako(address, &["attach", run_id]);
    assert_success("hako attach", &attached);
    let events_text = String::from_utf8(attached.stdout).unwrap();
    // Typed parsing is the point: every attach line must read as the
    // published language, so wire drift fails loudly here instead of
    // silently emptying a stringly filter further down.
    let events: Vec<EventEnvelope> = events_text
        .lines()
        .map(|line| {
            serde_json::from_str(line).unwrap_or_else(|error| {
                panic!("attach output is not a published EventEnvelope: {error}\n{line}")
            })
        })
        .collect();
    let workspace = root.path().join("runs").join(run_id).join("workspace");
    assert_verified_done(&events, &workspace);
    assert_checkpointed(&workspace, &events, run_id);
    assert_source_untouched(&source, &source_head);
    assert!(
        !events_text.contains(secret.trim_end_matches(['\r', '\n'])),
        "the Claude secret reached the Event Log"
    );

    daemon.stop();
    let machines_after = hako_machines();
    let leaked: Vec<_> = machines_after.difference(&machines_before).collect();
    assert!(leaked.is_empty(), "the run leaked microVMs: {leaked:?}");
}

fn required(name: &str) -> String {
    env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| panic!("{name} must be set; follow docs/quickstart.md"))
}

fn claude_secret(store: &Path) -> String {
    ["ANTHROPIC_API_KEY", "CLAUDE_CODE_OAUTH_TOKEN"]
        .into_iter()
        .find_map(|name| fs::read_to_string(store.join(name)).ok())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| {
            panic!(
                "{}/ANTHROPIC_API_KEY or CLAUDE_CODE_OAUTH_TOKEN must be provisioned",
                store.display()
            )
        })
}

fn seed_repo(root: &Path) -> PathBuf {
    let repo = root.join("source");
    fs::create_dir(&repo).unwrap();
    fs::copy(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/e2e/PROMPT.md"),
        repo.join("PROMPT.md"),
    )
    .unwrap();
    git_ok(&repo, &["init", "--quiet", "--initial-branch=main"]);
    git_ok(&repo, &["add", "PROMPT.md"]);
    git_ok(
        &repo,
        &[
            "-c",
            "user.name=hako-e2e",
            "-c",
            "user.email=hako-e2e@localhost",
            "commit",
            "--quiet",
            "--no-gpg-sign",
            "-m",
            "seed smoke objective",
        ],
    );
    repo
}

fn flow_for(repo: &Path) -> String {
    // Rust's debug escaping (`\\`, `\"`) is valid TOML basic-string
    // escaping for these commands.
    let checks = CHECKS.map(|check| format!("  {check:?},")).join("\n");
    format!(
        r#"[loop]
kernel = "pipeline"

[prompts]
plan = "PROMPT.md"

[agent]
engine = "claude"

[budget]
max_iterations = 2
max_hours = 1
iteration_timeout = "15m"

[verify]
checks = [
{checks}
]
on_fail = {{ retries = 0, then = "fail" }}

[workspace]
repo = {repo:?}
"#,
        repo = repo.display().to_string(),
    )
}

fn unused_address() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    address
}

fn hako(address: SocketAddr, args: &[&str]) -> Output {
    let binary = Path::new(env!("CARGO_BIN_EXE_hakod")).with_file_name("hako");
    assert!(
        binary.is_file(),
        "{} is missing; run this smoke through `just e2e`",
        binary.display()
    );
    Command::new(binary)
        .args([
            "--address",
            &format!("http://{address}"),
            "--token",
            DAEMON_TOKEN,
        ])
        .args(args)
        .output()
        .unwrap()
}

fn assert_success(operation: &str, output: &Output) {
    assert!(
        output.status.success(),
        "{operation} failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn assert_verified_done(events: &[EventEnvelope], workspace: &Path) {
    let terminal = events.last().expect("the Event Log is empty");
    if !matches!(
        terminal.event,
        RunEvent::StateChanged {
            state: RunState::Done
        }
    ) {
        panic!(
            "run did not reach Verified Done:\n{}\nworkspace:\n{}",
            event_trace(events),
            workspace_trace(workspace)
        );
    }

    // The kernel verifies every mutating stage and every done claim, so
    // the number of check events depends on which path the run took;
    // only the set of commands and their outcomes is stable.
    let checks: Vec<(&str, bool)> = events
        .iter()
        .filter_map(|envelope| match &envelope.event {
            RunEvent::VerifyCheckFinished {
                command, passed, ..
            } => Some((command.as_str(), *passed)),
            _ => None,
        })
        .collect();
    assert!(
        checks.iter().all(|(_, passed)| *passed),
        "a Verify Check failed:\n{}",
        event_trace(events)
    );
    let commands: BTreeSet<&str> = checks.iter().map(|(command, _)| *command).collect();
    assert_eq!(
        commands,
        BTreeSet::from(CHECKS),
        "a configured Verify Check never ran:\n{}",
        event_trace(events)
    );
    assert!(
        events.iter().any(|envelope| {
            matches!(
                &envelope.event,
                RunEvent::SkepticVerdict {
                    refuted: false,
                    findings,
                    ..
                } if findings.is_empty()
            )
        }),
        "no unrefuted Skeptic Iteration was recorded:\n{}",
        event_trace(events)
    );
}

fn workspace_trace(workspace: &Path) -> String {
    [
        ("status", vec!["status", "--porcelain"]),
        ("history", vec!["log", "--oneline", "--decorate", "--all"]),
        ("branch delta", vec!["diff", "--name-status", "main...HEAD"]),
    ]
    .into_iter()
    .map(|(label, args)| format!("{label}:\n{}", git(workspace, &args)))
    .collect::<Vec<_>>()
    .join("\n")
}

fn event_trace(events: &[EventEnvelope]) -> String {
    events
        .iter()
        .map(|envelope| match &envelope.event {
            RunEvent::StageStarted { stage, .. } => format!("stage_started {stage}"),
            RunEvent::StageReported { stage, report, .. } => {
                format!("stage_reported {stage} status={}", report["status"])
            }
            RunEvent::VerifyCheckFinished {
                command, passed, ..
            } => format!("verify_check_finished passed={passed} command={command}"),
            RunEvent::ReportRejected { errors, .. } => format!("report_rejected errors={errors:?}"),
            RunEvent::SkepticVerdict { refuted, .. } => {
                format!("skeptic_verdict refuted={refuted}")
            }
            RunEvent::StateChanged { state } => format!("state_changed state={state:?}"),
            // The rest matter to the trace only as waypoints; agent
            // output in particular must stay out of the diagnostics.
            event => serde_json::to_value(event).unwrap()["type"]
                .as_str()
                .unwrap_or("unknown")
                .to_owned(),
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn assert_checkpointed(workspace: &Path, events: &[EventEnvelope], run_id: &str) {
    assert_eq!(
        fs::read_to_string(workspace.join("SMOKE_RESULT.txt")).unwrap(),
        RESULT
    );
    assert_eq!(
        git(workspace, &["branch", "--show-current"]),
        format!("hako/{run_id}")
    );
    assert_eq!(git(workspace, &["remote"]), "");
    assert_eq!(
        git(workspace, &["diff", "--name-only", "main...HEAD"]),
        "SMOKE_RESULT.txt"
    );
    assert_eq!(
        git(
            workspace,
            &["diff-tree", "--no-commit-id", "--name-only", "-r", "HEAD"]
        ),
        "SMOKE_RESULT.txt"
    );
    assert_eq!(git(workspace, &["ls-files", ".hako"]), "");
    assert_eq!(git(workspace, &["status", "--porcelain"]), "?? .hako/");

    let iterations: BTreeSet<u32> = events
        .iter()
        .filter_map(|envelope| match &envelope.event {
            RunEvent::IterationStarted { iteration } => Some(*iteration),
            _ => None,
        })
        .collect();
    let checkpoints: BTreeSet<u32> = events
        .iter()
        .filter_map(|envelope| match &envelope.event {
            RunEvent::WorkspaceCheckpointed { iteration, .. } => Some(*iteration),
            _ => None,
        })
        .collect();
    assert!(!iterations.is_empty(), "the run started no iteration");
    assert_eq!(
        checkpoints, iterations,
        "an iteration left no checkpoint commit"
    );

    let head = git(workspace, &["rev-parse", "HEAD"]);
    assert!(events.iter().any(|envelope| {
        matches!(&envelope.event, RunEvent::WorkspaceCheckpointed { commit, .. } if *commit == head)
    }));
}

fn assert_source_untouched(source: &Path, original_head: &str) {
    assert_eq!(git(source, &["rev-parse", "HEAD"]), original_head);
    assert_eq!(git(source, &["status", "--porcelain"]), "");
    assert_eq!(
        git(
            source,
            &["for-each-ref", "--format=%(refname:short)", "refs/heads"]
        ),
        "main"
    );
    assert!(!source.join("SMOKE_RESULT.txt").exists());
}

fn git(repo: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .output()
        .unwrap();
    assert_success("git", &output);
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}

fn git_ok(repo: &Path, args: &[&str]) {
    git(repo, args);
}

fn utf8_path(path: &Path) -> &str {
    path.to_str().expect("e2e temporary path is not UTF-8")
}

fn hako_machines() -> BTreeSet<String> {
    let output = Command::new("smolvm")
        .args(["machine", "ls", "--json"])
        .output()
        .unwrap();
    assert_success("smolvm machine ls", &output);
    serde_json::from_slice::<Vec<Value>>(&output.stdout)
        .unwrap()
        .into_iter()
        .filter_map(|machine| machine["name"].as_str().map(str::to_owned))
        .filter(|name| name.starts_with("hako-"))
        .collect()
}

struct Daemon {
    child: Option<Child>,
    stdout: PathBuf,
    stderr: PathBuf,
}

impl Daemon {
    fn start(root: &Path, address: SocketAddr, image: &str, secrets: &Path) -> Self {
        let stdout = root.join("hakod.stdout.log");
        let stderr = root.join("hakod.stderr.log");
        let child = Command::new(env!("CARGO_BIN_EXE_hakod"))
            .env("HAKO_ADDR", address.to_string())
            .env("HAKO_TOKEN", DAEMON_TOKEN)
            .env("HAKO_RUNS_DIR", root.join("runs"))
            .env("HAKO_SECRETS_DIR", secrets)
            .env("HAKO_VM_IMAGE", image)
            .env("HAKO_VM_NET", "1")
            .stdout(Stdio::from(File::create(&stdout).unwrap()))
            .stderr(Stdio::from(File::create(&stderr).unwrap()))
            .spawn()
            .unwrap();
        Self {
            child: Some(child),
            stdout,
            stderr,
        }
    }

    fn wait_until_ready(&mut self, address: SocketAddr) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if TcpStream::connect(address).is_ok() {
                return;
            }
            if let Some(status) = self.child.as_mut().unwrap().try_wait().unwrap() {
                panic!("hakod exited during startup ({status}):\n{}", self.logs());
            }
            assert!(
                Instant::now() < deadline,
                "hakod did not bind:\n{}",
                self.logs()
            );
            thread::sleep(Duration::from_millis(25));
        }
    }

    fn logs(&self) -> String {
        format!(
            "stdout:\n{}\nstderr:\n{}",
            fs::read_to_string(&self.stdout).unwrap_or_default(),
            fs::read_to_string(&self.stderr).unwrap_or_default(),
        )
    }

    fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            if child.try_wait().unwrap().is_none() {
                child.kill().unwrap();
            }
            child.wait().unwrap();
        }
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        self.stop();
    }
}
