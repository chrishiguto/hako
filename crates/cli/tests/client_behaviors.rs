use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::process::{Command, Output};
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::Duration;

use api::SubmitRunRequest;

struct Request {
    head: String,
    body: String,
}

fn stub(response: String) -> (String, Receiver<Request>) {
    stub_sequence(vec![response])
}

fn stub_sequence(responses: Vec<String>) -> (String, Receiver<Request>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let (requests, received) = mpsc::channel();
    thread::spawn(move || {
        for response in responses {
            let (mut stream, _) = listener.accept().unwrap();
            let request = read_request(&mut stream);
            let _ = requests.send(request);
            stream.write_all(response.as_bytes()).unwrap();
        }
    });
    (address, received)
}

fn json_response(body: &str) -> String {
    format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}

fn error_response(status: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}

fn read_request(stream: &mut TcpStream) -> Request {
    let mut bytes = Vec::new();
    let header_end = loop {
        let mut chunk = [0; 1024];
        let read = stream.read(&mut chunk).unwrap();
        assert_ne!(read, 0, "request ended before its headers");
        bytes.extend_from_slice(&chunk[..read]);
        if let Some(end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            break end + 4;
        }
    };
    let head = String::from_utf8(bytes[..header_end].to_vec()).unwrap();
    let content_length = head
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .map(|value| value.trim().parse::<usize>().unwrap())
        })
        .unwrap_or(0);
    while bytes.len() - header_end < content_length {
        let mut chunk = [0; 1024];
        let read = stream.read(&mut chunk).unwrap();
        assert_ne!(read, 0, "request ended before its body");
        bytes.extend_from_slice(&chunk[..read]);
    }
    Request {
        head,
        body: String::from_utf8(bytes[header_end..header_end + content_length].to_vec()).unwrap(),
    }
}

fn hako(args: &[&str]) -> Output {
    hako_with_env(args, &[])
}

fn hako_with_env(args: &[&str], env: &[(&str, &str)]) -> Output {
    let config = tempfile::tempdir().unwrap();
    Command::new(env!("CARGO_BIN_EXE_hako"))
        .args(args)
        .env_remove("HAKO_ADDR")
        .env_remove("HAKO_TOKEN")
        .env("XDG_CONFIG_HOME", config.path())
        .envs(env.iter().copied())
        .output()
        .expect("hako runs")
}

fn repo_path(relative: &str) -> String {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(relative)
        .into_os_string()
        .into_string()
        .expect("path is UTF-8")
}

#[test]
fn run_submits_to_a_remote_daemon_and_returns_the_run_id() {
    let response_body = r#"{"run_id":"run-123"}"#;
    let response = format!(
        "HTTP/1.1 201 Created\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response_body}",
        response_body.len()
    );
    let (address, request) = stub(response);

    let output = hako(&[
        "--address",
        &address,
        "--token",
        "remote-token",
        "run",
        &repo_path("../../examples/pipeline.toml"),
    ]);

    assert!(output.status.success(), "{output:?}");
    assert_eq!(String::from_utf8_lossy(&output.stdout), "run-123\n");
    let request = request.recv().unwrap();
    assert!(request.head.starts_with("POST /v1/runs HTTP/1.1\r\n"));
    assert!(
        request
            .head
            .to_ascii_lowercase()
            .contains("authorization: bearer remote-token\r\n")
    );
    let submitted: SubmitRunRequest = serde_json::from_str(&request.body).unwrap();
    assert_eq!(
        submitted.flow,
        std::fs::read_to_string(repo_path("../../examples/pipeline.toml")).unwrap()
    );
}

#[test]
fn run_uses_the_remote_daemon_from_the_environment() {
    let response_body = r#"{"run_id":"run-from-env"}"#;
    let response = format!(
        "HTTP/1.1 201 Created\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response_body}",
        response_body.len()
    );
    let (address, request) = stub(response);

    let output = hako_with_env(
        &["run", &repo_path("../../examples/pipeline.toml")],
        &[("HAKO_ADDR", &address), ("HAKO_TOKEN", "env-token")],
    );

    assert!(output.status.success(), "{output:?}");
    assert_eq!(String::from_utf8_lossy(&output.stdout), "run-from-env\n");
    assert!(
        request
            .recv()
            .unwrap()
            .head
            .to_ascii_lowercase()
            .contains("authorization: bearer env-token\r\n")
    );
}

#[test]
fn run_distinguishes_validation_failure_from_submit_failure() {
    let invalid = hako(&["run", &repo_path("tests/fixtures/misspelled-kernel.toml")]);
    assert_eq!(invalid.status.code(), Some(2), "{invalid:?}");

    let unused = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = format!("http://{}", unused.local_addr().unwrap());
    drop(unused);
    let failed_submit = hako(&[
        "--address",
        &address,
        "run",
        &repo_path("../../examples/pipeline.toml"),
    ]);
    assert_eq!(failed_submit.status.code(), Some(3), "{failed_submit:?}");
}

#[test]
fn list_renders_every_state_and_pause_reason() {
    let response_body = r#"{"runs":[
        {"run_id":"r-running","state":"running","kernel":"pipeline","agent":"codex","created_at":"2026-08-12T08:00:00Z","updated_at":"2026-08-12T09:00:00Z"},
        {"run_id":"r-blocked","state":"paused","reason":"blocked","kernel":"pipeline","agent":"codex","created_at":"2026-08-12T08:00:00Z","updated_at":"2026-08-12T09:00:00Z"},
        {"run_id":"r-verify","state":"paused","reason":"verify_failed","kernel":"pipeline","agent":"codex","created_at":"2026-08-12T08:00:00Z","updated_at":"2026-08-12T09:00:00Z"},
        {"run_id":"r-timeout","state":"paused","reason":"timeout","kernel":"pipeline","agent":"codex","created_at":"2026-08-12T08:00:00Z","updated_at":"2026-08-12T09:00:00Z"},
        {"run_id":"r-drift","state":"paused","reason":"drift","kernel":"pipeline","agent":"codex","created_at":"2026-08-12T08:00:00Z","updated_at":"2026-08-12T09:00:00Z"},
        {"run_id":"r-budget","state":"paused","reason":"budget","kernel":"pipeline","agent":"codex","created_at":"2026-08-12T08:00:00Z","updated_at":"2026-08-12T09:00:00Z"},
        {"run_id":"r-human","state":"paused","reason":"awaiting_human","kernel":"pipeline","agent":"codex","created_at":"2026-08-12T08:00:00Z","updated_at":"2026-08-12T09:00:00Z"},
        {"run_id":"r-done","state":"done","kernel":"pipeline","agent":"codex","created_at":"2026-08-12T08:00:00Z","updated_at":"2026-08-12T09:00:00Z"},
        {"run_id":"r-failed","state":"failed","kernel":"pipeline","agent":"codex","created_at":"2026-08-12T08:00:00Z","updated_at":"2026-08-12T09:00:00Z"},
        {"run_id":"r-cancelled","state":"cancelled","kernel":"pipeline","agent":"codex","created_at":"2026-08-12T08:00:00Z","updated_at":"2026-08-12T09:00:00Z"},
        {"run_id":"r-broken","state":"unreadable","reason":"bad event log","kernel":"pipeline","agent":"codex","created_at":"2026-08-12T08:00:00Z"}
    ]}"#;
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response_body}",
        response_body.len()
    );
    let (address, _) = stub(response);

    let output = hako(&["--address", &address, "list"]);

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    for expected in [
        "r-running\trunning",
        "r-blocked\tpaused (blocked)",
        "r-verify\tpaused (verify_failed)",
        "r-timeout\tpaused (timeout)",
        "r-drift\tpaused (drift)",
        "r-budget\tpaused (budget)",
        "r-human\tpaused (awaiting_human)",
        "r-done\tdone",
        "r-failed\tfailed",
        "r-cancelled\tcancelled",
        "r-broken\tunreadable (bad event log)",
    ] {
        assert!(
            stdout.contains(expected),
            "missing `{expected}` in:\n{stdout}"
        );
    }
}

#[test]
fn attach_replays_a_finished_run_and_exits() {
    let first = r#"{"seq":0,"run_id":"r-done","at":"2026-08-12T08:00:00Z","type":"run_started","kernel":"pipeline","agent":"codex"}"#;
    let second = r#"{"seq":1,"run_id":"r-done","at":"2026-08-12T09:00:00Z","type":"state_changed","state":"done"}"#;
    let body = format!("id: 0\ndata: {first}\n\nid: 1\ndata: {second}\n\n");
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let (address, request) = stub(response);

    let output = hako(&["--address", &address, "attach", "r-done"]);

    assert!(output.status.success(), "{output:?}");
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        format!("{first}\n{second}\n")
    );
    let request = request.recv().unwrap();
    assert!(
        request
            .head
            .starts_with("GET /v1/runs/r-done/events HTTP/1.1\r\n")
    );
    assert!(!request.head.to_ascii_lowercase().contains("last-event-id:"));
}

#[test]
fn attach_follows_live_and_resumes_after_a_dropped_connection() {
    let replayed = r#"{"seq":0,"run_id":"r-live","at":"2026-08-12T08:00:00Z","type":"run_started","kernel":"pipeline","agent":"codex"}"#;
    let live = r#"{"seq":1,"run_id":"r-live","at":"2026-08-12T08:01:00Z","type":"iteration_started","iteration":1}"#;
    let terminal = r#"{"seq":2,"run_id":"r-live","at":"2026-08-12T08:02:00Z","type":"state_changed","state":"cancelled"}"#;
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let (requests, received) = mpsc::channel();
    let first_frame = format!("id: 0\ndata: {replayed}\n\n");
    let live_frame = format!("id: 1\ndata: {live}\n\n");
    let terminal_frame = format!("id: 2\ndata: {terminal}\n\n");
    thread::spawn(move || {
        let (mut first, _) = listener.accept().unwrap();
        requests.send(read_request(&mut first)).unwrap();
        first
            .write_all(
                b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n",
            )
            .unwrap();
        first.write_all(first_frame.as_bytes()).unwrap();
        first.flush().unwrap();
        thread::sleep(Duration::from_millis(25));
        first.write_all(live_frame.as_bytes()).unwrap();
        first.flush().unwrap();
        drop(first);

        let (mut resumed, _) = listener.accept().unwrap();
        requests.send(read_request(&mut resumed)).unwrap();
        write!(
            resumed,
            "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{terminal_frame}",
            terminal_frame.len()
        )
        .unwrap();
    });

    let output = hako(&["--address", &address, "attach", "r-live"]);

    assert!(output.status.success(), "{output:?}");
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        format!("{replayed}\n{live}\n{terminal}\n")
    );
    let initial = received.recv().unwrap();
    assert!(!initial.head.to_ascii_lowercase().contains("last-event-id:"));
    let resumed = received.recv().unwrap();
    assert!(
        resumed
            .head
            .to_ascii_lowercase()
            .contains("last-event-id: 1\r\n"),
        "{}",
        resumed.head
    );
}

#[test]
fn answer_displays_pending_questions_and_submits_one_structured_answer() {
    let status = r#"{
        "run_id":"r-paused","state":"paused","reason":"awaiting_human",
        "kernel":"pipeline","agent":"codex",
        "created_at":"2026-08-12T08:00:00Z","updated_at":"2026-08-12T09:00:00Z",
        "iterations_completed":1,"last_summary":"need a decision",
        "pending_questions":[
            {"id":"database","text":"Which database?","options":["sqlite","files"]},
            {"id":"retention","text":"Retain logs?","options":[]}
        ]
    }"#;
    let (address, requests) = stub_sequence(vec![json_response(status), json_response(status)]);

    let output = hako(&[
        "--address",
        &address,
        "answer",
        "r-paused",
        "database",
        "sqlite",
    ]);

    assert!(output.status.success(), "{output:?}");
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        "database: Which database?\n  options: sqlite, files\nretention: Retain logs?\nanswered database\n"
    );
    let status_request = requests.recv().unwrap();
    assert!(
        status_request
            .head
            .starts_with("GET /v1/runs/r-paused HTTP/1.1\r\n")
    );
    let answer_request = requests.recv().unwrap();
    assert!(
        answer_request
            .head
            .starts_with("POST /v1/runs/r-paused/answer HTTP/1.1\r\n")
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&answer_request.body).unwrap(),
        serde_json::json!({
            "answers": [{"question_id": "database", "answer": "sqlite"}]
        })
    );
}

#[test]
fn resume_sends_guidance_to_the_run() {
    let status = r#"{
        "run_id":"r-budget","state":"running",
        "kernel":"pipeline","agent":"codex",
        "created_at":"2026-08-12T08:00:00Z","updated_at":"2026-08-12T09:00:00Z",
        "iterations_completed":2,"last_summary":"budget exhausted","pending_questions":[]
    }"#;
    let (address, request) = stub(json_response(status));

    let output = hako(&[
        "--address",
        &address,
        "resume",
        "r-budget",
        "--note",
        "focus on the parser",
    ]);

    assert!(output.status.success(), "{output:?}");
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        "resumed r-budget\n"
    );
    let request = request.recv().unwrap();
    assert!(
        request
            .head
            .starts_with("POST /v1/runs/r-budget/resume HTTP/1.1\r\n")
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&request.body).unwrap(),
        serde_json::json!({"note": "focus on the parser", "extend": null})
    );
}

#[test]
fn cancel_drives_the_runs_cancel_endpoint() {
    let status = r#"{
        "run_id":"r-running","state":"cancelled",
        "kernel":"pipeline","agent":"codex",
        "created_at":"2026-08-12T08:00:00Z","updated_at":"2026-08-12T09:00:00Z",
        "iterations_completed":2,"last_summary":null,"pending_questions":[]
    }"#;
    let (address, request) = stub(json_response(status));

    let output = hako(&["--address", &address, "cancel", "r-running"]);

    assert!(output.status.success(), "{output:?}");
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        "cancelled r-running\n"
    );
    assert!(
        request
            .recv()
            .unwrap()
            .head
            .starts_with("POST /v1/runs/r-running/cancel HTTP/1.1\r\n")
    );
}

#[test]
fn interaction_rejections_use_the_daemon_failure_exit_code() {
    let body = r#"{"code":"run_not_running","message":"run already finished"}"#;
    let (address, _) = stub(error_response("409 Conflict", body));

    let output = hako(&["--address", &address, "cancel", "r-done"]);

    assert_eq!(output.status.code(), Some(3), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("cancel failed"), "{stderr}");
    assert!(stderr.contains("run already finished"), "{stderr}");
}

#[test]
fn attach_rejects_a_gap_in_the_event_history() {
    let event = r#"{"seq":1,"run_id":"r-gap","at":"2026-08-12T09:00:00Z","type":"state_changed","state":"done"}"#;
    let body = format!("id: 1\ndata: {event}\n\n");
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let (address, _) = stub(response);

    let output = hako(&["--address", &address, "attach", "r-gap"]);

    assert_eq!(output.status.code(), Some(3), "{output:?}");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("event sequence jumped from start to 1"),
        "{output:?}"
    );
}

#[cfg(unix)]
#[test]
fn run_auto_starts_a_local_daemon_that_outlives_the_client() {
    use std::os::unix::fs::PermissionsExt;

    let reserved = TcpListener::bind("127.0.0.1:0").unwrap();
    let socket = reserved.local_addr().unwrap().to_string();
    drop(reserved);

    let bin = tempfile::tempdir().unwrap();
    let config = tempfile::tempdir().unwrap();
    let hakod = bin.path().join("hakod");
    std::fs::write(
        &hakod,
        "#!/bin/sh\nexec \"$HAKO_STUB_TEST_BINARY\" --exact auto_start_daemon_helper --nocapture\n",
    )
    .unwrap();
    let mut permissions = std::fs::metadata(&hakod).unwrap().permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(&hakod, permissions).unwrap();
    let path = std::env::join_paths(std::iter::once(bin.path().to_path_buf()).chain(
        std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default()),
    ))
    .unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_hako"))
        .args(["run", &repo_path("../../examples/pipeline.toml")])
        .env("HAKO_ADDR", format!("http://{socket}"))
        .env_remove("HAKO_TOKEN")
        .env("XDG_CONFIG_HOME", config.path())
        .env("HAKO_STUB_DAEMON", "1")
        .env("HAKO_STUB_TEST_BINARY", std::env::current_exe().unwrap())
        .env("PATH", path)
        .output()
        .expect("hako runs");

    assert!(output.status.success(), "{output:?}");
    assert_eq!(String::from_utf8_lossy(&output.stdout), "local-run\n");
    assert_eq!(
        std::fs::metadata(config.path().join("hako/token"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );

    let listed = Command::new(env!("CARGO_BIN_EXE_hako"))
        .arg("list")
        .env("HAKO_ADDR", format!("http://{socket}"))
        .env_remove("HAKO_TOKEN")
        .env("XDG_CONFIG_HOME", config.path())
        .output()
        .expect("a second hako runs");
    assert!(listed.status.success(), "{listed:?}");
    assert!(String::from_utf8_lossy(&listed.stdout).contains("RUN ID\tSTATE"));
}

#[test]
fn auto_start_daemon_helper() {
    if std::env::var_os("HAKO_STUB_DAEMON").is_none() {
        return;
    }

    // The token the CLI handed us must be the one it persisted for
    // future clients — generated, written, and transmitted as one
    // value.
    let token = std::env::var("HAKO_TOKEN").unwrap();
    let config_home = std::env::var("XDG_CONFIG_HOME").unwrap();
    let persisted = std::fs::read_to_string(Path::new(&config_home).join("hako/token")).unwrap();
    assert_eq!(token, persisted);
    let listener = TcpListener::bind(std::env::var("HAKO_ADDR").unwrap()).unwrap();
    for expected_path in ["/v1/runs", "/v1/runs", "/v1/runs"] {
        let (mut stream, _) = listener.accept().unwrap();
        let request = read_request(&mut stream);
        assert!(request.head.contains(expected_path), "{}", request.head);
        assert!(request.head.to_ascii_lowercase().contains(&format!(
            "authorization: bearer {}\r\n",
            token.to_lowercase()
        )));
        let (status, body) = if request.head.starts_with("POST ") {
            ("201 Created", r#"{"run_id":"local-run"}"#)
        } else {
            ("200 OK", r#"{"runs":[]}"#)
        };
        write!(
            stream,
            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
        .unwrap();
    }
}
