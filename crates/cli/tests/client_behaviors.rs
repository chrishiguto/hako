use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::process::{Command, Output};
use std::sync::mpsc::{self, Receiver};
use std::thread;

use api::SubmitRunRequest;

struct Request {
    head: String,
    body: String,
}

fn stub(response: String) -> (String, Receiver<Request>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let (requests, received) = mpsc::channel();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let request = read_request(&mut stream);
        let _ = requests.send(request);
        stream.write_all(response.as_bytes()).unwrap();
    });
    (address, received)
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
