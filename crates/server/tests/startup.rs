#![cfg(unix)]

use std::fs;
use std::net::{TcpListener, TcpStream};
use std::os::unix::fs::PermissionsExt;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

#[test]
fn a_smolvm_pin_mismatch_refuses_before_the_listener_binds() {
    let root = tempfile::tempdir().unwrap();
    let bin = root.path().join("bin");
    fs::create_dir(&bin).unwrap();
    let started = root.path().join("preflight-started");
    let smolvm = bin.join("smolvm");
    fs::write(
        &smolvm,
        "#!/bin/sh\n: > \"$HAKO_PREFLIGHT_STARTED\"\n/bin/sleep 0.5\nprintf '%s\\n' 'smolvm 0.0.0'\n",
    )
    .unwrap();
    fs::set_permissions(&smolvm, fs::Permissions::from_mode(0o700)).unwrap();

    let reserved = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = reserved.local_addr().unwrap();
    drop(reserved);

    let daemon = Command::new(env!("CARGO_BIN_EXE_hakod"))
        .env_clear()
        .env("PATH", format!("{}:/usr/bin:/bin", bin.display()))
        .env("HAKO_ADDR", address.to_string())
        .env("HAKO_TOKEN", "startup-test-token")
        .env("HAKO_RUNS_DIR", root.path().join("runs"))
        .env("HAKO_SECRETS_DIR", root.path().join("secrets"))
        .env("HAKO_PREFLIGHT_STARTED", &started)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(2);
    while !started.exists() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(10));
    }
    assert!(started.exists(), "hakod never invoked smolvm preflight");
    assert!(
        TcpStream::connect(address).is_err(),
        "hakod bound its listener before preflight finished"
    );

    let output = daemon.wait_with_output().unwrap();
    assert!(!output.status.success(), "{output:?}");
    let stderr = String::from_utf8_lossy(&output.stderr);
    let refusal = format!(
        "smolvm 0.0.0 does not match the pinned {}",
        sandbox::PINNED_SMOLVM_VERSION
    );
    assert!(stderr.contains(&refusal), "{stderr}");
}
