//! The adapter's process plumbing proven against a scripted `smolvm`
//! stand-in: preflight's three failure modes, exec streaming with exit
//! codes, secret injection, and file transfer all run on a machine
//! with no smolvm installed. Only the real-CLI contract lives in the
//! ignored `smolvm` integration tests.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};

use engine::{ExecEvent, ExecSpec, Sandbox, SandboxHandle, SandboxSpec, SecretValue, Workspace};
use futures_util::StreamExt;
use sandbox::{PINNED_SMOLVM_VERSION, SmolvmConfig, SmolvmSandbox};

/// Writes an executable shell script posing as the smolvm binary.
/// Scripts dispatch on `$2` because the adapter always shapes argv as
/// `machine <subcommand> --name NAME …`.
///
/// The write happens in a child process: a write fd held here could be
/// inherited by another test's concurrently forked child and fail this
/// script's exec with ETXTBSY.
fn fake_smolvm(dir: &Path, body: &str) -> PathBuf {
    let path = dir.join("smolvm");
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
        .write_all(format!("#!/bin/sh\n{body}\n").as_bytes())
        .unwrap();
    assert!(writer.wait().unwrap().success());
    path
}

fn adapter_for(binary: PathBuf) -> SmolvmSandbox {
    SmolvmSandbox::new(SmolvmConfig {
        binary,
        ..SmolvmConfig::default()
    })
}

/// The tempdir-plus-scripted-binary pair most tests start from. The
/// tempdir rides along so the script outlives the adapter.
fn fake_adapter(body: &str) -> (tempfile::TempDir, SmolvmSandbox) {
    let dir = tempfile::tempdir().unwrap();
    let adapter = adapter_for(fake_smolvm(dir.path(), body));
    (dir, adapter)
}

fn spec_with_env(env: BTreeMap<String, SecretValue>) -> SandboxSpec {
    SandboxSpec {
        workspace: Workspace::at("/srv/runs/r1/workspace").mount(),
        env,
    }
}

fn spec() -> SandboxSpec {
    spec_with_env(BTreeMap::new())
}

/// Runs a throwaway command and collects the whole event stream; the
/// argv is irrelevant because the fake scripts its own output.
async fn exec_events(adapter: &SmolvmSandbox, vm: &SandboxHandle) -> Vec<ExecEvent> {
    adapter
        .exec_stream(
            vm,
            &ExecSpec {
                argv: vec!["anything".into()],
                cwd: None,
            },
        )
        .await
        .unwrap()
        .map(Result::unwrap)
        .collect()
        .await
}

#[tokio::test]
async fn preflight_names_the_missing_binary_and_the_pin() {
    let adapter = adapter_for(PathBuf::from("/nonexistent/smolvm"));
    let error = adapter.preflight().await.unwrap_err().to_string();
    assert!(error.contains("not found"), "{error}");
    assert!(error.contains("/nonexistent/smolvm"), "{error}");
    assert!(error.contains(PINNED_SMOLVM_VERSION), "{error}");
}

#[tokio::test]
async fn preflight_names_both_versions_on_a_pin_mismatch() {
    let (_dir, adapter) = fake_adapter(r#"echo "smolvm 0.9.0""#);
    let error = adapter.preflight().await.unwrap_err().to_string();
    assert!(error.contains("0.9.0"), "{error}");
    assert!(error.contains(PINNED_SMOLVM_VERSION), "{error}");
}

#[tokio::test]
async fn preflight_rejects_output_that_is_not_smolvm_at_all() {
    let (_dir, adapter) = fake_adapter(r#"echo "krunvm 0.2.6""#);
    let error = adapter.preflight().await.unwrap_err().to_string();
    assert!(error.contains("krunvm 0.2.6"), "{error}");
}

#[tokio::test]
async fn preflight_accepts_the_pinned_version() {
    let (_dir, adapter) = fake_adapter(&format!(r#"echo "smolvm {PINNED_SMOLVM_VERSION}""#));
    adapter.preflight().await.unwrap();
}

#[tokio::test]
async fn exec_streams_both_pipes_and_ends_with_the_exit_code() {
    let (_dir, adapter) = fake_adapter(
        r#"case "$2" in
exec) printf 'chunk-out'; printf 'chunk-err' >&2; exit 7;;
*) exit 0;;
esac"#,
    );

    let vm = adapter.create(&spec()).await.unwrap();
    let events = exec_events(&adapter, &vm).await;

    let mut stdout: Vec<u8> = Vec::new();
    let mut stderr: Vec<u8> = Vec::new();
    for event in &events[..events.len() - 1] {
        match event {
            ExecEvent::Stdout(bytes) => stdout.extend_from_slice(bytes),
            ExecEvent::Stderr(bytes) => stderr.extend_from_slice(bytes),
            ExecEvent::Exited(_) => panic!("Exited before the stream ended"),
        }
    }
    assert_eq!(stdout, b"chunk-out");
    assert_eq!(stderr, b"chunk-err");
    let ExecEvent::Exited(status) = events.last().unwrap() else {
        panic!("stream did not end with Exited: {events:?}");
    };
    assert_eq!(status.code, Some(7));
}

#[tokio::test]
async fn secrets_reach_the_exec_through_the_environment_not_argv() {
    // The stand-in leaks what the real smolvm would resolve: the
    // prefixed host-side variable the adapter must have set.
    let (_dir, adapter) = fake_adapter(
        r#"case "$2" in
exec) printf '%s' "$HAKO_SECRET_GH_TOKEN";;
*) exit 0;;
esac"#,
    );

    let spec = spec_with_env(BTreeMap::from([(
        "GH_TOKEN".to_string(),
        SecretValue::new("ghp_secret"),
    )]));
    let vm = adapter.create(&spec).await.unwrap();
    let events = exec_events(&adapter, &vm).await;

    assert!(
        events.contains(&ExecEvent::Stdout(b"ghp_secret".to_vec())),
        "{events:?}"
    );
}

#[tokio::test]
async fn a_host_path_that_breaks_the_volume_mapping_is_refused() {
    let (_dir, adapter) = fake_adapter("exit 0");
    let spec = SandboxSpec {
        workspace: Workspace::at("/srv/runs/r:1/workspace").mount(),
        env: BTreeMap::new(),
    };
    let error = adapter.create(&spec).await.unwrap_err().to_string();
    assert!(error.contains("r:1"), "{error}");
}

#[tokio::test]
async fn relative_guest_paths_are_refused_for_put_and_get() {
    let (_dir, adapter) = fake_adapter("exit 0");
    let vm = adapter.create(&spec()).await.unwrap();

    let error = adapter
        .put_file(&vm, Path::new("relative.txt"), b"x")
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("must be absolute"), "{error}");

    let error = adapter
        .get_file(&vm, Path::new("relative.txt"))
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("must be absolute"), "{error}");
}

#[tokio::test]
async fn an_env_name_that_breaks_the_secret_mapping_is_refused() {
    let (_dir, adapter) = fake_adapter("exit 0");
    let spec = spec_with_env(BTreeMap::from([(
        "BAD=NAME".to_string(),
        SecretValue::new("v"),
    )]));
    let error = adapter.create(&spec).await.unwrap_err().to_string();
    assert!(error.contains("BAD=NAME"), "{error}");
}

#[tokio::test]
async fn put_file_pipes_the_exact_bytes_to_exec_stdin() {
    let dir = tempfile::tempdir().unwrap();
    let captured = dir.path().join("captured");
    let adapter = adapter_for(fake_smolvm(
        dir.path(),
        &format!(
            r#"case "$2" in
exec) cat > {};;
*) exit 0;;
esac"#,
            captured.display()
        ),
    ));

    let vm = adapter.create(&spec()).await.unwrap();
    let contents = b"prompt\x00with\xffbinary";
    adapter
        .put_file(&vm, Path::new("/workspace/PROMPT.md"), contents)
        .await
        .unwrap();

    assert_eq!(std::fs::read(&captured).unwrap(), contents);
}

#[tokio::test]
async fn get_file_returns_exec_stdout_byte_for_byte() {
    let (_dir, adapter) = fake_adapter(
        r#"case "$2" in
exec) printf 'report\000bytes';;
*) exit 0;;
esac"#,
    );

    let vm = adapter.create(&spec()).await.unwrap();
    let bytes = adapter
        .get_file(&vm, Path::new("/workspace/.hako/progress.json"))
        .await
        .unwrap();
    assert_eq!(bytes, b"report\x00bytes");
}

#[tokio::test]
async fn a_failed_start_deletes_the_half_created_machine() {
    let dir = tempfile::tempdir().unwrap();
    let deleted = dir.path().join("deleted");
    let adapter = adapter_for(fake_smolvm(
        dir.path(),
        &format!(
            r#"case "$2" in
start) echo "boot failed" >&2; exit 1;;
delete) touch {};;
*) exit 0;;
esac"#,
            deleted.display()
        ),
    ));

    let error = adapter.create(&spec()).await.unwrap_err().to_string();
    assert!(error.contains("boot failed"), "{error}");
    assert!(deleted.exists(), "the machine leaked");
}

#[tokio::test]
async fn operations_on_an_unknown_handle_fail_without_spawning() {
    let adapter = adapter_for(PathBuf::from("/nonexistent/smolvm"));
    let stranger = SandboxHandle::new("hako-stranger");
    let error = adapter
        .exec_stream(
            &stranger,
            &ExecSpec {
                argv: vec!["true".into()],
                cwd: None,
            },
        )
        .await
        .err()
        .expect("an unknown handle must be an error")
        .to_string();
    assert!(error.contains("unknown sandbox"), "{error}");
}

#[tokio::test]
async fn destroy_consumes_the_handle_even_when_smolvm_fails() {
    let (_dir, adapter) = fake_adapter(
        r#"case "$2" in
delete) exit 1;;
*) exit 0;;
esac"#,
    );

    let vm = adapter.create(&spec()).await.unwrap();
    let name = vm.as_str().to_string();
    adapter.destroy(vm).await.unwrap_err();

    // The machine is forgotten regardless: nothing may address it.
    let error = adapter
        .get_file(&SandboxHandle::new(name), Path::new("/x"))
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("unknown sandbox"), "{error}");
}
