//! The real-CLI contract, run against an installed smolvm — ignored by
//! default because CI has none. Locally:
//!
//! ```sh
//! cargo test -p sandbox --test smolvm -- --ignored
//! ```
//!
//! Machines are bare (no `--image`), so no registry pull and no
//! network is needed. What lives here is exactly what the fake-binary
//! tests cannot prove: smolvm's mount, streaming, exit-code, and
//! teardown behavior on this machine. The rw-mount checks carry the
//! most weight: the mounted workspace is the only channel through
//! which an iteration's work survives, and Linux rw mounts are where
//! upstream has broken before (smol-machines/smolvm#428).

use std::collections::BTreeMap;
use std::path::Path;

use engine::{ExecEvent, ExecSpec, Sandbox, SandboxSpec, SecretValue, Workspace};
use futures_util::StreamExt;
use sandbox::{SmolvmConfig, SmolvmSandbox};

fn adapter() -> SmolvmSandbox {
    SmolvmSandbox::new(SmolvmConfig::default())
}

/// Collects a finished stream into (stdout, stderr, exit code),
/// asserting the contract: chunks first, exactly one final `Exited`.
async fn drain(
    adapter: &SmolvmSandbox,
    vm: &engine::SandboxHandle,
    argv: &[&str],
) -> (Vec<u8>, Vec<u8>, Option<i32>) {
    let command = ExecSpec {
        argv: argv.iter().map(|s| s.to_string()).collect(),
        cwd: None,
    };
    let events: Vec<ExecEvent> = adapter
        .exec_stream(vm, &command)
        .await
        .unwrap()
        .map(Result::unwrap)
        .collect()
        .await;

    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let mut code = None;
    for (i, event) in events.iter().enumerate() {
        match event {
            ExecEvent::Stdout(bytes) => stdout.extend(bytes),
            ExecEvent::Stderr(bytes) => stderr.extend(bytes),
            ExecEvent::Exited(status) => {
                assert_eq!(i, events.len() - 1, "Exited was not last: {events:?}");
                code = Some(status.code);
            }
        }
    }
    (stdout, stderr, code.expect("stream ended without Exited"))
}

#[tokio::test]
#[ignore = "requires smolvm installed"]
async fn preflight_accepts_the_installed_smolvm() {
    adapter().preflight().await.unwrap();
}

#[tokio::test]
#[ignore = "requires smolvm installed"]
async fn a_full_iteration_lifecycle_against_real_smolvm() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("h2g.txt"), "host to guest\n").unwrap();

    let adapter = adapter();
    let spec = SandboxSpec {
        workspace: Workspace::at(dir.path()).mount(),
        env: BTreeMap::from([("HAKO_TEST_TOKEN".to_string(), SecretValue::new("s3cret"))]),
    };
    let vm = adapter.create(&spec).await.unwrap();

    // The workspace mount is read-write and visible in both
    // directions — the only channel through which an iteration's work
    // survives.
    let (stdout, _, code) = drain(&adapter, &vm, &["cat", "/workspace/h2g.txt"]).await;
    assert_eq!(code, Some(0));
    assert_eq!(stdout, b"host to guest\n");

    let (_, _, code) = drain(
        &adapter,
        &vm,
        &["sh", "-c", "echo guest to host > /workspace/g2h.txt"],
    )
    .await;
    assert_eq!(code, Some(0));
    assert_eq!(
        std::fs::read_to_string(dir.path().join("g2h.txt")).unwrap(),
        "guest to host\n"
    );

    // Streaming keeps the pipes apart and the guest's exit code
    // arrives intact.
    let (stdout, stderr, code) = drain(
        &adapter,
        &vm,
        &["sh", "-c", "echo out; echo err >&2; exit 42"],
    )
    .await;
    assert_eq!(stdout, b"out\n");
    assert_eq!(stderr, b"err\n");
    assert_eq!(code, Some(42));

    // The exec working directory defaults to the workspace.
    let (stdout, _, code) = drain(&adapter, &vm, &["pwd"]).await;
    assert_eq!(code, Some(0));
    assert_eq!(stdout, b"/workspace\n");

    // Secrets injected at create are present in the exec environment.
    let (stdout, _, code) = drain(
        &adapter,
        &vm,
        &["sh", "-c", "printf '%s' \"$HAKO_TEST_TOKEN\""],
    )
    .await;
    assert_eq!(code, Some(0));
    assert_eq!(stdout, b"s3cret");

    // Files round-trip byte-exact through put/get, and a put under the
    // mount is the host's file too.
    let contents = b"prompt\x00with\xffbinary bytes";
    adapter
        .put_file(&vm, Path::new("/workspace/put.bin"), contents)
        .await
        .unwrap();
    assert_eq!(std::fs::read(dir.path().join("put.bin")).unwrap(), contents);
    assert_eq!(
        adapter
            .get_file(&vm, Path::new("/workspace/put.bin"))
            .await
            .unwrap(),
        contents
    );

    // Reading a file that is not there is an error, not empty bytes.
    adapter
        .get_file(&vm, Path::new("/workspace/absent.txt"))
        .await
        .unwrap_err();

    // Destroy leaves nothing behind: smolvm no longer knows the name.
    let name = vm.as_str().to_string();
    adapter.destroy(vm).await.unwrap();
    let machines = std::process::Command::new("smolvm")
        .args(["machine", "ls", "--json"])
        .output()
        .unwrap();
    let listing = String::from_utf8_lossy(&machines.stdout);
    assert!(
        !listing.contains(&name),
        "destroyed machine still listed: {listing}"
    );
}
