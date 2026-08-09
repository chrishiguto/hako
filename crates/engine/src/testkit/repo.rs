//! Real-git repository fixtures. The workspace is deliberately not a
//! seam — it is asserted through git effects — so every suite builds
//! the same kind of throwaway repositories.

use std::path::Path;

/// The one committed file every seeded repository starts with — a
/// stand-in for whatever a real repo holds.
pub const SEED_FILE: &str = "README.md";

/// A repository on branch `main` holding one committed file — what a
/// prepared workspace looks like before the loop runs.
pub fn seeded_repo() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    git(dir.path(), &["init", "-q", "-b", "main"]);
    std::fs::write(dir.path().join(SEED_FILE), "seed\n").unwrap();
    git(dir.path(), &["add", "-A"]);
    commit(dir.path(), "seed");
    dir
}

pub fn git(dir: &Path, args: &[&str]) {
    let status = std::process::Command::new("git")
        .args(args)
        .current_dir(dir)
        .status()
        .unwrap();
    assert!(status.success(), "git {args:?}");
}

/// A commit with a fixed test identity, so it succeeds on any host.
pub fn commit(dir: &Path, message: &str) {
    git(
        dir,
        &[
            "-c",
            "user.name=test",
            "-c",
            "user.email=test@localhost",
            "commit",
            "-qm",
            message,
        ],
    );
}

/// A git query's stdout, trimmed — `head` and friends.
pub fn git_stdout(dir: &Path, args: &[&str]) -> String {
    let output = std::process::Command::new("git")
        .args(args)
        .current_dir(dir)
        .output()
        .unwrap();
    assert!(output.status.success(), "git {args:?}");
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}

pub fn head(dir: &Path) -> String {
    git_stdout(dir, &["rev-parse", "HEAD"])
}

/// Every path the repository tracks — what a checkpoint committed.
pub fn tracked_files(dir: &Path) -> Vec<String> {
    git_stdout(dir, &["ls-files"])
        .lines()
        .map(str::to_owned)
        .collect()
}
