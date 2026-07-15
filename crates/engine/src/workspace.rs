//! The workspace a kernel works on — the one thing that survives
//! iterations. Deliberately not a seam: it is a real directory the
//! engine reaches with real git, in production and in tests alike.
//! Preparing one (clone vs mount) is a separate concern; a kernel
//! receives it ready.
//!
//! Scratch under [`PROGRESS_FILE`] is the exception: it is produced
//! inside the sandbox and read back through the sandbox seam, never
//! from here — kernels must keep working even where the guest's view
//! and the host's diverge.

use std::path::{Path, PathBuf};
use std::process::Output;

use tokio::process::Command;

use crate::sandbox::WorkspaceMount;

/// Where the workspace lands inside every sandbox. Fixed so domain
/// prompts and agent muscle memory transfer between flows.
const GUEST_ROOT: &str = "/workspace";

/// The engine's scratch directory inside the workspace — the agent
/// drops its progress report here, so checkpoints must never commit
/// it: scratch is conversation, not the loop's work.
const SCRATCH_DIR: &str = ".hako";

/// Where the agent must write its progress report, relative to the
/// workspace root. Part of the published agent contract — the preamble
/// quotes it verbatim.
pub const PROGRESS_FILE: &str = ".hako/progress.json";

/// The user-authored, agent-editable prompt file at the workspace
/// root. Re-read every iteration so an agent's edits take effect on
/// the next pass.
pub const DOMAIN_PROMPT_FILE: &str = "PROMPT.md";

/// A prepared workspace: a git repository the run owns. All host-side
/// effects — reading the domain prompt, checkpointing — go through
/// here.
#[derive(Debug, Clone)]
pub struct Workspace {
    root: PathBuf,
}

impl Workspace {
    pub fn at(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// How this workspace mounts into a sandbox.
    pub fn mount(&self) -> WorkspaceMount {
        WorkspaceMount {
            host: self.root.clone(),
            guest: PathBuf::from(GUEST_ROOT),
        }
    }

    /// Where the progress report lands inside the sandbox — the path a
    /// kernel hands the sandbox seam, because scratch is read through
    /// the guest's view, never the host's.
    pub fn guest_progress_path(&self) -> PathBuf {
        Path::new(GUEST_ROOT).join(PROGRESS_FILE)
    }

    /// The domain prompt as it stands right now.
    pub async fn domain_prompt(&self) -> Result<String, WorkspaceError> {
        let path = self.root.join(DOMAIN_PROMPT_FILE);
        tokio::fs::read_to_string(&path)
            .await
            .map_err(|error| WorkspaceError(format!("cannot read {}: {error}", path.display())))
    }

    /// Commits everything the iteration changed and returns the commit
    /// hash — or `None` when nothing changed, which is what drift
    /// detection watches for. Scratch under `.hako/` never enters
    /// history. Committer identity, hooks, and signing are forced off
    /// the user's config: a checkpoint is the engine's bookkeeping and
    /// must succeed on any host.
    pub async fn checkpoint(&self, message: &str) -> Result<Option<String>, WorkspaceError> {
        self.git_ok(&["add", "-A", "--", ".", &format!(":(exclude){SCRATCH_DIR}")])
            .await?;
        let staged = self.git(&["diff", "--cached", "--quiet"]).await?;
        match staged.status.code() {
            Some(0) => return Ok(None),
            Some(1) => {}
            _ => return Err(git_failure("diff --cached", &staged)),
        }
        self.git_ok(&[
            "-c",
            "user.name=hako",
            "-c",
            "user.email=hako@localhost",
            "commit",
            "--quiet",
            "--no-verify",
            "--no-gpg-sign",
            "-m",
            message,
        ])
        .await?;
        let head = self.git_ok(&["rev-parse", "HEAD"]).await?;
        Ok(Some(head.trim().to_owned()))
    }

    async fn git(&self, args: &[&str]) -> Result<Output, WorkspaceError> {
        Command::new("git")
            .args(args)
            .current_dir(&self.root)
            .output()
            .await
            .map_err(|error| WorkspaceError(format!("cannot run git: {error}")))
    }

    async fn git_ok(&self, args: &[&str]) -> Result<String, WorkspaceError> {
        let output = self.git(args).await?;
        if !output.status.success() {
            return Err(git_failure(&args.join(" "), &output));
        }
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    }
}

fn git_failure(command: &str, output: &Output) -> WorkspaceError {
    WorkspaceError(format!(
        "git {command} failed: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    ))
}

/// Host-side workspace work that failed — a run-fatal infrastructure
/// error, unlike anything the agent does inside the sandbox.
#[derive(Debug, thiserror::Error)]
#[error("workspace failure: {0}")]
pub struct WorkspaceError(pub String);

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    fn seeded_repo() -> (tempfile::TempDir, Workspace) {
        let dir = tempfile::tempdir().unwrap();
        git(dir.path(), &["init", "-q", "-b", "main"]);
        std::fs::write(dir.path().join(DOMAIN_PROMPT_FILE), "domain rules\n").unwrap();
        git(dir.path(), &["add", "-A"]);
        commit(dir.path(), "seed");
        let workspace = Workspace::at(dir.path());
        (dir, workspace)
    }

    fn git(dir: &Path, args: &[&str]) {
        let status = std::process::Command::new("git")
            .args(args)
            .current_dir(dir)
            .status()
            .unwrap();
        assert!(status.success(), "git {args:?}");
    }

    fn commit(dir: &Path, message: &str) {
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

    fn head(dir: &Path) -> String {
        let output = std::process::Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(dir)
            .output()
            .unwrap();
        String::from_utf8(output.stdout).unwrap().trim().to_owned()
    }

    #[test]
    fn the_progress_file_lives_in_the_scratch_dir() {
        assert!(Path::new(PROGRESS_FILE).starts_with(SCRATCH_DIR));
    }

    #[test]
    fn the_mount_lands_the_workspace_at_the_fixed_guest_root() {
        let workspace = Workspace::at("/srv/runs/r1/workspace");
        assert_eq!(
            workspace.mount(),
            WorkspaceMount {
                host: PathBuf::from("/srv/runs/r1/workspace"),
                guest: PathBuf::from("/workspace"),
            }
        );
    }

    #[tokio::test]
    async fn the_domain_prompt_is_read_from_the_workspace() {
        let (_dir, workspace) = seeded_repo();
        assert_eq!(workspace.domain_prompt().await.unwrap(), "domain rules\n");
    }

    #[tokio::test]
    async fn a_missing_domain_prompt_fails_naming_the_path() {
        let dir = tempfile::tempdir().unwrap();
        let error = Workspace::at(dir.path()).domain_prompt().await.unwrap_err();
        assert!(error.to_string().contains(DOMAIN_PROMPT_FILE), "{error}");
    }

    #[tokio::test]
    async fn a_checkpoint_commits_the_changes_and_reports_the_hash() {
        let (dir, workspace) = seeded_repo();
        std::fs::write(dir.path().join("work.rs"), "fn work() {}").unwrap();

        let commit = workspace.checkpoint("hako: iteration 1").await.unwrap();

        assert_eq!(commit, Some(head(dir.path())));
        let status = std::process::Command::new("git")
            .args(["status", "--porcelain"])
            .current_dir(dir.path())
            .output()
            .unwrap();
        assert!(status.stdout.is_empty(), "checkpoint left changes behind");
    }

    #[tokio::test]
    async fn an_unchanged_workspace_checkpoints_to_nothing() {
        let (dir, workspace) = seeded_repo();
        let before = head(dir.path());
        assert_eq!(
            workspace.checkpoint("hako: iteration 1").await.unwrap(),
            None
        );
        assert_eq!(head(dir.path()), before);
    }

    #[tokio::test]
    async fn scratch_never_enters_history() {
        let (dir, workspace) = seeded_repo();
        let scratch = dir.path().join(SCRATCH_DIR);
        std::fs::create_dir(&scratch).unwrap();
        std::fs::write(scratch.join("progress.json"), "{}").unwrap();

        assert_eq!(
            workspace.checkpoint("hako: iteration 1").await.unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn a_checkpoint_outside_a_repository_fails() {
        let dir = tempfile::tempdir().unwrap();
        let error = Workspace::at(dir.path())
            .checkpoint("hako: iteration 1")
            .await
            .unwrap_err();
        assert!(error.to_string().contains("git"), "{error}");
    }
}
