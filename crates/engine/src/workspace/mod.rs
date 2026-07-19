//! The workspace a kernel works on — the one thing that survives
//! iterations. Deliberately not a seam: it is a real directory the
//! engine reaches with real git, in production and in tests alike.
//! Preparing one (clone vs mount) is [`prepare`]'s concern; a kernel
//! receives it ready.
//!
//! Scratch under [`PROGRESS_FILE`] is the exception: it is produced
//! inside the sandbox and read back through the sandbox seam, never
//! from here — kernels must keep working even where the guest's view
//! and the host's diverge.

mod prepare;

use std::path::{Path, PathBuf};
use std::process::Output;
use std::sync::Arc;

use tokio::process::Command;

use crate::sandbox::{WorkspaceMount, into_text};

pub use prepare::prepare;

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
/// effects — reading the domain prompt, checkpointing, pushing — go
/// through here.
#[derive(Debug, Clone)]
pub struct Workspace {
    root: PathBuf,
    /// The branch [`Workspace::push`] sends to `origin` — set by clone
    /// preparation when the source is a remote URL. `None` pushes
    /// nowhere: the run branch stays inspectable in the workspace
    /// itself.
    push_branch: Option<String>,
    /// Mount mode's one-active-run lock, riding every clone of this
    /// workspace so it cannot release before the last holder is gone —
    /// the host keeps a `Workspace`, and the path stays held, by
    /// construction.
    #[expect(dead_code, reason = "held for its Drop: releases the mounted path")]
    lock: Option<Arc<prepare::MountLock>>,
}

impl Workspace {
    pub fn at(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            push_branch: None,
            lock: None,
        }
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

    /// Pushes the run branch to `origin` and names what it pushed —
    /// the durability step after a checkpoint, so a daemon crash
    /// cannot hold an AFK run's work hostage. `None` means this
    /// workspace has nowhere to push — a clone of a local path, a
    /// mounted checkout — and the work stays inspectable locally
    /// instead. The branch is flow-authored in the seed-from-branch
    /// case, so it rides behind `--end-of-options`: a name can never
    /// steer git.
    pub async fn push(&self) -> Result<Option<String>, WorkspaceError> {
        let Some(branch) = self.push_branch.as_deref() else {
            return Ok(None);
        };
        self.git_ok(&["push", "--quiet", "--end-of-options", "origin", branch])
            .await?;
        Ok(Some(branch.to_owned()))
    }

    async fn git(&self, args: &[&str]) -> Result<Output, WorkspaceError> {
        git(Some(&self.root), args).await
    }

    async fn git_ok(&self, args: &[&str]) -> Result<String, WorkspaceError> {
        git_ok(Some(&self.root), args).await
    }
}

/// Runs git in `dir` — or in the process's own cwd when `None`, which
/// is what `clone` needs: the workspace directory does not exist yet,
/// and relative path arguments must resolve the way the caller wrote
/// them. Fails only when git itself cannot run.
async fn git(dir: Option<&Path>, args: &[&str]) -> Result<Output, WorkspaceError> {
    let mut command = Command::new("git");
    command.args(args);
    if let Some(dir) = dir {
        command.current_dir(dir);
    }
    command
        .output()
        .await
        .map_err(|error| WorkspaceError(format!("cannot run git: {error}")))
}

/// Like [`git`], but demands success and hands back the stdout.
async fn git_ok(dir: Option<&Path>, args: &[&str]) -> Result<String, WorkspaceError> {
    let output = git(dir, args).await?;
    if !output.status.success() {
        return Err(git_failure(&args.join(" "), &output));
    }
    Ok(into_text(output.stdout))
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

/// Real-git fixtures shared by this module's tests and preparation's:
/// the workspace is asserted through git effects, so every suite
/// builds the same kind of throwaway repositories.
#[cfg(test)]
pub(crate) mod testkit {
    use std::path::Path;

    use super::DOMAIN_PROMPT_FILE;

    /// A repository on branch `main` holding one committed domain
    /// prompt.
    pub fn seeded_repo() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        git(dir.path(), &["init", "-q", "-b", "main"]);
        std::fs::write(dir.path().join(DOMAIN_PROMPT_FILE), "domain rules\n").unwrap();
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
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::testkit::{head, seeded_repo as seeded_dir};
    use super::*;

    fn seeded_repo() -> (tempfile::TempDir, Workspace) {
        let dir = seeded_dir();
        let workspace = Workspace::at(dir.path());
        (dir, workspace)
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

    /// `Workspace::at` builds the no-remote workspace tests and mount
    /// mode use — pushing is something only clone preparation can
    /// switch on.
    #[tokio::test]
    async fn a_workspace_without_a_push_target_pushes_nowhere() {
        let (_dir, workspace) = seeded_repo();
        assert_eq!(workspace.push().await.unwrap(), None);
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
