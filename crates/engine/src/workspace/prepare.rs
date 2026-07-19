//! Workspace preparation — how a run's workspace comes to exist,
//! before any kernel touches it. Two modes, deliberately an enum and
//! not a seam.
//!
//! Clone, the default: the source — git URL or local path — is cloned
//! into a run-owned directory on a run branch, so the user's checkout
//! and uncommitted work are unreachable by construction. A clone of a
//! remote URL keeps `origin`, so an agent whose prompts say to deliver
//! can push — the engine itself never pushes. A clone of a local path
//! has its remote stripped — the engine must never write into the
//! user's repository, and nothing the agent is told can either — so
//! its results stay in the workspace, inspectable as a local diff.
//!
//! Mount, the opt-in: the run works directly in an existing checkout —
//! refusing a dirty one unless forced, and locking the path to one
//! active run so the safety story survives the convenience path.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::io::AsyncWriteExt;
use tokio::process::Command;

use super::{Workspace, WorkspaceError, git, git_failure, git_ok};
use crate::run::RunId;
use proto::flow::{WorkspaceConfig, WorkspaceMode};

/// Every run branch a preparation creates: `hako/<run_id>`.
const RUN_BRANCH_PREFIX: &str = "hako/";

/// The dependency caches clone mode seeds from a local source
/// checkout — copy-on-write where the filesystem allows — so parallel
/// clones don't each pay a clean rebuild.
const DEP_CACHE_DIRS: [&str; 4] = ["target", "node_modules", ".venv", "vendor"];

/// The lock pinning a mounted checkout to one active run. It lives in
/// the checkout's git directory: with the repository, outside the
/// working tree.
const MOUNT_LOCK_FILE: &str = "hako-mount.lock";

/// Prepares the run's workspace as the flow's `[workspace]` table
/// asks. `clone_dest` is where a clone lands — the run's own
/// directory; mount mode ignores it and works at `repo` itself. The
/// workspace carries everything preparation established — a mounted
/// path's lock — so handing it to a kernel is the whole handover.
pub async fn prepare(
    config: &WorkspaceConfig,
    run_id: &RunId,
    clone_dest: &Path,
) -> Result<Workspace, WorkspaceError> {
    match config.mode {
        WorkspaceMode::Clone => {
            if config.force {
                return Err(WorkspaceError(
                    "workspace.force only applies to mount mode — clone mode never works in a \
                     checkout that could be dirty"
                        .into(),
                ));
            }
            clone(config, run_id, clone_dest).await
        }
        WorkspaceMode::Mount => {
            if config.branch.is_some() {
                return Err(WorkspaceError(
                    "workspace.branch only applies to clone mode — a mounted checkout works on \
                     the branch it already has checked out"
                        .into(),
                ));
            }
            mount(config, run_id).await
        }
    }
}

/// Whether `repo` names a local path — the deciding question of clone
/// mode. Anything that exists as a directory on this host is one;
/// everything else — ssh, https, and explicitly spelled `file://` URLs
/// included — is a remote by declaration: `origin` stays, so an agent
/// told to deliver can push the run branch there. The by-construction
/// safety story protects plain-path sources like the default
/// `repo = "."`, where a user points at a checkout without meaning to
/// publish anything into it.
fn is_local_path(repo: &str) -> bool {
    Path::new(repo).is_dir()
}

async fn clone(
    config: &WorkspaceConfig,
    run_id: &RunId,
    dest: &Path,
) -> Result<Workspace, WorkspaceError> {
    let local_source = is_local_path(&config.repo);

    let dest_text = utf8_path(dest)?;
    let mut args = vec!["clone", "--quiet"];
    if let Some(branch) = &config.branch {
        args.extend(["--branch", branch]);
    }
    // The repo string is flow-authored; after `--end-of-options` it
    // can only ever be a path or URL, dashes and all. The branch value
    // needs no fence: bound to `--branch`'s argument slot, it cannot
    // be reparsed as an option.
    args.extend(["--end-of-options", config.repo.as_str(), dest_text]);
    git_ok(None, &args).await?;
    // Canonical for the same reason mount's root is: the workspace
    // outlives any promise about the daemon's cwd.
    let dest = dest
        .canonicalize()
        .map_err(|error| WorkspaceError(format!("cannot resolve {}: {error}", dest.display())))?;

    // Seeding from a branch means continuing it — prior work piles up
    // there. Without one, the run gets its own.
    if config.branch.is_none() {
        let branch = format!("{RUN_BRANCH_PREFIX}{run_id}");
        git_ok(Some(&dest), &["checkout", "--quiet", "-b", &branch]).await?;
    }

    if local_source {
        // With the remote stripped, the user's repository is
        // unreachable from the workspace by construction — nothing the
        // run does, or is told to do, can push into it.
        git_ok(Some(&dest), &["remote", "remove", "origin"]).await?;
        seed_dep_caches(Path::new(&config.repo), &dest).await?;
    }

    Ok(Workspace {
        root: dest,
        lock: None,
    })
}

async fn mount(config: &WorkspaceConfig, run_id: &RunId) -> Result<Workspace, WorkspaceError> {
    let root = Path::new(&config.repo);
    if !root.is_dir() {
        return Err(WorkspaceError(format!(
            "mount path {} is not a directory",
            root.display()
        )));
    }
    // Canonical, because the daemon holds this path for the run's
    // whole life while its own cwd is nobody's promise.
    let root = root
        .canonicalize()
        .map_err(|error| WorkspaceError(format!("cannot resolve {}: {error}", root.display())))?;
    // Resolves through worktrees and split layouts alike — and proves
    // the path is a repository at all.
    let git_dir = git_ok(Some(&root), &["rev-parse", "--git-dir"]).await?;
    let git_dir = root.join(git_dir.trim());

    let status = git_ok(Some(&root), &["status", "--porcelain"]).await?;
    if !status.trim().is_empty() && !config.force {
        return Err(WorkspaceError(format!(
            "{} has uncommitted changes the run would sweep into its checkpoints; commit or \
             stash them, or set workspace.force = true to accept that",
            root.display()
        )));
    }

    let lock = MountLock::acquire(git_dir.join(MOUNT_LOCK_FILE), &root, run_id).await?;
    Ok(Workspace {
        root,
        lock: Some(Arc::new(lock)),
    })
}

/// Copies the source checkout's dependency caches into a fresh clone
/// with `cp --reflink=auto`: copy-on-write where the filesystem
/// supports it, a plain copy elsewhere. Only caches the clone ignores
/// are seeded — one in history arrives with the clone, and anything
/// else the checkpoint's `add -A` would sweep into history and push.
async fn seed_dep_caches(source: &Path, dest: &Path) -> Result<(), WorkspaceError> {
    for dir in DEP_CACHE_DIRS {
        let from = source.join(dir);
        let to = dest.join(dir);
        if !from.is_dir() || to.exists() {
            continue;
        }
        // Queried with a trailing slash so every spelling an author
        // writes — `target/`, `/target`, `target` — matches even
        // though the path does not exist in the clone yet.
        let ignored = git(Some(dest), &["check-ignore", "-q", &format!("{dir}/")]).await?;
        match ignored.status.code() {
            Some(0) => {}
            Some(1) => continue,
            _ => return Err(git_failure("check-ignore", &ignored)),
        }
        let output = Command::new("cp")
            .arg("--archive")
            .arg("--reflink=auto")
            .arg(&from)
            .arg(&to)
            .output()
            .await
            .map_err(|error| WorkspaceError(format!("cannot run cp: {error}")))?;
        if !output.status.success() {
            return Err(WorkspaceError(format!(
                "seeding {} failed: {}",
                from.display(),
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
    }
    Ok(())
}

/// One active run per mounted path, held as a file so it outlives the
/// daemon's memory. Dropping the guard releases it; a daemon that died
/// leaves it behind, and the refusal names the file to remove.
#[derive(Debug)]
pub(super) struct MountLock {
    path: PathBuf,
}

impl MountLock {
    async fn acquire(path: PathBuf, root: &Path, run_id: &RunId) -> Result<Self, WorkspaceError> {
        let mut file = match tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await
        {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                return Err(WorkspaceError(format!(
                    "{} is already mounted by {}; wait for it, or remove {} if that run is gone",
                    root.display(),
                    holder(&path).await,
                    path.display()
                )));
            }
            Err(error) => {
                return Err(WorkspaceError(format!(
                    "cannot lock {}: {error}",
                    path.display()
                )));
            }
        };
        // The guard exists from the moment the file does, so a failure
        // below unlinks the half-made lock through Drop instead of
        // leaving it to refuse every future run.
        let lock = Self { path };
        let write = async {
            file.write_all(run_id.as_str().as_bytes()).await?;
            file.flush().await
        };
        if let Err(error) = write.await {
            return Err(WorkspaceError(format!(
                "cannot lock {}: {error}",
                lock.path.display()
            )));
        }
        Ok(lock)
    }
}

/// Who a held lock says holds it — best effort, for the refusal
/// message only.
async fn holder(path: &Path) -> String {
    match tokio::fs::read_to_string(path).await {
        Ok(id) if !id.trim().is_empty() => format!("run {}", id.trim()),
        _ => "another run".into(),
    }
}

impl Drop for MountLock {
    fn drop(&mut self) {
        // Sync on purpose — Drop cannot await, and unlinking one file
        // does not block anything worth engineering around.
        let _ = std::fs::remove_file(&self.path);
    }
}

fn utf8_path(path: &Path) -> Result<&str, WorkspaceError> {
    path.to_str()
        .ok_or_else(|| WorkspaceError(format!("path {} is not valid UTF-8", path.display())))
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::super::testkit::{SEED_FILE, commit, git, git_stdout, head, seeded_repo};
    use super::*;

    fn clone_config(repo: impl Into<String>) -> WorkspaceConfig {
        WorkspaceConfig {
            repo: repo.into(),
            mode: WorkspaceMode::Clone,
            branch: None,
            force: false,
        }
    }

    fn mount_config(repo: impl Into<String>) -> WorkspaceConfig {
        WorkspaceConfig {
            mode: WorkspaceMode::Mount,
            ..clone_config(repo)
        }
    }

    fn run_id() -> RunId {
        RunId::new("r1")
    }

    /// Everything observable about a repository's git state: every ref
    /// with its target, plus the working-tree status. Equality before
    /// and after is what "never touches the source" means.
    fn repo_fingerprint(dir: &Path) -> String {
        format!(
            "{}\n{}\n{}",
            git_stdout(dir, &["for-each-ref"]),
            git_stdout(dir, &["status", "--porcelain"]),
            git_stdout(dir, &["symbolic-ref", "HEAD"]),
        )
    }

    /// A bare origin seeded from a fresh repository, addressed as a
    /// `file://` URL — the remote every push test pushes to, no
    /// network involved.
    fn bare_origin() -> (tempfile::TempDir, tempfile::TempDir, String) {
        let source = seeded_repo();
        let bare = tempfile::tempdir().unwrap();
        git(
            source.path(),
            &[
                "clone",
                "--quiet",
                "--bare",
                ".",
                bare.path().join("origin.git").to_str().unwrap(),
            ],
        );
        let url = format!("file://{}", bare.path().join("origin.git").display());
        (source, bare, url)
    }

    async fn prepared_clone(repo: &str, dest: &Path) -> Workspace {
        prepare(&clone_config(repo), &run_id(), dest).await.unwrap()
    }

    #[tokio::test]
    async fn a_clone_lands_on_a_run_branch_leaving_the_source_untouched() {
        let source = seeded_repo();
        // Uncommitted work in the source must stay unreachable.
        std::fs::write(source.path().join(SEED_FILE), "uncommitted edit\n").unwrap();
        std::fs::write(source.path().join("scratch.txt"), "untracked\n").unwrap();
        let before = repo_fingerprint(source.path());

        let dest = tempfile::tempdir().unwrap();
        let dest = dest.path().join("workspace");
        let workspace = prepared_clone(source.path().to_str().unwrap(), &dest).await;

        assert_eq!(workspace.mount().host, dest);
        assert_eq!(
            git_stdout(&dest, &["symbolic-ref", "--short", "HEAD"]),
            "hako/r1"
        );
        // The clone carries committed state only.
        assert_eq!(
            std::fs::read_to_string(dest.join(SEED_FILE)).unwrap(),
            "seed\n"
        );
        assert!(!dest.join("scratch.txt").exists());
        assert_eq!(repo_fingerprint(source.path()), before);
    }

    /// A local source is unreachable by construction: the clone keeps
    /// no remote at all, so no `git push` — the engine's or an
    /// agent's — has anywhere to land.
    #[tokio::test]
    async fn a_local_clone_has_no_remote_at_all() {
        let source = seeded_repo();
        let dest = tempfile::tempdir().unwrap();
        let dest = dest.path().join("workspace");
        let workspace = prepared_clone(source.path().to_str().unwrap(), &dest).await;

        assert_eq!(git_stdout(&dest, &["remote"]), "");
        std::fs::write(dest.join("work.rs"), "fn work() {}").unwrap();
        workspace.checkpoint("hako: iteration 1").await.unwrap();
        // The run branch and its checkpoint exist only in the
        // workspace — that is the locally inspectable diff.
        assert!(!git_stdout(source.path(), &["branch"]).contains("hako/r1"));
    }

    /// A URL clone keeps `origin`, so an agent whose prompts say to
    /// deliver can push the run branch — the push here is the agent's
    /// own `git push`, never the engine's.
    #[tokio::test]
    async fn a_url_clone_keeps_origin_for_the_agent_to_push_to() {
        let (_source, bare, url) = bare_origin();
        let origin = bare.path().join("origin.git");
        let dest = tempfile::tempdir().unwrap();
        let dest = dest.path().join("workspace");
        let workspace = prepared_clone(&url, &dest).await;

        assert_eq!(git_stdout(&dest, &["remote"]), "origin");
        std::fs::write(dest.join("work.rs"), "fn work() {}").unwrap();
        let checkpoint = workspace.checkpoint("hako: iteration 1").await.unwrap();

        git(&dest, &["push", "-q", "origin", "hako/r1"]);
        assert_eq!(
            git_stdout(&origin, &["rev-parse", "hako/r1"]),
            checkpoint.unwrap()
        );
    }

    /// Seed-from-branch continues prior work: the clone checks the
    /// branch out, and an agent-side push updates it — the branch's
    /// PR follows.
    #[tokio::test]
    async fn seeding_from_a_branch_continues_it() {
        let (source, bare, url) = bare_origin();
        let origin = bare.path().join("origin.git");
        git(source.path(), &["checkout", "-q", "-b", "feat/x"]);
        std::fs::write(source.path().join("prior.rs"), "fn prior() {}").unwrap();
        git(source.path(), &["add", "-A"]);
        commit(source.path(), "prior work");
        git(source.path(), &["push", "-q", &url, "feat/x"]);

        let dest = tempfile::tempdir().unwrap();
        let dest = dest.path().join("workspace");
        let config = WorkspaceConfig {
            branch: Some("feat/x".into()),
            ..clone_config(&url)
        };
        let workspace = prepare(&config, &run_id(), &dest).await.unwrap();

        assert_eq!(
            git_stdout(&dest, &["symbolic-ref", "--short", "HEAD"]),
            "feat/x"
        );
        assert!(dest.join("prior.rs").exists(), "prior work seeds the run");

        std::fs::write(dest.join("more.rs"), "fn more() {}").unwrap();
        workspace.checkpoint("hako: iteration 1").await.unwrap();
        git(&dest, &["push", "-q", "origin", "feat/x"]);
        assert_eq!(git_stdout(&origin, &["rev-parse", "feat/x"]), head(&dest));
    }

    /// Ignored dependency caches ride over from a local source so the
    /// clone doesn't pay a clean rebuild — without ever entering
    /// history.
    #[tokio::test]
    async fn dep_caches_seed_from_a_local_source_but_stay_out_of_history() {
        let source = seeded_repo();
        // Both ignore spellings authors actually write must seed.
        std::fs::write(source.path().join(".gitignore"), "target/\n/node_modules\n").unwrap();
        git(source.path(), &["add", "-A"]);
        commit(source.path(), "ignore the caches");
        std::fs::create_dir_all(source.path().join("target/debug")).unwrap();
        std::fs::write(source.path().join("target/debug/cache.o"), "warm").unwrap();
        std::fs::create_dir_all(source.path().join("node_modules/dep")).unwrap();
        std::fs::write(source.path().join("node_modules/dep/index.js"), "dep").unwrap();

        let dest = tempfile::tempdir().unwrap();
        let dest = dest.path().join("workspace");
        let workspace = prepared_clone(source.path().to_str().unwrap(), &dest).await;

        assert_eq!(
            std::fs::read_to_string(dest.join("target/debug/cache.o")).unwrap(),
            "warm"
        );
        assert_eq!(
            std::fs::read_to_string(dest.join("node_modules/dep/index.js")).unwrap(),
            "dep"
        );
        assert_eq!(
            workspace.checkpoint("hako: iteration 1").await.unwrap(),
            None,
            "a seeded cache is not the loop's work"
        );
    }

    /// A cache the clone does not ignore would be swept into the first
    /// checkpoint — and pushed. Seeding refuses to set that trap.
    #[tokio::test]
    async fn an_unignored_dep_cache_is_not_seeded() {
        let source = seeded_repo();
        std::fs::create_dir_all(source.path().join("vendor")).unwrap();
        std::fs::write(source.path().join("vendor/dep.js"), "cached").unwrap();

        let dest = tempfile::tempdir().unwrap();
        let dest = dest.path().join("workspace");
        let workspace = prepared_clone(source.path().to_str().unwrap(), &dest).await;

        assert!(!dest.join("vendor").exists());
        assert_eq!(
            workspace.checkpoint("hako: iteration 1").await.unwrap(),
            None
        );
    }

    /// A flow-authored repo string can never steer git on the host:
    /// behind `--end-of-options` it is a path or URL, dashes and all.
    #[tokio::test]
    async fn a_dashed_repo_is_a_repo_not_an_option() {
        let dest = tempfile::tempdir().unwrap();
        let error = prepare(
            &clone_config("--upload-pack=false"),
            &run_id(),
            &dest.path().join("workspace"),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("--upload-pack=false"), "{error}");
    }

    #[tokio::test]
    async fn force_is_rejected_outside_mount_mode() {
        let source = seeded_repo();
        let dest = tempfile::tempdir().unwrap();
        let config = WorkspaceConfig {
            force: true,
            ..clone_config(source.path().to_str().unwrap())
        };
        let error = prepare(&config, &run_id(), &dest.path().join("workspace"))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("mount"), "{error}");
    }

    #[tokio::test]
    async fn a_seed_branch_is_rejected_outside_clone_mode() {
        let source = seeded_repo();
        let dest = tempfile::tempdir().unwrap();
        let config = WorkspaceConfig {
            branch: Some("feat/x".into()),
            ..mount_config(source.path().to_str().unwrap())
        };
        let error = prepare(&config, &run_id(), &dest.path().join("workspace"))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("clone"), "{error}");
    }

    #[tokio::test]
    async fn a_mount_works_in_place_and_holds_the_path_for_one_run() {
        let checkout = seeded_repo();
        let unused = tempfile::tempdir().unwrap();
        let config = mount_config(checkout.path().to_str().unwrap());

        let first = prepare(&config, &run_id(), unused.path()).await.unwrap();
        assert_eq!(
            first.mount().host,
            checkout.path().canonicalize().unwrap(),
            "the mounted checkout itself is the workspace"
        );

        let second = prepare(&config, &RunId::new("r2"), unused.path())
            .await
            .unwrap_err();
        assert!(second.to_string().contains("run r1"), "{second}");
        assert!(second.to_string().contains(MOUNT_LOCK_FILE), "{second}");

        // The lock rides every clone of the workspace: it releases
        // only when the last holder is gone.
        let cloned = first.clone();
        drop(first);
        let still_held = prepare(&config, &RunId::new("r2"), unused.path())
            .await
            .unwrap_err();
        assert!(still_held.to_string().contains("run r1"), "{still_held}");

        drop(cloned);
        prepare(&config, &RunId::new("r2"), unused.path())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn a_dirty_mount_is_refused_unless_forced() {
        let checkout = seeded_repo();
        std::fs::write(checkout.path().join(SEED_FILE), "uncommitted\n").unwrap();
        let unused = tempfile::tempdir().unwrap();
        let config = mount_config(checkout.path().to_str().unwrap());

        let error = prepare(&config, &run_id(), unused.path())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("uncommitted"), "{error}");
        assert!(error.to_string().contains("force"), "{error}");

        let forced = WorkspaceConfig {
            force: true,
            ..config
        };
        prepare(&forced, &run_id(), unused.path()).await.unwrap();
    }

    #[tokio::test]
    async fn mounting_something_other_than_a_repository_fails() {
        let unused = tempfile::tempdir().unwrap();

        let missing = mount_config("/no/such/checkout");
        let error = prepare(&missing, &run_id(), unused.path())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("/no/such/checkout"), "{error}");

        let plain_dir = tempfile::tempdir().unwrap();
        let config = mount_config(plain_dir.path().to_str().unwrap());
        let error = prepare(&config, &run_id(), unused.path())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("git"), "{error}");
    }
}
