//! The daemon's secret store: one file per secret in a directory only
//! the daemon can read.
//!
//! A file per name, rather than a database or the daemon's own
//! environment, for the reason the rest of the run store is plain
//! files: provisioning a secret is `install -m600 /dev/stdin
//! <store>/GH_TOKEN`, revoking one is `rm`, and auditing what a host
//! holds is `ls`. Daemon env would work for a single-tenant box, but
//! it leaks through `/proc/<pid>/environ` to anything running as the
//! same user, which on a mothership is the interactive lane.
//!
//! The permission bar is the whole security story here, so it is
//! checked at startup rather than trusted: the store must be owned by
//! the daemon's own user and closed to group and world. A run that
//! starts is a run whose secrets were never readable by the yolo
//! agents next door.

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use engine::{SecretName, SecretValue, SecretsError, SecretsProvider};

/// Directory permissions the store demands: owner-only, no group, no
/// world.
#[cfg(unix)]
const STORE_MODE: u32 = 0o700;

/// A directory of secret files, one per name.
#[derive(Debug)]
pub struct FileSecrets {
    root: PathBuf,
}

impl FileSecrets {
    /// Opens the store, creating it `0700` if it does not exist, and
    /// refuses to run against a directory that is not the daemon's own
    /// to read. Fails startup rather than the first run that needs a
    /// secret: a daemon with an exposed store should not come up at
    /// all.
    pub fn open(root: impl Into<PathBuf>) -> Result<Self, SecretStoreError> {
        let root = root.into();
        create(&root)?;
        let metadata = std::fs::metadata(&root).map_err(|source| SecretStoreError {
            path: root.clone(),
            problem: format!("cannot be read: {source}"),
        })?;
        if !metadata.is_dir() {
            return Err(SecretStoreError {
                path: root,
                problem: "is not a directory".to_owned(),
            });
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::{MetadataExt, PermissionsExt};

            let mode = metadata.permissions().mode() & 0o777;
            // SAFETY: geteuid touches no memory and cannot fail.
            let daemon = unsafe { libc::geteuid() };
            if let Some(problem) = refusal(mode, metadata.uid(), daemon) {
                return Err(SecretStoreError {
                    path: root,
                    problem,
                });
            }
        }
        Ok(Self { root })
    }

    /// The file a name resolves to, or `None` for a name that could
    /// address anything but a file directly in the store. Parsing
    /// already enforces the shape at the boundary, but names also come
    /// from trusted constructors — and a flow file is written by an
    /// agent, so `../../etc/shadow` must be a miss here no matter who
    /// let it through.
    fn path_of(&self, name: &SecretName) -> Option<PathBuf> {
        name.is_env_shaped().then(|| self.root.join(name.as_str()))
    }
}

/// Why the daemon must refuse this store, or `None` for one only the
/// daemon's own user can touch. A mode at least as strict as `0700`
/// passes; any group or world bit fails. Ownership is judged as a uid
/// comparison, never by probing a read: root reads through any mode,
/// so under a root-run daemon a successful read would prove nothing —
/// a `0700` store owned by the interactive-lane user must fail here.
/// Pure over what `stat` reports, so the judgement is testable for
/// users the test suite cannot become.
#[cfg(unix)]
fn refusal(mode: u32, owner: u32, daemon: u32) -> Option<String> {
    if mode & 0o077 != 0 {
        return Some(format!(
            "must be closed to group and world so only the daemon's user can read it, \
             but is {mode:04o} — run `chmod {STORE_MODE:o}` on it"
        ));
    }
    if mode & 0o500 != 0o500 {
        return Some(format!(
            "must let its owner read and enter it, but is {mode:04o} — \
             run `chmod {STORE_MODE:o}` on it"
        ));
    }
    if owner != daemon {
        return Some(format!(
            "belongs to uid {owner} while the daemon runs as uid {daemon} \
             — the store must be owned by the daemon's user"
        ));
    }
    None
}

/// Creates the store at `0700` when it is absent — a daemon's first
/// start should leave a store to provision into, not an error. An
/// existing directory is left exactly as it is: the checks above judge
/// it, and silently tightening someone's directory is not this
/// function's call.
fn create(root: &Path) -> Result<(), SecretStoreError> {
    if root.exists() {
        return Ok(());
    }
    let failed = |source: std::io::Error| SecretStoreError {
        path: root.to_path_buf(),
        problem: format!("cannot be created: {source}"),
    };
    if let Some(parent) = root.parent() {
        std::fs::create_dir_all(parent).map_err(failed)?;
    }
    let mut builder = std::fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(STORE_MODE);
    }
    builder.create(root).map_err(failed)
}

#[async_trait]
impl SecretsProvider for FileSecrets {
    async fn resolve(&self, name: &SecretName) -> Result<SecretValue, SecretsError> {
        let Some(path) = self.path_of(name) else {
            return Err(SecretsError::NotFound(name.clone()));
        };
        match tokio::fs::read_to_string(&path).await {
            // One trailing newline is dropped: `echo -n` is how a
            // secret file *should* be written and not how anyone
            // writes one.
            Ok(raw) => Ok(SecretValue::new(
                raw.strip_suffix('\n').map_or(raw.as_str(), |trimmed| {
                    trimmed.strip_suffix('\r').unwrap_or(trimmed)
                }),
            )),
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                Err(SecretsError::NotFound(name.clone()))
            }
            // Anything else — a mode nobody can read, a directory
            // where a file belongs — is the store failing, not the
            // secret missing. The distinction decides whether a
            // one-of requirement may fall through to its alternative.
            Err(source) => Err(SecretsError::Provider(format!(
                "cannot read secret `{name}` from the store: {source}"
            ))),
        }
    }
}

/// A store the daemon must not run against. Carries the path, because
/// the operator's next action is on that directory.
#[derive(Debug, thiserror::Error)]
#[error("the secret store at {} {problem}", path.display())]
pub struct SecretStoreError {
    path: PathBuf,
    problem: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store(root: &Path) -> FileSecrets {
        FileSecrets::open(root).unwrap()
    }

    fn write(root: &Path, name: &str, contents: &str) {
        std::fs::write(root.join(name), contents).unwrap();
    }

    #[tokio::test]
    async fn a_provisioned_secret_resolves_to_its_file_contents() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("secrets");
        let secrets = store(&root);
        write(&root, "GH_TOKEN", "ghp_1");

        assert_eq!(
            secrets
                .resolve(&SecretName::new("GH_TOKEN"))
                .await
                .unwrap()
                .expose(),
            "ghp_1"
        );
    }

    /// `echo secret > GH_TOKEN` is how the file gets written, so the
    /// trailing newline is the file format's problem, not the
    /// operator's.
    #[tokio::test]
    async fn one_trailing_newline_is_not_part_of_the_value() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("secrets");
        let secrets = store(&root);
        for (written, expected) in [
            ("ghp_1\n", "ghp_1"),
            ("ghp_1\r\n", "ghp_1"),
            ("ghp_1", "ghp_1"),
            ("ghp_1\n\n", "ghp_1\n"),
        ] {
            write(&root, "GH_TOKEN", written);
            assert_eq!(
                secrets
                    .resolve(&SecretName::new("GH_TOKEN"))
                    .await
                    .unwrap()
                    .expose(),
                expected,
                "{written:?}"
            );
        }
    }

    #[tokio::test]
    async fn an_unprovisioned_secret_is_a_miss_naming_it() {
        let dir = tempfile::tempdir().unwrap();
        let secrets = store(&dir.path().join("secrets"));
        let error = secrets
            .resolve(&SecretName::new("GH_TOKEN"))
            .await
            .unwrap_err();
        assert!(matches!(&error, SecretsError::NotFound(name) if name.as_str() == "GH_TOKEN"));
    }

    /// Flow files are agent-writable, so a name is not a path: a
    /// traversal reads nothing and reports the same miss as any
    /// unprovisioned name.
    #[tokio::test]
    async fn a_name_that_is_not_env_shaped_reads_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("secrets");
        let secrets = store(&root);
        std::fs::write(dir.path().join("outside"), "leaked").unwrap();

        for name in ["../outside", "sub/GH_TOKEN", "", "GH-TOKEN", "."] {
            let error = secrets.resolve(&SecretName::new(name)).await.unwrap_err();
            assert!(
                matches!(&error, SecretsError::NotFound(missing) if missing.as_str() == name),
                "{name}: {error}"
            );
        }
    }

    #[test]
    fn a_missing_store_is_created_owner_only() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("nested/secrets");
        store(&root);

        assert!(root.is_dir());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&root).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, STORE_MODE);
        }
    }

    /// The permission bar is the store's whole security story, so a
    /// store anyone could read stops the daemon at startup — with the
    /// path and the fix in the message.
    #[cfg(unix)]
    #[test]
    fn a_store_readable_by_anyone_else_fails_startup_naming_the_path() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("secrets");
        std::fs::create_dir(&root).unwrap();
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o755)).unwrap();

        let error = FileSecrets::open(&root).unwrap_err();
        let message = error.to_string();
        assert!(message.contains(&root.display().to_string()), "{message}");
        assert!(message.contains("0755"), "{message}");
        assert!(message.contains("chmod 700"), "{message}");
    }

    /// Stricter than `0700` is not a misconfiguration: the daemon only
    /// reads the store after creating it, so an owner who dropped the
    /// write bit gets a running daemon, not advice to loosen modes.
    #[cfg(unix)]
    #[test]
    fn a_stricter_than_0700_store_is_accepted() {
        assert_eq!(refusal(0o700, 42, 42), None);
        assert_eq!(refusal(0o500, 42, 42), None);
    }

    #[cfg(unix)]
    #[test]
    fn any_group_or_world_bit_is_refused() {
        for mode in [0o755, 0o770, 0o707, 0o750, 0o710, 0o701] {
            let Some(problem) = refusal(mode, 42, 42) else {
                panic!("{mode:04o} was accepted");
            };
            assert!(problem.contains("chmod 700"), "{problem}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn an_owner_who_cannot_read_or_enter_is_refused() {
        for mode in [0o300, 0o400, 0o200, 0o000] {
            let Some(problem) = refusal(mode, 42, 42) else {
                panic!("{mode:04o} was accepted");
            };
            assert!(problem.contains("owner"), "{problem}");
        }
    }

    /// The case a read-probe cannot catch: root reads through any
    /// mode, so a `0700` store owned by the interactive-lane user
    /// would have passed a probe and handed that user every secret.
    /// Ownership is a uid comparison, and the message names both
    /// sides.
    #[cfg(unix)]
    #[test]
    fn a_store_owned_by_another_user_is_refused_even_for_root() {
        let problem = refusal(0o700, 1000, 0).unwrap();
        assert!(problem.contains("uid 1000"), "{problem}");
        assert!(problem.contains("uid 0"), "{problem}");
    }

    #[test]
    fn a_store_that_is_a_file_fails_startup() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("secrets");
        std::fs::write(&root, "not a directory").unwrap();

        let error = FileSecrets::open(&root).unwrap_err();
        assert!(error.to_string().contains("is not a directory"), "{error}");
    }
}
