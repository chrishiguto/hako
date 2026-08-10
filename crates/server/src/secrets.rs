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
//! checked at startup rather than trusted: the store must be `0700`
//! and readable by the daemon's own user. A run that starts is a run
//! whose secrets were never readable by the yolo agents next door.

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
    pub fn open(root: impl Into<PathBuf>) -> Result<Self, StoreError> {
        let root = root.into();
        create(&root)?;
        let metadata = std::fs::metadata(&root).map_err(|source| StoreError {
            path: root.clone(),
            problem: format!("cannot be read: {source}"),
        })?;
        if !metadata.is_dir() {
            return Err(StoreError {
                path: root,
                problem: "is not a directory".to_owned(),
            });
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let mode = metadata.permissions().mode() & 0o777;
            if mode != STORE_MODE {
                return Err(StoreError {
                    path: root,
                    problem: format!(
                        "must be mode {STORE_MODE:04o} so only the daemon's user can read it, \
                         but is {mode:04o} — run `chmod {STORE_MODE:o}` on it"
                    ),
                });
            }
            // Mode alone says owner-only; that the *daemon* is that
            // owner is what listing proves — a store belonging to
            // another user would deny this, and every later read.
            std::fs::read_dir(&root).map_err(|source| StoreError {
                path: root.clone(),
                problem: format!(
                    "is mode {STORE_MODE:04o} but the daemon's user cannot read it \
                     — it belongs to another user ({source})"
                ),
            })?;
        }
        Ok(Self { root })
    }

    /// The file a name resolves to, or `None` for a name that could
    /// address anything but a file directly in the store. Secret names
    /// come from flow files, and a flow file is written by an agent —
    /// `../../etc/shadow` must be a miss, not a read.
    fn path_of(&self, name: &SecretName) -> Option<PathBuf> {
        let name = name.as_str();
        let shaped = !name.is_empty()
            && name
                .chars()
                .all(|char| char.is_ascii_alphanumeric() || char == '_');
        shaped.then(|| self.root.join(name))
    }
}

/// Creates the store at `0700` when it is absent — a daemon's first
/// start should leave a store to provision into, not an error. An
/// existing directory is left exactly as it is: the checks above judge
/// it, and silently tightening someone's directory is not this
/// function's call.
fn create(root: &Path) -> Result<(), StoreError> {
    if root.exists() {
        return Ok(());
    }
    let failed = |source: std::io::Error| StoreError {
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
pub struct StoreError {
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

    #[test]
    fn a_store_that_is_a_file_fails_startup() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("secrets");
        std::fs::write(&root, "not a directory").unwrap();

        let error = FileSecrets::open(&root).unwrap_err();
        assert!(error.to_string().contains("is not a directory"), "{error}");
    }
}
