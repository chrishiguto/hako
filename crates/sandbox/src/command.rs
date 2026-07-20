//! The pure half of the adapter: smolvm argv construction, the input
//! validation guarding those argv formats, and version parsing. No
//! process is ever spawned here, so everything is unit-testable on a
//! machine without smolvm installed.

use std::path::Path;

use engine::{ExecSpec, SandboxError, WorkspaceMount};

/// The one smolvm version this adapter is written against. smolvm is
/// young and fast-moving, so the daemon refuses to run against
/// anything else: silent upstream drift must fail preflight, not
/// corrupt iterations.
pub const PINNED_SMOLVM_VERSION: &str = "1.6.3";

/// Prefix for the host-side variables that carry secret values into
/// `--secret-env GUEST=HOST`. The value travels via the spawned
/// process's environment, never its argv, so it cannot leak through a
/// process listing. The prefix keeps guest names like `PATH` or
/// `RUST_LOG` from steering the smolvm client itself.
const SECRET_HOST_PREFIX: &str = "HAKO_SECRET_";

/// Secret values enter the guest through this pair of names.
pub(crate) fn secret_env_host_var(guest_name: &str) -> String {
    format!("{SECRET_HOST_PREFIX}{guest_name}")
}

/// A guest name must survive the `--secret-env NAME=HOST_VAR` mapping
/// intact: a name with `=` would smuggle a second mapping in, and NUL
/// cannot cross an exec boundary at all. The rule lives here — like
/// its sibling guards below — beside the format it protects.
pub(crate) fn validate_env_name(name: &str) -> Result<(), SandboxError> {
    if name.is_empty() || name.contains(['=', '\0']) {
        return Err(SandboxError(format!(
            "invalid environment variable name {name:?}"
        )));
    }
    Ok(())
}

/// A host path must survive the `--volume HOST:GUEST` mapping intact:
/// `:` is the delimiter, so a path containing one would be misparsed
/// silently.
pub(crate) fn validate_mount_host(host: &Path) -> Result<(), SandboxError> {
    if host.as_os_str().as_encoded_bytes().contains(&b':') {
        return Err(SandboxError(format!(
            "workspace host path {} contains `:`, the --volume delimiter",
            host.display()
        )));
    }
    Ok(())
}

/// [`put_args`]/[`get_args`] exec without `--workdir`, so a relative
/// guest path would silently resolve against smolvm's default cwd
/// instead of the workspace.
pub(crate) fn validate_guest_path(path: &Path) -> Result<(), SandboxError> {
    if path.is_absolute() {
        return Ok(());
    }
    Err(SandboxError(format!(
        "guest path {} must be absolute",
        path.display()
    )))
}

pub(crate) fn version_args() -> Vec<String> {
    vec!["--version".into()]
}

pub(crate) fn create_args(
    name: &str,
    image: Option<&str>,
    net: bool,
    workspace: &WorkspaceMount,
) -> Vec<String> {
    let mut args = machine_command("create", name);
    if let Some(image) = image {
        args.extend(["--image".into(), image.into()]);
    }
    if net {
        args.push("--net".into());
    }
    args.extend([
        "--volume".into(),
        format!("{}:{}", workspace.host.display(), workspace.guest.display()),
    ]);
    args
}

pub(crate) fn start_args(name: &str) -> Vec<String> {
    machine_command("start", name)
}

pub(crate) fn exec_args<'a>(
    name: &str,
    command: &ExecSpec,
    workspace_guest: &Path,
    secret_names: impl IntoIterator<Item = &'a str>,
) -> Vec<String> {
    let mut args = machine_command("exec", name);
    args.push("--stream".into());
    let cwd = command.cwd.as_deref().unwrap_or(workspace_guest);
    args.extend(["--workdir".into(), cwd.display().to_string()]);
    for guest_name in secret_names {
        args.extend([
            "--secret-env".into(),
            format!("{guest_name}={}", secret_env_host_var(guest_name)),
        ]);
    }
    args.push("--".into());
    args.extend(command.argv.iter().cloned());
    args
}

/// Files go in through `tee` reading exec's stdin — not `machine cp`,
/// which writes to the storage-disk workspace that a `--volume` mount
/// shadows, so nothing put with it would be visible to commands
/// (observed on smolvm 1.6.3). `tee` takes the path as a plain
/// argument: no shell in the middle, byte-exact for binary content.
pub(crate) fn put_args(name: &str, path: &Path) -> Vec<String> {
    let mut args = machine_command("exec", name);
    args.extend([
        "--interactive".into(),
        "--".into(),
        "tee".into(),
        path.display().to_string(),
    ]);
    args
}

/// Files come out through `cat` on exec's stdout — byte-exact, and
/// mount-agnostic where `machine cp` is not (see [`put_args`]).
pub(crate) fn get_args(name: &str, path: &Path) -> Vec<String> {
    let mut args = machine_command("exec", name);
    args.extend(["--".into(), "cat".into(), path.display().to_string()]);
    args
}

/// `--force` skips the confirmation prompt; delete stops a running
/// machine itself, so destroy is one call.
pub(crate) fn delete_args(name: &str) -> Vec<String> {
    let mut args = machine_command("delete", name);
    args.push("--force".into());
    args
}

fn machine_command(subcommand: &str, name: &str) -> Vec<String> {
    vec![
        "machine".into(),
        subcommand.into(),
        "--name".into(),
        name.into(),
    ]
}

/// Extracts the version from `smolvm --version` output
/// (`smolvm 1.6.3`). `None` when the output isn't smolvm's — preflight
/// turns that into its own error rather than comparing garbage.
pub(crate) fn parse_version(output: &str) -> Option<&str> {
    let mut words = output.split_whitespace();
    match (words.next(), words.next()) {
        (Some("smolvm"), Some(version)) => Some(version),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    fn workspace() -> WorkspaceMount {
        WorkspaceMount {
            host: PathBuf::from("/srv/runs/r1/workspace"),
            guest: PathBuf::from("/workspace"),
        }
    }

    #[test]
    fn create_mounts_the_workspace_read_write() {
        assert_eq!(
            create_args("hako-1", None, false, &workspace()),
            [
                "machine",
                "create",
                "--name",
                "hako-1",
                "--volume",
                "/srv/runs/r1/workspace:/workspace",
            ]
        );
    }

    #[test]
    fn create_passes_image_and_net_only_when_asked() {
        assert_eq!(
            create_args("hako-1", Some("alpine"), true, &workspace()),
            [
                "machine",
                "create",
                "--name",
                "hako-1",
                "--image",
                "alpine",
                "--net",
                "--volume",
                "/srv/runs/r1/workspace:/workspace",
            ]
        );
    }

    #[test]
    fn exec_streams_and_defaults_the_workdir_to_the_workspace() {
        let command = ExecSpec {
            argv: vec!["claude".into(), "-p".into(), "the prompt".into()],
            cwd: None,
        };
        assert_eq!(
            exec_args("hako-1", &command, Path::new("/workspace"), []),
            [
                "machine",
                "exec",
                "--name",
                "hako-1",
                "--stream",
                "--workdir",
                "/workspace",
                "--",
                "claude",
                "-p",
                "the prompt",
            ]
        );
    }

    #[test]
    fn an_explicit_cwd_overrides_the_workspace_default() {
        let command = ExecSpec {
            argv: vec!["pwd".into()],
            cwd: Some(PathBuf::from("/workspace/subdir")),
        };
        let args = exec_args("hako-1", &command, Path::new("/workspace"), []);
        assert!(
            args.windows(2)
                .any(|w| w == ["--workdir", "/workspace/subdir"])
        );
    }

    #[test]
    fn exec_routes_secrets_through_prefixed_host_vars_not_argv() {
        let command = ExecSpec {
            argv: vec!["run".into()],
            cwd: None,
        };
        let args = exec_args(
            "hako-1",
            &command,
            Path::new("/workspace"),
            ["GH_TOKEN", "OPENAI_API_KEY"],
        );
        let pairs: Vec<&String> = args
            .windows(2)
            .filter(|w| w[0] == "--secret-env")
            .map(|w| &w[1])
            .collect();
        assert_eq!(
            pairs,
            [
                "GH_TOKEN=HAKO_SECRET_GH_TOKEN",
                "OPENAI_API_KEY=HAKO_SECRET_OPENAI_API_KEY",
            ]
        );
    }

    #[test]
    fn the_argv_is_fenced_off_from_flag_parsing() {
        let command = ExecSpec {
            argv: vec!["--stream".into()],
            cwd: None,
        };
        let args = exec_args("hako-1", &command, Path::new("/workspace"), []);
        let fence = args.iter().position(|a| a == "--").unwrap();
        assert_eq!(args[fence + 1..], ["--stream"]);
    }

    #[test]
    fn files_move_through_exec_tee_and_cat_not_machine_cp() {
        assert_eq!(
            put_args("hako-1", Path::new("/workspace/PROMPT.md")),
            [
                "machine",
                "exec",
                "--name",
                "hako-1",
                "--interactive",
                "--",
                "tee",
                "/workspace/PROMPT.md",
            ]
        );
        assert_eq!(
            get_args("hako-1", Path::new("/workspace/.hako/report.json")),
            [
                "machine",
                "exec",
                "--name",
                "hako-1",
                "--",
                "cat",
                "/workspace/.hako/report.json",
            ]
        );
    }

    #[test]
    fn delete_is_forced_so_destroy_never_blocks_on_a_prompt() {
        assert_eq!(
            delete_args("hako-1"),
            ["machine", "delete", "--name", "hako-1", "--force"]
        );
    }

    #[test]
    fn env_names_that_break_the_secret_mapping_are_invalid() {
        assert!(validate_env_name("GH_TOKEN").is_ok());
        assert!(validate_env_name("").is_err());
        assert!(validate_env_name("BAD=NAME").is_err());
        assert!(validate_env_name("BAD\0NAME").is_err());
    }

    #[test]
    fn host_paths_that_break_the_volume_mapping_are_invalid() {
        assert!(validate_mount_host(Path::new("/srv/runs/r1/workspace")).is_ok());
        assert!(validate_mount_host(Path::new("/srv/runs/r:1/workspace")).is_err());
    }

    #[test]
    fn relative_guest_paths_are_invalid_for_file_transfer() {
        assert!(validate_guest_path(Path::new("/workspace/PROMPT.md")).is_ok());
        assert!(validate_guest_path(Path::new("relative.txt")).is_err());
    }

    #[test]
    fn the_version_is_parsed_out_of_smolvm_version_output() {
        assert_eq!(parse_version("smolvm 1.6.3\n"), Some("1.6.3"));
        assert_eq!(
            parse_version("smolvm 1.6.3 (extra build info)"),
            Some("1.6.3")
        );
    }

    #[test]
    fn foreign_version_output_is_rejected_not_compared() {
        assert_eq!(parse_version(""), None);
        assert_eq!(parse_version("krunvm 0.2.6"), None);
        assert_eq!(parse_version("smolvm"), None);
    }
}
