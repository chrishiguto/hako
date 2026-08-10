//! Every environment variable `hakod` reads, read in one place.
//!
//! The daemon's configuration surface is env-only by design (ADR 0002:
//! the daemon is the deployment unit, and a systemd unit or a container
//! spec is where an operator already writes this down). One module owns
//! the reads so the answer to "what can I set on a hako host?" is one
//! file rather than a grep, and so an invalid value fails startup —
//! naming the variable — instead of surfacing as odd behaviour on the
//! first run that touches it.

use std::ffi::OsString;
use std::net::SocketAddr;
use std::path::PathBuf;

use sandbox::SmolvmConfig;

use crate::DaemonConfig;

const TOKEN: &str = "HAKO_TOKEN";
const ADDR: &str = "HAKO_ADDR";
const RUNS_DIR: &str = "HAKO_RUNS_DIR";
const SECRETS_DIR: &str = "HAKO_SECRETS_DIR";
const VM_IMAGE: &str = "HAKO_VM_IMAGE";
const VM_NET: &str = "HAKO_VM_NET";

const DEFAULT_ADDR: &str = "127.0.0.1:7878";
const DEFAULT_RUNS_ROOT: &str = ".hako/runs";
const DEFAULT_SECRETS_ROOT: &str = ".hako/secrets";

/// Everything the daemon binary needs to come up, already validated.
pub struct HostConfig {
    /// Where the HTTP listener binds.
    pub address: SocketAddr,
    pub daemon: DaemonConfig,
    /// The secret store's directory; opened — and its permissions
    /// judged — before the listener binds.
    pub secrets_root: PathBuf,
    /// What the daemon's microVMs boot and whether they reach the
    /// network. One baked image per daemon: image selection is an
    /// operator concern, so no flow key answers it (#45).
    pub sandbox: SmolvmConfig,
}

impl HostConfig {
    /// Reads the process environment.
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_vars(|name: &str| std::env::var_os(name))
    }

    /// The whole of the parsing, over a lookup rather than the process
    /// environment: env vars are global mutable state, and a test that
    /// set them would race every other test in the binary.
    fn from_vars(lookup: impl Fn(&str) -> Option<OsString>) -> Result<Self, ConfigError> {
        // Empty and absent mean the same thing: `HAKO_VM_IMAGE=` in a
        // unit file is how an operator clears a value, not how they
        // ask for an empty one.
        let text = |variable: &'static str| -> Result<Option<String>, ConfigError> {
            match lookup(variable) {
                None => Ok(None),
                Some(raw) => raw
                    .into_string()
                    .map(|value| Some(value).filter(|value| !value.is_empty()))
                    .map_err(|_| ConfigError::new(variable, "is not valid UTF-8")),
            }
        };
        let path = |variable: &str, default: &str| {
            lookup(variable)
                .filter(|raw| !raw.is_empty())
                .map_or_else(|| PathBuf::from(default), PathBuf::from)
        };

        let token = text(TOKEN)?
            .ok_or_else(|| ConfigError::new(TOKEN, "must hold the daemon's bearer token"))?;
        let address = match text(ADDR)? {
            None => DEFAULT_ADDR.parse().expect("the default address parses"),
            Some(raw) => raw.parse().map_err(|_| {
                ConfigError::new(
                    ADDR,
                    format!("must be an address and port like `{DEFAULT_ADDR}`, got `{raw}`"),
                )
            })?,
        };
        let image = text(VM_IMAGE)?;
        let net = match text(VM_NET)? {
            None => false,
            Some(raw) => flag(VM_NET, &raw)?,
        };

        Ok(Self {
            address,
            daemon: DaemonConfig::new(token, path(RUNS_DIR, DEFAULT_RUNS_ROOT)),
            secrets_root: path(SECRETS_DIR, DEFAULT_SECRETS_ROOT),
            sandbox: SmolvmConfig {
                image,
                net,
                ..SmolvmConfig::default()
            },
        })
    }
}

/// Reads a boolean the several ways an operator writes one. A value
/// that is none of them fails rather than defaulting: an unrecognized
/// `HAKO_VM_NET=enabled` silently meaning "off" is how a daemon ends up
/// unable to reach any model API with nothing in the log to say why.
fn flag(variable: &'static str, raw: &str) -> Result<bool, ConfigError> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(ConfigError::new(
            variable,
            format!("must be `1`/`0`, `true`/`false`, `yes`/`no`, or `on`/`off`, got `{raw}`"),
        )),
    }
}

/// A daemon that must not start. Always names the variable: the
/// operator's next action is on that line of their unit file.
#[derive(Debug, thiserror::Error)]
#[error("{variable} {problem}")]
pub struct ConfigError {
    variable: &'static str,
    problem: String,
}

impl ConfigError {
    fn new(variable: &'static str, problem: impl Into<String>) -> Self {
        Self {
            variable,
            problem: problem.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    /// A host environment holding exactly these variables.
    fn env(vars: &[(&str, &str)]) -> impl Fn(&str) -> Option<OsString> + use<> {
        let vars: BTreeMap<String, OsString> = vars
            .iter()
            .map(|(name, value)| ((*name).to_owned(), OsString::from(*value)))
            .collect();
        move |name| vars.get(name).cloned()
    }

    fn config(vars: &[(&str, &str)]) -> HostConfig {
        HostConfig::from_vars(env(vars)).unwrap()
    }

    fn error(vars: &[(&str, &str)]) -> String {
        HostConfig::from_vars(env(vars)).err().unwrap().to_string()
    }

    /// The one required variable, and what every optional one falls
    /// back to — the defaults are the documented behaviour of a bare
    /// `HAKO_TOKEN=… hakod`, so they are pinned here.
    #[test]
    fn a_token_alone_is_a_complete_configuration() {
        let config = config(&[(TOKEN, "t0ken")]);

        assert_eq!(config.address.to_string(), DEFAULT_ADDR);
        assert_eq!(config.secrets_root, PathBuf::from(DEFAULT_SECRETS_ROOT));
        // No image and no network: today's bare rootfs, unchanged.
        assert_eq!(config.sandbox.image, None);
        assert!(!config.sandbox.net);
    }

    #[test]
    fn every_variable_lands_where_it_is_read() {
        let config = config(&[
            (TOKEN, "t0ken"),
            (ADDR, "0.0.0.0:9000"),
            (RUNS_DIR, "/srv/hako/runs"),
            (SECRETS_DIR, "/srv/hako/secrets"),
            (VM_IMAGE, "ghcr.io/chrishiguto/hako-agent:2026-08"),
            (VM_NET, "1"),
        ]);

        assert_eq!(config.address.to_string(), "0.0.0.0:9000");
        assert_eq!(config.secrets_root, PathBuf::from("/srv/hako/secrets"));
        assert_eq!(
            config.sandbox.image.as_deref(),
            Some("ghcr.io/chrishiguto/hako-agent:2026-08")
        );
        assert!(config.sandbox.net);
    }

    /// The daemon has no business starting without a token, and an
    /// empty one is the same gap wearing a different hat — an
    /// unauthenticated daemon is worse than one that refused to start.
    #[test]
    fn a_missing_or_empty_token_stops_startup() {
        for vars in [vec![], vec![(TOKEN, "")]] {
            let message = error(&vars);
            assert!(message.contains(TOKEN), "{message}");
        }
    }

    /// Every rejection names its variable and quotes what it got, so
    /// the fix is the message.
    #[test]
    fn an_invalid_value_names_its_variable_and_what_it_read() {
        for (variable, value) in [(ADDR, "not-an-address"), (VM_NET, "enabled")] {
            let message = error(&[(TOKEN, "t0ken"), (variable, value)]);
            assert!(message.contains(variable), "{message}");
            assert!(message.contains(value), "{message}");
        }
    }

    /// Operators write booleans several ways; all of them are read,
    /// and case is not one of the ways to get it wrong.
    #[test]
    fn the_network_flag_reads_the_usual_spellings() {
        for on in ["1", "true", "TRUE", "yes", "on", " true "] {
            assert!(
                config(&[(TOKEN, "t0ken"), (VM_NET, on)]).sandbox.net,
                "{on}"
            );
        }
        for off in ["0", "false", "No", "off"] {
            assert!(
                !config(&[(TOKEN, "t0ken"), (VM_NET, off)]).sandbox.net,
                "{off}"
            );
        }
    }

    /// `HAKO_VM_IMAGE=` in a unit file is how an operator unsets an
    /// image, not how they ask for one named "" — and the same reading
    /// applies to every optional variable, the validated ones included.
    #[test]
    fn an_empty_optional_value_reads_as_unset() {
        let config = config(&[
            (TOKEN, "t0ken"),
            (ADDR, ""),
            (VM_IMAGE, ""),
            (VM_NET, ""),
            (SECRETS_DIR, ""),
            (RUNS_DIR, ""),
        ]);
        assert_eq!(config.address.to_string(), DEFAULT_ADDR);
        assert_eq!(config.sandbox.image, None);
        assert!(!config.sandbox.net);
        assert_eq!(config.secrets_root, PathBuf::from(DEFAULT_SECRETS_ROOT));
    }

    #[cfg(unix)]
    #[test]
    fn a_value_that_is_not_utf8_names_its_variable() {
        use std::os::unix::ffi::OsStringExt;

        let vars = BTreeMap::from([(TOKEN.to_owned(), OsString::from_vec(vec![0xff, 0xfe]))]);
        let error = HostConfig::from_vars(|name| vars.get(name).cloned())
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains(TOKEN), "{error}");
        assert!(error.contains("UTF-8"), "{error}");
    }
}
