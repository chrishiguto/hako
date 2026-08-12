use std::ffi::OsString;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::net::SocketAddr;
use std::path::PathBuf;

const DEFAULT_ADDRESS: &str = "http://127.0.0.1:7878";

pub(crate) struct Connection {
    /// Canonical base URL — scheme-prefixed, trailing slash trimmed.
    /// The address grammar is decided here and nowhere else; `Client`
    /// consumes this verbatim.
    pub(crate) address: String,
    pub(crate) token: String,
    /// Where an auto-started daemon would bind; `None` means remote —
    /// never auto-start.
    pub(crate) local_bind: Option<SocketAddr>,
}

pub(crate) fn connection(
    address_flag: Option<String>,
    token_flag: Option<String>,
) -> Result<Connection, ConfigError> {
    resolve(address_flag, token_flag, |name| std::env::var_os(name))
}

/// The whole of the resolution, over a lookup rather than the process
/// environment: env vars are global mutable state, and a test that
/// set them would race every other test in the binary.
fn resolve(
    address_flag: Option<String>,
    token_flag: Option<String>,
    lookup: impl Fn(&str) -> Option<OsString>,
) -> Result<Connection, ConfigError> {
    let address = match address_flag {
        Some(address) => address,
        None => env_text(&lookup, "HAKO_ADDR")?.unwrap_or_else(|| DEFAULT_ADDRESS.to_owned()),
    };
    let address = normalize(&address);
    let local_bind = local_bind_address(&address);
    let token = match token_flag {
        // An empty value typed at the prompt is a mistake to report,
        // not a variable to clear.
        Some(token) if token.is_empty() => {
            return Err(ConfigError("daemon bearer token cannot be empty".into()));
        }
        Some(token) => token,
        None => match env_text(&lookup, "HAKO_TOKEN")? {
            Some(token) => token,
            None if local_bind.is_some() => local_token(&lookup)?,
            None => {
                return Err(ConfigError(
                    "a remote daemon requires `--token` or `HAKO_TOKEN`".into(),
                ));
            }
        },
    };
    Ok(Connection {
        address,
        token,
        local_bind,
    })
}

/// Empty and absent mean the same thing, exactly as they do for
/// `hakod`: `HAKO_ADDR=` in a shell profile clears the variable, it
/// does not name an empty daemon. A value that is not UTF-8 fails
/// loudly, naming the variable — silently ignoring what the user set
/// is how a command talks to the wrong daemon.
fn env_text(
    lookup: &impl Fn(&str) -> Option<OsString>,
    variable: &str,
) -> Result<Option<String>, ConfigError> {
    match lookup(variable) {
        None => Ok(None),
        Some(raw) => raw
            .into_string()
            .map(|value| Some(value).filter(|value| !value.is_empty()))
            .map_err(|_| ConfigError(format!("{variable} is not valid UTF-8"))),
    }
}

fn normalize(address: &str) -> String {
    let address = address.trim_end_matches('/');
    if address.starts_with("http://") || address.starts_with("https://") {
        address.to_owned()
    } else {
        format!("http://{address}")
    }
}

/// Localness is read from the literal text — a loopback IP or
/// `localhost` — never DNS: a lookup would block every command just
/// to decide whether spawning is allowed, and a run of `hako` should
/// not spawn daemons on the say-so of a resolver.
fn local_bind_address(address: &str) -> Option<SocketAddr> {
    // An https or path-bearing URL is a remote deployment's shape,
    // whatever host it names.
    let authority = address.strip_prefix("http://")?;
    if authority.contains('/') {
        return None;
    }
    if let Some(port) = authority.strip_prefix("localhost:") {
        return Some(SocketAddr::from((
            [127, 0, 0, 1],
            port.parse::<u16>().ok()?,
        )));
    }
    let candidate: SocketAddr = authority.parse().ok()?;
    candidate.ip().is_loopback().then_some(candidate)
}

fn local_token(lookup: &impl Fn(&str) -> Option<OsString>) -> Result<String, ConfigError> {
    let path = token_path(lookup)?;
    match fs::read_to_string(&path) {
        Ok(token) => return nonempty(token),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(io_error(&path, error)),
    }

    let parent = path.parent().expect("the token path has a parent");
    create_private_dir(parent)?;
    let token = uuid::Uuid::new_v4().simple().to_string();
    match private_new_file(&path) {
        Ok(mut file) => {
            file.write_all(token.as_bytes())
                .map_err(|error| io_error(&path, error))?;
            Ok(token)
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => fs::read_to_string(&path)
            .map_err(|error| io_error(&path, error))
            .and_then(nonempty),
        Err(error) => Err(io_error(&path, error)),
    }
}

fn token_path(lookup: &impl Fn(&str) -> Option<OsString>) -> Result<PathBuf, ConfigError> {
    let root = nonempty_os(lookup("XDG_CONFIG_HOME"))
        .map(PathBuf::from)
        .or_else(|| {
            nonempty_os(lookup("HOME"))
                .map(PathBuf::from)
                .map(|home| home.join(".config"))
        })
        .ok_or_else(|| {
            ConfigError("cannot locate a user config directory for local auth".into())
        })?;
    Ok(root.join("hako/token"))
}

fn nonempty_os(value: Option<OsString>) -> Option<OsString> {
    value.filter(|value| !value.is_empty())
}

fn nonempty(token: String) -> Result<String, ConfigError> {
    let token = token.trim().to_owned();
    if token.is_empty() {
        Err(ConfigError("the local daemon token file is empty".into()))
    } else {
        Ok(token)
    }
}

#[cfg(unix)]
fn create_private_dir(path: &std::path::Path) -> Result<(), ConfigError> {
    use std::os::unix::fs::DirBuilderExt;

    let mut builder = fs::DirBuilder::new();
    builder.recursive(true).mode(0o700);
    builder.create(path).map_err(|error| io_error(path, error))
}

#[cfg(not(unix))]
fn create_private_dir(path: &std::path::Path) -> Result<(), ConfigError> {
    fs::create_dir_all(path).map_err(|error| io_error(path, error))
}

#[cfg(unix)]
fn private_new_file(path: &std::path::Path) -> io::Result<fs::File> {
    use std::os::unix::fs::OpenOptionsExt;

    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
}

#[cfg(not(unix))]
fn private_new_file(path: &std::path::Path) -> io::Result<fs::File> {
    OpenOptions::new().write(true).create_new(true).open(path)
}

fn io_error(path: &std::path::Path, error: io::Error) -> ConfigError {
    ConfigError(format!("{}: {error}", path.display()))
}

#[derive(Debug)]
pub(crate) struct ConfigError(String);

impl std::fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for ConfigError {}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    /// An environment holding exactly these variables.
    fn env(vars: &[(&str, &str)]) -> impl Fn(&str) -> Option<OsString> + use<> {
        let vars: BTreeMap<String, OsString> = vars
            .iter()
            .map(|(name, value)| ((*name).to_owned(), OsString::from(*value)))
            .collect();
        move |name| vars.get(name).cloned()
    }

    fn bind(address: &str) -> Option<SocketAddr> {
        local_bind_address(&normalize(address))
    }

    /// One grammar: scheme and trailing slash are spelling, not
    /// meaning — every spelling of the loopback address is local.
    #[test]
    fn every_spelling_of_a_loopback_address_is_local() {
        let expected: SocketAddr = "127.0.0.1:7878".parse().unwrap();
        for form in [
            "127.0.0.1:7878",
            "127.0.0.1:7878/",
            "http://127.0.0.1:7878",
            "http://127.0.0.1:7878/",
        ] {
            assert_eq!(bind(form), Some(expected), "{form}");
        }
    }

    #[test]
    fn localhost_and_ipv6_loopback_are_local() {
        assert_eq!(
            bind("localhost:7878"),
            Some("127.0.0.1:7878".parse().unwrap())
        );
        assert_eq!(
            bind("http://[::1]:7878"),
            Some("[::1]:7878".parse().unwrap())
        );
    }

    /// A hostname is never local — localness comes from the literal
    /// text, not from what a resolver says about it.
    #[test]
    fn remote_shapes_never_get_a_bind_address() {
        for form in [
            "https://127.0.0.1:7878",
            "192.0.2.7:7878",
            "daemon.internal:7878",
            "http://127.0.0.1:7878/api",
        ] {
            assert_eq!(bind(form), None, "{form}");
        }
    }

    /// `HAKO_ADDR=` clears the variable — the default address applies,
    /// exactly as it does for `hakod`.
    #[test]
    fn an_empty_environment_address_reads_as_unset() {
        let connection = resolve(None, Some("t0ken".into()), env(&[("HAKO_ADDR", "")])).unwrap();
        assert_eq!(connection.address, DEFAULT_ADDRESS);
        assert!(connection.local_bind.is_some());
    }

    /// `HAKO_TOKEN=` clears the variable — a local daemon falls back
    /// to the generated token, which is persisted for the daemon side.
    #[test]
    fn an_empty_environment_token_falls_back_to_the_local_token() {
        let config = tempfile::tempdir().unwrap();
        let lookup = env(&[
            ("HAKO_TOKEN", ""),
            ("XDG_CONFIG_HOME", config.path().to_str().unwrap()),
        ]);

        let connection = resolve(None, None, &lookup).unwrap();

        assert_eq!(
            connection.token,
            fs::read_to_string(config.path().join("hako/token")).unwrap()
        );
        let again = resolve(None, None, &lookup).unwrap();
        assert_eq!(again.token, connection.token);
    }

    /// An empty value typed at the prompt is intent gone wrong, not a
    /// variable being cleared.
    #[test]
    fn an_explicitly_empty_token_flag_is_an_error() {
        let error = resolve(None, Some(String::new()), env(&[])).err().unwrap();
        assert!(error.to_string().contains("empty"), "{error}");
    }

    #[test]
    fn a_remote_daemon_without_a_token_is_refused() {
        let error = resolve(Some("https://hako.example.com:7878".into()), None, env(&[]))
            .err()
            .unwrap();
        assert!(error.to_string().contains("--token"), "{error}");
    }

    #[cfg(unix)]
    #[test]
    fn a_garbled_environment_value_names_its_variable() {
        use std::os::unix::ffi::OsStringExt;

        let vars = BTreeMap::from([("HAKO_TOKEN".to_owned(), OsString::from_vec(vec![0xff]))]);
        let error = resolve(None, None, |name: &str| vars.get(name).cloned())
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains("HAKO_TOKEN"), "{error}");
        assert!(error.contains("UTF-8"), "{error}");
    }
}
