use std::ffi::OsString;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::PathBuf;

const DEFAULT_ADDRESS: &str = "http://127.0.0.1:7878";

pub(crate) struct Connection {
    pub(crate) address: String,
    pub(crate) token: String,
    pub(crate) local_bind: Option<SocketAddr>,
}

pub(crate) fn connection(
    address_flag: Option<String>,
    token_flag: Option<String>,
) -> Result<Connection, ConfigError> {
    let address = value(address_flag, "HAKO_ADDR").unwrap_or_else(|| DEFAULT_ADDRESS.to_owned());
    let local_bind = local_bind_address(&address);
    let token = match value(token_flag, "HAKO_TOKEN") {
        Some(token) if !token.is_empty() => token,
        Some(_) => return Err(ConfigError("daemon bearer token cannot be empty".into())),
        None if local_bind.is_some() => local_token()?,
        None => {
            return Err(ConfigError(
                "a remote daemon requires `--token` or `HAKO_TOKEN`".into(),
            ));
        }
    };
    Ok(Connection {
        address,
        token,
        local_bind,
    })
}

fn value(flag: Option<String>, environment: &str) -> Option<String> {
    flag.or_else(|| std::env::var(environment).ok())
}

fn local_bind_address(address: &str) -> Option<SocketAddr> {
    let authority = address
        .trim_end_matches('/')
        .strip_prefix("http://")
        .unwrap_or(address);
    if authority.contains('/') || address.starts_with("https://") {
        return None;
    }
    authority
        .to_socket_addrs()
        .ok()?
        .find(|address| address.ip().is_loopback())
}

fn local_token() -> Result<String, ConfigError> {
    let path = token_path()?;
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

fn token_path() -> Result<PathBuf, ConfigError> {
    let root = nonempty_os(std::env::var_os("XDG_CONFIG_HOME"))
        .map(PathBuf::from)
        .or_else(|| {
            nonempty_os(std::env::var_os("HOME"))
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
