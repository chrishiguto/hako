use std::io;
use std::net::SocketAddr;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use crate::client::Client;

const STARTUP_TIMEOUT: Duration = Duration::from_secs(5);
const RETRY_DELAY: Duration = Duration::from_millis(25);

pub(crate) fn start(bind: SocketAddr, token: &str, client: &Client) -> Result<(), StartError> {
    let mut command = Command::new("hakod");
    command
        .env("HAKO_ADDR", bind.to_string())
        .env("HAKO_TOKEN", token)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    command.spawn().map_err(StartError::Spawn)?;

    let deadline = Instant::now() + STARTUP_TIMEOUT;
    loop {
        match client.list() {
            Ok(_) => return Ok(()),
            Err(_) if Instant::now() < deadline => std::thread::sleep(RETRY_DELAY),
            Err(error) => return Err(StartError::NotReady(error.to_string())),
        }
    }
}

#[derive(Debug)]
pub(crate) enum StartError {
    Spawn(io::Error),
    NotReady(String),
}

impl std::fmt::Display for StartError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Spawn(error) => write!(formatter, "could not start `hakod`: {error}"),
            Self::NotReady(error) => write!(formatter, "`hakod` did not become ready: {error}"),
        }
    }
}

impl std::error::Error for StartError {}
