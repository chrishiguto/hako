use std::io;
use std::net::SocketAddr;
use std::process::{Command, ExitStatus, Stdio};
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
        // Null, never piped: a pipe's read end dies with this
        // process, and the detached daemon writing to it afterwards
        // would take a SIGPIPE.
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    let mut child = command.spawn().map_err(StartError::Spawn)?;

    let deadline = Instant::now() + STARTUP_TIMEOUT;
    loop {
        match client.list() {
            Ok(_) => return Ok(()),
            Err(error) => {
                // A child that already exited will never become ready
                // — report its exit now, not connection refusals for
                // the rest of the timeout.
                if let Ok(Some(status)) = child.try_wait() {
                    return Err(StartError::Exited(status));
                }
                if Instant::now() >= deadline {
                    return Err(StartError::NotReady(error.to_string()));
                }
                std::thread::sleep(RETRY_DELAY);
            }
        }
    }
}

#[derive(Debug)]
pub(crate) enum StartError {
    Spawn(io::Error),
    Exited(ExitStatus),
    NotReady(String),
}

impl std::fmt::Display for StartError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Spawn(error) => write!(formatter, "could not start `hakod`: {error}"),
            Self::Exited(status) => {
                write!(formatter, "`hakod` exited before becoming ready ({status})")
            }
            Self::NotReady(error) => write!(formatter, "`hakod` did not become ready: {error}"),
        }
    }
}

impl std::error::Error for StartError {}
