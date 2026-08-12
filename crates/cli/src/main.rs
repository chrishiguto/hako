//! `hako` — a pure client of the daemon; among workspace crates it may
//! depend on `api` and `proto` only. Flow validation is offline by
//! design and runs the daemon's own parser — the shared `proto::flow`
//! types — so a flow the CLI blesses is a flow the daemon accepts, and
//! the errors match down to the line.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use proto::flow::FlowConfig;

use api::{ListRunsResponse, PauseReason, RunListEntry, RunState};

mod client;
mod config;
mod daemon;

const VALIDATION_FAILURE: u8 = 2;
const DAEMON_FAILURE: u8 = 3;

/// Generated from proto's flow types by `cargo xtask schema`;
/// `just check` fails if it drifts from them. Embedded only to be
/// printed — validation goes through the types themselves.
const FLOW_SCHEMA: &str = include_str!("../../../schemas/flow.schema.json");

#[derive(Parser)]
#[command(
    name = "hako",
    version,
    about = "Run agent loops in ephemeral microVMs"
)]
struct Cli {
    /// Daemon URL or address.
    #[arg(long, global = true)]
    address: Option<String>,
    /// Daemon bearer token.
    #[arg(long, global = true)]
    token: Option<String>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Submit a flow to the daemon and return immediately.
    Run {
        /// Path to a flow TOML file.
        flow: PathBuf,
    },
    /// List every run and its current state.
    List,
    /// Validate a flow file with the daemon's parser — offline, no
    /// daemon needed.
    Validate {
        /// Path to a flow TOML file.
        flow: PathBuf,
    },
    /// Print the flow JSON Schema, for editors and LLMs authoring
    /// flows.
    Schema,
}

fn main() -> ExitCode {
    let Cli {
        address,
        token,
        command,
    } = Cli::parse();
    match dispatch(command, address, token) {
        Ok(()) => ExitCode::SUCCESS,
        Err(Failure::Validation(message)) => {
            eprintln!("{message}");
            ExitCode::from(VALIDATION_FAILURE)
        }
        Err(Failure::Daemon(message)) => {
            eprintln!("{message}");
            ExitCode::from(DAEMON_FAILURE)
        }
    }
}

/// Why the process exits nonzero, carrying its stderr line. The two
/// variants are the two published exit codes: a flow the parser
/// refused, and a daemon that could not be reached or refused us.
enum Failure {
    Validation(String),
    Daemon(String),
}

impl Failure {
    fn daemon(operation: &str, error: impl std::fmt::Display) -> Self {
        Self::Daemon(format!("{operation} failed: {error}"))
    }
}

fn dispatch(
    command: Command,
    address: Option<String>,
    token: Option<String>,
) -> Result<(), Failure> {
    match command {
        Command::Run { flow } => run(&flow, address, token),
        Command::List => list(address, token),
        Command::Validate { flow } => {
            validate(&flow)
                .map_err(|error| Failure::Validation(format!("{}: {error}", flow.display())))?;
            println!("{}: valid flow", flow.display());
            Ok(())
        }
        Command::Schema => {
            print!("{FLOW_SCHEMA}");
            Ok(())
        }
    }
}

/// Submit and return immediately. When the daemon is local and simply
/// not up, start it and submit once more — one retry, only on a
/// transport failure, only for an address we may bind.
fn run(path: &Path, address: Option<String>, token: Option<String>) -> Result<(), Failure> {
    let flow = read_flow(path)
        .map_err(|error| Failure::Validation(format!("{}: {error}", path.display())))?;
    let connection =
        config::connection(address, token).map_err(|error| Failure::daemon("submit", error))?;
    let client = client::Client::new(&connection.address, &connection.token);
    let submitted = match client.submit(&flow) {
        Err(error) if error.is_transport() => {
            let bind = connection
                .local_bind
                .ok_or_else(|| Failure::daemon("submit", &error))?;
            daemon::start(bind, &connection.token, &client)
                .map_err(|error| Failure::daemon("submit", error))?;
            client.submit(&flow)
        }
        result => result,
    }
    .map_err(|error| Failure::daemon("submit", error))?;
    println!("{}", submitted.run_id);
    Ok(())
}

fn list(address: Option<String>, token: Option<String>) -> Result<(), Failure> {
    let connection =
        config::connection(address, token).map_err(|error| Failure::daemon("list", error))?;
    let list = client::Client::new(&connection.address, &connection.token)
        .list()
        .map_err(|error| Failure::daemon("list", error))?;
    print_runs(list);
    Ok(())
}

fn print_runs(list: ListRunsResponse) {
    println!("RUN ID\tSTATE\tKERNEL\tAGENT\tUPDATED");
    for entry in list.runs {
        match entry {
            RunListEntry::Run(run) => println!(
                "{}\t{}\t{}\t{}\t{}",
                run.run_id,
                state(run.state),
                run.kernel,
                run.agent,
                run.updated_at
            ),
            RunListEntry::Unreadable(run) => println!(
                "{}\tunreadable ({})\t{}\t{}\t-",
                run.run_id, run.reason, run.kernel, run.agent
            ),
        }
    }
}

fn state(state: RunState) -> String {
    match state {
        RunState::Running => "running".into(),
        RunState::Paused { reason } => format!("paused ({})", pause_reason(reason)),
        RunState::Done => "done".into(),
        RunState::Failed => "failed".into(),
        RunState::Cancelled => "cancelled".into(),
    }
}

fn pause_reason(reason: PauseReason) -> &'static str {
    match reason {
        PauseReason::Blocked => "blocked",
        PauseReason::VerifyFailed => "verify_failed",
        PauseReason::Drift => "drift",
        PauseReason::Budget => "budget",
        PauseReason::AwaitingHuman => "awaiting_human",
    }
}

/// Strict parse with the shared flow types: the daemon's verdict and
/// the daemon's error text — offending line, caret, and suggestion.
/// Fails at the first error, exactly as the daemon would at submit.
fn validate(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    read_flow(path)?;
    Ok(())
}

fn read_flow(path: &Path) -> Result<String, Box<dyn std::error::Error>> {
    let flow = fs::read_to_string(path)?;
    FlowConfig::from_toml(&flow)?;
    Ok(flow)
}
