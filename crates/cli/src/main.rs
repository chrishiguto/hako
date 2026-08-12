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
    match command {
        Command::Run { flow } => {
            let flow = match read_flow(&flow) {
                Ok(flow) => flow,
                Err(error) => {
                    eprintln!("{}: {error}", flow.display());
                    return ExitCode::from(VALIDATION_FAILURE);
                }
            };
            let connection = match config::connection(address, token) {
                Ok(connection) => connection,
                Err(error) => {
                    eprintln!("submit failed: {error}");
                    return ExitCode::from(DAEMON_FAILURE);
                }
            };
            let client = client::Client::new(&connection.address, &connection.token);
            let submitted = match client.submit(&flow) {
                Err(error) if error.is_transport() => {
                    let Some(bind) = connection.local_bind else {
                        eprintln!("submit failed: {error}");
                        return ExitCode::from(DAEMON_FAILURE);
                    };
                    if let Err(error) = daemon::start(bind, &connection.token, &client) {
                        eprintln!("submit failed: {error}");
                        return ExitCode::from(DAEMON_FAILURE);
                    }
                    client.submit(&flow)
                }
                result => result,
            };
            match submitted {
                Ok(submitted) => {
                    println!("{}", submitted.run_id);
                    ExitCode::SUCCESS
                }
                Err(error) => {
                    eprintln!("submit failed: {error}");
                    ExitCode::from(DAEMON_FAILURE)
                }
            }
        }
        Command::List => {
            let connection = match config::connection(address, token) {
                Ok(connection) => connection,
                Err(error) => {
                    eprintln!("list failed: {error}");
                    return ExitCode::from(DAEMON_FAILURE);
                }
            };
            match client::Client::new(&connection.address, &connection.token).list() {
                Ok(list) => {
                    print_runs(list);
                    ExitCode::SUCCESS
                }
                Err(error) => {
                    eprintln!("list failed: {error}");
                    ExitCode::from(DAEMON_FAILURE)
                }
            }
        }
        Command::Validate { flow } => match validate(&flow) {
            Ok(()) => {
                println!("{}: valid flow", flow.display());
                ExitCode::SUCCESS
            }
            Err(error) => {
                eprintln!("{}: {error}", flow.display());
                ExitCode::from(VALIDATION_FAILURE)
            }
        },
        Command::Schema => {
            print!("{FLOW_SCHEMA}");
            ExitCode::SUCCESS
        }
    }
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
