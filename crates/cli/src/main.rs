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

use api::{ListRunsResponse, RunListEntry, RunState};

mod attach;
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
    /// Replay a run's events, then follow it until it finishes.
    Attach {
        /// Run to observe.
        run: String,
    },
    /// Show a paused run's questions and answer one of them.
    Answer {
        /// Run awaiting human input.
        run: String,
        /// Question identifier shown by the run.
        question: String,
        /// Answer text to record.
        answer: String,
    },
    /// Resume a paused run.
    Resume {
        /// Run to resume.
        run: String,
        /// Guidance for the next iteration.
        #[arg(long)]
        note: Option<String>,
    },
    /// Cancel a run cleanly.
    Cancel {
        /// Run to cancel.
        run: String,
    },
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

/// The two variants are the two published exit codes; each carries the
/// stderr line the process exits with.
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
        Command::Attach { run } => attach(&run, address, token),
        Command::Answer {
            run,
            question,
            answer: answer_text,
        } => answer(&run, &question, &answer_text, address, token),
        Command::Resume { run, note } => resume(&run, note, address, token),
        Command::Cancel { run } => cancel(&run, address, token),
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

fn cancel(run_id: &str, address: Option<String>, token: Option<String>) -> Result<(), Failure> {
    configured_client("cancel", address, token)?
        .cancel(run_id)
        .map_err(|error| Failure::daemon("cancel", error))?;
    println!("cancelled {run_id}");
    Ok(())
}

fn resume(
    run_id: &str,
    note: Option<String>,
    address: Option<String>,
    token: Option<String>,
) -> Result<(), Failure> {
    configured_client("resume", address, token)?
        .resume(run_id, note)
        .map_err(|error| Failure::daemon("resume", error))?;
    println!("resumed {run_id}");
    Ok(())
}

fn answer(
    run_id: &str,
    question_id: &str,
    answer: &str,
    address: Option<String>,
    token: Option<String>,
) -> Result<(), Failure> {
    let client = configured_client("answer", address, token)?;
    let status = client
        .status(run_id)
        .map_err(|error| Failure::daemon("answer", error))?;
    for question in status.pending_questions {
        println!("{}: {}", question.id, question.text);
        if !question.options.is_empty() {
            println!("  options: {}", question.options.join(", "));
        }
    }
    client
        .answer(run_id, question_id, answer)
        .map_err(|error| Failure::daemon("answer", error))?;
    println!("answered {question_id}");
    Ok(())
}

fn attach(run_id: &str, address: Option<String>, token: Option<String>) -> Result<(), Failure> {
    let client = configured_client("attach", address, token)?;
    attach::attach(&client, run_id, &mut std::io::stdout())
        .map_err(|error| Failure::daemon("attach", error))
}

fn configured_client(
    operation: &str,
    address: Option<String>,
    token: Option<String>,
) -> Result<client::Client, Failure> {
    let connection =
        config::connection(address, token).map_err(|error| Failure::daemon(operation, error))?;
    Ok(client::Client::new(&connection.address, &connection.token))
}

/// A local daemon that is simply not up is worth starting; anything
/// else — a rejection, a remote address — is the user's answer. One
/// retry only: a second transport failure means starting it did not
/// help, and looping would stall the command instead of reporting.
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
    let list = configured_client("list", address, token)?
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
        RunState::Paused { reason } => format!("paused ({})", reason.as_str()),
        RunState::Done => "done".into(),
        RunState::Failed => "failed".into(),
        RunState::Cancelled => "cancelled".into(),
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
