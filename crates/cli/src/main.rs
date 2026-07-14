//! `hako` — a pure client of the daemon; among workspace crates it may
//! depend on `api` and `proto` only (ADR 0006). Flow validation is
//! offline by design and runs the daemon's own parser — the shared
//! `proto::flow` types (ADR 0009) — so a flow the CLI blesses is a
//! flow the daemon accepts, and the errors match down to the line.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use proto::flow::FlowConfig;

// The crate's other allowed workspace edge, declared ahead of the
// client code that will use it.
use api as _;

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
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
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
    match Cli::parse().command {
        Command::Validate { flow } => match validate(&flow) {
            Ok(()) => {
                println!("{}: valid flow", flow.display());
                ExitCode::SUCCESS
            }
            Err(error) => {
                eprintln!("{error}");
                ExitCode::FAILURE
            }
        },
        Command::Schema => {
            print!("{FLOW_SCHEMA}");
            ExitCode::SUCCESS
        }
    }
}

/// Strict parse with the shared flow types: the daemon's verdict and
/// the daemon's error text — offending line, caret, and suggestion.
/// Fails at the first error, exactly as the daemon would at submit.
fn validate(path: &Path) -> Result<(), String> {
    let display = path.display();
    let source = fs::read_to_string(path).map_err(|error| format!("{display}: {error}"))?;
    FlowConfig::from_toml(&source).map_err(|error| format!("{display}: {error}"))?;
    Ok(())
}
