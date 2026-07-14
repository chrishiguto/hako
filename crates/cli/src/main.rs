//! `hako` — a pure client of the daemon; among workspace crates it may
//! depend on `api` only (ADR 0006). Flow validation is offline by
//! design: the committed flow schema is embedded at build time, so
//! `hako validate` needs neither a daemon nor an engine link.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, Subcommand};

// The crate's one allowed workspace edge, declared ahead of the client
// code that will use it.
use api as _;

/// Generated from the engine's flow types by `cargo xtask schema`;
/// `just check` fails if it drifts from them.
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
    /// Validate a flow file against the flow schema — offline, no
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
            Err(errors) => {
                for error in errors {
                    eprintln!("{error}");
                }
                ExitCode::FAILURE
            }
        },
        Command::Schema => {
            print!("{FLOW_SCHEMA}");
            ExitCode::SUCCESS
        }
    }
}

/// Every schema violation at once, so an author fixes a flow in one
/// round instead of one error per run.
fn validate(path: &Path) -> Result<(), Vec<String>> {
    let display = path.display();
    let source = fs::read_to_string(path).map_err(|error| vec![format!("{display}: {error}")])?;
    let flow: toml::Value =
        toml::from_str(&source).map_err(|error| vec![format!("{display}: {error}")])?;
    let flow = to_json(flow, "").map_err(|error| vec![format!("{display}: {error}")])?;
    let errors: Vec<String> = flow_validator()
        .iter_errors(&flow)
        .map(|error| {
            let pointer = error.instance_path().to_string();
            format!("{display}: {}: {error}", location(&pointer))
        })
        .collect();
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

/// The JSON image of a flow's TOML. Hand-rolled for the two values the
/// default conversion mangles into misleading schema errors: a
/// non-finite float would turn into JSON `null` and slip through
/// `Option` fields, and a datetime would leak the toml crate's private
/// serde marker. Both fail here instead, naming the offending key.
fn to_json(value: toml::Value, path: &str) -> Result<serde_json::Value, String> {
    Ok(match value {
        toml::Value::String(text) => serde_json::Value::String(text),
        toml::Value::Integer(number) => number.into(),
        toml::Value::Float(number) => serde_json::Number::from_f64(number)
            .ok_or_else(|| format!("{}: `{number}` is not a finite number", location(path)))?
            .into(),
        toml::Value::Boolean(flag) => serde_json::Value::Bool(flag),
        toml::Value::Datetime(datetime) => {
            return Err(format!(
                "{}: dates and times like `{datetime}` are not flow values",
                location(path)
            ));
        }
        toml::Value::Array(items) => serde_json::Value::Array(
            items
                .into_iter()
                .enumerate()
                .map(|(index, item)| to_json(item, &format!("{path}/{index}")))
                .collect::<Result<_, _>>()?,
        ),
        toml::Value::Table(entries) => serde_json::Value::Object(
            entries
                .into_iter()
                .map(|(key, entry)| {
                    let entry = to_json(entry, &format!("{path}/{key}"))?;
                    Ok((key, entry))
                })
                .collect::<Result<_, String>>()?,
        ),
    })
}

fn flow_validator() -> jsonschema::Validator {
    let schema = serde_json::from_str(FLOW_SCHEMA).expect("committed flow schema is JSON");
    jsonschema::validator_for(&schema).expect("committed flow schema is a valid JSON Schema")
}

/// Where in the flow a violation sits as a JSON pointer, e.g.
/// `/budget` — the schema analogue of the engine's line-and-key TOML
/// errors.
fn location(pointer: &str) -> &str {
    if pointer.is_empty() {
        "(root)"
    } else {
        pointer
    }
}
