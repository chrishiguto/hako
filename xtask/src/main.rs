//! Workspace automation (the rust-analyzer xtask pattern), exposed as
//! `cargo xtask <task>` via the alias in `.cargo/config.toml`. A dev tool,
//! never shipped — exempt from the product dependency rules it enforces.

use std::env;
use std::process;

mod deps;
mod metadata;
mod schema;

fn main() -> anyhow::Result<()> {
    let args: Vec<String> = env::args().skip(1).collect();
    match args.as_slice() {
        [task] if task == "deps" => deps::run(),
        [task] if task == "schema" => schema::run(false),
        [task, flag] if task == "schema" && flag == "--check" => schema::run(true),
        _ => {
            eprintln!("usage: cargo xtask <task>");
            eprintln!();
            eprintln!("tasks:");
            eprintln!("  deps              check the workspace dependency rules (ADR 0006)");
            eprintln!(
                "  schema [--check]  sync the committed schemas/ artifacts (or fail on drift)"
            );
            process::exit(2);
        }
    }
}
