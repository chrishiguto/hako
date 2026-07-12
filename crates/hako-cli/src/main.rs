//! `hako` — a pure client of the daemon's wire contract (see ADR 0002);
//! among workspace crates it may depend on `hako-api` only. Stub: reports
//! a version, nothing more.

// Declared edge from ADR 0006; unused until the client lands.
use hako_api as _;

fn main() {
    println!("hako {}", env!("CARGO_PKG_VERSION"));
}
