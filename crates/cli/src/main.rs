//! `hako` — a pure client of the daemon's wire contract; among workspace
//! crates it may depend on `api` only. Stub: reports a version,
//! nothing more.

// The crate's one allowed workspace edge, declared ahead of the client
// code that will use it.
use api as _;

fn main() {
    println!("hako {}", env!("CARGO_PKG_VERSION"));
}
