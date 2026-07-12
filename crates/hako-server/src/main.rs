//! `hakod` — the hako daemon. Runs execute only in here (see ADR 0002);
//! it hosts the engine and serves the wire contract. Stub: reports a
//! version, nothing more.

// Declared edges from ADR 0006; unused until the daemon lands.
use hako_api as _;
use hako_engine as _;
use hako_sandbox as _;

fn main() {
    println!("hakod {}", env!("CARGO_PKG_VERSION"));
}
