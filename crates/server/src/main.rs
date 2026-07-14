//! `hakod` — the hako daemon. Runs execute only in here; it hosts the
//! engine and serves the wire contract. Stub: reports a version, nothing
//! more.

// The crate's workspace edges, declared ahead of the daemon code that
// will use them.
use api as _;
use engine as _;
use sandbox as _;

fn main() {
    println!("hakod {}", env!("CARGO_PKG_VERSION"));
}
