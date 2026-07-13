//! The smolvm-backed implementation of the engine's `Sandbox` seam:
//! ephemeral microVMs, one per iteration.

// The crate's workspace edge, declared ahead of the `Sandbox` trait
// implementation that will use it.
use hako_engine as _;
