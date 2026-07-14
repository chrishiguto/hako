//! The shared `cargo metadata` invocation for xtask tasks.

use anyhow::Context;
use cargo_metadata::{Metadata, MetadataCommand};

/// Runs the given invocation `--locked`. A task must never mutate:
/// unlocked `cargo metadata` silently rewrites a stale Cargo.lock
/// instead of failing on it.
pub fn exec_locked(command: &mut MetadataCommand) -> anyhow::Result<Metadata> {
    command
        .other_options(vec!["--locked".to_string()])
        .exec()
        .context("failed to run `cargo metadata`")
}
