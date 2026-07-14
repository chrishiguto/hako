//! The flow-schema sync task. Proto's flow types are the source of
//! truth (ADR 0009); `schemas/flow.schema.json` is a committed
//! artifact of them for consumers that cannot link Rust — editors and
//! LLMs — and for `hako schema` to print. Write mode regenerates the
//! file; `--check` fails CI when it drifts from the types. The tests
//! in `tests/` pin the artifact's agreement with strict serde.

use std::fs;

use anyhow::{Context, bail};
use cargo_metadata::MetadataCommand;

const SCHEMA_PATH: &str = "schemas/flow.schema.json";

pub fn run(check: bool) -> anyhow::Result<()> {
    let generated = serde_json::to_string_pretty(&proto::flow::json_schema())
        .context("flow schema did not serialize")?
        + "\n";
    // Resolved at run time — a compile-time CARGO_MANIFEST_DIR would go
    // stale when a built xtask binary outlives a moved checkout.
    let metadata = MetadataCommand::new()
        .no_deps()
        .other_options(vec!["--locked".to_string()])
        .exec()
        .context("failed to run `cargo metadata`")?;
    let path = metadata.workspace_root.join(SCHEMA_PATH);
    if check {
        let committed = fs::read_to_string(&path)
            .with_context(|| format!("cannot read {SCHEMA_PATH} — run `cargo xtask schema`"))?;
        if committed != generated {
            bail!(
                "{SCHEMA_PATH} has drifted from the engine's flow types — \
                 run `cargo xtask schema` and commit the result"
            );
        }
        println!("{SCHEMA_PATH} matches the engine's flow types");
    } else {
        fs::write(&path, generated).with_context(|| format!("cannot write {SCHEMA_PATH}"))?;
        println!("wrote {SCHEMA_PATH}");
    }
    Ok(())
}
