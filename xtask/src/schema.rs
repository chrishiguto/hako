//! The schema sync task. Proto's types are the source of truth; the
//! files under `schemas/` are committed artifacts of them for
//! consumers that cannot link Rust — editors, LLMs, and the stage
//! preambles that quote a report contract. Write mode syncs the
//! directory to the generators, removing files no type generates;
//! `--check` fails CI when any artifact drifts. The tests in `tests/`
//! pin the artifacts' agreement with strict serde.

use std::collections::BTreeSet;
use std::fs;
use std::io;
use std::path::Path;

use anyhow::{Context, bail};
use cargo_metadata::MetadataCommand;

const SCHEMAS_DIR: &str = "schemas";

/// Every committed schema: its path under the workspace root, and the
/// generated content it must equal.
fn artifacts() -> anyhow::Result<Vec<(String, String)>> {
    let mut artifacts = vec![(
        format!("{SCHEMAS_DIR}/flow.schema.json"),
        render(&proto::flow::json_schema())?,
    )];
    for stage in proto::report::Stage::ALL {
        artifacts.push((
            format!("{SCHEMAS_DIR}/report/{}.schema.json", stage.as_str()),
            render(&proto::report::stage_schema(stage))?,
        ));
    }
    Ok(artifacts)
}

fn render(schema: &schemars::Schema) -> anyhow::Result<String> {
    Ok(serde_json::to_string_pretty(schema).context("schema did not serialize")? + "\n")
}

/// Every file under `schemas/`, relative to the workspace root — so an
/// artifact no type generates surfaces as stale instead of lingering
/// as a dead contract.
fn committed_files(root: &Path) -> anyhow::Result<Vec<String>> {
    let mut files = Vec::new();
    let mut pending = vec![root.join(SCHEMAS_DIR)];
    while let Some(dir) = pending.pop() {
        let entries = match fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(error).with_context(|| format!("cannot read {}", dir.display()));
            }
        };
        for entry in entries {
            let path = entry
                .with_context(|| format!("cannot read {}", dir.display()))?
                .path();
            if path.is_dir() {
                pending.push(path);
            } else {
                let relative = path
                    .strip_prefix(root)
                    .expect("walked paths sit under the root")
                    .to_str()
                    .with_context(|| format!("non-UTF-8 path {}", path.display()))?;
                files.push(relative.to_string());
            }
        }
    }
    Ok(files)
}

pub fn run(check: bool) -> anyhow::Result<()> {
    let artifacts = artifacts()?;
    // Resolved at run time — a compile-time CARGO_MANIFEST_DIR would go
    // stale when a built xtask binary outlives a moved checkout.
    let metadata = crate::metadata::exec_locked(MetadataCommand::new().no_deps())?;
    let root = metadata.workspace_root.as_std_path();
    let expected: BTreeSet<&str> = artifacts.iter().map(|(path, _)| path.as_str()).collect();
    let strays: Vec<String> = committed_files(root)?
        .into_iter()
        .filter(|path| !expected.contains(path.as_str()))
        .collect();

    if check {
        let mut wrong = Vec::new();
        for (path, generated) in &artifacts {
            let committed = fs::read_to_string(root.join(path))
                .with_context(|| format!("cannot read {path} — run `cargo xtask schema`"))?;
            if committed != *generated {
                wrong.push(format!("drifted: {path}"));
            }
        }
        wrong.extend(
            strays
                .iter()
                .map(|path| format!("stale: {path} — no type generates it")),
        );
        if !wrong.is_empty() {
            bail!(
                "{SCHEMAS_DIR}/ has drifted from proto's types — \
                 run `cargo xtask schema` and commit the result\n  {}",
                wrong.join("\n  ")
            );
        }
        println!("{SCHEMAS_DIR}/ matches proto's types");
    } else {
        for (path, generated) in &artifacts {
            let target = root.join(path);
            let parent = target.parent().expect("artifact paths have a parent");
            fs::create_dir_all(parent).with_context(|| format!("cannot create {SCHEMAS_DIR}/"))?;
            fs::write(&target, generated).with_context(|| format!("cannot write {path}"))?;
            println!("wrote {path}");
        }
        for path in &strays {
            fs::remove_file(root.join(path)).with_context(|| format!("cannot remove {path}"))?;
            println!("removed {path} — no type generates it");
        }
    }
    Ok(())
}
