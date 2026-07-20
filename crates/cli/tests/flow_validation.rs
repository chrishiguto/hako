//! `hako validate` / `hako schema`, driven at the binary boundary —
//! the CLI's highest seam. What matters is the contract flow authors
//! feel: exit codes, and errors that carry the offending line and the
//! fix. Validation runs the daemon's own parser, so these tests also
//! pin the error text a rejected submit would produce.

use std::path::Path;
use std::process::{Command, Output};

fn hako(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_hako"))
        .args(args)
        .output()
        .expect("hako runs")
}

/// A path relative to this crate's directory, as the UTF-8 string
/// `hako`'s argv needs.
fn repo_path(relative: &str) -> String {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(relative)
        .into_os_string()
        .into_string()
        .expect("path is UTF-8")
}

fn fixture(name: &str) -> String {
    repo_path(&format!("tests/fixtures/{name}"))
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

#[test]
fn the_committed_example_flow_validates() {
    let output = hako(&["validate", &repo_path("../../examples/pipeline.toml")]);
    assert!(output.status.success(), "{output:?}");
    assert!(String::from_utf8_lossy(&output.stdout).contains("valid flow"));
}

/// Each rejected fixture fails naming the offending key, its line, or
/// the fix — the error text is proto's, reaching stderr verbatim.
#[test]
fn invalid_flows_fail_with_the_parsers_error_text() {
    let corpus: &[(&str, &[&str])] = &[
        (
            "misspelled-key.toml",
            &["max_iteration", "max_iterations", "line"],
        ),
        ("misspelled-kernel.toml", &["pypeline", "pipeline"]),
        ("out-of-range.toml", &["max_iterations", "u32"]),
        ("nonfinite.toml", &["max_tokens", "inf"]),
        ("datetime.toml", &["repo", "string"]),
        (
            "bad-duration.toml",
            &["invalid duration", "030m", "\"30m\""],
        ),
        ("syntax-error.toml", &["line"]),
    ];
    for (name, expected) in corpus {
        let output = hako(&["validate", &fixture(name)]);
        assert!(!output.status.success(), "{name}");
        let stderr = stderr(&output);
        for fragment in *expected {
            assert!(stderr.contains(fragment), "{name}: {stderr}");
        }
    }
}

#[test]
fn a_missing_file_fails_naming_it() {
    let output = hako(&["validate", "no-such-flow.toml"]);
    assert!(!output.status.success());
    assert!(stderr(&output).contains("no-such-flow.toml"));
}

#[test]
fn schema_prints_the_committed_schema_verbatim() {
    let output = hako(&["schema"]);
    assert!(output.status.success(), "{output:?}");
    let committed = include_str!("../../../schemas/flow.schema.json");
    assert_eq!(String::from_utf8_lossy(&output.stdout), committed);
}
