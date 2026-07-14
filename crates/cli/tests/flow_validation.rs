//! `hako validate` / `hako schema`, driven at the binary boundary —
//! the CLI's highest seam. What matters is the contract flow authors
//! feel: exit codes, and errors that name the offending key.

use std::path::Path;
use std::process::{Command, Output};

fn hako(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_hako"))
        .args(args)
        .output()
        .expect("hako runs")
}

fn fixture(name: &str) -> String {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
        .into_os_string()
        .into_string()
        .expect("fixture path is UTF-8")
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

#[test]
fn the_committed_example_flow_validates() {
    let example = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../examples/ralph.toml")
        .into_os_string()
        .into_string()
        .expect("example path is UTF-8");
    let output = hako(&["validate", &example]);
    assert!(output.status.success(), "{output:?}");
    assert!(String::from_utf8_lossy(&output.stdout).contains("valid flow"));
}

#[test]
fn a_misspelled_key_fails_naming_it_and_its_section() {
    let output = hako(&["validate", &fixture("misspelled-key.toml")]);
    assert!(!output.status.success());
    let stderr = stderr(&output);
    assert!(stderr.contains("max_iteration"), "{stderr}");
    assert!(stderr.contains("/budget"), "{stderr}");
}

#[test]
fn a_misspelled_kernel_fails_naming_the_real_one() {
    let output = hako(&["validate", &fixture("misspelled-kernel.toml")]);
    assert!(!output.status.success());
    let stderr = stderr(&output);
    assert!(stderr.contains("ralf"), "{stderr}");
    assert!(stderr.contains("ralph"), "{stderr}");
}

#[test]
fn an_out_of_range_integer_fails_naming_the_maximum() {
    let output = hako(&["validate", &fixture("out-of-range.toml")]);
    assert!(!output.status.success());
    let stderr = stderr(&output);
    assert!(stderr.contains("/budget/max_iterations"), "{stderr}");
    assert!(stderr.contains("maximum"), "{stderr}");
}

#[test]
fn a_non_finite_number_fails_naming_its_key() {
    let output = hako(&["validate", &fixture("nonfinite.toml")]);
    assert!(!output.status.success());
    let stderr = stderr(&output);
    assert!(stderr.contains("/budget/max_tokens"), "{stderr}");
    assert!(stderr.contains("finite"), "{stderr}");
}

#[test]
fn a_datetime_fails_naming_its_key() {
    let output = hako(&["validate", &fixture("datetime.toml")]);
    assert!(!output.status.success());
    let stderr = stderr(&output);
    assert!(stderr.contains("/loop/goal"), "{stderr}");
    assert!(stderr.contains("dates"), "{stderr}");
}

#[test]
fn a_toml_syntax_error_fails_with_a_line_pointer() {
    let output = hako(&["validate", &fixture("syntax-error.toml")]);
    assert!(!output.status.success());
    let stderr = stderr(&output);
    assert!(stderr.contains("line"), "{stderr}");
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
    let committed = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../schemas/flow.schema.json");
    let committed = std::fs::read_to_string(committed).expect("committed schema exists");
    assert_eq!(String::from_utf8_lossy(&output.stdout), committed);
}
