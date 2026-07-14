//! Pins the generated flow schema to strict serde. The CLI validates
//! with the schema and the daemon parses with serde; any flow one
//! accepts and the other rejects is a contract break — the exact
//! failure this crate's strictness exists to prevent. A corpus of
//! accept and reject cases runs through both.
//!
//! One divergence is inherent and documented below: JSON Schema's
//! `integer` admits zero-fraction floats (`6.0`) that TOML-side serde
//! rejects. The schema errs permissive there — the daemon still fails
//! such a flow loudly at submit, before a run starts.

use engine::flow::{self, FlowConfig};

/// The representative flow the corpus mutates; also driven verbatim by
/// the CLI's validate tests.
const RALPH_EXAMPLE: &str = include_str!("../../../examples/ralph.toml");

const MINIMAL_FLOW: &str = r#"
[loop]
kernel = "ralph"
goal = "Fix the flaky test"

[agent]
engine = "claude"

[workspace]
repo = "."
"#;

fn schema_accepts(flow_toml: &str) -> bool {
    let value: toml::Value = toml::from_str(flow_toml).expect("corpus entries are valid TOML");
    let flow = serde_json::to_value(value).expect("TOML values are JSON-representable");
    let schema = serde_json::to_value(flow::json_schema()).expect("schema serializes");
    jsonschema::is_valid(&schema, &flow)
}

fn serde_accepts(flow_toml: &str) -> bool {
    FlowConfig::from_toml(flow_toml).is_ok()
}

#[test]
fn schema_and_serde_agree_on_the_corpus() {
    let misspelled_key = RALPH_EXAMPLE.replace("max_iterations", "max_iteration");
    let misspelled_kernel = RALPH_EXAMPLE.replace("\"ralph\"", "\"ralf\"");
    let missing_section = MINIMAL_FLOW.replace("[workspace]\nrepo = \".\"", "");
    let corpus: &[(&str, &str, bool)] = &[
        ("representative flow", RALPH_EXAMPLE, true),
        ("minimal flow", MINIMAL_FLOW, true),
        ("misspelled key", &misspelled_key, false),
        ("misspelled kernel", &misspelled_kernel, false),
        ("missing required section", &missing_section, false),
        (
            "max_iterations over u32",
            &format!("{MINIMAL_FLOW}\n[budget]\nmax_iterations = 4294967296\n"),
            false,
        ),
        (
            "retries over u32",
            &format!(
                "{MINIMAL_FLOW}\n[verify]\non_fail = {{ retries = 4294967296, then = \"pause\" }}\n"
            ),
            false,
        ),
        (
            "max_tokens at TOML's integer ceiling",
            &format!("{MINIMAL_FLOW}\n[budget]\nmax_tokens = 9223372036854775807\n"),
            true,
        ),
        (
            "timeout at the nine-digit bound",
            &format!("{MINIMAL_FLOW}\n[budget]\niteration_timeout = \"999999999h\"\n"),
            true,
        ),
        (
            "timeout over the nine-digit bound",
            &format!("{MINIMAL_FLOW}\n[budget]\niteration_timeout = \"1000000000h\"\n"),
            false,
        ),
        (
            "timeout with a leading zero",
            &format!("{MINIMAL_FLOW}\n[budget]\niteration_timeout = \"030m\"\n"),
            false,
        ),
        (
            "datetime where a string belongs",
            &MINIMAL_FLOW.replace("\"Fix the flaky test\"", "2026-01-01"),
            false,
        ),
    ];
    for (name, flow, accepted) in corpus {
        assert_eq!(schema_accepts(flow), *accepted, "schema on: {name}");
        assert_eq!(serde_accepts(flow), *accepted, "serde on: {name}");
    }
}

/// The known permissive edge, pinned so a fix (or a widening) shows up.
#[test]
fn zero_fraction_floats_are_the_one_known_divergence() {
    let flow = format!("{MINIMAL_FLOW}\n[budget]\nmax_hours = 6.0\n");
    assert!(schema_accepts(&flow));
    assert!(!serde_accepts(&flow));
}
