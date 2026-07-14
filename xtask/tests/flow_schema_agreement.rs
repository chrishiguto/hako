//! Pins the generated flow schema — the artifact editors and LLMs
//! validate against — to the strict serde parser every Rust consumer
//! shares (ADR 0009). Any flow one accepts and the other rejects is a
//! published-contract break. These tests live with the generator:
//! xtask is the one crate that always enables proto's `schema`
//! feature, so they run on every `cargo test --workspace`.
//!
//! One divergence is inherent and documented below: JSON Schema's
//! `integer` admits zero-fraction floats (`6.0`) that TOML-side serde
//! rejects. The schema errs permissive there — the daemon still fails
//! such a flow loudly at submit, before a run starts.
//!
//! The corpus's TOML→JSON step is the toml crate's default
//! serialization — since ADR 0009 the only such conversion in the
//! tree, and test-only: no product code converts a flow to JSON.

use proto::flow::{self, FlowConfig};

/// The representative flow the corpus mutates; also driven verbatim by
/// the CLI's validate tests.
const RALPH_EXAMPLE: &str = include_str!("../../examples/ralph.toml");

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

/// The schema must carry the same strictness as the serde types:
/// every object in it rejects unknown keys, or an editor would bless
/// flows the daemon rejects.
#[test]
fn every_object_in_the_schema_rejects_unknown_keys() {
    let schema = serde_json::to_value(flow::json_schema()).unwrap();
    let root = schema.as_object().unwrap();
    assert_eq!(root["additionalProperties"], serde_json::json!(false));
    for (name, definition) in root["$defs"].as_object().unwrap() {
        if definition["type"] == serde_json::json!("object") {
            assert_eq!(
                definition["additionalProperties"],
                serde_json::json!(false),
                "{name}"
            );
        }
    }
}

/// Every sub-64-bit integer anywhere in the schema must carry the
/// explicit bounds the generator's transform stamps — a config field
/// with an integer type the transform doesn't know fails here instead
/// of becoming a hole editors would bless. The 64-bit formats are
/// exempt: TOML integers are i64, so their bounds are unreachable
/// from a flow file.
#[test]
fn every_integer_format_in_the_schema_carries_bounds() {
    fn walk(node: &serde_json::Value, path: &str, found: &mut u32) {
        match node {
            serde_json::Value::Object(entries) => {
                let sub_64_bit = |f: &&str| f.contains("int") && *f != "uint64" && *f != "int64";
                let format = entries.get("format").and_then(serde_json::Value::as_str);
                if let Some(format) = format.filter(sub_64_bit) {
                    *found += 1;
                    for bound in ["minimum", "maximum"] {
                        assert!(
                            entries.contains_key(bound),
                            "{path}: format `{format}` lacks `{bound}` — teach the \
                             generator's bounds transform this format or use a bounded type"
                        );
                    }
                }
                for (key, entry) in entries {
                    walk(entry, &format!("{path}/{key}"), found);
                }
            }
            serde_json::Value::Array(items) => {
                for (index, item) in items.iter().enumerate() {
                    walk(item, &format!("{path}/{index}"), found);
                }
            }
            _ => {}
        }
    }
    let schema = serde_json::to_value(flow::json_schema()).unwrap();
    let mut found = 0;
    walk(&schema, "", &mut found);
    assert!(found > 0, "the walk matched no integer formats");
}

#[test]
fn the_schema_names_every_flow_section() {
    let schema = serde_json::to_value(flow::json_schema()).unwrap();
    let properties = schema["properties"].as_object().unwrap();
    for section in [
        "loop",
        "agent",
        "budget",
        "verify",
        "workspace",
        "secrets",
        "notify",
    ] {
        assert!(properties.contains_key(section), "{section}");
    }
}
