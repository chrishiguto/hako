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

use std::collections::BTreeSet;
use std::sync::LazyLock;

use proto::flow::{self, FlowConfig};

/// The representative flow the corpus mutates; also driven verbatim by
/// the CLI's validate tests.
const RALPH_EXAMPLE: &str = include_str!("../../examples/ralph.toml");

/// The committed smallest-valid flow; proto's flow tests drive the
/// same file.
const MINIMAL_FLOW: &str = include_str!("../../examples/minimal.toml");

/// Generated once for the whole binary: every test reads the same
/// schema, and compiling its validator is the expensive step.
static SCHEMA: LazyLock<serde_json::Value> =
    LazyLock::new(|| serde_json::to_value(flow::json_schema()).expect("schema serializes"));

static VALIDATOR: LazyLock<jsonschema::Validator> =
    LazyLock::new(|| jsonschema::validator_for(&SCHEMA).expect("schema compiles"));

fn schema_accepts(flow_toml: &str) -> bool {
    let value: toml::Value = toml::from_str(flow_toml).expect("corpus entries are valid TOML");
    let flow = serde_json::to_value(value).expect("TOML values are JSON-representable");
    VALIDATOR.is_valid(&flow)
}

fn serde_accepts(flow_toml: &str) -> bool {
    FlowConfig::from_toml(flow_toml).is_ok()
}

/// Depth-first visit of every object node in the schema, with its
/// path — the one walker the structural guards below share.
fn walk_objects(
    node: &serde_json::Value,
    path: &str,
    visit: &mut impl FnMut(&serde_json::Map<String, serde_json::Value>, &str),
) {
    match node {
        serde_json::Value::Object(entries) => {
            visit(entries, path);
            for (key, entry) in entries {
                walk_objects(entry, &format!("{path}/{key}"), visit);
            }
        }
        serde_json::Value::Array(items) => {
            for (index, item) in items.iter().enumerate() {
                walk_objects(item, &format!("{path}/{index}"), visit);
            }
        }
        _ => {}
    }
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
/// every object schema anywhere in it rejects unknown keys, or an
/// editor would bless flows the daemon rejects. Walking every node —
/// not just the top-level `$defs` — keeps future inline subschemas
/// covered too.
#[test]
fn every_object_in_the_schema_rejects_unknown_keys() {
    let mut found = 0;
    walk_objects(&SCHEMA, "", &mut |entries, path| {
        let is_object_schema = entries.contains_key("properties")
            || entries.get("type") == Some(&serde_json::json!("object"));
        if is_object_schema {
            found += 1;
            assert_eq!(
                entries.get("additionalProperties"),
                Some(&serde_json::json!(false)),
                "{path}: object schema does not reject unknown keys"
            );
        }
    });
    assert!(found > 0, "the walk matched no object schemas");
}

/// Every sub-64-bit integer anywhere in the schema must carry the
/// explicit bounds the generator's transform stamps — a config field
/// with an integer type the transform doesn't know fails here instead
/// of becoming a hole editors would bless. The 64-bit formats are
/// exempt: TOML integers are i64, so their bounds are unreachable
/// from a flow file.
#[test]
fn every_integer_format_in_the_schema_carries_bounds() {
    let mut found = 0;
    walk_objects(&SCHEMA, "", &mut |entries, path| {
        let sub_64_bit = |f: &&str| f.contains("int") && *f != "uint64" && *f != "int64";
        let format = entries.get("format").and_then(serde_json::Value::as_str);
        if let Some(format) = format.filter(sub_64_bit) {
            found += 1;
            for bound in ["minimum", "maximum"] {
                assert!(
                    entries.contains_key(bound),
                    "{path}: format `{format}` lacks `{bound}` — teach the \
                     generator's bounds transform this format or use a bounded type"
                );
            }
        }
    });
    assert!(found > 0, "the walk matched no integer formats");
}

/// The representative example documents every flow section, so the
/// schema's top-level properties must equal its tables — a new
/// section extends this guard the moment the example gains it, with
/// no hand-kept list to forget.
#[test]
fn the_schema_names_every_flow_section() {
    let example: toml::Value = toml::from_str(RALPH_EXAMPLE).unwrap();
    let sections: BTreeSet<&str> = example
        .as_table()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    let schema: &serde_json::Value = &SCHEMA;
    let properties: BTreeSet<&str> = schema["properties"]
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(properties, sections);
}
