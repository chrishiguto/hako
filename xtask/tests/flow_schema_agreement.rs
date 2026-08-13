//! Pins the generated flow schema — the artifact editors and LLMs
//! validate against — to the strict serde parser every Rust consumer
//! shares. Any flow one accepts and the other rejects is a
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
//! serialization — the only such conversion in the tree, and
//! test-only: no product code converts a flow to JSON.

mod common;

use std::collections::BTreeSet;
use std::sync::LazyLock;

use proto::flow::{self, FlowConfig, KernelName};

/// The representative flow the corpus mutates; also driven verbatim by
/// the CLI's validate tests.
const PIPELINE_EXAMPLE: &str = include_str!("../../examples/pipeline.toml");

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

#[test]
fn schema_and_serde_agree_on_the_corpus() {
    let misspelled_key = PIPELINE_EXAMPLE.replace("max_iterations", "max_iteration");
    let misspelled_kernel = PIPELINE_EXAMPLE.replace("\"pipeline\"", "\"pypeline\"");
    let missing_section = MINIMAL_FLOW.replace("[workspace]\nrepo = \".\"", "");
    let corpus: &[(&str, &str, bool)] = &[
        ("representative flow", PIPELINE_EXAMPLE, true),
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
            "a secret name with a dash",
            &format!("{MINIMAL_FLOW}\n[secrets]\nenv = [\"GH-TOKEN\"]\n"),
            false,
        ),
        (
            "an empty secret name",
            &format!("{MINIMAL_FLOW}\n[secrets]\nenv = [\"\"]\n"),
            false,
        ),
        (
            "datetime where a string belongs",
            &MINIMAL_FLOW.replace("repo = \".\"", "repo = 2026-01-01"),
            false,
        ),
        (
            "the deleted goal key",
            &MINIMAL_FLOW.replace(
                "kernel = \"pipeline\"",
                "kernel = \"pipeline\"\ngoal = \"Fix the flaky test\"",
            ),
            false,
        ),
        (
            "a prompt slot the kernel does not publish",
            &format!("{MINIMAL_FLOW}\n[prompts]\nplann = \"prompts/plan.md\"\n"),
            false,
        ),
        (
            "the optional deliver prompt slot",
            &format!("{MINIMAL_FLOW}\n[prompts]\ndeliver = \"prompts/deliver.md\"\n"),
            true,
        ),
        (
            "the reserved stages namespace",
            &format!("{MINIMAL_FLOW}\n[stages.review]\nagent = \"codex\"\n"),
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

#[test]
fn every_object_in_the_schema_rejects_unknown_keys() {
    common::assert_every_object_rejects_unknown_keys(&SCHEMA);
}

#[test]
fn every_integer_format_in_the_schema_carries_bounds() {
    let found = common::assert_integer_formats_carry_bounds(&SCHEMA);
    assert!(found > 0, "the walk matched no integer formats");
}

#[test]
fn the_prompts_schema_is_exact_for_every_kernel() {
    let all_slots: BTreeSet<&str> = KernelName::ALL
        .into_iter()
        .flat_map(KernelName::prompt_slots)
        .copied()
        .collect();
    for kernel in KernelName::ALL {
        for slot in &all_slots {
            let flow = MINIMAL_FLOW.replace(
                "kernel = \"pipeline\"",
                &format!("kernel = \"{}\"", kernel.as_str()),
            );
            let flow = format!("{flow}\n[prompts]\n{slot} = \"prompts/{slot}.md\"\n");
            let expected = kernel.prompt_slots().contains(slot);
            assert_eq!(
                schema_accepts(&flow),
                expected,
                "{}: {slot}",
                kernel.as_str()
            );
            assert_eq!(
                serde_accepts(&flow),
                expected,
                "{}: {slot}",
                kernel.as_str()
            );
        }
    }
}

/// The per-kernel prompt conditional must key on the kernel name
/// alone. Were its `if` to close the `[loop]` table, the first new
/// `[loop]` key would make every condition fail — and the slot
/// narrowing silently vanish — exactly for the flows using that key.
/// Simulated here by growing `LoopConfig` the way a regenerated
/// schema would.
#[test]
fn the_prompt_conditionals_survive_a_new_loop_key() {
    let mut grown = SCHEMA.clone();
    grown["$defs"]["LoopConfig"]["properties"]["max_children"] = serde_json::json!({"type": "integer", "format": "uint32", "minimum": 0, "maximum": 4294967295_u64});
    let validator = jsonschema::validator_for(&grown).expect("grown schema compiles");
    let accepts = |flow_toml: &str| {
        let value: toml::Value = toml::from_str(flow_toml).expect("probe flows are valid TOML");
        validator.is_valid(&serde_json::to_value(value).unwrap())
    };

    let fanout_with_new_key = MINIMAL_FLOW.replace(
        "kernel = \"pipeline\"",
        "kernel = \"fanout\"\nmax_children = 2",
    );
    // The grown table itself must validate, or the assertion below
    // would pass for the wrong reason.
    assert!(accepts(&fanout_with_new_key));
    assert!(
        !accepts(&format!(
            "{fanout_with_new_key}\n[prompts]\nimplement = \"prompts/implement.md\"\n"
        )),
        "a fanout flow gained a pipeline slot because `[loop]` grew a key"
    );
}

/// The representative example documents every flow section, so the
/// schema's top-level properties must equal its tables — a new
/// section extends this guard the moment the example gains it, with
/// no hand-kept list to forget.
#[test]
fn the_schema_names_every_flow_section() {
    let example: toml::Value = toml::from_str(PIPELINE_EXAMPLE).unwrap();
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
