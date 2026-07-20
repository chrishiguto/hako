//! Flow config — the ~20-line TOML that parameterizes a kernel, and
//! the most published surface hako has: authored by users and LLMs,
//! validated by editors, parsed by the daemon. Its types live here
//! so every Rust consumer shares one strict parser — `hako validate`
//! runs [`FlowConfig::from_toml`] too, making its verdict and errors
//! exactly the daemon's.
//!
//! Deserialization is strict: every table rejects unknown keys, so a
//! typo fails at validation time pointing at the offending key, not at
//! iteration 4. Consumers that cannot link Rust — editors, LLMs — get
//! the committed `schemas/flow.schema.json`, generated from these same
//! types (`json_schema` via `cargo xtask schema`, behind the `schema`
//! feature) and drift-checked in CI.

use std::collections::BTreeMap;
use std::str::FromStr;
use std::time::Duration;

use serde::Deserialize;

use crate::secrets::SecretName;

/// A parsed flow: which kernel to run, with which agent, under which
/// limits. Contains no logic — control flow belongs to the kernel —
/// and no objective: that lives in the domain prompts the kernel
/// reads.
///
/// The `stages` namespace is reserved for the future per-stage agent
/// override (`[stages.<name>]`); nothing else may occupy it. Until it
/// lands, a `stages` table is rejected like any other unknown key.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct FlowConfig {
    pub r#loop: LoopConfig,
    #[serde(default)]
    pub prompts: PromptsConfig,
    pub agent: AgentConfig,
    #[serde(default)]
    pub budget: BudgetConfig,
    #[serde(default)]
    pub verify: VerifyConfig,
    pub workspace: WorkspaceConfig,
    #[serde(default)]
    pub secrets: SecretsConfig,
    pub notify: Option<NotifyConfig>,
}

impl FlowConfig {
    /// Strictly parses flow TOML, then checks the one rule serde
    /// cannot see: `[prompts]` keys against the slots the selected
    /// kernel publishes — no single table knows both. Parse errors
    /// carry the offending line and key; the slot check names the
    /// slot, the kernel, and the legal set.
    pub fn from_toml(source: &str) -> Result<Self, FlowError> {
        let flow: Self = toml::from_str(source)?;
        let kernel = flow.r#loop.kernel;
        let published = kernel.prompt_slots();
        if let Some(slot) = flow.prompts.slots().find(|slot| !published.contains(slot)) {
            return Err(FlowError::UnknownPromptSlot {
                slot: slot.to_string(),
                kernel,
            });
        }
        Ok(flow)
    }
}

/// A flow that failed validation.
#[derive(Debug, thiserror::Error)]
pub enum FlowError {
    /// Strict parsing failed. Displays the TOML error verbatim — it
    /// already points at the offending line and key.
    #[error(transparent)]
    Toml(#[from] toml::de::Error),
    /// A `[prompts]` key named a slot the selected kernel does not
    /// publish.
    #[error(
        "unknown prompt slot `{slot}` for kernel `{}`: this kernel's slots are {}",
        kernel.as_str(),
        published_slots(*kernel)
    )]
    UnknownPromptSlot { slot: String, kernel: KernelName },
}

/// The kernel's published slots spelled for an error message —
/// backticked, comma-separated.
fn published_slots(kernel: KernelName) -> String {
    let slots: Vec<String> = kernel
        .prompt_slots()
        .iter()
        .map(|slot| format!("`{slot}`"))
        .collect();
    slots.join(", ")
}

/// Which kernel runs the loop.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct LoopConfig {
    pub kernel: KernelName,
}

/// The loop patterns the engine ships. A closed set by design: a new
/// loop shape is a new kernel in Rust, never logic in the flow file —
/// which is what lets the schema reject a misspelled kernel outright.
/// A name may be declared ahead of its kernel so the flow language
/// never has zero kernels; submitting such a flow fails at kernel
/// resolution, not here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum KernelName {
    /// The staged loop: one work unit per iteration, driven through a
    /// fixed stage sequence the kernel owns in Rust.
    Pipeline,
}

impl KernelName {
    /// Every kernel, for the consumers that sweep the set — today the
    /// prompts schema's slot union. Like [`crate::pipeline::Stage::ALL`],
    /// the one enumeration the compiler cannot check: the ordinal test
    /// below catches an insertion that forgets it; a forgotten append
    /// rests on review.
    pub const ALL: [Self; 1] = [Self::Pipeline];

    /// The wire string flows select the kernel by — the same string
    /// serde reads, spelled once for error messages and run metadata.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pipeline => "pipeline",
        }
    }

    /// The prompt slots the selected kernel publishes — the legal
    /// `[prompts]` keys. Slot vocabulary is dialect: the names live
    /// in the kernel's own module; only this dispatch is core.
    pub fn prompt_slots(self) -> &'static [&'static str] {
        match self {
            Self::Pipeline => &crate::pipeline::PROMPT_SLOTS,
        }
    }
}

/// The one prose both [`PromptsConfig`]'s rustdoc and its schema
/// description carry — schemars copies doc comments only for derived
/// impls, and `#[doc]` cannot read a `const`, so a macro is what keeps
/// the two from drifting.
macro_rules! prompts_config_doc {
    () => {
        "Per-slot prompt overrides: slot name → workspace-relative prompt file. The legal slots are the ones the selected kernel publishes; every absent slot falls back to the kernel-shipped default prompt, so an empty or missing table is a complete flow."
    };
}

#[doc = prompts_config_doc!()]
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize)]
#[serde(transparent)]
pub struct PromptsConfig(BTreeMap<String, String>);

impl PromptsConfig {
    /// The prompt file overriding `slot` — workspace-relative — if the
    /// flow names one.
    pub fn get(&self, slot: &str) -> Option<&str> {
        self.0.get(slot).map(String::as_str)
    }

    /// Every slot the flow overrides, in sorted order.
    pub fn slots(&self) -> impl Iterator<Item = &str> {
        self.0.keys().map(String::as_str)
    }
}

/// Which agent drives the iterations.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct AgentConfig {
    /// An agent adapter name, e.g. `claude`. An open set — adapters
    /// are registered at runtime — so unlike `kernel` it is not an
    /// enum.
    pub engine: String,
    /// The argv template for the `cmd` engine, e.g.
    /// `["aider", "--message", "{prompt}"]` — every `{prompt}`
    /// placeholder is replaced with the composed prompt. Meaningful
    /// only with `engine = "cmd"`; adapter resolution rejects it
    /// elsewhere and requires it there.
    pub command: Option<Vec<String>>,
}

/// The caps a flow puts on one run. Everything left unset keeps the
/// engine's default.
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct BudgetConfig {
    pub max_iterations: Option<u32>,
    /// Wall-clock cap on the whole run, in whole hours.
    pub max_hours: Option<u32>,
    pub max_tokens: Option<u64>,
    /// Cap on one iteration, e.g. `"30m"`. The one hard budget: on
    /// expiry the sandbox is destroyed and the iteration counts as
    /// failed.
    pub iteration_timeout: Option<FlowDuration>,
}

/// The verify checks an iteration must pass to count as progress. No
/// checks means every iteration counts.
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct VerifyConfig {
    /// Commands run in the sandbox after the agent's invocation; each
    /// must exit zero.
    #[serde(default)]
    pub checks: Vec<String>,
    #[serde(default)]
    pub on_fail: OnFail,
}

/// What the kernel does once an iteration's checks fail.
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct OnFail {
    /// Extra attempts the agent gets at the failing iteration before
    /// `then` applies.
    #[serde(default)]
    pub retries: u32,
    #[serde(default)]
    pub then: FailAction,
}

/// Where the run goes when retries are spent. Pausing is the default:
/// a run that stops making verified progress should wait for a human,
/// not burn budget or die.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum FailAction {
    #[default]
    Pause,
    Fail,
}

/// The one thing that survives iterations — the repo the loop works
/// on.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct WorkspaceConfig {
    /// The repository the run works on: a git URL or local path to
    /// clone from, or — in mount mode — the checkout to work in.
    pub repo: String,
    #[serde(default)]
    pub mode: WorkspaceMode,
    /// An existing branch the clone checks out, so a run continues
    /// prior work — and can update its PR when its prompts say to
    /// push. Meaningful only in clone mode; preparation rejects it
    /// elsewhere. Absent, the run gets its own fresh branch.
    pub branch: Option<String>,
    /// Runs against a dirty mounted checkout anyway. Meaningful only
    /// in mount mode; preparation rejects it elsewhere.
    #[serde(default)]
    pub force: bool,
}

/// How the run's workspace comes to exist. A closed set on purpose —
/// preparation is engine logic, not a seam.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum WorkspaceMode {
    /// The default: the run clones `repo` into a run-owned workspace
    /// on a run branch, so the source checkout stays unreachable by
    /// construction.
    #[default]
    Clone,
    /// Opt-in: the run works directly in the existing checkout at
    /// `repo` — for developing the tool itself and pre-cloned repos on
    /// a VPS.
    Mount,
}

/// Secret *names* only — values live in the daemon's store, so flow
/// files stay safe to commit and to hand to an LLM.
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct SecretsConfig {
    /// Secrets injected into the sandbox as environment variables, by
    /// name.
    #[serde(default)]
    pub env: Vec<SecretName>,
}

/// Where the daemon pushes run lifecycle notifications.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct NotifyConfig {
    /// URL POSTed to when the run pauses, finishes, or fails.
    pub webhook: String,
}

/// A duration as a flow author writes one: a positive integer with a
/// unit — `"500ms"`, `"90s"`, `"30m"`, `"6h"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlowDuration(Duration);

impl FlowDuration {
    pub fn as_duration(self) -> Duration {
        self.0
    }
}

/// The duration grammar: each unit with its length in milliseconds.
/// The parser, the error message, and the generated schema's pattern
/// all derive from this table and [`DURATION_MAX_DIGITS`], so they
/// cannot disagree.
const DURATION_UNITS: [(&str, u64); 4] = [("ms", 1), ("s", 1_000), ("m", 60_000), ("h", 3_600_000)];

/// Bounds the digit count so no unit conversion can overflow.
const DURATION_MAX_DIGITS: usize = 9;

/// The grammar spelled for humans — embedded in [`DurationError`]'s
/// message and the schema's `FlowDuration` description.
fn duration_grammar() -> String {
    let units: Vec<String> = DURATION_UNITS
        .iter()
        .map(|(unit, _)| format!("`{unit}`"))
        .collect();
    let (last, rest) = units.split_last().expect("at least one unit");
    format!(
        "a whole number (1\u{2013}{DURATION_MAX_DIGITS} digits, no leading zero) with unit {}, or {last}, like \"30m\"",
        rest.join(", ")
    )
}

impl FromStr for FlowDuration {
    type Err = DurationError;

    fn from_str(source: &str) -> Result<Self, Self::Err> {
        let error = || DurationError(source.to_string());
        let unit_start = source
            .find(|c: char| !c.is_ascii_digit())
            .ok_or_else(error)?;
        let (number, unit) = source.split_at(unit_start);
        // Hand-mirrors the `[1-9][0-9]*` prefix of the schema's
        // pattern — the one grammar rule the shared consts don't
        // carry, so the two must change together.
        if number.is_empty() || number.len() > DURATION_MAX_DIGITS || number.starts_with('0') {
            return Err(error());
        }
        let count: u64 = number.parse().map_err(|_| error())?;
        let millis = DURATION_UNITS
            .iter()
            .find(|(name, _)| *name == unit)
            .map(|(_, unit_millis)| count * unit_millis)
            .ok_or_else(error)?;
        Ok(Self(Duration::from_millis(millis)))
    }
}

/// A duration string the flow grammar doesn't accept.
#[derive(Debug, thiserror::Error)]
#[error("invalid duration `{0}`: expected {grammar}", grammar = duration_grammar())]
pub struct DurationError(String);

impl<'de> Deserialize<'de> for FlowDuration {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

#[cfg(feature = "schema")]
pub use schema::json_schema;

/// Schema generation, behind the `schema` feature so product crates
/// never carry schemars — mirroring the `openapi` feature.
#[cfg(feature = "schema")]
mod schema {
    use std::borrow::Cow;

    use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};

    use super::{
        DURATION_MAX_DIGITS, DURATION_UNITS, FlowConfig, FlowDuration, KernelName, PromptsConfig,
        duration_grammar,
    };

    /// Must accept exactly what [`FlowDuration::from_str`] accepts —
    /// the schema is a non-Rust consumer's only knowledge of the
    /// format — so it derives from the same grammar consts the parser
    /// reads.
    fn duration_pattern() -> String {
        let units = DURATION_UNITS.map(|(unit, _)| unit).join("|");
        format!("^[1-9][0-9]{{0,{}}}({units})$", DURATION_MAX_DIGITS - 1)
    }

    /// The flow schema, generated from these types so it can never
    /// disagree with them. Committed at `schemas/flow.schema.json`,
    /// drift-checked in CI, and embedded by the CLI for `hako schema`.
    pub fn json_schema() -> Schema {
        crate::schema::root_schema_for::<FlowConfig>()
    }

    impl JsonSchema for PromptsConfig {
        fn schema_name() -> Cow<'static, str> {
            "PromptsConfig".into()
        }

        /// One optional string property per published slot, closed
        /// like every other table — an unknown slot fails the schema
        /// just as it fails [`FlowConfig::from_toml`]. One property
        /// set serves all kernels, which is exact only while every
        /// kernel publishes the same slots (one kernel today); the
        /// agreement suite's exactness test fails the day a kernel
        /// diverges, naming the needed split into per-kernel
        /// conditionals.
        fn json_schema(_: &mut SchemaGenerator) -> Schema {
            let properties: serde_json::Map<String, serde_json::Value> = KernelName::ALL
                .into_iter()
                .flat_map(KernelName::prompt_slots)
                .map(|slot| ((*slot).to_string(), serde_json::json!({"type": "string"})))
                .collect();
            json_schema!({
                "type": "object",
                "description": prompts_config_doc!(),
                "properties": properties,
                "additionalProperties": false,
            })
        }
    }

    impl JsonSchema for FlowDuration {
        fn schema_name() -> Cow<'static, str> {
            "FlowDuration".into()
        }

        fn json_schema(_: &mut SchemaGenerator) -> Schema {
            json_schema!({
                "type": "string",
                "pattern": duration_pattern(),
                "description": format!("A duration: {}.", duration_grammar()),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    /// The committed example is the spec's representative flow — the
    /// CLI's validate tests drive the same file, so the two suites
    /// cannot drift apart on what a canonical flow looks like.
    const REPRESENTATIVE_FLOW: &str = include_str!("../../../examples/pipeline.toml");

    /// The committed smallest-valid flow — the schema agreement tests
    /// in `xtask/tests/` drive the same file, so the two suites cannot
    /// drift on what "minimal" means.
    const MINIMAL_FLOW: &str = include_str!("../../../examples/minimal.toml");

    #[test]
    fn representative_flow_parses() {
        let flow = FlowConfig::from_toml(REPRESENTATIVE_FLOW).unwrap();
        assert_eq!(flow.r#loop.kernel, KernelName::Pipeline);
        assert_eq!(flow.prompts.get("plan"), Some("prompts/plan.md"));
        assert_eq!(flow.prompts.get("review"), Some("prompts/review.md"));
        assert_eq!(flow.prompts.get("implement"), None);
        assert_eq!(flow.agent.engine, "claude");
        assert_eq!(
            flow.budget,
            BudgetConfig {
                max_iterations: Some(20),
                max_hours: Some(6),
                max_tokens: None,
                iteration_timeout: Some("30m".parse().unwrap()),
            }
        );
        assert_eq!(flow.verify.checks, ["cargo build", "cargo test"]);
        assert_eq!(
            flow.verify.on_fail,
            OnFail {
                retries: 1,
                then: FailAction::Pause,
            }
        );
        assert_eq!(flow.workspace.repo, ".");
        assert_eq!(flow.secrets.env, [SecretName::new("GH_TOKEN")]);
        assert_eq!(flow.notify.unwrap().webhook, "https://ntfy.sh/hako");
    }

    #[test]
    fn a_cmd_flow_carries_its_command_template() {
        let flow = MINIMAL_FLOW.replace(
            "engine = \"claude\"",
            "engine = \"cmd\"\ncommand = [\"aider\", \"--message\", \"{prompt}\"]",
        );
        let flow = FlowConfig::from_toml(&flow).unwrap();
        assert_eq!(flow.agent.engine, "cmd");
        assert_eq!(
            flow.agent.command.unwrap(),
            ["aider", "--message", "{prompt}"]
        );
    }

    #[test]
    fn minimal_flow_leaves_optional_sections_default() {
        let flow = FlowConfig::from_toml(MINIMAL_FLOW).unwrap();
        assert_eq!(flow.prompts, PromptsConfig::default());
        assert_eq!(flow.agent.command, None);
        assert_eq!(flow.budget, BudgetConfig::default());
        assert_eq!(flow.verify, VerifyConfig::default());
        assert_eq!(flow.verify.on_fail.then, FailAction::Pause);
        assert_eq!(flow.workspace.mode, WorkspaceMode::Clone);
        assert_eq!(flow.workspace.branch, None);
        assert!(!flow.workspace.force);
        assert!(flow.secrets.env.is_empty());
        assert_eq!(flow.notify, None);
    }

    #[test]
    fn a_mount_flow_carries_its_mode_and_force() {
        let flow = MINIMAL_FLOW.replace(
            "repo = \".\"",
            "repo = \".\"\nmode = \"mount\"\nforce = true",
        );
        let flow = FlowConfig::from_toml(&flow).unwrap();
        assert_eq!(flow.workspace.mode, WorkspaceMode::Mount);
        assert!(flow.workspace.force);
    }

    #[test]
    fn a_clone_flow_carries_its_seed_branch() {
        let flow = MINIMAL_FLOW.replace("repo = \".\"", "repo = \".\"\nbranch = \"feat/x\"");
        let flow = FlowConfig::from_toml(&flow).unwrap();
        assert_eq!(flow.workspace.mode, WorkspaceMode::Clone);
        assert_eq!(flow.workspace.branch.as_deref(), Some("feat/x"));
    }

    #[test]
    fn a_misspelled_workspace_mode_is_rejected_naming_the_real_ones() {
        let flow = MINIMAL_FLOW.replace("repo = \".\"", "repo = \".\"\nmode = \"mounted\"");
        let err = FlowConfig::from_toml(&flow).unwrap_err().to_string();
        assert!(err.contains("mounted"), "{err}");
        assert!(err.contains("mount"), "{err}");
        assert!(err.contains("clone"), "{err}");
    }

    #[test]
    fn a_misspelled_key_is_rejected_naming_it_and_the_fix() {
        let flow = REPRESENTATIVE_FLOW.replace("max_iterations", "max_iteration");
        let err = FlowConfig::from_toml(&flow).unwrap_err().to_string();
        assert!(err.contains("max_iteration"), "{err}");
        assert!(err.contains("max_iterations"), "{err}");
        assert!(err.contains("line"), "{err}");
    }

    #[test]
    fn an_unknown_table_is_rejected_naming_it() {
        let flow = format!("{REPRESENTATIVE_FLOW}\n[notifyy]\nwebhook = \"x\"\n");
        let err = FlowConfig::from_toml(&flow).unwrap_err().to_string();
        assert!(err.contains("notifyy"), "{err}");
    }

    #[test]
    fn a_misspelled_kernel_is_rejected_naming_the_real_one() {
        let flow = REPRESENTATIVE_FLOW.replace("\"pipeline\"", "\"pypeline\"");
        let err = FlowConfig::from_toml(&flow).unwrap_err().to_string();
        assert!(err.contains("pypeline"), "{err}");
        assert!(err.contains("pipeline"), "{err}");
    }

    #[test]
    fn every_published_slot_validates_for_its_kernel() {
        for kernel in KernelName::ALL {
            let overrides: String = kernel
                .prompt_slots()
                .iter()
                .map(|slot| format!("{slot} = \"prompts/{slot}.md\"\n"))
                .collect();
            let flow = format!("{MINIMAL_FLOW}\n[prompts]\n{overrides}");
            let flow = flow.replace(
                "kernel = \"pipeline\"",
                &format!("kernel = \"{}\"", kernel.as_str()),
            );
            let flow = FlowConfig::from_toml(&flow).unwrap();
            for slot in kernel.prompt_slots() {
                assert_eq!(
                    flow.prompts.get(slot),
                    Some(format!("prompts/{slot}.md").as_str()),
                    "{slot}"
                );
            }
        }
    }

    #[test]
    fn an_unknown_prompt_slot_is_rejected_naming_it_the_kernel_and_the_slots() {
        let flow = format!("{MINIMAL_FLOW}\n[prompts]\nplann = \"prompts/plan.md\"\n");
        let err = FlowConfig::from_toml(&flow).unwrap_err().to_string();
        assert!(err.contains("unknown prompt slot `plann`"), "{err}");
        assert!(err.contains("kernel `pipeline`"), "{err}");
        for slot in KernelName::Pipeline.prompt_slots() {
            assert!(err.contains(&format!("`{slot}`")), "{slot}: {err}");
        }
    }

    /// The reserved namespace for the future per-stage agent override:
    /// nothing may occupy it until that feature lands, so today it
    /// fails like any stray table.
    #[test]
    fn the_reserved_stages_namespace_is_rejected_as_unknown() {
        let flow = format!("{MINIMAL_FLOW}\n[stages.review]\nagent = \"codex\"\n");
        let err = FlowConfig::from_toml(&flow).unwrap_err().to_string();
        assert!(err.contains("stages"), "{err}");
    }

    /// Pins `ALL` the same way the stage-order test pins `Stage::ALL`:
    /// the exhaustive match forces every variant to declare its
    /// ordinal, so an insertion that forgets `ALL` fails here; a
    /// forgotten append only review can catch.
    #[test]
    fn all_lists_every_kernel() {
        fn ordinal(kernel: KernelName) -> usize {
            match kernel {
                KernelName::Pipeline => 0,
            }
        }
        for (index, kernel) in KernelName::ALL.into_iter().enumerate() {
            assert_eq!(ordinal(kernel), index, "{kernel:?}");
        }
    }

    #[test]
    fn the_kernel_name_matches_its_wire_string() {
        let parsed: KernelName = serde_json::from_value(json!("pipeline")).unwrap();
        assert_eq!(parsed, KernelName::Pipeline);
        assert_eq!(KernelName::Pipeline.as_str(), "pipeline");
    }

    #[test]
    fn a_flow_missing_a_required_section_is_rejected_naming_it() {
        let flow = MINIMAL_FLOW.replace("[workspace]\nrepo = \".\"", "");
        let err = FlowConfig::from_toml(&flow).unwrap_err().to_string();
        assert!(err.contains("workspace"), "{err}");
    }

    #[test]
    fn durations_parse_by_unit() {
        for (text, expected) in [
            ("500ms", Duration::from_millis(500)),
            ("90s", Duration::from_secs(90)),
            ("30m", Duration::from_secs(30 * 60)),
            ("6h", Duration::from_secs(6 * 3600)),
            ("999999999h", Duration::from_secs(999_999_999 * 3600)),
        ] {
            let parsed: FlowDuration = text.parse().unwrap();
            assert_eq!(parsed.as_duration(), expected, "{text}");
        }
    }

    #[test]
    fn malformed_durations_are_rejected() {
        for text in [
            "",
            "30",
            "0m",
            "030m",
            "1000000000s",
            "m",
            "1.5h",
            "30 m",
            "-5m",
            "5d",
        ] {
            assert!(text.parse::<FlowDuration>().is_err(), "{text}");
        }
    }
}
