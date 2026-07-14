//! Flow config — the ~20-line TOML that parameterizes a kernel.
//!
//! Deserialization is strict: every table rejects unknown keys, so a
//! typo fails at validation time pointing at the offending key, not at
//! iteration 4. The committed `schemas/flow.schema.json` is generated
//! from these same types ([`json_schema`] via `cargo xtask schema`),
//! which is how editors and the schema-embedding CLI validate flows
//! without linking the engine.

use std::borrow::Cow;
use std::str::FromStr;
use std::time::Duration;

use schemars::generate::SchemaSettings;
use schemars::transform::{Transform, transform_subschemas};
use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::Deserialize;

use crate::budget::Budgets;
use crate::secrets::SecretName;

/// A parsed flow: what to achieve, with which agent, under which
/// limits. Contains no logic — control flow belongs to the kernel
/// (ADR 0001).
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FlowConfig {
    pub r#loop: LoopConfig,
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
    /// Strictly parses flow TOML; the error carries the offending
    /// line and key.
    pub fn from_toml(source: &str) -> Result<Self, FlowError> {
        Ok(toml::from_str(source)?)
    }
}

/// A flow that failed strict parsing. Displays the TOML error
/// verbatim — it already points at the offending line and key.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct FlowError(#[from] toml::de::Error);

/// Which kernel runs the loop and what it is trying to achieve.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct LoopConfig {
    pub kernel: KernelName,
    /// What the loop is trying to achieve, passed verbatim to the
    /// kernel.
    pub goal: String,
}

/// The loop patterns the engine ships. A closed set by design: a new
/// loop shape is a new kernel in Rust, never logic in the flow file
/// (ADR 0001) — which is what lets the schema reject a misspelled
/// kernel outright.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum KernelName {
    Ralph,
}

/// Which agent drives the iterations.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AgentConfig {
    /// An agent adapter name, e.g. `claude`. An open set — adapters
    /// are registered at runtime — so unlike `kernel` it is not an
    /// enum.
    pub engine: String,
}

/// The caps a flow puts on one run. Everything left unset keeps the
/// engine default ([`Budgets::default`]).
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize, JsonSchema)]
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

impl BudgetConfig {
    /// Lowers the authored caps onto the engine's [`Budgets`].
    pub fn budgets(&self) -> Budgets {
        Budgets {
            max_iterations: self.max_iterations,
            max_wall_clock: self
                .max_hours
                .map(|hours| Duration::from_secs(u64::from(hours) * 3600)),
            max_tokens: self.max_tokens,
            iteration_timeout: self.iteration_timeout.map_or(
                Budgets::default().iteration_timeout,
                FlowDuration::as_duration,
            ),
        }
    }
}

/// The verify checks an iteration must pass to count as progress. No
/// checks means every iteration counts.
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize, JsonSchema)]
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
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize, JsonSchema)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum FailAction {
    #[default]
    Pause,
    Fail,
}

/// The one thing that survives iterations — the repo the loop works
/// on.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceConfig {
    /// Path to the repository the workspace is seeded from.
    pub repo: String,
}

/// Secret *names* only — values live in the daemon's store, so flow
/// files stay safe to commit and to hand to an LLM.
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SecretsConfig {
    /// Secrets injected into the sandbox as environment variables, by
    /// name.
    #[serde(default)]
    pub env: Vec<SecretName>,
}

/// Where the daemon pushes run lifecycle notifications.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NotifyConfig {
    /// URL POSTed to when the run pauses, finishes, or fails.
    pub webhook: String,
}

/// A duration as a flow author writes one: a positive integer with a
/// unit — `"500ms"`, `"90s"`, `"30m"`, `"6h"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlowDuration(Duration);

/// Must accept exactly what [`FlowDuration::from_str`] accepts — the
/// schema is the CLI's only knowledge of the format. The agreement is
/// pinned by `tests/flow_schema_agreement.rs`. Nine digits bounds the
/// count so no unit conversion can overflow.
const DURATION_PATTERN: &str = "^[1-9][0-9]{0,8}(ms|s|m|h)$";

impl FlowDuration {
    pub fn as_duration(self) -> Duration {
        self.0
    }
}

impl FromStr for FlowDuration {
    type Err = DurationError;

    fn from_str(source: &str) -> Result<Self, Self::Err> {
        let error = || DurationError(source.to_string());
        let unit_start = source
            .find(|c: char| !c.is_ascii_digit())
            .ok_or_else(error)?;
        let (number, unit) = source.split_at(unit_start);
        // Mirrors DURATION_PATTERN exactly: 1–9 digits, no leading
        // zero — small enough that no unit conversion can overflow.
        if number.is_empty() || number.len() > 9 || number.starts_with('0') {
            return Err(error());
        }
        let count: u64 = number.parse().map_err(|_| error())?;
        let duration = match unit {
            "ms" => Duration::from_millis(count),
            "s" => Duration::from_secs(count),
            "m" => Duration::from_secs(count * 60),
            "h" => Duration::from_secs(count * 3600),
            _ => return Err(error()),
        };
        Ok(Self(duration))
    }
}

/// A duration string the flow grammar doesn't accept.
#[derive(Debug, thiserror::Error)]
#[error(
    "invalid duration `{0}`: expected a whole number (1\u{2013}9 digits, no leading zero) with unit `ms`, `s`, `m`, or `h`, like \"30m\""
)]
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

impl JsonSchema for FlowDuration {
    fn schema_name() -> Cow<'static, str> {
        "FlowDuration".into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "string",
            "pattern": DURATION_PATTERN,
            "description": "A duration: a whole number (1\u{2013}9 digits, no leading zero) with unit `ms`, `s`, `m`, or `h`, like \"30m\".",
        })
    }
}

/// The flow schema, generated from these types so it can never
/// disagree with them. Committed at `schemas/flow.schema.json` and
/// embedded by the CLI at build time.
pub fn json_schema() -> Schema {
    SchemaSettings::default()
        .with_transform(BoundIntegerFormats)
        .into_generator()
        .into_root_schema_for::<FlowConfig>()
}

/// Stamps explicit bounds onto every sub-64-bit integer in the schema.
/// schemars emits only `minimum: 0` for them, and JSON Schema treats
/// `format` as an annotation — without real bounds the schema would
/// bless out-of-range values strict serde rejects. The 64-bit formats
/// need no bounds: TOML integers are i64, so theirs are unreachable
/// from a flow file. A format this table misses fails the
/// `every_integer_format_in_the_schema_carries_bounds` test.
#[derive(Debug, Clone)]
struct BoundIntegerFormats;

impl Transform for BoundIntegerFormats {
    fn transform(&mut self, schema: &mut Schema) {
        if let Some(format) = schema.get("format").and_then(serde_json::Value::as_str) {
            let bounds = match format {
                "uint32" => Some((serde_json::json!(0), serde_json::json!(u32::MAX))),
                "int32" => Some((serde_json::json!(i32::MIN), serde_json::json!(i32::MAX))),
                _ => None,
            };
            if let Some((minimum, maximum)) = bounds {
                schema.insert("minimum".into(), minimum);
                schema.insert("maximum".into(), maximum);
            }
        }
        transform_subschemas(self, schema);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The committed example is the spec's representative flow — the
    /// CLI's validate tests drive the same file, so the two suites
    /// cannot drift apart on what a canonical flow looks like.
    const REPRESENTATIVE_FLOW: &str = include_str!("../../../examples/ralph.toml");

    const MINIMAL_FLOW: &str = r#"
[loop]
kernel = "ralph"
goal = "Fix the flaky test"

[agent]
engine = "claude"

[workspace]
repo = "."
"#;

    #[test]
    fn representative_flow_parses() {
        let flow = FlowConfig::from_toml(REPRESENTATIVE_FLOW).unwrap();
        assert_eq!(flow.r#loop.kernel, KernelName::Ralph);
        assert_eq!(flow.r#loop.goal, "Implement all open GitHub issues");
        assert_eq!(flow.agent.engine, "claude");
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
    fn authored_budgets_lower_onto_engine_budgets() {
        let flow = FlowConfig::from_toml(REPRESENTATIVE_FLOW).unwrap();
        let budgets = flow.budget.budgets();
        assert_eq!(budgets.max_iterations, Some(20));
        assert_eq!(budgets.max_wall_clock, Some(Duration::from_secs(6 * 3600)));
        assert_eq!(budgets.max_tokens, None);
        assert_eq!(budgets.iteration_timeout, Duration::from_secs(30 * 60));
    }

    #[test]
    fn minimal_flow_gets_engine_defaults() {
        let flow = FlowConfig::from_toml(MINIMAL_FLOW).unwrap();
        assert_eq!(flow.budget.budgets(), Budgets::default());
        assert_eq!(flow.verify, VerifyConfig::default());
        assert_eq!(flow.verify.on_fail.then, FailAction::Pause);
        assert!(flow.secrets.env.is_empty());
        assert_eq!(flow.notify, None);
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
        let flow = REPRESENTATIVE_FLOW.replace("\"ralph\"", "\"ralf\"");
        let err = FlowConfig::from_toml(&flow).unwrap_err().to_string();
        assert!(err.contains("ralf"), "{err}");
        assert!(err.contains("ralph"), "{err}");
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

    /// The schema must carry the same strictness as the serde types:
    /// every object in it rejects unknown keys, or the CLI would bless
    /// flows the daemon rejects.
    #[test]
    fn every_object_in_the_schema_rejects_unknown_keys() {
        let schema = serde_json::to_value(json_schema()).unwrap();
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
    /// explicit bounds [`BoundIntegerFormats`] stamps — a config field
    /// with an integer type the transform doesn't know fails here
    /// instead of becoming a hole the schema-validating CLI would
    /// bless. The 64-bit formats are exempt: TOML integers are i64,
    /// so their bounds are unreachable from a flow file.
    #[test]
    fn every_integer_format_in_the_schema_carries_bounds() {
        fn walk(node: &serde_json::Value, path: &str, found: &mut u32) {
            match node {
                serde_json::Value::Object(entries) => {
                    let sub_64_bit =
                        |f: &&str| f.contains("int") && *f != "uint64" && *f != "int64";
                    let format = entries.get("format").and_then(serde_json::Value::as_str);
                    if let Some(format) = format.filter(sub_64_bit) {
                        *found += 1;
                        for bound in ["minimum", "maximum"] {
                            assert!(
                                entries.contains_key(bound),
                                "{path}: format `{format}` lacks `{bound}` — \
                                 teach BoundIntegerFormats this format or use a bounded type"
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
        let schema = serde_json::to_value(json_schema()).unwrap();
        let mut found = 0;
        walk(&schema, "", &mut found);
        assert!(found > 0, "the walk matched no integer formats");
    }

    #[test]
    fn the_schema_names_every_flow_section() {
        let schema = serde_json::to_value(json_schema()).unwrap();
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
}
