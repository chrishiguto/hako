//! The fanout kernel's dialect: one plan report naming independent,
//! opaque units of work. Their contents belong to the domain prompt;
//! the engine only scopes one child pipeline run to each string.

use serde::{Deserialize, Serialize};

use crate::report::{Question, ReportStatus};

/// The sole prompt slot published by the fanout kernel.
pub const PROMPT_SLOTS: [&str; 1] = ["plan"];

/// What a fanout planning pass leaves behind.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct PlanReport {
    pub status: ReportStatus,
    /// What the planning pass found and why these units may proceed
    /// independently.
    pub summary: String,
    /// Independent work units. Each string is passed verbatim to one
    /// child pipeline; tracker and frontier semantics live here, not
    /// in the engine.
    #[serde(default)]
    pub units: Vec<String>,
    /// What prevents the planner from finding ready work.
    #[serde(default)]
    pub blockers: Vec<String>,
    /// Questions only a human can answer.
    #[serde(default)]
    pub questions: Vec<Question>,
}

#[cfg(feature = "schema")]
pub fn plan_schema() -> schemars::Schema {
    crate::schema::root_schema_for::<PlanReport>()
}
