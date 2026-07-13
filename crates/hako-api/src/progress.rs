//! The progress report as it appears in streamed events and run
//! status. Mirrors the engine's shape; deliberately lenient about
//! unknown fields so older clients survive newer daemons.

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// What the agent declared at the end of an iteration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ProgressReport {
    pub status: ProgressStatus,
    /// What happened this iteration, in the agent's words.
    pub summary: String,
    #[serde(default)]
    pub remaining: Vec<String>,
    #[serde(default)]
    pub blockers: Vec<String>,
    /// Present when the status is `needs_input`.
    #[serde(default)]
    pub questions: Vec<Question>,
}

/// The agent's claim about where the loop stands. `done` is a claim
/// the engine independently verifies, never a verdict.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ProgressStatus {
    Continue,
    Done,
    Blocked,
    NeedsInput,
}

/// A question awaiting a human answer (`hako answer`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct Question {
    /// The handle an answer is addressed to.
    pub id: String,
    pub text: String,
    /// Suggested answers; free-text is always allowed.
    #[serde(default)]
    pub options: Vec<String>,
}
