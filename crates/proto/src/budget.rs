//! The wire-visible budget vocabulary. The caps themselves are engine
//! configuration, not wire types — they live with the engine.

use serde::{Deserialize, Serialize};

/// Which budget ran out — names the pause for events and notifications.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum BudgetKind {
    Iterations,
    WallClock,
    Tokens,
}

/// New caps granted when a paused run resumes. Integer seconds rather
/// than fractional hours: a float on a frozen wire admits negatives
/// and precision junk that every consumer would have to police
/// forever. Absent fields keep their current cap.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct BudgetExtension {
    pub max_iterations: Option<u32>,
    pub max_wall_clock_seconds: Option<u64>,
    pub max_tokens: Option<u64>,
}

/// Tokens one agent invocation consumed, as reported by its adapter.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct TokenUsage {
    pub input: u64,
    pub output: u64,
}

impl std::ops::AddAssign for TokenUsage {
    /// Saturating on both counters: budget arithmetic never wraps.
    fn add_assign(&mut self, other: Self) {
        self.input = self.input.saturating_add(other.input);
        self.output = self.output.saturating_add(other.output);
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn budget_kinds_are_snake_case_on_the_wire() {
        let kinds = [
            (BudgetKind::Iterations, "iterations"),
            (BudgetKind::WallClock, "wall_clock"),
            (BudgetKind::Tokens, "tokens"),
        ];
        for (kind, wire) in kinds {
            assert_eq!(serde_json::to_value(kind).unwrap(), json!(wire));
        }
    }

    #[test]
    fn token_usage_round_trips() {
        let usage = TokenUsage {
            input: 1200,
            output: 340,
        };
        let wire = serde_json::to_string(&usage).unwrap();
        assert_eq!(serde_json::from_str::<TokenUsage>(&wire).unwrap(), usage);
    }
}
