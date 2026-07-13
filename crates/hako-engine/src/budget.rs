//! Budgets — soft caps on a run. Exhaustion finishes the current
//! iteration and pauses resumably; a budget never fails a run.

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// The caps a flow sets on one run. `None` means uncapped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Budgets {
    pub max_iterations: Option<u32>,
    pub max_wall_clock: Option<Duration>,
    /// Counted only where the agent adapter can report usage; agents
    /// that report nothing simply aren't token-budgeted.
    pub max_tokens: Option<u64>,
    /// Not a soft cap: on expiry the sandbox is destroyed and the
    /// iteration counts as failed, so a hung agent can never stall the
    /// loop silently.
    pub iteration_timeout: Duration,
}

impl Default for Budgets {
    fn default() -> Self {
        Self {
            max_iterations: None,
            max_wall_clock: None,
            max_tokens: None,
            iteration_timeout: Duration::from_secs(30 * 60),
        }
    }
}

/// Which budget ran out — names the pause for events and notifications.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BudgetKind {
    Iterations,
    WallClock,
    Tokens,
}

/// Tokens one agent invocation consumed, as reported by its adapter.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenUsage {
    pub input: u64,
    pub output: u64,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn default_budgets_cap_nothing_but_the_iteration() {
        let budgets = Budgets::default();
        assert_eq!(budgets.max_iterations, None);
        assert_eq!(budgets.max_wall_clock, None);
        assert_eq!(budgets.max_tokens, None);
        assert_eq!(budgets.iteration_timeout, Duration::from_secs(30 * 60));
    }

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
