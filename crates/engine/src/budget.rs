//! Budgets — soft caps on a run. Exhaustion finishes the current
//! iteration and pauses resumably; a budget never fails a run.

use std::time::Duration;

use proto::flow::{BudgetConfig, FlowDuration};

pub use proto::budget::{BudgetKind, TokenUsage};

/// The cap on one iteration when the flow leaves it unset.
const DEFAULT_ITERATION_TIMEOUT: Duration = Duration::from_secs(30 * 60);

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
            iteration_timeout: DEFAULT_ITERATION_TIMEOUT,
        }
    }
}

/// Lowers a flow's authored caps onto the engine's budgets — the
/// conversion at the proto/engine edge. Everything left unset keeps
/// the default.
impl From<&BudgetConfig> for Budgets {
    fn from(config: &BudgetConfig) -> Self {
        Self {
            max_iterations: config.max_iterations,
            max_wall_clock: config
                .max_hours
                .map(|hours| Duration::from_secs(u64::from(hours) * 3600)),
            max_tokens: config.max_tokens,
            iteration_timeout: config
                .iteration_timeout
                .map_or(DEFAULT_ITERATION_TIMEOUT, FlowDuration::as_duration),
        }
    }
}

#[cfg(test)]
mod tests {
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
    fn authored_budgets_lower_onto_engine_budgets() {
        let flow =
            proto::flow::FlowConfig::from_toml(include_str!("../../../examples/pipeline.toml"))
                .unwrap();
        let budgets = Budgets::from(&flow.budget);
        assert_eq!(budgets.max_iterations, Some(20));
        assert_eq!(budgets.max_wall_clock, Some(Duration::from_secs(6 * 3600)));
        assert_eq!(budgets.max_tokens, None);
        assert_eq!(budgets.iteration_timeout, Duration::from_secs(30 * 60));
    }

    #[test]
    fn an_unset_budget_section_lowers_to_the_defaults() {
        assert_eq!(Budgets::from(&BudgetConfig::default()), Budgets::default());
    }
}
