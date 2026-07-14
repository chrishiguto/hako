//! Budgets — soft caps on a run. Exhaustion finishes the current
//! iteration and pauses resumably; a budget never fails a run.

use std::time::Duration;

pub use proto::budget::{BudgetKind, TokenUsage};

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
}
