use std::time::Duration;

use crate::kernel::KernelContext;
use proto::BudgetKind;
use proto::pipeline::StageReport;

const DRIFT_LIMIT: u32 = 3;

#[derive(Default)]
pub(super) struct Guardrails {
    no_commit_iterations: u32,
    timeout_failures: u32,
    summary: String,
}

impl Guardrails {
    pub(super) fn summary(&self) -> &str {
        if self.summary.is_empty() {
            "budget exhausted"
        } else {
            &self.summary
        }
    }

    pub(super) fn resumed_budget(&self, ctx: &KernelContext, iteration: u32) -> Option<BudgetKind> {
        ctx.budgets
            .max_iterations
            .filter(|&max| iteration > max)
            .map(|_| BudgetKind::Iterations)
    }

    pub(super) fn completed(
        &mut self,
        ctx: &KernelContext,
        iteration: u32,
        pass: &[StageReport],
        committed: bool,
    ) -> Option<BudgetKind> {
        self.timeout_failures = 0;
        self.summary = pass.last().map_or_else(
            || "iteration completed".into(),
            |report| report.summary().into(),
        );

        let exhausted = ctx
            .budgets
            .max_iterations
            .filter(|&max| iteration >= max)
            .map(|_| BudgetKind::Iterations)
            .or_else(|| {
                ctx.budgets
                    .max_wall_clock
                    .filter(|&max| ctx.budget_usage.elapsed() >= max)
                    .map(|_| BudgetKind::WallClock)
            })
            .or_else(
                || match (ctx.budget_usage.tokens(), ctx.budgets.max_tokens) {
                    (Some(used), Some(max)) if used >= max => Some(BudgetKind::Tokens),
                    _ => None,
                },
            );
        if exhausted.is_some() {
            return exhausted;
        }

        self.no_commit_iterations = if committed {
            0
        } else {
            self.no_commit_iterations.saturating_add(1)
        };
        None
    }

    pub(super) fn drifted(&self) -> bool {
        self.no_commit_iterations >= DRIFT_LIMIT
    }

    pub(super) fn timed_out(&mut self, iteration: u32, timeout: Duration) {
        self.timeout_failures += 1;
        self.summary = format!(
            "iteration {iteration} timed out after {} seconds",
            timeout.as_secs()
        );
    }

    pub(super) fn timeout_failures(&self) -> u32 {
        self.timeout_failures
    }
}
