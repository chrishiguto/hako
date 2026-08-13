use crate::invocation::ReportContract;
use proto::fanout::PlanReport;

pub(super) fn default_prompt() -> &'static str {
    include_str!("prompts/plan.md")
}

pub(super) fn report_schema() -> &'static str {
    include_str!("../../../../schemas/report/fanout/plan.schema.json")
}

pub(super) struct PlanContract;

impl ReportContract for PlanContract {
    type Report = PlanReport;

    fn schema(&self) -> &str {
        report_schema()
    }

    fn parse(&self, text: &str) -> Result<Self::Report, String> {
        serde_json::from_str(text).map_err(|error| error.to_string())
    }
}
