use proto::pipeline::StageReport;

pub(super) fn last_summary(pass: &[StageReport]) -> &str {
    pass.last()
        .map_or("budget exhausted", |report| report.summary())
}
