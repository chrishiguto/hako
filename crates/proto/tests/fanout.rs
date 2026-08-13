use proto::fanout::PlanReport;
use proto::report::ReportStatus;

#[test]
fn a_plan_report_names_opaque_independent_units() {
    let report: PlanReport = serde_json::from_str(
        r#"{
            "status": "continue",
            "summary": "two independent slices are ready",
            "units": ["issue #31: api", "issue #32: cli"]
        }"#,
    )
    .unwrap();

    assert_eq!(report.status, ReportStatus::Continue);
    assert_eq!(report.units, ["issue #31: api", "issue #32: cli"]);
}

#[test]
fn a_plan_report_rejects_unknown_fields() {
    let error = serde_json::from_str::<PlanReport>(
        r#"{
            "status": "continue",
            "summary": "ready",
            "units": [],
            "frontier": [31]
        }"#,
    )
    .unwrap_err();

    assert!(error.to_string().contains("unknown field"), "{error}");
}
