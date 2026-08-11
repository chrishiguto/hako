use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use api::{ApiError, ErrorCode, ListRunsResponse, RunStatusResponse, SubmitRunResponse};
use axum::Router;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use engine::testkit::{MapSecrets, NoSecrets, ScriptedSandbox, StubNotifier, seeded_repo};
use engine::{EventSink, ExecEvent, ExitStatus, RunDir, RunEvent, RunId, SecretsProvider};
use http_body_util::BodyExt;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use server::{Daemon, DaemonConfig, EngineRuntime, SERVED_ROUTES};
use tokio::sync::Barrier;
use tower::ServiceExt;

const TOKEN: &str = "test-bearer-token";

/// A sandbox whose every exec succeeds silently and "writes" the given
/// report — the shortest path from a submitted flow to a finished run.
fn fake_sandbox(report: Value, barrier: Option<Arc<Barrier>>) -> ScriptedSandbox {
    let mut sandbox =
        ScriptedSandbox::repeating(vec![Ok(ExecEvent::Exited(ExitStatus { code: Some(0) }))]);
    if let Some(barrier) = barrier {
        sandbox = sandbox.with_barrier(barrier);
    }
    sandbox.write_report_on_exec(serde_json::to_vec(&report).unwrap());
    if report["status"].as_str() == Some("done") {
        sandbox.write_report_when_argv_contains(
            "# hako pipeline — skeptic iteration",
            br#"{"refuted": false, "findings": []}"#,
        );
    }
    sandbox
}

struct TestHost {
    runs: tempfile::TempDir,
    repo: tempfile::TempDir,
    app: Router,
    sandbox: Arc<ScriptedSandbox>,
}

impl TestHost {
    async fn new(report: Value) -> Self {
        Self::with_barrier(report, None).await
    }

    async fn with_barrier(report: Value, barrier: Option<Arc<Barrier>>) -> Self {
        let runs = tempfile::tempdir().unwrap();
        let repo = seeded_repo();
        let sandbox = Arc::new(fake_sandbox(report, barrier));
        Self::with_parts(runs, repo, sandbox).await
    }

    async fn with_parts(
        runs: tempfile::TempDir,
        repo: tempfile::TempDir,
        sandbox: Arc<ScriptedSandbox>,
    ) -> Self {
        Self::with_secrets(runs, repo, sandbox, Arc::new(NoSecrets)).await
    }

    /// A host over a daemon that holds these secrets — the only part
    /// of a submission's fate that lives outside the flow file.
    async fn with_secrets(
        runs: tempfile::TempDir,
        repo: tempfile::TempDir,
        sandbox: Arc<ScriptedSandbox>,
        secrets: Arc<dyn SecretsProvider>,
    ) -> Self {
        let runtime = Arc::new(EngineRuntime::new(
            sandbox.clone(),
            Arc::new(StubNotifier),
            secrets,
        ));
        let daemon = Daemon::load(DaemonConfig::new(TOKEN, runs.path()), runtime)
            .await
            .unwrap();
        Self {
            runs,
            repo,
            app: daemon.router(),
            sandbox,
        }
    }

    fn flow(&self) -> String {
        flow_for(self.repo.path())
    }

    async fn submit(&self) -> SubmitRunResponse {
        let response = request(
            &self.app,
            Method::POST,
            "/v1/runs",
            Some(TOKEN),
            Some(json!({"flow": self.flow()})),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CREATED);
        body(response).await
    }

    async fn wait_for_state(&self, run_id: &str, expected: &str) -> RunStatusResponse {
        wait_for_state(&self.app, run_id, expected).await
    }
}

#[tokio::test]
async fn every_served_endpoint_requires_the_configured_bearer_token() {
    let host = TestHost::new(done_report()).await;
    for route in SERVED_ROUTES {
        let path = route.path.replace("{run_id}", "missing");
        for token in [None, Some("wrong-token")] {
            let response = request(
                &host.app,
                route.method.clone(),
                &path,
                token,
                route
                    .method
                    .eq(&Method::POST)
                    .then(|| json!({"flow": host.flow()})),
            )
            .await;
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{route:?}");
            assert_eq!(
                response.headers().get(header::WWW_AUTHENTICATE),
                Some(&header::HeaderValue::from_static("Bearer")),
                "{route:?}"
            );
            let error: ApiError = body(response).await;
            assert_eq!(error.code, ErrorCode::Unauthorized);
        }
    }

    let response = request(&host.app, Method::GET, "/v1/runs", Some(TOKEN), None).await;
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn bearer_scheme_is_case_insensitive_and_allows_extra_whitespace() {
    let host = TestHost::new(done_report()).await;
    for authorization in [format!("bearer {TOKEN}"), format!("Bearer   {TOKEN}")] {
        let response = raw_request(&host.app, Method::GET, "/v1/runs", Some(&authorization)).await;
        assert_eq!(response.status(), StatusCode::OK, "{authorization}");
    }
}

#[tokio::test]
async fn malformed_authorization_is_a_structured_unauthorized_response() {
    let host = TestHost::new(done_report()).await;
    for authorization in ["Basic credentials", "Bearer", "Bearer "] {
        let response = raw_request(&host.app, Method::GET, "/v1/runs", Some(authorization)).await;
        assert_eq!(
            response.status(),
            StatusCode::UNAUTHORIZED,
            "{authorization}"
        );
        assert_eq!(
            response.headers().get(header::WWW_AUTHENTICATE),
            Some(&header::HeaderValue::from_static("Bearer"))
        );
        let error: ApiError = body(response).await;
        assert_eq!(error.code, ErrorCode::Unauthorized);
    }
}

#[tokio::test]
async fn authentication_does_not_hide_unknown_routes() {
    let host = TestHost::new(done_report()).await;
    let response = raw_request(&host.app, Method::GET, "/missing", None).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn submit_rejects_invalid_flows_and_starts_valid_ones_detached() {
    let host = TestHost::new(done_report()).await;
    let invalid = request(
        &host.app,
        Method::POST,
        "/v1/runs",
        Some(TOKEN),
        Some(json!({"flow": "[loop]\nkernel = \"typo\""})),
    )
    .await;
    assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
    let error: ApiError = body(invalid).await;
    assert_eq!(error.code, ErrorCode::InvalidFlow);
    assert!(error.message.contains("typo"), "{}", error.message);

    let submitted = host.submit().await;
    assert!(!submitted.run_id.is_empty());
    let status = host.wait_for_state(&submitted.run_id, "done").await;
    assert_eq!(status.run.run_id, submitted.run_id);
    assert_eq!(
        status.last_summary.as_deref(),
        Some("finished from the fake")
    );
}

#[tokio::test]
async fn submit_distinguishes_well_formed_flows_the_engine_cannot_run() {
    let host = TestHost::new(done_report()).await;
    let flow = host
        .flow()
        .replace("engine = \"cmd\"", "engine = \"missing\"");
    let response = request(
        &host.app,
        Method::POST,
        "/v1/runs",
        Some(TOKEN),
        Some(json!({"flow": flow})),
    )
    .await;
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let error: ApiError = body(response).await;
    assert_eq!(error.code, ErrorCode::InvalidAgent);
    assert!(error.message.contains("missing"));
}

/// A malformed secret name is a flow error, not a provisioning gap:
/// answering `missing_secret` would send the operator to provision a
/// file the store can never address.
#[tokio::test]
async fn a_malformed_secret_name_fails_the_flow_not_the_provisioning() {
    let host = TestHost::new(done_report()).await;
    let flow = format!("{}\n[secrets]\nenv = [\"GH-TOKEN\"]\n", host.flow());
    let response = request(
        &host.app,
        Method::POST,
        "/v1/runs",
        Some(TOKEN),
        Some(json!({"flow": flow})),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let error: ApiError = body(response).await;
    assert_eq!(error.code, ErrorCode::InvalidFlow);
    assert!(error.message.contains("GH-TOKEN"), "{}", error.message);
}

/// A run that cannot get its secrets must not start: the submission
/// itself fails, naming what to provision, because that is the moment
/// a human is there to fix it. Provision the secret and the same flow
/// runs — with the value in every sandbox it boots.
#[tokio::test]
async fn submit_fails_naming_an_unprovisioned_secret_and_succeeds_once_it_exists() {
    let flow_with_secrets =
        |host: &TestHost| format!("{}\n[secrets]\nenv = [\"GH_TOKEN\"]\n", host.flow());

    let host = TestHost::new(done_report()).await;
    let response = request(
        &host.app,
        Method::POST,
        "/v1/runs",
        Some(TOKEN),
        Some(json!({"flow": flow_with_secrets(&host)})),
    )
    .await;
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let error: ApiError = body(response).await;
    assert_eq!(error.code, ErrorCode::MissingSecret);
    assert!(error.message.contains("GH_TOKEN"), "{}", error.message);

    let sandbox = Arc::new(fake_sandbox(done_report(), None));
    let host = TestHost::with_secrets(
        tempfile::tempdir().unwrap(),
        seeded_repo(),
        sandbox.clone(),
        Arc::new(MapSecrets::new([("GH_TOKEN", "ghp_provisioned")])),
    )
    .await;
    let response = request(
        &host.app,
        Method::POST,
        "/v1/runs",
        Some(TOKEN),
        Some(json!({"flow": flow_with_secrets(&host)})),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let submitted: SubmitRunResponse = body(response).await;
    host.wait_for_state(&submitted.run_id, "done").await;

    // The value never crossed the wire, but it did reach the machine
    // the agent ran in.
    let specs = sandbox.specs();
    assert!(!specs.is_empty());
    for spec in specs {
        assert_eq!(spec.env["GH_TOKEN"].expose(), "ghp_provisioned");
    }
}

/// The claude adapter takes either credential, so a daemon holding
/// just one of them can run the flow — and a daemon holding neither is
/// told both names it could provision.
#[tokio::test]
async fn an_adapter_requirement_is_satisfied_by_any_one_of_its_alternatives() {
    let claude_flow = |host: &TestHost| {
        host.flow().replace(
            "engine = \"cmd\"\ncommand = [\"fake-agent\", \"{prompt}\"]",
            "engine = \"claude\"",
        )
    };

    let host = TestHost::new(done_report()).await;
    let response = request(
        &host.app,
        Method::POST,
        "/v1/runs",
        Some(TOKEN),
        Some(json!({"flow": claude_flow(&host)})),
    )
    .await;
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let error: ApiError = body(response).await;
    assert_eq!(error.code, ErrorCode::MissingSecret);
    for name in ["ANTHROPIC_API_KEY", "CLAUDE_CODE_OAUTH_TOKEN"] {
        assert!(error.message.contains(name), "{}", error.message);
    }

    let sandbox = Arc::new(fake_sandbox(done_report(), None));
    let host = TestHost::with_secrets(
        tempfile::tempdir().unwrap(),
        seeded_repo(),
        sandbox.clone(),
        // Only the OAuth token: the alternative, not the primary.
        Arc::new(MapSecrets::new([("CLAUDE_CODE_OAUTH_TOKEN", "oauth")])),
    )
    .await;
    let response = request(
        &host.app,
        Method::POST,
        "/v1/runs",
        Some(TOKEN),
        Some(json!({"flow": claude_flow(&host)})),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let submitted: SubmitRunResponse = body(response).await;
    host.wait_for_state(&submitted.run_id, "done").await;
    assert_eq!(
        sandbox.specs()[0].env["CLAUDE_CODE_OAUTH_TOKEN"].expose(),
        "oauth"
    );
}

/// An agent that writes its credential into its own report cannot
/// poison the run's record: what the daemon serves — and what the log
/// behind it holds — is redacted.
#[tokio::test]
async fn a_secret_the_agent_reported_is_scrubbed_out_of_the_runs_record() {
    let report = json!({"status": "done", "summary": "pushed with ghp_provisioned"});
    let host = TestHost::with_secrets(
        tempfile::tempdir().unwrap(),
        seeded_repo(),
        Arc::new(fake_sandbox(report, None)),
        Arc::new(MapSecrets::new([("GH_TOKEN", "ghp_provisioned")])),
    )
    .await;
    let response = request(
        &host.app,
        Method::POST,
        "/v1/runs",
        Some(TOKEN),
        Some(json!({"flow": format!("{}\n[secrets]\nenv = [\"GH_TOKEN\"]\n", host.flow())})),
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let submitted: SubmitRunResponse = body(response).await;

    let status = host.wait_for_state(&submitted.run_id, "done").await;
    assert_eq!(
        status.last_summary.as_deref(),
        Some("pushed with [redacted secret]")
    );
}

#[tokio::test]
async fn a_panicking_execution_is_recorded_as_failed() {
    let runs = tempfile::tempdir().unwrap();
    let repo = seeded_repo();
    let sandbox = Arc::new(fake_sandbox(done_report(), None).panicking());
    let host = TestHost::with_parts(runs, repo, sandbox).await;
    let submitted = host.submit().await;
    host.wait_for_state(&submitted.run_id, "failed").await;
}

#[tokio::test]
async fn list_and_status_expose_pause_reasons_summaries_and_questions() {
    let host = TestHost::new(needs_input_report()).await;
    let submitted = host.submit().await;
    let status = host.wait_for_state(&submitted.run_id, "paused").await;
    assert_eq!(
        serde_json::to_value(status.run.state).unwrap()["reason"],
        "awaiting_human"
    );
    assert_eq!(status.last_summary.as_deref(), Some("need a decision"));
    assert_eq!(status.pending_questions.len(), 1);
    assert_eq!(status.pending_questions[0].id, "q1");

    let response = request(&host.app, Method::GET, "/v1/runs", Some(TOKEN), None).await;
    assert_eq!(response.status(), StatusCode::OK);
    let listed: ListRunsResponse = body(response).await;
    assert_eq!(listed.runs.len(), 1);
    assert_eq!(listed.runs[0], status.run);
}

/// A stage report reaches the log only after the kernel strict-parsed
/// it against its dialect, so a logged report that cannot yield the
/// shared report core is a damaged log — not a run in some odd state.
/// The daemon says so rather than serving a half-read run, and the
/// list the run belongs to fails with it: quietly dropping the run
/// would misreport the fleet as healthy.
#[tokio::test]
async fn a_logged_report_without_the_shared_core_reads_as_a_corrupt_log() {
    let runs = tempfile::tempdir().unwrap();
    let dir = RunDir::create(runs.path(), RunId::new("r1"), "pipeline", "scripted")
        .await
        .unwrap();
    dir.event_sink()
        .await
        .unwrap()
        .emit(RunEvent::StageReported {
            iteration: 1,
            stage: "plan".into(),
            report: json!({"weird": true}),
        })
        .await
        .unwrap();

    let sandbox = Arc::new(fake_sandbox(done_report(), None));
    let host = TestHost::with_parts(runs, seeded_repo(), sandbox).await;
    for uri in ["/v1/runs/r1", "/v1/runs"] {
        let response = request(&host.app, Method::GET, uri, Some(TOKEN), None).await;
        assert_eq!(
            response.status(),
            StatusCode::INTERNAL_SERVER_ERROR,
            "{uri}"
        );
        let error: ApiError = body(response).await;
        assert_eq!(error.code, ErrorCode::InternalError, "{uri}");
    }
}

#[tokio::test]
async fn concurrent_runs_have_independent_ids_directories_and_histories() {
    let barrier = Arc::new(Barrier::new(2));
    let host = TestHost::with_barrier(done_report(), Some(barrier)).await;
    let (first, second) = tokio::join!(host.submit(), host.submit());
    assert_ne!(first.run_id, second.run_id);

    let (first_status, second_status) = tokio::join!(
        host.wait_for_state(&first.run_id, "done"),
        host.wait_for_state(&second.run_id, "done")
    );
    assert_eq!(first_status.run.run_id, first.run_id);
    assert_eq!(second_status.run.run_id, second.run_id);
    assert!(host.sandbox.max_active() >= 2);
}

/// Disk is the source of truth: a run directory deleted under a live
/// daemon answers on every endpoint exactly as it would after a
/// restart.
#[tokio::test]
async fn a_run_whose_directory_vanished_is_missing_not_a_daemon_fault() {
    let host = TestHost::new(done_report()).await;
    let submitted = host.submit().await;
    host.wait_for_state(&submitted.run_id, "done").await;
    tokio::fs::remove_dir_all(host.runs.path().join(&submitted.run_id))
        .await
        .unwrap();

    let response = request(
        &host.app,
        Method::GET,
        &format!("/v1/runs/{}", submitted.run_id),
        Some(TOKEN),
        None,
    )
    .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let error: ApiError = body(response).await;
    assert_eq!(error.code, ErrorCode::RunNotFound);

    let response = request(&host.app, Method::GET, "/v1/runs", Some(TOKEN), None).await;
    assert_eq!(response.status(), StatusCode::OK);
    let listed: ListRunsResponse = body(response).await;
    assert!(listed.runs.is_empty());
}

#[tokio::test]
async fn startup_ignores_entries_that_are_not_run_directories() {
    let runs = tempfile::tempdir().unwrap();
    std::fs::create_dir(runs.path().join("not-a-run")).unwrap();
    std::fs::write(runs.path().join("stray-file"), b"junk").unwrap();
    let repo = seeded_repo();
    let sandbox = Arc::new(fake_sandbox(done_report(), None));
    let host = TestHost::with_parts(runs, repo, sandbox).await;

    let response = request(&host.app, Method::GET, "/v1/runs", Some(TOKEN), None).await;
    assert_eq!(response.status(), StatusCode::OK);
    let listed: ListRunsResponse = body(response).await;
    assert!(listed.runs.is_empty());
}

#[tokio::test]
async fn restart_reloads_runs_and_reduces_status_from_their_event_logs() {
    let runs = tempfile::tempdir().unwrap();
    let repo = seeded_repo();
    let runtime = || {
        Arc::new(EngineRuntime::new(
            Arc::new(fake_sandbox(done_report(), None)),
            Arc::new(StubNotifier),
            Arc::new(NoSecrets),
        ))
    };
    let config = || DaemonConfig::new(TOKEN, runs.path());
    let first = Daemon::load(config(), runtime()).await.unwrap();
    let app = first.router();
    let flow = flow_for(repo.path());
    let response = request(
        &app,
        Method::POST,
        "/v1/runs",
        Some(TOKEN),
        Some(json!({"flow": flow})),
    )
    .await;
    let submitted: SubmitRunResponse = body(response).await;
    wait_for_state(&app, &submitted.run_id, "done").await;
    drop(first);
    drop(app);

    let restarted = Daemon::load(config(), runtime()).await.unwrap();
    let response = request(
        &restarted.router(),
        Method::GET,
        "/v1/runs",
        Some(TOKEN),
        None,
    )
    .await;
    let listed: ListRunsResponse = body(response).await;
    assert_eq!(listed.runs.len(), 1);
    assert_eq!(listed.runs[0].run_id, submitted.run_id);
    assert_eq!(
        serde_json::to_value(listed.runs[0].state).unwrap()["state"],
        "done"
    );
}

#[test]
fn every_served_route_has_the_same_method_in_openapi() {
    let document = serde_json::to_value(api::openapi::document()).unwrap();
    for route in SERVED_ROUTES {
        let method = route.method.as_str().to_ascii_lowercase();
        assert!(
            !document["paths"][route.path][method].is_null(),
            "{} {} is missing from OpenAPI",
            route.method,
            route.path
        );
    }
}

async fn request(
    app: &Router,
    method: Method,
    uri: &str,
    token: Option<&str>,
    json: Option<Value>,
) -> axum::response::Response {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(token) = token {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    let body = match json {
        Some(json) => {
            builder = builder.header(header::CONTENT_TYPE, "application/json");
            Body::from(serde_json::to_vec(&json).unwrap())
        }
        None => Body::empty(),
    };
    app.clone()
        .oneshot(builder.body(body).unwrap())
        .await
        .unwrap()
}

async fn raw_request(
    app: &Router,
    method: Method,
    uri: &str,
    authorization: Option<&str>,
) -> axum::response::Response {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(authorization) = authorization {
        builder = builder.header(header::AUTHORIZATION, authorization);
    }
    app.clone()
        .oneshot(builder.body(Body::empty()).unwrap())
        .await
        .unwrap()
}

async fn body<T: DeserializeOwned>(response: axum::response::Response) -> T {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

async fn wait_for_state(app: &Router, run_id: &str, expected: &str) -> RunStatusResponse {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let response = request(
                app,
                Method::GET,
                &format!("/v1/runs/{run_id}"),
                Some(TOKEN),
                None,
            )
            .await;
            assert_eq!(response.status(), StatusCode::OK);
            let value: Value = body(response).await;
            if value["state"] == expected {
                return serde_json::from_value(value).unwrap();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("run did not reach expected state")
}

fn flow_for(repo: &Path) -> String {
    format!(
        r#"[loop]
kernel = "pipeline"

[agent]
engine = "cmd"
command = ["fake-agent", "{{prompt}}"]

[workspace]
repo = {:?}
"#,
        repo.to_str().unwrap()
    )
}

fn done_report() -> Value {
    json!({"status": "done", "summary": "finished from the fake"})
}

fn needs_input_report() -> Value {
    json!({
        "status": "needs_input",
        "summary": "need a decision",
        "questions": [{"id": "q1", "text": "which shape?", "options": ["a", "b"]}]
    })
}
