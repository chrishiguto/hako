use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use api::{ApiError, ListRunsResponse, RunStatusResponse, SubmitRunResponse};
use async_trait::async_trait;
use axum::Router;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use engine::{
    ExecEvent, ExecSpec, ExecStream, ExitStatus, Notification, Notifier, NotifierError, Sandbox,
    SandboxError, SandboxHandle, SandboxSpec, SecretName, SecretValue, SecretsError,
    SecretsProvider,
};
use futures_util::{StreamExt, stream};
use http_body_util::BodyExt;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use server::{Daemon, DaemonConfig, EngineRuntime, SERVED_ROUTES};
use tokio::sync::Barrier;
use tower::ServiceExt;

const TOKEN: &str = "test-bearer-token";

struct TestHost {
    _runs: tempfile::TempDir,
    repo: tempfile::TempDir,
    app: Router,
    sandbox: Arc<FakeSandbox>,
}

impl TestHost {
    async fn new(report: Value) -> Self {
        Self::with_barrier(report, None).await
    }

    async fn with_barrier(report: Value, barrier: Option<Arc<Barrier>>) -> Self {
        let runs = tempfile::tempdir().unwrap();
        let repo = seeded_repo();
        let sandbox = Arc::new(FakeSandbox::new(report, barrier));
        Self::with_parts(runs, repo, sandbox).await
    }

    async fn with_parts(
        runs: tempfile::TempDir,
        repo: tempfile::TempDir,
        sandbox: Arc<FakeSandbox>,
    ) -> Self {
        let runtime = Arc::new(EngineRuntime::new(
            sandbox.clone(),
            Arc::new(QuietNotifier),
            Arc::new(NoSecrets),
        ));
        let daemon = Daemon::load(DaemonConfig::new(TOKEN, runs.path()), runtime)
            .await
            .unwrap();
        Self {
            _runs: runs,
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
            let error: ApiError = body(response).await;
            assert_eq!(error.code, "unauthorized");
        }
    }

    let response = request(&host.app, Method::GET, "/v1/runs", Some(TOKEN), None).await;
    assert_eq!(response.status(), StatusCode::OK);
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
    assert_eq!(error.code, "invalid_flow");
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
    assert_eq!(error.code, "invalid_agent");
    assert!(error.message.contains("missing"));
}

#[tokio::test]
async fn a_panicking_execution_is_recorded_as_failed() {
    let runs = tempfile::tempdir().unwrap();
    let repo = seeded_repo();
    let sandbox = Arc::new(FakeSandbox::panicking());
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
    assert!(host.sandbox.max_active.load(Ordering::SeqCst) >= 2);
}

#[tokio::test]
async fn restart_reloads_runs_and_reduces_status_from_their_event_logs() {
    let runs = tempfile::tempdir().unwrap();
    let repo = seeded_repo();
    let runtime = || {
        Arc::new(EngineRuntime::new(
            Arc::new(FakeSandbox::new(done_report(), None)),
            Arc::new(QuietNotifier),
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

fn seeded_repo() -> tempfile::TempDir {
    let repo = tempfile::tempdir().unwrap();
    run_git(repo.path(), &["init", "--quiet"]);
    std::fs::write(repo.path().join("README.md"), "seed\n").unwrap();
    run_git(repo.path(), &["add", "README.md"]);
    run_git(
        repo.path(),
        &[
            "-c",
            "user.name=hako test",
            "-c",
            "user.email=hako@example.invalid",
            "commit",
            "--quiet",
            "-m",
            "seed",
        ],
    );
    repo
}

fn run_git(repo: &Path, args: &[&str]) {
    let output = Command::new("git")
        .args(args)
        .current_dir(repo)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {}: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stderr)
    );
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

struct FakeSandbox {
    report: Vec<u8>,
    barrier: Option<Arc<Barrier>>,
    next: AtomicUsize,
    active: AtomicUsize,
    max_active: AtomicUsize,
    panic_on_create: bool,
}

impl FakeSandbox {
    fn new(report: Value, barrier: Option<Arc<Barrier>>) -> Self {
        Self {
            report: serde_json::to_vec(&report).unwrap(),
            barrier,
            next: AtomicUsize::new(0),
            active: AtomicUsize::new(0),
            max_active: AtomicUsize::new(0),
            panic_on_create: false,
        }
    }

    fn panicking() -> Self {
        Self {
            panic_on_create: true,
            ..Self::new(done_report(), None)
        }
    }
}

#[async_trait]
impl Sandbox for FakeSandbox {
    async fn create(&self, _spec: &SandboxSpec) -> Result<SandboxHandle, SandboxError> {
        assert!(!self.panic_on_create, "scripted sandbox panic");
        let id = format!("fake-{}", self.next.fetch_add(1, Ordering::SeqCst));
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_active.fetch_max(active, Ordering::SeqCst);
        Ok(SandboxHandle::new(id))
    }

    async fn exec_stream(
        &self,
        _sandbox: &SandboxHandle,
        _command: &ExecSpec,
    ) -> Result<ExecStream, SandboxError> {
        if let Some(barrier) = &self.barrier {
            barrier.wait().await;
        }
        Ok(stream::iter([Ok(ExecEvent::Exited(ExitStatus { code: Some(0) }))]).boxed())
    }

    async fn put_file(
        &self,
        _sandbox: &SandboxHandle,
        _path: &Path,
        _contents: &[u8],
    ) -> Result<(), SandboxError> {
        Ok(())
    }

    async fn get_file(
        &self,
        _sandbox: &SandboxHandle,
        _path: &Path,
    ) -> Result<Vec<u8>, SandboxError> {
        Ok(self.report.clone())
    }

    async fn remove_file(
        &self,
        _sandbox: &SandboxHandle,
        _path: &Path,
    ) -> Result<(), SandboxError> {
        Ok(())
    }

    async fn destroy(&self, _sandbox: SandboxHandle) -> Result<(), SandboxError> {
        self.active.fetch_sub(1, Ordering::SeqCst);
        Ok(())
    }

    async fn preflight(&self) -> Result<(), SandboxError> {
        Ok(())
    }
}

struct QuietNotifier;

#[async_trait]
impl Notifier for QuietNotifier {
    async fn notify(&self, _notification: &Notification) -> Result<(), NotifierError> {
        Ok(())
    }
}

struct NoSecrets;

#[async_trait]
impl SecretsProvider for NoSecrets {
    async fn resolve(&self, name: &SecretName) -> Result<SecretValue, SecretsError> {
        Err(SecretsError::NotFound(name.clone()))
    }
}
