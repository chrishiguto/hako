use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use api::{
    ApiError, ErrorCode, EventEnvelope, ListRunsResponse, RunStatusResponse, SubmitRunResponse,
};
use axum::Router;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use engine::testkit::{
    MapSecrets, NoSecrets, SKEPTIC_PROMPT_HEADING, ScriptedSandbox, StubNotifier,
    UNREFUTED_SKEPTIC_REPORT, seeded_repo,
};
use engine::{EventSink, ExecEvent, ExitStatus, RunDir, RunEvent, RunId, SecretsProvider};
use http_body_util::BodyExt;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use server::{Daemon, DaemonConfig, EngineRuntime, SERVED_ROUTES};
use tokio::sync::Barrier;
use tower::ServiceExt;

#[path = "http/base.rs"]
mod base;
#[path = "http/commands.rs"]
mod commands;
#[path = "http/events.rs"]
mod events;

use events::{sse_events, sse_until};

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
        sandbox.write_report_when_argv_contains(SKEPTIC_PROMPT_HEADING, UNREFUTED_SKEPTIC_REPORT);
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
