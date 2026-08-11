use super::*;

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
    assert_eq!(listed.runs[0], RunListEntry::Run(status.run));
}

/// A stage report reaches the log only after the kernel strict-parsed
/// it against its dialect, so a logged report that cannot yield the
/// shared report core is a damaged log — not a run in some odd state.
/// The run's own endpoint says so rather than serving a half-read
/// run. The list does not fail with it: the list is how an operator
/// finds the broken run, so the damage is carried in-band as an
/// `unreadable` entry beside the healthy fleet — quietly dropping the
/// run would make absence ambiguous between "gone" and "broken".
#[tokio::test]
async fn a_corrupt_run_fails_its_own_status_but_is_listed_unreadable() {
    let runs = tempfile::tempdir().unwrap();
    let runs_root = runs.path().display().to_string();
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
    let submitted = host.submit().await;
    let healthy = host.wait_for_state(&submitted.run_id, "done").await;

    // The caller asked about the broken thing: loud failure is right.
    let response = request(&host.app, Method::GET, "/v1/runs/r1", Some(TOKEN), None).await;
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let error: ApiError = body(response).await;
    assert_eq!(error.code, ErrorCode::InternalError);

    // The caller asked about the fleet: the broken run rides along,
    // it does not take the healthy one down with it.
    let response = request(&host.app, Method::GET, "/v1/runs", Some(TOKEN), None).await;
    assert_eq!(response.status(), StatusCode::OK);
    let listed: ListRunsResponse = body(response).await;
    assert_eq!(listed.runs.len(), 2);
    assert_eq!(listed.runs[0], RunListEntry::Run(healthy.run));
    let RunListEntry::Unreadable(broken) = &listed.runs[1] else {
        panic!(
            "the corrupt run is not listed unreadable: {:?}",
            listed.runs[1]
        );
    };
    assert_eq!(broken.run_id, "r1");
    assert_eq!(broken.kernel, "pipeline");
    assert_eq!(broken.agent, "scripted");
    assert!(broken.reason.contains("report core"), "{}", broken.reason);
    // The reason names the damaged file, not where the daemon keeps
    // it: host paths stay in the server log, off the wire.
    assert!(broken.reason.contains("events.jsonl"), "{}", broken.reason);
    assert!(!broken.reason.contains(&runs_root), "{}", broken.reason);
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
    let RunListEntry::Run(reloaded) = &listed.runs[0] else {
        panic!("the reloaded run is not readable: {:?}", listed.runs[0]);
    };
    assert_eq!(reloaded.run_id, submitted.run_id);
    assert_eq!(
        serde_json::to_value(reloaded.state).unwrap()["state"],
        "done"
    );
}

#[test]
fn the_served_routes_equal_the_openapi_paths_and_methods() {
    let document = serde_json::to_value(api::openapi::document()).unwrap();
    let documented: std::collections::BTreeSet<_> = document["paths"]
        .as_object()
        .unwrap()
        .iter()
        .flat_map(|(path, methods)| {
            methods
                .as_object()
                .unwrap()
                .keys()
                .filter(|method| {
                    matches!(method.as_str(), "get" | "post" | "put" | "patch" | "delete")
                })
                .map(move |method| (method.to_ascii_uppercase(), path.clone()))
        })
        .collect();
    let served: std::collections::BTreeSet<_> = SERVED_ROUTES
        .iter()
        .map(|route| (route.method.as_str().to_owned(), route.path.to_owned()))
        .collect();
    assert_eq!(served, documented);
}
