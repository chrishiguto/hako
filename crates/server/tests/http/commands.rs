use super::*;

#[tokio::test]
async fn cancel_waits_for_sandbox_teardown_and_returns_the_cancelled_run() {
    let barrier = Arc::new(Barrier::new(2));
    let sandbox = Arc::new(ScriptedSandbox::hanging().with_barrier(barrier.clone()));
    let host =
        TestHost::with_parts(tempfile::tempdir().unwrap(), seeded_repo(), sandbox.clone()).await;
    let submitted = host.submit().await;
    barrier.wait().await;

    let response = request(
        &host.app,
        Method::POST,
        &format!("/v1/runs/{}/cancel", submitted.run_id),
        Some(TOKEN),
        None,
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let status: RunStatusResponse = body(response).await;
    assert_eq!(status.run.state, engine::RunState::Cancelled);
    assert_eq!(sandbox.created(), 1);
    assert_eq!(sandbox.destroyed(), 1);

    let events = sse_events(
        request(
            &host.app,
            Method::GET,
            &format!("/v1/runs/{}/events", submitted.run_id),
            Some(TOKEN),
            None,
        )
        .await,
    )
    .await;
    assert!(matches!(
        events.last().unwrap().event,
        RunEvent::StateChanged {
            state: engine::RunState::Cancelled
        }
    ));
}

#[tokio::test]
async fn cancel_distinguishes_missing_and_terminal_runs() {
    let host = TestHost::new(done_report()).await;
    let missing = request(
        &host.app,
        Method::POST,
        "/v1/runs/missing/cancel",
        Some(TOKEN),
        None,
    )
    .await;
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    assert_eq!(body::<ApiError>(missing).await.code, ErrorCode::RunNotFound);

    let submitted = host.submit().await;
    host.wait_for_state(&submitted.run_id, "done").await;
    let terminal = request(
        &host.app,
        Method::POST,
        &format!("/v1/runs/{}/cancel", submitted.run_id),
        Some(TOKEN),
        None,
    )
    .await;
    assert_eq!(terminal.status(), StatusCode::CONFLICT);
    assert_eq!(
        body::<ApiError>(terminal).await.code,
        ErrorCode::RunNotRunning
    );
}

#[tokio::test]
async fn cancel_makes_a_paused_run_terminal_without_booting_another_sandbox() {
    let host = TestHost::new(needs_input_report()).await;
    let submitted = host.submit().await;
    host.wait_for_state(&submitted.run_id, "paused").await;
    let created = host.sandbox.created();

    let response = request(
        &host.app,
        Method::POST,
        &format!("/v1/runs/{}/cancel", submitted.run_id),
        Some(TOKEN),
        None,
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let status: RunStatusResponse = body(response).await;
    assert_eq!(status.run.state, engine::RunState::Cancelled);
    assert_eq!(host.sandbox.created(), created);
    assert_eq!(host.sandbox.destroyed(), created);
}

#[tokio::test]
async fn answer_records_valid_answers_in_the_paused_runs_event_log() {
    let host = TestHost::new(needs_input_report()).await;
    let submitted = host.submit().await;
    host.wait_for_state(&submitted.run_id, "paused").await;

    let response = request(
        &host.app,
        Method::POST,
        &format!("/v1/runs/{}/answer", submitted.run_id),
        Some(TOKEN),
        Some(json!({"answers": [{"question_id": "q1", "answer": "a"}]})),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let status: RunStatusResponse = body(response).await;
    assert_eq!(
        status.run.state,
        engine::RunState::Paused {
            reason: engine::PauseReason::AwaitingHuman
        }
    );

    let events = sse_until(
        request(
            &host.app,
            Method::GET,
            &format!("/v1/runs/{}/events", submitted.run_id),
            Some(TOKEN),
            None,
        )
        .await,
        |event| matches!(event.event, RunEvent::QuestionAnswered { .. }),
    )
    .await;
    assert!(matches!(
        events.last().unwrap().event,
        RunEvent::QuestionAnswered {
            ref question_id,
            ref answer
        } if question_id == "q1" && answer == "a"
    ));
}

#[tokio::test]
async fn answer_rejects_unknown_questions_and_runs_that_are_not_awaiting_input() {
    let host = TestHost::new(needs_input_report()).await;
    let submitted = host.submit().await;
    host.wait_for_state(&submitted.run_id, "paused").await;
    let unknown = request(
        &host.app,
        Method::POST,
        &format!("/v1/runs/{}/answer", submitted.run_id),
        Some(TOKEN),
        Some(json!({"answers": [{"question_id": "q9", "answer": "x"}]})),
    )
    .await;
    assert_eq!(unknown.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        body::<ApiError>(unknown).await.code,
        ErrorCode::UnknownQuestion
    );

    let done = TestHost::new(done_report()).await;
    let submitted = done.submit().await;
    done.wait_for_state(&submitted.run_id, "done").await;
    let not_waiting = request(
        &done.app,
        Method::POST,
        &format!("/v1/runs/{}/answer", submitted.run_id),
        Some(TOKEN),
        Some(json!({"answers": [{"question_id": "q1", "answer": "a"}]})),
    )
    .await;
    assert_eq!(not_waiting.status(), StatusCode::CONFLICT);
    assert_eq!(
        body::<ApiError>(not_waiting).await.code,
        ErrorCode::NotAwaitingInput
    );
}

#[tokio::test]
async fn resume_injects_recorded_answers_and_note_into_the_next_preamble() {
    // The resumed stage claims done, so a fresh skeptic runs before the
    // run can reach `done`; it lets the claim stand.
    let sandbox = Arc::new(ScriptedSandbox::scripted(vec![
        engine::testkit::exec("paused\n", 0),
        engine::testkit::exec("done\n", 0),
        engine::testkit::exec("checked\n", 0),
    ]));
    sandbox.write_report_on_exec(serde_json::to_vec(&needs_input_report()).unwrap());
    sandbox.write_report_on_exec(serde_json::to_vec(&done_report()).unwrap());
    sandbox.write_report_on_exec(UNREFUTED_SKEPTIC_REPORT);
    let host =
        TestHost::with_parts(tempfile::tempdir().unwrap(), seeded_repo(), sandbox.clone()).await;
    let submitted = host.submit().await;
    host.wait_for_state(&submitted.run_id, "paused").await;
    let answer = request(
        &host.app,
        Method::POST,
        &format!("/v1/runs/{}/answer", submitted.run_id),
        Some(TOKEN),
        Some(json!({"answers": [{"question_id": "q1", "answer": "a"}]})),
    )
    .await;
    assert_eq!(answer.status(), StatusCode::OK);

    let resumed = request(
        &host.app,
        Method::POST,
        &format!("/v1/runs/{}/resume", submitted.run_id),
        Some(TOKEN),
        Some(json!({
            "note": "prefer the smaller design",
            "extend": {
                "max_iterations": 20,
                "max_wall_clock_seconds": 7200,
                "max_tokens": 100000
            }
        })),
    )
    .await;
    assert_eq!(resumed.status(), StatusCode::OK);
    host.wait_for_state(&submitted.run_id, "done").await;

    let execs = sandbox.execs();
    assert_eq!(execs.len(), 3);
    // The resumed stage — the one whose preamble must carry the answers.
    let prompt = execs[1].argv.last().unwrap();
    assert!(prompt.contains("Q: which shape?\n  A: a"), "{prompt}");
    assert!(
        prompt.contains("Note: prefer the smaller design"),
        "{prompt}"
    );

    let events = sse_events(
        request(
            &host.app,
            Method::GET,
            &format!("/v1/runs/{}/events", submitted.run_id),
            Some(TOKEN),
            None,
        )
        .await,
    )
    .await;
    assert!(events.iter().any(|event| matches!(
        event.event,
        RunEvent::RunResumed {
            note: Some(ref note),
            extend: Some(ref extend),
        } if note == "prefer the smaller design" && extend.max_iterations == Some(20)
    )));
}

/// The paused run through pause, resume, and completion in a mounted
/// checkout. The pause window holds no lock — the lock lives and dies
/// with the execution — and the run reaching `done` after resume
/// proves reattachment took the checkout back and worked in place.
#[tokio::test]
async fn resume_in_mount_mode_retakes_the_checkout_and_finishes_in_place() {
    let (host, submitted, lock) = paused_mount_run().await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while lock.exists() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the paused run still holds the mount lock"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let response = request(
        &host.app,
        Method::POST,
        &format!("/v1/runs/{}/resume", submitted.run_id),
        Some(TOKEN),
        Some(json!({"note": null, "extend": null})),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    host.wait_for_state(&submitted.run_id, "done").await;
    assert!(!lock.exists(), "the finished run released the checkout");
}

/// The lock is load-bearing on resume: a checkout claimed by another
/// run during the pause window fails the resumed run loudly instead
/// of letting two runs share one working tree.
#[tokio::test]
async fn resume_fails_loudly_when_another_run_claimed_the_mounted_checkout() {
    let (host, submitted, lock) = paused_mount_run().await;
    std::fs::write(&lock, "r2").unwrap();

    let response = request(
        &host.app,
        Method::POST,
        &format!("/v1/runs/{}/resume", submitted.run_id),
        Some(TOKEN),
        Some(json!({"note": null, "extend": null})),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    host.wait_for_state(&submitted.run_id, "failed").await;
}

/// A mount-mode run scripted to pause on its first stage and finish
/// after resume, paused and ready — plus the checkout's lock path.
async fn paused_mount_run() -> (TestHost, SubmitRunResponse, std::path::PathBuf) {
    let sandbox = Arc::new(ScriptedSandbox::scripted(vec![
        engine::testkit::exec("paused\n", 0),
        engine::testkit::exec("done\n", 0),
        engine::testkit::exec("checked\n", 0),
    ]));
    sandbox.write_report_on_exec(serde_json::to_vec(&needs_input_report()).unwrap());
    sandbox.write_report_on_exec(serde_json::to_vec(&done_report()).unwrap());
    sandbox.write_report_on_exec(UNREFUTED_SKEPTIC_REPORT);
    let host = TestHost::with_parts(tempfile::tempdir().unwrap(), seeded_repo(), sandbox).await;
    let flow = format!("{}mode = \"mount\"\n", flow_for(host.repo.path()));
    let submitted = host.submit_flow(&flow).await;
    host.wait_for_state(&submitted.run_id, "paused").await;
    let lock = host.repo.path().join(".git").join("hako-mount.lock");
    (host, submitted, lock)
}

/// A run whose directory vanished under a live entry answers 404 from
/// every command, exactly as it does from a status read — never a
/// daemon fault.
#[tokio::test]
async fn commands_on_a_vanished_run_directory_answer_not_found() {
    let host = TestHost::new(done_report()).await;
    let submitted = host.submit().await;
    host.wait_for_state(&submitted.run_id, "done").await;
    tokio::fs::remove_dir_all(host.runs.path().join(&submitted.run_id))
        .await
        .unwrap();

    for (uri, payload) in [
        (format!("/v1/runs/{}/cancel", submitted.run_id), None),
        (
            format!("/v1/runs/{}/answer", submitted.run_id),
            Some(json!({"answers": [{"question_id": "q1", "answer": "a"}]})),
        ),
        (
            format!("/v1/runs/{}/resume", submitted.run_id),
            Some(json!({"note": null, "extend": null})),
        ),
    ] {
        let response = request(&host.app, Method::POST, &uri, Some(TOKEN), payload).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{uri}");
        assert_eq!(
            body::<ApiError>(response).await.code,
            ErrorCode::RunNotFound,
            "{uri}"
        );
    }
}

/// A restart drops the relaunch material — resolved agent, secrets,
/// flow — so a reloaded paused run still reports `paused`, but answer
/// and resume refuse honestly instead of lying about its state or
/// answering with a daemon fault.
#[tokio::test]
async fn a_paused_run_from_before_a_restart_is_not_resumable() {
    let runs = tempfile::tempdir().unwrap();
    let repo = seeded_repo();
    let runtime = || {
        Arc::new(EngineRuntime::new(
            Arc::new(fake_sandbox(needs_input_report(), None)),
            Arc::new(StubNotifier),
            Arc::new(NoSecrets),
        ))
    };
    let first = Daemon::load(DaemonConfig::new(TOKEN, runs.path()), runtime())
        .await
        .unwrap();
    let app = first.router();
    let response = request(
        &app,
        Method::POST,
        "/v1/runs",
        Some(TOKEN),
        Some(json!({"flow": flow_for(repo.path())})),
    )
    .await;
    let submitted: SubmitRunResponse = body(response).await;
    wait_for_state(&app, &submitted.run_id, "paused").await;
    drop(app);
    drop(first);

    let restarted = Daemon::load(DaemonConfig::new(TOKEN, runs.path()), runtime())
        .await
        .unwrap();
    let app = restarted.router();
    wait_for_state(&app, &submitted.run_id, "paused").await;
    for (uri, payload) in [
        (
            format!("/v1/runs/{}/answer", submitted.run_id),
            json!({"answers": [{"question_id": "q1", "answer": "a"}]}),
        ),
        (
            format!("/v1/runs/{}/resume", submitted.run_id),
            json!({"note": null, "extend": null}),
        ),
    ] {
        let response = request(&app, Method::POST, &uri, Some(TOKEN), Some(payload)).await;
        assert_eq!(response.status(), StatusCode::CONFLICT, "{uri}");
        assert_eq!(
            body::<ApiError>(response).await.code,
            ErrorCode::NotResumable,
            "{uri}"
        );
    }
}

#[tokio::test]
async fn resume_rejects_missing_and_nonpaused_runs() {
    let host = TestHost::new(done_report()).await;
    let missing = request(
        &host.app,
        Method::POST,
        "/v1/runs/missing/resume",
        Some(TOKEN),
        Some(json!({"note": null, "extend": null})),
    )
    .await;
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);

    let submitted = host.submit().await;
    host.wait_for_state(&submitted.run_id, "done").await;
    let not_paused = request(
        &host.app,
        Method::POST,
        &format!("/v1/runs/{}/resume", submitted.run_id),
        Some(TOKEN),
        Some(json!({"note": null, "extend": null})),
    )
    .await;
    assert_eq!(not_paused.status(), StatusCode::CONFLICT);
    assert_eq!(
        body::<ApiError>(not_paused).await.code,
        ErrorCode::NotPaused
    );
}
