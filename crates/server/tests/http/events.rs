use super::*;

#[tokio::test]
async fn a_fresh_event_stream_replays_the_whole_run_history() {
    let host = TestHost::new(done_report()).await;
    let submitted = host.submit().await;
    host.wait_for_state(&submitted.run_id, "done").await;

    let response = request(
        &host.app,
        Method::GET,
        &format!("/v1/runs/{}/events", submitted.run_id),
        Some(TOKEN),
        None,
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[header::CONTENT_TYPE],
        "text/event-stream"
    );

    let events = sse_events(response).await;
    assert!(!events.is_empty());
    assert_eq!(events.first().unwrap().seq, 0);
    assert!(events.windows(2).all(|pair| pair[1].seq == pair[0].seq + 1));
    assert!(matches!(
        events.last().unwrap().event,
        RunEvent::StateChanged {
            state: engine::RunState::Done
        }
    ));
}

#[tokio::test]
async fn last_event_id_replays_each_missed_event_exactly_once() {
    let host = TestHost::new(done_report()).await;
    let submitted = host.submit().await;
    host.wait_for_state(&submitted.run_id, "done").await;
    let uri = format!("/v1/runs/{}/events", submitted.run_id);

    let all = sse_events(request(&host.app, Method::GET, &uri, Some(TOKEN), None).await).await;
    let cursor = all[all.len() / 2].seq;
    let resumed = sse_events(
        request_with_headers(
            &host.app,
            Method::GET,
            &uri,
            Some(TOKEN),
            [("last-event-id", cursor.to_string())],
            None,
        )
        .await,
    )
    .await;

    assert_eq!(resumed, all[(cursor as usize + 1)..]);
}

#[tokio::test]
async fn an_event_stream_replays_then_follows_events_appended_live() {
    let runs = tempfile::tempdir().unwrap();
    let dir = RunDir::create(runs.path(), RunId::new("r1"), "pipeline", "cmd")
        .await
        .unwrap();
    let sink = dir.event_sink().await.unwrap();
    sink.emit(RunEvent::RunStarted {
        kernel: "pipeline".into(),
        agent: "cmd".into(),
    })
    .await
    .unwrap();
    let host = TestHost::with_parts(
        runs,
        seeded_repo(),
        Arc::new(fake_sandbox(done_report(), None)),
    )
    .await;

    let response = request(
        &host.app,
        Method::GET,
        "/v1/runs/r1/events",
        Some(TOKEN),
        None,
    )
    .await;
    let collecting = tokio::spawn(sse_events(response));
    sink.emit(RunEvent::StateChanged {
        state: engine::RunState::Done,
    })
    .await
    .unwrap();

    let events = tokio::time::timeout(Duration::from_secs(2), collecting)
        .await
        .expect("event stream did not finish")
        .unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].seq, 0);
    assert_eq!(events[1].seq, 1);
}

async fn request_with_headers<const N: usize>(
    app: &Router,
    method: Method,
    uri: &str,
    token: Option<&str>,
    headers: [(&str, String); N],
    json: Option<Value>,
) -> axum::response::Response {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(token) = token {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    for (name, value) in headers {
        builder = builder.header(name, value);
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

pub(super) async fn sse_events(response: axum::response::Response) -> Vec<EventEnvelope> {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let text = std::str::from_utf8(&bytes).unwrap();
    text.split("\n\n")
        .filter(|frame| !frame.is_empty())
        .filter_map(parse_sse_event)
        .collect()
}

pub(super) async fn sse_until(
    response: axum::response::Response,
    done: impl Fn(&EventEnvelope) -> bool,
) -> Vec<EventEnvelope> {
    let mut body = response.into_body();
    let mut pending = String::new();
    let mut events = Vec::new();
    loop {
        let frame = body.frame().await.unwrap().unwrap();
        let Some(data) = frame.data_ref() else {
            continue;
        };
        pending.push_str(std::str::from_utf8(data).unwrap());
        while let Some(end) = pending.find("\n\n") {
            let raw = pending[..end].to_owned();
            pending.drain(..end + 2);
            let Some(event) = parse_sse_event(&raw) else {
                continue;
            };
            let finished = done(&event);
            events.push(event);
            if finished {
                return events;
            }
        }
    }
}

fn parse_sse_event(frame: &str) -> Option<EventEnvelope> {
    let id = frame
        .lines()
        .find_map(|line| line.strip_prefix("id: "))?
        .parse::<u64>()
        .unwrap();
    let data = frame
        .lines()
        .find_map(|line| line.strip_prefix("data: "))
        .unwrap();
    let event: EventEnvelope = serde_json::from_str(data).unwrap();
    assert_eq!(id, event.seq);
    Some(event)
}
