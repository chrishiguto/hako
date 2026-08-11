use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use api::proto::flow::FlowConfig;
use api::{
    AnswerRequest, ApiError, ErrorCode, EventEnvelope, ListRunsResponse, ResumeRequest,
    SubmitRunRequest, SubmitRunResponse,
};
use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, Request, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use axum_extra::TypedHeader;
use axum_extra::headers::{Authorization, authorization::Bearer};
use axum_extra::typed_header::TypedHeaderRejection;
use constant_time_eq::constant_time_eq;
use engine::RunId;
use futures_util::stream;

use crate::projection;
use crate::registry::{AnswerOutcome, CancelOutcome, CommandError, ResumeOutcome, RunRegistry};
use crate::runtime::{EngineRuntime, ResolveError};

pub(crate) struct AppState {
    pub(crate) token: String,
    pub(crate) registry: RunRegistry,
    pub(crate) runtime: Arc<EngineRuntime>,
}

/// One route this daemon version actually serves. The OpenAPI parity
/// test consumes the same declarations that construct the router.
#[derive(Debug, Clone)]
pub struct ServedRoute {
    pub method: Method,
    pub path: &'static str,
}

macro_rules! define_routes {
    ($(($method:ident, $route:ident, $path:literal, $handler:ident)),+ $(,)?) => {
        pub static SERVED_ROUTES: &[ServedRoute] = &[
            $(ServedRoute { method: Method::$method, path: $path }),+
        ];

        fn routes() -> Router<Arc<AppState>> {
            Router::new()
                $(.route($path, $route($handler)))+
        }
    };
}

define_routes!(
    (POST, post, "/v1/runs", submit_run),
    (GET, get, "/v1/runs", list_runs),
    (GET, get, "/v1/runs/{run_id}", run_status),
    (GET, get, "/v1/runs/{run_id}/events", run_events),
    (POST, post, "/v1/runs/{run_id}/answer", answer_run),
    (POST, post, "/v1/runs/{run_id}/cancel", cancel_run),
    (POST, post, "/v1/runs/{run_id}/resume", resume_run),
);

pub(crate) fn router(state: Arc<AppState>) -> Router {
    routes()
        .route_layer(middleware::from_fn_with_state(state.clone(), authenticate))
        .with_state(state)
}

async fn authenticate(
    State(state): State<Arc<AppState>>,
    credentials: Result<TypedHeader<Authorization<Bearer>>, TypedHeaderRejection>,
    request: Request,
    next: Next,
) -> Result<Response, HttpError> {
    let TypedHeader(credentials) = credentials.map_err(|_| HttpError::Unauthorized)?;
    if !constant_time_eq(credentials.token().as_bytes(), state.token.as_bytes()) {
        return Err(HttpError::Unauthorized);
    }
    Ok(next.run(request).await)
}

async fn submit_run(
    State(state): State<Arc<AppState>>,
    payload: Result<Json<SubmitRunRequest>, JsonRejection>,
) -> Result<impl IntoResponse, HttpError> {
    let Json(request) = payload.map_err(|error| HttpError::InvalidRequest(error.body_text()))?;
    let flow = FlowConfig::from_toml(&request.flow)
        .map_err(|error| HttpError::InvalidFlow(error.to_string()))?;
    let resolved = state
        .runtime
        .resolve(&flow)
        .await
        .map_err(HttpError::from)?;
    let run_id = state
        .registry
        .submit(flow, resolved, &state.runtime)
        .await
        .map_err(HttpError::store)?;
    Ok((
        StatusCode::CREATED,
        Json(SubmitRunResponse {
            run_id: run_id.to_string(),
        }),
    ))
}

async fn list_runs(State(state): State<Arc<AppState>>) -> Json<ListRunsResponse> {
    Json(ListRunsResponse {
        runs: state.registry.list().await,
    })
}

async fn run_status(
    State(state): State<Arc<AppState>>,
    Path(run_id): Path<String>,
) -> Result<Json<api::RunStatusResponse>, HttpError> {
    let run_id = RunId::new(run_id);
    let dir = state
        .registry
        .get(&run_id)
        .await
        .ok_or(HttpError::RunNotFound)?;
    status_json(&dir).await
}

async fn run_events(
    State(state): State<Arc<AppState>>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, HttpError> {
    let run_id = RunId::new(run_id);
    let dir = state
        .registry
        .get(&run_id)
        .await
        .ok_or(HttpError::RunNotFound)?;
    let after = headers
        .get("last-event-id")
        .map(|value| {
            value
                .to_str()
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .ok_or_else(|| {
                    HttpError::InvalidRequest("Last-Event-ID must be an unsigned integer".into())
                })
        })
        .transpose()?;
    let (history, cursor) = dir
        .events_tail(engine::LogCursor::start())
        .await
        .map_err(HttpError::run_store)?;
    let follower = EventFollower {
        ended: history
            .last()
            .is_some_and(|event| is_terminal_event(&event.event)),
        min_seq: after.map_or(0, |seq| seq.saturating_add(1)),
        pending: history.into(),
        cursor,
        dir,
    };
    let stream = stream::unfold(follower, |mut follower| async move {
        let envelope = follower.next_event().await?;
        let event = Event::default()
            .id(envelope.seq.to_string())
            .data(serde_json::to_string(&envelope).expect("event envelope serializes"));
        Some((Ok::<_, std::convert::Infallible>(event), follower))
    });
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

async fn cancel_run(
    State(state): State<Arc<AppState>>,
    Path(run_id): Path<String>,
) -> Result<Json<api::RunStatusResponse>, HttpError> {
    let run_id = RunId::new(run_id);
    match state
        .registry
        .cancel(&run_id)
        .await
        .map_err(HttpError::command)?
    {
        CancelOutcome::Cancelled(status) => Ok(Json(*status)),
        CancelOutcome::NotRunning => Err(HttpError::RunNotRunning),
        CancelOutcome::UnknownRun => Err(HttpError::RunNotFound),
    }
}

async fn answer_run(
    State(state): State<Arc<AppState>>,
    Path(run_id): Path<String>,
    payload: Result<Json<AnswerRequest>, JsonRejection>,
) -> Result<Json<api::RunStatusResponse>, HttpError> {
    let Json(request) = payload.map_err(|error| HttpError::InvalidRequest(error.body_text()))?;
    if request.answers.is_empty() {
        return Err(HttpError::InvalidRequest(
            "at least one answer is required".into(),
        ));
    }
    let mut seen = std::collections::BTreeSet::new();
    for answer in &request.answers {
        if !seen.insert(answer.question_id.as_str()) {
            return Err(HttpError::InvalidRequest(format!(
                "duplicate answer for question `{}`",
                answer.question_id
            )));
        }
    }
    let run_id = RunId::new(run_id);
    let dir = match state
        .registry
        .answer(&run_id, request.answers)
        .await
        .map_err(HttpError::command)?
    {
        AnswerOutcome::Recorded(dir) => dir,
        AnswerOutcome::NotAwaitingInput => return Err(HttpError::NotAwaitingInput),
        AnswerOutcome::UnknownQuestion(question_id) => {
            return Err(HttpError::UnknownQuestion(question_id));
        }
        AnswerOutcome::Detached => return Err(HttpError::NotResumable),
        AnswerOutcome::UnknownRun => return Err(HttpError::RunNotFound),
    };
    status_json(&dir).await
}

async fn resume_run(
    State(state): State<Arc<AppState>>,
    Path(run_id): Path<String>,
    payload: Result<Json<ResumeRequest>, JsonRejection>,
) -> Result<Json<api::RunStatusResponse>, HttpError> {
    let Json(request) = payload.map_err(|error| HttpError::InvalidRequest(error.body_text()))?;
    let run_id = RunId::new(run_id);
    let dir = match state
        .registry
        .resume(
            &run_id,
            request.note,
            request.extend,
            state.runtime.as_ref(),
        )
        .await
        .map_err(HttpError::command)?
    {
        ResumeOutcome::Resumed(dir) => dir,
        ResumeOutcome::NotPaused => return Err(HttpError::NotPaused),
        ResumeOutcome::Detached => return Err(HttpError::NotResumable),
        ResumeOutcome::UnknownRun => return Err(HttpError::RunNotFound),
    };
    status_json(&dir).await
}

/// The status body every successful run command answers with, so the
/// client sees the effect without a second request.
async fn status_json(dir: &engine::RunDir) -> Result<Json<api::RunStatusResponse>, HttpError> {
    Ok(Json(
        projection::status(dir)
            .await
            .map_err(HttpError::run_store)?,
    ))
}

/// One SSE subscription's tail of the durable log: replay first, then
/// poll for what appended since. The Event Log stays the source of
/// truth — the follower keeps only a cursor and never re-reads what
/// it already delivered.
struct EventFollower {
    dir: engine::RunDir,
    pending: VecDeque<EventEnvelope>,
    cursor: engine::LogCursor,
    /// Replay below this seq is skipped: the client already holds it,
    /// per its `Last-Event-ID`.
    min_seq: u64,
    /// The log holds its terminal event; nothing more will append.
    ended: bool,
}

impl EventFollower {
    async fn next_event(&mut self) -> Option<EventEnvelope> {
        loop {
            if let Some(envelope) = self.pending.pop_front() {
                if envelope.seq >= self.min_seq {
                    return Some(envelope);
                }
                continue;
            }
            if self.ended {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
            match self.dir.events_tail(self.cursor).await {
                Ok((events, cursor)) => {
                    self.cursor = cursor;
                    // The terminal event, when it comes, is the last
                    // ever appended — an empty poll keeps the verdict
                    // of the one before it.
                    if let Some(last) = events.last() {
                        self.ended = is_terminal_event(&last.event);
                    }
                    self.pending.extend(events);
                }
                Err(error) => {
                    tracing::error!(%error, "event stream stopped reading run log");
                    return None;
                }
            }
        }
    }
}

fn is_terminal_event(event: &engine::RunEvent) -> bool {
    matches!(
        event,
        engine::RunEvent::StateChanged {
            state: engine::RunState::Done | engine::RunState::Failed | engine::RunState::Cancelled
        }
    )
}

enum HttpError {
    Unauthorized,
    InvalidRequest(String),
    InvalidFlow(String),
    UnrunnableFlow(String),
    MissingSecret(String),
    RunNotFound,
    RunNotRunning,
    NotAwaitingInput,
    UnknownQuestion(String),
    NotPaused,
    NotResumable,
    Internal,
}

/// A well-formed flow the daemon cannot run: both halves answer 422,
/// and which code the client reads is which side has to change — the
/// flow file, or the host's provisioning.
impl From<ResolveError> for HttpError {
    fn from(error: ResolveError) -> Self {
        match error {
            ResolveError::Agent(error) => Self::UnrunnableFlow(error.to_string()),
            // A store that is broken rather than incomplete is the
            // daemon's failure, not the submission's: nothing the
            // client sends would fix it.
            ResolveError::Secrets(engine::SecretsError::Provider(message)) => {
                tracing::error!(%message, "daemon secret store failure");
                Self::Internal
            }
            ResolveError::Secrets(error) => Self::MissingSecret(error.to_string()),
        }
    }
}

impl HttpError {
    fn store(error: engine::StoreError) -> Self {
        tracing::error!(%error, "daemon run-store failure");
        Self::Internal
    }

    /// For a handler addressing one run: a vanished run directory is
    /// that run missing — the same answer a restarted daemon would
    /// give, since `load` would not index it.
    fn run_store(error: engine::StoreError) -> Self {
        match error {
            engine::StoreError::NotFound(_) => Self::RunNotFound,
            error => Self::store(error),
        }
    }

    /// For a run command: a vanished run answers `run_not_found` like
    /// every other route; a sink failure is the daemon's fault.
    fn command(error: CommandError) -> Self {
        match error {
            CommandError::Store(error) => Self::run_store(error),
            CommandError::Sink(error) => {
                tracing::error!(%error, "daemon event-sink failure");
                Self::Internal
            }
        }
    }
}

impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        let unauthorized = matches!(&self, Self::Unauthorized);
        let (status, code, message) = match self {
            Self::Unauthorized => (
                StatusCode::UNAUTHORIZED,
                ErrorCode::Unauthorized,
                "missing or invalid bearer token".to_owned(),
            ),
            Self::InvalidRequest(message) => {
                (StatusCode::BAD_REQUEST, ErrorCode::InvalidRequest, message)
            }
            Self::InvalidFlow(message) => {
                (StatusCode::BAD_REQUEST, ErrorCode::InvalidFlow, message)
            }
            Self::UnrunnableFlow(message) => (
                StatusCode::UNPROCESSABLE_ENTITY,
                ErrorCode::InvalidAgent,
                message,
            ),
            Self::MissingSecret(message) => (
                StatusCode::UNPROCESSABLE_ENTITY,
                ErrorCode::MissingSecret,
                message,
            ),
            Self::RunNotFound => (
                StatusCode::NOT_FOUND,
                ErrorCode::RunNotFound,
                "no such run".to_owned(),
            ),
            Self::RunNotRunning => (
                StatusCode::CONFLICT,
                ErrorCode::RunNotRunning,
                "run is not running".to_owned(),
            ),
            Self::NotAwaitingInput => (
                StatusCode::CONFLICT,
                ErrorCode::NotAwaitingInput,
                "run is not awaiting human input".to_owned(),
            ),
            Self::UnknownQuestion(question_id) => (
                StatusCode::BAD_REQUEST,
                ErrorCode::UnknownQuestion,
                format!("unknown pending question `{question_id}`"),
            ),
            Self::NotPaused => (
                StatusCode::CONFLICT,
                ErrorCode::NotPaused,
                "run is not paused".to_owned(),
            ),
            Self::NotResumable => (
                StatusCode::CONFLICT,
                ErrorCode::NotResumable,
                "run predates a daemon restart and can no longer be resumed".to_owned(),
            ),
            Self::Internal => (
                StatusCode::INTERNAL_SERVER_ERROR,
                ErrorCode::InternalError,
                "internal daemon error".to_owned(),
            ),
        };
        let mut response = (status, Json(ApiError { code, message })).into_response();
        if unauthorized {
            response
                .headers_mut()
                .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
        }
        response
    }
}
