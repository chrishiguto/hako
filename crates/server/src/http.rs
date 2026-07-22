use std::sync::Arc;

use api::proto::flow::FlowConfig;
use api::{ApiError, ListRunsResponse, SubmitRunRequest, SubmitRunResponse};
use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, Request, State};
use axum::http::{Method, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use engine::{RunDir, RunId};
use uuid::Uuid;

use crate::registry::{RunRegistry, status};
use crate::runtime::EngineRuntime;

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
);

pub(crate) fn router(state: Arc<AppState>) -> Router {
    routes()
        .route_layer(middleware::from_fn_with_state(state.clone(), authenticate))
        .with_state(state)
}

async fn authenticate(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Result<Response, HttpError> {
    let authorized = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split_once(' '))
        .is_some_and(|(scheme, token)| {
            scheme.eq_ignore_ascii_case("bearer") && constant_time_eq(token, &state.token)
        });
    if !authorized {
        return Err(HttpError::Unauthorized);
    }
    Ok(next.run(request).await)
}

fn constant_time_eq(left: &str, right: &str) -> bool {
    let left = left.as_bytes();
    let right = right.as_bytes();
    let mut difference = left.len() ^ right.len();
    let width = left.len().max(right.len());
    for index in 0..width {
        difference |= usize::from(left.get(index).copied().unwrap_or_default())
            ^ usize::from(right.get(index).copied().unwrap_or_default());
    }
    difference == 0
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
        .map_err(|error| HttpError::UnrunnableFlow(error.to_string()))?;
    let run_id = RunId::new(Uuid::now_v7().to_string());
    let dir = RunDir::create(
        state.registry.runs_root(),
        run_id.clone(),
        flow.r#loop.kernel.as_str(),
        &flow.agent.engine,
    )
    .await
    .map_err(HttpError::store)?;
    let execution = state
        .runtime
        .launch(dir.clone(), flow, resolved)
        .await
        .map_err(HttpError::store)?;
    state
        .registry
        .insert_live(run_id.clone(), dir, execution)
        .await;
    Ok((
        StatusCode::CREATED,
        Json(SubmitRunResponse {
            run_id: run_id.to_string(),
        }),
    ))
}

async fn list_runs(
    State(state): State<Arc<AppState>>,
) -> Result<Json<ListRunsResponse>, HttpError> {
    let runs = state.registry.list().await.map_err(HttpError::store)?;
    Ok(Json(ListRunsResponse { runs }))
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
    Ok(Json(status(&dir).await.map_err(HttpError::store)?))
}

enum HttpError {
    Unauthorized,
    InvalidRequest(String),
    InvalidFlow(String),
    UnrunnableFlow(String),
    RunNotFound,
    Internal,
}

impl HttpError {
    fn store(error: engine::StoreError) -> Self {
        eprintln!("daemon run-store failure: {error}");
        Self::Internal
    }
}

impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        let (status, code, message) = match self {
            Self::Unauthorized => (
                StatusCode::UNAUTHORIZED,
                "unauthorized",
                "missing or invalid bearer token".to_owned(),
            ),
            Self::InvalidRequest(message) => (StatusCode::BAD_REQUEST, "invalid_request", message),
            Self::InvalidFlow(message) => (StatusCode::BAD_REQUEST, "invalid_flow", message),
            Self::UnrunnableFlow(message) => {
                (StatusCode::UNPROCESSABLE_ENTITY, "invalid_agent", message)
            }
            Self::RunNotFound => (
                StatusCode::NOT_FOUND,
                "run_not_found",
                "no such run".to_owned(),
            ),
            Self::Internal => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                "internal daemon error".to_owned(),
            ),
        };
        (
            status,
            Json(ApiError {
                code: code.to_owned(),
                message,
            }),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_comparison_covers_different_lengths_without_accepting_prefixes() {
        assert!(constant_time_eq("token", "token"));
        assert!(!constant_time_eq("token", "token-long"));
        assert!(!constant_time_eq("token-long", "token"));
        assert!(!constant_time_eq("taken", "token"));
    }
}
