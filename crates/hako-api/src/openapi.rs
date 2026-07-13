//! The OpenAPI document, generated from the DTO types themselves so
//! the published contract can never drift from the code.

use utoipa::OpenApi;

/// The daemon's REST + SSE surface as an OpenAPI 3.1 document.
pub fn document() -> utoipa::openapi::OpenApi {
    ApiDoc::openapi()
}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "hako daemon API",
        description = "Commands go up as plain REST; events come down as SSE tailing each \
                       run's append-only event log. Every endpoint requires the daemon's \
                       bearer token."
    ),
    paths(
        paths::submit_run,
        paths::list_runs,
        paths::run_status,
        paths::run_events,
        paths::answer_run,
        paths::resume_run,
        paths::cancel_run
    ),
    modifiers(&BearerAuth),
    security(("bearer" = [])),
    tags((name = "runs", description = "Submitting, observing, and steering runs"))
)]
struct ApiDoc;

/// Registers the single bearer token the whole surface sits behind —
/// a security scheme has no derive, so it joins the document here.
struct BearerAuth;

impl utoipa::Modify for BearerAuth {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme};

        openapi
            .components
            .get_or_insert_default()
            .add_security_scheme(
                "bearer",
                SecurityScheme::Http(HttpBuilder::new().scheme(HttpAuthScheme::Bearer).build()),
            );
    }
}

/// Contract declarations, not handlers: the `#[utoipa::path]`
/// expansion is what the document consumes, so the function bodies
/// stay empty and unused until `hako-server` implements them.
mod paths {
    #![allow(dead_code)]

    use crate::dto::{
        AnswerRequest, ApiError, ListRunsResponse, ResumeRequest, RunStatusResponse,
        SubmitRunRequest, SubmitRunResponse,
    };
    use crate::event::EventEnvelope;

    /// Submit a flow; the run starts immediately and detaches.
    #[utoipa::path(
    post,
    path = "/v1/runs",
    tag = "runs",
    request_body = SubmitRunRequest,
    responses(
        (status = 201, description = "Run accepted and started", body = SubmitRunResponse),
        (status = 400, description = "Malformed request or invalid flow file", body = ApiError),
        (status = 401, description = "Missing or invalid bearer token", body = ApiError),
        (status = 422, description = "Well-formed flow the daemon cannot run — e.g. a referenced secret is not provisioned", body = ApiError),
    )
)]
    fn submit_run() {}

    /// List every run the daemon knows, newest first.
    #[utoipa::path(
    get,
    path = "/v1/runs",
    tag = "runs",
    responses(
        (status = 200, description = "All runs with their current state", body = ListRunsResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ApiError),
    )
)]
    fn list_runs() {}

    /// The full picture of one run.
    #[utoipa::path(
    get,
    path = "/v1/runs/{run_id}",
    tag = "runs",
    params(("run_id" = String, Path, description = "The run to inspect")),
    responses(
        (status = 200, description = "Current status", body = RunStatusResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ApiError),
        (status = 404, description = "No such run", body = ApiError),
    )
)]
    fn run_status() {}

    /// Stream the run's events: full replay from `Last-Event-ID` (or the
    /// beginning), then follow live.
    #[utoipa::path(
    get,
    path = "/v1/runs/{run_id}/events",
    tag = "runs",
    params(
        ("run_id" = String, Path, description = "The run to stream"),
        ("Last-Event-ID" = Option<u64>, Header, description = "Resume after this sequence number instead of replaying from the start"),
    ),
    responses(
        (status = 200, description = "SSE stream; each event's data is one envelope, each event's id is its seq", body = EventEnvelope, content_type = "text/event-stream"),
        (status = 401, description = "Missing or invalid bearer token", body = ApiError),
        (status = 404, description = "No such run", body = ApiError),
    )
)]
    fn run_events() {}

    /// Answer a paused run's questions. Answers are injected into the
    /// next iteration's prompt preamble.
    #[utoipa::path(
    post,
    path = "/v1/runs/{run_id}/answer",
    tag = "runs",
    params(("run_id" = String, Path, description = "The run to answer")),
    request_body = AnswerRequest,
    responses(
        (status = 200, description = "Answers recorded", body = RunStatusResponse),
        (status = 400, description = "Unknown question id", body = ApiError),
        (status = 401, description = "Missing or invalid bearer token", body = ApiError),
        (status = 404, description = "No such run", body = ApiError),
        (status = 409, description = "Run is not awaiting input", body = ApiError),
    )
)]
    fn answer_run() {}

    /// Resume a paused run, optionally with a note and extended budgets.
    #[utoipa::path(
    post,
    path = "/v1/runs/{run_id}/resume",
    tag = "runs",
    params(("run_id" = String, Path, description = "The run to resume")),
    request_body = ResumeRequest,
    responses(
        (status = 200, description = "Run resumed", body = RunStatusResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ApiError),
        (status = 404, description = "No such run", body = ApiError),
        (status = 409, description = "Run is not paused", body = ApiError),
    )
)]
    fn resume_run() {}

    /// Cancel a run cleanly: the sandbox is destroyed, nothing is left
    /// behind, and the state is terminal.
    #[utoipa::path(
    post,
    path = "/v1/runs/{run_id}/cancel",
    tag = "runs",
    params(("run_id" = String, Path, description = "The run to cancel")),
    responses(
        (status = 200, description = "Run cancelled", body = RunStatusResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ApiError),
        (status = 404, description = "No such run", body = ApiError),
        (status = 409, description = "Run already reached a terminal state", body = ApiError),
    )
)]
    fn cancel_run() {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_document_covers_the_whole_command_surface() {
        let doc = document();
        let paths: Vec<&str> = doc.paths.paths.keys().map(String::as_str).collect();
        assert_eq!(
            paths,
            vec![
                "/v1/runs",
                "/v1/runs/{run_id}",
                "/v1/runs/{run_id}/answer",
                "/v1/runs/{run_id}/cancel",
                "/v1/runs/{run_id}/events",
                "/v1/runs/{run_id}/resume",
            ]
        );
    }

    #[test]
    fn every_wire_type_lands_in_components() {
        let doc = document();
        let components = doc.components.expect("document has components");
        for schema in [
            "SubmitRunRequest",
            "SubmitRunResponse",
            "ListRunsResponse",
            "RunSummary",
            "RunStatusResponse",
            "AnswerRequest",
            "ResumeRequest",
            "RunState",
            "EventEnvelope",
            "RunEvent",
            "ProgressReport",
            "ApiError",
        ] {
            assert!(
                components.schemas.contains_key(schema),
                "schema `{schema}` missing from components"
            );
        }
    }

    #[test]
    fn the_bearer_scheme_guards_the_document() {
        let doc = document();
        let components = doc.components.expect("document has components");
        assert!(components.security_schemes.contains_key("bearer"));
        let doc = serde_json::to_value(document()).unwrap();
        assert_eq!(doc["security"], serde_json::json!([{"bearer": []}]));
    }

    #[test]
    fn the_document_serializes() {
        let json = document().to_pretty_json().unwrap();
        assert!(json.contains("\"openapi\""));
    }
}
