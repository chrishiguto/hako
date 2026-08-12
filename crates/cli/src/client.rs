use std::time::Duration;

use api::{
    Answer, AnswerRequest, ApiError, ListRunsResponse, ResumeRequest, RunStatusResponse,
    SubmitRunRequest, SubmitRunResponse,
};
use serde::de::DeserializeOwned;

pub(crate) struct Client {
    agent: ureq::Agent,
    stream_agent: ureq::Agent,
    address: String,
    authorization: String,
}

impl Client {
    /// `address` is the canonical base URL from
    /// [`crate::config::connection`] — scheme-prefixed, slash-trimmed.
    /// No address grammar lives here.
    pub(crate) fn new(address: &str, token: &str) -> Self {
        let agent = ureq::Agent::config_builder()
            .timeout_global(Some(Duration::from_secs(10)))
            .http_status_as_error(false)
            .build()
            .into();
        let stream_agent = ureq::Agent::config_builder()
            .timeout_connect(Some(Duration::from_secs(10)))
            .timeout_recv_response(Some(Duration::from_secs(10)))
            .http_status_as_error(false)
            .build()
            .into();
        Self {
            agent,
            stream_agent,
            address: address.to_owned(),
            authorization: format!("Bearer {token}"),
        }
    }

    pub(crate) fn submit(&self, flow: &str) -> Result<SubmitRunResponse, ClientError> {
        parse(
            self.agent
                .post(&format!("{}/v1/runs", self.address))
                .header("Authorization", &self.authorization)
                .send_json(&SubmitRunRequest {
                    flow: flow.to_owned(),
                }),
        )
    }

    pub(crate) fn list(&self) -> Result<ListRunsResponse, ClientError> {
        parse(
            self.agent
                .get(&format!("{}/v1/runs", self.address))
                .header("Authorization", &self.authorization)
                .call(),
        )
    }

    pub(crate) fn status(&self, run_id: &str) -> Result<RunStatusResponse, ClientError> {
        parse(
            self.agent
                .get(&format!("{}/v1/runs/{run_id}", self.address))
                .header("Authorization", &self.authorization)
                .call(),
        )
    }

    pub(crate) fn answer(
        &self,
        run_id: &str,
        question_id: &str,
        answer: &str,
    ) -> Result<RunStatusResponse, ClientError> {
        parse(
            self.agent
                .post(&format!("{}/v1/runs/{run_id}/answer", self.address))
                .header("Authorization", &self.authorization)
                .send_json(&AnswerRequest {
                    answers: vec![Answer {
                        question_id: question_id.to_owned(),
                        answer: answer.to_owned(),
                    }],
                }),
        )
    }

    pub(crate) fn resume(
        &self,
        run_id: &str,
        note: Option<String>,
    ) -> Result<RunStatusResponse, ClientError> {
        parse(
            self.agent
                .post(&format!("{}/v1/runs/{run_id}/resume", self.address))
                .header("Authorization", &self.authorization)
                .send_json(&ResumeRequest { note, extend: None }),
        )
    }

    pub(crate) fn cancel(&self, run_id: &str) -> Result<RunStatusResponse, ClientError> {
        parse(
            self.agent
                .post(&format!("{}/v1/runs/{run_id}/cancel", self.address))
                .header("Authorization", &self.authorization)
                .send_empty(),
        )
    }

    pub(crate) fn events(
        &self,
        run_id: &str,
        last_event_id: Option<u64>,
    ) -> Result<ureq::http::Response<ureq::Body>, ClientError> {
        let mut request = self
            .stream_agent
            .get(&format!("{}/v1/runs/{run_id}/events", self.address))
            .header("Authorization", &self.authorization);
        if let Some(id) = last_event_id {
            request = request.header("Last-Event-ID", id.to_string());
        }
        response(request.call())
    }
}

fn parse<T: DeserializeOwned>(
    result: Result<ureq::http::Response<ureq::Body>, ureq::Error>,
) -> Result<T, ClientError> {
    let mut response = response(result)?;
    response
        .body_mut()
        .read_json()
        .map_err(|error| ClientError::Response(format!("invalid daemon response: {error}")))
}

fn response(
    result: Result<ureq::http::Response<ureq::Body>, ureq::Error>,
) -> Result<ureq::http::Response<ureq::Body>, ClientError> {
    let mut response = result.map_err(ClientError::Transport)?;
    let status = response.status();
    if !status.is_success() {
        return Err(rejection(status, response.body_mut()));
    }
    Ok(response)
}

#[derive(Debug)]
pub(crate) enum ClientError {
    Transport(ureq::Error),
    Response(String),
}

impl ClientError {
    pub(crate) fn is_transport(&self) -> bool {
        matches!(self, Self::Transport(_))
    }
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transport(error) => error.fmt(formatter),
            Self::Response(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for ClientError {}

/// The command name is the caller's to add — [`crate::main`] prefixes
/// every failure with the operation it was performing, so naming it
/// here too would say it twice.
fn rejection(status: impl std::fmt::Display, body: &mut ureq::Body) -> ClientError {
    let detail = body
        .read_json::<ApiError>()
        .map(|error| format!(": {}", error.message))
        .unwrap_or_default();
    ClientError::Response(format!(
        "daemon rejected the request with HTTP {status}{detail}"
    ))
}
