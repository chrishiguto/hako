use std::time::Duration;

use api::{ApiError, ListRunsResponse, SubmitRunRequest, SubmitRunResponse};
use serde::de::DeserializeOwned;

pub(crate) struct Client {
    agent: ureq::Agent,
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
        Self {
            agent,
            address: address.to_owned(),
            authorization: format!("Bearer {token}"),
        }
    }

    pub(crate) fn submit(&self, flow: &str) -> Result<SubmitRunResponse, ClientError> {
        parse(
            "submission",
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
            "list",
            self.agent
                .get(&format!("{}/v1/runs", self.address))
                .header("Authorization", &self.authorization)
                .call(),
        )
    }
}

fn parse<T: DeserializeOwned>(
    operation: &str,
    result: Result<ureq::http::Response<ureq::Body>, ureq::Error>,
) -> Result<T, ClientError> {
    let mut response = result.map_err(ClientError::Transport)?;
    let status = response.status();
    if !status.is_success() {
        return Err(rejection(operation, status, response.body_mut()));
    }
    response
        .body_mut()
        .read_json()
        .map_err(|error| ClientError::Response(format!("invalid daemon response: {error}")))
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

fn rejection(
    operation: &str,
    status: impl std::fmt::Display,
    body: &mut ureq::Body,
) -> ClientError {
    let detail = body
        .read_json::<ApiError>()
        .map(|error| format!(": {}", error.message))
        .unwrap_or_default();
    ClientError::Response(format!(
        "daemon rejected {operation} with HTTP {status}{detail}"
    ))
}
