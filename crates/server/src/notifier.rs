use std::sync::Arc;

use api::proto::flow::NotifyConfig;
use async_trait::async_trait;
use engine::{Notification, Notifier, NotifierError};
use serde::Serialize;

pub(crate) fn resolve(
    config: Option<&NotifyConfig>,
) -> Result<Arc<dyn Notifier>, NotifierConfigError> {
    match config {
        Some(config) => Ok(Arc::new(WebhookNotifier::new(&config.webhook)?)),
        None => Ok(Arc::new(QuietNotifier)),
    }
}

struct QuietNotifier;

#[async_trait]
impl Notifier for QuietNotifier {
    async fn notify(&self, _notification: &Notification) -> Result<(), NotifierError> {
        Ok(())
    }
}

struct WebhookNotifier {
    client: reqwest::Client,
    target: reqwest::Url,
}

impl WebhookNotifier {
    fn new(target: &str) -> Result<Self, NotifierConfigError> {
        let target = reqwest::Url::parse(target)
            .map_err(|error| NotifierConfigError(format!("invalid webhook URL: {error}")))?;
        if !matches!(target.scheme(), "http" | "https") {
            return Err(NotifierConfigError(
                "webhook URL must use http or https".into(),
            ));
        }
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .map_err(|error| NotifierConfigError(error.without_url().to_string()))?;
        Ok(Self { client, target })
    }
}

#[derive(Serialize)]
struct SlackPayload<'a> {
    text: &'a str,
}

#[async_trait]
impl Notifier for WebhookNotifier {
    async fn notify(&self, notification: &Notification) -> Result<(), NotifierError> {
        let reason = notification.reason.as_str();
        let message = format!(
            "hako run {} paused ({reason}): {}",
            notification.run_id, notification.summary
        );
        let response = self
            .client
            .post(self.target.clone())
            .header(reqwest::header::CONTENT_TYPE, "text/plain; charset=utf-8")
            .body(message.clone())
            .send()
            .await
            .map_err(without_target)?;
        if response.status().is_success() {
            return Ok(());
        }
        if !response.status().is_client_error() {
            return response
                .error_for_status()
                .map(|_| ())
                .map_err(without_target);
        }

        // ntfy accepts the plain body directly; Slack rejects that
        // shape and requires the same message under JSON `text`.
        self.client
            .post(self.target.clone())
            .json(&SlackPayload { text: &message })
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map(|_| ())
            .map_err(without_target)
    }
}

fn without_target(error: reqwest::Error) -> NotifierError {
    NotifierError(error.without_url().to_string())
}

#[derive(Debug, thiserror::Error)]
#[error("invalid notifier configuration: {0}")]
pub(crate) struct NotifierConfigError(String);
