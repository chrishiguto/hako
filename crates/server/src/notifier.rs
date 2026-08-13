use std::sync::Arc;

use api::proto::flow::{NotifyConfig, NotifyFormat};
use async_trait::async_trait;
use engine::{Notification, Notifier, NotifierError};
use serde::Serialize;

pub(crate) fn resolve(
    config: Option<&NotifyConfig>,
) -> Result<Arc<dyn Notifier>, NotifierConfigError> {
    match config {
        Some(config) => Ok(Arc::new(WebhookNotifier::new(
            &config.webhook,
            config.format,
        )?)),
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
    format: NotifyFormat,
}

impl WebhookNotifier {
    fn new(target: &str, format: NotifyFormat) -> Result<Self, NotifierConfigError> {
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
        Ok(Self {
            client,
            target,
            format,
        })
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
        let message = match &notification.summary {
            Some(summary) => format!(
                "hako run {} paused ({reason}): {summary}",
                notification.run_id
            ),
            None => format!("hako run {} paused ({reason})", notification.run_id),
        };
        let request = self.client.post(self.target.clone());
        let request = match self.format {
            NotifyFormat::Text => request
                .header(reqwest::header::CONTENT_TYPE, "text/plain; charset=utf-8")
                .body(message),
            NotifyFormat::Slack => request.json(&SlackPayload { text: &message }),
        };
        request
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map(|_| ())
            .map_err(without_target)
    }
}

/// A webhook URL is itself a credential — a Slack incoming hook
/// carries its token in the path — and this string surfaces in the
/// run's log, so the target never rides along with the failure.
fn without_target(error: reqwest::Error) -> NotifierError {
    NotifierError(error.without_url().to_string())
}

#[derive(Debug, thiserror::Error)]
#[error("invalid notifier configuration: {0}")]
pub(crate) struct NotifierConfigError(String);
