//! The sink seam events flow through. The event vocabulary itself is
//! `proto`'s — the engine emits wire types directly, so the log a sink
//! writes is already the published format the daemon streams verbatim.

use std::borrow::Cow;
use std::sync::Arc;

use async_trait::async_trait;

use crate::secrets::SecretEnv;

pub use proto::event::{EventEnvelope, IterationOutcome, OutputStream, RunEvent};

/// Where a kernel's events go — an append-only log in production, a
/// vector in tests. Serves exactly one run.
#[async_trait]
pub trait EventSink: Send + Sync {
    /// Appends one event. Order is meaning: events must land in the
    /// order they were emitted.
    async fn emit(&self, event: RunEvent) -> Result<(), EventSinkError>;
}

/// An event that could not be recorded. Fatal to a run — a loop whose
/// audit trail has holes must not keep going.
#[derive(Debug, thiserror::Error)]
#[error("event sink failure: {0}")]
pub struct EventSinkError(pub String);

/// The sink a run's events actually go through: every known secret
/// value redacted before the event reaches the sink behind it.
///
/// One wrapper rather than a scrub at each site that builds an event,
/// because the sites are the leak: an agent echoing its environment
/// poisons whichever event happens to carry that text — its output
/// today, its stage report or a rejection message tomorrow. Wrapping
/// the seam covers every variant of [`RunEvent`], including the ones
/// this crate has yet to add.
pub struct ScrubbingSink {
    inner: Arc<dyn EventSink>,
    secrets: SecretEnv,
}

impl ScrubbingSink {
    pub fn new(inner: Arc<dyn EventSink>, secrets: SecretEnv) -> Self {
        Self { inner, secrets }
    }
}

#[async_trait]
impl EventSink for ScrubbingSink {
    async fn emit(&self, event: RunEvent) -> Result<(), EventSinkError> {
        self.inner.emit(scrub(&self.secrets, event)?).await
    }
}

/// Redacts secret values anywhere in an event, through its own wire
/// form: the alternative is a match over every variant and field,
/// which the next event added would silently escape. A run with no
/// secrets pays nothing — the common case short-circuits before the
/// round trip.
fn scrub(secrets: &SecretEnv, event: RunEvent) -> Result<RunEvent, EventSinkError> {
    if secrets.is_empty() {
        return Ok(event);
    }
    let failed = |error: serde_json::Error| EventSinkError(format!("cannot scrub event: {error}"));
    let mut value = serde_json::to_value(&event).map_err(failed)?;
    scrub_json(secrets, &mut value);
    serde_json::from_value(value).map_err(failed)
}

/// Walks every string in the JSON — keys included, because a stage
/// report is the agent's own JSON and an agent can name a field
/// whatever it printed.
fn scrub_json(secrets: &SecretEnv, value: &mut serde_json::Value) {
    match value {
        serde_json::Value::String(text) => {
            if let Cow::Owned(scrubbed) = secrets.scrub(text) {
                *text = scrubbed;
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                scrub_json(secrets, item);
            }
        }
        serde_json::Value::Object(fields) => {
            let mut scrubbed = serde_json::Map::with_capacity(fields.len());
            for (key, mut value) in std::mem::take(fields) {
                scrub_json(secrets, &mut value);
                scrubbed.insert(secrets.scrub(&key).into_owned(), value);
            }
            *fields = scrubbed;
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    use super::*;
    use crate::secrets::{REDACTED, SecretValue};

    #[derive(Default)]
    struct Recording(Mutex<Vec<RunEvent>>);

    #[async_trait]
    impl EventSink for Recording {
        async fn emit(&self, event: RunEvent) -> Result<(), EventSinkError> {
            self.0.lock().unwrap().push(event);
            Ok(())
        }
    }

    fn env() -> SecretEnv {
        SecretEnv::new(BTreeMap::from([(
            "GH_TOKEN".to_owned(),
            SecretValue::new("ghp_leaked"),
        )]))
    }

    async fn through_the_scrub(secrets: SecretEnv, event: RunEvent) -> RunEvent {
        let recording = Arc::new(Recording::default());
        ScrubbingSink::new(recording.clone(), secrets)
            .emit(event)
            .await
            .unwrap();
        recording.0.lock().unwrap().pop().unwrap()
    }

    /// The agent printing its own environment is the case this
    /// exists for.
    #[tokio::test]
    async fn agent_output_carrying_a_secret_reaches_the_sink_redacted() {
        let landed = through_the_scrub(
            env(),
            RunEvent::AgentOutput {
                iteration: 1,
                stream: OutputStream::Stdout,
                chunk: "GH_TOKEN=ghp_leaked\n".into(),
            },
        )
        .await;
        assert_eq!(
            landed,
            RunEvent::AgentOutput {
                iteration: 1,
                stream: OutputStream::Stdout,
                chunk: format!("GH_TOKEN={REDACTED}\n"),
            }
        );
    }

    /// A stage report is the agent's own JSON: nesting, arrays, and
    /// field names it chose are all places a value can hide.
    #[tokio::test]
    async fn a_secret_is_redacted_anywhere_in_a_nested_report() {
        let landed = through_the_scrub(
            env(),
            RunEvent::StageReported {
                iteration: 1,
                stage: "implement".into(),
                report: serde_json::json!({
                    "status": "continue",
                    "notes": ["used ghp_leaked to push"],
                    "env": {"ghp_leaked": {"value": "ghp_leaked"}},
                }),
            },
        )
        .await;
        let RunEvent::StageReported { report, .. } = landed else {
            panic!("the event changed shape");
        };
        assert_eq!(
            report,
            serde_json::json!({
                "status": "continue",
                "notes": [format!("used {REDACTED} to push")],
                "env": {REDACTED: {"value": REDACTED}},
            })
        );
        assert!(!report.to_string().contains("ghp_leaked"), "{report}");
    }

    /// The structural fields a run is read by must survive the round
    /// trip untouched — scrubbing an event is not re-typing it.
    #[tokio::test]
    async fn an_event_without_a_secret_arrives_unchanged() {
        let event = RunEvent::VerifyCheckFinished {
            iteration: 3,
            command: "cargo test".into(),
            passed: false,
            output: "test failed".into(),
        };
        assert_eq!(through_the_scrub(env(), event.clone()).await, event);
        assert_eq!(
            through_the_scrub(SecretEnv::default(), event.clone()).await,
            event
        );
    }
}
