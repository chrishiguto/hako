//! Fakes for the agent, event, notifier, and secrets seams — pure
//! in-memory stand-ins a test reads back after the run.

use std::collections::BTreeMap;
use std::sync::Mutex;

use async_trait::async_trait;

use crate::agent::AgentAdapter;
use crate::budget::TokenUsage;
use crate::event::{EventSink, EventSinkError, RunEvent};
use crate::notify::{Notification, Notifier, NotifierError};
use crate::sandbox::{ExecEvent, ExecSpec, ExitStatus, SandboxError};
use crate::secrets::{
    SecretEnv, SecretName, SecretRequirement, SecretValue, SecretsError, SecretsProvider,
};

/// One scripted exec: the events its stream replays.
pub type Transcript = Vec<Result<ExecEvent, SandboxError>>;

/// A transcript that prints `stdout` and exits with `code`.
pub fn exec(stdout: &str, code: i32) -> Transcript {
    vec![
        Ok(ExecEvent::Stdout(stdout.as_bytes().to_vec())),
        Ok(ExecEvent::Exited(ExitStatus { code: Some(code) })),
    ]
}

/// The scripted agent's binary name — argv[0] of every invocation it
/// builds, and the marker [`super::StagedSandbox`] keys on to tell an
/// agent exec from a verify check.
pub const AGENT_BIN: &str = "scripted-agent";

/// Recovers the prompt from an invocation [`ScriptedAgent`] built —
/// the argv layout's one decoder, kept beside its encoder.
pub(super) fn prompt_from(argv: &[String]) -> Option<&str> {
    match argv {
        [bin, flag, prompt] if bin == AGENT_BIN && flag == "--prompt" => Some(prompt),
        _ => None,
    }
}

/// A pure translator, like every real adapter: prompt in, argv out.
/// By default it requires no secrets and reports no usage; a test that
/// cares opts in.
#[derive(Default)]
pub struct ScriptedAgent {
    secrets: Vec<SecretRequirement>,
    usage: Option<TokenUsage>,
}

impl ScriptedAgent {
    pub fn new() -> Self {
        Self::default()
    }

    /// Declares a secret the agent requires, as a real adapter would.
    pub fn requiring(mut self, secret: &str) -> Self {
        self.secrets.push(SecretRequirement::named(secret));
        self
    }

    /// Reports this usage whenever the stdout carries the
    /// `tokens used` marker — the adapter-parses-stdout contract.
    pub fn reporting(mut self, usage: TokenUsage) -> Self {
        self.usage = Some(usage);
        self
    }
}

impl AgentAdapter for ScriptedAgent {
    fn name(&self) -> &str {
        "scripted"
    }

    fn required_secrets(&self) -> Vec<SecretRequirement> {
        self.secrets.clone()
    }

    fn invocation(&self, prompt: &str) -> ExecSpec {
        ExecSpec {
            argv: vec![AGENT_BIN.into(), "--prompt".into(), prompt.into()],
            cwd: None,
        }
    }

    fn token_usage(&self, stdout: &str) -> Option<TokenUsage> {
        self.usage.filter(|_| stdout.contains("tokens used"))
    }
}

/// An agent that must never be invoked — for tests whose kernel talks
/// to no agent, where an invocation is a test bug.
pub struct NoAgent;

impl AgentAdapter for NoAgent {
    fn name(&self) -> &str {
        "none"
    }

    fn required_secrets(&self) -> Vec<SecretRequirement> {
        vec![]
    }

    fn invocation(&self, _prompt: &str) -> ExecSpec {
        unreachable!("this test invokes no agent");
    }

    fn token_usage(&self, _stdout: &str) -> Option<TokenUsage> {
        None
    }
}

/// Records every emitted event for the test to assert on — the house
/// pattern's primary witness.
#[derive(Default)]
pub struct RecordingSink {
    events: Mutex<Vec<RunEvent>>,
}

impl RecordingSink {
    pub fn events(&self) -> Vec<RunEvent> {
        self.events.lock().unwrap().clone()
    }
}

#[async_trait]
impl EventSink for RecordingSink {
    async fn emit(&self, event: RunEvent) -> Result<(), EventSinkError> {
        self.events.lock().unwrap().push(event);
        Ok(())
    }
}

/// Swallows notifications — for tests that don't care who was told.
pub struct StubNotifier;

#[async_trait]
impl Notifier for StubNotifier {
    async fn notify(&self, _notification: &Notification) -> Result<(), NotifierError> {
        Ok(())
    }
}

/// Records notifications for the test to assert on.
#[derive(Default)]
pub struct RecordingNotifier {
    notifications: Mutex<Vec<Notification>>,
}

impl RecordingNotifier {
    pub fn notifications(&self) -> Vec<Notification> {
        self.notifications.lock().unwrap().clone()
    }
}

#[async_trait]
impl Notifier for RecordingNotifier {
    async fn notify(&self, notification: &Notification) -> Result<(), NotifierError> {
        self.notifications
            .lock()
            .unwrap()
            .push(notification.clone());
        Ok(())
    }
}

/// A run's resolved secrets, built the short way — what a host hands
/// a kernel once resolution is done, so a test that only cares what
/// the loop *spends* skips the provider entirely.
pub fn secret_env<'a>(secrets: impl IntoIterator<Item = (&'a str, &'a str)>) -> SecretEnv {
    SecretEnv::new(
        secrets
            .into_iter()
            .map(|(name, value)| (name.to_owned(), SecretValue::new(value)))
            .collect(),
    )
}

/// A provider with nothing in it: every resolve is a miss.
pub struct NoSecrets;

#[async_trait]
impl SecretsProvider for NoSecrets {
    async fn resolve(&self, name: &SecretName) -> Result<SecretValue, SecretsError> {
        Err(SecretsError::NotFound(name.clone()))
    }
}

/// A provider over a fixed map of secrets.
pub struct MapSecrets(BTreeMap<SecretName, SecretValue>);

impl MapSecrets {
    pub fn new<'a>(secrets: impl IntoIterator<Item = (&'a str, &'a str)>) -> Self {
        Self(
            secrets
                .into_iter()
                .map(|(name, value)| (SecretName::new(name), SecretValue::new(value)))
                .collect(),
        )
    }
}

#[async_trait]
impl SecretsProvider for MapSecrets {
    async fn resolve(&self, name: &SecretName) -> Result<SecretValue, SecretsError> {
        self.0
            .get(name)
            .cloned()
            .ok_or_else(|| SecretsError::NotFound(name.clone()))
    }
}
