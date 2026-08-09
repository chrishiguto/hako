//! In-process fakes for the six seams, a [`KernelContext`] builder,
//! and real-git fixtures — the payoff of the seam design, paid once.
//!
//! Feature-gated behind `testkit` so product builds never carry it:
//! the engine's own tests switch it on through a dev-dependency on
//! itself, and other crates enable it from their dev-dependencies the
//! same way. Nothing here is a test double of engine *logic* — every
//! fake sits behind a seam trait, and tests keep asserting emitted
//! events, run outcomes, and git effects, never internal call
//! patterns.

mod fakes;
mod repo;
mod sandbox;

pub use fakes::{
    AGENT_BIN, MapSecrets, NoAgent, NoSecrets, RecordingNotifier, RecordingSink, ScriptedAgent,
    StubNotifier, Transcript, exec,
};
pub use repo::{SEED_FILE, commit, git, git_stdout, head, seeded_repo, tracked_files};
pub use sandbox::{
    AgentStep, NoSandbox, ScriptedSandbox, StagedSandbox, crashes, malformed, omits_report, reports,
};

use std::sync::Arc;

use crate::agent::AgentAdapter;
use crate::budget::Budgets;
use crate::event::EventSink;
use crate::kernel::KernelContext;
use crate::notify::Notifier;
use crate::run::RunId;
use crate::sandbox::Sandbox;
use crate::secrets::SecretsProvider;
use crate::workspace::Workspace;
use proto::flow::{PromptsConfig, VerifyConfig};

/// A [`KernelContext`] ready to build: every collaborator defaulted so
/// a test overrides only what it cares about. The defaults are inert —
/// the sandbox and agent refuse use, the notifier and secrets are
/// quiet, events go to a sink nobody reads — so a context is one line
/// where nothing is exercised, and a forgotten override is a loud
/// panic, never silent green.
pub fn context() -> ContextBuilder {
    ContextBuilder {
        ctx: KernelContext {
            run_id: RunId::new("r1"),
            budgets: Budgets::default(),
            verify: VerifyConfig::default(),
            prompts: PromptsConfig::default(),
            workspace: Workspace::at("/srv/runs/r1/workspace"),
            sandbox: Arc::new(NoSandbox),
            agent: Arc::new(NoAgent),
            events: Arc::new(RecordingSink::default()),
            notifier: Arc::new(StubNotifier),
            secrets: Arc::new(NoSecrets),
        },
    }
}

/// Builds a [`KernelContext`] from [`context`]'s defaults. A field the
/// engine grows lands here once, with a fake default — not in every
/// test file that constructs a context.
pub struct ContextBuilder {
    ctx: KernelContext,
}

impl ContextBuilder {
    pub fn run_id(mut self, run_id: RunId) -> Self {
        self.ctx.run_id = run_id;
        self
    }

    pub fn budgets(mut self, budgets: Budgets) -> Self {
        self.ctx.budgets = budgets;
        self
    }

    pub fn verify(mut self, verify: VerifyConfig) -> Self {
        self.ctx.verify = verify;
        self
    }

    pub fn prompts(mut self, prompts: PromptsConfig) -> Self {
        self.ctx.prompts = prompts;
        self
    }

    pub fn workspace(mut self, workspace: Workspace) -> Self {
        self.ctx.workspace = workspace;
        self
    }

    pub fn sandbox(mut self, sandbox: Arc<dyn Sandbox>) -> Self {
        self.ctx.sandbox = sandbox;
        self
    }

    pub fn agent(mut self, agent: Arc<dyn AgentAdapter>) -> Self {
        self.ctx.agent = agent;
        self
    }

    pub fn events(mut self, events: Arc<dyn EventSink>) -> Self {
        self.ctx.events = events;
        self
    }

    pub fn notifier(mut self, notifier: Arc<dyn Notifier>) -> Self {
        self.ctx.notifier = notifier;
        self
    }

    pub fn secrets(mut self, secrets: Arc<dyn SecretsProvider>) -> Self {
        self.ctx.secrets = secrets;
        self
    }

    pub fn build(self) -> KernelContext {
        self.ctx
    }
}
