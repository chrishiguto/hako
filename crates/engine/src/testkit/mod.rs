//! In-process fakes for the six seams, a defaulted [`KernelContext`],
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

use crate::budget::Budgets;
use crate::kernel::KernelContext;
use crate::run::RunId;
use crate::workspace::Workspace;
use proto::flow::{PromptsConfig, VerifyConfig};

/// A [`KernelContext`] with every collaborator defaulted, for
/// struct-update syntax: `KernelContext { sandbox, ..context() }`
/// overrides only what a test cares about, and a field the engine
/// grows lands here once with a fake default — not in every test file
/// that constructs a context. The defaults are inert — the sandbox
/// and agent refuse use, the notifier and secrets are quiet, events go
/// to a sink nobody reads — so a forgotten override is a loud panic,
/// never silent green.
pub fn context() -> KernelContext {
    KernelContext {
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
    }
}
