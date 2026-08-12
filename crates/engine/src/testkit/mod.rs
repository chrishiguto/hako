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
mod prompt;
mod repo;
mod sandbox;

pub use fakes::{
    AGENT_BIN, MapSecrets, NoAgent, NoSecrets, RecordingNotifier, RecordingSink, ScriptedAgent,
    StubNotifier, Transcript, exec, secret_env,
};
pub use prompt::{
    SKEPTIC_PROMPT_HEADING, carries_handoff, carries_human_input, carries_report_from,
    carries_verify_feedback,
};
pub use repo::{SEED_FILE, commit, git, git_stdout, head, seeded_repo, tracked_files};
pub use sandbox::{
    AgentStep, NoSandbox, ScriptedSandbox, StagedSandbox, UNREFUTED_SKEPTIC_REPORT, crashes,
    malformed, omits_report, reports,
};

use std::sync::Arc;

use crate::budget::{BudgetUsage, Budgets};
use crate::cancel::CancelToken;
use crate::event::RunEvent;
use crate::kernel::KernelContext;
use crate::run::RunId;
use crate::secrets::SecretEnv;
use crate::workspace::Workspace;
use proto::flow::{PromptsConfig, VerifyConfig};

/// Each event's wire `type` tag, in order — the compact sequence
/// event-flow assertions compare against.
pub fn kinds(events: &[RunEvent]) -> Vec<String> {
    events
        .iter()
        .map(|event| {
            serde_json::to_value(event).unwrap()["type"]
                .as_str()
                .unwrap()
                .to_owned()
        })
        .collect()
}

/// A [`KernelContext`] with every collaborator defaulted, for
/// struct-update syntax: `KernelContext { sandbox, ..context() }`
/// overrides only what a test cares about, and a field the engine
/// grows lands here once with a fake default — not in every test file
/// that constructs a context. The defaults are inert — the sandbox
/// and agent refuse use, the notifier is quiet, the run holds no
/// secrets, events go to a sink nobody reads — so a forgotten
/// override is a loud panic, never silent green.
pub fn context() -> KernelContext {
    KernelContext {
        run_id: RunId::new("r1"),
        budgets: Budgets::default(),
        budget_usage: BudgetUsage::default(),
        resume: None,
        // A fresh token nobody holds the other end of: never fires.
        cancel: CancelToken::new(),
        verify: VerifyConfig::default(),
        prompts: PromptsConfig::default(),
        workspace: Workspace::at("/srv/runs/r1/workspace"),
        sandbox: Arc::new(NoSandbox),
        agent: Arc::new(NoAgent),
        events: Arc::new(RecordingSink::default()),
        notifier: Arc::new(StubNotifier),
        secrets: SecretEnv::default(),
    }
}
