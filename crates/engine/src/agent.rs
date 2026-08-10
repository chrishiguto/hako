//! The agent-adapter seam — the engine's knowledge of how to drive one
//! coding-agent CLI.

use crate::budget::TokenUsage;
use crate::sandbox::ExecSpec;
use crate::secrets::SecretRequirement;

/// How to invoke one agent headless, which secrets it needs, and how
/// to read its token usage.
///
/// Adapters are pure translators: every effect runs through `Sandbox`,
/// so an adapter needs no fake of its own beyond scripted return
/// values.
pub trait AgentAdapter: Send + Sync {
    /// The name flows select the agent by, e.g. `claude`.
    fn name(&self) -> &str;

    /// What must be provisioned before a run may start, one
    /// [`SecretRequirement`] per credential the CLI needs — a set of
    /// names rather than one, because an agent that takes either an
    /// API key or an OAuth token runs on either. Checked at submit so
    /// a provisioning gap surfaces immediately, not at iteration N.
    fn required_secrets(&self) -> Vec<SecretRequirement>;

    /// The headless invocation, given the fully composed prompt — the
    /// kernel's framing already applied.
    fn invocation(&self, prompt: &str) -> ExecSpec;

    /// Token usage parsed from the agent's stdout. `None` when this
    /// agent doesn't report usage — the run is then simply not
    /// token-budgeted.
    fn token_usage(&self, stdout: &str) -> Option<TokenUsage>;
}
