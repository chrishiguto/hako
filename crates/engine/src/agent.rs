//! The agent-adapter seam — the engine's knowledge of how to drive one
//! coding-agent CLI.

use crate::budget::TokenUsage;
use crate::sandbox::ExecSpec;
use crate::secrets::SecretName;

/// How to invoke one agent headless, which secrets it needs, and how
/// to read its token usage.
///
/// Adapters are pure translators: every effect runs through `Sandbox`,
/// so an adapter needs no fake of its own beyond scripted return
/// values.
pub trait AgentAdapter: Send + Sync {
    /// The name flows select the agent by, e.g. `claude`.
    fn name(&self) -> &str;

    /// Secret names that must resolve before a run may start. Checked
    /// at submit so a provisioning gap surfaces immediately, not at
    /// iteration N.
    fn required_secrets(&self) -> Vec<SecretName>;

    /// The headless invocation for one iteration, given the fully
    /// composed prompt (engine preamble wrapped around the domain
    /// prompt).
    fn invocation(&self, prompt: &str) -> ExecSpec;

    /// Token usage parsed from the agent's stdout. `None` when this
    /// agent doesn't report usage — the run is then simply not
    /// token-budgeted.
    fn token_usage(&self, stdout: &str) -> Option<TokenUsage>;
}
