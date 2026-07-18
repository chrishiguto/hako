//! The built-in agent adapters and their resolution from a flow's
//! `[agent]` table. Resolution runs at submit, so a flow naming an
//! engine that doesn't exist — or shaping `command` wrong — fails
//! before a sandbox ever boots.
//!
//! Submit is also as early as these rules *can* run: the engine set
//! is open (ADR 0009), so the schema behind `hako validate` cannot
//! know them, and the CLI never links this crate (ADR 0008). A flow
//! can therefore validate clean locally and still be rejected here —
//! an accepted consequence of keeping the flow language engine-free.

mod claude;
mod cmd;
mod codex;

use std::sync::Arc;

pub use claude::ClaudeAdapter;
pub use cmd::CmdAdapter;
pub use codex::CodexAdapter;
use proto::flow::AgentConfig;

use crate::agent::AgentAdapter;

/// Builds the adapter a flow's `[agent]` table selects.
///
/// The engine name is an open set at the flow-language level (ADR
/// 0009), so this is where a typo like `cluade` surfaces — with the
/// built-in names in the error, mirroring what the schema does for
/// kernels.
pub fn resolve(config: &AgentConfig) -> Result<Arc<dyn AgentAdapter>, AgentConfigError> {
    let adapter: Arc<dyn AgentAdapter> = match config.engine.as_str() {
        // Absent and `[]` deliberately share MissingCommand: the fix
        // is the same either way.
        "cmd" => {
            return Ok(Arc::new(CmdAdapter::new(
                config.command.clone().unwrap_or_default(),
            )?));
        }
        "claude" => Arc::new(ClaudeAdapter),
        "codex" => Arc::new(CodexAdapter),
        other => return Err(AgentConfigError::UnknownEngine(other.to_string())),
    };
    // Checked after the name resolves — a typo with a `command` is
    // still UnknownEngine — and outside the match, so every future
    // built-in inherits the rule instead of re-stating it in a guard.
    if config.command.is_some() {
        return Err(AgentConfigError::CommandNotAllowed(config.engine.clone()));
    }
    Ok(adapter)
}

/// An `[agent]` table no adapter accepts. Variant messages carry the
/// fix, in the flow language's own vocabulary.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum AgentConfigError {
    #[error("unknown agent engine `{0}` — the built-ins are `claude`, `codex`, and `cmd`")]
    UnknownEngine(String),
    #[error("engine `cmd` needs a non-empty `command` argv template")]
    MissingCommand,
    #[error("the `command` template never mentions `{{prompt}}`, so the agent would run blind")]
    PromptPlaceholderMissing,
    #[error("engine `{0}` builds its own invocation — `command` is only for `cmd`")]
    CommandNotAllowed(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(engine: &str, command: Option<&[&str]>) -> AgentConfig {
        AgentConfig {
            engine: engine.into(),
            command: command.map(|argv| argv.iter().map(ToString::to_string).collect()),
        }
    }

    /// `unwrap_err` needs `Debug` on the Ok side, which a trait object
    /// doesn't have.
    fn resolve_err(config: &AgentConfig) -> AgentConfigError {
        resolve(config).err().unwrap()
    }

    #[test]
    fn each_builtin_resolves_under_its_flow_name() {
        for engine in ["claude", "codex"] {
            assert_eq!(resolve(&config(engine, None)).unwrap().name(), engine);
        }
        let cmd = resolve(&config("cmd", Some(&["my-agent", "{prompt}"]))).unwrap();
        assert_eq!(cmd.name(), "cmd");
    }

    #[test]
    fn an_unknown_engine_fails_naming_it_and_the_builtins() {
        let error = resolve_err(&config("cluade", None));
        assert_eq!(error, AgentConfigError::UnknownEngine("cluade".into()));
        let message = error.to_string();
        for name in ["cluade", "claude", "codex", "cmd"] {
            assert!(message.contains(name), "{message}");
        }
    }

    #[test]
    fn an_unknown_engine_with_a_command_is_still_unknown() {
        assert_eq!(
            resolve_err(&config("cluade", Some(&["my-agent", "{prompt}"]))),
            AgentConfigError::UnknownEngine("cluade".into())
        );
    }

    #[test]
    fn cmd_without_a_command_fails() {
        for command in [None, Some(&[][..])] {
            assert_eq!(
                resolve_err(&config("cmd", command)),
                AgentConfigError::MissingCommand
            );
        }
    }

    #[test]
    fn a_command_on_a_builtin_engine_fails() {
        for engine in ["claude", "codex"] {
            assert_eq!(
                resolve_err(&config(engine, Some(&["my-agent", "{prompt}"]))),
                AgentConfigError::CommandNotAllowed(engine.into())
            );
        }
    }
}
