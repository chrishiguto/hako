//! The generic command-template adapter: any CLI as the agent.

use crate::agent::AgentAdapter;
use crate::agents::AgentConfigError;
use crate::budget::TokenUsage;
use crate::sandbox::ExecSpec;
use crate::secrets::SecretName;

/// The marker a template's argv elements use to receive the composed
/// prompt.
pub const PROMPT_PLACEHOLDER: &str = "{prompt}";

/// Runs whatever argv the flow's `command` template declares, with the
/// composed prompt interpolated. It knows nothing about the CLI it
/// launches: no secrets to require — the flow's `[secrets]` table
/// covers what the command needs — and no token usage to report, so a
/// cmd flow is budgeted by time and iterations only.
pub struct CmdAdapter {
    template: Vec<String>,
}

impl CmdAdapter {
    /// Accepts a template that can actually deliver a prompt: at least
    /// one element, at least one `{prompt}` — anything less would run
    /// the agent blind, so it fails at resolution instead.
    pub fn new(template: Vec<String>) -> Result<Self, AgentConfigError> {
        if template.is_empty() {
            return Err(AgentConfigError::MissingCommand);
        }
        if !template.iter().any(|arg| arg.contains(PROMPT_PLACEHOLDER)) {
            return Err(AgentConfigError::PromptPlaceholderMissing);
        }
        Ok(Self { template })
    }
}

impl AgentAdapter for CmdAdapter {
    fn name(&self) -> &str {
        "cmd"
    }

    fn required_secrets(&self) -> Vec<SecretName> {
        Vec::new()
    }

    fn invocation(&self, prompt: &str) -> ExecSpec {
        ExecSpec {
            argv: self
                .template
                .iter()
                .map(|arg| arg.replace(PROMPT_PLACEHOLDER, prompt))
                .collect(),
            cwd: None,
        }
    }

    fn token_usage(&self, _stdout: &str) -> Option<TokenUsage> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn adapter(template: &[&str]) -> CmdAdapter {
        CmdAdapter::new(template.iter().map(ToString::to_string).collect()).unwrap()
    }

    #[test]
    fn every_placeholder_receives_the_prompt() {
        let spec = adapter(&["aider", "--message", "{prompt}", "--log", "{prompt}.log"])
            .invocation("fix the build");
        assert_eq!(
            spec.argv,
            [
                "aider",
                "--message",
                "fix the build",
                "--log",
                "fix the build.log",
            ]
        );
        assert_eq!(spec.cwd, None);
    }

    #[test]
    fn elements_without_a_placeholder_pass_through_untouched() {
        let spec = adapter(&["my-agent", "{prompt}"]).invocation("a\nmultiline\nprompt");
        assert_eq!(spec.argv, ["my-agent", "a\nmultiline\nprompt"]);
    }

    #[test]
    fn an_empty_template_is_rejected() {
        assert!(matches!(
            CmdAdapter::new(Vec::new()),
            Err(AgentConfigError::MissingCommand)
        ));
    }

    #[test]
    fn a_template_that_never_takes_the_prompt_is_rejected() {
        assert!(matches!(
            CmdAdapter::new(vec!["my-agent".into(), "--yes".into()]),
            Err(AgentConfigError::PromptPlaceholderMissing)
        ));
    }

    #[test]
    fn no_secrets_and_no_usage_even_for_json_stdout() {
        let adapter = adapter(&["my-agent", "{prompt}"]);
        assert_eq!(adapter.required_secrets(), []);
        assert_eq!(
            adapter.token_usage(r#"{"usage":{"input_tokens":9,"output_tokens":1}}"#),
            None
        );
    }
}
