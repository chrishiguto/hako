//! The built-in adapter for the Claude Code CLI.

use crate::agent::AgentAdapter;
use crate::budget::TokenUsage;
use crate::sandbox::ExecSpec;
use crate::secrets::SecretName;

/// Drives `claude` headless: one-shot print mode with JSON output so
/// token usage is parseable, permissions skipped because the microVM
/// is the safety boundary.
pub struct ClaudeAdapter;

/// The result object `--output-format json` prints as the last line
/// of stdout. The `type` tag is part of the match: agent-echoed JSON
/// that happens to carry a `usage` key must never be mistaken for
/// the bill.
#[derive(serde::Deserialize)]
struct ResultLine {
    #[serde(rename = "type")]
    kind: String,
    usage: Usage,
}

/// Claude reports cache traffic separately from `input_tokens`; codex
/// folds it in. Summing the three input-side counts here keeps one
/// `max_tokens` meaning the same thing across engines.
#[derive(serde::Deserialize)]
struct Usage {
    input_tokens: u64,
    #[serde(default)]
    cache_creation_input_tokens: u64,
    #[serde(default)]
    cache_read_input_tokens: u64,
    output_tokens: u64,
}

impl AgentAdapter for ClaudeAdapter {
    fn name(&self) -> &str {
        "claude"
    }

    fn required_secrets(&self) -> Vec<SecretName> {
        vec![SecretName::new("ANTHROPIC_API_KEY")]
    }

    fn invocation(&self, prompt: &str) -> ExecSpec {
        ExecSpec {
            argv: vec![
                "claude".into(),
                "-p".into(),
                prompt.into(),
                "--output-format".into(),
                "json".into(),
                "--dangerously-skip-permissions".into(),
            ],
            cwd: None,
        }
    }

    fn token_usage(&self, stdout: &str) -> Option<TokenUsage> {
        // Scanned from the end: the result object is the last line,
        // but tool output the agent echoed may precede it.
        let result = stdout.lines().rev().find_map(|line| {
            serde_json::from_str::<ResultLine>(line)
                .ok()
                .filter(|result| result.kind == "result")
        })?;
        Some(TokenUsage {
            input: result.usage.input_tokens
                + result.usage.cache_creation_input_tokens
                + result.usage.cache_read_input_tokens,
            output: result.usage.output_tokens,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_invocation_is_one_shot_json_in_the_workspace() {
        let spec = ClaudeAdapter.invocation("fix the build");
        assert_eq!(
            spec.argv,
            [
                "claude",
                "-p",
                "fix the build",
                "--output-format",
                "json",
                "--dangerously-skip-permissions",
            ]
        );
        assert_eq!(spec.cwd, None);
    }

    #[test]
    fn the_anthropic_key_is_required() {
        assert_eq!(
            ClaudeAdapter.required_secrets(),
            [SecretName::new("ANTHROPIC_API_KEY")]
        );
    }

    #[test]
    fn usage_sums_cache_traffic_into_input() {
        let stdout = concat!(
            "some tool chatter\n",
            r#"{"type":"result","subtype":"success","result":"done","#,
            r#""usage":{"input_tokens":100,"cache_creation_input_tokens":20,"#,
            r#""cache_read_input_tokens":3000,"output_tokens":45},"total_cost_usd":0.12}"#,
            "\n",
        );
        assert_eq!(
            ClaudeAdapter.token_usage(stdout),
            Some(TokenUsage {
                input: 3120,
                output: 45,
            })
        );
    }

    #[test]
    fn usage_survives_absent_cache_fields() {
        let stdout = r#"{"type":"result","usage":{"input_tokens":7,"output_tokens":2}}"#;
        assert_eq!(
            ClaudeAdapter.token_usage(stdout),
            Some(TokenUsage {
                input: 7,
                output: 2,
            })
        );
    }

    #[test]
    fn echoed_json_with_a_usage_key_is_not_the_bill() {
        let stdout = r#"{"type":"api_response","usage":{"input_tokens":9,"output_tokens":9}}"#;
        assert_eq!(ClaudeAdapter.token_usage(stdout), None);
    }

    #[test]
    fn no_usage_when_stdout_is_not_the_json_contract() {
        assert_eq!(ClaudeAdapter.token_usage("plain text, no JSON"), None);
        assert_eq!(
            ClaudeAdapter.token_usage(r#"{"type":"result","result":"no usage key"}"#),
            None
        );
        assert_eq!(ClaudeAdapter.token_usage(""), None);
    }
}
