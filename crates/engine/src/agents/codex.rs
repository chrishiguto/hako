//! The built-in adapter for the OpenAI Codex CLI.

use crate::agent::AgentAdapter;
use crate::budget::TokenUsage;
use crate::sandbox::ExecSpec;
use crate::secrets::SecretName;

/// Drives `codex exec` headless: JSONL events on stdout, codex's own
/// sandbox disabled because the microVM is the safety boundary, the
/// git check skipped so mount-mode workspaces need not be repos.
pub struct CodexAdapter;

/// One `--json` event line. Only `turn.completed` carries `usage`;
/// every other event type parses with `usage: None` and is skipped.
#[derive(serde::Deserialize)]
struct EventLine {
    #[serde(rename = "type")]
    kind: String,
    usage: Option<Usage>,
}

/// Codex's `input_tokens` already includes cached input, so it maps
/// straight onto [`TokenUsage::input`].
#[derive(serde::Deserialize)]
struct Usage {
    input_tokens: u64,
    output_tokens: u64,
}

impl AgentAdapter for CodexAdapter {
    fn name(&self) -> &str {
        "codex"
    }

    fn required_secrets(&self) -> Vec<SecretName> {
        // The API-key variable `codex exec` reads; OPENAI_API_KEY only
        // feeds the interactive login flow.
        vec![SecretName::new("CODEX_API_KEY")]
    }

    fn invocation(&self, prompt: &str) -> ExecSpec {
        ExecSpec {
            argv: vec![
                "codex".into(),
                "exec".into(),
                "--json".into(),
                "--sandbox".into(),
                "danger-full-access".into(),
                "--skip-git-repo-check".into(),
                prompt.into(),
            ],
            cwd: None,
        }
    }

    fn token_usage(&self, stdout: &str) -> Option<TokenUsage> {
        stdout
            .lines()
            .filter_map(|line| serde_json::from_str::<EventLine>(line).ok())
            .filter(|event| event.kind == "turn.completed")
            .filter_map(|event| event.usage)
            .map(|usage| TokenUsage {
                input: usage.input_tokens,
                output: usage.output_tokens,
            })
            // A run may complete several turns; the budget cares
            // about their sum. No turns at all means no bill.
            .reduce(|sum, turn| TokenUsage {
                input: sum.input + turn.input,
                output: sum.output + turn.output,
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_invocation_is_exec_json_with_codex_sandboxing_off() {
        let spec = CodexAdapter.invocation("fix the build");
        assert_eq!(
            spec.argv,
            [
                "codex",
                "exec",
                "--json",
                "--sandbox",
                "danger-full-access",
                "--skip-git-repo-check",
                "fix the build",
            ]
        );
        assert_eq!(spec.cwd, None);
    }

    #[test]
    fn the_codex_key_is_required() {
        assert_eq!(
            CodexAdapter.required_secrets(),
            [SecretName::new("CODEX_API_KEY")]
        );
    }

    #[test]
    fn usage_comes_from_the_turn_completed_event() {
        let stdout = concat!(
            r#"{"type":"thread.started","thread_id":"t1"}"#,
            "\n",
            r#"{"type":"item.completed","item":{"type":"agent_message","text":"done"}}"#,
            "\n",
            r#"{"type":"turn.completed","usage":{"input_tokens":24763,"cached_input_tokens":24448,"output_tokens":122}}"#,
            "\n",
        );
        assert_eq!(
            CodexAdapter.token_usage(stdout),
            Some(TokenUsage {
                input: 24763,
                output: 122,
            })
        );
    }

    #[test]
    fn usage_sums_across_turns() {
        let stdout = concat!(
            r#"{"type":"turn.completed","usage":{"input_tokens":100,"output_tokens":10}}"#,
            "\n",
            r#"{"type":"turn.completed","usage":{"input_tokens":200,"output_tokens":20}}"#,
            "\n",
        );
        assert_eq!(
            CodexAdapter.token_usage(stdout),
            Some(TokenUsage {
                input: 300,
                output: 30,
            })
        );
    }

    #[test]
    fn no_usage_without_a_completed_turn() {
        assert_eq!(CodexAdapter.token_usage("plain text, no JSON"), None);
        assert_eq!(
            CodexAdapter.token_usage(r#"{"type":"turn.failed","error":{"message":"boom"}}"#),
            None
        );
        assert_eq!(CodexAdapter.token_usage(""), None);
    }
}
