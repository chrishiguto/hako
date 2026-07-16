# Progress is a schema-validated report; "done" must survive a skeptic

The only structured channel from agent to engine is the progress report the agent writes at the end of each iteration (continue | done | blocked | needs_input), validated against a published JSON Schema with one repair re-prompt before the iteration counts as failed. A done claim is accepted only when verify checks pass AND a fresh skeptic iteration — new sandbox, new context, prompted to find evidence the domain prompt's objective is unmet — fails to refute it.

## Considered Options

- Scraping agent stdout with regex/jq extractors (the predecessor's path) — rejected: brittle plumbing that dominated flow files.
- Trusting the agent's done claim — rejected: premature victory on placeholder work is the documented core failure mode of unattended loops.

## Consequences

Completion costs one extra agent invocation (the skeptic) per claimed done; a disputing skeptic's findings feed the next iteration's preamble, turning refutation into loop memory.
