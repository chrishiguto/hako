# Agent reports are schema-validated; "done" must survive a skeptic

The only structured channel from agent to engine is the schema-validated report the agent writes to end each invocation — a stage report carrying the uniform status (continue | done | blocked | needs_input) plus its stage's payload — validated against a published JSON Schema with one repair re-prompt before the invocation counts as failed. A done claim is accepted only when verify checks pass AND a fresh skeptic iteration — new sandbox, new context, prompted to find evidence the domain prompts' objective is unmet — fails to refute it.

## Considered Options

- Scraping agent stdout with regex/jq extractors (the predecessor's path) — rejected: brittle plumbing that dominated flow files.
- Trusting the agent's done claim — rejected: premature victory on placeholder work is the documented core failure mode of unattended loops.

## Consequences

Completion costs one extra agent invocation (the skeptic) per claimed done; a disputing skeptic's findings feed the next iteration's preamble, turning refutation into loop memory.
