# Status semantics are kernel-owned, not prompt-programmable; reports use parse-and-repair, not constrained decoding

The schema-validated report is the only structured channel from agent
to engine (ADR 0007), and a kernel composes each stage's prompt as an
overridable domain section — the flow's `[prompts]` slot or the
kernel-shipped default — wrapped by a kernel-authored report contract.
The meaning of the uniform status (continue | done | blocked |
needs_input), and how the loop branches on it — done ends the run,
blocked/needs_input pause it, continue advances — belong to that
contract section, never to the domain prompt. Editing a domain prompt
customizes the objective and the domain rules; it can never reach the
control flow, because the vocabulary the loop dispatches on is quoted
by the kernel, not authored by the user.

That report is enforced by quoting the stage's JSON Schema verbatim
into the contract, strict-parsing what the agent writes back
(`deny_unknown_fields`), and spending exactly one repair re-prompt —
the schema re-quoted with the parse errors — before the invocation
counts as failed. We deliberately do not use provider-native
constrained decoding, JSON mode, or function-calling. The agent is a
black-box CLI invoked headless inside the iteration's microVM (ADR
0003); the kernel exec's it and reads a report file back through the
sandbox, and never touches the model's decoding step. Parse-and-repair
is therefore the only enforcement available to us — and, being
model-agnostic, the only one that keeps any agent CLI a first-class
citizen.

## Considered Options

- Provider-native constrained decoding / JSON mode / function-calling
  — rejected, and mostly unavailable: the kernel drives an opaque
  agent CLI, so it never controls the sampling loop, and binding the
  contract to one provider's structured-output API would break the
  model-agnostic agent boundary. The evidence also cuts against it on
  the merits — agents that do control the request (SWE-agent,
  OpenHands, Cline) default to native tool-calling yet keep a text
  parse-and-repair fallback, Aider benchmarks JSON/function-calling as
  worse for code edits, and BAML benchmarks prompt-plus-forgiving-parse
  beating function-calling on accuracy and cost. Parse-and-repair is
  the right floor; for our constraint it is also the ceiling.
- Status semantics in the domain prompt (the overridable `[prompts]`
  slot) — rejected: a custom prompt could silently drop or contradict
  the meaning the loop branches on, and a mislabelled `done` ends a run
  early. AutoGen's "reply TERMINATE when done" keyword is the
  documented failure of this shape — order-fragile, and unreliable
  enough that its v0.4 redesign moved termination out of the prompt
  into typed code objects. Keeping the meaning in the kernel-authored
  contract makes it non-overridable by construction.
- Lenient parsing over strict `deny_unknown_fields` — rejected for v1:
  both ends of the wire ship from this workspace in lockstep (ADR 0008,
  0010), so an unknown or mistyped key is contract drift to surface,
  not verbosity to absorb. Strict parse plus one repair fails loudly
  where a forgiving parser would hide the drift.

## Consequences

The `[prompts]` table customizes domain rules only; the status
vocabulary, its meaning, and the loop's branch on it are kernel
property. Each stage's domain prompt still says what counts as done for
that stage's own work — genuinely domain judgement — while the contract
owns what done does to the run. Third-party kernels (post-v1, ADR 0010)
inherit the guarantee: a kernel authors its own contract section, so no
flow it runs can reprogram its control flow. This refines ADR 0007 by
naming the prompt-versus-contract ownership split and the
constrained-decoding alternative it left implicit, and follows from ADR
0001: control flow is kernel code, so the semantics that drive it are
kernel code too.
