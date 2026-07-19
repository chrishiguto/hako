# Fresh microVM per iteration; the workspace is the only memory

Each iteration boots an ephemeral hardware-isolated microVM, runs the agent with a fresh context, and destroys the VM; only the workspace (git-checkpointed every iteration) persists. Chosen over one VM per run so that a fresh context per agent invocation is structurally enforced rather than promised: an unattended loop's guardrails assume each invocation sees only what the engine hands it, and leftover processes, environment drift, and temp-file junk cannot leak between invocations except through the workspace, the one channel memory is supposed to flow through.

## Consequences

Toolchains must be baked into the sandbox image (ad-hoc installs don't survive an iteration), and the per-iteration boot cost (sub-second) is accepted as the price of hermeticity.
