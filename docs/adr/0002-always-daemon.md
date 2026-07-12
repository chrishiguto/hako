# Always-daemon execution (docker model)

Runs execute only inside the daemon, local or remote; the CLI is a pure API client (submit, attach, answer, resume, cancel) that auto-starts a local daemon when absent. Chosen over an embedded in-process mode because AFK runs need detach-by-default — they must survive the terminal and the laptop — and one execution path keeps run state, auth, and recovery in one place.

## Consequences

This is a product rule, not a code rule: the engine remains an embeddable library (ADR-0006), and tests or dev binaries may drive it in-process.
