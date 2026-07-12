# The engine is a library with exactly six trait seams

`hako-engine` never depends on `hako-server` or `hako-api`; the daemon is merely the engine's first host. `hako-cli` depends on `hako-api` only — clients (CLI, future web app) are peers consuming one wire contract. All engine I/O flows through exactly six traits — Kernel, Sandbox, AgentAdapter, EventSink, Notifier, SecretsProvider — handed to kernels via a context struct, never reached globally. The payoff: the entire Ralph kernel (verify gates, skeptic pass, budgets, pauses, HITL) is testable in-process with fakes — no VMs, no LLMs, no network.

## Consequences

Deliberately not abstracted: workspace preparation (an enum: clone vs mount), verification (kernel logic executed through Sandbox), run persistence (the run directory is the store), and time (tokio's pausable clock in tests). A new seam needs the same justification these six had — a fake is required for testing, or a swap is genuinely plausible.
