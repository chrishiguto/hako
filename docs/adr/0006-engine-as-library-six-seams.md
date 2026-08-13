# The engine is a library with exactly six trait seams

`engine` never depends on `server` or `api`; the daemon is merely the engine's first host. `cli` depends on `api` only — clients (CLI, future web app) are peers consuming one wire contract. All engine I/O flows through exactly six traits — Kernel, Sandbox, AgentAdapter, EventSink, Notifier, SecretsProvider — handed to kernels via a context struct, never reached globally. The payoff: an entire kernel — verify gates, skeptic pass, budgets, pauses, HITL — is testable in-process with fakes: no VMs, no LLMs, no network.

## Consequences

Deliberately not abstracted: workspace preparation (an enum: clone vs mount), verification (kernel logic executed through Sandbox), run persistence (the run directory is the store), and time (tokio's pausable clock in tests). A new seam needs the same justification these six had — a fake is required for testing, or a swap is genuinely plausible.

## Amendment (2026-08-10, #17): five seams reach the kernel, SecretsProvider is spent at submit

Secrets resolve once, before the kernel starts. The host — the only place that sees both the flow and the store — reads the flow's secret names and the adapter's requirements through `SecretsProvider` and hands the kernel the resolved `SecretEnv`: a value in the context, beside `budgets` and `cancel`, not a sixth seam a kernel calls. So the seam count stands at six, but only five of them are collaborators a kernel reaches through `KernelContext`.

Why resolution hoisted out of the loop: a provisioning gap becomes the submission's own answer (422, naming what to provision) instead of a run that fails at iteration 40, and the store is read once per run rather than once per sandbox — four times an iteration — so a store that goes down mid-run cannot kill a run already in flight. The testing payoff is unchanged: a kernel still takes its secrets from the context, and a test still hands it whatever it likes.

## Amendment (2026-08-13, #30): RunSpawner is the seventh seam

Fanout needs to submit child runs and watch their durable events, but the engine must still never depend on its daemon host. `RunSpawner` is therefore the seventh trait seam. The daemon implements it over the run registry; engine tests implement it with an in-memory fake. Child work remains ordinary Pipeline runs, so parallelism composes at the run boundary and the engine learns no registry or tracker concepts.

The resulting boundary has seven traits: Kernel, Sandbox, AgentAdapter, EventSink, Notifier, SecretsProvider, and RunSpawner. Five collaborators reach a kernel through `KernelContext` (Sandbox, AgentAdapter, EventSink, Notifier, RunSpawner); SecretsProvider is spent at submit, and Kernel is the host's entry point.
