# hako Glossary

hako runs unattended ("AFK") coding-agent loops: loop patterns are enforced by code rather than prompting, iterations execute in isolated microVMs, and humans are pulled in only when a loop pauses for them.

## Execution

**Engine**:
The library that executes kernels. Hosted by the daemon in production; embeddable directly in tests and tools.

**Kernel**:
A named loop pattern implemented inside the engine, owning all control flow (iterate, verify, retry, stop). A new loop shape is a new kernel in Rust, never logic in a flow.
_Avoid_: workflow engine, orchestrator

**Pipeline** (v1, specced):
The staged kernel: one iteration drives one work unit through plan → implement → review → simplify → deliver (optional). Stage order and gating live in Rust; flows customize each stage's prompt; stages communicate only through schema-validated stage reports.

**Fanout** (post-v1):
The dispatcher kernel: its plan stage decomposes ready work into independent units and spawns one child Pipeline run per unit — one child, one branch, one PR. Parallelism composes at the run level, never inside a run.

**Flow**:
A TOML file that parameterizes a kernel: agent, budgets, verify checks, workspace, secret names. Contains no logic and no objective — those belong to the kernel and the domain prompt.
_Avoid_: workflow, script

**Run**:
One execution of a flow by the daemon, from submission to a terminal state (done, failed, cancelled), possibly pausing along the way.
_Avoid_: job, session

**Iteration**:
The unit of work within a run: one fresh sandbox, one fresh-context agent invocation, verification, a checkpoint, and a progress report.
_Avoid_: step, turn, cycle

## Isolation & state

**Sandbox**:
The hardware-isolated microVM an iteration executes in, created and destroyed with the iteration.
_Avoid_: container

**Workspace**:
The persistent directory a run works on — a run-owned git clone on a run branch by default, an explicitly mounted checkout otherwise. The only state that survives across iterations.
_Avoid_: checkout, working copy

**Event Log**:
The append-only record of everything a run did; the source of truth for clients, resumption, and audit.
_Avoid_: run history, logs

## Agent interface

**Agent**:
The coding-agent CLI invoked headless inside the sandbox (claude, codex, or any command).
_Avoid_: model, bot

**Agent Adapter**:
The engine's knowledge of how to drive one agent: headless invocation, token-usage reporting, required secrets.

**Domain Prompt**:
A user-authored prompt carrying the objective and the domain rules, never loop mechanics. Which prompt files a kernel reads, and when, is kernel policy.
_Avoid_: system prompt

**Preamble**:
The frame a kernel composes around its prompts: feedback, human answers, and the report contract. The engine supplies the shared pieces; which sections, in what order, is kernel policy.

**Progress Report**:
The schema-validated report an agent writes to end an invocation, carrying the uniform status — continue, done, blocked, or needs_input — plus its kernel's own payload; the shapes are kernel-owned, the status vocabulary shared.
_Avoid_: outputs, output extraction

**Skeptic Iteration**:
A fresh agent invocation prompted to refute a done claim from any stage (see Verified Done).
_Avoid_: review pass

**Verified Done**:
Completion as the engine defines it: a stage claims done, verify checks pass, and a skeptic iteration cannot refute the claim.

## Control

**Verify Checks**:
The commands (build, test, lint) that must pass for an iteration to count as progress; failures retry, then pause or fail the run per the flow's on_fail policy.
_Avoid_: gates, validations

**Budget**:
A soft cap on a run — iterations, wall-clock, or tokens. Exhaustion finishes the current iteration and pauses; it never fails the run.
_Avoid_: limit, quota

**Pause**:
A resumable run state carrying a reason: blocked, verify_failed, drift, budget, or awaiting_human. Every pause notifies the user.
_Avoid_: stop, halt

**Drift**:
Consecutive iterations producing no commits and an unchanged remaining list — the loop is spinning without progress, so it pauses.
_Avoid_: stuck, stall

## Topology

**Daemon**:
`hakod` — the always-on host of the engine and the only network and auth surface. Runs anywhere: laptop or server.
_Avoid_: pool manager, orchestrator

**Client**:
Anything that speaks the API contract: the CLI today, a web control plane later. Clients hold no run state.
_Avoid_: frontend
