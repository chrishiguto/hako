# Documentation Index

Machine-friendly index of this repo's durable documents. One entry per document, ~200 characters:

`path — Type (Status) — one-line summary. Topics: comma, separated, keywords.`

Check here first to find the right document, then open that file directly. `/domain-modeling` adds or refreshes an entry whenever it writes an ADR or the glossary.

## ADRs

- `docs/adr/0001-kernels-own-control-flow.md` — ADR (Accepted) — Loop patterns are Rust kernels; flow files parameterize, never contain logic. Topics: kernel, flow, config, expression language, scripting.
- `docs/adr/0002-always-daemon.md` — ADR (Accepted) — Runs execute only inside the daemon; CLI is a pure client with auto-start. Topics: daemon, CLI, detach, docker model.
- `docs/adr/0003-fresh-vm-per-iteration.md` — ADR (Accepted) — Ephemeral microVM per iteration; workspace is the sole memory channel. Topics: iteration, sandbox, isolation, workspace, hermeticity.
- `docs/adr/0004-smolvm-behind-sandbox-trait.md` — ADR (Accepted) — In-house VM stack dropped; smolvm CLI wrapped behind a pinned Sandbox trait. Topics: smolvm, sandbox, trait, backend, risk.
- `docs/adr/0005-rest-sse-over-event-log.md` — ADR (Accepted) — Append-only event log as source of truth; SSE down, REST up; WebSocket deferred. Topics: protocol, SSE, event log, replay, API.
- `docs/adr/0006-engine-as-library-six-seams.md` — ADR (Accepted) — Engine never depends on server/api; six trait seams; kernel testable with fakes. Topics: crates, dependencies, seams, testing.
- `docs/adr/0007-progress-report-verified-done.md` — ADR (Accepted) — Schema-validated progress report is the agent→engine channel; done requires a skeptic pass. Topics: progress, verified done, skeptic, schema.
- `docs/adr/0008-shared-proto-crate.md` — ADR (Accepted) — Wire types defined once in leaf crate `proto`, shared by engine and api; replaces mirrored types + golden fixture; amends 0006's client rule. Topics: proto, wire contract, published language, crates, dependencies.
- `docs/adr/0009-flow-language-in-proto.md` — ADR (Accepted) — Flow config types live in proto; every Rust consumer shares one strict parser; the JSON Schema is generated for editors/LLMs only. Topics: flow, config, proto, schema, validate, published language.

## Domain

- `docs/GLOSSARY.md` — Glossary (Living) — Ubiquitous language for hako: kernel, flow, run, iteration, sandbox, workspace, progress report, verified done, pause, drift, budget, daemon, client. Topics: terminology, domain language.

## Specs / PRDs

Specs and PRDs are not repo documents — they live in the issue tracker. See `docs/agents/issue-tracker.md` to query them.
