# Kernel dialects are named modules in proto; the kernel set is closed in v1

A kernel's wire vocabulary — its report shapes today, its prompt slots
with #26 — is *published language*, not kernel-internal detail: the
schemas are quoted to agents in preambles and committed under
`schemas/`, and stage-scoped events must carry report payloads through
the event log clients replay (#27, #28) — which `proto` alone can
name, since it depends on no workspace crate. So dialect types live in
`proto`, but as one top-level module per kernel (`proto::pipeline`,
later `proto::fanout`) with artifacts under a kernel-named directory
(`schemas/report/pipeline/`), never mixed into the shared core
(`report`, `event`, `run`, `flow`). The namespace is the label: the
core is what every kernel speaks, a dialect is what one kernel adds,
and the line between them stays visible in the crate tree.

This holds because the kernel set is closed in v1: every kernel, and
every wire party, ships from this workspace in lockstep. Third-party
kernels are a real ambition — and an explicit non-goal until post-v1.

## Considered Options

- Dialect types in the engine, next to their kernel — rejected:
  resume-in-place (#28) derives resume points by replaying the event
  log, so events must carry stage reports, and `proto` cannot
  reference engine types. Engine-owned shapes would force opaque JSON
  through the events — the boundary-conversion layer ADR 0008 already
  rejected as freedom nobody currently needs.
- A kernel-agnostic wire now (the Temporal model: the server never
  deserializes workflow payloads; opaque bytes plus per-kernel schema
  registries; Kubernetes CRDs are the same shape) — rejected for v1:
  agnosticism pays off only when foreign code sits on one end of the
  wire. With every party in one workspace it buys nothing and costs
  typed events, serde-pinned schema agreement, and typed client
  rendering. This is the evidence-backed target *if* third-party
  kernels become real; that day, the change gets its own spec and
  grilling, not an incremental drift.
- Flat placement in `proto::report` (the state this ADR amends) —
  rejected: it blurred the very line this decision draws, making the
  pipeline's stages read as engine-universal vocabulary.

## Consequences

Adding a kernel means adding one dialect module, its schema directory,
and its agreement suite — the shared core changes only when a concept
is genuinely uniform across kernels (see the glossary's Kernel
Dialect). The engine's shared machinery never imports a dialect; only
the kernel that owns it does — held by review, since cargo cannot see
module boundaries inside one crate. Dialect types are not re-exported
from crate roots: reaching them through `proto::pipeline` is what
keeps the ownership legible. Amends ADR 0008's charter wording: the
report types it admits are the shared vocabulary plus named dialects,
under the same admission rule — more than one party must agree on the
bytes.
