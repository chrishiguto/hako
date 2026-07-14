# hako — agent instructions

hako is a model-agnostic agent-loop runner: loop patterns ("kernels") are Rust code that owns control flow; small TOML flow files parameterize them; iterations run in ephemeral microVMs. The canonical design record is the v1 spec in the issue tracker.

## Development

A Rust workspace, toolchain pinned in `rust-toolchain.toml`. `just` is the single command surface — CI runs the same recipes, so never duplicate a recipe's command line in `.github/workflows/`. The one split is tool provisioning: CI installs prebuilt binaries via install-action; keep its tool list in sync with `just setup`.

- `just fmt` — format everything
- `just check` — fmt-check, typos, cargo-deny, dependency rules, schema drift, clippy (warnings denied)
- `just test` — the workspace test suite
- `just schema` — regenerate `schemas/flow.schema.json` after changing the engine's flow types
- `just setup` — one-time install of cargo-deny and typos (rustup + just assumed)

Automation that outgrows a shell one-liner lives in the `xtask` crate, invoked as `cargo xtask <task>` (today: `deps`, the workspace dependency-rule check, and `schema`, the flow-schema generator and drift check). xtask is a dev tool, not a product crate — exempt from the dependency rules it enforces.

Product crates live in `crates/`: `proto` (the published language — wire types and the flow file format both sides speak), `engine`, `sandbox`, `api`, `server` (binary `hakod`), `cli` (binary `hako`). Three hard rules (ADRs 0006, 0008, and 0009, enforced by `cargo xtask deps` in `just check`): `engine` never depends on `server` or `api`; `cli` depends on `api` and `proto` only; `proto` depends on no workspace crate.

## Agent skills

### Issue tracker

Issues and specs/PRDs live in GitHub Issues on `chrishiguto/hako`; use the `gh` CLI. See `docs/agents/issue-tracker.md`.

### Triage labels

Two independent axes — audience (`afk`/`hitl`) and state (`needs-triage`/`ready`); label strings equal their names. See `docs/agents/triage-labels.md`.

### Domain docs

Ubiquitous language in `docs/GLOSSARY.md`, decisions in `docs/adr/`. See `docs/agents/domain.md` for how to read them.

## Documentation

Repo docs are indexed in `docs/DOCS_INDEX.md` — a machine-readable list with a one-line summary per document. Check the index first to find the right doc, then open that file directly. Specs/PRDs are not docs — they live in the issue tracker (see `docs/agents/issue-tracker.md`).
