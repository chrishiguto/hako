# hako — agent instructions

hako is a model-agnostic agent-loop runner: loop patterns ("kernels") are Rust code that owns control flow; small TOML flow files parameterize them; iterations run in ephemeral microVMs. The canonical design record is the v1 spec in the issue tracker.

## Agent skills

### Issue tracker

Issues and specs/PRDs live in GitHub Issues on `chrishiguto/hako`; use the `gh` CLI. See `docs/agents/issue-tracker.md`.

### Triage labels

Two independent axes — audience (`afk`/`hitl`) and state (`needs-triage`/`ready`); label strings equal their names. See `docs/agents/triage-labels.md`.

### Domain docs

Ubiquitous language in `docs/GLOSSARY.md`, decisions in `docs/adr/`. See `docs/agents/domain.md` for how to read them.

## Documentation

Repo docs are indexed in `docs/DOCS_INDEX.md` — a machine-readable list with a one-line summary per document. Check the index first to find the right doc, then open that file directly. Specs/PRDs are not docs — they live in the issue tracker (see `docs/agents/issue-tracker.md`).
