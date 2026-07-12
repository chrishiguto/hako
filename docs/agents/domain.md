# Domain Docs

How the engineering skills should consume this repo's domain documentation when exploring the codebase.

## Start at the index

`docs/DOCS_INDEX.md` is the machine-readable index of the repo's durable docs — one line per document with a short summary and key topics. Read it first to find which ADR or guide is relevant, then open that file directly instead of scanning the tree.

## Before exploring, read these

- **`docs/GLOSSARY.md`** — the project's ubiquitous language. Read it for vocabulary before you name anything.
- **`docs/adr/`** — read the ADRs that touch the area you're about to work in. Use `docs/DOCS_INDEX.md` to find the relevant ones by topic.

If any of these don't exist, **proceed silently**. Don't flag their absence; don't suggest creating them upfront. `/domain-modeling` (reached via `/grill-with-docs` and `/improve-codebase-architecture`) creates them lazily when terms or decisions actually get resolved, and adds them to the index at that point.

## File structure

```
/
├── docs/
│   ├── GLOSSARY.md      ← ubiquitous language
│   ├── DOCS_INDEX.md    ← machine index of the docs below
│   └── adr/
│       ├── 0001-event-sourced-orders.md
│       └── 0002-postgres-for-write-model.md
└── src/
```

A repo with multiple bounded contexts keeps them all in the one `docs/GLOSSARY.md`, separated by `## <Context>` sections. There is no context map and no per-context glossary files.

## Use the glossary's vocabulary

When your output names a domain concept (an issue title, a refactor proposal, a hypothesis, a test name), use the term as defined in `docs/GLOSSARY.md`. Don't drift to synonyms the glossary explicitly avoids.

If the concept you need isn't in the glossary yet, that's a signal — either you're inventing language the project doesn't use (reconsider) or there's a real gap (note it for `/domain-modeling`).

## Flag ADR conflicts

If your output contradicts an existing ADR, surface it explicitly rather than silently overriding:

> _Contradicts ADR-0007 (event-sourced orders) — but worth reopening because…_
