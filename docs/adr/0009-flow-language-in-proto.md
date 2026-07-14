# The flow file format is published language: its types live in proto

The flow config types (`FlowConfig` and friends, plus `SecretName`,
which flows reference) live in `proto`, not the engine. The flow file
is the most published surface hako has — authored by users and LLMs,
validated by editors, parsed by the daemon — and ADR 0008's rule
applies to it: one definition, the compiler as the sync. In-workspace
consumers parse with the types — `hako validate` runs the daemon's own
parser, so its verdict and error text (offending line, caret,
"expected one of" suggestion) are exactly what a rejected submit would
produce. Consumers that cannot link Rust — editors, LLMs — get
`schemas/flow.schema.json`, generated from the same types by `cargo
xtask schema` and drift-checked in CI. The schemars derives sit behind
proto's `schema` feature, mirroring the `openapi` feature, so product
crates never carry schema tooling.

## Considered Options

- The CLI validates against the embedded generated schema (the
  original slice of issue #4) — rejected after one iteration: it kept
  the CLI off the engine, but at the price of a second implementation
  of "what is a valid flow" (a hand-rolled TOML→JSON converter plus a
  JSON Schema evaluator) pinned to serde by a differential corpus,
  with strictly worse errors — JSON pointers instead of line numbers,
  no suggestions, and `anyOf` noise that swallowed the duration
  grammar entirely. The same fixture-synced duplication ADR 0008
  rejected for wire types, rebuilt one layer up.
- Flow types stay in the engine and the CLI links the engine —
  rejected: ADR 0006; clients are peers and stay off the engine.

## Consequences

The wire contract is unchanged: flows cross as verbatim TOML
(`SubmitRunRequest`) and the daemon re-parses what it receives;
clients pre-validate but never re-encode. The schema remains a
committed, published artifact; its agreement with strict serde is
pinned by the generator's tests (`xtask/tests/`), which also hold the
corpus's TOML→JSON conversion — the only such conversion left, and
test-only. `hako validate` now fails at the first error rather than
listing every violation — the price of serde; editors still get
multi-error validation from the schema. The engine sheds `schemars`
and `toml`; the CLI sheds `jsonschema`, whose only remaining use is
the agreement tests.
