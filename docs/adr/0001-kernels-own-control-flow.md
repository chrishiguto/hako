# Kernels own control flow; flow files are logic-free

The predecessor project (hakoflow) encoded loops, conditionals, fan-out, and output extraction in config, growing an ad-hoc expression language that made flows verbose and hard to author. hako inverts this: loop patterns (kernels) are Rust code inside the engine, and a flow file only parameterizes one — agent, budgets, verify checks, workspace, secret names. The flow schema contains no iteration, branching, or expressions of any kind.

## Considered Options

- A general-purpose workflow language in config (the predecessor's path) — rejected: config inevitably grows into a bad, untestable programming language (the GitHub Actions/Ansible trajectory; Dagger deprecated its CUE engine for the same reason).
- Embedded scripting (Rhai/Starlark) as an escape hatch — deferred: it reintroduces logic-in-config pressure. A new loop shape is a new kernel in Rust.

## Consequences

Flows stay ~20 lines, schema-validated, and LLM-authorable. Anything a kernel doesn't support requires a Rust change — deliberately: the pattern is the guardrail.
