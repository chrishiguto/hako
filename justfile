# hako dev commands. CI runs these same recipes — never duplicate a recipe's
# command line in .github/workflows/. The one deliberate split is tool
# provisioning: CI installs prebuilt binaries via install-action, whose tool
# list must be kept in sync with `setup`.

default:
    @just --list

# one-time tool install from source (rustup + just assumed; the toolchain is
# pinned by rust-toolchain.toml) — mirror changes into ci.yml's tool list
setup:
    cargo install --locked cargo-deny typos-cli

# format everything
fmt:
    cargo fmt --all

# fmt-check, typos, cargo-deny, dependency rules, schema drift, clippy (warnings denied) — cheapest first, --locked so the committed Cargo.lock stays authoritative
check:
    cargo fmt --all --check
    typos
    cargo deny --locked check
    cargo --locked xtask deps
    cargo --locked xtask schema --check
    cargo clippy --workspace --all-targets --locked -- -D warnings

# regenerate schemas/flow.schema.json from the engine's flow types
schema:
    cargo --locked xtask schema

# the test suite
test:
    cargo test --workspace --locked
