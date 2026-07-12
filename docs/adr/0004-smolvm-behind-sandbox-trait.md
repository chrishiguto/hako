# smolvm is the sandbox backend, wrapped behind a Sandbox trait

The predecessor's in-house libkrun VM stack (~15k LOC: manager, guest agent, pool daemon) is dropped, not ported — maintaining a microVM runtime is not this project's goal. Sandboxing is delegated to smolvm, driven via its CLI (it ships no Rust SDK) behind a small Sandbox trait, version-pinned and preflight-checked at daemon startup. smolvm stays local to the daemon and is never network-exposed; the daemon is the sole auth surface.

## Considered Options

- Keep maintaining the in-house stack — rejected: ~75% of the old codebase served the commodity layer instead of the product.
- Hardwire smolvm calls without a trait — rejected: smolvm is young (single-author, fast-moving), so the trait is the insurance policy; a replacement backend (docker, bare worktree, resurrected old stack) is an adapter, not surgery.

## Consequences

Known risk: smolvm's Linux volume-mount permissions bug (upstream #428) must be validated on target machines in the adapter's first implementation, since workspace mounts are load-bearing (ADR-0003).
