# REST + SSE over an append-only event log; WebSocket deferred

Each run's source of truth is an append-only event log on disk (doubling as the audit trail). Clients stream it via SSE with Last-Event-ID resume and send commands (submit, answer, resume, cancel) as plain REST. Chosen over WebSocket because the traffic is asymmetric — constant streaming down, a handful of discrete commands up — and because the dominant AFK scenario is "no client connected, attach later", making replay-then-follow the core operation; SSE provides that natively where WebSocket would require reimplementing it (reconnect, replay, re-auth).

## Consequences

Bearer auth, curl-ability, and proxy traversal come free with plain HTTP. If an interactive shell into running sandboxes ever ships, it gets its own WebSocket data plane alongside; this contract does not change.
