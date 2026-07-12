//! The agent-loop engine: kernels, runs, and the six trait seams
//! (see ADR 0006). A library with no knowledge of its hosts — it must
//! never depend on `hako-server` or `hako-api`.
