//! Run identity and the run state machine. The states themselves are
//! wire vocabulary and live in `proto`; the identity and the kernel's
//! view of an ending are engine-only.

use serde::{Deserialize, Serialize};

pub use proto::run::{PauseReason, RunState};

/// Names one run for its whole life — directory name, API path segment,
/// event-log subject.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RunId(String);

impl RunId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for RunId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// How a kernel invocation ended — every run state except `Running`,
/// which would mean the kernel returned while still owing work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunOutcome {
    Done,
    Failed,
    Paused(PauseReason),
    Cancelled,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn run_id_serializes_as_a_bare_string() {
        let id = RunId::new("run-7");
        assert_eq!(serde_json::to_value(&id).unwrap(), json!("run-7"));
    }
}
