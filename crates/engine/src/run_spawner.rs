//! The host boundary a fanout kernel uses to launch and observe child
//! runs. The engine names the contract; the daemon binds it to its run
//! registry, and tests bind it to an in-memory fake.

use async_trait::async_trait;

use crate::event::EventEnvelope;
use crate::run::RunId;

/// One child pipeline run requested by fanout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChildRunSpec {
    /// Opaque domain vocabulary passed verbatim into the child's plan
    /// prompt.
    pub scope: String,
}

#[async_trait]
pub trait RunSpawner: Send + Sync {
    /// Submit a child and return as soon as its run identity exists.
    async fn spawn(&self, child: ChildRunSpec) -> Result<RunId, RunSpawnerError>;

    /// Wait until a child is no longer running, then return the events
    /// that account for its outcome and budget use.
    async fn watch(&self, run_id: &RunId) -> Result<Vec<EventEnvelope>, RunSpawnerError>;
}

#[derive(Debug, thiserror::Error)]
#[error("run spawner: {0}")]
pub struct RunSpawnerError(pub String);
