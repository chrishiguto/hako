//! The notifier seam — how a pause reaches the human it is waiting on.

use async_trait::async_trait;

use crate::run::{PauseReason, RunId};

/// Delivers pause notifications. A webhook (ntfy/Slack-compatible) in
/// production, a recorder in tests.
#[async_trait]
pub trait Notifier: Send + Sync {
    /// Fires once per pause. Failure never takes the run down — a
    /// paused run with a lost notification is still resumable.
    async fn notify(&self, notification: &Notification) -> Result<(), NotifierError>;
}

/// What a phone needs to show for a human to decide whether to look:
/// which run paused, why, and what it last did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Notification {
    pub run_id: RunId,
    pub reason: PauseReason,
    pub summary: String,
}

/// A notification that could not be delivered.
#[derive(Debug, thiserror::Error)]
#[error("notification failed: {0}")]
pub struct NotifierError(pub String);
