//! Cooperative run cancellation. The token rides the `KernelContext`
//! as plain data, like budgets — not a seventh seam (ADR 0006): it is
//! never faked and never swapped, a test fires the real thing. The
//! host keeps a clone and signals; the engine observes it in exactly
//! one place — the sandbox bracket, which checks at entry and races
//! the token against the work in flight. Since every stage runs in a
//! bracket, that one point covers stage boundaries and mid-stage
//! alike, and every exit path still destroys the sandbox (ADR 0003:
//! teardown is the isolation guarantee). Cancel is terminal — a
//! cancelled run is over, unlike a Pause, which resumes.

use tokio::sync::watch;

/// A level-triggered cancel flag: once fired it stays fired, and a
/// waiter that arrives after the signal still wakes — which is why
/// this wraps `watch` rather than `Notify`, whose `notify_waiters`
/// reaches only whoever is already parked. Built on tokio features
/// the engine already carries; `tokio-util`'s `CancellationToken`
/// would buy hierarchy the engine has no use for at the price of a
/// new dependency. Cloning shares the flag — a cloned `Sender` sends
/// to the same channel — so any clone fires it, every clone sees it.
#[derive(Debug, Clone)]
pub struct CancelToken {
    flag: watch::Sender<bool>,
}

impl CancelToken {
    pub fn new() -> Self {
        Self {
            flag: watch::Sender::new(false),
        }
    }

    /// Fires the token. Idempotent; there is no un-cancel.
    pub fn cancel(&self) {
        self.flag.send_replace(true);
    }

    /// Whether the token has fired — the poll a kernel makes at a
    /// stage boundary.
    pub fn is_cancelled(&self) -> bool {
        *self.flag.borrow()
    }

    /// Resolves when the token fires — immediately, if it already has.
    /// Cancel-safe, so it can lose a `select!` any number of times and
    /// still fire on the next.
    pub async fn cancelled(&self) {
        let mut watched = self.flag.subscribe();
        // Cannot fail: `self` holds the sender as long as we wait.
        let _ = watched.wait_for(|&cancelled| cancelled).await;
    }
}

impl Default for CancelToken {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_fired_token_stays_fired_and_wakes_late_waiters() {
        let token = CancelToken::new();
        assert!(!token.is_cancelled());

        token.cancel();
        token.cancel();

        assert!(token.is_cancelled());
        // The waiter subscribes after the signal and must not hang.
        token.cancelled().await;
    }

    #[tokio::test]
    async fn every_clone_shares_the_one_flag() {
        let token = CancelToken::new();
        let observer = token.clone();
        let waiting = tokio::spawn(async move { observer.cancelled().await });

        token.clone().cancel();

        assert!(token.is_cancelled());
        waiting.await.unwrap();
    }
}
