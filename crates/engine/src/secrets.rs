//! The secrets seam — flows reference names, values exist only where
//! the daemon runs.

use async_trait::async_trait;

// The name type is published language — it appears in flow files — so
// it lives in proto (ADR 0009). Values and their resolution stay here,
// engine-side.
pub use proto::secrets::SecretName;

/// A resolved secret value. Deliberately not serializable and
/// debug-printed redacted, so a value cannot slip into an event log or
/// error message by derive.
#[derive(Clone, PartialEq, Eq)]
pub struct SecretValue(String);

impl SecretValue {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// The only way to read the value — grep for `expose` to audit
    /// every point where a secret leaves its wrapper.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for SecretValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SecretValue(<redacted>)")
    }
}

/// Resolves secret names to values. A file store with restrictive
/// permissions (or daemon env) in production, a map in tests.
#[async_trait]
pub trait SecretsProvider: Send + Sync {
    /// Resolves one name. `NotFound` is what fails a submission that
    /// references an unprovisioned secret.
    async fn resolve(&self, name: &SecretName) -> Result<SecretValue, SecretsError>;
}

/// Why a secret could not be resolved. `NotFound` is a distinct
/// variant because submit-time validation branches on it.
#[derive(Debug, thiserror::Error)]
pub enum SecretsError {
    #[error("secret `{0}` is not provisioned")]
    NotFound(SecretName),
    #[error("secrets provider failure: {0}")]
    Provider(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_values_debug_print_redacted() {
        let value = SecretValue::new("ghp_super_sensitive");
        let printed = format!("{value:?}");
        assert!(!printed.contains("sensitive"));
        assert_eq!(printed, "SecretValue(<redacted>)");
    }

    #[test]
    fn expose_returns_the_value() {
        assert_eq!(SecretValue::new("tok").expose(), "tok");
    }
}
