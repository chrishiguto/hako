//! Secret names — the only piece of the secrets vocabulary that is
//! published language. Values never cross a boundary: they resolve
//! daemon-side through the engine's `SecretsProvider` seam.

use serde::{Deserialize, Serialize};

/// A name a flow may reference — never a value, so flow files stay
/// safe to commit and safe for LLMs to read and write.
///
/// Env-var shaped: non-empty, ASCII letters, digits, and `_`.
/// Deserializing anything else fails, so a malformed name is a flow
/// error naming the rule — never a store miss claiming the secret
/// "is not provisioned" when no provisioning could make it true.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(transparent)]
pub struct SecretName(
    #[cfg_attr(feature = "schema", schemars(regex(pattern = r"^[A-Za-z0-9_]+$")))] String,
);

impl SecretName {
    /// Unchecked construction, for trusted callers — adapters stating
    /// requirements, tests probing the store. Names from outside
    /// arrive through deserialization, which enforces the shape; the
    /// daemon's store keeps its own check as defense-in-depth behind
    /// this door.
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Whether this name has the shape deserialization enforces — for
    /// the trusted paths that skipped it, like the daemon's store
    /// judging a name before letting it address a file.
    pub fn is_env_shaped(&self) -> bool {
        !self.0.is_empty()
            && self
                .0
                .chars()
                .all(|char| char.is_ascii_alphanumeric() || char == '_')
    }
}

impl<'de> Deserialize<'de> for SecretName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let name = Self(String::deserialize(deserializer)?);
        if name.is_env_shaped() {
            Ok(name)
        } else {
            Err(serde::de::Error::custom(format!(
                "secret name `{name}` must be env-var shaped: ASCII letters, digits, and `_` only"
            )))
        }
    }
}

impl std::fmt::Display for SecretName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
