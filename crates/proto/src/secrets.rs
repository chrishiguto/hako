//! Secret names — the only piece of the secrets vocabulary that is
//! published language. Values never cross a boundary: they resolve
//! daemon-side through the engine's `SecretsProvider` seam.

use serde::{Deserialize, Serialize};

/// A name a flow may reference — never a value, so flow files stay
/// safe to commit and safe for LLMs to read and write.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(transparent)]
pub struct SecretName(String);

impl SecretName {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for SecretName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
