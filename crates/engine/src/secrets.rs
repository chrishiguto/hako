//! The secrets seam — flows reference names, values exist only where
//! the daemon runs.
//!
//! Resolution happens once, at submit: the host reads the flow's names
//! and the adapter's requirements through a [`SecretsProvider`] and
//! hands the kernel a [`SecretEnv`] — a value, not a seam. Per-sandbox
//! resolution would hit the store four times an iteration and let a
//! store outage kill a run at iteration 40; resolving up front also
//! puts a provisioning gap in the submit's own answer, which is where
//! a human can act on it.

use std::borrow::Cow;
use std::collections::BTreeMap;

use async_trait::async_trait;

// The name type is published language — it appears in flow files — so
// it lives in proto. Values and their resolution stay here,
// engine-side.
pub use proto::secrets::SecretName;

/// What replaces a secret value wherever one is scrubbed. Distinctive
/// enough to grep the logs for, so a redaction reads as deliberate
/// rather than as the agent printing something odd.
pub const REDACTED: &str = "[redacted secret]";

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

/// One secret an adapter cannot run without, stated as the set of
/// names that satisfy it: the claude CLI takes either an API key or an
/// OAuth token, and a daemon provisioned with either can run it. Built
/// from a primary name so the set is never empty, and ordered — the
/// first name provisioned is the one injected, so an operator who has
/// both gets the adapter's preferred credential.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecretRequirement {
    primary: SecretName,
    alternatives: Vec<SecretName>,
}

impl SecretRequirement {
    /// A requirement only this name satisfies.
    pub fn named(name: &str) -> Self {
        Self {
            primary: SecretName::new(name),
            alternatives: Vec::new(),
        }
    }

    /// Widens the requirement: this name satisfies it too, tried after
    /// the ones already declared.
    #[must_use]
    pub fn or(mut self, alternative: &str) -> Self {
        self.alternatives.push(SecretName::new(alternative));
        self
    }

    /// Every name that satisfies this requirement, in preference
    /// order. Never empty.
    pub fn names(&self) -> impl Iterator<Item = &SecretName> {
        std::iter::once(&self.primary).chain(&self.alternatives)
    }
}

/// Reads as the alternatives do in an error message a human has to act
/// on: `ANTHROPIC_API_KEY or CLAUDE_CODE_OAUTH_TOKEN`.
impl std::fmt::Display for SecretRequirement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Vec<&str> = self.names().map(SecretName::as_str).collect();
        f.write_str(&names.join(" or "))
    }
}

/// One run's resolved secrets: the environment its sandboxes are built
/// with, and — because the same values are what must never reach the
/// event log — the single thing that knows how to [`scrub`] them out
/// of text.
///
/// [`scrub`]: Self::scrub
#[derive(Clone, Default)]
pub struct SecretEnv {
    values: BTreeMap<String, SecretValue>,
    /// The literals [`scrub`] hunts for, longest first so a value that
    /// contains another is replaced whole rather than leaving a tail.
    /// Held separately from `values` because scrubbing is a hot path:
    /// the agent's every output chunk passes through it.
    ///
    /// [`scrub`]: Self::scrub
    patterns: Vec<String>,
}

impl SecretEnv {
    /// Builds the env from resolved values, keyed by the environment
    /// variable each is injected as.
    pub fn new(values: BTreeMap<String, SecretValue>) -> Self {
        let mut patterns: Vec<String> = values
            .values()
            // An empty secret matches everywhere; scrubbing on it
            // would redact the whole log and hide nothing.
            .filter(|value| !value.expose().is_empty())
            .map(|value| value.expose().to_owned())
            .collect();
        patterns.sort_by(|left, right| right.len().cmp(&left.len()).then_with(|| left.cmp(right)));
        patterns.dedup();
        Self { values, patterns }
    }

    /// The environment variables a sandbox is built with.
    pub fn vars(&self) -> &BTreeMap<String, SecretValue> {
        &self.values
    }

    /// Whether there is anything to inject or to scrub.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Replaces every known secret value in `text` with [`REDACTED`].
    /// Borrowed back untouched when nothing matched, so the common
    /// case — agent output carrying no secret — copies nothing.
    ///
    /// Values are only caught whole: a secret split across two output
    /// chunks, or printed with a line break through it, passes. The
    /// scrub is a net over an agent echoing its environment, not a
    /// guarantee against one that means to smuggle its key out — the
    /// microVM boundary is what stands against that.
    pub fn scrub<'t>(&self, text: &'t str) -> Cow<'t, str> {
        let mut scrubbed = Cow::Borrowed(text);
        for pattern in &self.patterns {
            if scrubbed.contains(pattern.as_str()) {
                scrubbed = Cow::Owned(scrubbed.replace(pattern.as_str(), REDACTED));
            }
        }
        scrubbed
    }
}

/// Names only: the values are the whole point of this type, and
/// `patterns` holds them unwrapped.
impl std::fmt::Debug for SecretEnv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SecretEnv")
            .field("names", &self.values.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

/// Resolves everything one run needs before it starts: the names its
/// flow references, then the requirements its adapter states. Both
/// injected under their own names, so an adapter finds its credential
/// where its CLI looks for it.
///
/// A flow name must resolve exactly. A requirement is satisfied by any
/// one of its names — already-resolved ones first, so a flow that
/// lists the credential itself costs no second store read — and only a
/// requirement with nothing provisioned fails. A provider *failure*
/// (as opposed to a miss) never falls through to an alternative: a
/// store that cannot be read is not a store that lacks the secret.
pub async fn resolve(
    provider: &dyn SecretsProvider,
    names: &[SecretName],
    requirements: &[SecretRequirement],
) -> Result<SecretEnv, SecretsError> {
    let mut values = BTreeMap::new();
    for name in names {
        let value = provider.resolve(name).await?;
        values.insert(name.as_str().to_owned(), value);
    }
    for requirement in requirements {
        if requirement
            .names()
            .any(|name| values.contains_key(name.as_str()))
        {
            continue;
        }
        let mut satisfied = false;
        for name in requirement.names() {
            match provider.resolve(name).await {
                Ok(value) => {
                    values.insert(name.as_str().to_owned(), value);
                    satisfied = true;
                    break;
                }
                Err(SecretsError::NotFound(_)) => continue,
                Err(error) => return Err(error),
            }
        }
        if !satisfied {
            return Err(SecretsError::Unsatisfied(requirement.clone()));
        }
    }
    Ok(SecretEnv::new(values))
}

/// Resolves secret names to values. A file store with restrictive
/// permissions (or daemon env) in production, a map in tests. Reached
/// once per run, at submit — never from inside a kernel, which works
/// from the [`SecretEnv`] that resolution produced.
#[async_trait]
pub trait SecretsProvider: Send + Sync {
    /// Resolves one name. `NotFound` is what fails a submission that
    /// references an unprovisioned secret.
    async fn resolve(&self, name: &SecretName) -> Result<SecretValue, SecretsError>;
}

/// Why a run's secrets could not be resolved. `NotFound` and
/// `Unsatisfied` are the two a submission answers for — a gap a human
/// fixes by provisioning something — so both name what is missing.
#[derive(Debug, thiserror::Error)]
pub enum SecretsError {
    #[error("secret `{0}` is not provisioned")]
    NotFound(SecretName),
    #[error("the agent needs one of these secrets provisioned: {0}")]
    Unsatisfied(SecretRequirement),
    #[error("secrets provider failure: {0}")]
    Provider(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// A provider over a fixed map that records what it was asked for
    /// — resolution's store traffic is part of its contract, not an
    /// implementation detail: the whole point of resolving once is not
    /// hitting the store more than necessary.
    struct Store {
        secrets: BTreeMap<&'static str, &'static str>,
        asked: Mutex<Vec<String>>,
    }

    impl Store {
        fn with(secrets: &[(&'static str, &'static str)]) -> Self {
            Self {
                secrets: secrets.iter().copied().collect(),
                asked: Mutex::new(Vec::new()),
            }
        }

        fn asked(&self) -> Vec<String> {
            self.asked.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl SecretsProvider for Store {
        async fn resolve(&self, name: &SecretName) -> Result<SecretValue, SecretsError> {
            self.asked.lock().unwrap().push(name.to_string());
            self.secrets
                .get(name.as_str())
                .map(|value| SecretValue::new(*value))
                .ok_or_else(|| SecretsError::NotFound(name.clone()))
        }
    }

    /// A store that is there but broken — every read fails for a
    /// reason that is not "no such secret".
    struct BrokenStore;

    #[async_trait]
    impl SecretsProvider for BrokenStore {
        async fn resolve(&self, _name: &SecretName) -> Result<SecretValue, SecretsError> {
            Err(SecretsError::Provider("permission denied".into()))
        }
    }

    fn names(names: &[&str]) -> Vec<SecretName> {
        names.iter().map(|name| SecretName::new(*name)).collect()
    }

    fn claude() -> SecretRequirement {
        SecretRequirement::named("ANTHROPIC_API_KEY").or("CLAUDE_CODE_OAUTH_TOKEN")
    }

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

    /// The env holds values unwrapped to scrub with, so its own Debug
    /// has to redact them — a context or a spec printed in a trace
    /// prints through here.
    #[test]
    fn a_resolved_env_debug_prints_names_without_values() {
        let env = SecretEnv::new(
            [("GH_TOKEN".to_owned(), SecretValue::new("ghp_sensitive"))]
                .into_iter()
                .collect(),
        );
        let printed = format!("{env:?}");
        assert!(printed.contains("GH_TOKEN"), "{printed}");
        assert!(!printed.contains("ghp_sensitive"), "{printed}");
    }

    #[tokio::test]
    async fn flow_names_resolve_exactly_and_a_gap_names_the_secret() {
        let store = Store::with(&[("GH_TOKEN", "ghp_1"), ("NPM_TOKEN", "npm_1")]);
        let env = resolve(&store, &names(&["GH_TOKEN", "NPM_TOKEN"]), &[])
            .await
            .unwrap();
        assert_eq!(env.vars()["GH_TOKEN"].expose(), "ghp_1");
        assert_eq!(env.vars()["NPM_TOKEN"].expose(), "npm_1");

        let error = resolve(&store, &names(&["GH_TOKEN", "MISSING"]), &[])
            .await
            .unwrap_err();
        assert!(matches!(&error, SecretsError::NotFound(name) if name.as_str() == "MISSING"));
        assert!(error.to_string().contains("MISSING"), "{error}");
    }

    /// The one-of set: either credential satisfies the claude adapter,
    /// and the one that resolves is the one injected — under its own
    /// name, because that is where the CLI looks for it.
    #[tokio::test]
    async fn any_alternative_satisfies_a_requirement_and_is_injected_under_its_own_name() {
        for (provisioned, expected) in [
            ("ANTHROPIC_API_KEY", "ANTHROPIC_API_KEY"),
            ("CLAUDE_CODE_OAUTH_TOKEN", "CLAUDE_CODE_OAUTH_TOKEN"),
        ] {
            let store = Store::with(&[(provisioned, "the-credential")]);
            let env = resolve(&store, &[], &[claude()]).await.unwrap();
            assert_eq!(env.vars().keys().collect::<Vec<_>>(), [expected]);
            assert_eq!(env.vars()[expected].expose(), "the-credential");
        }
    }

    /// Preference order, and no store traffic past the winner: the
    /// first alternative provisioned is the one the adapter gets.
    #[tokio::test]
    async fn the_first_provisioned_alternative_wins() {
        let store = Store::with(&[
            ("ANTHROPIC_API_KEY", "key"),
            ("CLAUDE_CODE_OAUTH_TOKEN", "token"),
        ]);
        let env = resolve(&store, &[], &[claude()]).await.unwrap();
        assert_eq!(env.vars().keys().collect::<Vec<_>>(), ["ANTHROPIC_API_KEY"]);
        assert_eq!(store.asked(), ["ANTHROPIC_API_KEY"]);
    }

    /// A requirement the flow already covers costs no second read —
    /// resolving once per run means once per *secret*, too.
    #[tokio::test]
    async fn a_flow_listed_credential_satisfies_the_requirement_without_a_second_read() {
        let store = Store::with(&[("CLAUDE_CODE_OAUTH_TOKEN", "token")]);
        let env = resolve(&store, &names(&["CLAUDE_CODE_OAUTH_TOKEN"]), &[claude()])
            .await
            .unwrap();
        assert_eq!(
            env.vars().keys().collect::<Vec<_>>(),
            ["CLAUDE_CODE_OAUTH_TOKEN"]
        );
        assert_eq!(store.asked(), ["CLAUDE_CODE_OAUTH_TOKEN"]);
    }

    /// Failing only when *no* alternative is present is the whole
    /// amendment — and the message has to list every name that would
    /// have worked, or an operator cannot act on it.
    #[tokio::test]
    async fn a_requirement_with_nothing_provisioned_names_every_alternative() {
        let store = Store::with(&[("GH_TOKEN", "ghp_1")]);
        let error = resolve(&store, &[], &[claude()]).await.unwrap_err();
        let message = error.to_string();
        assert!(matches!(error, SecretsError::Unsatisfied(_)));
        for name in ["ANTHROPIC_API_KEY", "CLAUDE_CODE_OAUTH_TOKEN"] {
            assert!(message.contains(name), "{message}");
        }
        assert_eq!(
            store.asked(),
            ["ANTHROPIC_API_KEY", "CLAUDE_CODE_OAUTH_TOKEN"]
        );
    }

    /// A store that cannot be read is not a store that lacks the
    /// secret: a broken read fails the submission where it happened
    /// instead of silently falling through to an alternative.
    #[tokio::test]
    async fn a_provider_failure_does_not_fall_through_to_an_alternative() {
        let error = resolve(&BrokenStore, &[], &[claude()]).await.unwrap_err();
        assert!(matches!(error, SecretsError::Provider(_)), "{error}");
        let error = resolve(&BrokenStore, &names(&["GH_TOKEN"]), &[])
            .await
            .unwrap_err();
        assert!(matches!(error, SecretsError::Provider(_)), "{error}");
    }

    #[test]
    fn scrubbing_replaces_every_occurrence_of_every_value() {
        let env = SecretEnv::new(
            [
                ("GH_TOKEN".to_owned(), SecretValue::new("ghp_1")),
                ("NPM_TOKEN".to_owned(), SecretValue::new("npm_2")),
            ]
            .into_iter()
            .collect(),
        );
        let scrubbed = env.scrub("pushing with ghp_1, publishing with npm_2, again ghp_1");
        assert_eq!(
            scrubbed,
            format!("pushing with {REDACTED}, publishing with {REDACTED}, again {REDACTED}")
        );
    }

    /// A value that contains another must go whole: scrubbing the
    /// short one first would leave the rest of the long one in the
    /// clear.
    #[test]
    fn a_value_containing_another_is_scrubbed_whole() {
        let env = SecretEnv::new(
            [
                ("SHORT".to_owned(), SecretValue::new("ghp_1")),
                ("LONG".to_owned(), SecretValue::new("ghp_1_and_more")),
            ]
            .into_iter()
            .collect(),
        );
        assert_eq!(env.scrub("key=ghp_1_and_more"), format!("key={REDACTED}"));
    }

    /// The hot path: text with nothing to redact comes back borrowed,
    /// so the agent's every output chunk is not copied for nothing.
    #[test]
    fn text_without_a_secret_is_not_copied() {
        let env = SecretEnv::new(
            [("GH_TOKEN".to_owned(), SecretValue::new("ghp_1"))]
                .into_iter()
                .collect(),
        );
        assert!(matches!(env.scrub("nothing to see"), Cow::Borrowed(_)));
        assert!(matches!(
            SecretEnv::default().scrub("ghp_1"),
            Cow::Borrowed(_)
        ));
    }

    /// An empty secret would match between every character; scrubbing
    /// on it would redact the whole log and hide nothing.
    #[test]
    fn an_empty_value_scrubs_nothing() {
        let env = SecretEnv::new(
            [("EMPTY".to_owned(), SecretValue::new(""))]
                .into_iter()
                .collect(),
        );
        assert_eq!(env.scrub("ordinary output"), "ordinary output");
    }
}
