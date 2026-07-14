//! The workspace dependency-rule check.
//!
//! Three hard rules, enforced on transitive reachability between
//! workspace crates:
//!
//! - `engine` never reaches `server` or `api`
//! - `cli` reaches `api` and `proto` only
//! - `proto`, the published language, reaches no workspace crate
//!
//! Reachability is computed over the full resolved graph, by package ID:
//! identity by ID means an external crate that happens to share a member's
//! name is never mistaken for it, and walking non-member nodes means a
//! forbidden edge can't hide behind an out-of-workspace path dep. The
//! same identity choice puts git and registry copies of a member out of
//! scope — a different package ID, so never flagged here. Depending on a
//! foreign copy is a supply-chain problem, deny.toml's territory (its
//! source denials reject git deps outright).
//! Dev and build edges count only on the first hop — cargo does not
//! propagate them into dependents' builds. Declared manifest edges between
//! members (matched by path, not name) are overlaid on top, because the
//! resolve graph omits deps on bin-only crates that cargo would reject
//! at build time anyway — the check should name the broken rule, not
//! leave it to a compile error.
//!
//! `xtask` is a dev tool, not a product crate — exempt.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, bail};
use cargo_metadata::{CargoOpt, DependencyKind, MetadataCommand, PackageId};

/// Workspace-internal reachability, member name → member names. Direct
/// edges or a pre-closed set both work; `violations` takes the closure.
pub type Graph = BTreeMap<String, BTreeSet<String>>;

const ENGINE: &str = "engine";
const API: &str = "api";
const PROTO: &str = "proto";
const SANDBOX: &str = "sandbox";
const SERVER: &str = "server";
const CLI: &str = "cli";

// Also the vocabulary of the rules below — a rename that updates this
// list updates the rules with it.
const PRODUCT_CRATES: [&str; 6] = [API, CLI, ENGINE, PROTO, SANDBOX, SERVER];

/// Checks the rules against a graph of workspace-internal edges. Returns
/// one human-readable violation per broken rule; empty means they hold.
pub fn violations(graph: &Graph) -> Vec<String> {
    let mut found = Vec::new();
    for name in PRODUCT_CRATES {
        if !graph.contains_key(name) {
            found.push(format!(
                "product crate `{name}` is missing from the workspace — renamed or dropped?"
            ));
        }
    }
    let engine_reaches = reachable(graph, ENGINE);
    for forbidden in [API, SERVER] {
        if engine_reaches.contains(forbidden) {
            found.push(format!("`{ENGINE}` must never depend on `{forbidden}`"));
        }
    }
    for reached in reachable(graph, CLI) {
        if reached != API && reached != PROTO {
            found.push(format!(
                "`{CLI}` may depend on `{API}` and `{PROTO}` only, but reaches `{reached}`"
            ));
        }
    }
    for reached in reachable(graph, PROTO) {
        found.push(format!(
            "`{PROTO}` must stay a leaf, but depends on `{reached}`"
        ));
    }
    found
}

/// Crates reachable from `start` by one or more edges (`start` excluded
/// unless a cycle leads back to it).
fn reachable(graph: &Graph, start: &str) -> BTreeSet<String> {
    let mut seen = BTreeSet::new();
    let mut frontier = vec![start];
    while let Some(name) = frontier.pop() {
        for dep in graph.get(name).into_iter().flatten() {
            if seen.insert(dep.clone()) {
                frontier.push(dep);
            }
        }
    }
    seen
}

/// Runs the check against the real workspace; exits non-zero on violation.
pub fn run() -> anyhow::Result<()> {
    let graph = workspace_graph()?;
    let violations = violations(&graph);
    if !violations.is_empty() {
        for v in &violations {
            eprintln!("dependency rule violated: {v}");
        }
        bail!("{} dependency rule violation(s)", violations.len());
    }
    println!("dependency rules hold ({} crates)", graph.len());
    Ok(())
}

/// Maps each workspace member to the members it reaches in the resolved
/// graph, walking through non-member packages.
fn workspace_graph() -> anyhow::Result<Graph> {
    // A check must never mutate: unlocked `cargo metadata` silently
    // rewrites a stale Cargo.lock instead of failing on it.
    let metadata = MetadataCommand::new()
        .features(CargoOpt::AllFeatures)
        .other_options(vec!["--locked".to_string()])
        .exec()
        .context("failed to run `cargo metadata`")?;
    let resolve = metadata
        .resolve
        .as_ref()
        .context("cargo metadata returned no resolve graph")?;
    let members = metadata.workspace_packages();
    let nodes: BTreeMap<&PackageId, &cargo_metadata::Node> =
        resolve.nodes.iter().map(|node| (&node.id, node)).collect();
    let member_names: BTreeMap<&PackageId, String> = members
        .iter()
        .map(|package| (&package.id, package.name.to_string()))
        .collect();

    let mut graph = Graph::new();
    for (member, name) in &member_names {
        let mut reached = BTreeSet::new();
        let mut seen: BTreeSet<&PackageId> = BTreeSet::new();
        let mut frontier = vec![(*member, true)];
        while let Some((id, first_hop)) = frontier.pop() {
            let node = nodes
                .get(id)
                .with_context(|| format!("package `{id}` missing from resolve graph"))?;
            for dep in &node.deps {
                let propagates = first_hop
                    || dep
                        .dep_kinds
                        .iter()
                        .any(|info| info.kind == DependencyKind::Normal);
                if propagates && seen.insert(&dep.pkg) {
                    if let Some(dep_name) = member_names.get(&dep.pkg) {
                        reached.insert(dep_name.clone());
                    }
                    frontier.push((&dep.pkg, false));
                }
            }
        }
        graph.insert(name.clone(), reached);
    }

    let member_dirs: BTreeMap<_, _> = members
        .iter()
        .filter_map(|package| {
            let dir = package.manifest_path.parent()?;
            Some((dir.to_path_buf(), package.name.to_string()))
        })
        .collect();
    for package in &members {
        for dep in &package.dependencies {
            let Some(target) = dep.path.as_ref().and_then(|p| member_dirs.get(p)) else {
                continue;
            };
            graph
                .entry(package.name.to_string())
                .or_default()
                .insert(target.clone());
        }
    }
    Ok(graph)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every product crate with no edges, then `overrides` applied.
    fn graph(overrides: &[(&str, &[&str])]) -> Graph {
        let mut graph: Graph = PRODUCT_CRATES
            .iter()
            .map(|name| (name.to_string(), BTreeSet::new()))
            .collect();
        for (name, deps) in overrides {
            graph.insert(
                name.to_string(),
                deps.iter().map(|d| d.to_string()).collect(),
            );
        }
        graph
    }

    /// The intended shape as direct edges — the one place a legitimate
    /// new edge gets added; the closure test derives from it.
    fn intended_shape() -> Graph {
        graph(&[
            ("engine", &["proto"]),
            ("api", &["proto"]),
            ("sandbox", &["engine"]),
            ("server", &["engine", "api", "sandbox"]),
            ("cli", &["api", "proto"]),
            // Exempt from the rules but present in the graph: xtask
            // links proto to generate the flow schema.
            ("xtask", &["proto"]),
        ])
    }

    #[test]
    fn intended_workspace_shape_passes() {
        assert_eq!(violations(&intended_shape()), Vec::<String>::new());
    }

    /// Pins the check's sensitivity, not just its pass-case: a regression
    /// that silently dropped edges from the graph would keep `violations`
    /// green in CI while making the check blind. Fails only when a
    /// member-to-member edge changes — exactly when a human should look.
    #[test]
    fn real_workspace_matches_intended_shape() {
        let direct = intended_shape();
        let expected: Graph = direct
            .keys()
            .map(|name| (name.clone(), reachable(&direct, name)))
            .collect();
        let actual = workspace_graph().expect("cargo metadata on the real workspace");
        assert_eq!(actual, expected);
    }

    #[test]
    fn engine_depending_on_api_is_flagged() {
        let found = violations(&graph(&[("engine", &["api"])]));
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(found[0].contains("engine") && found[0].contains("api"));
    }

    #[test]
    fn engine_depending_on_server_is_flagged() {
        let found = violations(&graph(&[("engine", &["server"])]));
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(found[0].contains("engine") && found[0].contains("server"));
    }

    #[test]
    fn engine_reaching_api_transitively_is_flagged() {
        let found = violations(&graph(&[("engine", &["sandbox"]), ("sandbox", &["api"])]));
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(found[0].contains("engine") && found[0].contains("api"));
    }

    #[test]
    fn cli_depending_on_engine_is_flagged() {
        let found = violations(&graph(&[("cli", &["api", "engine"])]));
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(found[0].contains("cli") && found[0].contains("engine"));
    }

    #[test]
    fn cli_reaching_engine_through_api_is_flagged() {
        let found = violations(&graph(&[("cli", &["api"]), ("api", &["engine"])]));
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(found[0].contains("cli") && found[0].contains("engine"));
    }

    #[test]
    fn proto_depending_on_a_product_crate_is_flagged() {
        let found = violations(&graph(&[("proto", &["engine"])]));
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(found[0].contains("proto") && found[0].contains("engine"));
    }

    #[test]
    fn xtask_is_exempt() {
        let graph = graph(&[("xtask", &["engine", "api", "server"])]);
        assert_eq!(violations(&graph), Vec::<String>::new());
    }

    #[test]
    fn missing_product_crate_is_flagged() {
        let mut graph = graph(&[]);
        graph.remove("cli");
        let found = violations(&graph);
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(found[0].contains("cli") && found[0].contains("missing"));
    }
}
