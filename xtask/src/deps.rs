//! The workspace dependency-rule check.
//!
//! Two hard rules, enforced on transitive reachability between workspace
//! crates:
//!
//! - `hako-engine` never reaches `hako-server` or `hako-api`
//! - `hako-cli` reaches `hako-api` only
//!
//! Reachability is computed over the full resolved graph, by package ID:
//! identity by ID means an external crate that happens to share a member's
//! name is never mistaken for it, and walking non-member nodes means a
//! forbidden edge can't hide behind an out-of-workspace path or git dep.
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

const ENGINE: &str = "hako-engine";
const API: &str = "hako-api";
const SANDBOX: &str = "hako-sandbox";
const SERVER: &str = "hako-server";
const CLI: &str = "hako-cli";

// Also the vocabulary of the rules below — a rename that updates this
// list updates the rules with it.
const PRODUCT_CRATES: [&str; 5] = [API, CLI, ENGINE, SANDBOX, SERVER];

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
        if reached != API {
            found.push(format!(
                "`{CLI}` may depend on `{API}` only, but reaches `{reached}`"
            ));
        }
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

    /// The five product crates with no edges, then `overrides` applied.
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

    #[test]
    fn intended_workspace_shape_passes() {
        let graph = graph(&[
            ("hako-sandbox", &["hako-engine"]),
            ("hako-server", &["hako-engine", "hako-api", "hako-sandbox"]),
            ("hako-cli", &["hako-api"]),
        ]);
        assert_eq!(violations(&graph), Vec::<String>::new());
    }

    #[test]
    fn engine_depending_on_api_is_flagged() {
        let found = violations(&graph(&[("hako-engine", &["hako-api"])]));
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(found[0].contains("hako-engine") && found[0].contains("hako-api"));
    }

    #[test]
    fn engine_depending_on_server_is_flagged() {
        let found = violations(&graph(&[("hako-engine", &["hako-server"])]));
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(found[0].contains("hako-engine") && found[0].contains("hako-server"));
    }

    #[test]
    fn engine_reaching_api_transitively_is_flagged() {
        let found = violations(&graph(&[
            ("hako-engine", &["hako-sandbox"]),
            ("hako-sandbox", &["hako-api"]),
        ]));
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(found[0].contains("hako-engine") && found[0].contains("hako-api"));
    }

    #[test]
    fn cli_depending_on_engine_is_flagged() {
        let found = violations(&graph(&[("hako-cli", &["hako-api", "hako-engine"])]));
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(found[0].contains("hako-cli") && found[0].contains("hako-engine"));
    }

    #[test]
    fn cli_reaching_engine_through_api_is_flagged() {
        let found = violations(&graph(&[
            ("hako-cli", &["hako-api"]),
            ("hako-api", &["hako-engine"]),
        ]));
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(found[0].contains("hako-cli") && found[0].contains("hako-engine"));
    }

    #[test]
    fn xtask_is_exempt() {
        let graph = graph(&[("xtask", &["hako-engine", "hako-api", "hako-server"])]);
        assert_eq!(violations(&graph), Vec::<String>::new());
    }

    #[test]
    fn missing_product_crate_is_flagged() {
        let mut graph = graph(&[]);
        graph.remove("hako-cli");
        let found = violations(&graph);
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(found[0].contains("hako-cli") && found[0].contains("missing"));
    }
}
