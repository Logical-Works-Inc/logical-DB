//! One-way internal crate graph: lower crates never depend on higher ones.
//!
//! Evidence: S01 S02 S03. Source URLs are in `evidence/SOURCES.md`.
//!
//! `cargo metadata` is the one source of the workspace graph. Every workspace
//! crate must be declared below, and each may depend only on the internal
//! crates its row names. A new crate, a removed crate, or an inward edge fails.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::process::Command;

/// Test failure carrying its cause.
type Failure = Box<dyn std::error::Error>;

/// Each workspace crate and the internal crates it may depend on.
const ALLOWED: &[(&str, &[&str])] = &[
    ("logical-db-model", &[]),
    ("logical-db-platform", &[]),
    ("logical-db-protocol", &["logical-db-model"]),
    ("logical-db-crdt", &["logical-db-model"]),
    ("logical-db-store", &["logical-db-model"]),
    (
        "logical-db-sync",
        &["logical-db-model", "logical-db-protocol"],
    ),
    (
        "logical-db-service",
        &["logical-db-crdt", "logical-db-model", "logical-db-protocol"],
    ),
    (
        "logical-db-store-fdb",
        &[
            "logical-db-model",
            "logical-db-protocol",
            "logical-db-service",
        ],
    ),
    (
        "logical-db-http",
        &["logical-db-model", "logical-db-protocol", "logical-db-sync"],
    ),
    (
        "logical-db-server",
        &[
            "logical-db-crdt",
            "logical-db-model",
            "logical-db-protocol",
            "logical-db-service",
        ],
    ),
    (
        "logical-db-daemon",
        &[
            "logical-db-crdt",
            "logical-db-model",
            "logical-db-protocol",
            "logical-db-server",
            "logical-db-service",
            "logical-db-store-fdb",
        ],
    ),
    (
        "logical-db",
        &[
            "logical-db-crdt",
            "logical-db-http",
            "logical-db-model",
            "logical-db-platform",
            "logical-db-protocol",
            "logical-db-store",
            "logical-db-sync",
        ],
    ),
    ("logical-db-kv", &["logical-db", "logical-db-model"]),
    (
        "logical-db-e2e",
        &[
            "logical-db",
            "logical-db-crdt",
            "logical-db-daemon",
            "logical-db-http",
            "logical-db-kv",
            "logical-db-model",
            "logical-db-protocol",
            "logical-db-server",
            "logical-db-service",
            "logical-db-store-fdb",
        ],
    ),
];

/// Workspace crate name mapped to the internal crates it depends on.
type Graph = BTreeMap<String, BTreeSet<String>>;

/// Reads the internal dependency graph from `cargo metadata`.
fn workspace_graph() -> Result<Graph, Failure> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let output = Command::new(std::env::var("CARGO")?)
        .current_dir(root)
        .args(["metadata", "--locked", "--no-deps", "--format-version", "1"])
        .output()?;
    if !output.status.success() {
        return Err(format!("cargo metadata failed: {}", output.status).into());
    }
    let metadata: lgwks_std::json::Value = lgwks_std::json::from_slice(&output.stdout)?;
    let name_of = |value: &'_ lgwks_std::json::Value| {
        value
            .get("name")
            .and_then(lgwks_std::json::Value::as_str)
            .map(str::to_owned)
            .ok_or("an entry lacks a name")
    };
    let packages = metadata
        .get("packages")
        .and_then(lgwks_std::json::Value::as_array)
        .ok_or("cargo metadata lacks packages")?;
    let names: BTreeSet<String> = packages.iter().map(name_of).collect::<Result<_, _>>()?;
    packages
        .iter()
        .map(|package| {
            let name = name_of(package)?;
            let edges = package
                .get("dependencies")
                .and_then(lgwks_std::json::Value::as_array)
                .ok_or_else(|| format!("{name} lacks a dependency list"))?
                .iter()
                .map(name_of)
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .filter(|edge| names.contains(edge))
                .collect();
            Ok((name, edges))
        })
        .collect()
}

/// Every refusal of `graph` against the declared one-way table.
fn graph_failures(graph: &Graph) -> Vec<String> {
    let allowed: BTreeMap<&str, BTreeSet<&str>> = ALLOWED
        .iter()
        .map(|&(name, edges)| (name, edges.iter().copied().collect()))
        .collect();
    let mut failures: Vec<String> = allowed
        .keys()
        .filter(|name| !graph.contains_key(**name))
        .map(|name| format!("declared crate is missing: {name}"))
        .collect();
    for (name, edges) in graph {
        let Some(permitted) = allowed.get(name.as_str()) else {
            failures.push(format!("undeclared workspace crate: {name}"));
            continue;
        };
        let inward: Vec<&String> = edges
            .iter()
            .filter(|edge| !permitted.contains(edge.as_str()))
            .collect();
        if !inward.is_empty() {
            failures.push(format!(
                "{name} has forbidden inward dependencies: {inward:?}"
            ));
        }
    }
    failures
}

/// The live workspace graph matches the one-way table exactly.
#[test]
fn workspace_crate_graph_is_one_way() -> Result<(), Failure> {
    let failures = graph_failures(&workspace_graph()?);
    assert!(
        failures.is_empty(),
        "crate graph refused:\n{}",
        failures.join("\n")
    );
    Ok(())
}

/// An inward edge and an undeclared crate are each refused.
#[test]
fn inward_edges_and_undeclared_crates_are_refused() {
    let mut graph: Graph = ALLOWED
        .iter()
        .map(|&(name, edges)| {
            (
                name.to_owned(),
                edges.iter().map(|edge| (*edge).to_owned()).collect(),
            )
        })
        .collect();
    assert!(
        graph_failures(&graph).is_empty(),
        "the declared table must pass itself"
    );
    if let Some(edges) = graph.get_mut("logical-db-model") {
        edges.insert("logical-db-service".to_owned());
    }
    graph.insert("logical-db-rogue".to_owned(), BTreeSet::new());
    let failures = graph_failures(&graph);
    assert_eq!(
        failures.len(),
        2,
        "one inward edge and one rogue crate: {failures:?}"
    );
}
