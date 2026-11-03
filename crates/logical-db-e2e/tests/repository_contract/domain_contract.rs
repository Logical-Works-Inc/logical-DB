//! Four-domain ownership and allowed-edge contract (#73).
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
//! S33 S34 S35 S36 S37 S38 S39 S40 S41 S42 S43. Source URLs are in
//! `evidence/SOURCES.md`.
//!
//! `contract/domains/*.toml` declares who owns every workspace crate and which
//! owners each may import. This test reads the live `cargo metadata` graph and
//! refuses a crate with no owner or two, a declared crate that is not a member,
//! a cross-domain import the consumer's `may_depend_on` does not allow, a
//! product domain importing another domain's implementation instead of its
//! public contract, and a predecessor twin reappearing as a crate. The committed
//! `evidence/domain-graph.md` receipt must match the live graph; set
//! `LOGICAL_DB_EMIT_DOMAIN_GRAPH=1` to regenerate it. A gate that only ever
//! passes proves nothing, so each refusal has a falsifier that injects the
//! forbidden edge into a copy of the live graph.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use lgwks_std::json::Value;

use crate::workspace::{ContractError, read, read_toml, repository_root, workspace_graph};

/// The four product domains, each declared in its own manifest.
const DOMAINS: [&str; 4] = ["cloud", "cairn", "lem", "forge"];
/// Every owner label, product domains and layers together.
const KNOWN_OWNERS: [&str; 8] = [
    "kernel", "cloud", "cairn", "lem", "forge", "facade", "adapter", "e2e",
];
/// External dependency classes a policy may name; not internal owners.
const EXTERNAL_LABELS: [&str; 1] = ["approved-estate"];
/// The committed receipt, relative to the root: canonical JSON regenerated
/// with `LOGICAL_DB_EMIT_DOMAIN_GRAPH=1`, never hand-edited.
const RECEIPT: &str = "evidence/domain-graph.json";

/// A refusal with a formatted reason.
fn refused(reason: String) -> ContractError {
    ContractError::Refused(reason)
}

/// One owner's declared crates and import policy.
#[derive(Debug, Clone)]
struct Owner {
    /// Crates this owner declares.
    crates: BTreeSet<String>,
    /// Crates other domains may import.
    public_contracts: BTreeSet<String>,
    /// Owners this owner may import.
    may_depend_on: BTreeSet<String>,
    /// Owners this owner must never import.
    forbidden_domains: BTreeSet<String>,
}

/// Owner label to record.
type Owners = BTreeMap<String, Owner>;
/// Crate to the workspace crates it depends on.
type Graph = BTreeMap<String, BTreeSet<String>>;

/// The string list under `key`; `None` when the key is absent.
fn string_list(
    table: &toml::Table,
    key: &str,
    place: &str,
) -> Result<Option<BTreeSet<String>>, ContractError> {
    let Some(value) = table.get(key) else {
        return Ok(None);
    };
    let wrong = || refused(format!("{place} field {key} must be a list of strings"));
    value
        .as_array()
        .ok_or_else(wrong)?
        .iter()
        .map(|item| item.as_str().map(str::to_owned).ok_or_else(wrong))
        .collect::<Result<BTreeSet<_>, _>>()
        .map(Some)
}

/// The string list under `key`; an absent key declares no entries.
fn optional_list(
    table: &toml::Table,
    key: &str,
    place: &str,
) -> Result<BTreeSet<String>, ContractError> {
    match string_list(table, key, place)? {
        Some(values) => Ok(values),
        None => Ok(BTreeSet::new()),
    }
}

/// Whether `label` is an owner or external class a policy may name.
fn known_label(label: &str) -> bool {
    KNOWN_OWNERS.contains(&label) || EXTERNAL_LABELS.contains(&label)
}

/// One owner record; every policy label must be a known owner or class.
fn owner_record(table: &toml::Table, place: &str) -> Result<Owner, ContractError> {
    let record = Owner {
        crates: optional_list(table, "crates", place)?,
        public_contracts: optional_list(table, "public_contracts", place)?,
        may_depend_on: optional_list(table, "may_depend_on", place)?,
        forbidden_domains: optional_list(table, "forbidden_domains", place)?,
    };
    if let Some(label) = record
        .may_depend_on
        .iter()
        .chain(&record.forbidden_domains)
        .find(|label| !known_label(label))
    {
        return Err(refused(format!(
            "{place} names unknown owner label {label:?}"
        )));
    }
    Ok(record)
}

/// The descriptive fields every product-domain manifest must carry.
fn manifest_shape(table: &toml::Table, domain: &str) -> Vec<String> {
    let mut failures = Vec::new();
    for key in ["owner", "status", "release_lane"] {
        if !table
            .get(key)
            .and_then(toml::Value::as_str)
            .is_some_and(|text| !text.is_empty())
        {
            failures.push(format!("domain {domain} lacks nonempty {key}"));
        }
    }
    for key in [
        "crates",
        "may_depend_on",
        "forbidden_domains",
        "evidence",
        "blocked_claims",
    ] {
        if !matches!(string_list(table, key, domain), Ok(Some(_))) {
            failures.push(format!("domain {domain} lacks string list {key}"));
        }
    }
    failures
}

/// Every owner record, the composition layers, the twin denylist, and the
/// manifest-shape failures.
struct Contract {
    /// Owner label to record.
    owners: Owners,
    /// Layers that may import an allowed domain's implementation crates.
    composition: BTreeSet<String>,
    /// Predecessor names that must never reappear.
    denylist: BTreeSet<String>,
    /// Descriptive-field and manifest-inventory failures.
    shape: Vec<String>,
}

/// The product-domain manifests under `directory`: every `*.toml` but
/// `GRAPH.toml`.
fn domain_manifests(directory: &Path) -> Result<BTreeSet<std::path::PathBuf>, ContractError> {
    let entries = std::fs::read_dir(directory)
        .map_err(|cause| ContractError::Io(directory.to_path_buf(), cause))?;
    let mut manifests = BTreeSet::new();
    for entry in entries {
        let path = entry
            .map_err(|cause| ContractError::Io(directory.to_path_buf(), cause))?
            .path();
        if path
            .extension()
            .is_some_and(|extension| extension == "toml")
            && path.file_name().is_some_and(|name| name != "GRAPH.toml")
        {
            manifests.insert(path);
        }
    }
    Ok(manifests)
}

/// Loads the four domain manifests and `GRAPH.toml` under `root`.
fn load(root: &Path) -> Result<Contract, ContractError> {
    let directory = root.join("contract/domains");
    let mut owners = Owners::new();
    let mut shape = Vec::new();
    for path in &domain_manifests(&directory)? {
        let document = read_toml(path)?;
        let place = path.display().to_string();
        let Some(domain) = document.get("domain").and_then(toml::Value::as_str) else {
            shape.push(format!("domain manifest lacks string domain: {place}"));
            continue;
        };
        if !DOMAINS.contains(&domain) {
            shape.push(format!("unexpected domain manifest: {domain}"));
            continue;
        }
        if path.file_stem().is_none_or(|stem| stem != domain) {
            return Err(refused(format!(
                "{place} declares domain {domain:?}, expected its file name"
            )));
        }
        shape.extend(manifest_shape(&document, domain));
        owners.insert(domain.to_owned(), owner_record(&document, &place)?);
    }
    if let Some(missing) = DOMAINS.iter().find(|domain| !owners.contains_key(**domain)) {
        shape.push(format!("missing domain manifest: {missing}"));
    }
    let graph_path = directory.join("GRAPH.toml");
    let graph = read_toml(&graph_path)?;
    let place = graph_path.display().to_string();
    let layers = graph
        .get("layers")
        .and_then(toml::Value::as_table)
        .filter(|layers| !layers.is_empty())
        .ok_or_else(|| refused(format!("{place} must declare a nonempty [layers] table")))?;
    for (name, layer) in layers {
        if !KNOWN_OWNERS.contains(&name.as_str()) {
            return Err(refused(format!("{place} declares unknown layer {name:?}")));
        }
        let table = layer
            .as_table()
            .ok_or_else(|| refused(format!("{place} layer {name:?} must be a table")))?;
        owners.insert(
            name.clone(),
            owner_record(table, &format!("{place} layer {name}"))?,
        );
    }
    let unowned: Vec<&str> = KNOWN_OWNERS
        .iter()
        .copied()
        .filter(|owner| !owners.contains_key(*owner))
        .collect();
    if !unowned.is_empty() {
        return Err(refused(format!(
            "no ownership record for: {}",
            unowned.join(" ")
        )));
    }
    let section = |key: &str, field: &str| -> Result<BTreeSet<String>, ContractError> {
        match graph.get(key).and_then(toml::Value::as_table) {
            Some(table) => optional_list(table, field, &format!("{place} [{key}]")),
            None => Ok(BTreeSet::new()),
        }
    };
    let composition = section("composition", "domains")?;
    if let Some(unknown) = composition
        .iter()
        .find(|name| !KNOWN_OWNERS.contains(&name.as_str()))
    {
        return Err(refused(format!(
            "{place} composition names unknown owner {unknown}"
        )));
    }
    let denylist = section("twin_denylist", "crates")?;
    Ok(Contract {
        owners,
        composition,
        denylist,
        shape,
    })
}

/// Ownership failures and the crate-to-owner assignment.
fn ownership(owners: &Owners, graph: &Graph) -> (Vec<String>, BTreeMap<String, String>) {
    let mut failures = Vec::new();
    let mut assigned = BTreeMap::new();
    for (owner, record) in owners {
        for krate in &record.crates {
            if let Some(previous) = assigned.insert(krate.clone(), owner.clone()) {
                failures.push(format!(
                    "crate {krate} is owned by both {previous} and {owner}"
                ));
            }
        }
    }
    for krate in graph.keys().filter(|krate| !assigned.contains_key(*krate)) {
        failures.push(format!("workspace crate {krate} has no declared owner"));
    }
    for krate in assigned.keys().filter(|krate| !graph.contains_key(*krate)) {
        failures.push(format!("declared crate {krate} is not a workspace member"));
    }
    (failures, assigned)
}

/// Policy-consistency and public-contract failures.
fn contract_failures(owners: &Owners, graph: &Graph) -> Vec<String> {
    let mut failures = Vec::new();
    for (owner, record) in owners {
        let overlap: Vec<&String> = record
            .may_depend_on
            .intersection(&record.forbidden_domains)
            .collect();
        if !overlap.is_empty() {
            failures.push(format!(
                "{owner} lists {overlap:?} as both allowed and forbidden"
            ));
        }
        for contract in &record.public_contracts {
            if graph.contains_key(contract) && !record.crates.contains(contract) {
                failures.push(format!("{owner} publishes {contract} but does not own it"));
            }
        }
    }
    failures
}

/// One cross-owner import.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Edge<'graph> {
    /// The importing crate.
    krate: &'graph str,
    /// The imported crate.
    dependency: &'graph str,
    /// The importing crate's owner.
    consumer: &'graph str,
    /// The imported crate's owner.
    owner: &'graph str,
}

/// Every cross-owner edge, sorted by crate then dependency.
fn cross_edges<'graph>(
    owners: &'graph Owners,
    assigned: &'graph BTreeMap<String, String>,
    graph: &'graph Graph,
) -> Vec<Edge<'graph>> {
    let mut edges = Vec::new();
    for (consumer, record) in owners {
        for krate in &record.crates {
            for dependency in graph.get(krate).into_iter().flatten() {
                match assigned.get(dependency) {
                    Some(owner) if owner != consumer => edges.push(Edge {
                        krate,
                        dependency,
                        consumer,
                        owner,
                    }),
                    Some(_) | None => {}
                }
            }
        }
    }
    edges.sort_unstable();
    edges
}

/// Every cross-owner import the declared policy does not allow.
fn edge_failures(
    owners: &Owners,
    assigned: &BTreeMap<String, String>,
    graph: &Graph,
    composition: &BTreeSet<String>,
) -> Vec<String> {
    let mut failures = Vec::new();
    for edge in cross_edges(owners, assigned, graph) {
        let Edge {
            krate,
            dependency,
            consumer,
            owner,
        } = edge;
        let allowed = owners
            .get(consumer)
            .is_some_and(|record| record.may_depend_on.contains(owner));
        if !allowed {
            failures.push(format!(
                "{krate} -> {dependency}: {consumer} may not depend on {owner}"
            ));
            continue;
        }
        if composition.contains(consumer) || owner == "kernel" {
            continue;
        }
        let published = owners
            .get(owner)
            .is_some_and(|target| target.public_contracts.contains(dependency));
        if !published {
            failures.push(format!(
                "{krate} -> {dependency}: {consumer} may import only {owner} public contracts, not implementation"
            ));
        }
    }
    failures
}

/// Every workspace crate whose name matches a predecessor twin.
fn twin_failures(graph: &Graph, denylist: &BTreeSet<String>) -> Vec<String> {
    let mut failures = Vec::new();
    for krate in graph.keys() {
        let lowered = krate.to_lowercase();
        for token in denylist {
            if lowered == *token
                || lowered.ends_with(&format!("-{token}"))
                || lowered.contains(&format!("-{token}-"))
            {
                failures.push(format!(
                    "crate {krate} is a forbidden predecessor twin of {token}"
                ));
            }
        }
    }
    failures
}

/// Ownership, contract and edge failures for one owners/graph pair.
fn failures_for(owners: &Owners, graph: &Graph, composition: &BTreeSet<String>) -> Vec<String> {
    let (mut failures, assigned) = ownership(owners, graph);
    failures.extend(contract_failures(owners, graph));
    failures.extend(edge_failures(owners, &assigned, graph, composition));
    failures
}

/// A JSON array of the given strings.
fn strings<'item>(values: impl IntoIterator<Item = &'item String>) -> Value {
    Value::Array(values.into_iter().cloned().map(Value::String).collect())
}

/// The canonical receipt for the live graph.
fn receipt(contract: &Contract, graph: &Graph) -> Value {
    let (_, assigned) = ownership(&contract.owners, graph);
    let edges = cross_edges(&contract.owners, &assigned, graph)
        .into_iter()
        .map(|edge| {
            lgwks_std::json::json!({
                "consumer": edge.krate,
                "dependency": edge.dependency,
                "consumer_owner": edge.consumer,
                "dependency_owner": edge.owner,
            })
        })
        .collect();
    let owners = contract
        .owners
        .iter()
        .map(|(owner, record)| (owner.clone(), strings(&record.crates)))
        .collect();
    lgwks_std::json::json!({
        "Evidence": "S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15 \
                     S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32 \
                     S33 S34 S35 S36 S37 S38 S39 S40 S41 S42 S43. Source URLs are in \
                     evidence/SOURCES.md; regenerate with LOGICAL_DB_EMIT_DOMAIN_GRAPH=1.",
        "owners": Value::Object(owners),
        "edges": Value::Array(edges),
        "composition": strings(&contract.composition),
        "twin_denylist": strings(&contract.denylist),
    })
}

/// The live contract and workspace graph.
fn live() -> Result<(Contract, Graph), ContractError> {
    let root = repository_root()?;
    Ok((load(&root)?, workspace_graph(&root)?))
}

/// The failures after adding `consumer -> dependency` to a copy of the live
/// graph, judged under `owners`. The unmutated graph is asserted clean first,
/// so a refusal cannot come from an unrelated failure.
fn injected(
    consumer: &str,
    dependency: &str,
    owners: Option<&Owners>,
) -> Result<Vec<String>, ContractError> {
    let (contract, mut graph) = live()?;
    let baseline = failures_for(&contract.owners, &graph, &contract.composition);
    assert!(
        baseline.is_empty(),
        "the unmutated fixture is not clean; the falsifier would be vacuous: {baseline:?}"
    );
    graph
        .entry(consumer.to_owned())
        .or_insert_with(BTreeSet::new)
        .insert(dependency.to_owned());
    let judged = match owners {
        Some(owners) => owners,
        None => &contract.owners,
    };
    Ok(failures_for(judged, &graph, &contract.composition))
}

/// Asserts a failure naming `needle` is present.
fn assert_refused(failures: &[String], needle: &str) {
    assert!(
        failures.iter().any(|failure| failure.contains(needle)),
        "{needle} was not refused: {failures:?}"
    );
}

/// `contract.owners` with `krate` added to `owner`'s crates.
fn owners_with(contract: &Contract, owner: &str, krate: &str) -> Result<Owners, ContractError> {
    let mut owners = contract.owners.clone();
    owners
        .get_mut(owner)
        .ok_or_else(|| refused(format!("{owner} has a record")))?
        .crates
        .insert(krate.to_owned());
    Ok(owners)
}

#[test]
fn committed_graph_and_receipt_are_clean() -> Result<(), ContractError> {
    let root = repository_root()?;
    let (contract, graph) = live()?;
    let mut failures = failures_for(&contract.owners, &graph, &contract.composition);
    failures.extend(twin_failures(&graph, &contract.denylist));
    failures.extend(contract.shape.iter().cloned());
    assert!(
        failures.is_empty(),
        "the committed graph is not clean: {failures:?}"
    );
    let document = receipt(&contract, &graph);
    let path = root.join(RECEIPT);
    if std::env::var_os("LOGICAL_DB_EMIT_DOMAIN_GRAPH").is_some() {
        let json = lgwks_std::json::to_string_pretty(&document)?;
        std::fs::write(&path, format!("{json}\n"))
            .map_err(|cause| ContractError::Io(path.clone(), cause))?;
    }
    let committed: Value = lgwks_std::json::from_str(&read(&path)?)?;
    assert_eq!(
        committed, document,
        "{RECEIPT} is stale; regenerate it with LOGICAL_DB_EMIT_DOMAIN_GRAPH=1"
    );
    Ok(())
}

#[test]
fn cloud_may_not_depend_on_cairn() -> Result<(), ContractError> {
    let failures = injected("logical-db-crdt", "logical-db-cairn", None)?;
    assert_refused(&failures, "logical-db-crdt -> logical-db-cairn");
    Ok(())
}

#[test]
fn cairn_may_not_import_cloud_implementation() -> Result<(), ContractError> {
    let failures = injected("logical-db-cairn", "logical-db-store", None)?;
    assert_refused(&failures, "logical-db-cairn -> logical-db-store");
    Ok(())
}

#[test]
fn lem_may_not_import_cloud_implementation() -> Result<(), ContractError> {
    let failures = injected("logical-db-episode", "logical-db-store-fdb", None)?;
    assert_refused(&failures, "logical-db-episode -> logical-db-store-fdb");
    Ok(())
}

#[test]
fn forge_may_not_depend_on_cloud() -> Result<(), ContractError> {
    let (contract, _) = live()?;
    let owners = owners_with(&contract, "forge", "logical-db-forge-runtime")?;
    let failures = injected(
        "logical-db-forge-runtime",
        "logical-db-store",
        Some(&owners),
    )?;
    assert_refused(&failures, "logical-db-forge-runtime -> logical-db-store");
    Ok(())
}

#[test]
fn undeclared_crate_has_no_owner() -> Result<(), ContractError> {
    let (contract, mut graph) = live()?;
    graph.insert("logical-db-orphan".to_owned(), BTreeSet::new());
    let failures = failures_for(&contract.owners, &graph, &contract.composition);
    assert_refused(&failures, "logical-db-orphan has no declared owner");
    Ok(())
}

#[test]
fn double_owned_crate_is_refused() -> Result<(), ContractError> {
    let (contract, graph) = live()?;
    let owners = owners_with(&contract, "cairn", "logical-db-store")?;
    let failures = failures_for(&owners, &graph, &contract.composition);
    assert_refused(&failures, "logical-db-store is owned by both");
    Ok(())
}

#[test]
fn every_predecessor_twin_is_refused() -> Result<(), ContractError> {
    let (contract, graph) = live()?;
    assert!(
        !contract.denylist.is_empty(),
        "the twin denylist is declared"
    );
    for token in &contract.denylist {
        let mut mutated = graph.clone();
        let twin = format!("logical-db-{token}");
        mutated.insert(twin.clone(), BTreeSet::new());
        assert_refused(&twin_failures(&mutated, &contract.denylist), &twin);
    }
    Ok(())
}

#[test]
fn a_manifest_without_its_descriptive_fields_is_refused() -> Result<(), ContractError> {
    let root = repository_root()?;
    let mut cloud = read_toml(&root.join("contract/domains/cloud.toml"))?;
    cloud.remove("release_lane");
    cloud.remove("blocked_claims");
    let failures = manifest_shape(&cloud, "cloud");
    assert_refused(&failures, "domain cloud lacks nonempty release_lane");
    assert_refused(&failures, "domain cloud lacks string list blocked_claims");
    Ok(())
}

#[test]
fn an_unknown_policy_label_is_refused() -> Result<(), ContractError> {
    let root = repository_root()?;
    let mut cloud = read_toml(&root.join("contract/domains/cloud.toml"))?;
    cloud.insert(
        "may_depend_on".to_owned(),
        toml::Value::Array(vec!["kernel".into(), "nowhere".into()]),
    );
    let Err(error) = owner_record(&cloud, "cloud") else {
        return Err(refused("an unknown owner label was admitted".to_owned()));
    };
    assert!(
        error.to_string().contains("unknown owner label"),
        "an unknown label is named in the refusal, got {error}"
    );
    Ok(())
}
