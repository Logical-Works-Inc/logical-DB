//! Source-anchored execution-access registry (#25): closed, bounded, acyclic.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
//! S33 S34 S35 S36 S37 S38 S39 S40 S41 S42 S43. Source URLs are in
//! `evidence/SOURCES.md`.
//!
//! `contract/access-graph.toml` names every durable state transition, the
//! resources it reads and writes, its receipt, bound, atomicity and failure
//! behaviour, and the source anchor that implements it. This test refuses a
//! registry that drifts from the code: an absent anchor, a source the inventory
//! does not require, an unknown resource or predecessor, a causal cycle, an
//! unbounded limit, or a resource no system writes. Each refusal has a
//! falsifier below that mutates the committed registry and proves the refusal.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

/// Transition kinds the registry admits.
const KINDS: [&str; 7] = [
    "admission",
    "commit",
    "http",
    "migration",
    "recovery",
    "replication",
    "synchronization",
];

/// Why the registry was refused or could not be read.
#[derive(Debug)]
enum GraphError {
    /// The registry or an anchored source file could not be read.
    Unreadable(PathBuf, std::io::Error),
    /// The registry is not valid TOML (duplicate keys included).
    Syntax(toml::de::Error),
    /// The registry parsed but breaks a contract rule.
    Refused(String),
    /// The e2e crate is not two levels below the workspace root.
    Layout,
    /// Rendering the digraph failed.
    Render(std::fmt::Error),
}

impl From<std::fmt::Error> for GraphError {
    fn from(cause: std::fmt::Error) -> Self {
        Self::Render(cause)
    }
}

impl std::fmt::Display for GraphError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            Self::Unreadable(ref path, ref cause) => {
                write!(formatter, "cannot read {}: {cause}", path.display())
            }
            Self::Syntax(ref cause) => write!(formatter, "cannot parse access graph: {cause}"),
            Self::Refused(ref reason) => formatter.write_str(reason),
            Self::Layout => formatter.write_str("e2e crate is not below the workspace root"),
            Self::Render(ref cause) => write!(formatter, "cannot render access graph: {cause}"),
        }
    }
}

impl std::error::Error for GraphError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match *self {
            Self::Unreadable(_, ref cause) => Some(cause),
            Self::Syntax(ref cause) => Some(cause),
            Self::Render(ref cause) => Some(cause),
            Self::Refused(_) | Self::Layout => None,
        }
    }
}

impl From<toml::de::Error> for GraphError {
    fn from(cause: toml::de::Error) -> Self {
        Self::Syntax(cause)
    }
}

/// A refusal with a formatted reason.
fn refused(reason: String) -> GraphError {
    GraphError::Refused(reason)
}

/// One validated transition.
#[derive(Debug)]
struct System {
    /// Unique system name.
    name: String,
    /// `path#anchor` of the implementing code.
    source: String,
    /// Resources the transition reads.
    reads: Vec<String>,
    /// Resources the transition writes.
    writes: Vec<String>,
    /// Systems whose receipt must precede this one.
    predecessors: Vec<String>,
}

/// The validated registry.
#[derive(Debug)]
struct Graph {
    /// Declared resource names.
    resources: BTreeSet<String>,
    /// Declared systems in file order.
    systems: Vec<System>,
}

/// The workspace root, two levels above this crate's manifest.
fn repository_root() -> Result<PathBuf, GraphError> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .ok_or(GraphError::Layout)
}

/// Reads `path` as UTF-8, naming it in the failure.
fn read(path: &Path) -> Result<String, GraphError> {
    std::fs::read_to_string(path).map_err(|cause| GraphError::Unreadable(path.to_path_buf(), cause))
}

/// A required string list; empty only when `allow_empty`.
fn strings(
    record: &toml::Table,
    key: &str,
    location: &str,
    allow_empty: bool,
) -> Result<Vec<String>, GraphError> {
    let qualifier = if allow_empty {
        "a string list"
    } else {
        "a non-empty string list"
    };
    let wrong = || refused(format!("{location}.{key} must be {qualifier}"));
    let values = record
        .get(key)
        .and_then(toml::Value::as_array)
        .ok_or_else(wrong)?;
    if values.is_empty() && !allow_empty {
        return Err(wrong());
    }
    values
        .iter()
        .map(|value| match value.as_str() {
            Some(text) if !text.is_empty() => Ok(text.to_owned()),
            _ => Err(wrong()),
        })
        .collect()
}

/// A required non-empty string field.
fn required(record: &toml::Table, key: &str, location: &str) -> Result<String, GraphError> {
    match record.get(key).and_then(toml::Value::as_str) {
        Some(text) if !text.is_empty() => Ok(text.to_owned()),
        _ => Err(refused(format!("{location}.{key} is required"))),
    }
}

/// Refuses a list that names one entry twice.
fn unique(values: &[String]) -> bool {
    values.iter().collect::<BTreeSet<_>>().len() == values.len()
}

/// The `source` anchor must name a readable file that contains the marker.
fn anchored(root: &Path, source: &str, location: &str) -> Result<(), GraphError> {
    let (path, marker) = source
        .split_once('#')
        .filter(|&(path, marker)| !path.is_empty() && !marker.is_empty())
        .ok_or_else(|| refused(format!("{location}.source must be path#anchor")))?;
    if read(&root.join(path))?.contains(marker) {
        Ok(())
    } else {
        Err(refused(format!(
            "{location}.source anchor is absent: {source}"
        )))
    }
}

/// The array of tables under `key`, each a table.
fn tables<'doc>(
    document: &'doc toml::Table,
    key: &str,
) -> Result<Vec<&'doc toml::Table>, GraphError> {
    let records = document
        .get(key)
        .and_then(toml::Value::as_array)
        .filter(|records| !records.is_empty())
        .ok_or_else(|| refused("access graph needs resource and system records".to_owned()))?;
    records
        .iter()
        .enumerate()
        .map(|(index, record)| {
            record
                .as_table()
                .ok_or_else(|| refused(format!("{key}[{index}] must be a table")))
        })
        .collect()
}

/// Validates one resource record and returns its name.
fn resource(record: &toml::Table, location: &str) -> Result<String, GraphError> {
    let name = required(record, "name", location)?;
    required(record, "owner", location)?;
    match record.get("durability").and_then(toml::Value::as_str) {
        Some("durable" | "ephemeral") => Ok(name),
        _ => Err(refused(format!(
            "{location}.durability must be durable or ephemeral"
        ))),
    }
}

/// Validates one system record against the declared resources.
fn system(
    root: &Path,
    record: &toml::Table,
    location: &str,
    resources: &BTreeSet<String>,
) -> Result<System, GraphError> {
    let name = required(record, "name", location)?;
    required(record, "owner", location)?;
    let source = required(record, "source", location)?;
    anchored(root, &source, location)?;
    let kind = record.get("kind").and_then(toml::Value::as_str);
    if !kind.is_some_and(|kind| KINDS.contains(&kind)) {
        return Err(refused(format!(
            "{location}.kind must be a declared transition kind"
        )));
    }
    let reads = strings(record, "reads", location, false)?;
    let writes = strings(record, "writes", location, false)?;
    if !unique(&reads) || !unique(&writes) {
        return Err(refused(format!(
            "{location} resource lists must not contain duplicates"
        )));
    }
    if let Some(unknown) = reads
        .iter()
        .chain(&writes)
        .find(|name| !resources.contains(*name))
    {
        return Err(refused(format!(
            "{location} references unknown resource {unknown:?}"
        )));
    }
    for field in ["receipt", "limit", "atomicity", "failure"] {
        let value = required(record, field, location)?;
        if field == "limit" && value.to_lowercase().contains("unbounded") {
            return Err(refused(format!("{location}.limit must declare a bound")));
        }
    }
    let predecessors = strings(record, "predecessors", location, true)?;
    Ok(System {
        name,
        source,
        reads,
        writes,
        predecessors,
    })
}

/// Refuses a causal predecessor cycle.
fn acyclic(systems: &BTreeMap<&str, &System>) -> Result<(), GraphError> {
    /// Depth-first visit; `visiting` holds the current path.
    fn visit<'graph>(
        name: &'graph str,
        systems: &BTreeMap<&'graph str, &'graph System>,
        visiting: &mut BTreeSet<&'graph str>,
        visited: &mut BTreeSet<&'graph str>,
    ) -> Result<(), GraphError> {
        if visiting.contains(name) {
            return Err(refused(format!("causal predecessor cycle includes {name}")));
        }
        if visited.contains(name) {
            return Ok(());
        }
        visiting.insert(name);
        if let Some(system) = systems.get(name) {
            for predecessor in &system.predecessors {
                visit(predecessor, systems, visiting, visited)?;
            }
        }
        visiting.remove(name);
        visited.insert(name);
        Ok(())
    }
    let mut visiting = BTreeSet::new();
    let mut visited = BTreeSet::new();
    for name in systems.keys() {
        visit(name, systems, &mut visiting, &mut visited)?;
    }
    Ok(())
}

/// Loads and validates the registry at `path`, anchoring sources at `root`.
fn load(root: &Path, path: &Path) -> Result<Graph, GraphError> {
    let document: toml::Table = toml::from_str(&read(path)?)?;
    if document.get("version").and_then(toml::Value::as_integer) != Some(2) {
        return Err(refused("access graph version must be 2".to_owned()));
    }
    let required_sources = strings(&document, "required_source", "document", false)?;
    if !unique(&required_sources) {
        return Err(refused(
            "document.required_source must not contain duplicates".to_owned(),
        ));
    }
    let mut resources = BTreeSet::new();
    for (index, record) in tables(&document, "resource")?.into_iter().enumerate() {
        let location = format!("resource[{index}]");
        if !resources.insert(resource(record, &location)?) {
            return Err(refused(format!("{location}.name must be unique")));
        }
    }
    let mut systems = Vec::new();
    let mut names = BTreeSet::new();
    for (index, record) in tables(&document, "system")?.into_iter().enumerate() {
        let location = format!("system[{index}]");
        let validated = system(root, record, &location, &resources)?;
        if !names.insert(validated.name.clone()) {
            return Err(refused(format!("{location}.name must be unique")));
        }
        systems.push(validated);
    }
    let by_name: BTreeMap<&str, &System> = systems
        .iter()
        .map(|system| (system.name.as_str(), system))
        .collect();
    for system in &systems {
        if let Some(unknown) = system
            .predecessors
            .iter()
            .find(|predecessor| !by_name.contains_key(predecessor.as_str()))
        {
            return Err(refused(format!(
                "system {} has unknown predecessor {unknown:?}",
                system.name
            )));
        }
    }
    acyclic(&by_name)?;
    inventory(&required_sources, &systems)?;
    let unwritten: Vec<&str> = resources
        .iter()
        .filter(|resource| {
            !systems
                .iter()
                .any(|system| system.writes.contains(resource))
        })
        .map(String::as_str)
        .collect();
    if !unwritten.is_empty() {
        return Err(refused(format!(
            "resources without a declared writer: {}",
            unwritten.join(", ")
        )));
    }
    Ok(Graph { resources, systems })
}

/// The declared sources must equal the required inventory exactly.
fn inventory(required_sources: &[String], systems: &[System]) -> Result<(), GraphError> {
    let declared: BTreeSet<&str> = systems
        .iter()
        .map(|system| system.source.as_str())
        .collect();
    let required: BTreeSet<&str> = required_sources.iter().map(String::as_str).collect();
    let missing: Vec<&str> = required.difference(&declared).copied().collect();
    let unexpected: Vec<&str> = declared.difference(&required).copied().collect();
    let mut details = Vec::new();
    if !missing.is_empty() {
        details.push(format!("missing {missing:?}"));
    }
    if !unexpected.is_empty() {
        details.push(format!("unexpected {unexpected:?}"));
    }
    if details.is_empty() {
        Ok(())
    } else {
        Err(refused(format!(
            "source inventory mismatch: {}",
            details.join("; ")
        )))
    }
}

/// Renders the registry as a deterministic Graphviz digraph.
fn render_dot(graph: &Graph) -> Result<String, std::fmt::Error> {
    let mut out = String::from("digraph access_graph {\n  rankdir=LR;\n");
    let mut systems: Vec<&System> = graph.systems.iter().collect();
    systems.sort_by(|left, right| left.name.cmp(&right.name));
    for system in systems {
        let name = &system.name;
        writeln!(out, "  \"{name}\" [shape=box];")?;
        for predecessor in sorted(&system.predecessors) {
            writeln!(
                out,
                "  \"{predecessor}\" -> \"{name}\" [label=\"receipt\"];"
            )?;
        }
        for resource in sorted(&system.reads) {
            writeln!(out, "  \"{resource}\" -> \"{name}\" [label=\"read\"];")?;
        }
        for resource in sorted(&system.writes) {
            writeln!(out, "  \"{name}\" -> \"{resource}\" [label=\"write\"];")?;
        }
    }
    for resource in &graph.resources {
        writeln!(out, "  \"{resource}\" [shape=ellipse];")?;
    }
    out.push_str("}\n");
    Ok(out)
}

/// A sorted view of `values`.
fn sorted(values: &[String]) -> BTreeSet<&str> {
    values.iter().map(String::as_str).collect()
}

/// The committed registry path under `root`.
fn committed(root: &Path) -> PathBuf {
    root.join("contract/access-graph.toml")
}

/// Loads the committed registry with its first `old` replaced by `new`, and
/// returns the refusal reason. A mutation that is not applied, or a registry
/// that still loads, fails the falsifier rather than passing it.
fn refusal_of(old: &str, new: &str) -> Result<String, GraphError> {
    static COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let root = repository_root()?;
    let text = read(&committed(&root))?;
    if !text.contains(old) {
        return Err(refused(format!("fixture text is absent: {old}")));
    }
    let mutated = text.replacen(old, new, 1);
    let id = COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let path = std::env::temp_dir().join(format!(
        "logical-db-access-graph-{}-{id}.toml",
        std::process::id()
    ));
    std::fs::write(&path, mutated).map_err(|cause| GraphError::Unreadable(path.clone(), cause))?;
    let outcome = load(&root, &path);
    std::fs::remove_file(&path).map_err(|cause| GraphError::Unreadable(path.clone(), cause))?;
    match outcome {
        Ok(_) => Err(refused(format!("mutation was admitted: {old} -> {new}"))),
        Err(error) => Ok(error.to_string()),
    }
}

/// Asserts the mutation is refused with `expected` in the reason.
fn assert_refused(old: &str, new: &str, expected: &str) -> Result<(), GraphError> {
    let reason = refusal_of(old, new)?;
    assert!(
        reason.contains(expected),
        "expected a refusal naming {expected:?}, got {reason:?}"
    );
    Ok(())
}

#[test]
fn registry_is_source_anchored_closed_and_deterministic() -> Result<(), GraphError> {
    let root = repository_root()?;
    let graph = load(&root, &committed(&root))?;
    assert_eq!(
        graph.systems.len(),
        16,
        "the registry declares 16 transitions"
    );
    let first = render_dot(&graph)?;
    let second = render_dot(&load(&root, &committed(&root))?)?;
    assert_eq!(
        first, second,
        "rendering the same registry twice must match"
    );
    assert!(
        first.contains("\"embedded.remote-page\" -> \"replica.state\" [label=\"write\"];"),
        "the remote page install writes replica state"
    );
    Ok(())
}

#[test]
fn unknown_resource_is_refused() -> Result<(), GraphError> {
    assert_refused(
        "reads = [\"replica.state\", \"replica.outbox\"]",
        "reads = [\"unknown.resource\"]",
        "unknown resource",
    )
}

#[test]
fn absent_source_anchor_is_refused() -> Result<(), GraphError> {
    assert_refused(
        "source = \"crates/logical-db/src/edit.rs#pub fn edit<T>(\"",
        "source = \"crates/logical-db/src/edit.rs#pub fn absent<T>(\"",
        "anchor is absent",
    )
}

#[test]
fn unreadable_anchor_file_is_refused() -> Result<(), GraphError> {
    assert_refused(
        "source = \"crates/logical-db/src/edit.rs#pub fn edit<T>(\"",
        "source = \"crates/logical-db/src/absent.rs#pub fn edit<T>(\"",
        "cannot read",
    )
}

#[test]
fn missing_required_source_is_refused() -> Result<(), GraphError> {
    assert_refused(
        "  \"crates/logical-db/src/edit.rs#pub fn edit<T>(\",\n",
        "",
        "source inventory mismatch",
    )
}

#[test]
fn unknown_predecessor_is_refused() -> Result<(), GraphError> {
    assert_refused(
        "predecessors = []",
        "predecessors = [\"missing\"]",
        "unknown predecessor",
    )
}

#[test]
fn causal_cycle_is_refused() -> Result<(), GraphError> {
    assert_refused(
        "predecessors = []",
        "predecessors = [\"embedded.migrate\"]",
        "causal predecessor cycle",
    )
}

#[test]
fn missing_atomicity_is_refused() -> Result<(), GraphError> {
    assert_refused(
        "atomicity = \"state and outbox commit together\"",
        "# atomicity omitted",
        "atomicity is required",
    )
}

#[test]
fn unbounded_limit_is_refused() -> Result<(), GraphError> {
    assert_refused(
        "limit = \"one document and MAX_CHANGE_BYTES\"",
        "limit = \"unbounded\"",
        "must declare a bound",
    )
}

#[test]
fn duplicate_key_is_refused_not_overwritten() -> Result<(), GraphError> {
    assert_refused(
        "limit = \"one document and MAX_CHANGE_BYTES\"",
        "limit = \"one document and MAX_CHANGE_BYTES\"\nlimit = \"unbounded\"",
        "cannot parse access graph",
    )
}

#[test]
fn wrong_version_is_refused() -> Result<(), GraphError> {
    assert_refused("version = 2", "version = 3", "version must be 2")
}
