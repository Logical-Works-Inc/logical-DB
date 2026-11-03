//! Clean-room rewrite evidence: citations, the source ledger, the internal
//! dependency direction, and churn against the predecessor baseline.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.
//!
//! Every repository text file names at least twenty distinct ledger sources in
//! an explicit evidence header; the ledger declares exactly S01..S43, each with
//! one unique HTTPS URL; no crate depends inward against the layer table; and
//! hash, byte and line churn against the predecessor baseline each exceed 85%,
//! with no baseline blob left unchanged. The domain allowed-edge contract runs
//! in `domain_contract.rs` beside this module.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use lgwks_std::pattern::Regex;

use crate::workspace::{ContractError, output, read, repository_root, workspace_graph};

/// The predecessor tree the rewrite is measured against.
const BASELINE: &str = "7cffbc07b6f312854986ae306b583ef891623b2d";
/// Git's empty tree; a diff against it counts every baseline line.
const EMPTY_TREE: &str = "4b825dc642cb6eb9a060e54bf8d69288fbee4904";
/// Distinct valid sources every file must cite.
const MINIMUM_CITATIONS: usize = 20;
/// Churn every measure must exceed, in percent.
const MINIMUM_CHURN: u64 = 85;
/// The largest file the verifier reads.
const MAX_INSPECTABLE_BYTES: u64 = 16 * 1_048_576;
/// Citations count only inside this leading header.
const CITATION_HEADER_BYTES: usize = 4_096;
/// The source ledger, relative to the root.
const LEDGER: &str = "evidence/SOURCES.md";
/// Highest source number the ledger pins.
const LAST_SOURCE: u8 = 43;

/// The allowed internal dependencies of each workspace crate.
const LAYERS: &[(&str, &[&str])] = &[
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
        "logical-db-e2e",
        &[
            "logical-db-cairn",
            "logical-db-episode",
            "logical-db",
            "logical-db-http",
            "logical-db-kv",
            "logical-db-model",
            "logical-db-protocol",
            "logical-db-server",
            "logical-db-service",
            "logical-db-store-fdb",
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
            "logical-db-cairn",
            "logical-db-episode",
            "logical-db-model",
            "logical-db-crdt",
            "logical-db-http",
            "logical-db-platform",
            "logical-db-protocol",
            "logical-db-store",
            "logical-db-sync",
        ],
    ),
    ("logical-db-kv", &["logical-db", "logical-db-model"]),
    ("logical-db-cairn", &[]),
    ("logical-db-episode", &["logical-db-cairn"]),
    ("logical-db-cloud-contract", &["logical-db-model"]),
];

/// A refusal with a formatted reason.
fn refused(reason: String) -> ContractError {
    ContractError::Refused(reason)
}

/// Compiles one of this file's fixed patterns.
fn pattern(source: &str) -> Result<Regex, ContractError> {
    Regex::new(source).map_err(|cause| refused(cause.to_string()))
}

/// Every source identifier token in `text`.
fn cited(text: &str) -> Result<BTreeSet<String>, ContractError> {
    let ids = pattern(r"\bS\d{2}\b")?;
    Ok(ids
        .find_all(text)
        .map(|found| found.text.to_owned())
        .collect())
}

/// Every ledger declaration as (byte offset, identifier).
fn declarations(ledger: &str) -> Result<Vec<(usize, String)>, ContractError> {
    let declaration = pattern(r"(?m)^- \*\*S\d{2} — [^*]+\*\*")?;
    declaration
        .find_all(ledger)
        .map(|found| match found.text.get(4..7) {
            Some(id) => Ok((found.start, id.to_owned())),
            None => Err(refused("a ledger declaration has no identifier".to_owned())),
        })
        .collect()
}

/// The exact, uniquely declared source identifiers in `ledger`.
fn source_ids(ledger: &str) -> Result<BTreeSet<String>, ContractError> {
    let url = pattern(r"<https://[^>]+>")?;
    let starts = declarations(ledger)?;
    let declared: BTreeSet<String> = starts.iter().map(|entry| entry.1.clone()).collect();
    if declared.len() != starts.len() {
        return Err(refused(
            "source ledger contains duplicate declarations".to_owned(),
        ));
    }
    let expected: BTreeSet<String> = (1..=LAST_SOURCE)
        .map(|number| format!("S{number:02}"))
        .collect();
    if declared != expected {
        let missing: Vec<&String> = expected.difference(&declared).collect();
        let extra: Vec<&String> = declared.difference(&expected).collect();
        return Err(refused(format!(
            "source ledger declarations differ from the pinned set; missing={missing:?} extra={extra:?}"
        )));
    }
    let mut urls = BTreeSet::new();
    for (index, &(start, ref id)) in starts.iter().enumerate() {
        let end = match starts.get(index.saturating_add(1)) {
            Some(&(next, _)) => next,
            None => ledger.len(),
        };
        let entry = ledger
            .get(start..end)
            .ok_or_else(|| refused(format!("{id} is not a ledger span")))?;
        let found: Vec<&str> = url.find_all(entry).map(|found| found.text).collect();
        let &[only] = found.as_slice() else {
            return Err(refused(format!(
                "source {id} must declare exactly one HTTPS URL"
            )));
        };
        if !urls.insert(only) {
            return Err(refused(format!("source ledger repeats URL {only}")));
        }
    }
    Ok(declared)
}

/// Splits NUL-terminated git output into names.
fn names(raw: &[u8]) -> impl Iterator<Item = String> + '_ {
    raw.split(|byte| *byte == 0)
        .filter(|name| !name.is_empty())
        .map(|name| String::from_utf8_lossy(name).into_owned())
}

/// Tracked and new files, excluding ignored build products.
fn repository_files(root: &Path) -> Result<Vec<String>, ContractError> {
    let listed = output(
        root,
        "git",
        &[
            "ls-files",
            "--cached",
            "--others",
            "--exclude-standard",
            "-z",
        ],
    )?;
    Ok(names(&listed)
        .filter(|name| root.join(name).symlink_metadata().is_ok())
        .collect())
}

/// The longest prefix of `text` within the citation header bound that ends
/// on a character boundary.
fn header(text: &str) -> Option<&str> {
    let mut cut = CITATION_HEADER_BYTES.min(text.len());
    while !text.is_char_boundary(cut) {
        cut = cut.saturating_sub(1);
    }
    text.get(..cut)
}

/// Why one file's citations fail, if they do.
fn citation_failures(
    relative: &str,
    payload: &[u8],
    allowed: &BTreeSet<String>,
) -> Result<Vec<String>, ContractError> {
    if payload.contains(&0) {
        return Ok(vec![format!(
            "binary file has no inspectable citations: {relative}"
        )]);
    }
    let Ok(text) = std::str::from_utf8(payload) else {
        return Ok(vec![format!(
            "non-UTF-8 file has no inspectable citations: {relative}"
        )]);
    };
    let mut failures = Vec::new();
    let found = cited(text)?;
    let unknown: Vec<&String> = found.difference(allowed).collect();
    if !unknown.is_empty() {
        failures.push(format!("unknown source IDs in {relative}: {unknown:?}"));
    }
    let valid = if relative == LEDGER {
        found.intersection(allowed).count()
    } else {
        let lead = header(text)
            .ok_or_else(|| refused(format!("{relative} has no character-aligned header")))?;
        if !lead.contains("Evidence") {
            failures.push(format!("missing explicit evidence header: {relative}"));
        }
        cited(lead)?.intersection(allowed).count()
    };
    if valid < MINIMUM_CITATIONS {
        failures.push(format!(
            "{relative} cites {valid}, requires {MINIMUM_CITATIONS}"
        ));
    }
    Ok(failures)
}

/// Citation failures across every repository file.
fn repository_citations(
    root: &Path,
    allowed: &BTreeSet<String>,
) -> Result<Vec<String>, ContractError> {
    let mut failures = Vec::new();
    for relative in repository_files(root)? {
        let path = root.join(&relative);
        let metadata = path
            .symlink_metadata()
            .map_err(|cause| ContractError::Io(path.clone(), cause))?;
        if metadata.file_type().is_symlink() {
            failures.push(format!(
                "repository symlink cannot carry independent citations: {relative}"
            ));
        } else if !metadata.is_file() {
            failures.push(format!("non-regular repository entry: {relative}"));
        } else if metadata.len() > MAX_INSPECTABLE_BYTES {
            failures.push(format!(
                "file exceeds {MAX_INSPECTABLE_BYTES} byte verifier ceiling: {relative}"
            ));
        } else {
            let payload =
                std::fs::read(&path).map_err(|cause| ContractError::Io(path.clone(), cause))?;
            failures.extend(citation_failures(&relative, &payload, allowed)?);
        }
    }
    Ok(failures)
}

/// Inward or undeclared dependencies among the workspace crates.
fn dependency_failures(graph: &BTreeMap<String, BTreeSet<String>>) -> Vec<String> {
    let layers: BTreeMap<&str, &[&str]> = LAYERS.iter().copied().collect();
    let mut failures = Vec::new();
    let missing: Vec<&str> = layers
        .keys()
        .copied()
        .filter(|name| !graph.contains_key(*name))
        .collect();
    if !missing.is_empty() {
        failures.push(format!(
            "required rewrite crates are missing: {}",
            missing.join(" ")
        ));
    }
    let unexpected: Vec<&String> = graph
        .keys()
        .filter(|name| !layers.contains_key(name.as_str()))
        .collect();
    if !unexpected.is_empty() {
        failures.push(format!(
            "undeclared workspace crates are present: {unexpected:?}"
        ));
    }
    for (name, dependencies) in graph {
        let Some(allowed) = layers.get(name.as_str()) else {
            continue;
        };
        let forbidden: Vec<&String> = dependencies
            .iter()
            .filter(|dependency| {
                layers.contains_key(dependency.as_str()) && !allowed.contains(&dependency.as_str())
            })
            .collect();
        if !forbidden.is_empty() {
            failures.push(format!(
                "{name} has forbidden inward dependencies: {forbidden:?}"
            ));
        }
    }
    failures
}

/// Path to `(added, deleted)` from `git diff --numstat -z`; `None` for a
/// binary file.
fn numstat(raw: &[u8]) -> Result<BTreeMap<String, Option<(u64, u64)>>, ContractError> {
    let mut stats = BTreeMap::new();
    for record in names(raw) {
        let mut fields = record.splitn(3, '\t');
        let (Some(added), Some(deleted), Some(path)) =
            (fields.next(), fields.next(), fields.next())
        else {
            return Err(refused(format!("malformed numstat record {record:?}")));
        };
        let counts = if added == "-" || deleted == "-" {
            None
        } else {
            let parse = |count: &str| {
                count
                    .parse::<u64>()
                    .map_err(|cause| refused(format!("numstat count for {path}: {cause}")))
            };
            Some((parse(added)?, parse(deleted)?))
        };
        stats.insert(path.to_owned(), counts);
    }
    Ok(stats)
}

/// `part / whole` as a whole percentage; `None` when there is nothing to
/// measure, which no floor refuses.
fn percent(part: u64, whole: u64) -> Option<u64> {
    part.saturating_mul(100).checked_div(whole)
}

/// Churn against the baseline, in whole percent.
struct Churn {
    /// Changed baseline blobs over all baseline blobs.
    by_file: Option<u64>,
    /// Bytes of changed baseline blobs over all baseline bytes.
    by_bytes: Option<u64>,
    /// Deleted baseline lines over all baseline lines.
    by_lines: Option<u64>,
    /// Baseline paths whose blob is unchanged in the working tree.
    unchanged: Vec<String>,
}

/// Every baseline blob path and its byte size.
fn baseline_sizes(root: &Path) -> Result<BTreeMap<String, u64>, ContractError> {
    let mut sizes = BTreeMap::new();
    for record in names(&output(root, "git", &["ls-tree", "-rl", "-z", BASELINE])?) {
        let Some((meta, path)) = record.split_once('\t') else {
            return Err(refused(format!("malformed ls-tree record {record:?}")));
        };
        let fields: Vec<&str> = meta.split_whitespace().collect();
        if let &[_, "blob", _, size] = fields.as_slice() {
            let size = size
                .parse::<u64>()
                .map_err(|cause| refused(format!("blob size of {path}: {cause}")))?;
            sizes.insert(path.to_owned(), size);
        }
    }
    Ok(sizes)
}

/// Measures churn of the working tree under `root` against [`BASELINE`].
fn churn(root: &Path) -> Result<Churn, ContractError> {
    let sizes = baseline_sizes(root)?;
    let changed: BTreeSet<String> = names(&output(
        root,
        "git",
        &["diff", "--no-renames", "--name-only", "-z", BASELINE, "--"],
    )?)
    .collect();
    let unchanged: Vec<String> = sizes
        .keys()
        .filter(|path| !changed.contains(*path))
        .cloned()
        .collect();
    let total_bytes = sizes.values().copied().fold(0_u64, u64::saturating_add);
    let changed_bytes = sizes
        .iter()
        .filter(|&(path, _)| changed.contains(path))
        .map(|(_, size)| *size)
        .fold(0_u64, u64::saturating_add);
    let lines = numstat(&output(
        root,
        "git",
        &[
            "diff",
            "--numstat",
            "--no-renames",
            "-z",
            EMPTY_TREE,
            BASELINE,
            "--",
        ],
    )?)?;
    let deleted = numstat(&output(
        root,
        "git",
        &["diff", "--numstat", "--no-renames", "-z", BASELINE, "--"],
    )?)?;
    let (mut total_lines, mut deleted_lines) = (0_u64, 0_u64);
    for (path, counts) in &lines {
        let Some((baseline_lines, _)) = *counts else {
            continue;
        };
        total_lines = total_lines.saturating_add(baseline_lines);
        if let Some(Some((_, removed))) = deleted.get(path).copied() {
            deleted_lines = deleted_lines.saturating_add(removed.min(baseline_lines));
        }
    }
    let count = |len: usize| u64::try_from(len).map_err(|cause| refused(cause.to_string()));
    let blobs = count(sizes.len())?;
    let kept = count(unchanged.len())?;
    Ok(Churn {
        by_file: percent(blobs.saturating_sub(kept), blobs),
        by_bytes: percent(changed_bytes, total_bytes),
        by_lines: percent(deleted_lines, total_lines),
        unchanged,
    })
}

/// The pinned ledger's declared identifiers.
fn ledger_ids(root: &Path) -> Result<BTreeSet<String>, ContractError> {
    source_ids(&read(&root.join(LEDGER))?)
}

#[test]
fn every_file_cites_the_pinned_ledger() -> Result<(), ContractError> {
    let root = repository_root()?;
    let failures = repository_citations(&root, &ledger_ids(&root)?)?;
    assert!(failures.is_empty(), "citation failures: {failures:#?}");
    Ok(())
}

#[test]
fn no_crate_depends_inward_against_the_layer_table() -> Result<(), ContractError> {
    let failures = dependency_failures(&workspace_graph(&repository_root()?)?);
    assert!(failures.is_empty(), "dependency failures: {failures:#?}");
    Ok(())
}

#[test]
fn churn_against_the_baseline_exceeds_the_floor() -> Result<(), ContractError> {
    let measured = churn(&repository_root()?)?;
    assert!(
        measured.unchanged.is_empty(),
        "unchanged baseline blobs: {:?}",
        measured.unchanged
    );
    for (name, value) in [
        ("hash churn by file", measured.by_file),
        ("hash churn by baseline bytes", measured.by_bytes),
        ("line churn by baseline lines", measured.by_lines),
    ] {
        assert!(
            !matches!(value, Some(measured) if measured <= MINIMUM_CHURN),
            "{name} is {value:?}%, must exceed {MINIMUM_CHURN}%"
        );
    }
    Ok(())
}

#[test]
fn a_file_without_an_evidence_header_is_refused() -> Result<(), ContractError> {
    let allowed = ledger_ids(&repository_root()?)?;
    let failures = citation_failures("src/bare.rs", b"fn main() {}\n// S01 S02\n", &allowed)?;
    assert!(
        failures
            .iter()
            .any(|failure| failure.contains("missing explicit evidence header")),
        "a headerless file passed: {failures:?}"
    );
    assert!(
        failures
            .iter()
            .any(|failure| failure.contains("cites 2, requires 20")),
        "a two-source file passed: {failures:?}"
    );
    Ok(())
}

#[test]
fn an_unknown_source_and_a_binary_file_are_refused() -> Result<(), ContractError> {
    let allowed = ledger_ids(&repository_root()?)?;
    // Built at run time so this source file never cites an unpinned ID itself.
    let fixture = format!("Evidence: S{}{}\n", 9, 9);
    let unknown = citation_failures("docs/x.md", fixture.as_bytes(), &allowed)?;
    assert!(
        unknown
            .iter()
            .any(|failure| failure.contains("unknown source IDs")),
        "an unknown source passed: {unknown:?}"
    );
    let binary = citation_failures("assets/x.bin", b"Evidence\0", &allowed)?;
    assert!(
        binary.iter().any(|failure| failure.contains("binary file")),
        "a binary file passed: {binary:?}"
    );
    Ok(())
}

#[test]
fn a_ledger_with_a_duplicate_declaration_is_refused() -> Result<(), ContractError> {
    let ledger = read(&repository_root()?.join(LEDGER))?;
    let line = ledger
        .lines()
        .find(|line| line.starts_with("- **S01 — "))
        .ok_or_else(|| refused("the ledger declares S01".to_owned()))?;
    let Err(error) = source_ids(&format!("{ledger}\n{line}\n")) else {
        return Err(refused("a duplicate declaration was admitted".to_owned()));
    };
    assert!(
        error.to_string().contains("duplicate declarations"),
        "a duplicate is named in the refusal, got {error}"
    );
    Ok(())
}

#[test]
fn an_inward_dependency_is_refused() -> Result<(), ContractError> {
    let mut graph = workspace_graph(&repository_root()?)?;
    graph
        .entry("logical-db-model".to_owned())
        .or_insert_with(BTreeSet::new)
        .insert("logical-db-store".to_owned());
    let failures = dependency_failures(&graph);
    assert!(
        failures
            .iter()
            .any(|failure| failure.contains("logical-db-model has forbidden inward dependencies")),
        "an inward edge passed: {failures:?}"
    );
    Ok(())
}
