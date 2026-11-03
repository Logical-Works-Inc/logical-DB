//! Clean-room evidence gate: the mechanical acceptance the rewrite must hold.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.
//!
//! Every repository file carries twenty distinct ledger citations in its first
//! 4 KiB; the ledger declares S01..S43 exactly once, each with one unique HTTPS
//! URL; the internal crate graph keeps its one-way edges; the four domain
//! manifests are complete (`manifest`); and at least 85% of the clean-room
//! baseline is replaced by file count, by bytes, and by lines (`churn`). The
//! domain allowed-edge graph and its committed receipt are enforced by the
//! `repository_contract` test (docs/adr/0006).
//!
//! The one exemption is `LICENSE`: the Apache License 2.0 is fixed legal text
//! and cannot carry a citation header without ceasing to be the license. It is
//! exempt only while its bytes are exactly the text published at
//! <https://www.apache.org/licenses/LICENSE-2.0.txt>, pinned by Git blob id.

#![forbid(unsafe_code)]

/// Baseline replacement by file, byte, and line.
#[path = "clean_room_evidence/churn.rs"]
mod churn;
/// Domain manifest completeness.
#[path = "clean_room_evidence/manifest.rs"]
mod manifest;

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

/// A gate failure carried to the test boundary.
type Failure = Box<dyn std::error::Error>;

/// Distinct valid citations every non-ledger file states in its header.
const MINIMUM_CITATIONS: usize = 20;
/// The ledger declares exactly `S01..=S43`.
const LAST_SOURCE: u8 = 43;
/// Files above this size are refused rather than inspected.
const MAX_INSPECTABLE_BYTES: u64 = 16_777_216;
/// Citations count only inside this leading byte window.
const CITATION_HEADER_BYTES: usize = 4_096;
/// The only path exempt from the citation rule.
const LICENSE_PATH: &str = "LICENSE";
/// Git blob id of the verbatim Apache License 2.0 text.
const LICENSE_BLOB: &str = "d645695673349e3947e8e5ae42332d0ac3164cd7";
/// The ledger is the one file whose citations are declarations, not a header.
const LEDGER_PATH: &str = "evidence/SOURCES.md";

/// Each workspace crate and the internal crates it may depend on.
const ALLOWED_INTERNAL_DEPENDENCIES: &[(&str, &[&str])] = &[
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

/// The repository root, two directories above this crate's manifest.
fn repository_root() -> Result<PathBuf, Failure> {
    Ok(Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .ok_or("e2e crate lives two levels below the workspace root")?
        .to_path_buf())
}

/// Runs `program` at `root`, feeds `input` on stdin, and returns exact stdout.
///
/// Stdin is written on a scoped thread while stdout drains, so a large batch
/// cannot deadlock on a full pipe in either direction.
fn run(program: &str, root: &Path, arguments: &[&str], input: &[u8]) -> Result<Vec<u8>, Failure> {
    let mut child = Command::new(program)
        .args(arguments)
        .current_dir(root)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let stdin = child.stdin.take().ok_or("child stdin was not piped")?;
    let (written, output) = std::thread::scope(|scope| {
        let writer = scope.spawn(move || {
            let mut stdin = stdin;
            stdin.write_all(input)
        });
        let output = child.wait_with_output();
        (writer.join(), output)
    });
    written.map_err(|_panic| "stdin writer panicked")??;
    let output = output?;
    if !output.status.success() {
        return Err(format!(
            "{program} {}: {}",
            arguments.join(" "),
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    Ok(output.stdout)
}

/// Runs Git at the repository root.
fn git(root: &Path, arguments: &[&str], input: &[u8]) -> Result<Vec<u8>, Failure> {
    run("git", root, arguments, input)
}

/// Splits NUL-terminated Git output into UTF-8 records.
fn records(output: &[u8]) -> Result<Vec<String>, Failure> {
    output
        .split(|byte| *byte == 0)
        .filter(|record| !record.is_empty())
        .map(|record| Ok(String::from_utf8(record.to_vec())?))
        .collect()
}

/// The source id a ledger declaration at the start of `text` declares.
///
/// The declaration form is `- **Snn — <title>**`; the title may not contain
/// `*` and must be nonempty.
fn declared_id(text: &str) -> Option<String> {
    let rest = text.strip_prefix("- **S")?;
    let digits = rest.get(..2)?;
    if !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let title = rest.get(2..)?.strip_prefix(" — ")?;
    let star = title.find('*')?;
    (star > 0 && title.get(star..)?.starts_with("**")).then(|| format!("S{digits}"))
}

/// Every nonempty `<https://...>` URL in `text`, in order.
fn https_urls(text: &str) -> Vec<&str> {
    const OPEN: &str = "<https://";
    let mut urls = Vec::new();
    let mut rest = text;
    while let Some(start) = rest.find(OPEN) {
        let (_before, candidate) = rest.split_at(start);
        let Some(body) = candidate.strip_prefix(OPEN) else {
            break;
        };
        let Some(end) = body.find('>') else { break };
        if end == 0 {
            // `<https://>` is not a URL; nothing inside "https://" can begin
            // another match, so scanning resumes after the prefix.
            rest = body;
            continue;
        }
        let (url, after) = candidate.split_at(OPEN.len().saturating_add(end).saturating_add(1));
        urls.push(url);
        rest = after;
    }
    urls
}

/// The exact, uniquely declared source ids, each with one unique HTTPS URL.
fn source_ids(ledger: &str) -> Result<BTreeSet<String>, Failure> {
    let mut declarations = Vec::new();
    let mut offset = 0_usize;
    for line in ledger.split_inclusive('\n') {
        if let Some(id) = ledger.get(offset..).and_then(declared_id) {
            declarations.push((offset, id));
        }
        offset = offset.saturating_add(line.len());
    }
    let declared: BTreeSet<String> = declarations
        .iter()
        .map(|declaration| declaration.1.clone())
        .collect();
    if declared.len() != declarations.len() {
        return Err("source ledger contains duplicate declarations".into());
    }
    let expected: BTreeSet<String> = (1..=LAST_SOURCE)
        .map(|number| format!("S{number:02}"))
        .collect();
    if declared != expected {
        let missing: Vec<&String> = expected.difference(&declared).collect();
        let extra: Vec<&String> = declared.difference(&expected).collect();
        return Err(format!(
            "source ledger declarations differ from the pinned set; missing={missing:?} extra={extra:?}"
        )
        .into());
    }
    let mut urls = BTreeSet::new();
    for (index, declaration) in declarations.iter().enumerate() {
        let (start, id) = (declaration.0, &declaration.1);
        let end = match declarations.get(index.saturating_add(1)) {
            Some(next) => next.0,
            None => ledger.len(),
        };
        let entry = ledger
            .get(start..end)
            .ok_or("declaration offsets left the ledger")?;
        let entry_urls = https_urls(entry);
        let [url] = *entry_urls.as_slice() else {
            return Err(format!("source {id} must declare exactly one HTTPS URL").into());
        };
        if !urls.insert(url) {
            return Err(format!("source ledger repeats URL {url}").into());
        }
    }
    Ok(declared)
}

/// Whether `character` is a regex word character.
fn is_word(character: char) -> bool {
    character.is_alphanumeric() || character == '_'
}

/// Every `\bS\d{2}\b` token in `text`.
fn source_tokens(text: &str) -> BTreeSet<String> {
    let characters: Vec<char> = text.chars().collect();
    let mut tokens = BTreeSet::new();
    for (index, window) in characters.windows(3).enumerate() {
        let [letter, tens, units] = *window else {
            continue;
        };
        let before = index
            .checked_sub(1)
            .and_then(|previous| characters.get(previous));
        let after = characters.get(index.saturating_add(3));
        if letter == 'S'
            && tens.is_ascii_digit()
            && units.is_ascii_digit()
            && !before.is_some_and(|&character| is_word(character))
            && !after.is_some_and(|&character| is_word(character))
        {
            tokens.insert(format!("S{tens}{units}"));
        }
    }
    tokens
}

/// The citation failures for one repository file.
fn citation_failures(
    root: &Path,
    relative: &str,
    allowed: &BTreeSet<String>,
) -> Result<Vec<String>, Failure> {
    let path = root.join(relative);
    let metadata = std::fs::symlink_metadata(&path)?;
    if metadata.file_type().is_symlink() {
        return Ok(vec![format!(
            "repository symlink cannot carry independent citations: {relative}"
        )]);
    }
    if !metadata.is_file() {
        return Ok(vec![format!("non-regular repository entry: {relative}")]);
    }
    if metadata.len() > MAX_INSPECTABLE_BYTES {
        return Ok(vec![format!(
            "file exceeds {MAX_INSPECTABLE_BYTES} byte verifier ceiling: {relative}"
        )]);
    }
    if relative == LICENSE_PATH {
        let blob = git(root, &["hash-object", "--", LICENSE_PATH], &[])?;
        return Ok(if String::from_utf8(blob)?.trim() == LICENSE_BLOB {
            Vec::new()
        } else {
            vec![format!(
                "{relative} is not the verbatim Apache License 2.0 text"
            )]
        });
    }
    let payload = std::fs::read(&path)?;
    if payload.contains(&0) {
        return Ok(vec![format!(
            "binary file has no inspectable citations: {relative}"
        )]);
    }
    let Ok(text) = std::str::from_utf8(&payload) else {
        return Ok(vec![format!(
            "non-UTF-8 file has no inspectable citations: {relative}"
        )]);
    };
    let mut failures = Vec::new();
    let found = source_tokens(text);
    let unknown: Vec<&String> = found.difference(allowed).collect();
    if !unknown.is_empty() {
        failures.push(format!("unknown source IDs in {relative}: {unknown:?}"));
    }
    let valid = if relative == LEDGER_PATH {
        found.intersection(allowed).count()
    } else {
        // The window is a byte bound, so a multibyte character can straddle
        // it; keep only the complete characters before the cut.
        let window = match payload.get(..CITATION_HEADER_BYTES) {
            Some(window) => window,
            None => payload.as_slice(),
        };
        let header = match std::str::from_utf8(window) {
            Ok(header) => header,
            Err(error) => text
                .get(..error.valid_up_to())
                .ok_or("header cut left the text")?,
        };
        if !header.contains("Evidence") {
            failures.push(format!("missing explicit evidence header: {relative}"));
        }
        source_tokens(header).intersection(allowed).count()
    };
    if valid < MINIMUM_CITATIONS {
        failures.push(format!(
            "{relative} cites {valid}, requires {MINIMUM_CITATIONS}"
        ));
    }
    Ok(failures)
}

/// Tracked and new files, excluding ignored build products.
fn repository_files(root: &Path) -> Result<Vec<String>, Failure> {
    let output = git(
        root,
        &[
            "ls-files",
            "--cached",
            "--others",
            "--exclude-standard",
            "-z",
        ],
        &[],
    )?;
    Ok(records(&output)?
        .into_iter()
        .filter(|relative| std::fs::symlink_metadata(root.join(relative)).is_ok())
        .collect())
}

/// Inward or undeclared dependencies among the workspace crates.
fn dependency_failures(root: &Path) -> Result<Vec<String>, Failure> {
    let cargo = std::env::var("CARGO")?;
    let output = run(
        &cargo,
        root,
        &["metadata", "--locked", "--no-deps", "--format-version", "1"],
        &[],
    )?;
    let metadata: lgwks_std::json::Value = lgwks_std::json::from_slice(&output)?;
    let allowed: BTreeMap<&str, BTreeSet<&str>> = ALLOWED_INTERNAL_DEPENDENCIES
        .iter()
        .map(|&(name, edges)| (name, edges.iter().copied().collect()))
        .collect();
    let packages = metadata["packages"]
        .as_array()
        .ok_or("cargo metadata lacks packages")?;
    let present: BTreeSet<&str> = packages
        .iter()
        .filter_map(|package| package["name"].as_str())
        .collect();
    let mut failures = Vec::new();
    let missing: Vec<&&str> = allowed
        .keys()
        .filter(|name| !present.contains(**name))
        .collect();
    if !missing.is_empty() {
        failures.push(format!("required rewrite crates are missing: {missing:?}"));
    }
    let unexpected: Vec<&&str> = present
        .iter()
        .filter(|name| !allowed.contains_key(**name))
        .collect();
    if !unexpected.is_empty() {
        failures.push(format!(
            "undeclared workspace crates are present: {unexpected:?}"
        ));
    }
    for package in packages {
        let Some(name) = package["name"].as_str() else {
            continue;
        };
        let Some(permitted) = allowed.get(name) else {
            continue;
        };
        let Some(dependencies) = package["dependencies"].as_array() else {
            failures.push(format!("{name} has no dependency list in cargo metadata"));
            continue;
        };
        let forbidden: BTreeSet<&str> = dependencies
            .iter()
            .filter_map(|dependency| dependency["name"].as_str())
            .filter(|dependency| {
                allowed.contains_key(dependency) && !permitted.contains(dependency)
            })
            .collect();
        if !forbidden.is_empty() {
            failures.push(format!(
                "{name} has forbidden inward dependencies: {forbidden:?}"
            ));
        }
    }
    Ok(failures)
}

/// Every clean-room evidence gate holds on the working tree.
#[test]
fn clean_room_evidence_gates_hold() -> Result<(), Failure> {
    let root = repository_root()?;
    let allowed = source_ids(&std::fs::read_to_string(root.join(LEDGER_PATH))?)?;
    let files = repository_files(&root)?;
    let mut failures = Vec::new();
    for relative in &files {
        failures.extend(citation_failures(&root, relative, &allowed)?);
    }
    failures.extend(dependency_failures(&root)?);
    failures.extend(manifest::domain_manifest_failures(&root)?);
    let churn = churn::measure(&root)?;
    failures.extend(
        churn
            .unchanged
            .iter()
            .map(|path| format!("unchanged: {path}")),
    );
    let receipt = churn.receipt(files.len(), failures.len());
    let mut stdout = std::io::stdout().lock();
    stdout.write_all(receipt.as_bytes())?;
    assert!(
        failures.is_empty(),
        "clean-room evidence refused:\n{}",
        failures.join("\n")
    );
    assert!(
        churn.holds(),
        "hash and line churn must reach {}% of the baseline:\n{receipt}",
        churn::Churn::minimum_percent()
    );
    Ok(())
}

/// Word boundaries decide what counts as a citation, exactly as `\bS\d{2}\b`.
#[test]
fn source_tokens_respect_word_boundaries() {
    let tokens = source_tokens("S01, (S02) xS03 S04x S5 S100 _S06 S07_ S08.");
    let expected: BTreeSet<String> = ["S01", "S02", "S08"]
        .iter()
        .map(|&token| token.to_owned())
        .collect();
    assert_eq!(
        tokens, expected,
        "only bounded two-digit tokens are citations"
    );
}

/// A declaration needs its em-dash title and closing emphasis.
#[test]
fn ledger_declarations_are_parsed_strictly() {
    assert_eq!(
        declared_id("- **S07 — Notifications:** text"),
        Some(String::from("S07")),
        "canonical form"
    );
    for refused in [
        "- **S7 — X**",
        "- **S07 - X**",
        "- **S07 — **",
        "- **S07 — X* text",
        "* **S07 — X**",
    ] {
        assert_eq!(
            declared_id(refused),
            None,
            "{refused:?} is not a declaration"
        );
    }
    assert_eq!(
        https_urls("<https://a> <https://> <https://b>"),
        ["<https://a>", "<https://b>"],
        "nonempty URLs only"
    );
}
