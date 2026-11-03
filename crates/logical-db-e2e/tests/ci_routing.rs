//! CI routing gate: every workflow job runs on one pinned GitHub-hosted image.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.
//!
//! The repository is public, so a workflow job routed to a self-hosted runner
//! would execute whatever a trigger supplies on the owner's machine; GitHub's
//! own hardening guide says never to pair self-hosted runners with public
//! repositories. Each job therefore names exactly one literal label from the
//! pinned hosted set. Routing that cannot be read literally is refused rather
//! than interpreted: unresolved `${{ }}` labels, label lists, mappings,
//! reusable-workflow jobs, YAML anchors and aliases, tabs, flow-style jobs,
//! odd indentation, and duplicate keys, any of which could hide a second route.

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

/// A gate failure carried to the test boundary.
type Failure = Box<dyn std::error::Error>;

/// The pinned GitHub-hosted images a job may name: native Linux x86-64,
/// macOS ARM64, and Windows x86-64. A moving `*-latest` alias is refused so
/// the image under a recorded run is the image the label names.
const HOSTED_IMAGES: [&str; 3] = ["ubuntu-24.04", "macos-15", "windows-2025"];

/// Check names the workflow publishes; a renamed or dropped lane fails here.
const CHECK_NAMES: [&str; 9] = [
    "advisory, license, and source policy",
    "clean-room evidence",
    "linux / end-to-end journeys",
    "linux / test-clippy",
    "macos / test-clippy",
    "official FDB client / daemon feature",
    "official FDB client / linux",
    "rustfmt",
    "windows / test-clippy",
];

/// The key and inline value of a `key: value` line, without indentation.
fn key_value(line: &str) -> Option<(&str, &str)> {
    let (key, value) = line.trim_start().split_once(':')?;
    let key = key.trim();
    let plain =
        !key.is_empty() && !key.starts_with('-') && !key.contains(' ') && !key.contains('"');
    plain.then(|| (key, value.trim()))
}

/// The plain scalar of `value` with a trailing ` # comment` and quotes removed.
fn scalar(value: &str) -> &str {
    let bare = match value.split_once(" #") {
        Some((bare, _comment)) => bare.trim_end(),
        None => value,
    };
    let quoted = bare
        .strip_prefix('"')
        .and_then(|inner| inner.strip_suffix('"'))
        .or_else(|| {
            bare.strip_prefix('\'')
                .and_then(|inner| inner.strip_suffix('\''))
        });
    match quoted {
        Some(inner) => inner.trim(),
        None => bare,
    }
}

/// One workflow job: its key and its job-level fields.
struct Job {
    /// The job's key under `jobs:`.
    key: String,
    /// Job-level `key: inline value` fields; a duplicate key is refused.
    fields: BTreeMap<String, String>,
}

/// Leading spaces on `line`.
fn indent(line: &str) -> usize {
    line.len()
        .saturating_sub(line.trim_start_matches(' ').len())
}

/// The jobs one workflow routes, refusing anything not literally hosted.
///
/// Returns the published job names (`name:` or the job key).
fn verify(text: &str) -> Result<Vec<String>, String> {
    if text.contains('\t') {
        return Err(String::from("tabs make YAML indentation ambiguous"));
    }
    let mut top_level = BTreeSet::new();
    let mut in_jobs = false;
    let mut jobs: Vec<Job> = Vec::new();
    let mut block_scalar_indent: Option<usize> = None;
    for (index, line) in text.lines().enumerate() {
        let number = index.saturating_add(1);
        let content = line.trim();
        let depth = indent(line);
        if content.is_empty() || content.starts_with('#') {
            continue;
        }
        if let Some(owner) = block_scalar_indent {
            if depth > owner {
                continue;
            }
            block_scalar_indent = None;
        }
        let parsed = key_value(line);
        // A sequence item (`- run: |`) opens block scalars and anchors too.
        let entry = match content.strip_prefix("- ") {
            Some(item) => key_value(item),
            None => parsed,
        };
        if let Some((_key, value)) = entry {
            let opener = scalar(value);
            if opener.starts_with('&') || opener.starts_with('*') {
                return Err(format!(
                    "line {number}: anchors and aliases can hide routing"
                ));
            }
            if matches!(opener.chars().next(), Some('|' | '>')) {
                block_scalar_indent = Some(depth);
            }
        }
        if content.starts_with("<<") || content.starts_with("- *") || content.starts_with("- &") {
            return Err(format!(
                "line {number}: merge keys and aliases can hide routing"
            ));
        }
        if depth == 0 {
            let (key, value) =
                parsed.ok_or_else(|| format!("line {number}: top-level line is not a key"))?;
            if !top_level.insert(key.to_owned()) {
                return Err(format!("line {number}: duplicate top-level key {key}"));
            }
            in_jobs = key == "jobs";
            if in_jobs && !value.is_empty() {
                return Err(format!("line {number}: jobs must be a block mapping"));
            }
            continue;
        }
        if !in_jobs {
            continue;
        }
        match depth {
            2 => {
                let (key, value) =
                    parsed.ok_or_else(|| format!("line {number}: job line is not a key"))?;
                if !value.is_empty() {
                    return Err(format!("line {number}: job {key} must be a block mapping"));
                }
                if jobs.iter().any(|job| job.key == key) {
                    return Err(format!("line {number}: duplicate job {key}"));
                }
                jobs.push(Job {
                    key: key.to_owned(),
                    fields: BTreeMap::new(),
                });
            }
            4 => {
                let (key, value) =
                    parsed.ok_or_else(|| format!("line {number}: job field is not a key"))?;
                let job = jobs
                    .last_mut()
                    .ok_or_else(|| format!("line {number}: field outside a job"))?;
                if job
                    .fields
                    .insert(key.to_owned(), value.to_owned())
                    .is_some()
                {
                    return Err(format!(
                        "line {number}: duplicate key {key} in job {}",
                        job.key
                    ));
                }
            }
            1 | 3 => return Err(format!("line {number}: odd indentation inside jobs")),
            _ => {}
        }
    }
    if jobs.is_empty() {
        return Err(String::from("workflow must declare jobs"));
    }
    let mut names = Vec::new();
    for job in &jobs {
        let key = &job.key;
        if job.fields.contains_key("uses") {
            return Err(format!("{key}: a reusable workflow hides its routing"));
        }
        let route = job
            .fields
            .get("runs-on")
            .map(|value| scalar(value))
            .ok_or_else(|| format!("{key}: explicit hosted runs-on required"))?;
        if !HOSTED_IMAGES.contains(&route) {
            return Err(format!(
                "{key}: runs-on {route:?} is not one pinned hosted image {HOSTED_IMAGES:?}"
            ));
        }
        let name = match job.fields.get("name") {
            Some(value) => scalar(value),
            None => key.as_str(),
        };
        if name.contains("${{") {
            return Err(format!("{key}: check name must be literal"));
        }
        names.push(name.to_owned());
    }
    Ok(names)
}

/// The one save rule every rust-cache step carries.
const MAIN_ONLY_SAVE: &str = "save-if: ${{ github.ref == 'refs/heads/main' }}";

/// The rust-cache steps in `text` and the 1-based lines of those that may
/// save from a ref other than main.
///
/// The repository cache holds 10 GiB and evicts least-recently-used entries;
/// a pull request that saves its own entries pushes main's out, and a run on
/// main cannot restore what a pull request saved, so every later run compiles
/// cold. A step's fields are the lines indented deeper than its `- ` marker.
fn rust_cache_steps(text: &str) -> (usize, Vec<usize>) {
    let lines: Vec<&str> = text.lines().collect();
    let mut steps = 0_usize;
    let mut unsaved = Vec::new();
    for (index, line) in lines.iter().enumerate() {
        let content = line.trim_start();
        let step = match content.strip_prefix("- ") {
            Some(item) => item,
            None => content,
        };
        if !step.starts_with("uses: Swatinem/rust-cache@") {
            continue;
        }
        steps = steps.saturating_add(1);
        let marker = indent(line);
        let saves_from_main = lines
            .iter()
            .skip(index.saturating_add(1))
            .take_while(|field| field.trim().is_empty() || indent(field) > marker)
            .any(|field| field.trim() == MAIN_ONLY_SAVE);
        if !saves_from_main {
            unsaved.push(index.saturating_add(1));
        }
    }
    (steps, unsaved)
}

/// The repository root, two directories above this crate's manifest.
fn repository_root() -> Result<PathBuf, Failure> {
    Ok(Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .ok_or("e2e crate lives two levels below the workspace root")?
        .to_path_buf())
}

/// Every committed workflow routes only to pinned hosted images and publishes
/// exactly the expected check names.
#[test]
fn committed_workflows_route_to_hosted_images() -> Result<(), Failure> {
    let directory = repository_root()?.join(".github").join("workflows");
    let mut workflows = Vec::new();
    for entry in std::fs::read_dir(&directory)? {
        let path = entry?.path();
        if matches!(
            path.extension().and_then(|extension| extension.to_str()),
            Some("yml" | "yaml")
        ) {
            workflows.push(path);
        }
    }
    assert!(
        !workflows.is_empty(),
        "no workflows found under {}",
        directory.display()
    );
    let mut names = BTreeSet::new();
    for path in &workflows {
        let text = std::fs::read_to_string(path)?;
        let published = verify(&text).map_err(|error| format!("{}: {error}", path.display()))?;
        names.extend(published);
    }
    let expected: BTreeSet<String> = CHECK_NAMES.iter().map(|&name| name.to_owned()).collect();
    assert_eq!(names, expected, "published check names changed");
    Ok(())
}

/// Every committed rust-cache step saves from main only, so pull requests
/// cannot evict main's entries from the repository cache.
#[test]
fn every_rust_cache_saves_from_main_only() -> Result<(), Failure> {
    let directory = repository_root()?.join(".github").join("workflows");
    let mut steps = 0_usize;
    let mut findings = Vec::new();
    for entry in std::fs::read_dir(&directory)? {
        let path = entry?.path();
        if !matches!(
            path.extension().and_then(|extension| extension.to_str()),
            Some("yml" | "yaml")
        ) {
            continue;
        }
        let (found, unsaved) = rust_cache_steps(&std::fs::read_to_string(&path)?);
        steps = steps.saturating_add(found);
        findings.extend(
            unsaved
                .into_iter()
                .map(|line| format!("{}:{line}", path.display())),
        );
    }
    assert!(
        steps > 0,
        "no rust-cache step found under {}",
        directory.display()
    );
    assert!(
        findings.is_empty(),
        "rust-cache steps without `{MAIN_ONLY_SAVE}`: {findings:?}"
    );
    Ok(())
}

/// A step without the rule, with another ref's rule, or with the rule in a
/// later step is refused; the rule inside the step's own fields passes.
#[test]
fn rust_cache_save_rule_belongs_to_its_own_step() {
    let saved = "    steps:\n      - uses: Swatinem/rust-cache@abc\n        with:\n          save-if: ${{ github.ref == 'refs/heads/main' }}\n      - run: cargo test\n";
    assert_eq!(
        rust_cache_steps(saved),
        (1, Vec::new()),
        "main-only save passes"
    );
    let bare = "    steps:\n      - uses: Swatinem/rust-cache@abc\n      - run: cargo test\n";
    assert_eq!(rust_cache_steps(bare), (1, vec![2]), "no rule is refused");
    let other_ref = "    steps:\n      - uses: Swatinem/rust-cache@abc\n        with:\n          save-if: ${{ github.ref == 'refs/heads/dev' }}\n";
    assert_eq!(
        rust_cache_steps(other_ref),
        (1, vec![2]),
        "another ref is refused"
    );
    let later_step = "    steps:\n      - uses: Swatinem/rust-cache@abc\n      - uses: other/action@abc\n        with:\n          save-if: ${{ github.ref == 'refs/heads/main' }}\n";
    assert_eq!(
        rust_cache_steps(later_step),
        (1, vec![2]),
        "a later step's rule is not this step's"
    );
}

/// A pinned hosted label passes; the job key stands in for a missing name.
#[test]
fn literal_hosted_image_passes() {
    assert_eq!(
        verify("jobs:\n  test:\n    runs-on: ubuntu-24.04 # pinned\n"),
        Ok(vec![String::from("test")]),
        "one literal pinned image is the accepted route"
    );
}

/// Self-hosted, moving, unresolved, list, mapping, and empty routes fail.
#[test]
fn self_hosted_and_unresolved_routes_fail() {
    let routes = [
        "self-hosted",
        "[self-hosted, macOS, ARM64]",
        "ubuntu-latest",
        "macos-latest",
        "windows-latest",
        "[ubuntu-24.04]",
        "\"${{ matrix.os }}\"",
        "[]",
        "null",
        "{group: hosted}",
        "",
    ];
    for route in routes {
        let workflow = format!("jobs:\n  test:\n    runs-on: {route}\n");
        assert!(verify(&workflow).is_err(), "{route:?} must be refused");
    }
}

/// Structures that could hide a second route are refused outright.
#[test]
fn ambiguous_structure_fails() {
    let refused = [
        "jobs:\n  good:\n    runs-on: ubuntu-24.04\n  bad:\n    runs-on: self-hosted\n",
        "jobs:\n  test:\n    runs-on: self-hosted\n    runs-on: ubuntu-24.04\n",
        "jobs:\n  test:\n    runs-on: ubuntu-24.04\n  test:\n    runs-on: ubuntu-24.04\n",
        "jobs:\n  test:\n    uses: external/repo/.github/workflows/ci.yml@main\n",
        "jobs:\n  test: {runs-on: ubuntu-24.04}\n",
        "jobs: {test: {runs-on: ubuntu-24.04}}\n",
        "jobs:\n  test:\n    runs-on: &route ubuntu-24.04\n",
        "jobs:\n  test:\n    runs-on: *route\n",
        "jobs:\n  test:\n    <<: *defaults\n    runs-on: ubuntu-24.04\n",
        "jobs:\n   test:\n    runs-on: ubuntu-24.04\n",
        "jobs:\n  test:\n\truns-on: ubuntu-24.04\n",
        "jobs:\n  test:\n    runs-on: ubuntu-24.04\njobs:\n  other:\n    runs-on: self-hosted\n",
        "jobs:\n  test:\n    runs-on: ubuntu-24.04\n    name: ${{ matrix.name }}\n",
    ];
    for workflow in refused {
        assert!(verify(workflow).is_err(), "{workflow:?} must be refused");
    }
}

/// Block scalars may hold arbitrary text without being read as routing.
#[test]
fn block_scalar_content_is_not_routing() {
    let workflow = "jobs:\n  test:\n    runs-on: macos-15\n    steps:\n      - run: |\n          echo runs-on: self-hosted\n";
    assert_eq!(
        verify(workflow),
        Ok(vec![String::from("test")]),
        "script text is not a route"
    );
}

#[path = "ci_routing/sim_routing.rs"]
mod sim_routing;

/// A workflow with no jobs routes nothing and is refused.
#[test]
fn missing_jobs_fail() {
    for document in ["", "name: ci\n", "jobs:\n", "on: push\n"] {
        assert!(verify(document).is_err(), "{document:?} must be refused");
    }
}
