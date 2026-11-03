//! CI routing: every workflow job names a literal self-hosted runner.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.
//!
//! A job routed to a hosted image, to an expression the workflow resolves at
//! run time, or to a reusable workflow elsewhere would run this repository's
//! code on a machine nobody here owns. The reader refuses duplicate mapping
//! keys, so a second `runs-on:` cannot silently overwrite the first one, and it
//! refuses aliases, so a route cannot be hidden behind an anchor.

use std::collections::BTreeSet;

use yaml_rust2::parser::{Event, Parser};
use yaml_rust2::scanner::TScalarStyle;

use crate::workspace::{ContractError, read, repository_root};

/// One YAML node, as much of the data model as routing needs.
#[derive(Debug)]
enum Node {
    /// A scalar; `plain` is false when it was quoted or a block scalar.
    Scalar {
        /// The scalar text.
        value: String,
        /// Whether the scalar was written unquoted.
        plain: bool,
    },
    /// A sequence of nodes.
    Sequence(Vec<Node>),
    /// A mapping whose keys are unique scalars, in document order.
    Mapping(Vec<(String, Node)>),
}

impl Node {
    /// The value under `key` when this is a mapping.
    fn get(&self, key: &str) -> Option<&Node> {
        match *self {
            Self::Mapping(ref entries) => entries
                .iter()
                .find(|entry| entry.0 == key)
                .map(|entry| &entry.1),
            Self::Scalar { .. } | Self::Sequence(_) => None,
        }
    }

    /// The entries when this is a mapping.
    fn mapping(&self) -> Option<&[(String, Node)]> {
        match *self {
            Self::Mapping(ref entries) => Some(entries),
            Self::Scalar { .. } | Self::Sequence(_) => None,
        }
    }

    /// The items when this is a sequence.
    fn sequence(&self) -> Option<&[Node]> {
        match *self {
            Self::Sequence(ref items) => Some(items),
            Self::Scalar { .. } | Self::Mapping(_) => None,
        }
    }

    /// The text of a YAML string: a quoted scalar, or a plain one that YAML
    /// does not resolve to null, a boolean or a number.
    fn string(&self) -> Option<&str> {
        match *self {
            Self::Scalar {
                ref value,
                plain: false,
            } => Some(value),
            Self::Scalar {
                ref value,
                plain: true,
            } => {
                let resolved = matches!(
                    value.as_str(),
                    "" | "~"
                        | "null"
                        | "Null"
                        | "NULL"
                        | "true"
                        | "True"
                        | "TRUE"
                        | "false"
                        | "False"
                        | "FALSE"
                ) || value.parse::<f64>().is_ok();
                (!resolved).then_some(value.as_str())
            }
            Self::Sequence(_) | Self::Mapping(_) => None,
        }
    }
}

/// A refusal with a formatted reason.
fn refused(reason: String) -> ContractError {
    ContractError::Refused(reason)
}

/// The next parser event.
fn next(parser: &mut Parser<std::str::Chars<'_>>) -> Result<Event, ContractError> {
    parser
        .next_token()
        .map(|(event, _)| event)
        .map_err(|cause| refused(format!("invalid YAML: {cause}")))
}

/// Builds the node that `first` opens.
fn node(parser: &mut Parser<std::str::Chars<'_>>, first: Event) -> Result<Node, ContractError> {
    match first {
        Event::Scalar(value, style, _, _) => Ok(Node::Scalar {
            value,
            plain: style == TScalarStyle::Plain,
        }),
        Event::SequenceStart(..) => {
            let mut items = Vec::new();
            loop {
                match next(parser)? {
                    Event::SequenceEnd => return Ok(Node::Sequence(items)),
                    event => items.push(node(parser, event)?),
                }
            }
        }
        Event::MappingStart(..) => {
            let mut entries: Vec<(String, Node)> = Vec::new();
            loop {
                let key = match next(parser)? {
                    Event::MappingEnd => return Ok(Node::Mapping(entries)),
                    Event::Scalar(key, ..) => key,
                    other => return Err(refused(format!("non-scalar mapping key {other:?}"))),
                };
                if entries.iter().any(|entry| entry.0 == key) {
                    return Err(refused(format!("duplicate YAML key: {key}")));
                }
                let first = next(parser)?;
                entries.push((key, node(parser, first)?));
            }
        }
        Event::Alias(_) => Err(refused("YAML aliases are refused".to_owned())),
        other => Err(refused(format!("unexpected YAML event {other:?}"))),
    }
}

/// Parses one YAML document; an empty stream is a null scalar.
fn document(text: &str) -> Result<Node, ContractError> {
    let mut parser = Parser::new_from_str(text);
    if next(&mut parser)? != Event::StreamStart {
        return Err(refused("YAML stream did not start".to_owned()));
    }
    match next(&mut parser)? {
        Event::StreamEnd => Ok(Node::Scalar {
            value: String::new(),
            plain: true,
        }),
        Event::DocumentStart => {
            let first = next(&mut parser)?;
            let root = node(&mut parser, first)?;
            match (next(&mut parser)?, next(&mut parser)?) {
                (Event::DocumentEnd, Event::StreamEnd) => Ok(root),
                _ => Err(refused(
                    "a workflow file holds exactly one document".to_owned(),
                )),
            }
        }
        other => Err(refused(format!("unexpected YAML event {other:?}"))),
    }
}

/// The number of jobs when every one routes to a literal self-hosted list.
pub(crate) fn verify(text: &str) -> Result<usize, ContractError> {
    let workflow = document(text)?;
    if workflow.mapping().is_none() {
        return Err(refused("workflow must be a mapping".to_owned()));
    }
    let jobs = workflow
        .get("jobs")
        .and_then(Node::mapping)
        .filter(|jobs| !jobs.is_empty())
        .ok_or_else(|| refused("workflow must declare jobs".to_owned()))?;
    for entry in jobs {
        let (name, job) = (&entry.0, &entry.1);
        if job.mapping().is_none() {
            return Err(refused(format!("{name}: job must be a mapping")));
        }
        let labels = job
            .get("runs-on")
            .and_then(Node::sequence)
            .filter(|labels| !labels.is_empty())
            .ok_or_else(|| refused(format!("{name}: explicit self-hosted label list required")))?;
        let literal: Option<Vec<&str>> = labels
            .iter()
            .map(|label| label.string().filter(|text| !text.contains("${{")))
            .collect();
        let Some(literal) = literal else {
            return Err(refused(format!(
                "{name}: runner labels must be literal strings"
            )));
        };
        if !literal.contains(&"self-hosted") {
            return Err(refused(format!("{name}: missing self-hosted label")));
        }
    }
    Ok(jobs.len())
}

/// Asserts `text` is refused.
fn assert_refused(text: &str) {
    assert!(
        verify(text).is_err(),
        "a hosted or unresolved route passed: {text:?}"
    );
}

#[test]
fn a_literal_self_hosted_route_passes() -> Result<(), ContractError> {
    let jobs = verify("jobs: {test: {runs-on: [self-hosted, macOS, ARM64]}}")?;
    assert_eq!(jobs, 1, "one literal self-hosted job is counted");
    Ok(())
}

#[test]
fn hosted_and_unresolved_routes_are_refused() {
    for route in [
        "ubuntu-latest",
        "windows-latest",
        "macos-latest",
        "[Linux, X64]",
        "\"${{ matrix.os }}\"",
        "[self-hosted, \"${{ matrix.os }}\"]",
        "[self-hosted, null]",
        "[]",
        "null",
        "{group: unverified}",
    ] {
        assert_refused(&format!("jobs:\n  test:\n    runs-on: {route}\n"));
    }
}

#[test]
fn one_hosted_job_refuses_the_whole_workflow() {
    assert_refused("jobs: {good: {runs-on: [self-hosted]}, bad: {runs-on: ubuntu-latest}}");
}

#[test]
fn a_duplicate_route_is_not_silently_overwritten() {
    assert_refused("jobs: {test: {runs-on: ubuntu-latest, runs-on: [self-hosted]}}");
}

#[test]
fn an_aliased_route_is_refused() {
    assert_refused("labels: &hosted [ubuntu-latest]\njobs: {test: {runs-on: *hosted}}");
}

#[test]
fn a_reusable_workflow_cannot_hide_external_routing() {
    assert_refused("jobs: {test: {uses: external/repo/.github/workflows/ci.yml@main}}");
}

#[test]
fn a_workflow_without_jobs_is_refused() {
    for text in ["", "[]", "{}", "jobs: {}", "jobs: {test: []}"] {
        assert_refused(text);
    }
}

/// The `.github/workflows` files of the repository.
fn workflows() -> Result<Vec<std::path::PathBuf>, ContractError> {
    let directory = repository_root()?.join(".github/workflows");
    let entries = std::fs::read_dir(&directory)
        .map_err(|cause| ContractError::Io(directory.clone(), cause))?;
    let mut found = Vec::new();
    for entry in entries {
        let path = entry
            .map_err(|cause| ContractError::Io(directory.clone(), cause))?
            .path();
        if path
            .extension()
            .is_some_and(|extension| extension == "yml" || extension == "yaml")
        {
            found.push(path);
        }
    }
    found.sort();
    Ok(found)
}

#[test]
fn every_committed_workflow_routes_to_self_hosted_runners() -> Result<(), ContractError> {
    let paths = workflows()?;
    assert!(!paths.is_empty(), "the repository carries workflows");
    for path in paths {
        let jobs = verify(&read(&path)?)?;
        assert!(jobs > 0, "{} declares jobs", path.display());
    }
    Ok(())
}

#[test]
fn the_required_check_names_remain_available() -> Result<(), ContractError> {
    let workflow = document(&read(&repository_root()?.join(".github/workflows/ci.yml"))?)?;
    let jobs = workflow
        .get("jobs")
        .and_then(Node::mapping)
        .ok_or_else(|| refused("ci.yml declares jobs".to_owned()))?;
    let mut names = BTreeSet::new();
    for entry in jobs {
        let job = &entry.1;
        let name = job
            .get("name")
            .and_then(Node::string)
            .ok_or_else(|| refused("every job is named".to_owned()))?;
        if name == "${{ matrix.check-name }} / test-clippy" {
            let rows = job
                .get("strategy")
                .and_then(|strategy| strategy.get("matrix"))
                .and_then(|matrix| matrix.get("include"))
                .and_then(Node::sequence)
                .ok_or_else(|| refused("the matrix job lists its rows".to_owned()))?;
            for row in rows {
                let check = row
                    .get("check-name")
                    .and_then(Node::string)
                    .ok_or_else(|| refused("every matrix row names its check".to_owned()))?;
                names.insert(format!("{check} / test-clippy"));
            }
        } else {
            names.insert(name.to_owned());
        }
    }
    let expected: BTreeSet<String> = [
        "clean-room evidence",
        "ubuntu-latest / test-clippy",
        "macos-latest / test-clippy",
        "windows-latest / test-clippy",
        "rustfmt",
        "advisory, license, and source policy",
        "official FDB client / daemon feature",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();
    assert_eq!(
        names, expected,
        "branch protection names every required check"
    );
    Ok(())
}
