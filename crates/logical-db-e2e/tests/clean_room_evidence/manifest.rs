//! Domain manifests: the four product domains each declare one complete
//! manifest under `contract/domains/`.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use super::Failure;

/// The four product domains that must each declare one manifest.
const REQUIRED_DOMAINS: [&str; 4] = ["cairn", "cloud", "forge", "lem"];

/// A top-level value in a domain manifest.
#[derive(Debug, PartialEq, Eq)]
enum ManifestValue {
    /// A basic string without escapes.
    Text(String),
    /// A single-line array of such strings.
    List(Vec<String>),
}

/// Parses one escape-free basic string at the start of `text`, returning the
/// string and the remainder.
fn parse_string(text: &str) -> Result<(String, &str), String> {
    let body = text.strip_prefix('"').ok_or("expected a string")?;
    let close = body.find('"').ok_or("unterminated string")?;
    let (value, rest) = body.split_at(close);
    if value.contains('\\') {
        return Err(String::from("escapes are outside the manifest subset"));
    }
    let rest = rest
        .strip_prefix('"')
        .ok_or("string lost its closing quote")?;
    Ok((value.to_owned(), rest))
}

/// Parses the top-level key/value subset domain manifests are written in.
///
/// The subset is comments, `key = "string"`, and `key = ["a", "b"]` on one
/// line. Anything else, including tables and duplicate keys, is refused rather
/// than guessed at, so a manifest this parser accepts reads the same in TOML.
fn parse_manifest(text: &str) -> Result<BTreeMap<String, ManifestValue>, String> {
    let mut document = BTreeMap::new();
    for (index, raw) in text.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let number = index.saturating_add(1);
        let (key, value) = line
            .split_once('=')
            .ok_or_else(|| format!("line {number}: expected key = value"))?;
        let key = key.trim();
        if key.is_empty()
            || !key
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
        {
            return Err(format!("line {number}: unsupported key {key:?}"));
        }
        let value = value.trim();
        let parsed = if let Some(items) = value.strip_prefix('[') {
            let mut rest = items.trim_start();
            let mut list = Vec::new();
            while !rest.starts_with(']') {
                let (item, after) =
                    parse_string(rest).map_err(|error| format!("line {number}: {error}"))?;
                list.push(item);
                rest = after.trim_start();
                if let Some(after_comma) = rest.strip_prefix(',') {
                    rest = after_comma.trim_start();
                }
            }
            if rest != "]" {
                return Err(format!("line {number}: trailing content after array"));
            }
            ManifestValue::List(list)
        } else {
            let (string, rest) =
                parse_string(value).map_err(|error| format!("line {number}: {error}"))?;
            if !rest.trim().is_empty() {
                return Err(format!("line {number}: trailing content after string"));
            }
            ManifestValue::Text(string)
        };
        if document.insert(key.to_owned(), parsed).is_some() {
            return Err(format!("line {number}: duplicate key {key}"));
        }
    }
    Ok(document)
}

/// The string value of `key`, when it is a string.
fn text_field<'document>(
    document: &'document BTreeMap<String, ManifestValue>,
    key: &str,
) -> Option<&'document str> {
    match *document.get(key)? {
        ManifestValue::Text(ref text) => Some(text),
        ManifestValue::List(_) => None,
    }
}

/// Completeness failures across the four domain manifests.
pub(super) fn domain_manifest_failures(root: &Path) -> Result<Vec<String>, Failure> {
    let directory = root.join("contract").join("domains");
    if !directory.is_dir() {
        return Ok(vec![String::from("missing contract/domains directory")]);
    }
    let mut failures = Vec::new();
    let mut manifests = Vec::new();
    for entry in std::fs::read_dir(&directory)? {
        let path = entry?.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            failures.push(format!(
                "non-UTF-8 entry in contract/domains: {}",
                path.display()
            ));
            continue;
        };
        if name.ends_with(".toml") && name != "GRAPH.toml" {
            manifests.push((name.to_owned(), path.clone()));
        }
    }
    manifests.sort();
    let mut found = BTreeSet::new();
    for (name, path) in manifests {
        let document = match parse_manifest(&std::fs::read_to_string(&path)?) {
            Ok(document) => document,
            Err(error) => {
                failures.push(format!(
                    "invalid domain manifest contract/domains/{name}: {error}"
                ));
                continue;
            }
        };
        let Some(domain) = text_field(&document, "domain") else {
            failures.push(format!(
                "domain manifest lacks string domain: contract/domains/{name}"
            ));
            continue;
        };
        if !found.insert(domain.to_owned()) {
            failures.push(format!("duplicate domain manifest declaration: {domain}"));
        }
        for key in ["owner", "status", "release_lane"] {
            if !text_field(&document, key).is_some_and(|text| !text.is_empty()) {
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
            if !document
                .get(key)
                .is_some_and(|value| matches!(*value, ManifestValue::List(_)))
            {
                failures.push(format!("domain {domain} lacks string list {key}"));
            }
        }
    }
    let required: BTreeSet<String> = REQUIRED_DOMAINS
        .iter()
        .map(|&domain| domain.to_owned())
        .collect();
    let missing: Vec<&String> = required.difference(&found).collect();
    if !missing.is_empty() {
        failures.push(format!("missing domain manifests: {missing:?}"));
    }
    let unexpected: Vec<&String> = found.difference(&required).collect();
    if !unexpected.is_empty() {
        failures.push(format!("unexpected domain manifests: {unexpected:?}"));
    }
    Ok(failures)
}

/// The manifest parser refuses what it cannot read exactly.
#[test]
fn manifest_subset_refuses_ambiguity() {
    let parsed = parse_manifest("# c\ndomain = \"cloud\"\ncrates = [\"a\", \"b\"]\nempty = []\n");
    let expected = BTreeMap::from([
        (
            String::from("crates"),
            ManifestValue::List(vec![String::from("a"), String::from("b")]),
        ),
        (
            String::from("domain"),
            ManifestValue::Text(String::from("cloud")),
        ),
        (String::from("empty"), ManifestValue::List(Vec::new())),
    ]);
    assert_eq!(parsed, Ok(expected), "the subset round-trips");
    for refused in [
        "[table]\n",
        "a = \"x\"\na = \"y\"\n",
        "a = \"x\\\"\"\n",
        "a = [\"x\"\n",
        "a = 1\n",
        "a = \"x\" b\n",
    ] {
        assert!(
            parse_manifest(refused).is_err(),
            "{refused:?} must be refused"
        );
    }
}
