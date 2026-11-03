//! Baseline churn: at least 85% of the clean-room baseline is replaced by file
//! count, by bytes, and by lines.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use super::{Failure, git, records};

/// The pre-rewrite commit every churn measure is taken against.
const BASELINE: &str = "7cffbc07b6f312854986ae306b583ef891623b2d";
/// Minimum replaced share of the baseline, in percent, for every measure.
const MINIMUM_CHURN_PERCENT: u128 = 85;

/// One baseline blob: path, byte size, and object id.
struct BaselineBlob {
    /// Repository-relative path at the baseline.
    path: String,
    /// Byte size of the baseline blob.
    size: u128,
    /// Git object id of the baseline blob.
    object: String,
}

/// Baseline replacement, measured three ways as `(changed, total)`.
pub(super) struct Churn {
    /// Baseline paths whose blob is byte-identical in the working tree.
    pub(super) unchanged: Vec<String>,
    /// Changed and total baseline files.
    files: (u128, u128),
    /// Changed and total baseline bytes.
    bytes: (u128, u128),
    /// Replaced and total baseline lines.
    lines: (u128, u128),
}

impl Churn {
    /// The receipt the gate prints, one `key=value` per line.
    pub(super) fn receipt(&self, current_files: usize, gate_failures: usize) -> String {
        format!(
            "baseline={BASELINE}\ncurrent_files={current_files}\nhash_churn_by_file={}\n\
             hash_churn_by_baseline_bytes={}\nline_churn_by_baseline_lines={}\ngate_failures={gate_failures}\n",
            percent(self.files.0, self.files.1),
            percent(self.bytes.0, self.bytes.1),
            percent(self.lines.0, self.lines.1),
        )
    }

    /// Whether every measure reaches the minimum share.
    pub(super) fn holds(&self) -> bool {
        meets_minimum(self.files.0, self.files.1)
            && meets_minimum(self.bytes.0, self.bytes.1)
            && meets_minimum(self.lines.0, self.lines.1)
    }

    /// The minimum share every measure must reach, for the refusal message.
    pub(super) const fn minimum_percent() -> u128 {
        MINIMUM_CHURN_PERCENT
    }
}

/// Measures the working tree against the baseline.
pub(super) fn measure(root: &Path) -> Result<Churn, Failure> {
    let blobs = baseline_blobs(root)?;
    let current = current_blobs(root, &blobs)?;
    let lines = baseline_line_counts(root, &blobs)?;
    let deleted = deleted_lines(root)?;
    let mut churn = Churn {
        unchanged: Vec::new(),
        files: (0, u128::try_from(blobs.len())?),
        bytes: (0, 0),
        lines: (0, 0),
    };
    for blob in &blobs {
        churn.bytes.1 = churn.bytes.1.saturating_add(blob.size);
        if current.get(&blob.path) == Some(&blob.object) {
            churn.unchanged.push(blob.path.clone());
        } else {
            churn.files.0 = churn.files.0.saturating_add(1);
            churn.bytes.0 = churn.bytes.0.saturating_add(blob.size);
        }
        if let Some(&count) = lines.get(&blob.object) {
            churn.lines.1 = churn.lines.1.saturating_add(count);
            if let Some(&removed) = deleted.get(&blob.path) {
                churn.lines.0 = churn.lines.0.saturating_add(count.min(removed));
            }
        }
    }
    Ok(churn)
}

/// Every blob in the baseline tree.
fn baseline_blobs(root: &Path) -> Result<Vec<BaselineBlob>, Failure> {
    let mut blobs = Vec::new();
    for record in records(&git(root, &["ls-tree", "-rl", "-z", BASELINE], &[])?)? {
        let (metadata, path) = record
            .split_once('\t')
            .ok_or("ls-tree record lacks a path")?;
        let fields: Vec<&str> = metadata.split_whitespace().collect();
        if let [_mode, "blob", object, size] = *fields.as_slice() {
            blobs.push(BaselineBlob {
                path: path.to_owned(),
                size: size.parse()?,
                object: object.to_owned(),
            });
        }
    }
    Ok(blobs)
}

/// The working-tree blob id of each baseline path that still exists.
fn current_blobs(root: &Path, blobs: &[BaselineBlob]) -> Result<BTreeMap<String, String>, Failure> {
    let mut current = BTreeMap::new();
    let mut files = Vec::new();
    for blob in blobs {
        let Ok(metadata) = std::fs::symlink_metadata(root.join(&blob.path)) else {
            continue;
        };
        if metadata.file_type().is_symlink() {
            let target = std::fs::read_link(root.join(&blob.path))?;
            let id = git(
                root,
                &["hash-object", "--stdin"],
                target.to_string_lossy().as_bytes(),
            )?;
            current.insert(blob.path.clone(), String::from_utf8(id)?.trim().to_owned());
        } else if metadata.is_file() {
            files.push(blob.path.as_str());
        }
    }
    if files.iter().any(|path| path.contains('\n')) {
        return Err("a baseline path contains a newline and cannot be batch-hashed".into());
    }
    let mut input = files.join("\n");
    input.push('\n');
    let ids = String::from_utf8(git(
        root,
        &["hash-object", "--stdin-paths"],
        input.as_bytes(),
    )?)?;
    let ids: Vec<&str> = ids.lines().collect();
    if ids.len() != files.len() {
        return Err("hash-object returned a different number of ids than paths".into());
    }
    for (path, id) in files.into_iter().zip(ids) {
        current.insert(path.to_owned(), id.to_owned());
    }
    Ok(current)
}

/// Line counts of every text blob among `blobs`, by object id.
fn baseline_line_counts(
    root: &Path,
    blobs: &[BaselineBlob],
) -> Result<BTreeMap<String, u128>, Failure> {
    let objects: BTreeSet<&str> = blobs.iter().map(|blob| blob.object.as_str()).collect();
    let mut input = objects.iter().copied().collect::<Vec<_>>().join("\n");
    input.push('\n');
    let output = git(root, &["cat-file", "--batch"], input.as_bytes())?;
    let mut counts = BTreeMap::new();
    let mut rest = output.as_slice();
    while !rest.is_empty() {
        let newline = rest
            .iter()
            .position(|byte| *byte == b'\n')
            .ok_or("cat-file header lacks a newline")?;
        let (header, after) = rest.split_at(newline);
        let header = std::str::from_utf8(header)?;
        let fields: Vec<&str> = header.split(' ').collect();
        let [object, _kind, size] = *fields.as_slice() else {
            return Err(format!("unexpected cat-file header {header:?}").into());
        };
        let size: usize = size.parse()?;
        let body = after
            .strip_prefix(b"\n")
            .ok_or("cat-file header lost its newline")?;
        let (content, trailer) = body
            .split_at_checked(size)
            .ok_or("cat-file body truncated")?;
        if !content.contains(&0) {
            let newlines = content.iter().filter(|byte| **byte == b'\n').count();
            let unterminated = !content.is_empty() && !content.ends_with(b"\n");
            let lines = u128::try_from(newlines)?.saturating_add(u128::from(unterminated));
            counts.insert(object.to_owned(), lines);
        }
        rest = trailer
            .strip_prefix(b"\n")
            .ok_or("cat-file record lacks its terminator")?;
    }
    Ok(counts)
}

/// Deleted-line counts per path between the baseline and the working tree.
fn deleted_lines(root: &Path) -> Result<BTreeMap<String, u128>, Failure> {
    let output = git(
        root,
        &["diff", "--numstat", "--no-renames", "-z", BASELINE, "--"],
        &[],
    )?;
    let mut deleted = BTreeMap::new();
    for record in records(&output)? {
        let mut fields = record.splitn(3, '\t');
        let (Some(added), Some(removed), Some(path)) =
            (fields.next(), fields.next(), fields.next())
        else {
            return Err(format!("unexpected numstat record {record:?}").into());
        };
        if added != "-" && removed != "-" {
            deleted.insert(path.to_owned(), removed.parse()?);
        }
    }
    Ok(deleted)
}

/// `changed` is at least the minimum share of `total`; an empty total passes.
fn meets_minimum(changed: u128, total: u128) -> bool {
    match (
        changed.checked_mul(100),
        total.checked_mul(MINIMUM_CHURN_PERCENT),
    ) {
        (Some(left), Some(right)) => total == 0 || left >= right,
        _ => false,
    }
}

/// `part` as a percentage of `whole` with two decimals, for the receipt.
fn percent(part: u128, whole: u128) -> String {
    // An empty baseline is fully replaced by definition.
    let basis = if whole == 0 {
        10_000
    } else {
        part.saturating_mul(10_000).div_euclid(whole)
    };
    format!("{}.{:02}%", basis.div_euclid(100), basis.rem_euclid(100))
}

/// The churn threshold is exact at its boundary and never overflows open.
#[test]
fn churn_threshold_is_exact() {
    assert!(meets_minimum(85, 100), "85% meets the minimum");
    assert!(!meets_minimum(84, 100), "84% does not");
    assert!(meets_minimum(0, 0), "an empty baseline passes");
    assert!(!meets_minimum(u128::MAX, 1), "overflow fails closed");
    assert_eq!(percent(1, 3), "33.33%", "two-decimal receipt");
}
