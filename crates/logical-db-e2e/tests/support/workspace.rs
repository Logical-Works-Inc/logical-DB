//! Workspace readers shared by the repository-contract tests.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

/// Why a repository contract was refused or could not be read.
#[derive(Debug)]
pub(crate) enum ContractError {
    /// A file could not be read or written.
    Io(PathBuf, std::io::Error),
    /// A contract document is not valid TOML (duplicate keys included).
    Toml(PathBuf, toml::de::Error),
    /// JSON from `cargo metadata` or a receipt did not parse.
    Json(lgwks_std::json::Error),
    /// A child process could not run or reported failure.
    Command(String),
    /// The contract parsed but a rule refused it.
    Refused(String),
    /// The e2e crate is not two levels below the workspace root.
    Layout,
}

impl std::fmt::Display for ContractError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            Self::Io(ref path, ref cause) => write!(formatter, "{}: {cause}", path.display()),
            Self::Toml(ref path, ref cause) => {
                write!(
                    formatter,
                    "invalid contract document {}: {cause}",
                    path.display()
                )
            }
            Self::Json(ref cause) => write!(formatter, "invalid JSON: {cause}"),
            Self::Command(ref reason) | Self::Refused(ref reason) => formatter.write_str(reason),
            Self::Layout => formatter.write_str("e2e crate is not below the workspace root"),
        }
    }
}

impl std::error::Error for ContractError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match *self {
            Self::Io(_, ref cause) => Some(cause),
            Self::Toml(_, ref cause) => Some(cause),
            Self::Json(ref cause) => Some(cause),
            Self::Command(_) | Self::Refused(_) | Self::Layout => None,
        }
    }
}

impl From<lgwks_std::json::Error> for ContractError {
    fn from(cause: lgwks_std::json::Error) -> Self {
        Self::Json(cause)
    }
}

/// The workspace root, two levels above the e2e manifest.
pub(crate) fn repository_root() -> Result<PathBuf, ContractError> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .ok_or(ContractError::Layout)
}

/// Reads `path` as UTF-8, naming it in the failure.
pub(crate) fn read(path: &Path) -> Result<String, ContractError> {
    std::fs::read_to_string(path).map_err(|cause| ContractError::Io(path.to_path_buf(), cause))
}

/// Reads and parses one TOML contract document.
pub(crate) fn read_toml(path: &Path) -> Result<toml::Table, ContractError> {
    toml::from_str(&read(path)?).map_err(|cause| ContractError::Toml(path.to_path_buf(), cause))
}

/// Runs `program` at `root` and returns its standard output.
pub(crate) fn output(
    root: &Path,
    program: &str,
    arguments: &[&str],
) -> Result<Vec<u8>, ContractError> {
    let run = std::process::Command::new(program)
        .args(arguments)
        .current_dir(root)
        .output()
        .map_err(|cause| ContractError::Io(PathBuf::from(program), cause))?;
    if run.status.success() {
        Ok(run.stdout)
    } else {
        Err(ContractError::Command(format!(
            "{program} {} failed: {}",
            arguments.join(" "),
            String::from_utf8_lossy(&run.stderr)
        )))
    }
}

/// Every workspace crate and the workspace crates it depends on, from the
/// real `cargo metadata` graph (normal, build and dev edges alike).
pub(crate) fn workspace_graph(
    root: &Path,
) -> Result<BTreeMap<String, BTreeSet<String>>, ContractError> {
    let raw = output(
        root,
        "cargo",
        &["metadata", "--locked", "--no-deps", "--format-version", "1"],
    )?;
    let metadata: lgwks_std::json::Value = lgwks_std::json::from_slice(&raw)?;
    let packages = metadata
        .get("packages")
        .and_then(lgwks_std::json::Value::as_array)
        .ok_or_else(|| ContractError::Refused("cargo metadata has no packages".to_owned()))?;
    let name_of = |package: &lgwks_std::json::Value| -> Result<String, ContractError> {
        package
            .get("name")
            .and_then(lgwks_std::json::Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| ContractError::Refused("a package without a name".to_owned()))
    };
    let members = packages
        .iter()
        .map(name_of)
        .collect::<Result<BTreeSet<_>, _>>()?;
    let mut graph = BTreeMap::new();
    for package in packages {
        let dependencies = package
            .get("dependencies")
            .and_then(lgwks_std::json::Value::as_array)
            .ok_or_else(|| ContractError::Refused("a package without dependencies".to_owned()))?;
        let internal = dependencies
            .iter()
            .filter_map(|dependency| {
                dependency
                    .get("name")
                    .and_then(lgwks_std::json::Value::as_str)
            })
            .filter(|name| members.contains(*name))
            .map(str::to_owned)
            .collect();
        graph.insert(name_of(package)?, internal);
    }
    Ok(graph)
}
