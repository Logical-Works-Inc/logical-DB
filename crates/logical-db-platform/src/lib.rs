//! Cross-platform placement policy for embedded replica files.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
//! S33 S34 S35 S36 S37 S38 S39. Source URLs are in `evidence/SOURCES.md`.

#![forbid(unsafe_code)]

use std::{env, ffi::OsString, path::PathBuf};

use thiserror::Error;

/// Upper bound on a stable application identifier before it is rejected.
const MAX_APPLICATION_ID_BYTES: usize = 128;

/// Returns a per-user default replica path without creating directories.
///
/// Callers that already own placement policy should continue passing an
/// explicit path to `Database::open`. This helper exists for desktop apps that
/// want the native per-user data root on Windows, macOS, or freedesktop hosts.
///
/// # Errors
///
/// Returns a typed error for an invalid stable application identifier or a
/// process environment with no usable per-user data root.
pub fn default_database_path(application_id: &str) -> Result<PathBuf, PlatformError> {
    database_path_in(platform_data_root()?, application_id)
}

/// Constructs a replica path below a caller-supplied root.
///
/// # Errors
///
/// Refuses identifiers that are empty, path-like, non-ASCII, or oversized.
pub fn database_path_in(
    data_root: impl Into<PathBuf>,
    application_id: &str,
) -> Result<PathBuf, PlatformError> {
    validate_application_id(application_id)?;
    Ok(data_root
        .into()
        .join("LogicalDB")
        .join(application_id)
        .join("replica-v3.redb"))
}

/// Resolves the native per-user data root for the compiled target.
///
/// Each supported target binds `root` exactly once, so an unsupported target is
/// a typed refusal rather than unreachable code. This replaces the previous
/// an `allow` on `unreachable_code`, which lowered a lint to reach the fallback.
fn platform_data_root() -> Result<PathBuf, PlatformError> {
    #[cfg(target_os = "windows")]
    let root = absolute_data_root(nonempty_env("LOCALAPPDATA")?);

    #[cfg(target_os = "macos")]
    let root = absolute_data_root(nonempty_env("HOME")?)
        .map(|home| home.join("Library").join("Application Support"));

    #[cfg(all(unix, not(target_os = "macos")))]
    let root = match nonempty_env("XDG_DATA_HOME") {
        Ok(explicit) => absolute_data_root(explicit),
        Err(_) => {
            absolute_data_root(nonempty_env("HOME")?).map(|home| home.join(".local").join("share"))
        }
    };

    #[cfg(not(any(target_os = "windows", target_os = "macos", unix)))]
    let root = Err(PlatformError::MissingDataRoot {
        context: "unsupported platform",
    });

    root
}

/// Refuses an environment-supplied root that is not absolute.
fn absolute_data_root(value: OsString) -> Result<PathBuf, PlatformError> {
    let root = PathBuf::from(value);
    if root.is_absolute() {
        Ok(root)
    } else {
        Err(PlatformError::RelativeDataRoot)
    }
}

/// Reads a required, non-empty environment variable.
fn nonempty_env(name: &'static str) -> Result<OsString, PlatformError> {
    env::var_os(name)
        .filter(|value| !value.is_empty())
        .ok_or(PlatformError::MissingDataRoot { context: name })
}

/// Refuses an application identifier that could escape or destabilize a path.
///
/// The refusal is carried entirely by [`PlatformError::InvalidApplicationId`];
/// the crate no longer writes the reason to stderr, because a library returning
/// a typed error must not also print it.
fn validate_application_id(value: &str) -> Result<(), PlatformError> {
    if value.is_empty()
        || value.len() > MAX_APPLICATION_ID_BYTES
        || value == "."
        || value == ".."
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
    {
        return Err(PlatformError::InvalidApplicationId);
    }
    Ok(())
}

/// Portable placement failures.
///
/// Marked `#[non_exhaustive]` so a new placement refusal is a compatible
/// addition rather than a breaking change to downstream match statements.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum PlatformError {
    /// Stable application identifier could escape or destabilize placement.
    #[error("application identifier must be bounded ASCII without path separators")]
    InvalidApplicationId,
    /// The process has no usable native per-user data root.
    #[error("native per-user data root is unavailable: {context}")]
    MissingDataRoot {
        /// Required environment variable or unsupported platform.
        context: &'static str,
    },
    /// A process environment variable would place state relative to the CWD.
    #[error("native per-user data root must be absolute")]
    RelativeDataRoot,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    #[test]
    fn explicit_windows_shaped_root_preserves_spaces_and_unicode() -> Result<(), Box<dyn Error>> {
        let root = PathBuf::from(r"C:\Users\Renée Example\AppData\Local");
        let path = database_path_in(&root, "com.example.notes")?;
        assert_eq!(
            path,
            root.join("LogicalDB")
                .join("com.example.notes")
                .join("replica-v3.redb")
        );
        Ok(())
    }

    #[test]
    fn application_identifier_cannot_become_a_path() -> Result<(), Box<dyn Error>> {
        for invalid in ["", ".", "..", "../escape", r"a\b", "snowman-☃"] {
            assert_eq!(
                database_path_in("root", invalid),
                Err(PlatformError::InvalidApplicationId),
                "{invalid:?} must not become a path"
            );
        }
        let maximum = "a".repeat(MAX_APPLICATION_ID_BYTES);
        assert!(
            database_path_in("root", &maximum).is_ok(),
            "an identifier exactly at the bound is accepted"
        );
        assert_eq!(
            database_path_in("root", &format!("{maximum}a")),
            Err(PlatformError::InvalidApplicationId),
            "an identifier one byte over the bound is refused"
        );
        Ok(())
    }

    #[test]
    fn native_root_and_default_path_are_nonempty_and_composed_exactly() -> Result<(), Box<dyn Error>>
    {
        let root = platform_data_root()?;
        assert!(!root.as_os_str().is_empty());
        assert_eq!(
            default_database_path("com.example.logicaldb")?,
            database_path_in(root, "com.example.logicaldb")?
        );
        #[cfg(any(target_os = "macos", all(unix, not(target_os = "macos"))))]
        assert!(!nonempty_env("HOME")?.is_empty());
        Ok(())
    }

    #[test]
    fn environment_derived_root_must_never_be_relative_to_process_cwd() {
        assert_eq!(
            absolute_data_root(OsString::from("relative/data")),
            Err(PlatformError::RelativeDataRoot)
        );
    }
}
