//! Argument parsing and typed failure shared by the subprocess fixtures
//! (`crash_writer`, `kill_writer`).
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use std::{fmt, io};

use logical_db_model::{AccountId, ReplicaId, Scope, SpaceId};

use crate::support::parse_identifier;

/// Why a subprocess fixture failed. A failed fixture returns this from its
/// `main`, so the process exits nonzero and reports the variant.
#[derive(Debug)]
#[non_exhaustive]
pub enum FixtureError {
    /// The command line did not carry exactly five arguments; the count given.
    Arguments(usize),
    /// An identifier argument was not a canonical identifier.
    Identifier(io::Error),
    /// The replica refused to open or to commit an edit.
    Database(logical_db::DatabaseError),
    /// The ready file could not be written.
    Ready(io::Error),
    /// A committed edit is not in the outbox.
    NotCommitted,
}

impl fmt::Display for FixtureError {
    /// Names the failing step and its cause.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::Arguments(count) => {
                write!(formatter, "expected five fixture arguments, got {count}")
            }
            Self::Identifier(ref error) => write!(formatter, "invalid identifier: {error}"),
            Self::Database(ref error) => write!(formatter, "replica refused: {error}"),
            Self::Ready(ref error) => write!(formatter, "ready file failed: {error}"),
            Self::NotCommitted => formatter.write_str("the edit is not in the outbox"),
        }
    }
}

impl std::error::Error for FixtureError {}

impl From<logical_db::DatabaseError> for FixtureError {
    /// Adapts a replica refusal without discarding it.
    fn from(error: logical_db::DatabaseError) -> Self {
        Self::Database(error)
    }
}

/// The replica a subprocess fixture opens, parsed from its command line:
/// `path account space replica last`.
#[derive(Debug)]
#[non_exhaustive]
pub struct FixtureReplica {
    /// Replica file the fixture opens.
    path: String,
    /// Scope the replica is bound to.
    scope: Scope,
    /// Replica identity.
    replica: ReplicaId,
    /// The fixture-specific fifth argument: a document id or a ready-file path.
    last: String,
}

impl FixtureReplica {
    /// Parses the five fixture arguments.
    ///
    /// # Errors
    ///
    /// Returns [`FixtureError::Arguments`] for any other count, and
    /// [`FixtureError::Identifier`] naming the argument that is not an identifier.
    pub fn from_arguments(arguments: impl Iterator<Item = String>) -> Result<Self, FixtureError> {
        let [path, account, space, replica, last]: [String; 5] = arguments
            .collect::<Vec<_>>()
            .try_into()
            .map_err(|given: Vec<String>| FixtureError::Arguments(given.len()))?;
        Ok(Self {
            path,
            scope: Scope::new(
                parse_identifier::<AccountId>(&account, "account")
                    .map_err(FixtureError::Identifier)?,
                parse_identifier::<SpaceId>(&space, "space").map_err(FixtureError::Identifier)?,
            ),
            replica: parse_identifier::<ReplicaId>(&replica, "replica")
                .map_err(FixtureError::Identifier)?,
            last,
        })
    }

    /// The replica file the fixture opens.
    #[must_use]
    pub fn path(&self) -> &str {
        &self.path
    }

    /// The scope the replica is bound to.
    #[must_use]
    pub const fn scope(&self) -> Scope {
        self.scope
    }

    /// The replica identity.
    #[must_use]
    pub const fn replica(&self) -> ReplicaId {
        self.replica
    }

    /// The fixture-specific fifth argument.
    #[must_use]
    pub fn last(&self) -> &str {
        &self.last
    }
}
