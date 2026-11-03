//! Application-declared projection identities.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use std::num::NonZeroU32;
use std::sync::Arc;

use lgwks_std::hash::Hasher;
use logical_db_crdt::{CrdtError, Editor, Reader};
use logical_db_model::{IndexId, IndexValue};

/// Application-declared semantic identity of one materialized projection.
///
/// The durable schema digest binds every [`IndexId`] to this identity, so two
/// projection functions that share an index identity but compute different rows
/// cannot share one schema binding. The application states the identity because
/// projection meaning is not derivable from Rust closure machine code: bump the
/// version, or change the digest, whenever the function's output for any
/// document can change. Changing only the function while leaving the declared
/// semantics fixed is a caller defect this type cannot detect; it is what makes
/// that obligation explicit and binds the declared identity, so a stale
/// materialized index set is refused with [`StoreError::SchemaMigrationRequired`]
/// until an explicit [`crate::Database::migrate_schema`] rebuilds it.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum ProjectionSemantics {
    /// Monotonic, nonzero application-declared projection version.
    Version(NonZeroU32),
    /// Opaque application-computed semantic fingerprint.
    Digest([u8; 32]),
}

impl ProjectionSemantics {
    /// The first semantic version, for a projection whose logic never changed.
    pub const V1: Self = Self::Version(NonZeroU32::MIN);

    /// Declares a semantic version; zero is not a version.
    ///
    /// Test-only construction: no shipped path builds a version from a raw
    /// integer, so it stays out of the library surface until an external
    /// caller needs it.
    #[must_use]
    #[cfg(test)]
    pub(crate) const fn version(version: u32) -> Option<Self> {
        match NonZeroU32::new(version) {
            Some(version) => Some(Self::Version(version)),
            None => None,
        }
    }

    #[must_use]
    pub const fn digest(digest: [u8; 32]) -> Self {
        Self::Digest(digest)
    }

    /// Writes the canonical identity bytes into a schema digest.
    pub(crate) fn encode_into(&self, digest: &mut Hasher) {
        match *self {
            Self::Version(version) => {
                digest.update(b"version-v1");
                digest.update(&version.get().to_be_bytes());
            }
            Self::Digest(fingerprint) => {
                digest.update(b"digest-v1");
                digest.update(&fingerprint);
            }
        }
    }
}

/// ProjectionFunction
pub(crate) type ProjectionFunction = dyn for<'document> Fn(&mut Reader<'document>) -> Result<Option<IndexValue>, CrdtError>
    + Send
    + Sync;
/// BatchEditFunction
pub(crate) type BatchEditFunction =
    dyn for<'document> FnOnce(&mut Editor<'document>) -> Result<(), CrdtError> + Send;

/// One application-owned materialized secondary-index projection.
#[derive(Clone)]
#[non_exhaustive]
pub struct Projection {
    /// Application-chosen index identity.
    pub(crate) index: IndexId,
    /// Declared semantic identity bound into the durable schema digest.
    pub(crate) semantics: ProjectionSemantics,
    /// Deterministic projection function computing one row per document.
    pub(crate) function: Arc<ProjectionFunction>,
}

impl Projection {
    /// Creates a projection with an explicit application-declared semantics.
    ///
    /// Projection code must be deterministic for the same materialized
    /// document. The supplied [`ProjectionSemantics`] is bound into the durable
    /// schema digest; changing the projection's meaning requires changed
    /// semantics and a deliberate [`crate::Database::migrate_schema`].
    pub fn new(
        index: IndexId,
        semantics: ProjectionSemantics,
        function: impl for<'document> Fn(
            &mut Reader<'document>,
        ) -> Result<Option<IndexValue>, CrdtError>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        Self {
            index,
            semantics,
            function: Arc::new(function),
        }
    }

    /// Returns the stable index identity.
    #[must_use]
    pub const fn index(&self) -> IndexId {
        self.index
    }

    /// Returns the application-declared semantic identity.
    #[must_use]
    pub const fn semantics(&self) -> ProjectionSemantics {
        self.semantics
    }
}
