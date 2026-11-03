//! Embedded replica handle and its scope-bound accessors.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use logical_db_crdt::Document;
use logical_db_model::{IndexEntry, ReplicaId, Scope};
use logical_db_store::EmbeddedStore;

use super::error::DatabaseError;
use super::projection::Projection;
use super::schema::project_with;

/// Application-owned embedded replica handle.
#[non_exhaustive]
pub struct Database {
    /// Durable embedded store behind this handle.
    pub(crate) store: EmbeddedStore,
    /// Exact account and application space this handle is bound to.
    pub(crate) scope: Scope,
    /// Stable sender replica identity for local changes.
    pub(crate) replica: ReplicaId,
    /// Application-declared projections materialized by this handle.
    pub(crate) projections: Vec<Projection>,
}

impl Database {
    /// Returns the logical ownership scope fixed when this handle opened.
    #[must_use]
    pub const fn scope(&self) -> Scope {
        self.scope
    }

    /// Returns this durable installation's replica identity.
    #[must_use]
    pub const fn replica(&self) -> ReplicaId {
        self.replica
    }

    /// Project
    pub(crate) fn project(&self, state: &mut Document) -> Result<Vec<IndexEntry>, DatabaseError> {
        project_with(&self.projections, state)
    }
}

#[cfg(test)]
mod tests {
    use logical_db_model::{CapabilityProfile, common_guarantees};

    #[test]
    fn common_subset_between_embedded_and_distributed_is_empty()
    -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(
            common_guarantees(
                &CapabilityProfile::embedded_local(),
                &CapabilityProfile::distributed_transactional()
            ),
            Vec::new()
        );
        Ok(())
    }
}
