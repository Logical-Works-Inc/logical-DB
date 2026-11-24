//! Multi-tenant protocol service with authenticated opaque checkpoints.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
//! S33. Source URLs are in `evidence/SOURCES.md`.

#![forbid(unsafe_code)]

mod authority;
mod checkpoint;
mod service;
mod store;
#[cfg(test)]
mod tests;

pub use authority::{Authority, ExecutionGrant, LocalScopeAuthority};
pub use checkpoint::{CheckpointCodec, CheckpointError, tags_equal};
pub use service::{ProtocolService, ServiceBaseline, ServicePull};
pub use store::{
    AcceptResult, BaselinePageResult, BaselinePageSlice, ConsumedResult, PageResult, PruneReceipt,
    PruneRefusal, PruneResult, RetentionStore, SealResult, ServiceStore, StoreFailureClass,
    StoredPage, prune_decision,
};

use std::{collections::BTreeMap, num::TryFromIntError};

use logical_db_crdt::{CrdtError, Document, EncodedChange};
use logical_db_model::{ModelError, Scope, SnapshotArtifact, SnapshotDocument};
use logical_db_protocol::{ChangeEnvelope, ProtocolError};
use thiserror::Error;

/// Typed failures from state-bearing baseline materialization; each keeps its source.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SnapshotMaterializationError {
    /// The supplied base artifact failed model validation.
    #[error("snapshot base artifact is invalid")]
    InvalidBase(#[source] ModelError),
    /// A base document failed a native CRDT read.
    #[error("snapshot base document failed at {context}")]
    InvalidBaseDocument {
        /// Native read that rejected the base document.
        context: &'static str,
        /// Native CRDT failure.
        #[source]
        source: Box<CrdtError>,
    },
    /// A base tombstone marker disagrees with its native document state.
    #[error("snapshot base tombstone marker disagrees with its document")]
    TombstoneMismatch,
    /// The ordered change slice does not cover exactly the requested prefix.
    #[error("snapshot changes do not exactly cover the requested prefix")]
    Coverage,
    /// The change count does not fit a durable position.
    #[error("snapshot change count exceeds a durable position")]
    CountOverflow(#[source] TryFromIntError),
    /// A change belongs to another logical scope.
    #[error("snapshot change belongs to another scope")]
    ScopeMismatch,
    /// A change envelope failed protocol validation.
    #[error("snapshot change envelope is invalid")]
    InvalidEnvelope(#[source] ProtocolError),
    /// A change failed native decode, digest, dependency, or apply validation.
    #[error("snapshot change failed native validation at {context}")]
    InvalidChange {
        /// Native step that rejected the change.
        context: &'static str,
        /// Native CRDT failure.
        #[source]
        source: Box<CrdtError>,
    },
    /// A materialized document failed native encoding.
    #[error("snapshot document failed native encoding")]
    Encode(#[source] Box<CrdtError>),
    /// The materialized artifact exceeded one of its bounded model limits.
    #[error("snapshot materialization exceeds its bounded resource limits")]
    ResourceLimit(#[source] ModelError),
}

/// Materializes one deterministic, bounded state artifact from a validated log prefix.
///
/// `base` is an optional state-bearing artifact whose position precedes `changes`.
/// The caller owns sequence verification; this function enforces the exact count,
/// scope, native CRDT validation, tombstone consistency, and final model bounds.
///
/// # Errors
///
/// Returns [`SnapshotMaterializationError`] when the base, coverage, scope, native
/// changes, or bounded artifact is invalid.
pub fn materialize_snapshot(
    scope: Scope,
    through: u64,
    base: Option<&SnapshotArtifact>,
    changes: &[ChangeEnvelope],
) -> Result<SnapshotArtifact, SnapshotMaterializationError> {
    let base_position = match base {
        Some(artifact) => {
            artifact
                .validate(scope)
                .map_err(SnapshotMaterializationError::InvalidBase)?;
            artifact.baseline().position()
        }
        None => 0,
    };
    let expected = through
        .checked_sub(base_position)
        .ok_or(SnapshotMaterializationError::Coverage)?;
    let change_count =
        u64::try_from(changes.len()).map_err(SnapshotMaterializationError::CountOverflow)?;
    if expected != change_count {
        return Err(SnapshotMaterializationError::Coverage);
    }

    let base_read = |context| {
        move |source| SnapshotMaterializationError::InvalidBaseDocument {
            context,
            source: Box::new(source),
        }
    };
    let mut documents = BTreeMap::<logical_db_model::DocumentId, Document>::new();
    if let Some(artifact) = base {
        for state in artifact.documents() {
            let mut document =
                Document::load(state.snapshot()).map_err(base_read("document load"))?;
            let tombstone = document.is_deleted().map_err(base_read("tombstone read"))?;
            if tombstone != state.is_tombstone() {
                return Err(SnapshotMaterializationError::TombstoneMismatch);
            }
            documents.insert(state.document(), document);
        }
    }

    let native = |context| {
        move |source| SnapshotMaterializationError::InvalidChange {
            context,
            source: Box::new(source),
        }
    };
    for change in changes {
        if change.scope() != scope {
            return Err(SnapshotMaterializationError::ScopeMismatch);
        }
        change
            .validate()
            .map_err(SnapshotMaterializationError::InvalidEnvelope)?;
        let encoded = EncodedChange::new(change.digest(), change.bytes().to_vec());
        encoded.validate().map_err(native("decode"))?;
        documents
            .entry(change.document())
            .or_default()
            .apply(&[encoded])
            .map_err(native("apply"))?;
    }

    let states = documents
        .into_iter()
        .map(|(document, mut state)| {
            let tombstone = state.is_deleted().map_err(native("tombstone read"))?;
            let snapshot = state
                .save()
                .map_err(|source| SnapshotMaterializationError::Encode(Box::new(source)))?;
            Ok(SnapshotDocument::new(document, snapshot, tombstone))
        })
        .collect::<Result<Vec<_>, SnapshotMaterializationError>>()?;
    SnapshotArtifact::new(scope, through, states)
        .map_err(SnapshotMaterializationError::ResourceLimit)
}

/// Service boundary failures.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ServiceError<StoreError> {
    /// Protocol validation failed.
    #[error("service protocol rejected input: {0}")]
    Protocol(ProtocolError),
    /// Pull authority was denied.
    #[error("service authority denied the pull request")]
    Forbidden,
    /// Durable state changed after pull authority was justified.
    #[error("service authority became stale before the pull transaction")]
    StaleAuthority,
    /// Opaque checkpoint was malformed, forged, or cross-scoped.
    #[error("service checkpoint rejected input: {0}")]
    Checkpoint(CheckpointError),
    /// Durable adapter failed.
    #[error("service storage failed")]
    Store(StoreError),
    /// Durable adapter violated account/space isolation.
    #[error("service storage returned cross-scope material")]
    CrossScopeStoreData,
    /// Durable bytes failed CRDT-native decoding or digest verification.
    #[error("service storage returned corrupt change material")]
    CorruptStoreData,
    /// A stored change failed native CRDT decode or digest verification.
    #[error("service storage returned an undecodable change")]
    CorruptChange(#[source] CrdtError),
    /// A model invariant refused the operation.
    #[error("service model invariant refused: {0}")]
    Model(ModelError),
}
