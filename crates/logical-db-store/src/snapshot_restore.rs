//! Snapshot restore-or-replay over either relational engine (#119 row 4).
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.
//!
//! This absorbs the kernel snapshot-restore integration (`tape-store::
//! snapshot_restore`, U3): restore a projection from its snapshot plus tail,
//! falling back to genesis replay on any refusal. The caller owns the tail:
//! `restore_or_replay` never fabricates a frontier or a clock.

use logical_db_model::{IndexId, Scope};

use super::projection_relational::{
    ProjectionError, RelationalProjection, SqliteRelationalProjection,
};
use super::snapshot_envelope::{
    SnapshotEnvelope, SnapshotEnvelopeError, SnapshotSeal, SnapshotStore,
};

/// A projection that restores from snapshot state: the minimum surface
/// [`restore_or_replay`] needs.
pub trait RestorableProjection: Sized {
    /// Computes the checkpoint of the current state.
    fn checkpoint(&self) -> [u8; 32];
    /// Serializes the current state.
    fn serialize_state(&self) -> Vec<u8>;
    /// Restores from [`Self::serialize_state`] bytes.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotEnvelopeError`] when the bytes are not well-formed.
    fn restore_from_state(bytes: &[u8]) -> Result<Self, SnapshotEnvelopeError>;
}

/// Maps a relational engine refusal into the envelope error.
fn map_relational(error: ProjectionError) -> SnapshotEnvelopeError {
    match error {
        ProjectionError::Malformed(message) => SnapshotEnvelopeError::Malformed(message),
        ProjectionError::Backend(message) => SnapshotEnvelopeError::BackendFailed {
            at: "restore relational state",
            reason: message,
        },
        ProjectionError::RowBudgetExceeded { limit } => {
            SnapshotEnvelopeError::Malformed(format!("restored rows exceed {limit}"))
        }
    }
}

impl RestorableProjection for RelationalProjection {
    fn checkpoint(&self) -> [u8; 32] {
        RelationalProjection::checkpoint(self)
    }

    fn serialize_state(&self) -> Vec<u8> {
        RelationalProjection::serialize_state(self)
    }

    fn restore_from_state(bytes: &[u8]) -> Result<Self, SnapshotEnvelopeError> {
        RelationalProjection::restore_from_state(bytes).map_err(map_relational)
    }
}

impl RestorableProjection for SqliteRelationalProjection {
    fn checkpoint(&self) -> [u8; 32] {
        SqliteRelationalProjection::checkpoint(self)
    }

    fn serialize_state(&self) -> Vec<u8> {
        SqliteRelationalProjection::serialize_state(self)
    }

    fn restore_from_state(bytes: &[u8]) -> Result<Self, SnapshotEnvelopeError> {
        SqliteRelationalProjection::restore_from_state(bytes).map_err(map_relational)
    }
}

/// Statistics about one restore.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct RestoreStats {
    /// True when the snapshot path was taken.
    pub from_snapshot: bool,
    /// Why genesis replay ran instead, when it did.
    pub fallback_reason: Option<String>,
    /// Facts replayed after the snapshot frontier, or all facts on genesis.
    pub tail_replayed: u64,
    /// Milliseconds spent restoring the snapshot state.
    pub snapshot_restore_ms: u128,
    /// Milliseconds spent on tail or genesis replay.
    pub tail_replay_ms: u128,
    /// Checkpoint of the final projection.
    pub checkpoint: [u8; 32],
}

/// The frontier and identity one snapshot is created under, as one value so
/// a half-specified creation cannot exist.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct SnapshotInput<'a, P> {
    /// The projected index identity.
    pub projection: IndexId,
    /// Opaque projection semantics fingerprint bound at seal.
    pub semantics: [u8; 32],
    /// The projection state to seal.
    pub state: &'a P,
    /// Last row hash covered by the snapshot.
    pub last_row: [u8; 32],
    /// Sequence number of the last covered row.
    pub last_seq: u64,
    /// Creation time in Unix seconds, caller-supplied.
    pub created_at_secs: u64,
}

impl<'a, P> SnapshotInput<'a, P> {
    /// Bundles one whole creation frontier so a half-specified input cannot exist.
    #[must_use]
    pub fn new(
        projection: IndexId,
        semantics: [u8; 32],
        state: &'a P,
        last_row: [u8; 32],
        last_seq: u64,
        created_at_secs: u64,
    ) -> Self {
        Self {
            projection,
            semantics,
            state,
            last_row,
            last_seq,
            created_at_secs,
        }
    }
}

/// Serializes the projection, seals an envelope at the caller-supplied
/// frontier, and persists it. The caller owns the frontier and the clock.
///
/// # Errors
///
/// Returns [`SnapshotEnvelopeError`] when serialization, sealing, or the
/// save fails.
pub fn create_snapshot<S, P>(
    store: &S,
    scope: Scope,
    input: SnapshotInput<'_, P>,
) -> Result<SnapshotEnvelope, SnapshotEnvelopeError>
where
    S: SnapshotStore + ?Sized,
    P: RestorableProjection,
{
    let envelope = SnapshotEnvelope::seal(SnapshotSeal {
        scope,
        projection: input.projection,
        semantics: input.semantics,
        checkpoint: input.state.checkpoint(),
        last_row: input.last_row,
        last_seq: input.last_seq,
        state: input.state.serialize_state(),
        created_at_secs: input.created_at_secs,
    })?;
    store.save_snapshot(&envelope)?;
    Ok(envelope)
}

/// Restores a projection from its snapshot plus tail, falling back to
/// genesis replay on any refusal. `tail_replay` replays rows after
/// `last_seq` into the projection and returns how many it applied.
///
/// # Errors
///
/// Returns [`SnapshotEnvelopeError`] only when genesis replay itself fails;
/// every snapshot or tail refusal falls back with its reason recorded.
pub fn restore_or_replay<S, P, F>(
    store: &S,
    scope: Scope,
    projection: IndexId,
    seed: P,
    mut tail_replay: F,
) -> Result<(P, RestoreStats), SnapshotEnvelopeError>
where
    S: SnapshotStore + ?Sized,
    P: RestorableProjection,
    F: FnMut(&mut P, u64) -> Result<u64, String>,
{
    let latest = store
        .load_latest_snapshot(scope, projection)
        .map_err(|error| SnapshotEnvelopeError::BackendFailed {
            at: "load latest snapshot",
            reason: error.to_string(),
        })?;
    let Some(snapshot) = latest else {
        return fallback_to_genesis(seed, scope, &mut tail_replay, None);
    };
    match restore_from_snapshot::<P>(&snapshot) {
        Ok((mut restored, snapshot_ms)) => {
            let tail_start = std::time::Instant::now();
            match tail_replay(&mut restored, snapshot.last_seq) {
                Ok(tail) => {
                    let stats = RestoreStats {
                        from_snapshot: true,
                        fallback_reason: None,
                        tail_replayed: tail,
                        snapshot_restore_ms: snapshot_ms,
                        tail_replay_ms: tail_start.elapsed().as_millis(),
                        checkpoint: restored.checkpoint(),
                    };
                    Ok((restored, stats))
                }
                Err(reason) => fallback_to_genesis(
                    seed,
                    scope,
                    &mut tail_replay,
                    Some(format!(
                        "tail replay after snapshot restore failed: {reason}"
                    )),
                ),
            }
        }
        Err(error) => fallback_to_genesis(
            seed,
            scope,
            &mut tail_replay,
            Some(format!("snapshot restore rejected: {error}")),
        ),
    }
}

/// Decodes and checkpoint-gates one envelope's state blob.
fn restore_from_snapshot<P: RestorableProjection>(
    snapshot: &SnapshotEnvelope,
) -> Result<(P, u128), SnapshotEnvelopeError> {
    let started = std::time::Instant::now();
    snapshot.check_version()?;
    let restored = P::restore_from_state(&snapshot.state)?;
    if restored.checkpoint() != snapshot.checkpoint {
        return Err(SnapshotEnvelopeError::Tampered {
            at: "restore_from_snapshot",
            reason: String::from("checkpoint mismatch after restore"),
        });
    }
    Ok((restored, started.elapsed().as_millis()))
}

/// Replays from genesis on the seed projection.
fn fallback_to_genesis<P, F>(
    mut seed: P,
    _scope: Scope,
    tail_replay: &mut F,
    fallback_reason: Option<String>,
) -> Result<(P, RestoreStats), SnapshotEnvelopeError>
where
    P: RestorableProjection,
    F: FnMut(&mut P, u64) -> Result<u64, String>,
{
    let started = std::time::Instant::now();
    let total =
        tail_replay(&mut seed, 0).map_err(|reason| SnapshotEnvelopeError::BackendFailed {
            at: "genesis replay",
            reason: format!("genesis replay failed: {reason}"),
        })?;
    let checkpoint = seed.checkpoint();
    Ok((
        seed,
        RestoreStats {
            from_snapshot: false,
            fallback_reason,
            tail_replayed: total,
            snapshot_restore_ms: 0,
            tail_replay_ms: started.elapsed().as_millis(),
            checkpoint,
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::super::projection_relational::ProjectionAction;
    use super::super::snapshot_backend::RedbSnapshotStore;
    use super::*;
    use logical_db_model::{AccountId, SpaceId};
    use std::sync::atomic::{AtomicU64, Ordering};

    /// One unique temporary snapshot store path for a named fixture.
    fn store_path(name: &str) -> std::path::PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let sequence = COUNTER.fetch_add(1, Ordering::SeqCst);
        std::env::temp_dir().join(format!(
            "ldb-snapstore-{name}-{}-{sequence}.redb",
            std::process::id()
        ))
    }

    /// Applies rows `0..n` through the relational engine.
    fn apply_rows(projection: &mut RelationalProjection, n: u64) -> Result<(), ProjectionError> {
        for i in 0..n {
            let digest = lgwks_std::hash::blake3(&i.to_le_bytes());
            let row = *digest.as_bytes();
            projection.apply(ProjectionAction::Apply(row))?;
        }
        Ok(())
    }

    /// Seals a snapshot of `state` at the given frontier.
    fn seal_at(
        store: &RedbSnapshotStore,
        scope: Scope,
        projection: IndexId,
        state: &RelationalProjection,
        last_row: [u8; 32],
        last_seq: u64,
    ) -> Result<SnapshotEnvelope, SnapshotEnvelopeError> {
        create_snapshot(
            store,
            scope,
            SnapshotInput {
                projection,
                semantics: [9_u8; 32],
                state,
                last_row,
                last_seq,
                created_at_secs: 1_700_000_000,
            },
        )
    }

    #[test]
    fn snapshot_leg_interchangeable() -> Result<(), Box<dyn std::error::Error>> {
        let path = store_path("interchange");
        let store = RedbSnapshotStore::open(&path)?;
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let projection = IndexId::new();

        let mut mem = RelationalProjection::new();
        apply_rows(&mut mem, 100)?;
        let last_row = *mem.state().last().ok_or("rows should exist")?;
        seal_at(&store, scope, projection, &mem, last_row, 99)?;

        let mut tail = RelationalProjection::new();
        apply_rows(&mut tail, 200)?;
        let full_checkpoint = tail.checkpoint();

        let sqlite_seed = SqliteRelationalProjection::new()?;
        let replay =
            |restored: &mut SqliteRelationalProjection, after: u64| -> Result<u64, String> {
                let mut applied = 0_u64;
                for i in (after.saturating_add(1))..200 {
                    let digest = lgwks_std::hash::blake3(&i.to_le_bytes());
                    let row = *digest.as_bytes();
                    restored
                        .apply(ProjectionAction::Apply(row))
                        .map_err(|error| error.to_string())?;
                    applied = applied.saturating_add(1);
                }
                Ok(applied)
            };
        let (restored, stats) = restore_or_replay(&store, scope, projection, sqlite_seed, replay)?;
        assert!(stats.from_snapshot, "must take the snapshot path");
        assert_eq!(
            restored.checkpoint(),
            full_checkpoint,
            "sqlite restore plus tail must match full replay"
        );
        std::fs::remove_file(&path)?;
        Ok(())
    }

    #[test]
    fn genesis_fallback_on_missing_snapshot() -> Result<(), Box<dyn std::error::Error>> {
        let path = store_path("genesis");
        let store = RedbSnapshotStore::open(&path)?;
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let seed = RelationalProjection::new();
        let replay = |restored: &mut RelationalProjection, _after: u64| -> Result<u64, String> {
            apply_rows(restored, 10).map_err(|error| error.to_string())?;
            Ok(10)
        };
        let (restored, stats) = restore_or_replay(&store, scope, IndexId::new(), seed, replay)?;
        assert!(!stats.from_snapshot, "no snapshot means genesis");
        assert_eq!(restored.state().len(), 10, "genesis must replay all rows");
        std::fs::remove_file(&path)?;
        Ok(())
    }

    #[test]
    fn checkpoint_mismatch_falls_back_to_genesis() -> Result<(), Box<dyn std::error::Error>> {
        let path = store_path("mismatch");
        let store = RedbSnapshotStore::open(&path)?;
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let projection = IndexId::new();

        let mut mem = RelationalProjection::new();
        apply_rows(&mut mem, 20)?;
        let mut envelope = seal_at(
            &store,
            scope,
            projection,
            &mem,
            *mem.state().last().ok_or("rows should exist")?,
            19,
        )?;
        envelope.checkpoint = [0xFF_u8; 32];
        envelope.hash = envelope.compute_hash()?;
        store.save_snapshot(&envelope)?;

        let seed = RelationalProjection::new();
        let replay = |restored: &mut RelationalProjection, _after: u64| -> Result<u64, String> {
            apply_rows(restored, 20).map_err(|error| error.to_string())?;
            Ok(20)
        };
        let (restored, stats) = restore_or_replay(&store, scope, projection, seed, replay)?;
        assert!(!stats.from_snapshot, "a lying envelope must fall back");
        assert!(
            stats
                .fallback_reason
                .as_deref()
                .is_some_and(|reason| reason.contains("checkpoint mismatch")),
            "the reason must name the checkpoint mismatch"
        );
        assert_eq!(restored.state().len(), 20, "genesis must replay all rows");
        std::fs::remove_file(&path)?;
        Ok(())
    }
}
