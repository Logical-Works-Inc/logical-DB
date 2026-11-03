//! Shared seeded-simulation harness for the projection snapshot sims.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.
//!
//! One definition each of the deterministic RNG, row identities, temp paths,
//! trace tags, and the run-twice determinism check, so the sim files share
//! them instead of repeating them. One seed still drives every input of
//! every trace; the same seed always hashes the same trace.

use std::sync::atomic::{AtomicU64, Ordering};

use lgwks_std::hash::Hasher;
use logical_db_model::{AccountId, IndexId, Scope, SpaceId};
use logical_db_store::projection_indexed::IndexedProjection;
use logical_db_store::projection_relational::{
    ProjectionAction, RelationalProjection, SqliteRelationalProjection,
};
use logical_db_store::snapshot_backend::RedbSnapshotStore;
use logical_db_store::snapshot_envelope::SnapshotEnvelope;
use logical_db_store::snapshot_file::FileProjectionIndex;
use logical_db_store::snapshot_restore::{SnapshotInput, create_snapshot, restore_or_replay};

/// Creation-time base every sim derives its timestamps from.
pub const TIME_BASE: u64 = 1_700_000_000;

/// Deterministic xorshift64: the only randomness the sims use.
pub struct XorShift {
    /// Current state, never zero.
    state: u64,
}

impl XorShift {
    /// Creates the generator; a zero seed maps to a fixed nonzero start.
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self {
            state: if seed == 0 {
                0x243F_6A88_85A3_08D3
            } else {
                seed
            },
        }
    }

    /// Returns the next pseudorandom word.
    #[must_use]
    pub fn next_u64(&mut self) -> u64 {
        let mut mixer = self.state;
        mixer ^= mixer << 13;
        mixer ^= mixer >> 7;
        mixer ^= mixer << 17;
        self.state = mixer;
        mixer
    }

    /// Returns a word below `bound`.
    ///
    /// # Errors
    ///
    /// Returns a draw exhaustion when `bound` is zero, which the
    /// `debug_assert` plus every caller passing a nonzero bound excludes.
    pub fn below(&mut self, bound: u64) -> Result<u64, Box<dyn std::error::Error>> {
        debug_assert!(bound > 0, "sim bound must be nonzero");
        self.next_u64()
            .checked_rem(bound)
            .ok_or("sim draw exhausted")
            .map_err(Into::into)
    }

    /// Returns 32 seeded bytes.
    #[must_use]
    pub fn bytes32(&mut self) -> [u8; 32] {
        let mut out = [0_u8; 32];
        for chunk in out.chunks_mut(8) {
            chunk.copy_from_slice(&self.next_u64().to_le_bytes());
        }
        out
    }
}

/// One deterministic row identity from a sequence number.
#[must_use]
pub fn test_row(i: u64) -> [u8; 32] {
    let mut row = [0_u8; 32];
    row[..8].copy_from_slice(&i.to_le_bytes());
    row[8..16].copy_from_slice(&i.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_le_bytes());
    row[16..24].copy_from_slice(&i.wrapping_mul(0xBF58_476D_1CE4_E5B9).to_le_bytes());
    row
}

/// One unique temporary path for this run.
#[must_use]
pub fn temp_path(prefix: &str, ext: &str) -> std::path::PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let sequence = COUNTER.fetch_add(1, Ordering::SeqCst);
    std::env::temp_dir().join(format!(
        "ldb-sim-{prefix}-{}-{sequence}.{ext}",
        std::process::id()
    ))
}

/// Opens a seeded RNG plus a domain-tagged trace hasher.
#[must_use]
pub fn opened(seed: u64, domain: &[u8], tag: &[u8]) -> (XorShift, Hasher) {
    let mut hasher = Hasher::new();
    hasher.update(domain);
    hasher.update(tag);
    (XorShift::new(seed), hasher)
}

/// Finalizes a trace hash.
#[must_use]
pub fn trace_tag(hasher: Hasher) -> [u8; 32] {
    *hasher.finalize().as_bytes()
}

/// Runs one trace twice per seed and asserts identical trace hashes.
pub fn check(
    seeds: [u64; 2],
    trace: impl Fn(u64) -> Result<[u8; 32], Box<dyn std::error::Error>>,
) -> Result<(), Box<dyn std::error::Error>> {
    for seed in seeds {
        let first = trace(seed)?;
        let second = trace(seed)?;
        assert_eq!(first, second, "same seed must hash identically");
    }
    Ok(())
}

/// Replays rows `after+1..total` through one caller-supplied row apply, so
/// the counting loop exists exactly once for both relational engines.
fn replay_rows<P>(
    restored: &mut P,
    after: u64,
    total: u64,
    mut apply: impl FnMut(&mut P, u64) -> Result<(), String>,
) -> Result<u64, String> {
    let mut applied = 0_u64;
    for idx in (after.saturating_add(1))..total {
        apply(restored, idx)?;
        applied = applied.saturating_add(1);
    }
    Ok(applied)
}

/// Applies one seeded test row to a projection, reporting the backend refusal.
fn apply_test_row(restored: &mut RelationalProjection, idx: u64) -> Result<(), String> {
    restored
        .apply(ProjectionAction::Apply(test_row(idx)))
        .map_err(|error| error.to_string())
}

/// Applies one seeded test row to a SQLite-backed projection.
fn apply_test_row_sqlite(
    restored: &mut SqliteRelationalProjection,
    idx: u64,
) -> Result<(), String> {
    restored
        .apply(ProjectionAction::Apply(test_row(idx)))
        .map_err(|error| error.to_string())
}

/// Replays rows `after+1..total` into a relational projection.
pub fn replay_all(total: u64) -> impl FnMut(&mut RelationalProjection, u64) -> Result<u64, String> {
    move |restored: &mut RelationalProjection, after: u64| -> Result<u64, String> {
        replay_rows(restored, after, total, apply_test_row)
    }
}

/// Replays rows `after+1..total` into a Sqlite-backed relational projection.
pub fn replay_all_sqlite(
    total: u64,
) -> impl FnMut(&mut SqliteRelationalProjection, u64) -> Result<u64, String> {
    move |restored: &mut SqliteRelationalProjection, after: u64| -> Result<u64, String> {
        replay_rows(restored, after, total, apply_test_row_sqlite)
    }
}

/// Replays rows `0..rows` into a relational projection, ignoring the tail
/// cursor: the genesis path.
pub fn replay_genesis(
    rows: u64,
) -> impl FnMut(&mut RelationalProjection, u64) -> Result<u64, String> {
    move |restored: &mut RelationalProjection, _after: u64| -> Result<u64, String> {
        apply_rows(restored, rows).map_err(|error| error.to_string())?;
        Ok(rows)
    }
}

/// One deterministic caller-seed vector of `len` components.
///
/// # Errors
///
/// Returns a seed length overflow that cannot happen for sim spreads.
pub fn seed_vec(rng: &mut XorShift, len: usize) -> Result<Vec<i32>, Box<dyn std::error::Error>> {
    let mut out = Vec::with_capacity(len);
    for _ in 0..len {
        let bits = rng.next_u64() & 0x7FFF_FFFF;
        let value =
            i32::try_from(bits).map_err(|error| format!("sim seed bits overflow: {error}"))?;
        out.push(value);
    }
    Ok(out)
}

/// One seeded count in `base..base+spread`.
///
/// # Errors
///
/// Returns a count overflow that cannot happen for sim spreads.
pub fn count(
    rng: &mut XorShift,
    base: usize,
    spread: u64,
) -> Result<usize, Box<dyn std::error::Error>> {
    let extra = usize::try_from(rng.below(spread.max(1))?)
        .map_err(|error| format!("sim count overflow: {error}"))?;
    Ok(base.saturating_add(extra))
}

/// Builds one seeded index with a shared term plus per-row terms.
///
/// # Errors
///
/// Returns a length overflow that cannot happen for sim counts.
pub fn seeded_index(
    rng: &mut XorShift,
    rows: usize,
) -> Result<IndexedProjection, Box<dyn std::error::Error>> {
    let mut projection = IndexedProjection::new();
    let count = u64::try_from(rows).map_err(|error| format!("sim row count overflow: {error}"))?;
    for idx in 0..count {
        let lane = idx.checked_rem(7).ok_or("sim lane dispatch overflow")?;
        projection.insert(
            test_row(idx),
            vec![
                String::from("shared"),
                format!(
                    "term{}",
                    rng.below(25)
                        .map_err(|error| format!("sim term draw overflow: {error}"))?
                ),
                format!("u{lane}"),
            ],
        );
    }
    Ok(projection)
}

/// Builds one seeded index through the file path and returns its bytes.
///
/// # Errors
///
/// Returns a file I/O failure.
pub fn build_snapshot(
    projection: &IndexedProjection,
) -> Result<(std::path::PathBuf, Vec<u8>), Box<dyn std::error::Error>> {
    let path = temp_path("snap", "lwmx");
    FileProjectionIndex::build(&path, projection)?;
    let bytes = std::fs::read(&path)?;
    Ok((path, bytes))
}

/// Removes one temporary file the sim created.
///
/// # Errors
///
/// Returns a file I/O failure.
pub fn cleanup(path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    std::fs::remove_file(path)?;
    Ok(())
}

/// Applies rows `0..count` through a relational projection.
///
/// # Errors
///
/// Returns [`ProjectionError`] past the row budget, which the sim counts
/// never reach.
pub fn apply_rows(
    projection: &mut RelationalProjection,
    count: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    for idx in 0..count {
        projection.apply(ProjectionAction::Apply(test_row(idx)))?;
    }
    Ok(())
}

/// One isolated snapshot store with its scope and projection identity.
#[non_exhaustive]
pub struct Fixture {
    /// Temporary store path, removed by the trace when done.
    pub path: std::path::PathBuf,
    /// The dedicated snapshot store.
    pub store: RedbSnapshotStore,
    /// The isolated tenant scope.
    pub scope: Scope,
    /// The projected index identity.
    pub projection: IndexId,
}

/// Opens one isolated snapshot store for a trace.
///
/// # Errors
///
/// Returns a backend failure when the store file cannot be opened.
pub fn fixture(rng: &mut XorShift, prefix: &str) -> Result<Fixture, Box<dyn std::error::Error>> {
    let path = temp_path(prefix, "redb");
    let store = RedbSnapshotStore::open(&path)?;
    Ok(Fixture {
        path,
        store,
        scope: fresh_scope(rng)?,
        projection: fresh_projection(rng)?,
    })
}

/// One trace's seeded RNG, trace hasher, and isolated store.
///
/// Traces that share the open/verify/finish skeleton run through one of
/// these instead of repeating it, so the skeleton exists exactly once.
#[non_exhaustive]
pub struct Case {
    /// The seeded RNG driving every input of the trace.
    pub rng: XorShift,
    /// The trace hasher accumulating the seeded trace.
    pub hasher: Hasher,
    /// The isolated snapshot store with its scope and projection.
    pub own: Fixture,
}

impl Case {
    /// Opens one trace: seeded RNG, domain-tagged hasher, isolated store.
    ///
    /// # Errors
    ///
    /// Returns a backend failure when the store file cannot be opened.
    pub fn open(
        seed: u64,
        domain: &[u8],
        tag: &[u8],
        prefix: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let (mut rng, hasher) = opened(seed, domain, tag);
        let own = fixture(&mut rng, prefix)?;
        Ok(Self { rng, hasher, own })
    }

    /// Closes one trace: removes the store file and finalizes the trace hash.
    ///
    /// # Errors
    ///
    /// Returns a file I/O failure.
    pub fn finish(self) -> Result<[u8; 32], Box<dyn std::error::Error>> {
        cleanup(&self.own.path)?;
        Ok(trace_tag(self.hasher))
    }
}

/// Formats 16 seeded bytes as quoted hyphenated lowercase hex: the canonical
/// UUID text form the model's Deserialize contract parses. Quoted, because
/// the estate JSON owner parses JSON documents, not bare strings.
fn quoted(rng: &mut XorShift) -> String {
    let bytes = rng.bytes32();
    let hex: String = bytes[..16]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    format!(
        "\"{}-{}-{}-{}-{}\"",
        &hex[..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..]
    )
}

/// One fresh isolated tenant scope, derived from the seed.
///
/// # Errors
///
/// Returns the model's parse refusal, which the canonical text never triggers.
pub fn fresh_scope(rng: &mut XorShift) -> Result<Scope, Box<dyn std::error::Error>> {
    Ok(Scope::new(
        lgwks_std::json::from_str(&quoted(rng))?,
        lgwks_std::json::from_str(&quoted(rng))?,
    ))
}

/// One fresh isolated projection identity, derived from the seed.
///
/// # Errors
///
/// Returns the model's parse refusal, which the canonical text never triggers.
pub fn fresh_projection(rng: &mut XorShift) -> Result<IndexId, Box<dyn std::error::Error>> {
    Ok(lgwks_std::json::from_str(&quoted(rng))?)
}

/// One fresh isolated account identity, derived from the seed.
///
/// # Errors
///
/// Returns the model's parse refusal, which the canonical text never triggers.
pub fn fresh_account(rng: &mut XorShift) -> Result<AccountId, Box<dyn std::error::Error>> {
    Ok(lgwks_std::json::from_str(&quoted(rng))?)
}

/// One fresh isolated space identity, derived from the seed.
///
/// # Errors
///
/// Returns the model's parse refusal, which the canonical text never triggers.
pub fn fresh_space(rng: &mut XorShift) -> Result<SpaceId, Box<dyn std::error::Error>> {
    Ok(lgwks_std::json::from_str(&quoted(rng))?)
}

/// Seals `rows` relational rows at one frontier and persists the envelope.
///
/// # Errors
///
/// Returns a backend or envelope failure when sealing or saving fails.
pub fn seal_rows(
    store: &RedbSnapshotStore,
    scope: Scope,
    projection: IndexId,
    rows: u64,
    semantics: [u8; 32],
    created_at_secs: u64,
) -> Result<(RelationalProjection, SnapshotEnvelope), Box<dyn std::error::Error>> {
    let mut mem = RelationalProjection::new();
    apply_rows(&mut mem, rows)?;
    let (last_row, last_seq) = match mem.state().last() {
        Some(row) => (*row, rows.saturating_sub(1)),
        None => ([0_u8; 32], 0),
    };
    let envelope = create_snapshot(
        store,
        scope,
        SnapshotInput::new(
            projection,
            semantics,
            &mem,
            last_row,
            last_seq,
            created_at_secs,
        ),
    )?;
    Ok((mem, envelope))
}

/// The harness exercises its own surface on a fixed seed.
#[test]
fn harness_self_check() -> Result<(), Box<dyn std::error::Error>> {
    let (mut rng, mut hasher) = opened(0x5EED_1234, b"ldb.sim.harness.v1", b"self-check");
    let row = test_row(rng.next_u64());
    hasher.update(&row);
    let index = seeded_index(&mut rng, 3)?;
    assert_eq!(index.len(), 3, "seeded helper indexes three rows");
    let (built, bytes) = build_snapshot(&index)?;
    assert!(!bytes.is_empty(), "built snapshot has bytes");
    cleanup(&built)?;
    let own = fixture(&mut rng, "self-check")?;
    let scope = fresh_scope(&mut rng)?;
    hasher.update(scope.space().as_uuid().as_bytes());
    let probe_account = fresh_account(&mut rng)?;
    let probe_space = fresh_space(&mut rng)?;
    hasher.update(probe_account.as_uuid().as_bytes());
    hasher.update(probe_space.as_uuid().as_bytes());
    let semantics = rng.bytes32();
    let probe_count = count(&mut rng, 2, 3)?;
    let probe = seed_vec(&mut rng, probe_count)?;
    let probe_len =
        u64::try_from(probe.len()).map_err(|error| format!("sim probe overflow: {error}"))?;
    hasher.update(&probe_len.to_le_bytes());
    let (_mem, envelope) = seal_rows(
        &own.store,
        own.scope,
        own.projection,
        2,
        semantics,
        TIME_BASE,
    )?;
    envelope.verify()?;
    hasher.update(&envelope.hash);
    cleanup(&own.path)?;
    let mut probe = Case::open(7, b"ldb.sim.harness.v1", b"self-check-case", "self-check")?;
    probe.hasher.update(&probe.rng.next_u64().to_le_bytes());
    let (_pmem, _penvelope) = seal_rows(
        &probe.own.store,
        probe.own.scope,
        probe.own.projection,
        2,
        [2_u8; 32],
        TIME_BASE,
    )?;
    let (_restored, stats) = restore_or_replay(
        &probe.own.store,
        probe.own.scope,
        probe.own.projection,
        RelationalProjection::new(),
        replay_all(2),
    )?;
    assert!(
        stats.from_snapshot,
        "harness self-check takes the snapshot path"
    );
    let sqlite_seed = SqliteRelationalProjection::new()?;
    let (_srestored, _sstats) = restore_or_replay(
        &probe.own.store,
        probe.own.scope,
        probe.own.projection,
        sqlite_seed,
        replay_all_sqlite(2),
    )?;
    let (_genesis, _) = restore_or_replay(
        &probe.own.store,
        probe.own.scope,
        probe.own.projection,
        RelationalProjection::new(),
        replay_genesis(1),
    )?;
    let tag = probe.finish()?;
    assert!(
        tag.iter().any(|byte| *byte != 0),
        "the self-check trace is non-degenerate"
    );
    Ok(())
}
