//! Deterministic simulation of the KV cache against an in-memory model.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.
//!
//! One seed drives every choice: the key, the operation and the value. Each
//! seeded episode puts, deletes, batches and reads through the public
//! `KvCache` surface and checks every observable result against a
//! `BTreeMap` model, which is the property-based state-machine check of
//! FoundationDB's simulation testing. Values never carry a TTL, so no wall
//! clock reaches an assertion. The same seed must yield the same trace
//! digest, and a reopened replica must serve exactly the model's state
//! (restart durability).

use std::{collections::BTreeMap, path::Path, path::PathBuf};

use lgwks_std::hash::Hasher;
use logical_db_kv::{AccountId, BatchPut, KvCache, KvError, ReplicaId, Scope, SpaceId};

/// Every failure a simulated episode reports, typed by the layer that broke.
#[derive(Debug, thiserror::Error)]
enum SimError {
    /// The cache under simulation returned an error.
    #[error(transparent)]
    Kv(#[from] KvError),
    /// A replica fixture file could not be removed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// A draw bound or index did not fit its integer width.
    #[error(transparent)]
    Width(#[from] std::num::TryFromIntError),
    /// The simulation's own bookkeeping reached an impossible state.
    #[error("{0}")]
    Harness(&'static str),
}

impl From<&'static str> for SimError {
    /// Wraps a harness invariant failure.
    fn from(message: &'static str) -> Self {
        Self::Harness(message)
    }
}

/// Keys the episodes draw from; a small space forces overwrites and deletes
/// of live keys.
const KEYS: [&str; 6] = ["a", "b", "c", "user:1", "user:2", "user:3"];
/// Operations per seeded episode.
const STEPS: usize = 64;
/// Seeds explored by the model-equivalence run.
const SEEDS: u64 = 24;
/// Namespace every simulated cache writes.
const NAMESPACE: &str = "sim";

/// SplitMix64 stream: the single seed source for every simulated choice.
struct SeededRng {
    /// Current stream position.
    state: u64,
}

impl SeededRng {
    /// Starts the stream at `seed`.
    const fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// Returns the next 64-bit draw.
    fn draw(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut mixed = self.state;
        mixed = (mixed ^ mixed.wrapping_shr(30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        mixed = (mixed ^ mixed.wrapping_shr(27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        mixed ^ mixed.wrapping_shr(31)
    }

    /// Draws an index below `bound`.
    fn below(&mut self, bound: usize) -> Result<usize, SimError> {
        let draw = self
            .draw()
            .checked_rem(u64::try_from(bound)?)
            .ok_or("empty draw range")?;
        Ok(usize::try_from(draw)?)
    }

    /// Draws one key from [`KEYS`].
    fn key(&mut self) -> Result<&'static str, SimError> {
        Ok(*KEYS
            .get(self.below(KEYS.len())?)
            .ok_or("key index out of range")?)
    }
}

/// A replica path unique per call.
fn replica_path(tag: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "logical-db-kv-sim-{tag}-{:?}.redb",
        ReplicaId::new()
    ))
}

/// Runs one seeded episode against `cache`, checking every read and delete
/// against `model`, and returns the trace digest.
fn run_episode(
    seed: u64,
    cache: &KvCache,
    model: &mut BTreeMap<String, Vec<u8>>,
) -> Result<[u8; 32], SimError> {
    let mut rng = SeededRng::new(seed);
    let mut trace = Hasher::new();
    for step in 0..STEPS {
        let key = rng.key()?;
        let value = format!("{seed}:{step}").into_bytes();
        match rng.below(4)? {
            0 => {
                cache.put(key, &value, None)?;
                model.insert(key.to_owned(), value.clone());
                trace.update(b"put");
            }
            1 => {
                let removed = cache.delete(key)?;
                let expected = model.remove(key).is_some();
                assert_eq!(
                    removed, expected,
                    "seed {seed} step {step}: delete liveness"
                );
                trace.update(if removed { b"del+" } else { b"del-" });
            }
            2 => {
                let second = rng.key()?;
                cache.put_many(&[
                    BatchPut::new(key, value.clone(), None),
                    BatchPut::new(second, value.clone(), None),
                ])?;
                model.insert(key.to_owned(), value.clone());
                model.insert(second.to_owned(), value.clone());
                trace.update(b"batch");
                trace.update(second.as_bytes());
            }
            _ => {
                let read = cache.get(key)?;
                assert_eq!(
                    read.as_ref(),
                    model.get(key),
                    "seed {seed} step {step}: get"
                );
                trace.update(if read.is_some() { b"get+" } else { b"get-" });
            }
        }
        trace.update(key.as_bytes());
        trace.update(&value);
    }
    Ok(*trace.finalize().as_bytes())
}

/// Asserts the cache serves exactly the model: every live key in key order
/// through `scan`, and every absent key reads `None`.
fn assert_matches_model(
    cache: &KvCache,
    model: &BTreeMap<String, Vec<u8>>,
    seed: u64,
) -> Result<(), SimError> {
    let scanned = cache.scan("", None, KEYS.len())?;
    let served: Vec<(&str, &[u8])> = scanned
        .iter()
        .map(|entry| (entry.key(), entry.value()))
        .collect();
    let expected: Vec<(&str, &[u8])> = model
        .iter()
        .map(|entry| (entry.0.as_str(), entry.1.as_slice()))
        .collect();
    assert_eq!(
        served, expected,
        "seed {seed}: scan diverged from the model"
    );
    for key in KEYS {
        assert_eq!(
            cache.get(key)?.as_ref(),
            model.get(key),
            "seed {seed}: point read of {key}"
        );
    }
    Ok(())
}

/// Removes a simulated replica file.
fn remove_replica(path: &Path) -> Result<(), SimError> {
    std::fs::remove_file(path)?;
    Ok(())
}

#[test]
fn sim_cache_matches_model_across_seeds() -> Result<(), SimError> {
    for seed in 0..SEEDS {
        let path = replica_path("model");
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let cache = KvCache::open(&path, scope, ReplicaId::new(), NAMESPACE)?;
        let mut model = BTreeMap::new();
        run_episode(seed, &cache, &mut model)?;
        assert_matches_model(&cache, &model, seed)?;
        drop(cache);
        remove_replica(&path)?;
    }
    Ok(())
}

#[test]
fn sim_same_seed_same_trace_and_restart_serves_model() -> Result<(), SimError> {
    let seed = 0x5EED_u64;
    let first_path = replica_path("trace-a");
    let second_path = replica_path("trace-b");
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let replica = ReplicaId::new();
    let mut first_model = BTreeMap::new();
    let first = {
        let cache = KvCache::open(&first_path, scope, ReplicaId::new(), NAMESPACE)?;
        run_episode(seed, &cache, &mut first_model)?
    };
    let mut second_model = BTreeMap::new();
    let second = {
        let cache = KvCache::open(&second_path, scope, replica, NAMESPACE)?;
        run_episode(seed, &cache, &mut second_model)?
    };
    assert_eq!(first, second, "one seed must replay one trace");
    assert_eq!(first_model, second_model, "one seed must reach one state");
    let reopened = KvCache::open(&second_path, scope, replica, NAMESPACE)?;
    assert_matches_model(&reopened, &second_model, seed)?;
    drop(reopened);
    remove_replica(&first_path)?;
    remove_replica(&second_path)?;
    Ok(())
}
