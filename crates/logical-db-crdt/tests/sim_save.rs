//! Seeded hostile simulation of `Document::save`, the per-edit hot path.
//!
//! Requirement: a durable edit costs O(change), not O(history), so `save`
//! stores the uncompressed automerge encoding with no reload (automerge's
//! default save DEFLATEs every column over 256 bytes and `save_and_verify`
//! reloads the whole document). Verification belongs to `Document::load`,
//! which must refuse every corrupted snapshot.
//!
//! Oracle: a reference model of the last accepted write per key, beside the
//! documented refusal rules (a key is 1..=512 bytes and never the tombstone
//! property; numbers are finite; strings and bytes stay within
//! `MAX_CHANGE_BYTES`). Each run is driven by one xorshift64* stream over its
//! seed, so a failure names the seed that replays it.

#![forbid(unsafe_code)]

use std::{collections::BTreeMap, error::Error};

use logical_db_crdt::{Document, Item, Object, Scalar, TOMBSTONE_PROPERTY};
use logical_db_model::MAX_CHANGE_BYTES;

/// Seeds each fool explores.
const SEEDS: u64 = 64;
/// Seeded writes per run.
const WRITES: usize = 48;
/// Longest accepted property name, in bytes.
const MAX_PROPERTY_BYTES: usize = 512;
/// Fragments a weird-bytes value or key is assembled from.
const WEIRD: &[&str] = &[
    "\0", "\u{202e}", "\u{200b}", "\u{feff}", "😀", "é", "e\u{301}", "../", "%00", "\r\n", " ",
];

/// A failing run: every defect names its seed.
type Failure = Box<dyn Error>;

/// Seeded choices (xorshift64*): std has no seeded generator.
#[derive(Debug)]
struct Stream(u64);

impl Stream {
    /// A stream fully determined by `seed` (never the all-zero state).
    const fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x2545_f491_4f6c_dd1d) | 1)
    }

    /// The next 64 seeded bits.
    const fn bits(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    /// A choice in `0..bound`.
    fn pick(&mut self, bound: usize) -> Result<usize, Failure> {
        let choice = self
            .bits()
            .checked_rem(u64::try_from(bound)?)
            .ok_or("a choice needs a nonzero range")?;
        Ok(usize::try_from(choice)?)
    }

    /// A seeded string of fewer than `most` weird fragments (possibly none).
    fn weird(&mut self, most: usize) -> Result<String, Failure> {
        let fragments = self.pick(most)?;
        (0..fragments)
            .map(|_| {
                WEIRD
                    .get(self.pick(WEIRD.len())?)
                    .copied()
                    .ok_or_else(|| "fragment out of range".into())
            })
            .collect()
    }
}

/// What the rules say a put of `value` under `key` must do.
fn refused(key: &str, value: &Scalar) -> bool {
    let oversize = |length: usize| length > MAX_CHANGE_BYTES;
    key.is_empty()
        || key.len() > MAX_PROPERTY_BYTES
        || key == TOMBSTONE_PROPERTY
        || match *value {
            Scalar::Number(number) => !number.is_finite(),
            Scalar::String(ref text) => oversize(text.len()),
            Scalar::Bytes(ref bytes) => oversize(bytes.len()),
            _ => false,
        }
}

/// One document beside its reference model.
#[derive(Debug, Default)]
struct Run {
    /// The real document under test.
    document: Document,
    /// Last accepted value per key.
    model: BTreeMap<String, Scalar>,
    /// Keys written and later deleted: they must read back absent.
    deleted: Vec<String>,
}

impl Run {
    /// Puts `value` under `key` and checks the outcome against the rules.
    fn put(&mut self, seed: u64, key: &str, value: Scalar) -> Result<(), Failure> {
        let expect_refusal = refused(key, &value);
        let outcome = self
            .document
            .edit(|editor| editor.put(&Object::root(), key, value.clone()));
        match (outcome.is_err(), expect_refusal) {
            (false, false) => {
                self.model.insert(key.to_owned(), value);
                self.deleted.retain(|gone| gone != key);
                Ok(())
            }
            (true, true) => Ok(()),
            (accepted_wrongly, _) => Err(format!(
                "seed {seed}: put of {} bytes under a {}-byte key {}",
                format!("{value:?}").len(),
                key.len(),
                if accepted_wrongly {
                    "was refused"
                } else {
                    "was accepted"
                },
            )
            .into()),
        }
    }

    /// Deletes `key` and moves it from the model to the absent set.
    fn delete(&mut self, key: &str) -> Result<(), Failure> {
        self.document
            .edit(|editor| editor.delete(&Object::root(), key))?;
        if self.model.remove(key).is_some() {
            self.deleted.push(key.to_owned());
        }
        Ok(())
    }

    /// Saves, then proves the bytes are the uncompressed encoding and reload
    /// to exactly the model. Returns the saved bytes.
    fn save_and_check(&mut self, seed: u64) -> Result<Vec<u8>, Failure> {
        let saved = self.document.save()?;
        let uncompressed = automerge::AutoCommit::load(&saved)?.save_nocompress();
        if saved != uncompressed {
            return Err(format!("seed {seed}: save stored a compressed encoding").into());
        }
        let mut reloaded = Document::load(&saved)?;
        for (key, value) in &self.model {
            let observed = reloaded.get(&Object::root(), key)?;
            if observed != Some(Item::Scalar(value.clone())) {
                return Err(format!(
                    "seed {seed}: key of {} bytes reloaded as {observed:?}",
                    key.len()
                )
                .into());
            }
        }
        for key in &self.deleted {
            if reloaded.get(&Object::root(), key)?.is_some() {
                return Err(format!("seed {seed}: a deleted key came back after reload").into());
            }
        }
        if reloaded.heads() != self.document.heads() {
            return Err(format!("seed {seed}: reload moved the frontier").into());
        }
        Ok(saved)
    }
}

/// A seeded scalar from every variant, edges included.
fn scalar(stream: &mut Stream) -> Result<Scalar, Failure> {
    let bits = stream.bits();
    Ok(match stream.pick(9)? {
        0 => Scalar::Null,
        1 => Scalar::Boolean(bits.is_multiple_of(2)),
        2 => Scalar::Integer(
            *[i64::MIN, i64::MAX, -1, 0]
                .get(stream.pick(4)?)
                .ok_or("edge")?,
        ),
        3 => Scalar::Unsigned(*[u64::MAX, 0, bits].get(stream.pick(3)?).ok_or("edge")?),
        4 => Scalar::Number(f64::from_bits(bits)),
        5 => Scalar::Number(
            *[-0.0, f64::MIN_POSITIVE, f64::MAX, f64::NAN]
                .get(stream.pick(4)?)
                .ok_or("edge")?,
        ),
        6 => Scalar::String(stream.weird(6)?),
        7 => Scalar::Bytes(
            bits.to_le_bytes()
                .get(..stream.pick(9)?)
                .ok_or("slice")?
                .to_vec(),
        ),
        _ => Scalar::Counter(i64::from_le_bytes(bits.to_le_bytes())),
    })
}

/// giant-and-empty: empty, one byte, the bound and one past it, as values and
/// as keys; the bound is accepted, one past it refused, the rest unchanged.
#[test]
fn sim_save_giant_and_empty_bounds_hold_and_save_stays_uncompressed() -> Result<(), Failure> {
    for seed in 0..SEEDS {
        let mut stream = Stream::new(seed);
        let mut run = Run::default();
        let sizes = [0, 1, MAX_CHANGE_BYTES, MAX_CHANGE_BYTES.saturating_add(1)];
        let size = *sizes.get(stream.pick(sizes.len())?).ok_or("size")?;
        run.put(seed, "giant", Scalar::String("x".repeat(size)))?;
        run.put(seed, "giant-bytes", Scalar::Bytes(vec![0xff; size]))?;
        for key_size in [
            0,
            1,
            MAX_PROPERTY_BYTES,
            MAX_PROPERTY_BYTES.saturating_add(1),
        ] {
            run.put(
                seed,
                &"k".repeat(key_size),
                Scalar::Integer(i64::try_from(key_size)?),
            )?;
        }
        run.put(seed, "empty", Scalar::String(String::new()))?;
        run.save_and_check(seed)?;
    }
    Ok(())
}

/// weird-bytes: NUL, bidi and zero-width marks, combining forms, traversal
/// text, NaN and every float bit pattern, integer extremes, as keys and values.
#[test]
fn sim_save_weird_bytes_round_trip_and_invalid_values_are_refused() -> Result<(), Failure> {
    for seed in 0..SEEDS {
        let mut stream = Stream::new(seed);
        let mut run = Run::default();
        for _ in 0..WRITES {
            let key = stream.weird(4)?;
            let value = scalar(&mut stream)?;
            run.put(seed, &key, value)?;
        }
        run.put(seed, TOMBSTONE_PROPERTY, Scalar::Boolean(true))?;
        run.save_and_check(seed)?;
    }
    Ok(())
}

/// double-clicker: every write lands twice, then a burst rewrites one key;
/// the last accepted write wins and survives a save and reload.
#[test]
fn sim_save_double_clicker_last_write_wins_across_reload() -> Result<(), Failure> {
    for seed in 0..SEEDS {
        let mut stream = Stream::new(seed);
        let mut run = Run::default();
        for write in 0..WRITES {
            let key = format!("k{}", stream.pick(4)?);
            let value = Scalar::Integer(i64::try_from(write)?);
            run.put(seed, &key, value.clone())?;
            run.put(seed, &key, value)?;
        }
        for burst in 0..stream.pick(200)? {
            run.put(seed, "burst", Scalar::Unsigned(u64::try_from(burst)?))?;
        }
        let saved = run.save_and_check(seed)?;
        run.document = Document::load(&saved)?;
        run.save_and_check(seed)?;
    }
    Ok(())
}

/// monkey: puts, deletes, deletes of absent keys and mid-stream reloads in
/// seeded nonsense order; every save stays uncompressed and matches the model.
#[test]
fn sim_save_monkey_reloads_mid_stream_and_keeps_editing() -> Result<(), Failure> {
    for seed in 0..SEEDS {
        let mut stream = Stream::new(seed);
        let mut run = Run::default();
        for _ in 0..WRITES {
            let key = format!("m{}", stream.pick(6)?);
            match stream.pick(4)? {
                0 => run.delete(&key)?,
                1 => {
                    let saved = run.save_and_check(seed)?;
                    run.document = Document::load(&saved)?;
                }
                _ => {
                    let value = scalar(&mut stream)?;
                    run.put(seed, &key, value)?;
                }
            }
        }
        run.save_and_check(seed)?;
    }
    Ok(())
}

/// saboteur: with no compression and no verify on save, load is the only
/// check left, so one flipped byte anywhere in a saved snapshot is refused.
#[test]
fn sim_save_saboteur_any_flipped_byte_is_refused_at_load() -> Result<(), Failure> {
    for seed in 0..SEEDS {
        let mut stream = Stream::new(seed);
        let mut run = Run::default();
        for _ in 0..stream.pick(WRITES)?.saturating_add(1) {
            let key = format!("s{}", stream.pick(8)?);
            let value = scalar(&mut stream)?;
            run.put(seed, &key, value)?;
        }
        let saved = run.save_and_check(seed)?;
        let mut flipped = saved.clone();
        let at = stream.pick(flipped.len())?;
        let byte = flipped.get_mut(at).ok_or("flip out of range")?;
        *byte ^= 1 << stream.pick(8)?;
        if Document::load(&flipped).is_ok() {
            return Err(
                format!("seed {seed}: a flip at byte {at} of {} loaded", saved.len()).into(),
            );
        }
    }
    Ok(())
}
