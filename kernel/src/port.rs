//! Owns the ports: the seams outboard engines plug into. Core stays
//! deterministic and provider-independent — the MemKv reference
//! implementation lives in `memkv`, a FoundationDB adapter arrives at
//! G5 behind the same trait (G2 rule: nothing embedded by default).

use crate::Result;

// ── KvPort ───────────────────────────────────────────────────

/// Ordered key-value port. Implementations must keep `scan_prefix`
/// ascending by key; that is what makes history scans free.
pub trait KvPort {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()>;
    fn delete(&mut self, key: &[u8]) -> Result<()>;
    /// All `(key, value)` whose key begins with `prefix`, ascending.
    fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;
    /// Applies one whole committed batch atomically. The default loops
    /// point ops — correct only when an external journal owns
    /// durability (the reference path). Self-durable backends override
    /// this with a single transaction so a batch is never half-applied
    /// (issue #6 §5 backend contract).
    fn apply_batch(&mut self, entries: &[(Vec<u8>, Vec<u8>)], version: u64) -> Result<()> {
        let _ = version;
        for (key, value) in entries {
            self.put(key.clone(), value.clone())?;
        }
        Ok(())
    }
    /// Whether this backend is self-durable (implements `DurableKv`).
    /// Used to enforce INV-ONE-CRASH-STORY: a durable backend must not
    /// be opened through the journaled constructor after F4 retirement
    /// (ADR-002 INV-PARITY-BEFORE-DELETE).
    fn is_durable(&self) -> bool {
        false
    }
}

// ── DurableKv ────────────────────────────────────────────────

/// A self-durable ordered KV backend: committed batches survive abrupt
/// process termination by the backend's own mechanics, with no external
/// journal (issue #6 §5 — one crash story per production path). The
/// store refuses to open this kind of backend through the journaled
/// constructor; durability is visible in the type.
pub trait DurableKv: KvPort {
    /// The highest `version` previously accepted by [`KvPort::apply_batch`].
    /// A fresh store reports 0.
    fn committed_version(&self) -> Result<u64>;
}

// ── ClockPort ────────────────────────────────────────────────

/// Logical-time source. INV-TS-MONOTONIC is enforced against whatever
/// this returns: an implementation may be injectable and deterministic
/// (tests), wall-clock, or later a versionstamp oracle. Graft of the
/// turso LogicalClock organ (`core/mvcc/clock.rs:9`); the
/// publish-under-lock refinement lands with concurrent txns at G4.
pub trait ClockPort {
    fn now(&self) -> u64;
}

/// Monotonic logical clock — the 1-click default for `Ldb::open_at`.
/// Replaces the 10-line `Seq(Cell<u64>)` copy-pasted across every
/// example and test. `Cell` is single-threaded, matching the `Ldb`
/// single-owner contract; `Default` starts at `0` so first `now()`
/// returns `1`, preserving `last_ts` seeding.
pub struct MonotonicClock(std::cell::Cell<u64>);

impl MonotonicClock {
    pub fn new(start: u64) -> Self {
        Self(std::cell::Cell::new(start))
    }
}

impl Default for MonotonicClock {
    fn default() -> Self {
        Self(std::cell::Cell::new(0))
    }
}

impl ClockPort for MonotonicClock {
    fn now(&self) -> u64 {
        let n = self.0.get() + 1;
        self.0.set(n);
        n
    }
}
