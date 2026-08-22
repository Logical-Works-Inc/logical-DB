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
