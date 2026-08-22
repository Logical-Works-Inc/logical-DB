//! ldb-kernel: the logical-DB spine (DB-A, issue #1).
//!
//! Owns the canonical row model, the storage and clock ports, and the
//! framed journal that makes batch commits atomic. Invariants enforced
//! here: INV-ONE-CORE-FOUR-CLASSES (five classes, unknown tags
//! unconstructable), INV-BATCH-IS-ONE-FRAME (a batch is one journal
//! frame, so replay applies it whole or not at all), INV-TS-MONOTONIC
//! (journal ts never regresses — version-ordered commits), and
//! INV-DURABLE-BEFORE-ACK (fsync lands before the KV sees the write).
#![forbid(unsafe_code)]

pub use lgwks_std;

pub mod address;
pub mod adjacency;
pub mod error;
pub mod journal;
pub mod memkv;
pub mod model;
pub mod port;
pub mod record;
pub mod store;
pub mod term;
pub mod veccode;
#[cfg(kani)]
mod verify;

pub use error::KernelError;
pub type Result<T> = std::result::Result<T, KernelError>;
