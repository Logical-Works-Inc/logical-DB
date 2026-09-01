//! Logical-DB: one admitted application-data contract with two deployments.
//!
//! [`admitted::HybridDb`] is the product boundary. It applies the same bounded,
//! Braid-authorized request model and account/container/zone tuple bytes either
//! in-process over durable redb or through the official FoundationDB client.
//! Provider identity is private, so applications cannot unwrap the handle and
//! bypass admission, state binding, idempotency, or the ordered change feed.
//!
//! The older five-class model, storage ports, and framed journal remain public
//! only as migration compatibility and deterministic fault oracles. They are
//! not the canonical application API and must not acquire new product meaning.
//!
//! Raw transaction internals are deliberately sealed:
//!
//! ```compile_fail
//! use logical_db::fdb::LogicalDb;
//! ```
#![forbid(unsafe_code)]

pub use lgwks_std;

#[cfg(feature = "braid")]
pub mod address;
pub mod adjacency;
#[cfg(feature = "braid")]
pub mod admitted;
pub mod claim;
pub mod contains;
pub mod error;
// The pre-ADR-003 transaction shim remains compiled for migration tests but is
// not reachable by product callers unless they explicitly opt into legacy-raw.
#[cfg_attr(not(feature = "legacy-raw"), allow(dead_code))]
mod fdb;
pub mod journal;
pub mod memkv;
pub mod migration;
pub mod model;
pub mod port;
pub mod record;
pub mod store;
pub mod term;
pub mod tuple;
pub mod veccode;
#[cfg(kani)]
mod verify;

pub use error::KernelError;
pub use fdb::DatabaseOptions;
#[cfg(feature = "legacy-raw")]
pub use fdb::FdbBackend;
pub type Result<T> = std::result::Result<T, KernelError>;
