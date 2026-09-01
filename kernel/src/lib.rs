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
pub mod fdb;
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
pub type Result<T> = std::result::Result<T, KernelError>;
