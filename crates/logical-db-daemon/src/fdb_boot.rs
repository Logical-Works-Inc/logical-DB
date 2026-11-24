//! The one FoundationDB client-network boot, and the only `unsafe` in
//! `logical-db-daemon`.
//!
//! foundationdb 0.11 starts its network only through `unsafe fn`s
//! (`boot`, `NetworkBuilder::boot`, `NetworkRunner::run`). This module wraps the
//! one call behind a safe fn so the daemon and every FDB journey boot the same
//! way, with the safety obligation stated once.

use foundationdb::{
    FdbResult,
    api::{FdbApiBuilder, NetworkAutoStop},
    options::NetworkOption,
};

/// Selects the API version, applies `options`, and starts the client network.
///
/// The returned guard stops the network when dropped. The caller binds it to a
/// local that outlives every database handle it opens and drops it explicitly
/// before the process exits.
///
/// # Errors
///
/// Returns the FDB error when the API version is refused, an option is refused,
/// or the network fails to start (including a second boot in one process).
pub fn boot_network(
    options: impl IntoIterator<Item = NetworkOption>,
) -> FdbResult<NetworkAutoStop> {
    let mut builder = FdbApiBuilder::default().build()?;
    for option in options {
        builder = builder.set_option(option)?;
    }
    // SAFETY: `NetworkBuilder::boot` requires that the returned `NetworkAutoStop`
    // is dropped before the program exits and never leaked. It is handed to the
    // caller, which keeps it in a local that outlives every database handle and
    // drops it explicitly; `mem_forget` is forbidden for this crate.
    unsafe { builder.boot() }
}
