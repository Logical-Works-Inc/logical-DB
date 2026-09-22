//! Loopback journey host for external-consumer sync evidence.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.
//!
//! This binary binds the reviewed journey composition to an ephemeral loopback
//! port and prints `READY <address>` once it is serving. It exists so that an
//! *external* consumer process — the tape in `logical-db-tape` — can prove the
//! two-replica HTTP convergence journey as a pure client, without constructing
//! a server itself and without importing any internal crate.
//!
//! It is a test composition: it holds no durable state across restarts and
//! makes no availability claim. `scripts/external-tape.sh` owns its lifetime.

#![forbid(unsafe_code)]
#![expect(
    clippy::print_stdout,
    reason = "src/bin/ is the PRINTS-exempt binary surface per the execution contract"
)]

use std::{error::Error, sync::Arc};

use logical_db_e2e::support::{MemoryServiceStore, serve_journey};

/// Boots the async runtime and serves the journey composition on stdio.
fn main() -> Result<(), Box<dyn Error>> {
    lgwks_bot::Runtime::new()?.block_on(run())
}

/// Serves the journey composition until the orchestrator terminates it.
async fn run() -> Result<(), Box<dyn Error>> {
    let store = Arc::new(MemoryServiceStore::default());
    let (address, _server) = serve_journey(store).await?;
    println!("READY {address}");
    // Serve until the orchestrator terminates this process. A park, not a
    // poll: the composition is push-driven by the listener.
    std::future::pending::<()>().await;
    Ok(())
}
