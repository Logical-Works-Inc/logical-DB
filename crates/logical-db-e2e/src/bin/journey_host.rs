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

use std::io::{self, Write};

use logical_db_e2e::support::{Allow, BLACK_BOX_TOKEN, TestAuthentication, serve_journey};

/// Boots the async runtime and serves the journey composition on stdio.
fn main() -> Result<(), io::Error> {
    lgwks_bot::rt::runtime::Builder::new()
        .build()?
        .block_on(run())
}

/// Serves the journey composition until the orchestrator terminates it.
async fn run() -> Result<(), io::Error> {
    // The loopback host admits every justified request of the external tape.
    let authentication = TestAuthentication::new(
        BLACK_BOX_TOKEN,
        Allow::when(|_scope, _effect, _action| true),
    );
    let (address, _server) = serve_journey(authentication).await?;
    let ready = format!("READY {address}\n");
    io::stdout().lock().write_all(ready.as_bytes())?;
    io::stdout().lock().flush()?;
    // Serve until the orchestrator terminates this process. A park, not a
    // poll: the composition is push-driven by the listener.
    std::future::pending::<()>().await;
    Ok(())
}
