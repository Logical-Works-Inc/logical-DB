//! Projection-views journey binary: hybrid queries and durable snapshots.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.
//!
//! Runs the projection-view journeys from [`logical_db_e2e::views`] as a
//! shipped binary through the public facade API and writes one receipt line
//! per journey to the path given as the first argument. The e2e suite
//! asserts the same journeys; this binary is the runnable form of that
//! proof. Receipts go to a file, never to stdout, so the binary stays
//! under the workspace print lints without an exemption.

#![forbid(unsafe_code)]

/// Runs both projection-view journeys and records their receipts.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .ok_or("usage: projection_journey <receipt-path>")?;
    logical_db_e2e::views::projection_views_agree_across_live_and_snapshot_paths()?;
    logical_db_e2e::views::snapshot_envelope_round_trip_restores_across_engines()?;
    let receipt = "projection_views_agree_across_live_and_snapshot_paths: pass\nsnapshot_envelope_round_trip_restores_across_engines: pass\n";
    std::fs::write(&path, receipt)?;
    Ok(())
}
