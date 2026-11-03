//! Projection views journey: hybrid queries and durable snapshots (#119 row 4).
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.
//!
//! Thin black-box wrappers: the journeys live in
//! [`logical_db_e2e::views`], driven through the shipped public API.

/// Hybrid queries, durable snapshots, and tenant isolation agree end to end.
#[test]
fn projection_views_agree_across_live_and_snapshot_paths() -> Result<(), Box<dyn std::error::Error>>
{
    logical_db_e2e::views::projection_views_agree_across_live_and_snapshot_paths()
}

/// Sealed envelopes survive a backend round-trip and restore plus tail replay
/// converges across engines; any refusal falls back to genesis with a reason.
#[test]
fn snapshot_envelope_round_trip_restores_across_engines() -> Result<(), Box<dyn std::error::Error>>
{
    logical_db_e2e::views::snapshot_envelope_round_trip_restores_across_engines()
}
