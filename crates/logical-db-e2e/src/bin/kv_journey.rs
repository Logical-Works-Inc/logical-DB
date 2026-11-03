//! Key-value cache journey binary.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.
//!
//! Runs [`logical_db_e2e::kv::kv_cache_journey`] as a shipped binary with its
//! replicas under the directory given as the first argument, and writes one
//! receipt line per step to the path given as the second. Receipts go to a
//! file, never to stdout, so the binary stays under the workspace print lints.

#![forbid(unsafe_code)]

/// Runs the cache journey and records its receipt.
fn main() -> Result<(), logical_db_e2e::kv::KvJourneyError> {
    let mut args = std::env::args().skip(1);
    let (Some(dir), Some(receipt)) = (args.next(), args.next()) else {
        return Err(logical_db_e2e::kv::KvJourneyError::Diverged(
            "usage: kv_journey <replica-dir> <receipt-path>",
        ));
    };
    let lines = logical_db_e2e::kv::kv_cache_journey(std::path::Path::new(&dir))?;
    std::fs::write(&receipt, lines.join("\n"))?;
    Ok(())
}
