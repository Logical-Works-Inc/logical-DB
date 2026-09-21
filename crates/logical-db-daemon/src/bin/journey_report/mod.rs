//! Emits the fdb-ambiguity journey's PASS lines.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.
//!
//! This path is the PRINTS-exempt surface for the journey binary: the journey
//! logic in `src/ambiguity.rs` stays pure and returns report strings, and this
//! module is the only place that writes them to stdout for CI to parse.

/// Writes one journey report line to stdout for the CI PASS parser.
pub fn emit(line: &str) {
    println!("{line}");
}
