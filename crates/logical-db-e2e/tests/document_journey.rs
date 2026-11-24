//! The document journey binary exercises every editor and reader operation.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

#![forbid(unsafe_code)]

use std::process::Command;

/// The journey exits zero and reports its receipt line.
#[test]
fn document_journey_edits_reads_and_resolves_conflicts() -> Result<(), std::io::Error> {
    let output = Command::new(env!("CARGO_BIN_EXE_document_journey")).output()?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "document journey failed: {:?} {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        stdout.contains(
            "PASS document-journey text=hello there tags=2 map_conflicts=2 index_conflicts=2"
        ),
        "missing receipt line in {stdout:?}"
    );
    Ok(())
}
