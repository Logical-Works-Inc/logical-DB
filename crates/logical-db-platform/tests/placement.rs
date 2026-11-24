//! Replica placement contract: one named file name, one composed path.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
//! S33 S34 S35 S36 S37 S38 S39. Source URLs are in `evidence/SOURCES.md`.
//!
//! The replica file name is a persisted-format identity: a host that finds an
//! older `replica-v*.redb` beside it must see a different name, never a silent
//! reuse. One constant owns the name so the path and every reader agree.

use logical_db_platform::database_path_in;

/// The composed path ends in one owned, versioned replica file name, whatever
/// the data root or the application.
#[test]
fn the_replica_path_ends_in_the_owned_file_name() -> Result<(), Box<dyn std::error::Error>> {
    let path = database_path_in("/data", "com.example.notes")?;
    let name = path
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .ok_or("the placement path must end in a UTF-8 file name")?;
    assert!(
        name.starts_with("replica-v") && name.ends_with(".redb"),
        "the replica file name carries its format version: {name}"
    );
    let elsewhere = database_path_in("/other-root", "org.example.mail")?;
    assert_eq!(
        elsewhere.file_name(),
        path.file_name(),
        "the placement path must end in the one owned replica file name"
    );
    Ok(())
}
