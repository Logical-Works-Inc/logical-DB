//! Retained dependency-register contract: every authored external edge is owned.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.
//!
//! The same `check_dependencies` verdict that the estate runs as its first CI
//! lane runs here, against this repository's committed register. A new crate,
//! an unowned edge, a drifted requirement, or a stale approval fails this test
//! before any behavioural test runs.

/// The repository root is two directories above the e2e crate manifest.
fn repository_root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("e2e crate lives two levels below the workspace root")
        .to_path_buf()
}

#[test]
fn every_authored_external_edge_is_owned() {
    let root = repository_root();
    let (contract, refusals) =
        lgwks_deps::check_dependencies(&root).expect("dependency register is readable");
    assert!(
        refusals.is_empty(),
        "{} semantic approvals do not cover every authored edge: {refusals:#?}",
        contract.entries.len()
    );
    assert!(
        contract.enforce,
        "the register stands enforcement down; that is a reviewable diff, not a default"
    );
}
