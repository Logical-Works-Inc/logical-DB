//! README installation-contract regression for the public embedded facade.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
//! S33 S34 S35 S36 S37 S38 S39 S40 S41 S42 S43. Source URLs are in
//! `evidence/SOURCES.md`.

#![forbid(unsafe_code)]

const README: &str = include_str!("../../../README.md");
const GIT_DEPENDENCY_PREFIX: &str =
    "logical-db = { git = \"https://github.com/srinji-kaggss/logical-DB\", rev = \"";
const JOURNEY: &str = "experience/journeys/embedded-git-consumer-v1.yaml";
const REGISTRY_BOUNDARY: &str = "Crates.io installation is not supported yet:";

/// The README pins an immutable Git rev and states the crates.io boundary.
#[test]
fn documented_embedded_installation_is_immutable_and_explicit()
-> Result<(), Box<dyn std::error::Error>> {
    let dependency = README
        .find(GIT_DEPENDENCY_PREFIX)
        .map(|offset| &README[offset.saturating_add(GIT_DEPENDENCY_PREFIX.len())..])
        .ok_or("README must name the public Git-source dependency")?;
    let revision = dependency
        .split_once('"')
        .map(|(revision, _)| revision)
        .ok_or("README Git dependency must close its revision string")?;

    assert_eq!(revision.len(), 40, "README must pin a full Git revision");
    assert!(
        revision
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "README revision must be lowercase hexadecimal"
    );
    assert!(
        !README.contains("branch = \"main\""),
        "README must not replace the immutable revision with a moving branch"
    );
    assert!(
        README.contains(JOURNEY),
        "README must link the executable journey profile"
    );
    assert!(
        README.contains(REGISTRY_BOUNDARY),
        "README must state that registry installation is not yet supported"
    );
    Ok(())
}
