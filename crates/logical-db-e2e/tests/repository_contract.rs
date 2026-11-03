//! Repository contracts checked against the live workspace (docs/adr/0006).
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
//! S33 S34 S35 S36 S37 S38 S39 S40 S41 S42 S43. Source URLs are in
//! `evidence/SOURCES.md`.
//!
//! The four-domain ownership and allowed-edge contract, judged against the
//! real `cargo metadata` graph. CI routing and the clean-room evidence have
//! their own gates (`ci_routing`, `clean_room_evidence`).

#[path = "support/workspace.rs"]
mod workspace;

#[path = "repository_contract/domain_contract.rs"]
mod domain_contract;
