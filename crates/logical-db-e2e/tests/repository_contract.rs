//! Repository contracts checked against the live workspace (docs/adr/0006).
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
//! S33 S34 S35 S36 S37 S38 S39 S40 S41 S42 S43. Source URLs are in
//! `evidence/SOURCES.md`.
//!
//! One test binary, so the workspace readers are built once: the four-domain
//! ownership and allowed-edge contract, the clean-room rewrite evidence, and
//! the self-hosted CI routing.

#[path = "support/workspace.rs"]
mod workspace;

#[path = "repository_contract/ci_routing.rs"]
mod ci_routing;
#[path = "repository_contract/clean_room.rs"]
mod clean_room;
#[path = "repository_contract/domain_contract.rs"]
mod domain_contract;
#[path = "repository_contract/sim_routing.rs"]
mod sim_routing;
