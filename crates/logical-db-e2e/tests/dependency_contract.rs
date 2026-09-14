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

/// The committed register, read once so every fixture mutates a real document.
fn committed_register() -> &'static str {
    static REGISTER: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    REGISTER.get_or_init(|| {
        std::fs::read_to_string(repository_root().join("contract/APPROVED.toml"))
            .expect("committed register is readable")
    })
}

/// Rewrites one `[[approved]]` block for `krate`.
fn mutate_block(register: &str, krate: &str, edit: impl Fn(&str) -> String) -> String {
    let needle = format!("crate = \"{krate}\"\n");
    let (head, rest) = register
        .split_once("[[approved]]")
        .expect("a register has approvals");
    let mut blocks = Vec::new();
    let mut current = String::from("[[approved]]\n");
    for line in rest.lines() {
        if line.trim() == "[[approved]]" {
            blocks.push(std::mem::take(&mut current));
            current = String::from("[[approved]]\n");
        } else {
            current.push_str(line);
            current.push('\n');
        }
    }
    blocks.push(current);
    let kept: Vec<String> = blocks
        .into_iter()
        .map(|block| {
            if block.contains(&needle) {
                edit(&block)
            } else {
                block
            }
        })
        .filter(|block| !block.contains("crate = \"logical-db-gate-dropped-uuid\""))
        .collect();
    format!("{head}{}", kept.join(""))
}

/// Drops the whole approval for `krate`.
fn drop_block(register: &str, krate: &str) -> String {
    mutate_block(register, krate, |_| String::new())
}

fn replace_field(block: &str, field: &str, value: &str) -> String {
    let prefix = format!("{field} = ");
    block
        .lines()
        .map(|line| {
            if line.starts_with(&prefix) {
                format!("{field} = {value}\n")
            } else {
                format!("{line}\n")
            }
        })
        .collect()
}

/// Runs the real gate entry point against a mutated copy of the register.
fn gate_with(
    register: &str,
) -> Result<(lgwks_deps::contract::Contract, Vec<lgwks_deps::Refusal>), lgwks_deps::GateError> {
    static COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let id = COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let path =
        std::env::temp_dir().join(format!("logical-db-gate-{}-{id}.toml", std::process::id()));
    std::fs::write(&path, register).expect("mutated register is writable");
    let outcome = lgwks_deps::check_dependencies_against(&repository_root(), &path);
    std::fs::remove_file(&path).expect("mutated register is removable");
    outcome
}

fn refusals_of(register: &str) -> Vec<lgwks_deps::Refusal> {
    gate_with(register)
        .expect("the gate accepts a well-formed register")
        .1
}

/// The gate must refuse an authored edge whose approval was withdrawn.
#[test]
fn withdrawing_an_approval_surfaces_an_unregistered_edge() {
    let refusals = refusals_of(&drop_block(committed_register(), "uuid"));
    assert!(
        refusals.iter().any(|refusal| matches!(
            refusal,
            lgwks_deps::Refusal::UnregisteredEdge { krate, .. } if krate == "uuid"
        )),
        "a withdrawn approval left no refusal: {refusals:?}"
    );
}

/// An approval names its consumers exactly; a crate outside that list is a
/// refusal, not a silent widening.
#[test]
fn a_consumer_outside_the_approval_list_is_refused() {
    let mutated = mutate_block(committed_register(), "uuid", |block| {
        replace_field(block, "allowed_consumers", "\"logical-db-model\"")
    });
    let refusals = refusals_of(&mutated);
    assert!(
        refusals.iter().any(|refusal| matches!(
            refusal,
            lgwks_deps::Refusal::ConsumerNotAllowed { krate, .. } if krate == "uuid"
        )),
        "an unlisted consumer passed the gate: {refusals:?}"
    );
}

/// Approving `normal` must not authorise a `dev` edge for the same crate.
#[test]
fn a_dev_edge_under_a_normal_only_approval_is_refused() {
    let mutated = mutate_block(committed_register(), "ciborium", |block| {
        replace_field(block, "allowed_kinds", "\"normal\"")
    });
    let refusals = refusals_of(&mutated);
    assert!(
        refusals.iter().any(|refusal| matches!(
            refusal,
            lgwks_deps::Refusal::KindNotAllowed { krate, kind, .. }
                if krate == "ciborium" && kind == "dev"
        )),
        "kind drift passed the gate: {refusals:?}"
    );
}

/// The approved requirement is exact; a bumped manifest edge is a refusal.
#[test]
fn a_requirement_that_differs_from_the_approval_is_refused() {
    let mutated = mutate_block(committed_register(), "uuid", |block| {
        replace_field(block, "version", "\"=1.25.0\"")
    });
    let refusals = refusals_of(&mutated);
    assert!(
        refusals.iter().any(|refusal| matches!(
            refusal,
            lgwks_deps::Refusal::RequirementDrift { krate, approved, .. }
                if krate == "uuid" && approved == "=1.25.0"
        )),
        "requirement drift passed the gate: {refusals:?}"
    );
}

/// Changing a registry edge to a git edge is a source-class refusal.
#[test]
fn a_source_class_change_is_refused() {
    let mutated = mutate_block(committed_register(), "uuid", |block| {
        replace_field(block, "source", "\"git\"")
    });
    let refusals = refusals_of(&mutated);
    assert!(
        refusals.iter().any(|refusal| matches!(
            refusal,
            lgwks_deps::Refusal::SourceDrift { krate, approved, .. }
                if krate == "uuid" && approved == "git"
        )),
        "source-class drift passed the gate: {refusals:?}"
    );
}

/// An approval with no authored edge is stale authority and must be refused.
#[test]
fn an_approval_with_no_authored_edge_is_stale() {
    let mutated = format!(
        "{}\n[[approved]]\ncrate = \"logical-db-gate-dropped-uuid\"\ntier = \"boundary\"\nversion = \"=1.26.0\"\nowner = \"logical-db-model\"\ncapability = \"identity.unused\"\nsource = \"registry\"\nallowed_consumers = \"logical-db-model\"\nallowed_kinds = \"normal\"\nreason = \"Fixture proving stale authority is refused.\"\napproved_by = \"Director\"\napproved_on = \"2026-09-14\"\nreview = \"https://github.com/srinji-kaggss/logical-DB/pull/47\"\n",
        committed_register()
    );
    let refusals = refusals_of(&mutated);
    assert!(
        refusals.iter().any(|refusal| matches!(
            refusal,
            lgwks_deps::Refusal::UnusedApproval { krate, .. }
                if krate == "logical-db-gate-dropped-uuid"
        )),
        "stale authority passed the gate: {refusals:?}"
    );
}

/// A malformed register fails the entry point loudly; it never degrades to
/// "no edges to check".
#[test]
fn a_malformed_register_fails_the_gate() {
    let outcome = gate_with("[[approved\ncrate = ");
    assert!(
        matches!(outcome, Err(lgwks_deps::GateError::Contract(_))),
        "a malformed register must be a contract error, got {outcome:?}"
    );
}

/// Standing enforcement down is visible to the caller, never implied.
#[test]
fn an_enforcement_stand_down_is_reported_not_silently_honoured() {
    let mutated = committed_register().replace("enforce = true", "enforce = false");
    let (contract, refusals) = gate_with(&mutated).expect("register still parses");
    assert!(!contract.enforce, "the stand-down was not read back");
    assert!(
        refusals.is_empty(),
        "the fixture register must otherwise be clean: {refusals:?}"
    );
}

/// Scope of this gate, stated as an assertion: `source` is a CLASS, so the
/// exact git URL and revision are pinned by the lockfile and the `--locked`
/// plus source-policy lanes, not by the register. A drift in the pinned rev
/// must therefore be visible in `Cargo.lock`.
#[test]
fn a_git_revision_is_pinned_by_the_lockfile_beyond_this_gates_class_check() {
    let lock = std::fs::read_to_string(repository_root().join("Cargo.lock")).expect("lockfile");
    let estate = lock
        .split("[[package]]")
        .find(|block| block.contains("name = \"lgwks_std\"") && block.contains("source = \"git+"))
        .expect("the git-pinned estate edge is in the lockfile");
    let source = estate
        .lines()
        .find(|line| line.starts_with("source = "))
        .expect("a git source line");
    assert!(
        source.contains("rev="),
        "the lockfile lost the pinned revision: {source}"
    );
    let manifest =
        std::fs::read_to_string(repository_root().join("Cargo.toml")).expect("workspace manifest");
    let pinned = manifest
        .lines()
        .find(|line| line.starts_with("lgwks_std = "))
        .expect("the estate edge is declared");
    let manifest_rev = pinned
        .split("rev = \"")
        .nth(1)
        .and_then(|rest| rest.split('"').next())
        .expect("a pinned rev in the manifest");
    assert!(
        source.contains(manifest_rev),
        "the lockfile revision no longer matches the manifest pin"
    );
    assert!(
        committed_register().contains("source = \"git\""),
        "the register documents git as a class, not a URL"
    );
}
