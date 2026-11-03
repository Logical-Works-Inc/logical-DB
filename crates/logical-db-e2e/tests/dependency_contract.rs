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
fn repository_root() -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
    Ok(std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .ok_or("e2e crate lives two levels below the workspace root")?
        .to_path_buf())
}

/// The gate's verdict, or the capture-cleanup failure the collection carries.
///
/// A cleanup that could not be confirmed is a test failure here, never a
/// silently dropped warning.
fn settled<T>(
    collected: lgwks_deps::metadata::Collected<T>,
) -> Result<T, Box<dyn std::error::Error>> {
    let (value, unresolved) = collected.into_parts();
    match unresolved {
        Some(cleanup) => Err(cleanup.into()),
        None => Ok(value),
    }
}

/// Every authored external edge is owned by an approved register entry.
#[test]
fn every_authored_external_edge_is_owned() -> Result<(), Box<dyn std::error::Error>> {
    let root = repository_root()?;
    let (contract, refusals) = settled(lgwks_deps::check_dependencies(&root)?)?;
    assert!(
        refusals.is_empty(),
        "{} semantic approvals do not cover every authored edge: {refusals:#?}",
        contract.entry_count()
    );
    assert!(
        contract.enforce,
        "the register stands enforcement down; that is a reviewable diff, not a default"
    );
    Ok(())
}

/// The committed register, read once so every fixture mutates a real document.
fn committed_register() -> Result<&'static str, Box<dyn std::error::Error>> {
    static REGISTER: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    if let Some(loaded) = REGISTER.get() {
        return Ok(loaded);
    }
    let text = std::fs::read_to_string(repository_root()?.join("contract/APPROVED.toml"))?;
    Ok(REGISTER.get_or_init(|| text))
}

/// Rewrites one `[[approved]]` block for `krate`.
fn mutate_block(
    register: &str,
    krate: &str,
    edit: impl Fn(&str) -> String,
) -> Result<String, Box<dyn std::error::Error>> {
    let needle = format!("crate = \"{krate}\"\n");
    let (head, rest) = register
        .split_once("[[approved]]")
        .ok_or("a register has approvals")?;
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
    Ok(format!("{head}{}", kept.join("")))
}

/// Drops the whole approval for `krate`.
fn drop_block(register: &str, krate: &str) -> Result<String, Box<dyn std::error::Error>> {
    mutate_block(register, krate, |_| String::new())
}

/// Rewrites each `field = ...` line so the block pins `value`.
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
) -> Result<(lgwks_deps::contract::Contract, Vec<lgwks_deps::Refusal>), Box<dyn std::error::Error>>
{
    static COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let id = COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let path =
        std::env::temp_dir().join(format!("logical-db-gate-{}-{id}.toml", std::process::id()));
    std::fs::write(&path, register)?;
    let outcome = lgwks_deps::check_dependencies_against(&repository_root()?, &path);
    std::fs::remove_file(&path)?;
    settled(outcome?)
}

/// The refusals the gate reports for `register`, or the gate failure itself.
fn refusals_of(register: &str) -> Result<Vec<lgwks_deps::Refusal>, Box<dyn std::error::Error>> {
    Ok(gate_with(register)?.1)
}

/// The gate must refuse an authored edge whose approval was withdrawn.
#[test]
fn withdrawing_an_approval_surfaces_an_unregistered_edge() -> Result<(), Box<dyn std::error::Error>>
{
    let refusals = refusals_of(&drop_block(committed_register()?, "uuid")?)?;
    assert!(
        refusals.iter().any(|refusal| matches!(
            refusal,
            lgwks_deps::Refusal::UnregisteredEdge { krate, .. } if krate == "uuid"
        )),
        "a withdrawn approval left no refusal: {refusals:?}"
    );
    Ok(())
}

/// An approval names its consumers exactly; a crate outside that list is a
/// refusal, not a silent widening.
#[test]
fn a_consumer_outside_the_approval_list_is_refused() -> Result<(), Box<dyn std::error::Error>> {
    let mutated = mutate_block(committed_register()?, "uuid", |block| {
        replace_field(block, "allowed_consumers", "\"logical-db-model\"")
    })?;
    let refusals = refusals_of(&mutated)?;
    assert!(
        refusals.iter().any(|refusal| matches!(
            refusal,
            lgwks_deps::Refusal::ConsumerNotAllowed { krate, .. } if krate == "uuid"
        )),
        "an unlisted consumer passed the gate: {refusals:?}"
    );
    Ok(())
}

/// Approving `normal` must not authorise a `dev` edge for the same crate.
#[test]
fn a_dev_edge_under_a_normal_only_approval_is_refused() -> Result<(), Box<dyn std::error::Error>> {
    let mutated = mutate_block(committed_register()?, "ciborium", |block| {
        replace_field(block, "allowed_kinds", "\"normal\"")
    })?;
    let refusals = refusals_of(&mutated)?;
    assert!(
        refusals.iter().any(|refusal| matches!(
            refusal,
            lgwks_deps::Refusal::KindNotAllowed { krate, kind, .. }
                if krate == "ciborium" && kind == "dev"
        )),
        "kind drift passed the gate: {refusals:?}"
    );
    Ok(())
}

/// The approved requirement is exact; a bumped manifest edge is a refusal.
#[test]
fn a_requirement_that_differs_from_the_approval_is_refused()
-> Result<(), Box<dyn std::error::Error>> {
    let mutated = mutate_block(committed_register()?, "uuid", |block| {
        replace_field(block, "version", "\"=1.25.0\"")
    })?;
    let refusals = refusals_of(&mutated)?;
    assert!(
        refusals.iter().any(|refusal| matches!(
            refusal,
            lgwks_deps::Refusal::RequirementDrift { krate, approved, .. }
                if krate == "uuid" && approved == "=1.25.0"
        )),
        "requirement drift passed the gate: {refusals:?}"
    );
    Ok(())
}

/// Changing a registry edge to a git edge is a source-class refusal.
#[test]
fn a_source_class_change_is_refused() -> Result<(), Box<dyn std::error::Error>> {
    let mutated = mutate_block(committed_register()?, "uuid", |block| {
        replace_field(block, "source", "\"git\"")
    })?;
    let refusals = refusals_of(&mutated)?;
    assert!(
        refusals.iter().any(|refusal| matches!(
            refusal,
            lgwks_deps::Refusal::SourceDrift { krate, approved, .. }
                if krate == "uuid" && approved == "git"
        )),
        "source-class drift passed the gate: {refusals:?}"
    );
    Ok(())
}

/// An approval with no authored edge is stale authority and must be refused.
#[test]
fn an_approval_with_no_authored_edge_is_stale() -> Result<(), Box<dyn std::error::Error>> {
    let mutated = format!(
        "{}\n[[approved]]\ncrate = \"logical-db-gate-dropped-uuid\"\ntier = \"boundary\"\nversion = \"=1.26.0\"\nowner = \"logical-db-model\"\ncapability = \"identity.unused\"\nsource = \"registry\"\nlicense = \"MIT\"\nallowed_consumers = \"logical-db-model\"\nallowed_kinds = \"normal\"\nreason = \"Fixture proving stale authority is refused.\"\napproved_by = \"Director\"\napproved_on = \"2026-09-14\"\nreview = \"https://github.com/srinji-kaggss/logical-DB/pull/47\"\n",
        committed_register()?
    );
    let refusals = refusals_of(&mutated)?;
    assert!(
        refusals.iter().any(|refusal| matches!(
            refusal,
            lgwks_deps::Refusal::UnusedApproval { krate, .. }
                if krate == "logical-db-gate-dropped-uuid"
        )),
        "stale authority passed the gate: {refusals:?}"
    );
    Ok(())
}

/// A malformed register fails the entry point loudly; it never degrades to
/// "no edges to check".
#[test]
fn a_malformed_register_fails_the_gate() -> Result<(), Box<dyn std::error::Error>> {
    let Err(outcome) = gate_with("[[approved\ncrate = ") else {
        return Err("a malformed register must not pass the gate".into());
    };
    assert!(
        matches!(
            outcome.downcast_ref::<lgwks_deps::GateError>(),
            Some(lgwks_deps::GateError::Contract(_))
        ),
        "a malformed register must be a contract error, got {outcome}"
    );
    Ok(())
}

/// Standing enforcement down is visible to the caller, never implied.
#[test]
fn an_enforcement_stand_down_is_reported_not_silently_honoured()
-> Result<(), Box<dyn std::error::Error>> {
    let mutated = committed_register()?.replace("enforce = true", "enforce = false");
    let (contract, refusals) = gate_with(&mutated)?;
    assert!(!contract.enforce, "the stand-down was not read back");
    assert!(
        refusals.is_empty(),
        "the fixture register must otherwise be clean: {refusals:?}"
    );
    Ok(())
}

/// Scope of this gate, stated as an assertion: `source` is a CLASS, so the
/// exact git URL and revision are pinned by the lockfile and the `--locked`
/// plus source-policy lanes, not by the register. A drift in the pinned rev
/// must therefore be visible in `Cargo.lock`.
#[test]
fn an_estate_registry_edge_is_version_pinned_by_the_lockfile()
-> Result<(), Box<dyn std::error::Error>> {
    // The pinned version is read from the manifest, never restated here: an
    // estate upgrade moves one line in `Cargo.toml`, and this gate then demands
    // the lockfile resolve exactly that release from the registry.
    let manifest = std::fs::read_to_string(repository_root()?.join("Cargo.toml"))?;
    let declared = manifest
        .lines()
        .find(|line| line.starts_with("lgwks_std = "))
        .ok_or("the estate edge is declared")?;
    assert!(
        !declared.contains("git = "),
        "the workspace estate edge must not fall back to a git pin: {declared}"
    );
    let requirement = declared
        .split_once("version = \"")
        .and_then(|(_, rest)| rest.split_once('"'))
        .map(|(version, _)| version)
        .ok_or("the estate edge declares a registry version")?;
    assert!(
        requirement.split('.').count() == 3
            && requirement
                .split('.')
                .all(|part| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit())),
        "the estate edge names one exact release: {requirement}"
    );
    let lock = std::fs::read_to_string(repository_root()?.join("Cargo.lock"))?;
    let pinned = format!("version = \"{requirement}\"");
    let estate = lock
        .split("[[package]]")
        .find(|block| {
            block.contains("name = \"lgwks_std\"")
                && block.lines().any(|line| line == pinned)
                && block
                    .contains("source = \"registry+https://github.com/rust-lang/crates.io-index\"")
        })
        .ok_or("the lockfile resolves the declared lgwks_std release from the registry")?;
    assert!(
        estate
            .lines()
            .filter(|line| line.starts_with("version = "))
            .count()
            == 1,
        "one resolved version on the registry estate edge: {estate}"
    );
    assert!(
        lock.matches("name = \"lgwks_std\"").count() == 1,
        "the lockfile carries exactly one lgwks_std"
    );
    assert!(
        !declared.contains("rev = "),
        "a registry edge is versioned, not revision-pinned: {declared}"
    );
    assert!(
        committed_register()?.contains("source = \"registry\""),
        "the register documents registry as the estate edge class"
    );
    Ok(())
}
