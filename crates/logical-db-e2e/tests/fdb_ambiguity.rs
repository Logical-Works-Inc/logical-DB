//! Live injection of FDB's post-commit `commit_unknown_result` path.
//!
//! Client buggify makes FDB commits return `commit_unknown_result` after the
//! write is durable. The journey proves, against a real cluster, that the
//! store recovers such a commit exactly once, that the HTTP action binding and
//! live baseline retention hold on the same store, and that every rebound
//! grant is refused. It needs `LOGICAL_DB_FDB_CLUSTER_FILE`; the FDB network is
//! booted through the daemon's one safe `boot_network`.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
//! S33 S34 S35 S36 S37 S38 S39 S42 S43. Source URLs are in `evidence/SOURCES.md`.

#![cfg(feature = "fdb")]

#[path = "fdb_ambiguity/binding.rs"]
mod binding;
#[path = "fdb_ambiguity/recovery.rs"]
mod recovery;
#[path = "fdb_ambiguity/retention.rs"]
mod retention;
#[path = "fdb_ambiguity/retries.rs"]
mod retries;

use std::{error::Error, io};

use logical_db_daemon::boot_network;
use logical_db_e2e::support::Grant;
use logical_db_model::{AccountId, Effect, OperationDigest, ReplicaId, Scope, SpaceId};
use logical_db_protocol::{PageAction, write_action};
use logical_db_service::{AcceptResult, ServiceStore};
use logical_db_store_fdb::{
    FdbServiceStore,
    foundationdb::{Database, options::NetworkOption},
    foundationdb_tuple::Subspace,
};

/// Probe attempts before buggify is declared to have missed the unknown path.
const PROBE_ATTEMPTS: u16 = 512;

/// Whether a probe outcome lets the probe continue to its next attempt.
const fn probe_result_is_nonterminal(result: AcceptResult) -> bool {
    matches!(result, AcceptResult::Accepted | AcceptResult::Duplicate)
}

/// The page action every verifier reads with: a fixed request digest.
fn page_action(
    scope: Scope,
    replica: ReplicaId,
    after: u64,
    max_changes: u16,
    max_bytes: u32,
) -> PageAction {
    PageAction::new(
        scope,
        replica,
        OperationDigest::new([0x70; 32]),
        after,
        max_changes,
        max_bytes,
    )
}

/// Runs every live proof and returns one `PASS` receipt per proof.
async fn run(cluster_file: &str) -> Result<Vec<String>, Box<dyn Error>> {
    // The one network boot of this process, with client buggify on so commits
    // can return `commit_unknown_result`. `network` is declared before every
    // database and store, so it is dropped after all of them.
    let network = boot_network([
        NetworkOption::ClientBuggifyEnable,
        NetworkOption::ClientBuggifySectionActivatedProbability(100),
        NetworkOption::ClientBuggifySectionFiredProbability(12),
    ])?;
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let root = Subspace::from_bytes(
        format!(
            "logical-db/ambiguity/{}/{}/",
            scope.account(),
            scope.space()
        )
        .into_bytes(),
    );
    let audit_store = FdbServiceStore::new(
        Database::new_compat(Some(cluster_file)).await?,
        root.clone(),
    )?;
    let http_store = FdbServiceStore::new(
        Database::new_compat(Some(cluster_file)).await?,
        root.clone(),
    )?;
    let store = FdbServiceStore::new(Database::new_compat(Some(cluster_file)).await?, root)?;
    let replica = ReplicaId::new();
    let binding_report =
        binding::verify_http_action_binding(http_store, &audit_store, scope, replica).await?;
    let retention_report = retention::verify_live_baseline_retention(&store, replica).await?;
    let mut reports = vec![binding_report, retention_report];

    let mut last_error = None;
    for attempt in 0..PROBE_ATTEMPTS {
        let change = recovery::ambiguity_probe_change(scope, replica, attempt)?;
        let frontier = retries::read_frontier_with_retry(&store, scope).await?;
        let grant = Grant::new(scope, Effect::Write, write_action(&change), frontier);
        match store.accept(&change, &grant).await {
            Ok(AcceptResult::RecoveredUnknownCommit) => {
                let observed =
                    recovery::complete_recovery_path(&store, scope, frontier, &change, &grant)
                        .await?;
                reports.push(format!(
                    "PASS fdb-commit-unknown-recovered attempt={attempt} frontier={observed} invocation=consumed page=bounded scope=isolated"
                ));
                drop(store);
                drop(audit_store);
                drop(network);
                return Ok(reports);
            }
            Ok(result) if probe_result_is_nonterminal(result) => {}
            Ok(other) => {
                return Err(
                    io::Error::other(format!("unexpected acceptance result: {other:?}")).into(),
                );
            }
            Err(error) => last_error = Some(error),
        }
    }
    Err(io::Error::other(format!(
        "FDB buggify did not exercise a post-commit unknown result; last error {last_error:?}"
    ))
    .into())
}

#[test]
/// A duplicate or plain accept keeps probing; every other outcome ends it.
fn ambiguity_probe_continues_after_durable_duplicate() {
    assert!(
        probe_result_is_nonterminal(AcceptResult::Accepted),
        "a plain accept keeps probing"
    );
    assert!(
        probe_result_is_nonterminal(AcceptResult::Duplicate),
        "a durable duplicate keeps probing"
    );
    assert!(
        !probe_result_is_nonterminal(AcceptResult::RecoveredUnknownCommit),
        "a recovered unknown commit ends the probe"
    );
    assert!(
        !probe_result_is_nonterminal(AcceptResult::Conflict),
        "a conflict ends the probe"
    );
}

#[test]
/// The page action digest binds the scope, replica, position and both limits.
fn page_fixture_identity_binds_scope_position_and_limits() {
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let replica = ReplicaId::new();
    let baseline = page_action(scope, replica, 0, 1, 1024).digest();
    let variants = [
        page_action(scope, ReplicaId::new(), 0, 1, 1024),
        page_action(scope, replica, 1, 1, 1024),
        page_action(scope, replica, 0, 2, 1024),
        page_action(scope, replica, 0, 1, 1025),
        page_action(
            Scope::new(AccountId::new(), SpaceId::new()),
            replica,
            0,
            1,
            1024,
        ),
    ];
    for variant in variants {
        assert_ne!(
            baseline,
            variant.digest(),
            "every page field is bound into the digest: {variant:?}"
        );
    }
}

#[test]
/// Against a live cluster: unknown-commit recovery, action binding and
/// baseline retention all hold, each reported as one `PASS` receipt.
fn live_fdb_recovers_unknown_commit_and_preserves_all_axes() -> Result<(), Box<dyn Error>> {
    let Some(cluster_file) = std::env::var_os("LOGICAL_DB_FDB_CLUSTER_FILE") else {
        return Ok(());
    };
    let cluster_file = cluster_file
        .into_string()
        .map_err(|raw| io::Error::other(format!("cluster file path is not UTF-8: {raw:?}")))?;
    let reports = lgwks_bot::rt::runtime::Builder::new()
        .worker_threads(std::num::NonZeroUsize::new(2))
        .build()?
        .block_on(run(&cluster_file))?;
    assert_eq!(reports.len(), 3, "one receipt per live proof: {reports:?}");
    assert!(
        reports.iter().all(|report| report.starts_with("PASS ")),
        "every live proof passed: {reports:?}"
    );
    Ok(())
}
