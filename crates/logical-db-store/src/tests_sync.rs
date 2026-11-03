//! Sync unit tests; split from the store root.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use crate::codec::encode_outbox;
use crate::error::StoreError;
use crate::keys::{DATA, key_checkpoint, key_outbox, key_outbox_ack};
use crate::remote::InstallMode;
use crate::store::EmbeddedStore;
use crate::test_support::{change, commit_live, insert_legacy_value, test_path};
use crate::types::{ChangeEvent, CommitOutcome, CommittedOperation, LocalChange, LocalCommit};
use logical_db_model::{
    AccountId, ChangeDigest, DocumentId, DocumentRevision, OperationDigest, OperationId, PeerId,
    ReplicaId, Scope, SpaceId,
};
use std::{error::Error, fs};

/// Commits one expected-revision successor with no indexes and no tombstone.
///
/// The sync and store suites assert the same successor shape from different
/// fixtures, so the commit literal exists once instead of once per suite.
pub(crate) fn commit_secondary<'a>(
    store: &EmbeddedStore,
    change: LocalChange<'a>,
    expected_revision: Option<DocumentRevision>,
    snapshot: &'a [u8],
) -> Result<CommitOutcome, StoreError> {
    store.commit_local(LocalCommit::new(
        change,
        expected_revision,
        snapshot,
        &[],
        false,
    ))
}

#[test]
fn operation_lookup_rejects_a_corrupt_atomic_outbox_companion() -> Result<(), Box<dyn Error>> {
    let path = test_path("corrupt-outbox-companion");
    let store = EmbeddedStore::open(&path, ReplicaId::new())?;
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let document = DocumentId::new();
    let operation = OperationId::new();
    let change = change(scope, document, operation, 1, 2)?;
    store.commit_local(LocalCommit {
        change,
        expected_revision: None,
        snapshot: b"snapshot",
        indexes: &[],
        tombstone: false,
    })?;
    {
        let transaction = store.database.begin_write()?;
        {
            let mut table = transaction.open_table(DATA)?;
            table.insert(key_outbox(scope, 1).as_slice(), b"broken".as_slice())?;
        }
        transaction.commit()?;
    }
    assert!(
        matches!(
            store.operation(scope, operation),
            Err(StoreError::CorruptInternalValue)
        ),
        "a broken outbox companion must fail closed as corrupt"
    );

    let valid = encode_outbox(change);
    for (offset, replacement) in [
        (0_usize, DocumentId::new().as_uuid().as_bytes().to_vec()),
        (16_usize, OperationId::new().as_uuid().as_bytes().to_vec()),
        (32_usize, ChangeDigest::new([91; 32]).into_bytes().to_vec()),
    ] {
        let mut corrupted = valid;
        let end = offset.saturating_add(replacement.len());
        corrupted[offset..end].copy_from_slice(&replacement);
        let transaction = store.database.begin_write()?;
        {
            let mut table = transaction.open_table(DATA)?;
            table.insert(key_outbox(scope, 1).as_slice(), corrupted.as_slice())?;
        }
        transaction.commit()?;
        assert!(
            matches!(
                store.operation(scope, operation),
                Err(StoreError::CorruptInternalValue)
            ),
            "every torn outbox field must fail closed as corrupt"
        );
    }
    drop(store);
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn accessors_acknowledgement_and_change_feed_preserve_exact_durable_values()
-> Result<(), Box<dyn Error>> {
    let path = test_path("accessors-and-feed");
    let store = EmbeddedStore::open(&path, ReplicaId::new())?;
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let document = DocumentId::new();
    let peer = PeerId::new();
    let operation = OperationId::new();
    let first = change(scope, document, operation, 4, 5)?;
    assert_eq!(
        store.load_snapshot(scope, document)?,
        None,
        "an empty store must load no snapshot"
    );
    assert_eq!(
        store.load_change(scope, document, first.digest)?,
        None,
        "an empty store must load no change"
    );
    commit_live(&store, first, b"snapshot-one")?;
    assert_eq!(
        store.load_change(scope, document, first.digest)?,
        Some(b"verified-change".to_vec()),
        "the committed change bytes must be loadable"
    );
    assert_eq!(
        store.operation(scope, operation)?,
        Some(CommittedOperation {
            document,
            request: OperationDigest::new([4; 32]),
            digest: ChangeDigest::new([5; 32]),
            sequence: 1,
        }),
        "the operation lookup must return the committed record"
    );
    assert_eq!(
        store.changes_since(scope, 0, 10)?,
        vec![ChangeEvent::Document {
            sequence: 1,
            document,
            digest: first.digest,
        }],
        "the change feed must open with the committed event"
    );
    assert!(
        store.changes_since(scope, 1, 10)?.is_empty(),
        "a feed past the last sequence must be empty"
    );
    assert!(
        store.changes_since(scope, u64::MAX, 10)?.is_empty(),
        "a feed from the maximum sequence must be empty"
    );
    assert!(
        matches!(
            store.changes_since(scope, 0, 0),
            Err(StoreError::InvalidEventLimit(0))
        ),
        "a zero event limit must be refused"
    );
    assert!(
        matches!(
            store.acknowledge_outbox(scope, peer, 2),
            Err(StoreError::InvalidAcknowledgement {
                through: 2,
                highest: 1
            })
        ),
        "acknowledging past the highest sequence must be refused"
    );
    store.acknowledge_outbox(scope, peer, 1)?;
    store.acknowledge_outbox(scope, peer, 0)?;
    assert!(
        store.outbox_page(scope, 0, 10)?.is_empty(),
        "a full acknowledgement must drain the outbox"
    );

    let second = change(scope, document, OperationId::new(), 6, 7)?;
    commit_secondary(
        &store,
        second,
        Some(DocumentRevision::new(1)),
        b"snapshot-two",
    )?;
    assert_eq!(
        store.outbox_page(scope, 0, 10)?[0].sequence,
        2,
        "the second commit must occupy sequence two"
    );
    assert_eq!(
        store.changes_since(scope, 1, 10)?[0].sequence(),
        2,
        "the feed must resume at the second event"
    );
    assert!(
        matches!(
            store.outbox_page(scope, 0, 0),
            Err(StoreError::InvalidOutboxLimit(0))
        ),
        "a zero outbox limit must be refused"
    );
    drop(store);
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn global_outbox_acknowledgement_binds_exactly_one_upstream() -> Result<(), Box<dyn Error>> {
    let path = test_path("single-upstream");
    let store = EmbeddedStore::open(&path, ReplicaId::new())?;
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let configured = PeerId::new();
    let requested = PeerId::new();
    store.bind_upstream(scope, configured)?;
    store.bind_upstream(scope, configured)?;
    assert!(
        matches!(
            store.bind_upstream(scope, requested),
            Err(StoreError::UpstreamMismatch {
                configured: actual,
                requested: attempted,
            }) if actual == configured && attempted == requested
        ),
        "a second upstream identity must be refused"
    );
    assert!(
        matches!(
            store.acknowledge_outbox(scope, requested, 0),
            Err(StoreError::UpstreamMismatch { .. })
        ),
        "only the bound upstream may acknowledge"
    );
    assert!(
        matches!(
            store.install_remote_page(
                scope,
                requested,
                &[],
                &[],
                b"checkpoint",
                InstallMode::Remote
            ),
            Err(StoreError::UpstreamMismatch { .. })
        ),
        "only the bound upstream may install pages"
    );
    drop(store);
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn legacy_checkpoint_migrates_only_to_its_attributable_upstream() -> Result<(), Box<dyn Error>> {
    let path = test_path("legacy-single-upstream");
    let store = EmbeddedStore::open(&path, ReplicaId::new())?;
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let legacy = PeerId::new();
    insert_legacy_value(&store, &key_checkpoint(scope, legacy), b"legacy-checkpoint")?;
    assert!(
        matches!(
            store.bind_upstream(scope, PeerId::new()),
            Err(StoreError::UpstreamMismatch { configured, .. }) if configured == legacy
        ),
        "a foreign peer must not claim the attributable legacy checkpoint"
    );
    store.bind_upstream(scope, legacy)?;
    drop(store);
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn ambiguous_or_unattributed_legacy_sync_state_fails_closed() -> Result<(), Box<dyn Error>> {
    let ambiguous_path = test_path("legacy-ambiguous-upstream");
    let ambiguous = EmbeddedStore::open(&ambiguous_path, ReplicaId::new())?;
    let ambiguous_scope = Scope::new(AccountId::new(), SpaceId::new());
    for peer in [PeerId::new(), PeerId::new()] {
        insert_legacy_value(
            &ambiguous,
            &key_checkpoint(ambiguous_scope, peer),
            b"checkpoint",
        )?;
    }
    assert!(
        matches!(
            ambiguous.bind_upstream(ambiguous_scope, PeerId::new()),
            Err(StoreError::AmbiguousLegacyUpstream)
        ),
        "two legacy checkpoints must fail closed as ambiguous"
    );
    drop(ambiguous);
    fs::remove_file(ambiguous_path)?;

    let ack_path = test_path("legacy-unattributed-ack");
    let acknowledged = EmbeddedStore::open(&ack_path, ReplicaId::new())?;
    let acknowledged_scope = Scope::new(AccountId::new(), SpaceId::new());
    insert_legacy_value(
        &acknowledged,
        &key_outbox_ack(acknowledged_scope),
        &1_u64.to_be_bytes(),
    )?;
    assert!(
        matches!(
            acknowledged.bind_upstream(acknowledged_scope, PeerId::new()),
            Err(StoreError::UnattributedLegacyAcknowledgement)
        ),
        "an unattributed legacy acknowledgement must fail closed"
    );
    drop(acknowledged);
    fs::remove_file(ack_path)?;

    let corrupt_path = test_path("legacy-corrupt-checkpoint");
    let corrupt = EmbeddedStore::open(&corrupt_path, ReplicaId::new())?;
    let corrupt_scope = Scope::new(AccountId::new(), SpaceId::new());
    let corrupt_peer = PeerId::new();
    insert_legacy_value(&corrupt, &key_checkpoint(corrupt_scope, corrupt_peer), b"")?;
    assert!(
        matches!(
            corrupt.bind_upstream(corrupt_scope, corrupt_peer),
            Err(StoreError::CorruptInternalValue)
        ),
        "an empty legacy checkpoint must fail closed as corrupt"
    );
    drop(corrupt);
    fs::remove_file(corrupt_path)?;
    Ok(())
}
