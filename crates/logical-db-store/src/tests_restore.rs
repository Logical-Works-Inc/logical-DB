//! Restore unit tests; split from the store root.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use crate::error::StoreError;
use crate::export::restore_operation;
use crate::remote::InstallMode;
use crate::store::EmbeddedStore;
use crate::test_support::{TestError, change, test_path};
use crate::types::{
    IndexHit, LocalChange, LocalCommit, RemoteChange, RemoteDocument, RestoreCursor,
};
use logical_db_model::{
    AccountId, ChangeDigest, DocumentId, DocumentRevision, IndexEntry, IndexId, IndexValue,
    OperationId, PeerId, ReplicaId, Scope, SpaceId,
};
use std::{error::Error, fs};

/// Parameters for one live remote page install in the tombstone journey.
///
/// The journey needs the same shape three times with different bytes; a
/// struct keeps the helper under the argument ceiling while the shared shape
/// lives here instead of in three repeated literals.
struct LiveSnapshot<'a> {
    scope: Scope,
    peer: PeerId,
    document: DocumentId,
    snapshot: &'a [u8],
    digest: ChangeDigest,
    bytes: &'a [u8],
    checkpoint: &'a [u8],
}

/// Installs one live remote page through the documented constructor path.
fn install_live_snapshot(store: &EmbeddedStore, page: LiveSnapshot<'_>) -> Result<(), StoreError> {
    let documents = [RemoteDocument::new(
        page.document,
        None,
        page.snapshot,
        &[],
        false,
    )];
    let changes = [RemoteChange::new(page.document, page.digest, page.bytes)];
    store.install_remote_page(
        page.scope,
        page.peer,
        &documents,
        &changes,
        page.checkpoint,
        InstallMode::Remote,
    )
}

#[test]
fn remote_page_atomically_materializes_checkpoint_index_event_and_duplicate()
-> Result<(), Box<dyn Error>> {
    let path = test_path("remote-page");
    let store = EmbeddedStore::open(&path, ReplicaId::new())?;
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let peer = PeerId::new();
    let document = DocumentId::new();
    let index = IndexId::new();
    let digest = ChangeDigest::new([8; 32]);
    let indexes = [IndexEntry::new(index, IndexValue::Text("remote".into()))];
    let changes = [RemoteChange {
        document,
        digest,
        bytes: b"remote-change",
    }];
    store.install_remote_page(
        scope,
        peer,
        &[RemoteDocument {
            document,
            expected_revision: None,
            snapshot: b"remote-snapshot",
            indexes: &indexes,
            tombstone: false,
        }],
        &changes,
        b"checkpoint-one",
        InstallMode::Remote,
    )?;
    assert_eq!(
        store.checkpoint(scope, peer)?,
        Some(b"checkpoint-one".to_vec()),
        "the checkpoint must materialize with the page"
    );
    assert_eq!(
        store.load_change(scope, document, digest)?,
        Some(b"remote-change".to_vec()),
        "the remote change bytes must be loadable"
    );
    assert_eq!(
        store.changes_since(scope, 0, 10)?.len(),
        1,
        "the page must emit one change event"
    );
    assert_eq!(
        store.query_index(scope, index, &IndexValue::Text("remote".into()), None, 10)?,
        vec![IndexHit { document }],
        "the page must materialize its index row"
    );
    store.install_remote_page(
        scope,
        peer,
        &[RemoteDocument {
            document,
            expected_revision: Some(DocumentRevision::new(1)),
            snapshot: b"remote-snapshot-two",
            indexes: &indexes,
            tombstone: false,
        }],
        &changes,
        b"checkpoint-two",
        InstallMode::Remote,
    )?;
    assert_eq!(
        store.changes_since(scope, 0, 10)?.len(),
        1,
        "a repeated operation must not emit a second event"
    );
    assert_eq!(
        store.checkpoint(scope, peer)?,
        Some(b"checkpoint-two".to_vec()),
        "the duplicate install must still advance the checkpoint"
    );

    drop(store);
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn remote_page_rejects_each_resource_and_document_coverage_violation() -> Result<(), TestError> {
    let path = test_path("remote-page-bounds");
    let store = EmbeddedStore::open(&path, ReplicaId::new())?;
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let peer = PeerId::new();
    let document = DocumentId::new();
    let digest = ChangeDigest::new([8; 32]);
    assert!(
        matches!(
            store.install_remote_page(scope, peer, &[], &[], b"", InstallMode::Remote),
            Err(StoreError::InvalidCheckpointSize(0))
        ),
        "an empty checkpoint must be refused"
    );
    let too_many = (0..=256)
        .map(|_| RemoteChange {
            document,
            digest,
            bytes: b"x".as_slice(),
        })
        .collect::<Vec<_>>();
    assert!(
        matches!(
            store.install_remote_page(
                scope,
                peer,
                &[],
                &too_many,
                b"checkpoint",
                InstallMode::Remote
            ),
            Err(StoreError::InvalidRemotePage)
        ),
        "one change past the remote page ceiling must be refused"
    );
    let duplicate_documents = [
        RemoteDocument {
            document,
            expected_revision: None,
            snapshot: b"x",
            indexes: &[],
            tombstone: false,
        },
        RemoteDocument {
            document,
            expected_revision: None,
            snapshot: b"y",
            indexes: &[],
            tombstone: false,
        },
    ];
    assert!(
        matches!(
            store.install_remote_page(
                scope,
                peer,
                &duplicate_documents,
                &[],
                b"checkpoint",
                InstallMode::Remote
            ),
            Err(StoreError::InvalidRemotePage)
        ),
        "a repeated document in one page must be refused"
    );
    let other = DocumentId::new();
    let documents = [RemoteDocument {
        document,
        expected_revision: None,
        snapshot: b"x",
        indexes: &[],
        tombstone: false,
    }];
    let changes = [RemoteChange {
        document: other,
        digest: ChangeDigest::new([9; 32]),
        bytes: b"x",
    }];
    assert!(
        matches!(
            store.install_remote_page(
                scope,
                peer,
                &documents,
                &changes,
                b"checkpoint",
                InstallMode::Remote
            ),
            Err(StoreError::InvalidRemotePage)
        ),
        "a change without its document must be refused"
    );
    assert_eq!(
        store.checkpoint(scope, peer)?,
        None,
        "every refusal must leave the checkpoint untouched"
    );
    drop(store);
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn tombstone_flag_commits_restarts_and_guards_resurrection() -> Result<(), Box<dyn Error>> {
    let path = test_path("tombstone-flag");
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let document = DocumentId::new();
    let replica = ReplicaId::new();
    {
        let store = EmbeddedStore::open(&path, replica)?;
        assert!(
            !store.is_tombstoned(scope, document)?,
            "a fresh document must not start tombstoned"
        );
        assert!(
            !store.is_tombstoned(scope, DocumentId::new())?,
            "an unrelated document must not be tombstoned"
        );
        store.commit_local(LocalCommit {
            change: change(scope, document, OperationId::new(), 1, 2)?,
            expected_revision: None,
            snapshot: b"snapshot",
            indexes: &[],
            tombstone: true,
        })?;
        assert!(
            store.is_tombstoned(scope, document)?,
            "the tombstone commit must mark the document deleted"
        );
        assert!(
            matches!(
                store.commit_local(LocalCommit {
                    change: change(scope, document, OperationId::new(), 3, 4)?,
                    expected_revision: Some(DocumentRevision::new(1)),
                    snapshot: b"resurrect",
                    indexes: &[],
                    tombstone: false,
                }),
                Err(StoreError::DocumentDeleted)
            ),
            "resurrection of a tombstoned document must be refused"
        );
        store.commit_local(LocalCommit {
            change: change(scope, document, OperationId::new(), 5, 6)?,
            expected_revision: Some(DocumentRevision::new(1)),
            snapshot: b"snapshot-two",
            indexes: &[],
            tombstone: true,
        })?;
        assert!(
            store.is_tombstoned(scope, document)?,
            "a second tombstone commit must keep the flag set"
        );
    }
    {
        let store = EmbeddedStore::open(&path, replica)?;
        assert!(
            store.is_tombstoned(scope, document)?,
            "the tombstone flag must survive restart"
        );
        assert_eq!(
            store.outbox_page(scope, 0, 10)?.len(),
            2,
            "both tombstone commits must occupy the outbox"
        );
    }
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn remote_install_sets_tombstone_flag_monotonically() -> Result<(), Box<dyn Error>> {
    let path = test_path("tombstone-remote");
    let store = EmbeddedStore::open(&path, ReplicaId::new())?;
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let peer = PeerId::new();
    let document = DocumentId::new();
    install_live_snapshot(
        &store,
        LiveSnapshot {
            scope,
            peer,
            document,
            snapshot: b"snapshot",
            digest: ChangeDigest::new([1; 32]),
            bytes: b"change-one",
            checkpoint: b"checkpoint-one",
        },
    )?;
    assert!(
        !store.is_tombstoned(scope, document)?,
        "the first live install must not tombstone"
    );
    let documents = [RemoteDocument {
        document,
        expected_revision: Some(DocumentRevision::new(1)),
        snapshot: b"snapshot-two",
        indexes: &[],
        tombstone: true,
    }];
    let changes = [RemoteChange {
        document,
        digest: ChangeDigest::new([2; 32]),
        bytes: b"change-two",
    }];
    store.install_remote_page(
        scope,
        peer,
        &documents,
        &changes,
        b"checkpoint-two",
        InstallMode::Remote,
    )?;
    assert!(
        store.is_tombstoned(scope, document)?,
        "the remote tombstone must set the flag"
    );
    let documents = [RemoteDocument {
        document,
        expected_revision: Some(DocumentRevision::new(2)),
        snapshot: b"snapshot-three",
        indexes: &[],
        tombstone: false,
    }];
    let changes = [RemoteChange {
        document,
        digest: ChangeDigest::new([3; 32]),
        bytes: b"change-three",
    }];
    store.install_remote_page(
        scope,
        peer,
        &documents,
        &changes,
        b"checkpoint-three",
        InstallMode::Remote,
    )?;
    assert!(
        store.is_tombstoned(scope, document)?,
        "a later live page must not clear the tombstone flag"
    );
    drop(store);
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn restore_cursor_survives_reopen_and_finalizes_revision_checked() -> Result<(), Box<dyn Error>> {
    let path = test_path("restore-cursor");
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let replica = ReplicaId::new();
    let digest_a = [7_u8; 32];
    let digest_b = [9_u8; 32];
    {
        let store = EmbeddedStore::open(&path, replica)?;
        assert_eq!(
            store.restore_cursor(scope)?,
            None,
            "a fresh store must hold no restore cursor"
        );
        assert!(
            matches!(
                store.restore_advance(scope, &digest_a, 1),
                Err(StoreError::RestoreNotInProgress)
            ),
            "advancing without a restore must fail"
        );
        assert!(
            matches!(
                store.restore_finish(scope, &digest_a, &[]),
                Err(StoreError::RestoreNotInProgress)
            ),
            "finishing without a restore must fail"
        );
        assert_eq!(
            store.restore_begin(scope, &digest_a)?,
            RestoreCursor {
                payload: digest_a,
                next: 0,
            },
            "begin must open the cursor at zero"
        );
        assert_eq!(
            store.restore_begin(scope, &digest_a)?.next,
            0,
            "rebeginning the same payload must restart at zero"
        );
        assert!(
            matches!(
                store.restore_begin(scope, &digest_b),
                Err(StoreError::RestoreInProgress)
            ),
            "beginning another payload mid-restore must fail"
        );
        store.restore_advance(scope, &digest_a, 3)?;
        assert!(
            matches!(
                store.restore_advance(scope, &digest_b, 4),
                Err(StoreError::RestoreInProgress)
            ),
            "advancing another payload mid-restore must fail"
        );
    }
    {
        let store = EmbeddedStore::open(&path, replica)?;
        assert_eq!(
            store.restore_cursor(scope)?,
            Some(RestoreCursor {
                payload: digest_a,
                next: 3,
            }),
            "the restore cursor must survive restart"
        );
        assert!(
            matches!(
                store.restore_finish(scope, &digest_b, &[]),
                Err(StoreError::RestoreInProgress)
            ),
            "finishing a foreign payload mid-restore must fail"
        );
        let document = DocumentId::new();
        store.commit_local(LocalCommit {
            change: change(scope, document, OperationId::new(), 1, 2)?,
            expected_revision: None,
            snapshot: b"snapshot",
            indexes: &[],
            tombstone: false,
        })?;
        assert!(
            matches!(
                store.restore_finish(scope, &digest_a, &[(document, DocumentRevision::new(2))]),
                Err(StoreError::RestoreIncomplete)
            ),
            "a wrong final revision must fail the restore"
        );
        assert_eq!(
            store.restore_cursor(scope)?,
            Some(RestoreCursor {
                payload: digest_a,
                next: 3,
            }),
            "a failed finish must leave the cursor intact"
        );
        store.restore_finish(scope, &digest_a, &[(document, DocumentRevision::new(1))])?;
        assert_eq!(
            store.restore_cursor(scope)?,
            None,
            "a successful finish must clear the cursor"
        );
    }
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn restore_operation_synthesis_is_deterministic_and_committable() -> Result<(), Box<dyn Error>> {
    let path = test_path("restore-operation");
    let store = EmbeddedStore::open(&path, ReplicaId::new())?;
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let document = DocumentId::new();
    let digest = ChangeDigest::new([4; 32]);
    let (operation, request) = restore_operation(document, digest)?;
    assert_eq!(
        restore_operation(document, digest)?,
        (operation, request),
        "restore operation synthesis must be deterministic"
    );
    assert_ne!(
        restore_operation(document, ChangeDigest::new([5; 32]))?.0,
        operation,
        "a different digest must synthesize a different operation"
    );
    assert_ne!(
        restore_operation(DocumentId::new(), digest)?.0,
        operation,
        "a different document must synthesize a different operation"
    );
    store.commit_local(LocalCommit {
        change: LocalChange::new(scope, document, operation, request, digest, b"change")?,
        expected_revision: None,
        snapshot: b"snapshot",
        indexes: &[],
        tombstone: false,
    })?;
    drop(store);
    fs::remove_file(path)?;
    Ok(())
}
