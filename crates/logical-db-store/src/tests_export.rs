//! Export unit tests; split from the store root.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use crate::codec::decode_uuid;
use crate::error::StoreError;
use crate::export::{decode_export, encode_export};
use crate::keys::MAX_EXPORT_BYTES;
use crate::remote::InstallMode;
use crate::store::EmbeddedStore;
use crate::test_support::{change, test_path};
use crate::types::{ExportDocument, ExportEntry, LocalCommit, RemoteChange, RemoteDocument};
use logical_db_model::{
    AccountId, ChangeDigest, DocumentId, DocumentRevision, OperationDigest, OperationId, PeerId,
    ReplicaId, Scope, SpaceId,
};
use std::{error::Error, fs};

#[test]
fn export_scan_pages_documents_in_order_with_tombstone_flags() -> Result<(), Box<dyn Error>> {
    let path = test_path("export-scan");
    let store = EmbeddedStore::open(&path, ReplicaId::new())?;
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    store.bind_schema(scope, 1, [1; 32])?;
    let documents = [
        DocumentId::from_uuid(decode_uuid(&3_u128.to_be_bytes())?),
        DocumentId::from_uuid(decode_uuid(&1_u128.to_be_bytes())?),
        DocumentId::from_uuid(decode_uuid(&2_u128.to_be_bytes())?),
    ];
    store.commit_local(LocalCommit {
        change: change(scope, documents[0], OperationId::new(), 1, 11)?,
        expected_revision: None,
        snapshot: b"snapshot-three",
        indexes: &[],
        tombstone: false,
    })?;
    store.commit_local(LocalCommit {
        change: change(scope, documents[1], OperationId::new(), 2, 12)?,
        expected_revision: None,
        snapshot: b"snapshot-one",
        indexes: &[],
        tombstone: false,
    })?;
    store.commit_local(LocalCommit {
        change: change(scope, documents[2], OperationId::new(), 3, 13)?,
        expected_revision: None,
        snapshot: b"snapshot-two",
        indexes: &[],
        tombstone: true,
    })?;
    let first = store.export_scan(scope, None, 2)?;
    assert_eq!(
        first.after, None,
        "the first page starts at the cursor origin"
    );
    assert_eq!(first.documents.len(), 2, "the first page holds its limit");
    assert_eq!(
        first.documents[0].document, documents[1],
        "the scan must order by document id"
    );
    assert_eq!(
        first.documents[0].snapshot, b"snapshot-one",
        "the first page must carry the live snapshot"
    );
    assert!(
        !first.documents[0].tombstone,
        "the live document must not be flagged"
    );
    assert_eq!(
        first.documents[1].document, documents[2],
        "the second hit must be the next document"
    );
    assert_eq!(
        first.documents[1].snapshot, b"snapshot-two",
        "the tombstoned document keeps its snapshot bytes"
    );
    assert!(
        first.documents[1].tombstone,
        "the tombstoned document must be flagged"
    );
    let cursor = Some(first.documents[1].document);
    let second = store.export_scan(scope, cursor, 2)?;
    assert_eq!(
        second.after, cursor,
        "the resumed scan must echo its cursor"
    );
    assert_eq!(
        second.documents.len(),
        1,
        "the resumed page holds the remainder"
    );
    assert_eq!(
        second.documents[0].document, documents[0],
        "the resumed page must yield the last document"
    );
    assert!(!second.documents[0].tombstone, "the last document is live");
    let done = store.export_scan(scope, Some(documents[0]), 2)?;
    assert_eq!(
        done.after,
        Some(documents[0]),
        "the final page echoes the last cursor"
    );
    assert!(
        done.documents.is_empty(),
        "a past-the-end scan must be empty"
    );
    assert!(
        matches!(
            store.export_scan(scope, None, 0),
            Err(StoreError::InvalidExportLimit(0))
        ),
        "a zero export limit must be refused"
    );
    assert!(
        matches!(
            store.export_scan(scope, None, 257),
            Err(StoreError::InvalidExportLimit(257))
        ),
        "one past the export ceiling must be refused"
    );
    drop(store);
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn history_page_keeps_acknowledged_commits_while_outbox_advances() -> Result<(), Box<dyn Error>> {
    let path = test_path("export-history");
    let store = EmbeddedStore::open(&path, ReplicaId::new())?;
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let peer = PeerId::new();
    let document = DocumentId::new();
    let first_operation = OperationId::new();
    let second_operation = OperationId::new();
    store.commit_local(LocalCommit {
        change: change(scope, document, first_operation, 1, 11)?,
        expected_revision: None,
        snapshot: b"snapshot-one",
        indexes: &[],
        tombstone: false,
    })?;
    store.commit_local(LocalCommit {
        change: change(scope, document, second_operation, 2, 12)?,
        expected_revision: Some(DocumentRevision::new(1)),
        snapshot: b"snapshot-two",
        indexes: &[],
        tombstone: false,
    })?;
    store.acknowledge_outbox(scope, peer, 1)?;
    let history = store.history_page(scope, 0, 256)?;
    assert_eq!(history.len(), 2, "history keeps both acknowledged commits");
    assert_eq!(
        history[0].operation, first_operation,
        "history must order by sequence"
    );
    assert_eq!(
        history[0].sequence, 1,
        "the first history row is sequence one"
    );
    assert_eq!(
        history[1].operation, second_operation,
        "the second history row carries its operation"
    );
    assert_eq!(
        history[1].sequence, 2,
        "the second history row is sequence two"
    );
    assert_eq!(
        store.outbox_page(scope, 0, 256)?.len(),
        1,
        "acknowledgement must drain the outbox to one unacked entry"
    );
    assert!(
        matches!(
            store.history_page(scope, 0, 0),
            Err(StoreError::InvalidOutboxLimit(0))
        ),
        "a zero history limit must be refused"
    );
    let remote_digest = ChangeDigest::new([21; 32]);
    store.install_remote_page(
        scope,
        peer,
        &[RemoteDocument {
            document,
            expected_revision: Some(DocumentRevision::new(2)),
            snapshot: b"snapshot-remote",
            indexes: &[],
            tombstone: false,
        }],
        &[RemoteChange {
            document,
            digest: remote_digest,
            bytes: b"remote-change",
        }],
        b"checkpoint-remote",
        InstallMode::Remote,
    )?;
    assert_eq!(
        store.load_change(scope, document, remote_digest)?,
        Some(b"remote-change".to_vec()),
        "the remote change must be loadable"
    );
    assert_eq!(
        store.history_page(scope, 0, 256)?.len(),
        2,
        "the deduplicated remote page must not extend history"
    );
    let events = store.changes_since(scope, 0, 256)?;
    assert_eq!(events.len(), 3, "the feed still records every sequence");
    assert_eq!(
        events[2].digest(),
        Some(remote_digest),
        "the remote event must close the feed"
    );
    drop(store);
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn export_codec_round_trips_and_refuses_hostile_inputs() -> Result<(), Box<dyn Error>> {
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let document = DocumentId::new();
    let entries = vec![ExportEntry {
        document,
        operation: OperationId::new(),
        request: OperationDigest::new([7; 32]),
        digest: ChangeDigest::new([9; 32]),
        bytes: b"change".to_vec(),
    }];
    let documents = vec![ExportDocument {
        document,
        tombstone: false,
        snapshot: b"snap".to_vec(),
    }];
    let bytes = encode_export(scope, &entries, &documents)?;
    let (decoded_scope, decoded_entries, decoded_documents) = decode_export(&bytes)?;
    assert_eq!(decoded_scope, scope, "the export must roundtrip its scope");
    assert_eq!(
        decoded_entries, entries,
        "the export must roundtrip its entries"
    );
    assert_eq!(
        decoded_documents, documents,
        "the export must roundtrip its documents"
    );
    assert!(
        decode_export(&encode_export(scope, &[], &[])?)?
            .1
            .is_empty(),
        "an empty export must roundtrip to no entries"
    );
    assert!(
        matches!(
            decode_export(&vec![0_u8; MAX_EXPORT_BYTES.saturating_add(1)]),
            Err(StoreError::InvalidExportSize(_))
        ),
        "one byte past the export ceiling must be refused"
    );
    let mut damaged = bytes.clone();
    damaged[0] ^= 0xff;
    assert!(
        matches!(decode_export(&damaged), Err(StoreError::InvalidExport)),
        "a damaged header must fail closed"
    );
    let mut future = bytes.clone();
    future[4] = 2;
    assert!(
        matches!(
            decode_export(&future),
            Err(StoreError::UnknownExportVersion(2))
        ),
        "an unknown export version must be refused"
    );
    assert!(
        matches!(
            decode_export(&bytes[..bytes.len().saturating_sub(1)]),
            Err(StoreError::InvalidExport)
        ),
        "a truncated export must fail closed"
    );
    let mut trailing = bytes;
    trailing.push(0);
    assert!(
        matches!(decode_export(&trailing), Err(StoreError::InvalidExport)),
        "a trailing byte must fail closed"
    );
    assert!(
        matches!(decode_export(&[]), Err(StoreError::InvalidExport)),
        "an empty export must fail closed"
    );
    Ok(())
}
