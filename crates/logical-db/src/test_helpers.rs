//! Shared black-box helpers for the facade unit tests.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use std::collections::BTreeMap;
use std::convert::Infallible;

use logical_db_crdt::{Document, EncodedChange, Item, Object, Scalar};
use logical_db_model::{
    ChangeDigest, DocumentId, DocumentRevision, IndexEntry, IndexId, IndexValue, OperationDigest,
    OperationId, ReplicaId, Scope,
};
use logical_db_store::{EmbeddedStore, LocalChange, LocalCommit};

use super::outcomes::{PendingChange, RemoteChange};
use super::projection::{Projection, ProjectionSemantics};

/// Path
pub(crate) fn path() -> std::path::PathBuf {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let sequence = COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    std::env::temp_dir().join(format!(
        "logical-db-facade-{sequence}-{}.redb",
        std::process::id()
    ))
}

/// Remote page
pub(crate) fn remote_page(changes: &[PendingChange]) -> Vec<RemoteChange> {
    changes
        .iter()
        .map(|change| RemoteChange {
            document: change.document,
            digest: change.digest,
            bytes: change.bytes.clone(),
        })
        .collect()
}

/// AcceptingTransport
pub(crate) struct AcceptingTransport;

impl crate::transport::Transport for AcceptingTransport {
    /// Error
    type Error = Infallible;

    /// Push
    fn push(
        &mut self,
        request: crate::transport::PushRequest,
    ) -> impl std::future::Future<Output = Result<crate::transport::PushResponse, Self::Error>>
    {
        let outcomes = request
            .changes
            .iter()
            .map(|change| crate::transport::PushOutcome::Accepted {
                operation: change.operation,
            })
            .collect();
        std::future::ready(Ok(crate::transport::PushResponse::new(
            logical_db_protocol::PROTOCOL_VERSION,
            outcomes,
        )))
    }

    /// Pull
    fn pull(
        &mut self,
        _request: crate::transport::PullRequest,
    ) -> impl std::future::Future<Output = Result<crate::transport::PullResult, Self::Error>> {
        let checkpoint = match crate::transport::Checkpoint::from_wire(vec![9]) {
            Ok(checkpoint) => checkpoint,
            // Infallible transport cannot carry ProtocolError; expiry is
            // the fail-closed Ok path for an undecodable wire checkpoint.
            Err(_) => {
                return std::future::ready(Ok(crate::transport::PullResult::CheckpointExpired));
            }
        };
        std::future::ready(Ok(crate::transport::PullResult::Page(
            crate::transport::PullPage::new(
                logical_db_protocol::PROTOCOL_VERSION,
                vec![],
                checkpoint,
                false,
            ),
        )))
    }

    /// Baseline
    fn baseline(
        &mut self,
        _request: crate::transport::BaselineRequest,
    ) -> impl std::future::Future<Output = Result<crate::transport::BaselineResult, Self::Error>>
    {
        std::future::ready(Ok(crate::transport::BaselineResult::Unavailable))
    }
}

/// Kind projection
pub(crate) fn kind_projection(index: IndexId) -> Projection {
    Projection::new(index, ProjectionSemantics::V1, |reader| {
        Ok(match reader.get(&Object::root(), "kind")? {
            Some(Item::Scalar(Scalar::String(value))) => Some(IndexValue::Text(value)),
            _ => None,
        })
    })
}

/// Kind entry
pub(crate) fn kind_entry(index: IndexId, kind: &str) -> Vec<IndexEntry> {
    vec![IndexEntry::new(index, IndexValue::Text(kind.into()))]
}

/// Read kind
pub(crate) fn read_kind(database: &super::db::Database, document: DocumentId) -> Option<String> {
    database
        .read(document, |reader| reader.get(&Object::root(), "kind"))
        .ok()?
        .and_then(|item| item)
        .and_then(|item| match item {
            Item::Scalar(Scalar::String(value)) => Some(value),
            _ => None,
        })
}

/// Compares the durable outbox cursor content, excluding the presenting
/// replica: a restored replica attributes replayed entries to its own
/// installation, while sequence, document, operation, request, digest,
/// and bytes reproduce exactly.
pub(crate) fn pending_identity(
    changes: &[PendingChange],
) -> Vec<(
    u64,
    DocumentId,
    OperationId,
    OperationDigest,
    ChangeDigest,
    Vec<u8>,
)> {
    changes
        .iter()
        .map(|change| {
            (
                change.sequence,
                change.document,
                change.operation,
                change.request,
                change.digest,
                change.bytes.clone(),
            )
        })
        .collect()
}

/// RestorePrefix
pub(crate) struct RestorePrefix<'a> {
    pub(crate) target_path: &'a std::path::Path,
    pub(crate) replica: ReplicaId,
    pub(crate) scope: Scope,
    pub(crate) index: IndexId,
    pub(crate) payload: &'a [u8],
    pub(crate) identity: &'a [u8; 32],
    pub(crate) prefix: usize,
    pub(crate) first: DocumentId,
}

/// Replay restore prefix
pub(crate) fn replay_restore_prefix(
    fixture: &RestorePrefix<'_>,
) -> Result<usize, Box<dyn std::error::Error>> {
    use logical_db_store::decode_export;

    let store = EmbeddedStore::open(fixture.target_path, fixture.replica)?;
    let (_, entries, _) = decode_export(fixture.payload)?;
    let replayed = entries.len();
    store.restore_begin(fixture.scope, fixture.identity)?;
    let mut states: BTreeMap<DocumentId, Document> = BTreeMap::new();
    let mut revisions: BTreeMap<DocumentId, Option<DocumentRevision>> = BTreeMap::new();
    for entry in entries.iter().take(fixture.prefix) {
        let state = states.entry(entry.document).or_default();
        state.apply(std::slice::from_ref(&EncodedChange::new(
            entry.digest,
            entry.bytes().to_vec(),
        )))?;
        let tombstone = state.is_deleted()?;
        let snapshot = state.save()?;
        let expected = revisions.get(&entry.document).copied().flatten();
        let indexes = kind_entry(
            fixture.index,
            if entry.document == fixture.first {
                "note"
            } else {
                "task"
            },
        );
        let change = LocalChange::new(
            fixture.scope,
            entry.document,
            entry.operation,
            entry.request,
            entry.digest,
            entry.bytes(),
        )?;
        store.commit_local(LocalCommit::new(
            change, expected, &snapshot, &indexes, tombstone,
        ))?;
        let next = match expected {
            Some(revision) => revision.next().ok_or("expected Some")?,
            None => DocumentRevision::new(1),
        };
        revisions.insert(entry.document, Some(next));
    }
    store.restore_advance(
        fixture.scope,
        fixture.identity,
        u64::try_from(fixture.prefix)?,
    )?;
    Ok(replayed)
}
