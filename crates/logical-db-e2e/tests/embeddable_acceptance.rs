//! Black-box acceptance journeys for the embeddable path (ER-T02..ER-T06).
//!
//! Covers AE1 delete-then-sync convergence, AE2 checkpoint-expiry recovery,
//! AE3 corrupt-restore refusal, AE4 TTL expiry visibility, AE5 tiered-value
//! replication and eviction, exact-retry identity, batch bounds, and the
//! weird-edge key/TTL/tier/export family. Red is correct where behavior is
//! missing; green locks existing behavior.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
//! S33 S34 S35 S36 S37 S38 S39. Source URLs are in `evidence/SOURCES.md`.

#![forbid(unsafe_code)]

use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use logical_db::{
    AccountId, BatchEdit, Database, DatabaseError, DocumentId, EditOutcome, Object, OperationId,
    PeerId, ReplicaId, Scalar, Scope, SpaceId, SyncLimits, SyncReport,
    cache::{KvCache, KvError, TIER_THRESHOLD_BYTES},
    content_digest,
    sync::SyncError,
    transport::{
        BaselineRequest, BaselineResult, Checkpoint, PullPage, PullRequest, PullResult,
        PushOutcome, PushRequest, PushResponse, Transport,
    },
};
use logical_db_protocol::ChangeEnvelope;

/// Builds one unique temp replica path tagged with a test label.
fn temp_path(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "logical-db-embeddable-{label}-{}.redb",
        DocumentId::new()
    ))
}

/// Removes one test replica's durable file and its tiered-blob directory.
fn remove_replica(path: &Path) -> std::io::Result<()> {
    let blobs = PathBuf::from(format!("{}.blobs", path.display()));
    if blobs.exists() {
        fs::remove_dir_all(blobs)?;
    }
    if path.exists() {
        fs::remove_file(path)?;
    }
    Ok(())
}

/// Derives one deterministic delete request digest from the document identity.
fn delete_request(document: DocumentId) -> logical_db::OperationDigest {
    content_digest(document, "deleted", &Scalar::Null)
}

/// Opens one fresh replica on a unique path, for a journey that needs no
/// second replica and no shared hub.
fn fresh_replica(
    label: &str,
) -> Result<(PathBuf, Scope, ReplicaId, Database), Box<dyn std::error::Error>> {
    let path = temp_path(label);
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let replica = ReplicaId::new();
    let database = Database::open(&path, scope, replica)?;
    Ok((path, scope, replica, database))
}

/// Sets `status` on one document to `text`, the edit every journey repeats.
fn put_status(
    editor: &mut logical_db::Editor<'_>,
    _object: &Object,
    text: &str,
) -> Result<(), logical_db::CrdtError> {
    editor.put(
        &Object::root(),
        "status",
        Scalar::String(String::from(text)),
    )
}

/// Derives the request digest for a `status` write, matching `put_status`.
fn status_request(document: DocumentId, text: &str) -> logical_db::OperationDigest {
    content_digest(document, "status", &Scalar::String(String::from(text)))
}

/// Opens one fresh cache on a unique path, for a key/TTL/tier journey.
fn fresh_cache(label: &str) -> Result<(PathBuf, KvCache), Box<dyn std::error::Error>> {
    let path = temp_path(label);
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let cache = KvCache::open(&path, scope, ReplicaId::new(), "app")?;
    Ok((path, cache))
}

/// Asserts one restore refused with exactly `message` on the store surface.
fn assert_restore_refusal(
    observed: Result<logical_db::RestoreReport, DatabaseError>,
    message: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    match observed {
        Err(DatabaseError::Store(inner)) => {
            assert_eq!(
                inner.to_string(),
                message,
                "restore must refuse with the exact typed message"
            );
        }
        other => {
            return Err(format!("restore must refuse; observed {other:?}").into());
        }
    }
    Ok(())
}

/// One committed `status` write and the identity an exact retry reuses.
struct CommittedStatus {
    /// Receipt of the first durable commit.
    receipt: EditOutcome<()>,
    /// Document the write landed on.
    document: DocumentId,
    /// Operation identity the write is keyed by.
    operation: OperationId,
    /// Request digest an exact retry must repeat.
    request: logical_db::OperationDigest,
}

/// Commits one `status` write, the prelude every exact-retry journey starts
/// from.
fn committed_status(
    database: &Database,
    text: &str,
) -> Result<CommittedStatus, Box<dyn std::error::Error>> {
    let document = DocumentId::new();
    let operation = OperationId::new();
    let request = status_request(document, text);
    let receipt = database.edit(document, operation, request, |editor| {
        put_status(editor, &Object::root(), text)
    })?;
    Ok(CommittedStatus {
        receipt,
        document,
        operation,
        request,
    })
}

/// Two replicas on one shared hub, for a journey that needs a real sync pair.
struct ReplicaPair {
    /// The first replica and the path backing it.
    first: (Database, PathBuf),
    /// The second replica and the path backing it.
    second: (Database, PathBuf),
    /// Scope both replicas share.
    scope: Scope,
    /// Logical peer both replicas sync through.
    peer: PeerId,
    /// Transport speaking for the first replica; its hub is the shared log.
    transport_a: HubTransport,
    /// Transport speaking for the second replica.
    transport_b: HubTransport,
}

impl ReplicaPair {
    /// Splits the pair into the two replicas, their paths, the peer, and their
    /// transports, leaving the scope and hub to a journey that wants them.
    fn into_parts(
        self,
    ) -> (
        (Database, PathBuf),
        (Database, PathBuf),
        PeerId,
        HubTransport,
        HubTransport,
    ) {
        (
            self.first,
            self.second,
            self.peer,
            self.transport_a,
            self.transport_b,
        )
    }

    /// Splits the pair and also hands back the shared service log.
    fn into_parts_with_hub(
        self,
    ) -> (
        (Database, PathBuf),
        (Database, PathBuf),
        PeerId,
        Arc<Mutex<Hub>>,
        HubTransport,
        HubTransport,
    ) {
        let (first, second, peer, transport_a, transport_b) = self.into_parts();
        let hub = Arc::clone(&transport_a.hub);
        (first, second, peer, hub, transport_a, transport_b)
    }

    /// Opens two fresh replicas on a shared hub.
    fn open(label: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let path_a = temp_path(&format!("{label}-a"));
        let path_b = temp_path(&format!("{label}-b"));
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica_a = ReplicaId::new();
        let replica_b = ReplicaId::new();
        let hub = shared_hub();
        let first = (Database::open(&path_a, scope, replica_a)?, path_a);
        let second = (Database::open(&path_b, scope, replica_b)?, path_b);
        let transport_a = HubTransport {
            hub: Arc::clone(&hub),
            replica: replica_a,
        };
        let transport_b = HubTransport {
            hub: Arc::clone(&hub),
            replica: replica_b,
        };
        Ok(Self {
            first,
            second,
            scope,
            peer: PeerId::new(),
            transport_a,
            transport_b,
        })
    }
}

/// Builds `count` batch edits over distinct documents with integer payloads.
fn batch_of(count: usize) -> Vec<BatchEdit> {
    (0..count)
        .map(|index| {
            let document = DocumentId::new();
            let operation = OperationId::new();
            let value = i64::try_from(index).unwrap_or(i64::MAX);
            let request = content_digest(document, "n", &Scalar::Integer(value));
            BatchEdit::new(document, operation, request, move |editor| {
                editor.put(&Object::root(), "n", Scalar::Integer(value))
            })
        })
        .collect()
}

/// Typed failures the in-process transport fakes can surface.
#[derive(Debug)]
enum TransportFailure {
    /// The failure is not worth retrying and must surface immediately.
    Permanent {
        /// Stable reason string asserted by the caller.
        reason: &'static str,
    },
}

impl std::fmt::Display for TransportFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let &Self::Permanent { reason } = self;
        write!(formatter, "permanent transport failure: {reason}")
    }
}

impl std::error::Error for TransportFailure {}

/// Typed failures an in-memory hub transport can surface.
#[derive(Debug)]
enum HubError {
    /// The shared hub lock was poisoned by a panicked test.
    Poisoned,
    /// An opaque checkpoint did not encode a hub log position.
    BadCheckpoint,
}

impl std::fmt::Display for HubError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            Self::Poisoned => formatter.write_str("in-memory hub lock was poisoned"),
            Self::BadCheckpoint => formatter.write_str("checkpoint did not encode a hub position"),
        }
    }
}

impl std::error::Error for HubError {}

/// In-memory upstream service shared by every replica transport in a test.
struct Hub {
    /// Ordered accepted changes forming the durable service log.
    log: Vec<ChangeEnvelope>,
    /// Operation identities already accepted, used for duplicate detection.
    seen: BTreeSet<(ReplicaId, OperationId)>,
    /// When set, every non-empty pull cursor is treated as expired.
    expire_cursors: bool,
}

/// Builds one fresh shared in-memory hub.
fn shared_hub() -> Arc<Mutex<Hub>> {
    Arc::new(Mutex::new(Hub {
        log: Vec::new(),
        seen: BTreeSet::new(),
        expire_cursors: false,
    }))
}

/// Encodes a hub log position as an opaque 8-byte checkpoint token.
fn encode_position(position: usize) -> Vec<u8> {
    u64::try_from(position)
        .unwrap_or(u64::MAX)
        .to_be_bytes()
        .to_vec()
}

/// Decodes an opaque checkpoint token back into a hub log position.
fn decode_position(checkpoint: &Checkpoint) -> Result<usize, HubError> {
    let wire = checkpoint.as_wire();
    if wire.len() != 8 {
        return Err(HubError::BadCheckpoint);
    }
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(wire);
    usize::try_from(u64::from_be_bytes(bytes)).map_err(|_error| HubError::BadCheckpoint)
}

/// Per-replica transport handle over one shared in-memory hub.
struct HubTransport {
    /// Shared service log and cursor policy.
    hub: Arc<Mutex<Hub>>,
    /// Replica this transport speaks for; its own changes are never echoed.
    replica: ReplicaId,
}

impl Transport for HubTransport {
    type Error = HubError;

    /// Accepts one bounded upload page into the shared service log.
    async fn push(&mut self, request: PushRequest) -> Result<PushResponse, Self::Error> {
        let protocol = request.protocol;
        let changes = request.into_changes();
        let mut hub = self.hub.lock().map_err(|_poisoned| HubError::Poisoned)?;
        let mut outcomes = Vec::with_capacity(changes.len());
        for change in changes {
            let operation = change.operation;
            let identity = (change.replica, operation);
            if hub.seen.contains(&identity) {
                outcomes.push(PushOutcome::Duplicate { operation });
            } else {
                hub.seen.insert(identity);
                hub.log.push(change);
                outcomes.push(PushOutcome::Accepted { operation });
            }
        }
        Ok(PushResponse::new(protocol, outcomes))
    }

    /// Serves one bounded pull page after the caller's cursor.
    async fn pull(&mut self, request: PullRequest) -> Result<PullResult, Self::Error> {
        let protocol = request.protocol;
        let hub = self.hub.lock().map_err(|_poisoned| HubError::Poisoned)?;
        if hub.expire_cursors && request.after.is_some() {
            return Ok(PullResult::CheckpointExpired);
        }
        let after_position = match request.after {
            Some(checkpoint) => decode_position(&checkpoint)?,
            None => 0_usize,
        };
        let max_changes = usize::from(request.max_changes);
        let max_bytes =
            usize::try_from(request.max_bytes).map_err(|_error| HubError::BadCheckpoint)?;
        let mut page_changes = Vec::new();
        let mut page_bytes = 0_usize;
        let mut position = after_position;
        while position < hub.log.len() && page_changes.len() < max_changes {
            let change = &hub.log[position];
            if change.replica == self.replica {
                position = position.saturating_add(1);
                continue;
            }
            let width = change.as_bytes().len();
            let fits = page_changes.is_empty()
                || page_bytes
                    .checked_add(width)
                    .is_some_and(|next| next <= max_bytes);
            if !fits {
                break;
            }
            page_bytes = page_bytes.saturating_add(width);
            page_changes.push(change.clone());
            position = position.saturating_add(1);
        }
        let more = position < hub.log.len();
        let checkpoint = Checkpoint::from_wire(encode_position(position))
            .map_err(|_error| HubError::BadCheckpoint)?;
        Ok(PullResult::Page(PullPage::new(
            protocol,
            page_changes,
            checkpoint,
            more,
        )))
    }

    /// Reports that no recovery baseline is sealed for this hub.
    async fn baseline(&mut self, _request: BaselineRequest) -> Result<BaselineResult, Self::Error> {
        Ok(BaselineResult::Unavailable)
    }
}

/// Transport that surfaces one permanent failure on every call.
struct FailingTransport {
    /// Stable reason string reported with every failure.
    reason: &'static str,
}

impl Transport for FailingTransport {
    type Error = TransportFailure;

    /// Refuses the upload permanently.
    async fn push(&mut self, _request: PushRequest) -> Result<PushResponse, Self::Error> {
        Err(TransportFailure::Permanent {
            reason: self.reason,
        })
    }

    /// Refuses the pull permanently.
    async fn pull(&mut self, _request: PullRequest) -> Result<PullResult, Self::Error> {
        Err(TransportFailure::Permanent {
            reason: self.reason,
        })
    }

    /// Refuses the baseline fetch permanently.
    async fn baseline(&mut self, _request: BaselineRequest) -> Result<BaselineResult, Self::Error> {
        Err(TransportFailure::Permanent {
            reason: self.reason,
        })
    }
}

/// Runs one push-then-pull cycle for `database` against its hub transport.
fn sync_once(
    database: &Database,
    peer: PeerId,
    transport: &mut HubTransport,
) -> Result<SyncReport, Box<dyn std::error::Error>> {
    let runtime = lgwks_bot::Runtime::new()?;
    match runtime.block_on(database.synchronize(peer, transport, SyncLimits::default(), || false)) {
        Ok(report) => Ok(report),
        Err(error) => Err(format!("synchronize failed: {error:?}").into()),
    }
}

/// AE3 + ER-T02: corrupt export restore refuses and preserves live state.
#[test]
fn restore_corrupt_refuses_and_preserves_live_state() -> Result<(), Box<dyn std::error::Error>> {
    // Given: a replica holding one committed value and a fresh export of it.
    let (path, scope, replica, database) = fresh_replica("restore-corrupt")?;
    let document = DocumentId::new();
    database.put_value(document, "status", Scalar::String(String::from("live")))?;
    let mut payload = database.export()?;

    // When: the export bytes are corrupted after write and offered to restore.
    payload[0] = b'X';
    let observed = database.restore(&payload);

    // Then: restore refuses with the typed malformed-export message.
    match observed {
        Err(DatabaseError::Store(inner)) => {
            assert_eq!(
                inner.to_string(),
                "logical export payload is malformed",
                "corrupt restore must refuse with the typed malformed-export message"
            );
        }
        other => {
            return Err(format!("corrupt restore must refuse; observed {other:?}").into());
        }
    }
    assert_eq!(
        database.get_value(document, "status")?,
        Some(Scalar::String(String::from("live"))),
        "a refused restore must leave live state untouched"
    );
    assert!(
        database.restore_cursor()?.is_none(),
        "a refused restore must leave no restore cursor"
    );
    drop(database);
    remove_replica(&path)?;
    Ok(())
}

/// ER-T02: a truncated export restore refuses with a typed message.
#[test]
fn restore_truncated_refuses() -> Result<(), Box<dyn std::error::Error>> {
    // Given: a valid export shorter than the versioned header.
    let (path, scope, replica, database) = fresh_replica("restore-truncated")?;
    let document = DocumentId::new();
    database.put_value(document, "status", Scalar::String(String::from("live")))?;
    let payload = database.export()?;
    let truncated = payload[..16].to_vec();

    // When: the truncated payload is offered to restore.
    let observed = database.restore(&truncated);

    // Then: restore refuses with the typed malformed-export message.
    assert_restore_refusal(observed, "logical export payload is malformed")?;
    drop(database);
    remove_replica(&path)?;
    Ok(())
}

/// ER-T02: an export carrying a foreign codec version refuses fail-closed.
#[test]
fn restore_foreign_version_refuses() -> Result<(), Box<dyn std::error::Error>> {
    // Given: a payload whose magic is intact but whose version byte is foreign.
    let (path, scope, replica, database) = fresh_replica("restore-version")?;
    let mut payload = vec![0_u8; 37];
    payload[..4].copy_from_slice(b"LDBE");
    payload[4] = 2_u8;

    // When: the foreign-version payload is offered to restore.
    let observed = database.restore(&payload);

    // Then: restore refuses naming the unsupported version.
    assert_restore_refusal(observed, "logical export version 2 is not supported")?;
    drop(database);
    remove_replica(&path)?;
    Ok(())
}

/// ER-T02: a restore input one byte past `MAX_EXPORT_BYTES` refuses typed.
#[test]
fn restore_oversize_refuses() -> Result<(), Box<dyn std::error::Error>> {
    // Given: a payload one byte past the export size bound.
    let (path, scope, replica, database) = fresh_replica("restore-oversize")?;
    let payload = vec![0_u8; logical_db::MAX_EXPORT_BYTES.saturating_add(1)];

    // When: the oversize payload is offered to restore.
    let observed = database.restore(&payload);

    // Then: restore refuses naming the observed byte length.
    assert_restore_refusal(
        observed,
        "logical export byte length 67108865 is outside local bounds",
    )?;
    drop(database);
    remove_replica(&path)?;
    Ok(())
}

/// ER-T02: a restore input exactly at `MAX_EXPORT_BYTES` refuses, never truncates.
#[test]
fn restore_at_bound_refuses() -> Result<(), Box<dyn std::error::Error>> {
    // Given: a payload of exactly the export size bound with no valid magic.
    let (path, scope, replica, database) = fresh_replica("restore-at-bound")?;
    let payload = vec![0_u8; logical_db::MAX_EXPORT_BYTES];

    // When: the at-bound payload is offered to restore.
    let observed = database.restore(&payload);

    // Then: restore refuses typed instead of silently truncating input.
    match observed {
        Err(DatabaseError::Store(inner)) => {
            assert_eq!(
                inner.to_string(),
                "logical export payload is malformed",
                "an at-bound export must refuse typed rather than truncate"
            );
        }
        other => {
            return Err(format!("at-bound restore must refuse; observed {other:?}").into());
        }
    }
    drop(database);
    remove_replica(&path)?;
    Ok(())
}

/// ER-T02: an empty edit batch is refused with a typed size error (TDD red).
#[test]
fn edit_many_zero_refuses() -> Result<(), Box<dyn std::error::Error>> {
    // Given: an open replica and no edits.
    let (path, scope, replica, database) = fresh_replica("batch-zero")?;

    // When: an empty edit batch is offered.
    // Then: the batch is refused with `InvalidEditBatchSize(0)`.
    match database.edit_many(Vec::new()) {
        Err(DatabaseError::InvalidEditBatchSize(size)) => {
            assert_eq!(size, 0, "the refusal must name the observed batch size 0");
        }
        observed => {
            return Err(format!(
                "gap: edit_many must refuse batch size 0 with InvalidEditBatchSize(0); observed {observed:?}"
            )
            .into());
        }
    }
    drop(database);
    remove_replica(&path)?;
    Ok(())
}

/// ER-T02: a one-element edit batch applies exactly one durable change.
#[test]
fn edit_many_one_applies() -> Result<(), Box<dyn std::error::Error>> {
    // Given: an open replica and one prepared edit.
    let (path, scope, replica, database) = fresh_replica("batch-one")?;
    let edits = batch_of(1);
    let document = edits[0].document;

    // When: the one-element batch is committed.
    let outcomes = database.edit_many(edits)?;

    // Then: exactly one applied outcome lands and the document is readable.
    assert_eq!(
        outcomes.len(),
        1,
        "a one-element batch must report one outcome"
    );
    match outcomes.into_iter().next() {
        Some(EditOutcome::Applied(receipt)) => {
            assert!(
                receipt.sequence >= 1,
                "the applied receipt must carry a sequence"
            );
        }
        other => return Err(format!("a one-element batch must apply; observed {other:?}").into()),
    }
    assert_eq!(
        database.get_value(document, "n")?,
        Some(Scalar::Integer(0)),
        "the batched edit must land its integer payload"
    );
    drop(database);
    remove_replica(&path)?;
    Ok(())
}

/// ER-T02: an edit batch at the ceiling applies every member.
#[test]
fn edit_many_at_limit_applies() -> Result<(), Box<dyn std::error::Error>> {
    // Given: an open replica and a full batch of distinct documents.
    let (path, scope, replica, database) = fresh_replica("batch-at-limit")?;
    let edits = batch_of(256);
    let last_document = edits[255].document;

    // When: the full batch is committed.
    let outcomes = database.edit_many(edits)?;

    // Then: every member applies and the tail document is readable.
    assert_eq!(
        outcomes.len(),
        256,
        "a batch at the ceiling must report one outcome per edit"
    );
    assert!(
        outcomes
            .iter()
            .all(|outcome| matches!(outcome, EditOutcome::Applied(_))),
        "every edit at the batch ceiling must apply"
    );
    assert_eq!(
        database.get_value(last_document, "n")?,
        Some(Scalar::Integer(255)),
        "the tail edit must land its integer payload"
    );
    drop(database);
    remove_replica(&path)?;
    Ok(())
}

/// ER-T02: an edit batch one past the ceiling refuses with a typed size error.
#[test]
fn edit_many_over_limit_refuses() -> Result<(), Box<dyn std::error::Error>> {
    // Given: an open replica and one edit past the ceiling.
    let (path, scope, replica, database) = fresh_replica("batch-over")?;
    let edits = batch_of(257);

    // When: the oversize batch is offered.
    let observed = database.edit_many(edits);

    // Then: the batch is refused naming the observed size before any write.
    match observed {
        Err(DatabaseError::InvalidEditBatchSize(size)) => {
            assert_eq!(
                size, 257,
                "the refusal must name the observed batch size 257"
            );
        }
        other => {
            return Err(format!("an oversize batch must refuse; observed {other:?}").into());
        }
    }
    assert_eq!(
        database.changes_since(0, 64)?.len(),
        0,
        "a refused batch must leave the durable log empty"
    );
    drop(database);
    remove_replica(&path)?;
    Ok(())
}

/// Beat-eval ER-T06: an exact retry returns the original receipt, not a write.
#[test]
fn edit_exact_retry_returns_duplicate_receipt() -> Result<(), Box<dyn std::error::Error>> {
    // Given: one committed edit identified by an operation and request digest.
    let (path, scope, replica, database) = fresh_replica("exact-retry")?;
    let document = DocumentId::new();
    let operation = OperationId::new();
    let request = status_request(document, "draft");
    let first = database.edit(document, operation, request, |editor| {
        put_status(editor, &Object::root(), "draft")
    })?;

    // When: the same operation and request digest are replayed with divergent bytes.
    let second = database.edit(document, operation, request, |editor| {
        put_status(editor, &Object::root(), "HIT")
    })?;

    // Then: the replay returns the original receipt and stages no second change.
    let (first_sequence, first_digest) = match first {
        EditOutcome::Applied(receipt) => (receipt.sequence, receipt.digest),
        other => return Err(format!("the first edit must apply; observed {other:?}").into()),
    };
    let (second_sequence, second_digest) = match second {
        EditOutcome::Duplicate(duplicate) => (duplicate.sequence, duplicate.digest),
        other => return Err(format!("the exact retry must duplicate; observed {other:?}").into()),
    };
    assert_eq!(
        second_sequence, first_sequence,
        "an exact retry must return the original outbox sequence"
    );
    assert_eq!(
        second_digest, first_digest,
        "an exact retry must return the original change digest"
    );
    assert_eq!(
        database.get_value(document, "status")?,
        Some(Scalar::String(String::from("draft"))),
        "the duplicate must not run the edit closure"
    );
    assert_eq!(
        database.changes_since(0, 64)?.len(),
        1,
        "an exact retry must not stage a second change"
    );
    drop(database);
    remove_replica(&path)?;
    Ok(())
}

/// ER-T06: exact retry after reopen still returns the original receipt.
#[test]
fn edit_exact_retry_after_reopen_returns_duplicate() -> Result<(), Box<dyn std::error::Error>> {
    // Given: one committed edit and a reopen of the same durable replica.
    let path = temp_path("exact-retry-reopen");
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let replica = ReplicaId::new();
    let document = DocumentId::new();
    let operation = OperationId::new();
    let request = status_request(document, "draft");
    let first = {
        let database = Database::open(&path, scope, replica)?;
        let outcome = database.edit(document, operation, request, |editor| {
            editor.put(
                &Object::root(),
                "status",
                Scalar::String(String::from("draft")),
            )
        })?;
        match outcome {
            EditOutcome::Applied(receipt) => (receipt.sequence, receipt.digest),
            other => {
                return Err(format!("the first edit must apply; observed {other:?}").into());
            }
        }
    };
    let database = Database::open(&path, scope, replica)?;

    // When: the same operation and request digest are replayed after reopen.
    let second = database.edit(document, operation, request, |editor| {
        put_status(editor, &Object::root(), "HIT")
    })?;

    // Then: the replay returns the original receipt across the restart.
    match second {
        EditOutcome::Duplicate(duplicate) => {
            assert_eq!(
                duplicate.sequence, first.0,
                "a post-reopen exact retry must return the original outbox sequence"
            );
            assert_eq!(
                duplicate.digest, first.1,
                "a post-reopen exact retry must return the original change digest"
            );
        }
        other => {
            return Err(
                format!("a post-reopen exact retry must duplicate; observed {other:?}").into(),
            );
        }
    }
    assert_eq!(
        database.get_value(document, "status")?,
        Some(Scalar::String(String::from("draft"))),
        "the post-reopen duplicate must not run the edit closure"
    );
    drop(database);
    remove_replica(&path)?;
    Ok(())
}

/// ER-T03: an empty cache key is refused with a typed error.
#[test]
fn put_empty_key_refuses() -> Result<(), Box<dyn std::error::Error>> {
    // Given: an open cache and an empty key.
    let (path, cache) = fresh_cache("key-empty")?;

    // When: the empty key is offered to put.
    // Then: the key is refused with `InvalidKey`.
    match cache.put("", b"x", None) {
        Err(KvError::InvalidKey) => {}
        observed => {
            return Err(
                format!("an empty key must refuse with InvalidKey; observed {observed:?}").into(),
            );
        }
    }
    drop(cache);
    remove_replica(&path)?;
    Ok(())
}

/// ER-T03: a whitespace-only key is distinct from empty and roundtrips.
#[test]
fn put_whitespace_key_roundtrips() -> Result<(), Box<dyn std::error::Error>> {
    // Given: an open cache and a whitespace-only key.
    let (path, cache) = fresh_cache("key-whitespace")?;

    // When: a value is stored under the whitespace-only key.
    cache.put(" ", b"space-key", None)?;

    // Then: the key roundtrips its exact bytes and stays distinct from a
    // neighbouring key. An empty key is refused outright by
    // `put_empty_key_refuses`, so distinctness is proven against a neighbour.
    assert_eq!(
        cache.get(" ")?,
        Some(b"space-key".to_vec()),
        "a whitespace-only key must roundtrip its exact bytes"
    );
    cache.put("  ", b"other-key", None)?;
    assert_eq!(
        cache.get(" ")?,
        Some(b"space-key".to_vec()),
        "a one-space key must never alias a two-space key"
    );
    assert_eq!(
        cache.get("  ")?,
        Some(b"other-key".to_vec()),
        "a two-space key must roundtrip its own bytes"
    );
    drop(cache);
    remove_replica(&path)?;
    Ok(())
}

/// ER-T03: an absent key reads `None`, distinct from empty-string and whitespace.
#[test]
fn get_absent_key_returns_none() -> Result<(), Box<dyn std::error::Error>> {
    // Given: an open cache with no writes.
    let (path, cache) = fresh_cache("key-absent")?;

    // When: a never-written key is read.
    // Then: the read reports exact absence.
    assert_eq!(
        cache.get("never-written")?,
        None,
        "an absent key must read None"
    );
    drop(cache);
    remove_replica(&path)?;
    Ok(())
}

/// ER-T03: unicode keys roundtrip through the digest and index paths.
#[test]
fn put_unicode_keys_scan_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
    // Given: an open cache and three unicode keys (Latin, RTL, CJK).
    let (path, cache) = fresh_cache("key-unicode")?;

    // When: values are stored under each unicode key.
    cache.put("Ægir", b"1", None)?;
    cache.put("الجبر", b"2", None)?;
    cache.put("李", b"3", None)?;

    // Then: each key roundtrips its exact bytes and the index lists all three.
    assert_eq!(
        cache.get("Ægir")?,
        Some(b"1".to_vec()),
        "the Latin key must roundtrip its exact bytes"
    );
    assert_eq!(
        cache.get("الجبر")?,
        Some(b"2".to_vec()),
        "the RTL key must roundtrip its exact bytes"
    );
    assert_eq!(
        cache.get("李")?,
        Some(b"3".to_vec()),
        "the CJK key must roundtrip its exact bytes"
    );
    let entries = cache.scan("", None, 16)?;
    assert_eq!(
        entries.len(),
        3,
        "the index path must list every unicode key exactly once"
    );
    assert_eq!(
        entries[0].key(),
        "Ægir",
        "the index must order the Latin key first"
    );
    assert_eq!(
        entries[1].key(),
        "الجبر",
        "the index must order the RTL key second"
    );
    assert_eq!(
        entries[2].key(),
        "李",
        "the index must order the CJK key third"
    );
    drop(cache);
    remove_replica(&path)?;
    Ok(())
}

/// ER-T03: unicode keys produce stable, distinct request digests.
#[test]
fn edit_unicode_key_digests_distinct() -> Result<(), Box<dyn std::error::Error>> {
    // Given: one document and three unicode keys over one scalar value.
    let document = DocumentId::new();
    let value = Scalar::String(String::from("v"));

    // When: the request digest is derived for each key.
    let latin = content_digest(document, "Ægir", &value);
    let rtl = content_digest(document, "الجبر", &value);
    let cjk = content_digest(document, "李", &value);
    let latin_again = content_digest(document, "Ægir", &value);

    // Then: digests are stable per key and distinct across keys.
    assert_eq!(
        latin, latin_again,
        "the same unicode key must derive a stable request digest"
    );
    assert_ne!(
        latin, rtl,
        "the Latin and RTL keys must derive distinct request digests"
    );
    assert_ne!(
        rtl, cjk,
        "the RTL and CJK keys must derive distinct request digests"
    );
    assert_ne!(
        cjk, latin,
        "the CJK and Latin keys must derive distinct request digests"
    );
    Ok(())
}

/// AE4: a 60s-TTL entry reads its value before expiry with no eviction call.
#[test]
fn put_60s_ttl_reads_value() -> Result<(), Box<dyn std::error::Error>> {
    // Given: an open cache and one entry with a 60s TTL.
    let (path, cache) = fresh_cache("ttl-60s")?;
    cache.put("fresh", b"still-here", Some(Duration::from_secs(60)))?;

    // When: the entry is read immediately, before expiry.
    // Then: the value is present with no manual eviction call.
    assert_eq!(
        cache.get("fresh")?,
        Some(b"still-here".to_vec()),
        "a 60s-TTL entry must read its value before expiry"
    );
    drop(cache);
    remove_replica(&path)?;
    Ok(())
}

/// AE4: an expired entry reads absent with no manual eviction call.
#[test]
fn put_expired_reads_absent() -> Result<(), Box<dyn std::error::Error>> {
    // Given: an open cache and one entry whose TTL has already elapsed.
    let (path, cache) = fresh_cache("ttl-expired")?;
    cache.put("ephemeral", b"soon-gone", Some(Duration::ZERO))?;

    // When: the entry is read after its zero-length TTL elapsed.
    // Then: the read reports absent without any manual eviction call.
    assert_eq!(
        cache.get("ephemeral")?,
        None,
        "an expired entry must read absent without a manual eviction call"
    );
    drop(cache);
    remove_replica(&path)?;
    Ok(())
}

/// ER-T03: a no-TTL entry never expires (`NEVER_EXPIRES_AT_MS = i64::MAX`).
#[test]
fn put_no_ttl_stays_readable() -> Result<(), Box<dyn std::error::Error>> {
    // Given: an open cache and one entry with no TTL.
    let (path, cache) = fresh_cache("ttl-none")?;
    cache.put("immortal", b"forever", None)?;

    // When: the entry is read alongside a sibling whose zero TTL already lapsed.
    cache.put("mortal", b"gone", Some(Duration::ZERO))?;

    // Then: the no-TTL value stays readable, matching `NEVER_EXPIRES_AT_MS = i64::MAX`.
    assert_eq!(
        cache.get("immortal")?,
        Some(b"forever".to_vec()),
        "a no-TTL entry must stay readable while a lapsed sibling reads absent"
    );
    assert_eq!(
        cache.get("mortal")?,
        None,
        "a zero-TTL sibling must read absent in the same read pass"
    );
    drop(cache);
    remove_replica(&path)?;
    Ok(())
}

/// Stores `size` bytes and asserts they roundtrip inline with no blob
/// reference, the shared shape of both tier-threshold boundary journeys.
fn assert_stays_inline(
    label: &str,
    key: &str,
    size: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let (path, cache) = fresh_cache(label)?;
    let value = vec![7_u8; size];
    cache.put(key, &value, None)?;
    assert_eq!(
        cache.get(key)?,
        Some(value),
        "a value at this size must roundtrip its exact bytes"
    );
    assert_eq!(
        cache.blob_reference(key)?,
        None,
        "a value at this size must stay inline with no blob reference"
    );
    drop(cache);
    remove_replica(&path)?;
    Ok(())
}

/// ER-T03: a value one byte under the tier threshold stays inline.
#[test]
fn put_under_threshold_stays_inline() -> Result<(), Box<dyn std::error::Error>> {
    // Given: a value one byte under the tier threshold.
    // Then: it roundtrips inline with no tiered reference.
    assert_stays_inline(
        "tier-under",
        "under",
        TIER_THRESHOLD_BYTES.saturating_sub(1),
    )
}

/// ER-T03: a value exactly at the tier threshold stays inline.
#[test]
fn put_at_threshold_stays_inline() -> Result<(), Box<dyn std::error::Error>> {
    // Given: a value exactly at the tier threshold.
    // Then: it roundtrips inline with no tiered reference.
    assert_stays_inline("tier-at", "at", TIER_THRESHOLD_BYTES)
}

/// ER-T03 + AE5: a value one byte over the tier threshold tiers to a blob.
#[test]
fn put_over_threshold_tiers_blob() -> Result<(), Box<dyn std::error::Error>> {
    // Given: an open cache and a value one byte over the tier threshold.
    let (path, cache) = fresh_cache("tier-over")?;
    let value = vec![7_u8; TIER_THRESHOLD_BYTES.saturating_add(1)];
    cache.put("over", &value, None)?;

    // When: the entry and its blob reference are inspected.
    // Then: the value tiers to a blob of the declared size and still reads back.
    let reference = cache.blob_reference("over")?;
    let Some(reference) = reference else {
        return Err("an over-threshold value must carry a blob reference".into());
    };
    assert_eq!(
        reference.size,
        u64::try_from(TIER_THRESHOLD_BYTES.saturating_add(1))
            .map_err(|error| format!("size conversion failed: {error}"))?,
        "the blob reference must declare the exact tiered byte count"
    );
    assert_eq!(
        cache.get("over")?,
        Some(value),
        "an over-threshold value must roundtrip its exact bytes"
    );
    drop(cache);
    remove_replica(&path)?;
    Ok(())
}

/// AE5: a tiered value syncs to a second replica as identical bytes.
#[test]
fn put_tiered_syncs_identical_bytes() -> Result<(), Box<dyn std::error::Error>> {
    // Given: two replicas sharing a hub and one tiered value on the first.
    let path_a = temp_path("tier-sync-a");
    let path_b = temp_path("tier-sync-b");
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let replica_a = ReplicaId::new();
    let replica_b = ReplicaId::new();
    let peer = PeerId::new();
    let cache_a = KvCache::open(&path_a, scope, replica_a, "app")?;
    let cache_b = KvCache::open(&path_b, scope, replica_b, "app")?;
    let value = vec![9_u8; TIER_THRESHOLD_BYTES.saturating_add(1)];
    cache_a.put("big", &value, None)?;
    let reference = match cache_a.blob_reference("big")? {
        Some(reference) => reference,
        None => return Err("a tiered value must carry a blob reference".into()),
    };
    let hub = shared_hub();
    let mut transport_a = HubTransport {
        hub: Arc::clone(&hub),
        replica: replica_a,
    };
    let mut transport_b = HubTransport {
        hub: Arc::clone(&hub),
        replica: replica_b,
    };

    // When: the sidecar reference syncs and the tiered bytes are copied over.
    sync_once(cache_a.database(), peer, &mut transport_a)?;
    sync_once(cache_b.database(), peer, &mut transport_b)?;
    let chunk = cache_a.read_blob_chunk(&reference.digest, 0, value.len())?;
    let complete = cache_b.write_blob_chunk(&reference.digest, reference.size, 0, &chunk)?;

    // Then: both replicas read identical bytes and the transfer completes.
    assert!(complete, "a single-chunk blob transfer must complete");
    let on_a = cache_a.get("big")?;
    let on_b = cache_b.get("big")?;
    assert_eq!(on_a, on_b, "both replicas must read identical tiered bytes");
    assert_eq!(
        on_a,
        Some(value),
        "the tiered value must roundtrip its exact bytes on both replicas"
    );
    drop(cache_a);
    drop(cache_b);
    remove_replica(&path_a)?;
    remove_replica(&path_b)?;
    Ok(())
}

/// AE5: deleting a tiered value removes its reference and its blob bytes.
#[test]
fn delete_tiered_removes_reference_and_bytes() -> Result<(), Box<dyn std::error::Error>> {
    // Given: an open cache holding one tiered value.
    let (path, cache) = fresh_cache("tier-delete")?;
    let value = vec![9_u8; TIER_THRESHOLD_BYTES.saturating_add(1)];
    cache.put("big", &value, None)?;
    assert!(
        cache.blob_reference("big")?.is_some(),
        "the tiered value must start with a blob reference"
    );

    // When: the entry is deleted and unreferenced blobs are swept.
    let removed = cache.delete("big")?;
    let swept = cache.sweep_blobs()?;

    // Then: the reference is gone and the tiered bytes are reclaimed.
    assert!(
        removed,
        "the delete must report the tiered entry as removed"
    );
    assert_eq!(
        swept, 1,
        "the sweep must reclaim exactly the one orphaned blob file"
    );
    assert_eq!(
        cache.blob_reference("big")?,
        None,
        "the delete must remove the tiered blob reference"
    );
    assert_eq!(
        cache.get("big")?,
        None,
        "the delete must make the tiered value read absent"
    );
    drop(cache);
    remove_replica(&path)?;
    Ok(())
}

/// AE1: delete-then-sync converges both replicas to absent.
#[test]
fn delete_then_sync_converges_to_absent() -> Result<(), Box<dyn std::error::Error>> {
    // Given: two replicas that both hold one shared document.
    let (first, second, peer, mut transport_a, mut transport_b) =
        ReplicaPair::open("ae1")?.into_parts();
    let (database_a, path_a) = first;
    let (database_b, path_b) = second;
    let document = DocumentId::new();
    database_a.put_value(document, "status", Scalar::String(String::from("shared")))?;
    sync_once(&database_a, peer, &mut transport_a)?;
    sync_once(&database_b, peer, &mut transport_b)?;
    assert_eq!(
        database_b.get_value(document, "status")?,
        Some(Scalar::String(String::from("shared"))),
        "the second replica must hold the shared document before deletion"
    );

    // When: the first replica deletes the document and both sync again.
    database_a.delete(document, OperationId::new(), delete_request(document))?;
    sync_once(&database_a, peer, &mut transport_a)?;
    sync_once(&database_b, peer, &mut transport_b)?;

    // Then: both replicas converge to absent.
    assert_eq!(
        database_a.get_value(document, "status")?,
        None,
        "the deleting replica must read the document absent"
    );
    assert_eq!(
        database_b.get_value(document, "status")?,
        None,
        "the second replica must converge to absent"
    );
    drop(database_a);
    drop(database_b);
    remove_replica(&path_a)?;
    remove_replica(&path_b)?;
    Ok(())
}

/// AE1: a late third replica converges to absent, never resurrect.
#[test]
fn delete_late_replica_converges_to_absent() -> Result<(), Box<dyn std::error::Error>> {
    // Given: one replica that edited then deleted a document, already in the hub.
    let path_a = temp_path("ae1-late-a");
    let path_c = temp_path("ae1-late-c");
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let replica_a = ReplicaId::new();
    let replica_c = ReplicaId::new();
    let peer = PeerId::new();
    let document = DocumentId::new();
    let hub = shared_hub();
    let mut transport_a = HubTransport {
        hub: Arc::clone(&hub),
        replica: replica_a,
    };
    let mut transport_c = HubTransport {
        hub: Arc::clone(&hub),
        replica: replica_c,
    };
    let database_a = Database::open(&path_a, scope, replica_a)?;
    database_a.put_value(document, "status", Scalar::String(String::from("shared")))?;
    database_a.delete(document, OperationId::new(), delete_request(document))?;
    sync_once(&database_a, peer, &mut transport_a)?;

    // When: a late third replica syncs the full history.
    let database_c = Database::open(&path_c, scope, replica_c)?;
    sync_once(&database_c, peer, &mut transport_c)?;

    // Then: the late replica converges to absent and cannot resurrect the value.
    assert_eq!(
        database_c.get_value(document, "status")?,
        None,
        "a late replica must converge to absent, never resurrect the deleted value"
    );
    assert!(
        database_c.is_deleted(document)?,
        "a late replica must observe the tombstone as a first-class deletion"
    );
    drop(database_a);
    drop(database_c);
    remove_replica(&path_a)?;
    remove_replica(&path_c)?;
    Ok(())
}

/// AE2: checkpoint-expired pull recovers full state, never a partial prefix.
#[test]
fn pull_checkpoint_expired_recovers_full_state() -> Result<(), Box<dyn std::error::Error>> {
    // Given: a second replica that already holds an early prefix and a hub
    // whose cursors then expire behind the retention floor.
    let (first, second, peer, hub, mut transport_a, mut transport_b) =
        ReplicaPair::open("ae2")?.into_parts_with_hub();
    let (database_a, path_a) = first;
    let (database_b, path_b) = second;
    let early = DocumentId::new();
    let late_one = DocumentId::new();
    let late_two = DocumentId::new();
    database_a.put_value(early, "status", Scalar::String(String::from("early")))?;
    sync_once(&database_a, peer, &mut transport_a)?;
    sync_once(&database_b, peer, &mut transport_b)?;
    database_a.put_value(late_one, "status", Scalar::String(String::from("late-one")))?;
    database_a.put_value(late_two, "status", Scalar::String(String::from("late-two")))?;
    // The late documents must reach the shared log before the cursor expires,
    // otherwise a full resync correctly recovers nothing that was never pushed.
    sync_once(&database_a, peer, &mut transport_a)?;
    {
        let mut shared = hub.lock().map_err(|_poisoned| HubError::Poisoned)?;
        shared.expire_cursors = true;
    }

    // When: the second replica pulls past the retention floor.
    let report = sync_once(&database_b, peer, &mut transport_b)?;

    // Then: recovery is explicit and the replica holds full state, not a prefix.
    assert!(
        report.full_resync,
        "a checkpoint-expired pull must surface an explicit full resync"
    );
    assert_eq!(
        database_b.get_value(early, "status")?,
        Some(Scalar::String(String::from("early"))),
        "recovery must retain the early document, never drop it as a partial prefix"
    );
    assert_eq!(
        database_b.get_value(late_one, "status")?,
        Some(Scalar::String(String::from("late-one"))),
        "recovery must install the first late document"
    );
    assert_eq!(
        database_b.get_value(late_two, "status")?,
        Some(Scalar::String(String::from("late-two"))),
        "recovery must install the second late document"
    );
    drop(database_a);
    drop(database_b);
    remove_replica(&path_a)?;
    remove_replica(&path_b)?;
    Ok(())
}

/// AE7: a permanent transport failure surfaces as a typed `SyncError`.
#[test]
fn synchronize_permanent_failure_surfaces_typed_error() -> Result<(), Box<dyn std::error::Error>> {
    // Given: an open replica and a transport that fails permanently on push.
    let path = temp_path("ae7-permanent");
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let replica = ReplicaId::new();
    let peer = PeerId::new();
    let database = Database::open(&path, scope, replica)?;
    let document = DocumentId::new();
    database.put_value(document, "status", Scalar::String(String::from("live")))?;
    let mut transport = FailingTransport {
        reason: "push refused",
    };

    // When: the replica synchronizes against the failing transport.
    let runtime = lgwks_bot::Runtime::new()?;
    let observed =
        runtime
            .block_on(database.synchronize(peer, &mut transport, SyncLimits::default(), || false));

    // Then: the failure surfaces as a typed permanent transport error.
    match observed {
        Err(SyncError::Transport(TransportFailure::Permanent { reason })) => {
            assert_eq!(
                reason, "push refused",
                "the typed failure must carry its stable reason string"
            );
        }
        other => {
            return Err(format!(
                "a permanent transport failure must surface as SyncError::Transport; observed {other:?}"
            )
            .into());
        }
    }
    drop(database);
    remove_replica(&path)?;
    Ok(())
}

/// ER-T05: a full embeddable journey runs through `logical_db` imports only.
#[test]
fn import_boundary_journey_completes() -> Result<(), Box<dyn std::error::Error>> {
    // Given: a fresh app project that names only `logical_db` paths.
    use logical_db::{
        AccountId, Database, DocumentId, EditOutcome, Object, OperationId, ReplicaId, Scalar,
        Scope, SpaceId,
        cache::{CacheEntry, KvCache},
        content_digest,
    };
    // When: the app runs open, exact-retry edit, read, export, restore, cache.
    let (path, scope, replica, database) = fresh_replica("import-boundary")?;
    let CommittedStatus {
        receipt: first,
        document,
        operation,
        request,
    } = committed_status(&database, "ready")?;
    let second = database.edit(document, operation, request, |editor| {
        put_status(editor, &Object::root(), "HIT")
    })?;
    let read = database.get_value(document, "status")?;
    let payload = database.export()?;
    let restore_path = temp_path("import-boundary-restore");
    let restore = Database::open(&restore_path, scope, ReplicaId::new())?;
    let report = restore.restore(&payload)?;
    let cache_path = temp_path("import-boundary-cache");
    let cache = KvCache::open(&cache_path, scope, ReplicaId::new(), "app")?;
    cache.put("k", b"v", None)?;
    let entries = cache.scan("", None, 16)?;

    // Then: every stage of the journey returns its exact contract value.
    let (first_sequence, first_digest) = match first {
        EditOutcome::Applied(receipt) => (receipt.sequence, receipt.digest),
        other => return Err(format!("the journey edit must apply; observed {other:?}").into()),
    };
    let (second_sequence, second_digest) = match second {
        EditOutcome::Duplicate(duplicate) => (duplicate.sequence, duplicate.digest),
        other => {
            return Err(format!("the journey retry must duplicate; observed {other:?}").into());
        }
    };
    assert_eq!(
        second_sequence, first_sequence,
        "the import-boundary journey must observe exact-retry identity"
    );
    assert_eq!(
        second_digest, first_digest,
        "the import-boundary journey must observe the original change digest"
    );
    assert_eq!(
        read,
        Some(Scalar::String(String::from("ready"))),
        "the import-boundary journey must read the committed value"
    );
    assert_eq!(
        report.documents, 1,
        "the import-boundary restore must carry exactly one document"
    );
    assert_eq!(
        restore.get_value(document, "status")?,
        Some(Scalar::String(String::from("ready"))),
        "the import-boundary restore must reproduce the committed value"
    );
    assert_eq!(
        entries.len(),
        1,
        "the import-boundary cache must expose exactly one entry"
    );
    assert_eq!(
        entries[0],
        CacheEntry::new(String::from("k"), b"v".to_vec()),
        "the import-boundary cache entry must carry its exact key and value"
    );
    drop(database);
    drop(restore);
    drop(cache);
    remove_replica(&path)?;
    remove_replica(&restore_path)?;
    remove_replica(&cache_path)?;
    Ok(())
}
