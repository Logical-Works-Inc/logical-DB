//! Sealed baseline artifacts: seal, write, reconstruct, verify and page reads.

use foundationdb::{FdbBindingError, KeySelector, RangeOption};
use foundationdb_tuple::Subspace;
use logical_db_model::{
    DocumentId, MAX_BASELINE_BYTES, MAX_BASELINE_PAGE_DOCUMENTS, Scope, SnapshotArtifact,
    SnapshotBaseline, SnapshotDocument, SnapshotRecord,
};
use logical_db_protocol::ChangeEnvelope;
use logical_db_service::{
    BaselinePageSlice, SealResult, SnapshotMaterializationError, materialize_snapshot,
};

use super::transaction::{
    binding, binding_baseline, binding_invalid, configure_transaction, end_after,
    load_change_bytes, read_frontier, verify_operation,
};
use super::{
    BaselineSpaces, KIND_CHUNK, KIND_SNAPSHOT, KIND_SNAPSHOT_CHUNK, KIND_SNAPSHOT_DOCUMENT,
    SealOperation,
};
use crate::{
    BaselineDocumentManifest, CHUNK_BYTES, DataError, KIND_LOG, MAX_BASELINE_CHANGES,
    decode_baseline_document, decode_manifest, decode_snapshot, encode_baseline_document,
    estate_id, tuple_id,
};

/// Runs `seal_transaction`.
pub(super) async fn seal_transaction(
    transaction: &foundationdb::RetryableTransaction,
    operation: SealOperation,
) -> Result<SealResult, FdbBindingError> {
    configure_transaction(transaction)?;
    let frontier = read_frontier(transaction, &operation.sequence_key).await?;
    if frontier != operation.expected_frontier {
        return Ok(SealResult::StaleAuthority);
    }
    if operation.position > frontier {
        return Ok(SealResult::BeyondFrontier { frontier });
    }
    let existing = read_snapshot_record(transaction, &operation.spaces.pointer_key).await?;
    let existing_position = match existing.as_ref() {
        Some(record) => match *record {
            SnapshotRecord::LegacyPosition(position) => position,
            SnapshotRecord::State(ref state) => state.position(),
            // A future descriptor class cannot authorize a seal; fail closed
            // as corrupt rather than guessing its coverage position.
            _ => return Err(binding(DataError::BaselineCorrupt)),
        },
        None => 0,
    };
    if existing_position > operation.position {
        return Err(binding(DataError::BaselineCoverage));
    }
    let rebuilt = reconstruct_artifact(
        transaction,
        operation.scope,
        operation.position,
        &operation.spaces,
    )
    .await?;
    if rebuilt != operation.artifact {
        return Err(binding(DataError::BaselineCoverage));
    }
    if let Some(SnapshotRecord::State(previous)) = existing
        && previous.digest() != operation.artifact.baseline().digest()
    {
        clear_artifact(transaction, previous.digest(), &operation.spaces);
    }
    write_artifact(transaction, &operation.artifact, &operation.spaces)?;
    Ok(SealResult::Sealed {
        position: operation.position,
    })
}

/// Baseline spaces
pub(super) fn baseline_spaces(scope: &Subspace) -> BaselineSpaces {
    BaselineSpaces {
        scope: scope.clone(),
        pointer_key: scope.pack(&(KIND_SNAPSHOT,)),
        log: scope.subspace(&(KIND_LOG,)),
        change_chunk: scope.subspace(&(KIND_CHUNK,)),
        document: scope.subspace(&(KIND_SNAPSHOT_DOCUMENT,)),
        snapshot_chunk: scope.subspace(&(KIND_SNAPSHOT_CHUNK,)),
    }
}

/// Write artifact
pub(super) fn write_artifact(
    transaction: &foundationdb::RetryableTransaction,
    artifact: &SnapshotArtifact,
    spaces: &BaselineSpaces,
) -> Result<(), FdbBindingError> {
    let digest = artifact.baseline().digest().into_bytes();
    for document in artifact.documents() {
        let manifest = encode_baseline_document(document).map_err(binding)?;
        let id = tuple_id(document.document().as_uuid());
        let key = spaces.document.pack(&(digest.as_slice(), id));
        transaction.set(&key, &manifest);
        for (index, chunk) in document.snapshot().chunks(CHUNK_BYTES).enumerate() {
            let index = u16::try_from(index).map_err(binding_invalid)?;
            let key = spaces.snapshot_chunk.pack(&(digest.as_slice(), id, index));
            transaction.set(&key, chunk);
        }
    }
    transaction.set(&spaces.pointer_key, &artifact.baseline().encode());
    Ok(())
}

/// Clear artifact
pub(super) fn clear_artifact(
    transaction: &foundationdb::RetryableTransaction,
    digest: logical_db_model::BaselineDigest,
    spaces: &BaselineSpaces,
) {
    let digest = digest.into_bytes();
    for space in [&spaces.document, &spaces.snapshot_chunk] {
        let artifact = space.subspace(&(digest.as_slice(),));
        let (begin, end) = artifact.range();
        transaction.clear_range(&begin, &end);
    }
}

/// Runs `reconstruct_artifact`.
pub(super) async fn reconstruct_artifact(
    transaction: &foundationdb::RetryableTransaction,
    scope: Scope,
    through: u64,
    spaces: &BaselineSpaces,
) -> Result<SnapshotArtifact, FdbBindingError> {
    let mut position = 0_u64;
    let mut replayed_bytes = 0_usize;
    let mut base = None;
    if let Some(record) = read_snapshot_record(transaction, &spaces.pointer_key).await?
        && let SnapshotRecord::State(baseline) = record
        && baseline.position() <= through
    {
        let artifact = load_artifact(transaction, scope, &baseline, spaces).await?;
        position = baseline.position();
        base = Some(artifact);
    }
    let end = end_after(&spaces.log, through);
    // A single range read may be truncated by the client before either the
    // row or byte budget is reached, so continue from the last sequence
    // until the range is exhausted or the change budget is exceeded.
    let mut manifests = Vec::new();
    let mut next_sequence = position.saturating_add(1);
    while manifests.len() <= MAX_BASELINE_CHANGES {
        let begin = spaces.log.pack(&(next_sequence,));
        let remaining = MAX_BASELINE_CHANGES
            .saturating_add(1)
            .saturating_sub(manifests.len());
        let options = RangeOption {
            begin: KeySelector::first_greater_or_equal(begin.as_slice()),
            end: KeySelector::first_greater_or_equal(end.as_slice()),
            limit: Some(remaining.max(1)),
            target_bytes: MAX_BASELINE_BYTES,
            ..RangeOption::default()
        };
        let values = transaction.get_range(&options, 1, false).await?;
        let more = values.more();
        manifests.extend(
            values
                .iter()
                .map(|value| (value.key().to_vec(), value.value().to_vec())),
        );
        if manifests.len() > MAX_BASELINE_CHANGES {
            return Err(binding(DataError::BaselineResourceLimit));
        }
        if !more {
            break;
        }
        let Some(last_pair) = manifests.last() else {
            return Err(binding(DataError::BaselineResourceLimit));
        };
        let last = &last_pair.0;
        let (sequence,) = spaces.log.unpack::<(u64,)>(last).map_err(binding_invalid)?;
        next_sequence = sequence.saturating_add(1);
    }
    if manifests.len() > MAX_BASELINE_CHANGES {
        return Err(binding(DataError::BaselineResourceLimit));
    }
    let mut changes = Vec::with_capacity(manifests.len());
    for (key, value) in manifests {
        let (sequence, change, next_bytes) = load_baseline_change(
            transaction,
            spaces,
            scope,
            position,
            replayed_bytes,
            &key,
            &value,
        )
        .await?;
        changes.push(change);
        position = sequence;
        replayed_bytes = next_bytes;
    }
    if position != through {
        return Err(binding(DataError::BaselineCoverage));
    }
    materialize_snapshot(scope, through, base.as_ref(), &changes)
        .map_err(|error| materialization_failure(&error))
}

/// Classifies a materialization failure into the stable FDB data class a
/// caller retries, resizes, or treats as corruption.
fn materialization_failure(error: &SnapshotMaterializationError) -> FdbBindingError {
    use SnapshotMaterializationError as Failure;
    binding(match *error {
        Failure::Coverage | Failure::CountOverflow(_) => DataError::BaselineCoverage,
        Failure::ResourceLimit(_) | Failure::Encode(_) => DataError::BaselineResourceLimit,
        Failure::InvalidBase(_)
        | Failure::InvalidBaseDocument { .. }
        | Failure::TombstoneMismatch
        | Failure::ScopeMismatch
        | Failure::InvalidEnvelope(_)
        | Failure::InvalidChange { .. } => DataError::BaselineCorrupt,
        // A class added later in the defining crate fails closed as corrupt.
        _ => DataError::BaselineCorrupt,
    })
}

/// Runs `load_baseline_change`.
pub(super) async fn load_baseline_change(
    transaction: &foundationdb::RetryableTransaction,
    spaces: &BaselineSpaces,
    scope: Scope,
    previous: u64,
    replayed_bytes: usize,
    key: &[u8],
    value: &[u8],
) -> Result<(u64, ChangeEnvelope, usize), FdbBindingError> {
    let (sequence,) = spaces.log.unpack::<(u64,)>(key).map_err(binding_invalid)?;
    if previous.checked_add(1) != Some(sequence) {
        return Err(binding(DataError::BaselineCoverage));
    }
    let record = decode_manifest(value, sequence).map_err(binding)?;
    let next_bytes = replayed_bytes
        .checked_add(record.byte_len)
        .filter(|total| *total <= MAX_BASELINE_BYTES)
        .ok_or_else(|| binding(DataError::BaselineResourceLimit))?;
    verify_operation(transaction, &spaces.scope, &record).await?;
    let bytes = load_change_bytes(transaction, &spaces.change_chunk, sequence, &record).await?;
    Ok((sequence, record.envelope(scope, bytes), next_bytes))
}

/// Runs `read_snapshot_record`.
pub(super) async fn read_snapshot_record(
    transaction: &foundationdb::RetryableTransaction,
    pointer_key: &[u8],
) -> Result<Option<SnapshotRecord>, FdbBindingError> {
    transaction
        .get(pointer_key, false)
        .await?
        .map(|bytes| decode_snapshot(bytes.as_ref()).map_err(binding))
        .transpose()
}

/// Runs `scan_baseline_manifests`.
pub(super) async fn scan_baseline_manifests(
    transaction: &foundationdb::RetryableTransaction,
    space_begin: &[u8],
    space_end: &[u8],
    expected_count: usize,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, FdbBindingError> {
    let mut manifests = Vec::new();
    let mut cursor = space_begin.to_vec();
    let mut exclusive = false;
    while manifests.len() <= expected_count {
        let remaining = expected_count
            .saturating_add(1)
            .saturating_sub(manifests.len());
        let begin = if exclusive {
            KeySelector::first_greater_than(cursor.as_slice())
        } else {
            KeySelector::first_greater_or_equal(cursor.as_slice())
        };
        let options = RangeOption {
            begin,
            end: KeySelector::first_greater_or_equal(space_end),
            limit: Some(remaining.max(1)),
            target_bytes: MAX_BASELINE_BYTES,
            ..RangeOption::default()
        };
        let values = transaction.get_range(&options, 1, false).await?;
        let more = values.more();
        let last = values.iter().last().map(|value| value.key().to_vec());
        manifests.extend(
            values
                .iter()
                .map(|value| (value.key().to_vec(), value.value().to_vec())),
        );
        if manifests.len() > expected_count {
            return Err(binding(DataError::BaselineCorrupt));
        }
        if !more {
            break;
        }
        let Some(last) = last else {
            return Err(binding(DataError::BaselineCorrupt));
        };
        cursor = last;
        exclusive = true;
    }
    if manifests.len() > expected_count {
        return Err(binding(DataError::BaselineCorrupt));
    }
    if manifests.len() != expected_count {
        return Err(binding(DataError::BaselineCorrupt));
    }
    Ok(manifests)
}

/// Runs `load_baseline_documents`.
pub(super) async fn load_baseline_documents(
    transaction: &foundationdb::RetryableTransaction,
    document_space: &foundationdb_tuple::Subspace,
    spaces: &BaselineSpaces,
    digest: [u8; 32],
    expected_bytes: usize,
    manifests: Vec<(Vec<u8>, Vec<u8>)>,
) -> Result<Vec<SnapshotDocument>, FdbBindingError> {
    let mut documents = Vec::with_capacity(manifests.len());
    let mut aggregate_bytes = 0_usize;
    for (key, value) in manifests {
        let manifest = decode_baseline_document(&value).map_err(binding)?;
        let Some(next_total) = aggregate_bytes
            .checked_add(manifest.byte_len)
            .filter(|total| *total <= MAX_BASELINE_BYTES && *total <= expected_bytes)
        else {
            return Err(binding(DataError::BaselineCorrupt));
        };
        let document =
            load_artifact_document(transaction, document_space, spaces, digest, &key, manifest)
                .await?;
        aggregate_bytes = next_total;
        documents.push(document);
    }
    if aggregate_bytes != expected_bytes {
        return Err(binding(DataError::BaselineCorrupt));
    }
    Ok(documents)
}

/// Verify baseline artifact
pub(super) fn verify_baseline_artifact(
    scope: Scope,
    baseline: &logical_db_model::SnapshotBaseline,
    documents: Vec<SnapshotDocument>,
) -> Result<SnapshotArtifact, FdbBindingError> {
    let artifact =
        SnapshotArtifact::new(scope, baseline.position(), documents).map_err(binding_baseline)?;
    if artifact.baseline() != baseline {
        return Err(binding(DataError::BaselineCorrupt));
    }
    let native = materialize_snapshot(scope, baseline.position(), Some(&artifact), &[])
        .map_err(|error| materialization_failure(&error))?;
    if native != artifact {
        return Err(binding(DataError::BaselineCorrupt));
    }
    Ok(artifact)
}

/// Runs `load_artifact`.
pub(super) async fn load_artifact(
    transaction: &foundationdb::RetryableTransaction,
    scope: Scope,
    baseline: &logical_db_model::SnapshotBaseline,
    spaces: &BaselineSpaces,
) -> Result<SnapshotArtifact, FdbBindingError> {
    let digest = baseline.digest().into_bytes();
    let document_space = spaces.document.subspace(&(digest.as_slice(),));
    let (space_begin, space_end) = document_space.range();
    let expected_count = usize::try_from(baseline.document_count()).map_err(binding_invalid)?;
    let expected_bytes = usize::try_from(baseline.byte_len()).map_err(binding_invalid)?;

    let manifests = scan_baseline_manifests(
        transaction,
        space_begin.as_slice(),
        space_end.as_slice(),
        expected_count,
    )
    .await?;

    let documents = load_baseline_documents(
        transaction,
        &document_space,
        spaces,
        digest,
        expected_bytes,
        manifests,
    )
    .await?;

    verify_baseline_artifact(scope, baseline, documents)
}

/// Runs `read_baseline_page_transaction`.
pub(super) async fn read_baseline_page_transaction(
    transaction: &foundationdb::RetryableTransaction,
    spaces: &BaselineSpaces,
    baseline: &SnapshotBaseline,
    after: Option<foundationdb_tuple::Uuid>,
) -> Result<BaselinePageSlice, FdbBindingError> {
    let digest = baseline.digest().into_bytes();
    let document_space = spaces.document.subspace(&(digest.as_slice(),));
    let (space_begin, space_end) = document_space.range();
    // No cursor starts at the first document of the artifact; a cursor
    // resumes strictly after the last document already returned.
    let begin = match after {
        Some(cursor) => KeySelector::first_greater_than(document_space.pack(&(cursor,))),
        None => KeySelector::first_greater_or_equal(space_begin),
    };
    let options = RangeOption {
        begin,
        end: KeySelector::first_greater_or_equal(space_end),
        limit: Some(MAX_BASELINE_PAGE_DOCUMENTS + 1),
        target_bytes: MAX_BASELINE_BYTES,
        ..RangeOption::default()
    };
    let values = transaction.get_range(&options, 1, false).await?;
    let more = values.len() > MAX_BASELINE_PAGE_DOCUMENTS || values.more();
    let manifests = values
        .iter()
        .take(MAX_BASELINE_PAGE_DOCUMENTS)
        .map(|value| (value.key().to_vec(), value.value().to_vec()))
        .collect::<Vec<_>>();
    drop(values);
    let mut documents = Vec::with_capacity(manifests.len());
    let mut aggregate_bytes = 0_usize;
    for (key, value) in manifests {
        let manifest = decode_baseline_document(&value).map_err(binding)?;
        aggregate_bytes = aggregate_bytes
            .checked_add(manifest.byte_len)
            .filter(|total| *total <= MAX_BASELINE_BYTES)
            .ok_or_else(|| binding(DataError::BaselineCorrupt))?;
        documents.push(
            load_artifact_document(transaction, &document_space, spaces, digest, &key, manifest)
                .await?,
        );
    }
    Ok(BaselinePageSlice::new(documents, more))
}

/// Runs `load_artifact_document`.
pub(super) async fn load_artifact_document(
    transaction: &foundationdb::RetryableTransaction,
    document_space: &Subspace,
    spaces: &BaselineSpaces,
    digest: [u8; 32],
    key: &[u8],
    manifest: BaselineDocumentManifest,
) -> Result<SnapshotDocument, FdbBindingError> {
    use lgwks_std::hash::blake3;

    let (document_id,) = document_space
        .unpack::<(foundationdb_tuple::Uuid,)>(key)
        .map_err(binding_invalid)?;
    let snapshot = load_snapshot_bytes(
        transaction,
        &spaces.snapshot_chunk,
        digest,
        document_id,
        manifest,
    )
    .await?;
    if *blake3(&snapshot).as_bytes() != manifest.digest {
        return Err(binding(DataError::BaselineCorrupt));
    }
    Ok(SnapshotDocument::new(
        DocumentId::from_uuid(estate_id(document_id)),
        snapshot,
        manifest.tombstone,
    ))
}

/// Runs `load_snapshot_bytes`.
pub(super) async fn load_snapshot_bytes(
    transaction: &foundationdb::RetryableTransaction,
    chunk_space: &Subspace,
    artifact: [u8; 32],
    document: foundationdb_tuple::Uuid,
    manifest: BaselineDocumentManifest,
) -> Result<Vec<u8>, FdbBindingError> {
    let mut bytes = Vec::with_capacity(manifest.byte_len);
    for index in 0..manifest.chunks {
        let key = chunk_space.pack(&(artifact.as_slice(), document, index));
        let chunk = transaction
            .get(&key, false)
            .await?
            .ok_or_else(|| binding(DataError::BaselineCorrupt))?;
        if chunk.is_empty() || chunk.len() > CHUNK_BYTES {
            return Err(binding(DataError::BaselineCorrupt));
        }
        bytes.extend_from_slice(chunk.as_ref());
    }
    if bytes.len() != manifest.byte_len {
        return Err(binding(DataError::BaselineCorrupt));
    }
    Ok(bytes)
}
