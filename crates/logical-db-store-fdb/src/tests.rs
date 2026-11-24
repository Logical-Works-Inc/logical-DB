use logical_db_model::{AccountId, SpaceId};
use std::collections::BTreeMap;

use super::*;

#[test]
/// Tuple axis is scope isolated and sequence ordered
fn tuple_axis_is_scope_isolated_and_sequence_ordered() {
    let root = Subspace::from_bytes(b"directory-allocated/".to_vec());
    let first = Scope::new(AccountId::new(), SpaceId::new());
    let second = Scope::new(AccountId::new(), SpaceId::new());
    let first_log = root.subspace(&(
        FORMAT_VERSION,
        tuple_id(first.account().as_uuid()),
        tuple_id(first.space().as_uuid()),
        KIND_LOG,
    ));
    let second_log = root.subspace(&(
        FORMAT_VERSION,
        tuple_id(second.account().as_uuid()),
        tuple_id(second.space().as_uuid()),
        KIND_LOG,
    ));
    assert_ne!(first_log.bytes(), second_log.bytes());
    assert!(first_log.pack(&(1_u64,)) < first_log.pack(&(2_u64,)));
    assert!(first_log.is_start_of(&first_log.pack(&(99_u64,))));
    assert!(!first_log.is_start_of(&second_log.pack(&(1_u64,))));
}

#[test]
/// RFC 9562 section 4: an identifier keeps its sixteen bytes across the
/// estate and tuple-layer types, so every packed key written before the
/// estate `Uuid` migration still addresses the same record.
fn estate_and_tuple_ids_round_trip_byte_identically() {
    let account = AccountId::new().as_uuid();
    let tuple = tuple_id(account);
    assert_eq!(tuple.as_bytes(), account.as_bytes());
    assert_eq!(estate_id(tuple), account);
    let fixed = lgwks_std::id::Uuid::from([0xa5; 16]);
    assert_eq!(
        Subspace::from_bytes(b"k/".to_vec()).pack(&(tuple_id(fixed),)),
        Subspace::from_bytes(b"k/".to_vec())
            .pack(&(foundationdb_tuple::Uuid::from_bytes([0xa5; 16]),))
    );
}

/// Change
fn change(byte_len: usize) -> ChangeEnvelope {
    ChangeEnvelope::new(
        Scope::new(AccountId::new(), SpaceId::new()),
        DocumentId::new(),
        ReplicaId::new(),
        OperationId::new(),
        OperationDigest::new([7; 32]),
        ChangeDigest::new([9; 32]),
        vec![3; byte_len],
    )
}

#[test]
/// Manifest round trip preserves every axis and rejects each rebinding
fn manifest_round_trip_preserves_every_axis_and_rejects_each_rebinding()
-> Result<(), Box<dyn std::error::Error>> {
    let change = change(CHUNK_BYTES + 1);
    let invocation_id = [0x5a; 32];
    let binding = invocation_binding(&change);
    assert_eq!(&binding[..16], change.operation().as_uuid().as_bytes());
    assert_eq!(&binding[16..48], &change.request().into_bytes());
    assert_eq!(&binding[48..], &change.digest().into_bytes());
    let encoded = encode_manifest(&change, 41, invocation_id)?;
    assert_ne!(encoded, [0; OPERATION_BYTES]);
    let operation = decode_operation(&encoded)?;
    assert_eq!(operation.invocation_id, invocation_id);
    let record = operation.record;
    assert_eq!(record.sequence, 41);
    assert!(record.matches(&change));
    assert_eq!(
        record.envelope(change.scope(), change.bytes().to_vec()),
        change
    );
    assert_eq!(
        decode_manifest(&encoded[..MANIFEST_BYTES], 99)?.sequence,
        99
    );

    let (document, replica, operation) = (change.document(), change.replica(), change.operation());
    let (request, digest, bytes) = (change.request(), change.digest(), change.bytes().to_vec());
    let rebind = |doc, rep, op, req, dig, body| {
        ChangeEnvelope::new(change.scope(), doc, rep, op, req, dig, body)
    };
    let shorter = bytes
        .get(1..)
        .map(<[u8]>::to_vec)
        .ok_or("change has bytes")?;
    for altered in [
        rebind(
            DocumentId::new(),
            replica,
            operation,
            request,
            digest,
            bytes.clone(),
        ),
        rebind(
            document,
            ReplicaId::new(),
            operation,
            request,
            digest,
            bytes.clone(),
        ),
        rebind(
            document,
            replica,
            OperationId::new(),
            request,
            digest,
            bytes.clone(),
        ),
        rebind(
            document,
            replica,
            operation,
            OperationDigest::new([8; 32]),
            digest,
            bytes.clone(),
        ),
        rebind(
            document,
            replica,
            operation,
            request,
            ChangeDigest::new([10; 32]),
            bytes,
        ),
        rebind(document, replica, operation, request, digest, shorter),
    ] {
        assert!(!record.matches(&altered), "a rebound axis must not match");
    }
    assert!(
        !Record {
            chunks: record.chunks + 1,
            ..record
        }
        .matches(&change)
    );
    Ok(())
}

#[test]
/// Page limits and frontiers fail closed without a cluster
fn page_limits_and_frontiers_fail_closed_without_a_cluster()
-> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(CHUNK_BYTES, 65_536);
    assert!(valid_page_limits(1, 1));
    assert!(valid_page_limits(
        u16::try_from(MAX_CHANGES_PER_PAGE)?,
        u32::try_from(MAX_PAGE_BYTES)?
    ));
    assert!(!valid_page_limits(0, 1));
    assert!(!valid_page_limits(1, 0));
    assert!(!valid_page_limits(
        u16::try_from(MAX_CHANGES_PER_PAGE + 1)?,
        1
    ));
    assert!(!valid_page_limits(1, u32::try_from(MAX_PAGE_BYTES + 1)?));
    assert_eq!(MANIFEST_BYTES, 118);
    assert_eq!(OPERATION_BYTES, 158);
    assert_eq!(
        page_read_plan(256),
        PageReadPlan {
            limit: 257,
            target_bytes: 30_208
        }
    );
    assert_eq!(logical_page_more(10, 10, true, false), Ok(false));
    assert_eq!(logical_page_more(10, 9, true, false), Ok(true));
    assert_eq!(logical_page_more(10, 9, false, true), Ok(true));
    assert_eq!(
        logical_page_more(10, 11, false, false),
        Err(DataError::InvalidManifest { at: "manifest" })
    );
    assert_eq!(
        logical_page_more(10, 10, false, true),
        Err(DataError::InvalidManifest { at: "manifest" })
    );
    assert_eq!(
        logical_page_more(10, 9, false, false),
        Err(DataError::InvalidManifest { at: "manifest" })
    );
    Ok(())
}

#[test]
/// Manifest chunk bounds reject inconsistent lengths
fn manifest_chunk_bounds_reject_inconsistent_lengths() -> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(chunk_count(0), Some(0));
    assert_eq!(chunk_count(1), Some(1));
    assert_eq!(chunk_count(CHUNK_BYTES), Some(1));
    assert_eq!(chunk_count(CHUNK_BYTES + 1), Some(2));
    assert_eq!(
        chunk_count(CHUNK_BYTES * usize::from(u16::MAX)),
        Some(u16::MAX)
    );
    assert_eq!(chunk_count(CHUNK_BYTES * usize::from(u16::MAX) + 1), None);
    assert_eq!(chunk_count(usize::MAX), None);
    assert_eq!(
        encode_manifest(&change(0), 1, [1; 32]),
        Err(DataError::InvalidManifest { at: "manifest" })
    );
    assert_eq!(
        encode_manifest(&change(MAX_CHANGE_BYTES + 1), 1, [1; 32]),
        Err(DataError::InvalidManifest { at: "manifest" })
    );
    let encoded = encode_manifest(&change(1), u64::MAX, [2; 32])?;
    assert_eq!(decode_operation(&encoded)?.record.sequence, u64::MAX);
    assert_eq!(
        decode_operation(&encoded[..OPERATION_BYTES - 1]),
        Err(DataError::InvalidManifest { at: "manifest" })
    );
    assert_eq!(
        decode_manifest(&encoded[..MANIFEST_BYTES - 1], 1),
        Err(DataError::InvalidManifest { at: "manifest" })
    );
    let mut zero_length = encoded;
    zero_length[112..116].copy_from_slice(&0_u32.to_be_bytes());
    zero_length[116..118].copy_from_slice(&0_u16.to_be_bytes());
    assert_eq!(
        decode_operation(&zero_length),
        Err(DataError::InvalidManifest { at: "manifest" })
    );
    let mut zero_chunks = encoded;
    zero_chunks[116..118].copy_from_slice(&0_u16.to_be_bytes());
    assert_eq!(
        decode_operation(&zero_chunks),
        Err(DataError::InvalidManifest { at: "manifest" })
    );
    let mut wrong_chunks = encoded;
    wrong_chunks[116..118].copy_from_slice(&2_u16.to_be_bytes());
    assert_eq!(
        decode_operation(&wrong_chunks),
        Err(DataError::InvalidManifest { at: "manifest" })
    );
    let mut hostile_length = encoded;
    hostile_length[112..116].copy_from_slice(&u32::MAX.to_be_bytes());
    hostile_length[116..118].copy_from_slice(&u16::MAX.to_be_bytes());
    assert_eq!(
        decode_operation(&hostile_length),
        Err(DataError::InvalidManifest { at: "manifest" })
    );
    Ok(())
}

#[test]
/// Scalar record codecs reject truncation
fn scalar_record_codecs_reject_truncation() {
    assert_eq!(decode_u64(&41_u64.to_be_bytes()), Ok(41));
    assert_eq!(
        decode_u64(&[0; 7]),
        Err(DataError::InvalidManifest { at: "manifest" })
    );
    assert_eq!(
        decode_u64(&[0; 9]),
        Err(DataError::InvalidManifest { at: "manifest" })
    );
    let mut fields = Fields(&[1, 2, 3]);
    assert_eq!(fields.take::<2>(), Some([1, 2]), "a full field reads");
    assert!(!fields.is_done(), "one byte is left");
    assert_eq!(fields.take::<2>(), None, "a short field is refused");
    assert_eq!(fields.take::<1>(), Some([3]), "a refusal consumes nothing");
    assert!(fields.is_done(), "every byte was read");
}

#[test]
/// Retention record codec is versioned and fail closed
fn retention_record_codec_is_versioned_and_fail_closed() {
    assert_eq!(RETENTION_BYTES, 18);
    assert_eq!(RETENTION_VERSION, 1);
    let encoded = encode_retention(7, 9);
    assert_eq!(decode_retention(&encoded), Ok((7, 9)));
    assert_eq!(
        decode_retention(&[0; 18]),
        Err(DataError::InvalidManifest { at: "manifest" })
    );
    assert_eq!(
        decode_retention(&encoded[..RETENTION_BYTES - 1]),
        Err(DataError::InvalidManifest { at: "manifest" })
    );
    let mut unknown = encoded;
    unknown[..2].copy_from_slice(&(RETENTION_VERSION + 1).to_be_bytes());
    assert_eq!(
        decode_retention(&unknown),
        Err(DataError::InvalidManifest { at: "manifest" })
    );
    let mut regressed = encoded;
    regressed[2..10].copy_from_slice(&u64::MAX.to_be_bytes());
    regressed[10..18].copy_from_slice(&0_u64.to_be_bytes());
    assert_eq!(decode_retention(&regressed), Ok((u64::MAX, 0)));
}

#[test]
/// Snapshot record round trips through the versioned model codec
fn snapshot_record_round_trips_through_the_versioned_model_codec()
-> Result<(), Box<dyn std::error::Error>> {
    use logical_db_model::SnapshotBaseline;

    let baseline = SnapshotBaseline::new(41, logical_db_model::BaselineDigest::new([7; 32]), 0, 0);
    let encoded = baseline.encode();
    assert_eq!(
        decode_snapshot(&encoded),
        Ok(SnapshotRecord::State(baseline))
    );
    let (_, truncated) = encoded
        .split_last()
        .ok_or("an encoded baseline has bytes")?;
    assert_eq!(
        decode_snapshot(truncated),
        Err(DataError::InvalidManifest { at: "manifest" })
    );
    let mut foreign = encoded;
    foreign[..2].copy_from_slice(&0_u16.to_be_bytes());
    assert_eq!(
        decode_snapshot(&foreign),
        Err(DataError::InvalidManifest { at: "manifest" })
    );
    Ok(())
}

#[test]
/// State baseline atlas rejects missing or corrupt chunks
fn state_baseline_atlas_rejects_missing_or_corrupt_chunks() -> Result<(), Box<dyn std::error::Error>>
{
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let document = SnapshotDocument::new(DocumentId::new(), b"bounded snapshot".to_vec(), false);
    let artifact = SnapshotArtifact::new(scope, 7, vec![document.clone()])?;
    let digest = artifact.baseline().digest().into_bytes();
    let manifest = encode_baseline_document(&document)?;
    assert_eq!(manifest.len(), BASELINE_DOCUMENT_MANIFEST_BYTES);
    assert_eq!(
        decode_baseline_document(&manifest[..BASELINE_DOCUMENT_MANIFEST_BYTES - 1]),
        Err(DataError::BaselineCorrupt)
    );
    let mut atlas = BaselineAtlas {
        manifests: BTreeMap::from([((digest, document.document()), manifest.to_vec())]),
        chunks: BTreeMap::from([(
            (digest, document.document(), 0),
            document.snapshot().to_vec(),
        )]),
    };
    assert_eq!(
        validate_baseline_atlas(scope, artifact.baseline(), &atlas),
        Ok(artifact.clone())
    );
    let Some(chunk) = atlas.chunks.remove(&(digest, document.document(), 0)) else {
        return Err(std::io::Error::other("baseline fixture chunk must exist").into());
    };
    assert_eq!(
        validate_baseline_atlas(scope, artifact.baseline(), &atlas),
        Err(DataError::BaselineCorrupt)
    );
    atlas.chunks.insert((digest, document.document(), 0), chunk);
    let Some(corruptible) = atlas.chunks.get_mut(&(digest, document.document(), 0)) else {
        return Err(
            std::io::Error::other("baseline fixture chunk must exist after reinsert").into(),
        );
    };
    corruptible.push(0xff);
    assert_eq!(
        validate_baseline_atlas(scope, artifact.baseline(), &atlas),
        Err(DataError::BaselineCorrupt)
    );
    Ok(())
}

/// CollectionAtlas
struct CollectionAtlas {
    /// manifests
    manifests: BTreeMap<u64, Vec<u8>>,
    /// Chunks
    chunks: BTreeMap<(u64, u16), Vec<u8>>,
    /// operations
    operations: BTreeMap<OperationId, Vec<u8>>,
    /// invocations
    invocations: BTreeMap<[u8; 32], Vec<u8>>,
}

/// Build atlas
fn build_atlas(
    sequences: &[u64],
) -> Result<(CollectionAtlas, Vec<Record>), Box<dyn std::error::Error>> {
    let mut atlas = CollectionAtlas {
        manifests: BTreeMap::new(),
        chunks: BTreeMap::new(),
        operations: BTreeMap::new(),
        invocations: BTreeMap::new(),
    };
    let mut records = Vec::new();
    for (index, sequence) in sequences.iter().enumerate() {
        let byte_len = index.saturating_add(3);
        let change = change(byte_len);
        let tag = u8::try_from(index)?;
        let invocation_id = [tag.wrapping_add(1); 32];
        let manifest = encode_manifest(&change, *sequence, invocation_id)?;
        let record = decode_operation(&manifest)?.record;
        let mut remaining = byte_len;
        for chunk_index in 0..record.chunks {
            let take = remaining.min(CHUNK_BYTES);
            atlas
                .chunks
                .insert((*sequence, chunk_index), vec![tag.wrapping_add(9); take]);
            remaining = remaining.saturating_sub(take);
        }
        atlas
            .manifests
            .insert(*sequence, manifest[..MANIFEST_BYTES].to_vec());
        atlas.operations.insert(record.operation, manifest.to_vec());
        atlas
            .invocations
            .insert(invocation_id, invocation_binding(&change).to_vec());
        records.push(record);
    }
    Ok((atlas, records))
}

/// Atlas view
fn atlas_view(atlas: &CollectionAtlas) -> CollectionView<'_> {
    CollectionView {
        manifests: &atlas.manifests,
        chunks: &atlas.chunks,
        operations: &atlas.operations,
        invocations: &atlas.invocations,
    }
}

#[test]
/// Prune plan keeps idempotency axes while dropping the pruned prefix
fn prune_plan_keeps_idempotency_axes_while_dropping_the_pruned_prefix()
-> Result<(), Box<dyn std::error::Error>> {
    use logical_db_service::{PruneRefusal, prune_decision};

    let (mut atlas, _records) = build_atlas(&[1, 2, 3, 4])?;
    assert_eq!(check_collection_consistency(&atlas_view(&atlas)), Ok(()));
    assert_eq!(prune_decision(4, 0, 2, None, 4), Ok(2));
    assert_eq!(
        prune_decision(4, 0, 2, None, 1),
        Err(PruneRefusal::PendingOperations {
            consumed_through: 1
        })
    );
    atlas.manifests.remove(&1);
    atlas.manifests.remove(&2);
    atlas.chunks.retain(|&(sequence, _), _| sequence > 2u64);
    assert!(!atlas.manifests.contains_key(&1));
    assert!(!atlas.manifests.contains_key(&2));
    assert!(atlas.chunks.keys().all(|&(sequence, _)| sequence > 2u64));
    assert_eq!(atlas.operations.len(), 4);
    assert_eq!(atlas.invocations.len(), 4);
    assert_eq!(check_collection_consistency(&atlas_view(&atlas)), Ok(()));
    Ok(())
}

#[test]
/// Collection consistency checker rejects each broken cross reference
fn collection_consistency_checker_rejects_each_broken_cross_reference()
-> Result<(), Box<dyn std::error::Error>> {
    let (mut atlas, records) = build_atlas(&[1, 2])?;
    let second = records
        .get(1)
        .ok_or("the atlas holds two records")?
        .operation;
    assert_eq!(check_collection_consistency(&atlas_view(&atlas)), Ok(()));
    let missing_chunk = atlas.chunks.remove(&(2, 0)).ok_or("expected Some")?;
    assert_eq!(
        check_collection_consistency(&atlas_view(&atlas)),
        Err(DataError::MissingChunk)
    );
    atlas.chunks.insert((2, 0), missing_chunk);
    assert_eq!(check_collection_consistency(&atlas_view(&atlas)), Ok(()));
    let missing_operation = atlas.operations.remove(&second).ok_or("expected Some")?;
    assert_eq!(
        check_collection_consistency(&atlas_view(&atlas)),
        Err(DataError::InvalidManifest { at: "manifest" })
    );
    atlas.operations.insert(second, missing_operation);
    assert_eq!(check_collection_consistency(&atlas_view(&atlas)), Ok(()));
    atlas.manifests.insert(2, vec![7; MANIFEST_BYTES]);
    assert_eq!(
        check_collection_consistency(&atlas_view(&atlas)),
        Err(DataError::InvalidManifest { at: "manifest" })
    );
    Ok(())
}
