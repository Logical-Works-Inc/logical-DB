//! Tiered blob storage, transfer, sweep, and naming tests.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use super::*;
use logical_db::{CrdtError, Object, OperationDigest, OperationId, Scalar};

use crate::{
    blob::{content_digest, hex_digest, hex_to_digest, is_blob_name},
    request::document_for,
    tests::{TestError, cleanup, open_cache, path, scope},
};

pub(super) fn cleanup_tree(db: &std::path::Path, blobs: &std::path::Path) -> Result<(), TestError> {
    if blobs.exists() {
        std::fs::remove_dir_all(blobs)?;
    }
    if db.exists() {
        std::fs::remove_file(db)?;
    }
    Ok(())
}

/// Byte `i` is `(31 * i + 7) mod 251`, stepped in `u8` so no index
/// overflows: adding 31 modulo 251 is subtracting 220 once past 219.
fn deterministic_bytes(len: usize) -> Vec<u8> {
    std::iter::successors(Some(7_u8), |&byte| {
        Some(if byte >= 220 {
            byte.wrapping_sub(220)
        } else {
            byte.wrapping_add(31)
        })
    })
    .take(len)
    .collect()
}

/// Ships every pending writer change to `reader` as one remote page,
/// returning how many changes travelled.
pub(super) fn install_all(
    writer: &KvCache,
    reader: &KvCache,
    peer: PeerId,
) -> Result<usize, TestError> {
    let pending = writer.database().pending_changes(0, 64)?;
    let remote: Vec<logical_db::RemoteChange> = pending
        .iter()
        .map(|change| {
            logical_db::RemoteChange::new(change.document, change.digest, change.bytes().to_vec())
        })
        .collect();
    let count = remote.len();
    reader
        .database()
        .install_remote_page(peer, &remote, b"cursor-u8")?;
    Ok(count)
}

#[test]
fn tiered_value_round_trips_byte_identical_on_two_synced_replicas() -> Result<(), TestError> {
    let writer_path = path("tier-sync-a");
    let reader_path = path("tier-sync-b");
    let scope = scope();
    let peer = PeerId::new();
    let writer = KvCache::open(&writer_path, scope, ReplicaId::new(), "media")?;
    let reader = KvCache::open(&reader_path, scope, ReplicaId::new(), "media")?;
    let big = deterministic_bytes(3 * MAX_BLOB_CHUNK_BYTES + 12_345);
    writer.put("big", &big, None)?;
    assert_eq!(writer.get("big")?, Some(big.clone()));
    let pending = writer.database().pending_changes(0, 64)?;
    assert_eq!(pending.len(), 1);
    for change in &pending {
        assert!(
            change.bytes.len() < TIER_THRESHOLD_BYTES,
            "sidecar reference must stay inside the bounded change"
        );
    }
    install_all(&writer, &reader, peer)?;
    let reference = reader
        .blob_reference("big")?
        .ok_or("expected tiered blob reference")?;
    assert_eq!(reference.size, u64::try_from(big.len())?);
    assert!(
        matches!(reader.get("big"), Err(KvError::MissingBlob { .. })),
        "reference without bytes must fail loudly, never partial"
    );
    let mut offset = 0_u64;
    let mut chunks = 0_u32;
    while offset < reference.size {
        let want = usize::try_from(reference.size - offset)?.min(MAX_BLOB_CHUNK_BYTES);
        let bytes = writer.read_blob_chunk(&reference.digest, offset, want)?;
        assert!(!bytes.is_empty() && bytes.len() <= MAX_BLOB_CHUNK_BYTES);
        let complete =
            reader.write_blob_chunk(&reference.digest, reference.size, offset, &bytes)?;
        offset = offset.saturating_add(u64::try_from(bytes.len())?);
        chunks += 1;
        assert_eq!(complete, offset == reference.size);
    }
    assert!(chunks > 1, "large value must travel in multiple chunks");
    assert_eq!(reader.get("big")?, Some(big));
    cleanup_tree(&writer_path, &writer.blob_dir)?;
    cleanup_tree(&reader_path, &reader.blob_dir)?;
    Ok(())
}

#[test]
fn small_values_never_touch_the_blob_path() -> Result<(), TestError> {
    let db_path = path("tier-small");
    let cache = open_cache(&db_path, "sessions")?;
    cache.put("a", b"1", Some(Duration::from_secs(60)))?;
    cache.put("empty-ok", b"x", None)?;
    assert_eq!(cache.get("a")?, Some(b"1".to_vec()));
    assert_eq!(cache.scan("", None, 10)?.len(), 2);
    assert_eq!(cache.recent("", 10)?.len(), 2);
    assert_eq!(cache.blob_reference("a")?, None);
    assert_eq!(cache.evict_expired(10)?, 0);
    assert!(
        !cache.blob_dir.exists(),
        "small values must never create the blob directory"
    );
    cleanup_tree(&db_path, &cache.blob_dir)?;
    Ok(())
}

#[test]
fn delete_and_evict_remove_reference_and_tiered_bytes() -> Result<(), TestError> {
    let db_path = path("tier-delete");
    let cache = open_cache(&db_path, "media")?;
    let first = deterministic_bytes(TIER_THRESHOLD_BYTES + 1_000);
    cache.put("gone", &first, None)?;
    let digest = cache
        .blob_reference("gone")?
        .ok_or("expected tiered blob reference")?
        .digest;
    assert!(cache.blob_path(&digest).exists());
    assert!(cache.delete("gone")?);
    assert_eq!(cache.get("gone")?, None);
    assert_eq!(cache.sweep_blobs()?, 1);
    assert!(
        !cache.blob_path(&digest).exists(),
        "sweep must remove tiered bytes after the reference commit"
    );
    cache.put("old", &first, Some(Duration::ZERO))?;
    let old_digest = cache
        .blob_reference("old")?
        .ok_or("expected tiered blob reference")?
        .digest;
    assert_eq!(cache.get("old")?, None);
    assert_eq!(cache.evict_expired(10)?, 1);
    assert_eq!(cache.sweep_blobs()?, 1);
    assert!(!cache.blob_path(&old_digest).exists());
    let shared = deterministic_bytes(TIER_THRESHOLD_BYTES + 7);
    cache.put("s1", &shared, None)?;
    cache.put("s2", &shared, None)?;
    let shared_digest = cache
        .blob_reference("s1")?
        .ok_or("expected tiered blob reference")?
        .digest;
    assert_eq!(
        cache
            .blob_reference("s2")?
            .ok_or("expected tiered blob reference")?
            .digest,
        shared_digest
    );
    assert!(cache.delete("s1")?);
    assert!(
        cache.blob_path(&shared_digest).exists(),
        "shared bytes survive while a live reference remains"
    );
    assert_eq!(cache.get("s2")?, Some(shared));
    assert!(cache.delete("s2")?);
    assert_eq!(cache.sweep_blobs()?, 1);
    assert!(!cache.blob_path(&shared_digest).exists());
    cleanup_tree(&db_path, &cache.blob_dir)?;
    Ok(())
}

#[test]
fn crash_between_reference_removal_and_blob_deletion_recovered_by_sweep() -> Result<(), TestError> {
    let db_path = path("tier-crash");
    let scope = scope();
    let replica = ReplicaId::new();
    let digest = {
        let cache = KvCache::open(&db_path, scope, replica, "media")?;
        let big = deterministic_bytes(TIER_THRESHOLD_BYTES + 511);
        cache.put("big", &big, None)?;
        let digest = cache
            .blob_reference("big")?
            .ok_or("expected tiered blob reference")?
            .digest;
        let document = document_for("media", "big")?;
        let outcome = cache.remove_reference(document, "big")?;
        assert!(outcome.live);
        let blobs = cache.blob_dir.clone();
        drop(cache);
        assert!(
            blobs.join(hex_digest(&digest)).exists(),
            "crashed delete leaves an orphan blob file"
        );
        digest
    };
    {
        let cache = KvCache::open(&db_path, scope, replica, "media")?;
        assert_eq!(cache.get("big")?, None);
        assert!(cache.blob_path(&digest).exists());
        assert_eq!(cache.sweep_blobs()?, 1);
        assert!(!cache.blob_path(&digest).exists());
        assert_eq!(cache.sweep_blobs()?, 0);
        let blobs = cache.blob_dir.clone();
        drop(cache);
        cleanup_tree(&db_path, &blobs)?;
    }
    Ok(())
}

#[test]
fn sweep_promotes_referenced_gc_staging_back_to_canonical_storage() -> Result<(), TestError> {
    let db_path = path("tier-gc-republish");
    let cache = open_cache(&db_path, "media")?;
    let value = deterministic_bytes(TIER_THRESHOLD_BYTES + 512);
    cache.put("old", &value, None)?;
    let digest = cache
        .blob_reference("old")?
        .ok_or("expected tiered blob reference")?
        .digest;
    assert!(cache.delete("old")?);
    let staged = cache
        .blob_dir
        .join(format!("{}.tmp-gc-test", hex_digest(&digest)));
    std::fs::rename(cache.blob_path(&digest), &staged)?;
    assert!(!cache.blob_path(&digest).exists());

    let document = document_for("media", "new")?;
    cache.database().edit(
        document,
        OperationId::new(),
        OperationDigest::new([78; 32]),
        |editor| {
            editor.put(
                &Object::root(),
                FIELD_VERSION,
                Scalar::Unsigned(ENVELOPE_VERSION),
            )?;
            editor.put(&Object::root(), FIELD_KEY, Scalar::String("new".into()))?;
            editor.put(
                &Object::root(),
                FIELD_NAMESPACE,
                Scalar::String("media".into()),
            )?;
            editor.put(&Object::root(), FIELD_BLOB, Scalar::Bytes(digest.to_vec()))?;
            editor.put(
                &Object::root(),
                FIELD_SIZE,
                Scalar::Unsigned(
                    u64::try_from(value.len())
                        .map_err(|_error| CrdtError::InvalidChangeSize(value.len()))?,
                ),
            )?;
            Ok(())
        },
    )?;
    assert_eq!(cache.sweep_blobs()?, 0);
    assert_eq!(cache.get("new")?, Some(value));
    cleanup_tree(&db_path, &cache.blob_dir)?;
    Ok(())
}

#[test]
fn missing_or_damaged_blob_fails_read_loudly_without_partial_bytes() -> Result<(), TestError> {
    let db_path = path("tier-loud");
    let cache = open_cache(&db_path, "media")?;
    let big = deterministic_bytes(TIER_THRESHOLD_BYTES + 999);
    cache.put("big", &big, None)?;
    let digest = cache
        .blob_reference("big")?
        .ok_or("expected tiered blob reference")?
        .digest;
    let file = cache.blob_path(&digest);
    std::fs::remove_file(&file)?;
    assert!(matches!(cache.get("big"), Err(KvError::MissingBlob { .. })));
    assert!(matches!(
        cache.scan("", None, 10),
        Err(KvError::MissingBlob { .. })
    ));
    cache.put("big", &big, None)?;
    let mut damaged = std::fs::read(&file)?;
    damaged[100] ^= 0xFF;
    std::fs::write(&file, &damaged)?;
    assert!(matches!(cache.get("big"), Err(KvError::CorruptBlob { .. })));
    assert!(matches!(
        cache.read_blob_chunk(&digest, 0, 1),
        Err(KvError::CorruptBlob { .. })
    ));
    cache.put("big", &big, None)?;
    let truncated = std::fs::read(&file)?;
    let half = truncated
        .len()
        .checked_div(2)
        .ok_or("halving a length never fails")?;
    std::fs::write(&file, &truncated[..half])?;
    assert!(matches!(cache.get("big"), Err(KvError::CorruptBlob { .. })));
    assert!(cache.blob_reference("big")?.is_some());
    cleanup_tree(&db_path, &cache.blob_dir)?;
    Ok(())
}

#[test]
fn invalid_chunk_never_mutates_an_existing_canonical_blob() -> Result<(), TestError> {
    let db_path = path("tier-chunk-isolation");
    let cache = open_cache(&db_path, "media")?;
    let original = deterministic_bytes(TIER_THRESHOLD_BYTES + 64);
    cache.put("big", &original, None)?;
    let reference = cache
        .blob_reference("big")?
        .ok_or("expected tiered blob reference")?;
    let invalid = vec![0_u8; original.len()];
    assert!(matches!(
        cache.write_blob_chunk(&reference.digest, reference.size, 0, &invalid),
        Err(KvError::CorruptBlob { .. })
    ));
    assert_eq!(cache.get("big")?, Some(original));
    assert!(cache.blob_path(&reference.digest).exists());
    assert_eq!(cache.sweep_blobs()?, 1);
    assert!(cache.blob_path(&reference.digest).exists());
    cleanup_tree(&db_path, &cache.blob_dir)?;
    Ok(())
}

#[test]
fn sweep_refuses_unsupported_and_corrupt_cache_envelopes() -> Result<(), TestError> {
    for (tag, version, blob) in [("unsupported", 99_u64, None), ("corrupt", 2, Some(vec![1]))] {
        let db_path = path(tag);
        let cache = open_cache(&db_path, "media")?;
        let document = document_for("media", "bad")?;
        cache.database().edit(
            document,
            OperationId::new(),
            OperationDigest::new([41; 32]),
            |editor| {
                editor.put(&Object::root(), FIELD_VERSION, Scalar::Unsigned(version))?;
                editor.put(&Object::root(), FIELD_KEY, Scalar::String("bad".into()))?;
                editor.put(
                    &Object::root(),
                    FIELD_NAMESPACE,
                    Scalar::String("media".into()),
                )?;
                if let Some(blob) = blob {
                    editor.put(&Object::root(), FIELD_BLOB, Scalar::Bytes(blob))?;
                    editor.put(&Object::root(), FIELD_SIZE, Scalar::Unsigned(1))?;
                } else {
                    editor.put(
                        &Object::root(),
                        FIELD_VALUE,
                        Scalar::Bytes(b"future".to_vec()),
                    )?;
                }
                Ok(())
            },
        )?;
        let Err(error) = cache.sweep_blobs() else {
            return Err("expected sweep error".into());
        };
        if version == 99 {
            assert!(matches!(
                error,
                KvError::UnsupportedEnvelopeVersion { actual: 99 }
            ));
        } else {
            assert!(matches!(error, KvError::CorruptEnvelope));
        }
        cleanup_tree(&db_path, &cache.blob_dir)?;
    }
    Ok(())
}

#[test]
fn point_reads_refuse_malformed_tiered_shapes() -> Result<(), TestError> {
    let db_path = path("tier-shape");
    let cache = open_cache(&db_path, "media")?;
    for (key, digest, size) in [("inline", None, None), ("small", Some([8; 32]), Some(1))] {
        let document = document_for("media", key)?;
        cache.database().edit(
            document,
            OperationId::new(),
            OperationDigest::new([42; 32]),
            |editor| {
                editor.put(&Object::root(), FIELD_VERSION, Scalar::Unsigned(2))?;
                editor.put(&Object::root(), FIELD_KEY, Scalar::String(key.into()))?;
                editor.put(
                    &Object::root(),
                    FIELD_NAMESPACE,
                    Scalar::String("media".into()),
                )?;
                match (digest, size) {
                    (None, None) => {
                        editor.put(&Object::root(), FIELD_VALUE, Scalar::Bytes(vec![1]))?;
                    }
                    (Some(digest), Some(size)) => {
                        editor.put(&Object::root(), FIELD_BLOB, Scalar::Bytes(digest.to_vec()))?;
                        editor.put(&Object::root(), FIELD_SIZE, Scalar::Unsigned(size))?;
                    }
                    _ => return Err(CrdtError::InvalidChangeSize(0)),
                }
                Ok(())
            },
        )?;
        assert!(matches!(cache.get(key), Err(KvError::CorruptEnvelope)));
    }
    cleanup_tree(&db_path, &cache.blob_dir)?;
    Ok(())
}

#[test]
fn point_reads_refuse_unbounded_envelope_axes() -> Result<(), TestError> {
    let db_path = path("envelope-bounds");
    let cache = open_cache(&db_path, "media")?;
    let oversized = "x".repeat(logical_db_model::MAX_NAME_BYTES + 1);
    let malformed = vec![
        (
            document_for("media", "empty-key")?,
            String::new(),
            "media".to_owned(),
        ),
        (
            document_for("media", "oversized-key")?,
            oversized,
            "media".to_owned(),
        ),
        (
            document_for("media", "empty-namespace")?,
            "key".to_owned(),
            String::new(),
        ),
    ];
    for (index, (document, key, namespace)) in malformed.into_iter().enumerate() {
        let request_byte = 50_u8 + u8::try_from(index)?;
        cache.database().edit(
            document,
            OperationId::new(),
            OperationDigest::new([request_byte; 32]),
            move |editor| {
                editor.put(&Object::root(), FIELD_KEY, Scalar::String(key))?;
                editor.put(&Object::root(), FIELD_NAMESPACE, Scalar::String(namespace))?;
                editor.put(&Object::root(), FIELD_VALUE, Scalar::Bytes(vec![1]))?;
                Ok(())
            },
        )?;
        assert!(matches!(
            cache.document_namespace(document),
            Err(KvError::CorruptEnvelope)
        ));
    }
    cleanup_tree(&db_path, &cache.blob_dir)?;
    Ok(())
}

#[test]
fn values_beyond_inline_bound_tier_and_huge_values_refused_before_allocation()
-> Result<(), TestError> {
    let db_path = path("tier-bounds");
    let cache = open_cache(&db_path, "media")?;
    let large = deterministic_bytes(MAX_VALUE_BYTES + 1);
    cache.put("large", &large, None)?;
    assert_eq!(cache.get("large")?, Some(large));
    assert!(cache.blob_reference("large")?.is_some());
    let blobs = cache.blob_dir.clone();
    assert!(blobs.exists());
    let before = std::fs::read_dir(&blobs)?.count();
    let huge = vec![7_u8; MAX_TIERED_VALUE_BYTES + 1];
    let Err(error) = cache.put("huge", &huge, None) else {
        return Err("expected blob too large".into());
    };
    assert!(matches!(
        error,
        KvError::BlobTooLarge { actual, maximum }
        if actual == MAX_TIERED_VALUE_BYTES + 1 && maximum == MAX_TIERED_VALUE_BYTES
    ));
    assert_eq!(cache.get("huge")?, None);
    assert_eq!(std::fs::read_dir(&blobs)?.count(), before);
    drop(cache);
    cleanup_tree(&db_path, &blobs)?;
    Ok(())
}

#[test]
fn tiered_overwrite_and_batch_reclaim_displaced_bytes_on_sweep() -> Result<(), TestError> {
    let db_path = path("tier-overwrite");
    let cache = open_cache(&db_path, "media")?;
    let first = deterministic_bytes(TIER_THRESHOLD_BYTES + 11);
    let second = deterministic_bytes(TIER_THRESHOLD_BYTES + 12);
    cache.put("k", &first, None)?;
    let first_digest = cache
        .blob_reference("k")?
        .ok_or("expected tiered blob reference")?
        .digest;
    cache.put("k", &second, None)?;
    assert!(cache.blob_path(&first_digest).exists());
    assert_eq!(cache.sweep_blobs()?, 1);
    assert!(!cache.blob_path(&first_digest).exists());
    assert_eq!(cache.get("k")?, Some(second));
    cache.put_many(&[
        BatchPut {
            key: "b1".into(),
            value: deterministic_bytes(TIER_THRESHOLD_BYTES + 21),
            ttl: None,
        },
        BatchPut {
            key: "b2".into(),
            value: b"small".to_vec(),
            ttl: Some(Duration::from_secs(60)),
        },
    ])?;
    assert!(cache.blob_reference("b1")?.is_some());
    assert_eq!(cache.blob_reference("b2")?, None);
    assert_eq!(
        cache.get("b1")?.ok_or("expected b1 value")?.len(),
        TIER_THRESHOLD_BYTES + 21
    );
    let keep = deterministic_bytes(TIER_THRESHOLD_BYTES + 31);
    cache.put("keep", &keep, None)?;
    let keep_digest = cache
        .blob_reference("keep")?
        .ok_or("expected tiered blob reference")?
        .digest;
    let replacement = deterministic_bytes(TIER_THRESHOLD_BYTES + 32);
    let Err(error) = cache.put_many(&[
        BatchPut {
            key: "keep".into(),
            value: replacement,
            ttl: None,
        },
        BatchPut {
            key: "bad".into(),
            value: vec![0_u8; MAX_TIERED_VALUE_BYTES + 1],
            ttl: None,
        },
    ]) else {
        return Err("expected blob too large".into());
    };
    assert!(matches!(error, KvError::BlobTooLarge { .. }));
    assert_eq!(cache.get("keep")?, Some(keep));
    assert!(cache.blob_path(&keep_digest).exists());
    assert_eq!(cache.get("bad")?, None);
    let expired = deterministic_bytes(TIER_THRESHOLD_BYTES + 33);
    cache.put("expired", &expired, Some(Duration::ZERO))?;
    let expired_digest = cache
        .blob_reference("expired")?
        .ok_or("expected tiered blob reference")?
        .digest;
    cache.put_many(&[BatchPut {
        key: "expired".into(),
        value: b"replaced".to_vec(),
        ttl: None,
    }])?;
    assert!(cache.blob_path(&expired_digest).exists());
    assert_eq!(cache.sweep_blobs()?, 1);
    assert!(!cache.blob_path(&expired_digest).exists());
    cleanup_tree(&db_path, &cache.blob_dir)?;
    Ok(())
}

#[test]
fn concurrent_writers_do_not_expose_partial_tiered_values() -> Result<(), TestError> {
    let db_path = path("tier-concurrent");
    let cache = std::sync::Arc::new(open_cache(&db_path, "media")?);
    let workers: Vec<_> = (0..6)
        .map(|index| {
            let cache = std::sync::Arc::clone(&cache);
            std::thread::Builder::new().spawn(move || {
                let value = deterministic_bytes(TIER_THRESHOLD_BYTES + index + 1);
                let mut result = cache.put("race", &value, None);
                for _ in 0..8 {
                    if result.is_ok() {
                        break;
                    }
                    if !matches!(result, Err(KvError::Database(_))) {
                        break;
                    }
                    std::thread::yield_now();
                    result = cache.put("race", &value, None);
                }
                result
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    for worker in workers {
        match worker.join() {
            Ok(result) => result?,
            Err(_) => return Err("concurrent writer panicked".into()),
        }
    }
    let value = cache.get("race")?.ok_or("expected race value")?;
    assert!(value.len() > TIER_THRESHOLD_BYTES);
    let blobs = cache.blob_dir.clone();
    drop(cache);
    cleanup_tree(&db_path, &blobs)?;
    Ok(())
}

#[test]
fn blob_chunk_transfer_rejects_out_of_bounds_before_allocation() -> Result<(), TestError> {
    let db_path = path("tier-chunks");
    let cache = open_cache(&db_path, "media")?;
    let value = deterministic_bytes(TIER_THRESHOLD_BYTES + 64);
    cache.put("v", &value, None)?;
    let reference = cache
        .blob_reference("v")?
        .ok_or("expected tiered blob reference")?;
    let oversize = vec![0_u8; MAX_BLOB_CHUNK_BYTES + 1];
    let Err(error) = cache.read_blob_chunk(&reference.digest, 0, oversize.len()) else {
        return Err("expected invalid chunk length".into());
    };
    assert!(matches!(error, KvError::InvalidChunkLength(_)));
    let Err(error) = cache.write_blob_chunk(&reference.digest, reference.size, 0, &oversize) else {
        return Err("expected invalid chunk length".into());
    };
    assert!(matches!(error, KvError::InvalidChunkLength(_)));
    let Err(error) =
        cache.write_blob_chunk(&reference.digest, reference.size, reference.size, b"x")
    else {
        return Err("expected invalid chunk offset".into());
    };
    assert!(matches!(error, KvError::InvalidChunkOffset(_)));
    let Err(error) = cache.read_blob_chunk(&[9_u8; 32], 0, 8) else {
        return Err("expected missing blob".into());
    };
    assert!(matches!(error, KvError::MissingBlob { .. }));
    let foreign = [7_u8; 32];
    let Err(error) = cache.write_blob_chunk(&foreign, 3, 0, b"xyz") else {
        return Err("expected corrupt blob".into());
    };
    assert!(matches!(error, KvError::CorruptBlob { .. }));
    assert_eq!(
        cache.sweep_blobs()?,
        1,
        "a completed-but-diverged staging file is an unreferenced orphan"
    );
    assert!(!cache.blob_path(&foreign).exists());
    let payload = b"hello-chunked-world";
    let staged = content_digest(payload);
    let total = u64::try_from(payload.len())?;
    let complete = cache.write_blob_chunk(&staged, total, 0, &payload[..5])?;
    assert!(!complete);
    let complete = cache.write_blob_chunk(&staged, total, 5, &payload[5..])?;
    assert!(complete);
    assert_eq!(cache.read_blob_chunk(&staged, 0, payload.len())?, payload);
    std::fs::remove_file(cache.blob_path(&staged))?;
    cleanup_tree(&db_path, &cache.blob_dir)?;
    Ok(())
}

#[test]
fn document_namespace_classifies_slots_for_sync_filtering() -> Result<(), TestError> {
    let db_path = path("ns-classify");
    let scope = scope();
    let replica = ReplicaId::new();
    {
        let first = KvCache::open(&db_path, scope, replica, "one")?;
        first.put("shared", b"1", Some(Duration::from_secs(60)))?;
        assert_eq!(
            first.document_namespace(document_for("one", "shared")?)?,
            Some("one".to_owned())
        );
        assert_eq!(
            first.document_namespace(document_for("two", "shared")?)?,
            None
        );
        assert_eq!(first.document_namespace(DocumentId::new())?, None);
    }
    {
        let second = KvCache::open(&db_path, scope, replica, "two")?;
        second.put("shared", b"2", Some(Duration::from_secs(60)))?;
        assert_eq!(
            second.document_namespace(document_for("two", "shared")?)?,
            Some("two".to_owned())
        );
        assert_eq!(
            second.document_namespace(document_for("one", "shared")?)?,
            Some("one".to_owned())
        );
    }
    cleanup(&db_path)?;
    Ok(())
}

#[test]
fn blob_names_are_canonical_lowercase_hex_and_reject_every_near_miss() -> Result<(), TestError> {
    let canonical = hex_digest(&[0xab; 32]);
    assert_eq!(canonical.len(), 64);
    assert_eq!(hex_to_digest(&canonical), Some([0xab; 32]));
    assert!(is_blob_name(&canonical));
    let short = canonical[..63].to_owned();
    let long = format!("{canonical}0");
    let mixed_case = canonical.replace('a', "A");
    let invalid_digit = format!("z{}", &canonical[1..]);
    assert_eq!(hex_to_digest(&short), None);
    assert_eq!(hex_to_digest(&long), None);
    assert_eq!(hex_to_digest(&mixed_case), None);
    assert_eq!(hex_to_digest(&invalid_digit), None);
    assert!(!is_blob_name(&short));
    assert!(!is_blob_name(&long));
    assert!(!is_blob_name(&mixed_case));
    assert!(!is_blob_name(&invalid_digit));
    Ok(())
}
