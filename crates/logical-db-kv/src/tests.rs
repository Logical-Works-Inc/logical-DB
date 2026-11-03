//! Cache operation, envelope, batch, and namespace tests.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use super::*;
use logical_db::{Object, OperationDigest, OperationId, Scalar};

use crate::{
    envelope::Decoded,
    request::document_for,
    tests_blob::{cleanup_tree, install_all},
};

/// Every failure a cache test reports, typed so a failing run names which
/// layer broke instead of an opaque boxed error.
#[derive(Debug, Error)]
pub(super) enum TestError {
    /// The cache under test returned an error.
    #[error(transparent)]
    Kv(#[from] KvError),
    /// The embedded replica below the cache returned an error.
    #[error(transparent)]
    Database(#[from] DatabaseError),
    /// A namespace checkpoint fixture was refused by the model.
    #[error(transparent)]
    Model(#[from] logical_db_model::ModelError),
    /// A fixture file or thread could not be created or removed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// A fixture length or index did not fit its integer width.
    #[error(transparent)]
    Width(#[from] std::num::TryFromIntError),
    /// The cache answered, but not with what the test expected.
    #[error("{0}")]
    Expectation(&'static str),
}

impl From<&'static str> for TestError {
    /// Wraps a failed expectation.
    fn from(message: &'static str) -> Self {
        Self::Expectation(message)
    }
}

pub(super) fn scope() -> Scope {
    Scope::new(AccountId::new(), SpaceId::new())
}

/// A replica path unique per call: the operation identity is a fresh
/// UUIDv7, so parallel test processes never share a file.
pub(super) fn path(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("logical-db-kv-{tag}-{:?}.redb", OperationId::new()))
}

pub(super) fn open_cache(path: &std::path::Path, namespace: &str) -> Result<KvCache, TestError> {
    Ok(KvCache::open(path, scope(), ReplicaId::new(), namespace)?)
}

pub(super) fn cleanup(path: &std::path::Path) -> Result<(), TestError> {
    std::fs::remove_file(path)?;
    Ok(())
}

#[test]
fn round_trip_put_get_delete_and_prefix_scan_without_caller_identity() -> Result<(), TestError> {
    let db_path = path("roundtrip");
    let cache = open_cache(&db_path, "sessions")?;
    cache.put("user:ada", b"1", Some(Duration::from_secs(60)))?;
    cache.put("user:grace", b"2", Some(Duration::from_secs(60)))?;
    cache.put("config:theme", b"dark", Some(Duration::from_secs(60)))?;
    assert_eq!(cache.get("user:ada")?, Some(b"1".to_vec()));
    let page = cache.scan("user:", None, 10)?;
    assert_eq!(
        page,
        vec![
            CacheEntry {
                key: "user:ada".into(),
                value: b"1".to_vec()
            },
            CacheEntry {
                key: "user:grace".into(),
                value: b"2".to_vec()
            },
        ]
    );
    assert!(cache.delete("user:ada")?);
    assert_eq!(cache.get("user:ada")?, None);
    assert!(!cache.delete("user:ada")?);
    assert!(!cache.delete("missing")?);
    let page = cache.scan("user:", None, 10)?;
    assert_eq!(
        page,
        vec![CacheEntry {
            key: "user:grace".into(),
            value: b"2".to_vec()
        }]
    );
    let first = cache.scan("", None, 1)?;
    assert_eq!(first.len(), 1);
    let rest = cache.scan("", Some(&first[0].key), 10)?;
    assert!(
        rest.iter()
            .all(|entry| entry.key.as_str() > first[0].key.as_str())
    );
    assert_eq!(first.len() + rest.len(), 2);
    cleanup(&db_path)?;
    Ok(())
}

#[test]
fn expired_entries_read_absent_and_evict_in_bounded_batches() -> Result<(), TestError> {
    let db_path = path("expiry");
    let cache = open_cache(&db_path, "sessions")?;
    cache.put("gone:1", b"a", Some(Duration::ZERO))?;
    cache.put("gone:2", b"b", Some(Duration::ZERO))?;
    cache.put("gone:3", b"c", Some(Duration::ZERO))?;
    cache.put("live", b"z", Some(Duration::from_secs(60)))?;
    assert_eq!(cache.get("gone:1")?, None);
    assert_eq!(cache.get("live")?, Some(b"z".to_vec()));
    assert_eq!(cache.evict_expired(2)?, 2);
    assert_eq!(cache.evict_expired(10)?, 1);
    assert_eq!(cache.evict_expired(10)?, 0);
    assert_eq!(cache.get("live")?, Some(b"z".to_vec()));
    assert!(cache.scan("gone:", None, 10)?.is_empty());
    cleanup(&db_path)?;
    Ok(())
}

#[test]
fn eviction_revision_guard_preserves_a_concurrent_replacement() -> Result<(), TestError> {
    let db_path = path("eviction-revision");
    let scope = scope();
    let replica = ReplicaId::new();
    let (document, revision) = {
        let first = KvCache::open(&db_path, scope, replica, "sessions")?;
        first.put("key", b"old", None)?;
        let document = document_for("sessions", "key")?;
        let revision = match first.decode_slot(document)? {
            Decoded::Found(envelope) => envelope.revision,
            _ => return Err(std::io::Error::other("expected stored envelope").into()),
        };
        (document, revision)
    };
    {
        let second = KvCache::open(&db_path, scope, replica, "sessions")?;
        second.put("key", b"new", None)?;
    }
    let first = KvCache::open(&db_path, scope, replica, "sessions")?;
    let outcome = first.remove_reference_at(document, "key", Some(revision))?;
    assert!(!outcome.cleared);
    assert_eq!(first.get("key")?, Some(b"new".to_vec()));
    cleanup(&db_path)?;
    Ok(())
}

#[test]
fn older_envelope_reads_with_new_fields_defaulted_and_newer_refused() -> Result<(), TestError> {
    let db_path = path("envelope");
    let scope = scope();
    let replica = ReplicaId::new();
    {
        let raw = Database::open(&db_path, scope, replica)?;
        let document = document_for("sessions", "legacy")?;
        raw.edit(
            document,
            OperationId::new(),
            OperationDigest::new([11; 32]),
            |editor| {
                editor.put(&Object::root(), FIELD_VERSION, Scalar::Unsigned(1))?;
                editor.put(&Object::root(), FIELD_KEY, Scalar::String("legacy".into()))?;
                editor.put(
                    &Object::root(),
                    FIELD_NAMESPACE,
                    Scalar::String("sessions".into()),
                )?;
                editor.put(&Object::root(), FIELD_VALUE, Scalar::Bytes(b"old".to_vec()))?;
                Ok(())
            },
        )?;
        let future = document_for("sessions", "future")?;
        raw.edit(
            future,
            OperationId::new(),
            OperationDigest::new([12; 32]),
            |editor| {
                editor.put(&Object::root(), FIELD_VERSION, Scalar::Unsigned(99))?;
                editor.put(&Object::root(), FIELD_KEY, Scalar::String("future".into()))?;
                editor.put(
                    &Object::root(),
                    FIELD_NAMESPACE,
                    Scalar::String("sessions".into()),
                )?;
                editor.put(&Object::root(), FIELD_VALUE, Scalar::Bytes(b"new".to_vec()))?;
                Ok(())
            },
        )?;
    }
    let (cache, report) = KvCache::migrate_schema(&db_path, scope, replica, "sessions", 16)?;
    assert_eq!(report.documents, 2);
    assert_eq!(cache.get("legacy")?, Some(b"old".to_vec()));
    let stored = cache.database().query_index(
        expiry_index()?,
        &IndexValue::Timestamp(NEVER_EXPIRES_AT_MS),
        None,
        10,
    )?;
    assert_eq!(stored, vec![document_for("sessions", "legacy")?]);
    let Err(error) = cache.get("future") else {
        return Err("expected unsupported envelope version".into());
    };
    assert!(matches!(
        error,
        KvError::UnsupportedEnvelopeVersion { actual: 99 }
    ));
    cleanup(&db_path)?;
    Ok(())
}

#[test]
fn tier_boundary_values_round_trip_and_huge_refused_before_commit() -> Result<(), TestError> {
    let db_path = path("bounds");
    let cache = open_cache(&db_path, "blobs")?;
    let inline = vec![7_u8; TIER_THRESHOLD_BYTES];
    cache.put("inline", &inline, Some(Duration::from_secs(60)))?;
    assert_eq!(cache.get("inline")?, Some(inline));
    assert_eq!(cache.blob_reference("inline")?, None);
    let tiered = vec![9_u8; MAX_VALUE_BYTES + 1];
    cache.put("tiered", &tiered, Some(Duration::from_secs(60)))?;
    assert_eq!(cache.get("tiered")?, Some(tiered));
    assert!(cache.blob_reference("tiered")?.is_some());
    let huge = vec![1_u8; MAX_TIERED_VALUE_BYTES + 1];
    let Err(error) = cache.put("huge", &huge, Some(Duration::from_secs(60))) else {
        return Err("expected blob too large".into());
    };
    assert!(matches!(
        error,
        KvError::BlobTooLarge { actual, maximum }
        if actual == MAX_TIERED_VALUE_BYTES + 1 && maximum == MAX_TIERED_VALUE_BYTES
    ));
    assert_eq!(cache.get("huge")?, None);
    let stored: Vec<_> = std::fs::read_dir(&cache.blob_dir)?.collect();
    assert_eq!(
        stored.len(),
        1,
        "the refused value must leave no blob file behind"
    );
    cleanup_tree(&db_path, &cache.blob_dir)?;
    Ok(())
}

#[test]
fn schema_migration_preserves_documents_and_installs_indexes() -> Result<(), TestError> {
    let db_path = path("migrate");
    let scope = scope();
    let replica = ReplicaId::new();
    {
        let raw = Database::open(&db_path, scope, replica)?;
        raw.edit(
            DocumentId::new(),
            OperationId::new(),
            OperationDigest::new([21; 32]),
            |editor| editor.put(&Object::root(), "note", Scalar::String("plain".into())),
        )?;
    }
    let (cache, _) = KvCache::migrate_schema(&db_path, scope, replica, "sessions", 16)?;
    cache.put("forever", b"v", None)?;
    assert_eq!(cache.get("forever")?, Some(b"v".to_vec()));
    let hits = cache.database().query_index(
        expiry_index()?,
        &IndexValue::Timestamp(NEVER_EXPIRES_AT_MS),
        None,
        10,
    )?;
    assert_eq!(hits, vec![document_for("sessions", "forever")?]);
    cleanup(&db_path)?;
    Ok(())
}

#[test]
fn namespaces_never_share_slots_and_inputs_are_validated() -> Result<(), TestError> {
    let db_path = path("namespaces");
    let scope = scope();
    let replica = ReplicaId::new();
    {
        let first = KvCache::open(&db_path, scope, replica, "one")?;
        first.put("shared", b"1", Some(Duration::from_secs(60)))?;
        assert_eq!(first.get("shared")?, Some(b"1".to_vec()));
    }
    {
        let second = KvCache::open(&db_path, scope, replica, "two")?;
        assert_eq!(second.get("shared")?, None);
        second.put("shared", b"2", Some(Duration::from_secs(60)))?;
        assert_eq!(second.get("shared")?, Some(b"2".to_vec()));
    }
    {
        let first = KvCache::open(&db_path, scope, replica, "one")?;
        assert_eq!(first.get("shared")?, Some(b"1".to_vec()));
        let Err(error) = first.get("") else {
            return Err("expected invalid key".into());
        };
        assert!(matches!(error, KvError::InvalidKey));
        let Err(error) = first.scan("", None, 0) else {
            return Err("expected invalid scan limit".into());
        };
        assert!(matches!(error, KvError::InvalidScanLimit(0)));
        let Err(error) = first.evict_expired(0) else {
            return Err("expected invalid eviction limit".into());
        };
        assert!(matches!(error, KvError::InvalidEvictionLimit(0)));
    }
    assert!(matches!(
        KvCache::open(&db_path, scope, replica, ""),
        Err(KvError::InvalidNamespace)
    ));
    cleanup(&db_path)?;
    Ok(())
}

#[test]
fn batch_put_applies_every_entry_atomically() -> Result<(), TestError> {
    let db_path = path("batch-apply");
    let cache = open_cache(&db_path, "sessions")?;
    cache.put_many(&[
        BatchPut {
            key: "a".into(),
            value: b"1".to_vec(),
            ttl: Some(Duration::from_secs(60)),
        },
        BatchPut {
            key: "b".into(),
            value: b"2".to_vec(),
            ttl: None,
        },
        BatchPut {
            key: "c".into(),
            value: b"3".to_vec(),
            ttl: Some(Duration::from_secs(60)),
        },
    ])?;
    assert_eq!(cache.get("a")?, Some(b"1".to_vec()));
    assert_eq!(cache.get("b")?, Some(b"2".to_vec()));
    assert_eq!(cache.get("c")?, Some(b"3".to_vec()));
    cleanup(&db_path)?;
    Ok(())
}

#[test]
fn batch_put_validation_failure_applies_nothing() -> Result<(), TestError> {
    let db_path = path("batch-atomic");
    let cache = open_cache(&db_path, "sessions")?;
    cache.put("keep", b"v0", Some(Duration::from_secs(60)))?;
    let oversize = vec![7_u8; MAX_TIERED_VALUE_BYTES + 1];
    let Err(error) = cache.put_many(&[
        BatchPut {
            key: "first".into(),
            value: b"1".to_vec(),
            ttl: Some(Duration::from_secs(60)),
        },
        BatchPut {
            key: "keep".into(),
            value: b"replaced".to_vec(),
            ttl: Some(Duration::from_secs(60)),
        },
        BatchPut {
            key: "big".into(),
            value: oversize,
            ttl: Some(Duration::from_secs(60)),
        },
        BatchPut {
            key: "last".into(),
            value: b"z".to_vec(),
            ttl: Some(Duration::from_secs(60)),
        },
    ]) else {
        return Err("expected blob too large".into());
    };
    assert!(matches!(
        error,
        KvError::BlobTooLarge { actual, maximum }
        if actual == MAX_TIERED_VALUE_BYTES + 1 && maximum == MAX_TIERED_VALUE_BYTES
    ));
    assert_eq!(cache.get("first")?, None);
    assert_eq!(cache.get("last")?, None);
    assert_eq!(cache.get("big")?, None);
    assert_eq!(cache.get("keep")?, Some(b"v0".to_vec()));
    cleanup(&db_path)?;
    Ok(())
}

#[test]
fn batch_put_rejects_empty_and_oversize_batches() -> Result<(), TestError> {
    let db_path = path("batch-bounds");
    let cache = open_cache(&db_path, "sessions")?;
    let Err(error) = cache.put_many(&[]) else {
        return Err("expected invalid batch size".into());
    };
    assert!(matches!(error, KvError::InvalidBatchSize(0)));
    let oversize: Vec<BatchPut> = (0..=MAX_BATCH_PUT)
        .map(|offset| BatchPut {
            key: format!("k{offset}"),
            value: b"v".to_vec(),
            ttl: None,
        })
        .collect();
    let Err(error) = cache.put_many(&oversize) else {
        return Err("expected invalid batch size".into());
    };
    assert!(matches!(
        error,
        KvError::InvalidBatchSize(actual) if actual == MAX_BATCH_PUT + 1
    ));
    assert_eq!(cache.get("k0")?, None);
    cleanup(&db_path)?;
    Ok(())
}

#[test]
fn batch_put_with_operation_retries_without_appending_changes() -> Result<(), TestError> {
    let db_path = path("batch-retry");
    let cache = open_cache(&db_path, "sessions")?;
    let operation = OperationId::new();
    let entries = [
        BatchPut {
            key: "a".into(),
            value: b"1".to_vec(),
            ttl: Some(Duration::from_secs(60)),
        },
        BatchPut {
            key: "b".into(),
            value: b"2".to_vec(),
            ttl: None,
        },
    ];
    cache.put_many_with_operation(operation, &entries)?;
    let changes = cache.database().changes_since(0, MAX_PAGE_LIMIT)?.len();
    cache.put_many_with_operation(operation, &entries)?;
    assert_eq!(
        cache.database().changes_since(0, MAX_PAGE_LIMIT)?.len(),
        changes
    );
    assert_eq!(cache.get("a")?, Some(b"1".to_vec()));
    assert_eq!(cache.get("b")?, Some(b"2".to_vec()));
    let extended = [
        entries[0].clone(),
        entries[1].clone(),
        BatchPut {
            key: "c".into(),
            value: b"3".to_vec(),
            ttl: None,
        },
    ];
    assert!(cache.put_many_with_operation(operation, &extended).is_err());
    assert_eq!(cache.get("c")?, None);
    let overwritten = [
        BatchPut {
            key: "a".into(),
            value: b"ignored-before-final".to_vec(),
            ttl: Some(Duration::from_secs(60)),
        },
        entries[0].clone(),
        entries[1].clone(),
    ];
    assert!(
        cache
            .put_many_with_operation(operation, &overwritten)
            .is_err()
    );
    assert_eq!(cache.get("a")?, Some(b"1".to_vec()));
    assert_eq!(cache.get("b")?, Some(b"2".to_vec()));
    assert!(
        cache
            .put_many_with_operation(
                operation,
                &[BatchPut {
                    key: "a".into(),
                    value: b"different".to_vec(),
                    ttl: Some(Duration::from_secs(60)),
                }],
            )
            .is_err()
    );
    assert_eq!(cache.get("a")?, Some(b"1".to_vec()));
    cleanup(&db_path)?;
    Ok(())
}

#[test]
fn batch_reserves_recency_before_a_following_put() -> Result<(), TestError> {
    let db_path = path("batch-sequence");
    let cache = open_cache(&db_path, "sessions")?;
    let batch_operation = OperationId::new();
    cache.put_many_with_operation(
        batch_operation,
        &[BatchPut {
            key: "batch".into(),
            value: b"batch-value".to_vec(),
            ttl: None,
        }],
    )?;
    let batch_sequence = match cache.decode_slot(document_for("sessions", "batch")?)? {
        Decoded::Found(envelope) => envelope.sequence,
        _ => {
            return Err(std::io::Error::other("batch envelope must be present").into());
        }
    };
    cache.put("following", b"following-value", None)?;
    let following_sequence = match cache.decode_slot(document_for("sessions", "following")?)? {
        Decoded::Found(envelope) => envelope.sequence,
        _ => {
            return Err(std::io::Error::other("following envelope must be present").into());
        }
    };
    assert!(following_sequence > batch_sequence);
    cleanup(&db_path)?;
    Ok(())
}

#[test]
fn batch_put_with_operation_rejects_non_uuidv7_identity() -> Result<(), TestError> {
    let db_path = path("batch-operation-id");
    let cache = open_cache(&db_path, "sessions")?;
    let Err(error) = cache.put_many_with_operation(
        logical_db::decode_identity_id::<OperationId>(&[0; 16])?,
        &[BatchPut {
            key: "key".into(),
            value: b"value".to_vec(),
            ttl: None,
        }],
    ) else {
        return Err("expected invalid operation id".into());
    };
    assert!(matches!(error, KvError::InvalidOperationId));
    assert_eq!(cache.get("key")?, None);
    cleanup(&db_path)?;
    Ok(())
}

#[test]
fn fresh_device_catches_up_newest_first_then_backfills() -> Result<(), TestError> {
    let writer_path = path("catchup-a");
    let reader_path = path("catchup-b");
    let scope = scope();
    let peer = logical_db_model::PeerId::new();
    {
        let writer = KvCache::open(&writer_path, scope, ReplicaId::new(), "sessions")?;
        for key in ["k1", "k2", "k3", "k4", "k5"] {
            writer.put(key, key.as_bytes(), Some(Duration::from_secs(60)))?;
        }
        let reader = KvCache::open(&reader_path, scope, ReplicaId::new(), "sessions")?;
        assert_eq!(install_all(&writer, &reader, peer)?, 5);
        let newest = reader.recent("", 2)?;
        assert_eq!(
            newest,
            vec![
                CacheEntry {
                    key: "k5".into(),
                    value: b"k5".to_vec(),
                },
                CacheEntry {
                    key: "k4".into(),
                    value: b"k4".to_vec(),
                },
            ]
        );
        let backfill = reader.scan("", None, 10)?;
        assert_eq!(
            backfill
                .iter()
                .map(|entry| entry.key.as_str())
                .collect::<Vec<_>>(),
            vec!["k1", "k2", "k3", "k4", "k5"]
        );
        let full_recent = reader.recent("", 10)?;
        assert_eq!(
            full_recent
                .iter()
                .map(|entry| entry.key.as_str())
                .collect::<Vec<_>>(),
            vec!["k5", "k4", "k3", "k2", "k1"]
        );
    }
    cleanup(&writer_path)?;
    cleanup(&reader_path)?;
    Ok(())
}

#[test]
fn namespace_checkpoint_map_round_trips_through_the_durable_slot() -> Result<(), TestError> {
    let db_path = path("ns-slot");
    let scope = scope();
    let peer = logical_db_model::PeerId::new();
    let cache = KvCache::open(&db_path, scope, ReplicaId::new(), "sessions")?;
    assert!(cache.namespace_checkpoints(peer)?.is_empty());
    let mut map = logical_db_model::NamespaceCheckpoints::new();
    map.insert(
        "hot",
        logical_db_model::NamespaceCheckpoint::new(vec![1, 2], 1_000)?,
    )?;
    map.insert(
        "cold",
        logical_db_model::NamespaceCheckpoint::new(vec![3], 2_000)?,
    )?;
    cache.store_namespace_checkpoints(peer, &map)?;
    let loaded = cache.namespace_checkpoints(peer)?;
    assert_eq!(loaded.get("hot").ok_or("expected Some")?.cursor(), &[1, 2]);
    assert_eq!(loaded.get("cold").ok_or("expected Some")?.cursor(), &[3]);
    let mut advanced = loaded;
    advanced.insert(
        "hot",
        logical_db_model::NamespaceCheckpoint::new(vec![9], 3_000)?,
    )?;
    cache.store_namespace_checkpoints(peer, &advanced)?;
    let reloaded = cache.namespace_checkpoints(peer)?;
    assert_eq!(reloaded.get("hot").ok_or("expected Some")?.cursor(), &[9]);
    assert_eq!(reloaded.get("cold").ok_or("expected Some")?.cursor(), &[3]);
    cleanup(&db_path)?;
    Ok(())
}
