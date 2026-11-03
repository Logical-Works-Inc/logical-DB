//! Codec and key unit tests; split from the store root.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use crate::codec::{
    decode_event, decode_migration, decode_sequence_key, decode_sequence_key_for_kind, decode_uuid,
    encode_event, encode_migration, schema_value,
};
use crate::error::StoreError;
use crate::indexes::{encode_index_value, validate_indexes};
use crate::keys::{
    EVENT_VALUE_BYTES, FORMAT, KIND_EVENT, KIND_OUTBOX, KIND_SNAPSHOT, MAX_CHECKPOINT_BYTES,
    MAX_INDEX_VALUE_BYTES, MAX_INDEXES_PER_DOCUMENT, MAX_MATERIALIZED_PAGE_BYTES,
    MAX_REMOTE_CHANGE_BYTES, MAX_REMOTE_CHANGES, MIGRATION_VALUE_BYTES, add_materialized_bytes,
    is_at_most, is_nonempty_bounded, key_change, key_checkpoint, key_event, key_event_sequence,
    key_index_reverse, key_index_row, key_migration, key_operation, key_outbox, key_outbox_ack,
    key_restore, key_revision, key_schema, key_sequence, key_snapshot, key_tombstone, next_prefix,
    scope_prefix,
};
use crate::types::ChangeEvent;
use logical_db_model::{
    AccountId, ChangeDigest, DocumentId, IndexEntry, IndexId, IndexValue, MAX_CHANGE_BYTES,
    MAX_SNAPSHOT_BYTES, OperationId, PeerId, Scope, SpaceId,
};
use std::collections::BTreeSet;
use std::error::Error;

#[test]
fn embedded_key_axis_is_versioned_scoped_and_order_preserving()
-> Result<(), Box<dyn std::error::Error>> {
    let account = logical_db_model::AccountId::from_uuid(decode_uuid(&[1_u8; 16])?);
    let space = SpaceId::from_uuid(decode_uuid(&[2_u8; 16])?);
    let document = DocumentId::from_uuid(decode_uuid(&[3_u8; 16])?);
    let scope = Scope::new(account, space);
    let snapshot = key_snapshot(scope, document);
    let mut expected = FORMAT.to_vec();
    expected.extend_from_slice(account.as_uuid().as_bytes());
    expected.extend_from_slice(space.as_uuid().as_bytes());
    expected.push(KIND_SNAPSHOT);
    expected.extend_from_slice(document.as_uuid().as_bytes());
    assert_eq!(
        snapshot, expected,
        "the snapshot key must be format, scope, kind, document in order"
    );
    assert!(
        key_outbox(scope, 1) < key_outbox(scope, 2),
        "outbox keys must order by sequence"
    );
    assert_ne!(
        key_snapshot(
            Scope::new(account, SpaceId::from_uuid(decode_uuid(&[4_u8; 16])?)),
            document
        ),
        snapshot,
        "snapshot keys must separate scopes"
    );
    assert_ne!(
        key_change(scope, document, ChangeDigest::new([0; 32])),
        snapshot,
        "a change key must never collide with a snapshot key"
    );
    Ok(())
}

#[test]
fn index_value_codecs_preserve_order_escaping_and_resource_bounds() -> Result<(), Box<dyn Error>> {
    assert!(
        encode_index_value(&IndexValue::Integer(-1))?
            < encode_index_value(&IndexValue::Integer(0))?,
        "negative integers must encode below zero"
    );
    assert!(
        encode_index_value(&IndexValue::Unsigned(1))?
            < encode_index_value(&IndexValue::Unsigned(2))?,
        "unsigned integers must encode in numeric order"
    );
    assert!(
        encode_index_value(&IndexValue::Timestamp(-1))?
            < encode_index_value(&IndexValue::Timestamp(0))?,
        "negative timestamps must encode below zero"
    );
    assert_ne!(
        encode_index_value(&IndexValue::Text("a\0b".into()))?,
        encode_index_value(&IndexValue::Text("a".into()))?,
        "embedded nuls must be escaped distinctly"
    );
    assert_eq!(
        next_prefix(&[1, 0xff]),
        Some(vec![2]),
        "incrementing a carry prefix must produce the next key"
    );
    assert_eq!(
        next_prefix(&[0xff]),
        None,
        "an all-ff prefix has no successor"
    );

    let index = IndexId::new();
    let duplicate = [
        IndexEntry::new(index, IndexValue::Boolean(false)),
        IndexEntry::new(index, IndexValue::Boolean(true)),
    ];
    assert!(
        matches!(
            validate_indexes(&duplicate),
            Err(StoreError::InvalidIndexes)
        ),
        "duplicate index ids must be refused"
    );
    let excessive = (0..=MAX_INDEXES_PER_DOCUMENT)
        .map(|_| IndexEntry::new(IndexId::new(), IndexValue::Boolean(true)))
        .collect::<Vec<_>>();
    assert!(
        matches!(
            validate_indexes(&excessive),
            Err(StoreError::InvalidIndexes)
        ),
        "one index past the ceiling must be refused"
    );
    assert!(
        matches!(
            encode_index_value(&IndexValue::Bytes(vec![0; MAX_INDEX_VALUE_BYTES])),
            Err(StoreError::InvalidIndexes)
        ),
        "an oversized byte value must be refused"
    );
    assert_eq!(
        encode_index_value(&IndexValue::Bytes(vec![1; 4_093]))?.len(),
        MAX_INDEX_VALUE_BYTES,
        "the exact ceiling byte length must encode"
    );
    let maximum = (0..MAX_INDEXES_PER_DOCUMENT)
        .map(|_| IndexEntry::new(IndexId::new(), IndexValue::Boolean(true)))
        .collect::<Vec<_>>();
    assert!(
        validate_indexes(&maximum).is_ok(),
        "the exact index ceiling must be accepted"
    );
    Ok(())
}

#[test]
fn every_resource_ceiling_accepts_its_exact_edge_and_rejects_the_next_value() {
    assert_eq!(
        MAX_SNAPSHOT_BYTES, 16_777_216,
        "the snapshot ceiling is fixed"
    );
    assert_eq!(
        MAX_MATERIALIZED_PAGE_BYTES, 67_108_864,
        "the materialized page ceiling is fixed"
    );
    assert_eq!(
        add_materialized_bytes(MAX_MATERIALIZED_PAGE_BYTES.saturating_sub(1), 1),
        Some(MAX_MATERIALIZED_PAGE_BYTES),
        "the byte below the ceiling must still add up to it"
    );
    assert_eq!(
        add_materialized_bytes(MAX_MATERIALIZED_PAGE_BYTES, 1),
        None,
        "one byte past the ceiling must overflow to none"
    );
    assert_eq!(
        add_materialized_bytes(usize::MAX, 1),
        None,
        "usize::MAX must overflow to none"
    );
    assert_eq!(
        MAX_REMOTE_CHANGE_BYTES, 8_388_608,
        "the remote change ceiling is fixed"
    );
    for maximum in [MAX_CHANGE_BYTES, MAX_SNAPSHOT_BYTES, MAX_CHECKPOINT_BYTES] {
        assert!(
            !is_nonempty_bounded(0, maximum),
            "zero is never a valid nonempty length"
        );
        assert!(
            is_nonempty_bounded(1, maximum),
            "one is always a valid nonempty length"
        );
        assert!(
            is_nonempty_bounded(maximum, maximum),
            "the exact ceiling is valid"
        );
        assert!(
            !is_nonempty_bounded(maximum.saturating_add(1), maximum),
            "one past the ceiling is invalid"
        );
    }
    for maximum in [
        MAX_REMOTE_CHANGES,
        MAX_REMOTE_CHANGE_BYTES,
        MAX_INDEXES_PER_DOCUMENT,
        MAX_INDEX_VALUE_BYTES,
    ] {
        assert!(is_at_most(0, maximum), "zero is at most any ceiling");
        assert!(
            is_at_most(maximum, maximum),
            "the exact ceiling is at most itself"
        );
        assert!(
            !is_at_most(maximum.saturating_add(1), maximum),
            "one past the ceiling fails the bound"
        );
    }
}

#[test]
fn private_key_and_event_codecs_keep_every_axis_distinct() -> Result<(), Box<dyn Error>> {
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let document = DocumentId::new();
    let operation = OperationId::new();
    let peer = PeerId::new();
    let index = IndexId::new();
    let digest = ChangeDigest::new([1; 32]);
    let keys = [
        key_snapshot(scope, document),
        key_change(scope, document, digest),
        key_operation(scope, operation),
        key_outbox(scope, 1),
        key_sequence(scope),
        key_revision(scope, document),
        key_checkpoint(scope, peer),
        key_outbox_ack(scope),
        key_schema(scope),
        key_event_sequence(scope),
        key_event(scope, 1),
        key_migration(scope),
        key_tombstone(scope, document),
        key_restore(scope),
        key_index_row(scope, index, &[1], document),
        key_index_reverse(scope, document, index),
    ];
    assert!(
        keys.iter().all(|key| key.starts_with(&scope_prefix(scope))),
        "every private key must open with the scope prefix"
    );
    assert_eq!(
        keys.iter().collect::<BTreeSet<_>>().len(),
        keys.len(),
        "every private key must be distinct"
    );
    assert_eq!(
        decode_sequence_key(&key_outbox(scope, 2))?,
        2,
        "the outbox key must decode back to its sequence"
    );
    assert!(
        matches!(
            decode_sequence_key_for_kind(&key_event(scope, 2), KIND_OUTBOX),
            Err(StoreError::CorruptInternalValue)
        ),
        "an event key must not decode as an outbox key"
    );
    assert!(
        matches!(
            decode_sequence_key_for_kind(&[0; 3], KIND_EVENT),
            Err(StoreError::CorruptInternalValue)
        ),
        "a short key must fail closed"
    );
    let event = encode_event(document, digest);
    assert_eq!(
        decode_event(4, &event)?,
        ChangeEvent::Document {
            sequence: 4,
            document,
            digest,
        },
        "the event codec must roundtrip every axis"
    );
    assert!(
        matches!(
            decode_event(4, &[0; EVENT_VALUE_BYTES - 1]),
            Err(StoreError::CorruptInternalValue)
        ),
        "a short event must fail closed"
    );
    let target = schema_value(2, [7; 32]);
    assert_eq!(
        decode_migration(&encode_migration(target, None))?,
        (target, None),
        "a cursorless migration must roundtrip"
    );
    assert_eq!(
        decode_migration(&encode_migration(target, Some(document)))?,
        (target, Some(document)),
        "a cursor migration must roundtrip"
    );
    assert!(
        matches!(decode_migration(&[]), Err(StoreError::CorruptInternalValue)),
        "an empty migration must fail closed"
    );
    let mut invalid_flag = encode_migration(target, None);
    invalid_flag[0] = 2;
    assert!(
        matches!(
            decode_migration(&invalid_flag),
            Err(StoreError::CorruptInternalValue)
        ),
        "an unknown migration flag must fail closed"
    );
    let mut hidden_cursor = encode_migration(target, None);
    hidden_cursor[MIGRATION_VALUE_BYTES - 1] = 1;
    assert!(
        matches!(
            decode_migration(&hidden_cursor),
            Err(StoreError::CorruptInternalValue)
        ),
        "a cursor without the cursor flag must fail closed"
    );
    Ok(())
}
