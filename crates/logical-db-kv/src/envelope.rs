//! Slot envelopes: encoding, decoding, clearing, recency, and projections.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use std::{collections::BTreeSet, sync::atomic::Ordering};

use logical_db::{
    BatchEdit, ChangeEvent, CrdtError, EditOutcome, Editor, Item, Object, OperationDigest,
    OperationId, Projection, Reader, Scalar,
};

use crate::blob::{blob_len, content_digest};
use crate::request::{
    batch_member_operation, batch_sequence, bind_batch_request, clear_digest, document_for, now_ms,
    put_digest, tiered_put_digest,
};
use crate::{
    BatchPut, DocumentId, DocumentRevision, ENVELOPE_VERSION, FIELD_BLOB, FIELD_EXPIRY, FIELD_KEY,
    FIELD_NAMESPACE, FIELD_SEQUENCE, FIELD_SIZE, FIELD_VALUE, FIELD_VERSION, IndexValue,
    KV_PROJECTION_SEMANTICS, KvCache, KvError, MAX_PAGE_LIMIT, MAX_TIERED_VALUE_BYTES,
    MAX_VALUE_BYTES, NEVER_EXPIRES_AT_MS, TIER_THRESHOLD_BYTES, expiry_index, recency_index,
};

impl KvCache {
    /// Builds the inline or tiered slot and the batch edit for one entry.
    pub(crate) fn prepare_batch_member(
        &self,
        entry: &BatchPut,
        expires_at_ms: i64,
        sequence: u64,
        batch_operation: OperationId,
        index: usize,
        batch_digest: &OperationDigest,
    ) -> Result<(SlotValue, BatchEdit), KvError> {
        let slot = self.stage_value(&entry.value)?;
        let operation = batch_member_operation(batch_operation, index)?;
        let request = slot_request(
            &operation,
            &self.namespace,
            &entry.key,
            &slot,
            expires_at_ms,
            sequence,
        );
        let request = bind_batch_request(request, batch_digest);
        let document = document_for(&self.namespace, &entry.key)?;
        let stored = StoredSlot {
            key: entry.key.clone(),
            namespace: self.namespace.clone(),
            slot: slot.clone(),
            expires_at_ms,
            sequence,
        };
        let edit = BatchEdit::new(document, operation, request, move |editor| {
            stored.write(editor)
        });
        Ok((slot, edit))
    }

    /// Writes a value with an explicit expiry and sequence.
    pub(crate) fn put_absolute(
        &self,
        key: &str,
        value: &[u8],
        expires_at_ms: i64,
        sequence: u64,
    ) -> Result<(), KvError> {
        let slot = self.stage_value(value)?;
        self.put_envelope(key, &slot, expires_at_ms, sequence)
    }

    /// Keeps a small value inline, or persists a large one to the blob store
    /// first and returns its sidecar reference.
    fn stage_value(&self, value: &[u8]) -> Result<SlotValue, KvError> {
        if value.len() <= TIER_THRESHOLD_BYTES {
            return Ok(SlotValue::Inline(value.to_vec()));
        }
        let digest = content_digest(value);
        self.write_blob_file(&digest, value)?;
        Ok(SlotValue::Tiered {
            digest,
            size: blob_len(value.len())?,
        })
    }

    /// Persists a decoded slot envelope into the database document.
    fn put_envelope(
        &self,
        key: &str,
        slot: &SlotValue,
        expires_at_ms: i64,
        sequence: u64,
    ) -> Result<(), KvError> {
        let document = document_for(&self.namespace, key)?;
        match self.decode_slot(document)? {
            Decoded::Found(_) | Decoded::Absent => {}
            Decoded::Corrupt => return Err(KvError::CorruptEnvelope),
            Decoded::Unsupported(actual) => {
                return Err(KvError::UnsupportedEnvelopeVersion { actual });
            }
        }
        let operation = OperationId::new();
        let digest = slot_request(
            &operation,
            &self.namespace,
            key,
            slot,
            expires_at_ms,
            sequence,
        );
        let stored = StoredSlot {
            key: key.to_owned(),
            namespace: self.namespace.clone(),
            slot: slot.clone(),
            expires_at_ms,
            sequence,
        };
        match self
            .database
            .edit(document, operation, digest, |editor| stored.write(editor))?
        {
            EditOutcome::Applied(_) | EditOutcome::Duplicate(_) => Ok(()),
            // `EditOutcome` is non_exhaustive: a future outcome is not a
            // successful slot write, so this fails closed.
            _ => Err(KvError::CorruptEnvelope),
        }
    }

    /// Reads and validates the envelope stored under a document id.
    pub(crate) fn decode_slot(&self, document: DocumentId) -> Result<Decoded, KvError> {
        let decoded = self.database.read_with_revision(document, |reader| {
            let Some((key, namespace)) = read_identity(reader)? else {
                return Ok(Decoded::Absent);
            };
            if !names_in_bounds(&key, &namespace) {
                return Ok(Decoded::Corrupt);
            }
            let version = match unsigned_field(reader, FIELD_VERSION)? {
                Field::Absent => 1,
                Field::Value(0) | Field::Malformed => return Ok(Decoded::Corrupt),
                Field::Value(version) => version,
            };
            if version > ENVELOPE_VERSION {
                return Ok(Decoded::Unsupported(version));
            }
            let inline = match bytes_field(reader, FIELD_VALUE)? {
                Field::Absent => None,
                Field::Value(value) => Some(value),
                Field::Malformed => return Ok(Decoded::Corrupt),
            };
            let blob = match bytes_field(reader, FIELD_BLOB)? {
                Field::Absent => None,
                Field::Value(blob) => Some(blob),
                Field::Malformed => return Ok(Decoded::Corrupt),
            };
            let size = match unsigned_field(reader, FIELD_SIZE)? {
                Field::Absent => None,
                Field::Value(size) => Some(size),
                Field::Malformed => return Ok(Decoded::Corrupt),
            };
            let value = match (inline, blob, size) {
                (Some(value), None, None) if version == 1 && value.len() <= MAX_VALUE_BYTES => {
                    SlotValue::Inline(value)
                }
                (None, Some(blob), Some(size))
                    if version == ENVELOPE_VERSION && tiered_size_in_range(size) =>
                {
                    let Ok(digest) = <[u8; 32]>::try_from(blob) else {
                        return Ok(Decoded::Corrupt);
                    };
                    SlotValue::Tiered { digest, size }
                }
                (None, None, None) => return Ok(Decoded::Absent),
                _ => return Ok(Decoded::Corrupt),
            };
            let expires_at_ms = match reader.get(&Object::root(), FIELD_EXPIRY)? {
                None => NEVER_EXPIRES_AT_MS,
                Some(Item::Scalar(Scalar::Timestamp(millis) | Scalar::Integer(millis))) => millis,
                Some(_) => return Ok(Decoded::Corrupt),
            };
            let sequence = match unsigned_field(reader, FIELD_SEQUENCE)? {
                Field::Absent => 0,
                Field::Value(sequence) => sequence,
                Field::Malformed => return Ok(Decoded::Corrupt),
            };
            Ok(Decoded::Found(Envelope {
                key,
                namespace,
                value,
                expires_at_ms,
                sequence,
                revision: DocumentRevision::new(1),
            }))
        })?;
        let Some((revision, decoded)) = decoded else {
            return Ok(Decoded::Absent);
        };
        Ok(match decoded {
            Decoded::Found(mut envelope) => {
                envelope.revision = revision;
                Decoded::Found(envelope)
            }
            decoded => decoded,
        })
    }

    /// Clears a reference if the stored key and namespace match.
    pub(crate) fn remove_reference(
        &self,
        document: DocumentId,
        key: &str,
    ) -> Result<ClearOutcome, KvError> {
        let expected_revision = match self.decode_slot(document)? {
            Decoded::Found(envelope)
                if envelope.namespace == self.namespace && envelope.key == key =>
            {
                Some(envelope.revision)
            }
            Decoded::Found(_) | Decoded::Absent => None,
            Decoded::Corrupt => return Err(KvError::CorruptEnvelope),
            Decoded::Unsupported(actual) => {
                return Err(KvError::UnsupportedEnvelopeVersion { actual });
            }
        };
        self.remove_reference_at(document, key, expected_revision)
    }

    /// Clears a reference when an optional revision precondition holds.
    pub(crate) fn remove_reference_at(
        &self,
        document: DocumentId,
        key: &str,
        expected_revision: Option<DocumentRevision>,
    ) -> Result<ClearOutcome, KvError> {
        let now = now_ms()?;
        match self.decode_slot(document)? {
            Decoded::Found(envelope)
                if envelope.namespace == self.namespace && envelope.key == key =>
            {
                if expected_revision.is_some_and(|expected| envelope.revision != expected) {
                    return Ok(ClearOutcome {
                        live: false,
                        cleared: false,
                    });
                }
                let live = envelope.expires_at_ms > now;
                let sequence = self.next_sequence(now);
                let operation = OperationId::new();
                let digest = clear_digest(&operation, &self.namespace, key);
                let clear = |editor: &mut Editor<'_>| {
                    editor.delete(&Object::root(), FIELD_VALUE)?;
                    editor.delete(&Object::root(), FIELD_BLOB)?;
                    editor.delete(&Object::root(), FIELD_SIZE)?;
                    editor.put(&Object::root(), FIELD_EXPIRY, Scalar::Timestamp(0))?;
                    editor.put(&Object::root(), FIELD_SEQUENCE, Scalar::Unsigned(sequence))?;
                    Ok(())
                };
                let result = match expected_revision {
                    Some(revision) => self
                        .database
                        .edit_if_revision(document, revision, operation, digest, clear),
                    None => self.database.edit(document, operation, digest, clear),
                }?;
                match result {
                    EditOutcome::Applied(_) | EditOutcome::Duplicate(_) => Ok(ClearOutcome {
                        live,
                        cleared: true,
                    }),
                    // `EditOutcome` is non_exhaustive: fail closed.
                    _ => Err(KvError::CorruptEnvelope),
                }
            }
            Decoded::Found(_) | Decoded::Absent => Ok(ClearOutcome {
                live: false,
                cleared: false,
            }),
            Decoded::Corrupt => Err(KvError::CorruptEnvelope),
            Decoded::Unsupported(actual) => Err(KvError::UnsupportedEnvelopeVersion { actual }),
        }
    }

    /// Pages change history to collect every scoped document id.
    pub(crate) fn snapshot_documents(&self) -> Result<BTreeSet<DocumentId>, KvError> {
        let mut documents = BTreeSet::new();
        let mut after = 0_u64;
        loop {
            let events = self.database.changes_since(after, MAX_PAGE_LIMIT)?;
            if events.is_empty() {
                break;
            }
            let complete = events.len() < MAX_PAGE_LIMIT;
            for event in &events {
                let &ChangeEvent::Document {
                    ref document,
                    sequence,
                    ..
                } = event
                else {
                    return Err(KvError::ScopeRewritten);
                };
                documents.insert(*document);
                after = sequence;
            }
            if complete {
                break;
            }
        }
        Ok(documents)
    }

    /// Advances the recency sequence past a wall-clock floor and returns it.
    ///
    /// A clock reading before the epoch floors at zero: recency only ever
    /// moves forward, so an earlier floor leaves the sequence where it is.
    pub(crate) fn next_sequence(&self, now: i64) -> u64 {
        self.sequence
            .fetch_max(now.max(0).unsigned_abs(), Ordering::SeqCst);
        self.sequence
            .fetch_add(1, Ordering::SeqCst)
            .saturating_add(1)
    }

    /// Reserves recency sequences covering every member of a batch.
    pub(crate) fn reserve_batch_sequences(
        &self,
        started_at_ms: i64,
        count: usize,
    ) -> Result<(), KvError> {
        let last_index = count.checked_sub(1).ok_or(KvError::InvalidBatchSize(0))?;
        let last_sequence = batch_sequence(started_at_ms, last_index)?;
        self.sequence.fetch_max(last_sequence, Ordering::SeqCst);
        Ok(())
    }
}

/// Builds the request digest for one inline or tiered slot write.
fn slot_request(
    operation: &OperationId,
    namespace: &str,
    key: &str,
    slot: &SlotValue,
    expires_at_ms: i64,
    sequence: u64,
) -> OperationDigest {
    match *slot {
        SlotValue::Inline(ref value) => {
            put_digest(operation, namespace, key, value, expires_at_ms, sequence)
        }
        SlotValue::Tiered { ref digest, size } => tiered_put_digest(
            operation,
            namespace,
            key,
            digest,
            size,
            expires_at_ms,
            sequence,
        ),
    }
}

/// Everything one slot write commits, owned so the edit closure can move it.
struct StoredSlot {
    /// Original cache key.
    key: String,
    /// Owning namespace.
    namespace: String,
    /// Inline bytes or the tiered sidecar reference.
    slot: SlotValue,
    /// Absolute expiry in Unix milliseconds.
    expires_at_ms: i64,
    /// Recency sequence stamped on the write.
    sequence: u64,
}

impl StoredSlot {
    /// Replaces the slot's envelope fields with this write, in one edit.
    fn write(self, editor: &mut Editor<'_>) -> Result<(), CrdtError> {
        editor.delete(&Object::root(), FIELD_VALUE)?;
        editor.delete(&Object::root(), FIELD_BLOB)?;
        editor.delete(&Object::root(), FIELD_SIZE)?;
        match self.slot {
            SlotValue::Inline(value) => {
                editor.put(&Object::root(), FIELD_VERSION, Scalar::Unsigned(1))?;
                editor.put(&Object::root(), FIELD_VALUE, Scalar::Bytes(value))?;
            }
            SlotValue::Tiered { digest, size } => {
                editor.put(
                    &Object::root(),
                    FIELD_VERSION,
                    Scalar::Unsigned(ENVELOPE_VERSION),
                )?;
                editor.put(&Object::root(), FIELD_BLOB, Scalar::Bytes(digest.to_vec()))?;
                editor.put(&Object::root(), FIELD_SIZE, Scalar::Unsigned(size))?;
            }
        }
        editor.put(&Object::root(), FIELD_KEY, Scalar::String(self.key))?;
        editor.put(
            &Object::root(),
            FIELD_NAMESPACE,
            Scalar::String(self.namespace),
        )?;
        editor.put(
            &Object::root(),
            FIELD_EXPIRY,
            Scalar::Timestamp(self.expires_at_ms),
        )?;
        editor.put(
            &Object::root(),
            FIELD_SEQUENCE,
            Scalar::Unsigned(self.sequence),
        )?;
        Ok(())
    }
}

/// One optional envelope field: absent, a well-formed value, or malformed.
enum Field<T> {
    /// The property is not present.
    Absent,
    /// The property holds a well-formed value.
    Value(T),
    /// The property is present with a shape this layout never writes.
    Malformed,
}

/// Reads an unsigned envelope field, accepting a non-negative integer.
fn unsigned_field(reader: &mut Reader<'_>, field: &str) -> Result<Field<u64>, CrdtError> {
    Ok(match reader.get(&Object::root(), field)? {
        None => Field::Absent,
        Some(Item::Scalar(Scalar::Unsigned(value))) => Field::Value(value),
        Some(Item::Scalar(Scalar::Integer(value))) => match u64::try_from(value) {
            Ok(value) => Field::Value(value),
            Err(_) => Field::Malformed,
        },
        Some(_) => Field::Malformed,
    })
}

/// Reads a byte-string envelope field.
fn bytes_field(reader: &mut Reader<'_>, field: &str) -> Result<Field<Vec<u8>>, CrdtError> {
    Ok(match reader.get(&Object::root(), field)? {
        None => Field::Absent,
        Some(Item::Scalar(Scalar::Bytes(value))) => Field::Value(value),
        Some(_) => Field::Malformed,
    })
}

/// Reads the key and namespace a slot was written under, or `None` when the
/// slot is absent or either marker is not a string.
fn read_identity(reader: &mut Reader<'_>) -> Result<Option<(String, String)>, CrdtError> {
    let key = match reader.get(&Object::root(), FIELD_KEY)? {
        None => return Ok(None),
        Some(Item::Scalar(Scalar::String(key))) => key,
        Some(_) => return Ok(Some((String::new(), String::new()))),
    };
    let namespace = match reader.get(&Object::root(), FIELD_NAMESPACE)? {
        None => return Ok(None),
        Some(Item::Scalar(Scalar::String(namespace))) => namespace,
        Some(_) => return Ok(Some((String::new(), String::new()))),
    };
    Ok(Some((key, namespace)))
}

/// Reports whether a stored key and namespace are both within the name bound.
fn names_in_bounds(key: &str, namespace: &str) -> bool {
    !key.is_empty()
        && key.len() <= logical_db_model::MAX_NAME_BYTES
        && !namespace.is_empty()
        && namespace.len() <= logical_db_model::MAX_NAME_BYTES
}

/// Reports whether a tiered size lies above the inline threshold and within
/// the tiered bound; a size no `usize` can hold lies beyond it.
fn tiered_size_in_range(size: u64) -> bool {
    matches!(
        usize::try_from(size),
        Ok(bytes) if bytes > TIER_THRESHOLD_BYTES && bytes <= MAX_TIERED_VALUE_BYTES
    )
}

/// Returns the schema-bound expiry and recency projections every cache scope
/// carries. The functions never fail a commit: slots without a live value
/// project no rows, missing expiry defaults to never expire, missing
/// recency defaults to zero, a negative recency clamps to zero, and
/// unexpected shapes project nothing. A tiered slot is live exactly when it
/// carries a well-formed 32-byte content address; point reads stay strict
/// (see [`KvError`]).
pub(crate) fn kv_projections() -> Result<Vec<Projection>, KvError> {
    Ok(vec![
        Projection::new(expiry_index()?, KV_PROJECTION_SEMANTICS, |reader| {
            if !projection_has_live_value(reader)? {
                return Ok(None);
            }
            Ok(match reader.get(&Object::root(), FIELD_EXPIRY)? {
                None => Some(IndexValue::Timestamp(NEVER_EXPIRES_AT_MS)),
                Some(Item::Scalar(Scalar::Timestamp(millis) | Scalar::Integer(millis))) => {
                    Some(IndexValue::Timestamp(millis))
                }
                Some(_) => None,
            })
        }),
        Projection::new(recency_index()?, KV_PROJECTION_SEMANTICS, |reader| {
            if !projection_has_live_value(reader)? {
                return Ok(None);
            }
            Ok(match reader.get(&Object::root(), FIELD_SEQUENCE)? {
                None => Some(IndexValue::Unsigned(0)),
                Some(Item::Scalar(Scalar::Unsigned(sequence))) => {
                    Some(IndexValue::Unsigned(sequence))
                }
                Some(Item::Scalar(Scalar::Integer(sequence))) => {
                    Some(IndexValue::Unsigned(sequence.max(0).unsigned_abs()))
                }
                Some(_) => None,
            })
        }),
    ])
}

/// Returns whether a reader describes a well-formed live slot value.
fn projection_has_live_value(reader: &mut Reader<'_>) -> Result<bool, CrdtError> {
    let Some((key, namespace)) = read_identity(reader)? else {
        return Ok(false);
    };
    if !names_in_bounds(&key, &namespace) {
        return Ok(false);
    }
    let version = match unsigned_field(reader, FIELD_VERSION)? {
        Field::Absent => 1,
        Field::Value(version) => version,
        Field::Malformed => return Ok(false),
    };
    let inline_len = match bytes_field(reader, FIELD_VALUE)? {
        Field::Absent => None,
        Field::Value(value) => Some(value.len()),
        Field::Malformed => return Ok(false),
    };
    let blob_len = match bytes_field(reader, FIELD_BLOB)? {
        Field::Absent => None,
        Field::Value(blob) => Some(blob.len()),
        Field::Malformed => return Ok(false),
    };
    // A negative size never matches a tiered slot, exactly as an absent one;
    // a non-numeric size projects nothing.
    let size = match reader.get(&Object::root(), FIELD_SIZE)? {
        None => None,
        Some(Item::Scalar(Scalar::Unsigned(size))) => Some(size),
        Some(Item::Scalar(Scalar::Integer(size))) => u64::try_from(size).ok(),
        Some(_) => return Ok(false),
    };
    match version {
        1 => Ok(inline_len.is_some_and(|len| len <= MAX_VALUE_BYTES)
            && blob_len.is_none()
            && size.is_none()),
        ENVELOPE_VERSION => Ok(blob_len == Some(32)
            && inline_len.is_none()
            && size.is_some_and(tiered_size_in_range)),
        _ => Ok(false),
    }
}

/// Decoded identity, payload, and metadata for one KV document slot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Envelope {
    /// Logical key stored in the envelope.
    pub(crate) key: String,
    /// Namespace the envelope belongs to.
    pub(crate) namespace: String,
    /// Inline bytes or a tiered content address.
    pub(crate) value: SlotValue,
    /// Absolute expiry in Unix milliseconds, or never-expires sentinel.
    pub(crate) expires_at_ms: i64,
    /// Monotonic recency sequence assigned at write time.
    pub(crate) sequence: u64,
    /// Document revision observed while decoding.
    pub(crate) revision: DocumentRevision,
}

/// How a KV value is stored: inline bytes or a tiered blob reference.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SlotValue {
    /// Value kept entirely inside the document.
    Inline(Vec<u8>),
    /// Content address and byte length of an external blob.
    Tiered {
        /// Blake3 content digest of the blob file.
        digest: [u8; 32],
        /// Exact blob length in bytes.
        size: u64,
    },
}

/// Result of clearing a reference: liveness before clear and whether it cleared.
pub(crate) struct ClearOutcome {
    /// Whether the value was unexpired when the clear was attempted.
    pub(crate) live: bool,
    /// Whether a clear edit was applied to the document.
    pub(crate) cleared: bool,
}

/// Outcome of decoding a document slot during point reads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Decoded {
    /// Document has no KV envelope fields.
    Absent,
    /// Envelope fields are present but malformed.
    Corrupt,
    /// Envelope declares a newer format version than this crate understands.
    Unsupported(u64),
    /// Envelope decoded successfully.
    Found(Envelope),
}
