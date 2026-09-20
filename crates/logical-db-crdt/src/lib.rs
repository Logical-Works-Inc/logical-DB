//! Narrow Automerge boundary for durable structured document convergence.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

#![forbid(unsafe_code)]

use automerge::{
    AutoCommit, AutomergeError, Change, ChangeHash, LoadChangeError, LoadOptions, ObjId, ObjType,
    ROOT, ReadDoc, ScalarValue, TextEncoding, Value, transaction::Transactable,
};
use logical_db_model::{
    ChangeDigest, MAX_CHANGE_BYTES, MAX_CHANGES_PER_PAGE, MAX_PAGE_BYTES, MAX_SNAPSHOT_BYTES,
};
use thiserror::Error;

/// Reserved root property carrying the monotonic document tombstone.
///
/// A present value means the document is deleted: reads report absent while
/// the snapshot and its conflict candidates are retained. Application edits
/// through [`Editor`] are refused on this property so user state can never
/// collide with or clear the marker; only [`Document::delete`] advances it.
pub const TOMBSTONE_PROPERTY: &str = "__logical_db_tombstone";

/// Opaque object handle valid only for the document that produced it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Object(ObjId);

impl Object {
    /// Root map of every Logical-DB document.
    #[must_use]
    pub fn root() -> Self {
        Self(ROOT)
    }
}

/// Application scalar accepted by the stable document editor.
#[derive(Clone, Debug, PartialEq)]
pub enum Scalar {
    /// Explicit null.
    Null,
    /// Boolean value.
    Boolean(bool),
    /// Signed integer value.
    Integer(i64),
    /// Unsigned integer value.
    Unsigned(u64),
    /// IEEE-754 finite number.
    Number(f64),
    /// UTF-8 string value.
    String(String),
    /// Opaque binary value.
    Bytes(Vec<u8>),
    /// Convergent integer counter.
    Counter(i64),
    /// Milliseconds since the Unix epoch.
    Timestamp(i64),
    /// Forward-compatible scalar not understood by this client version.
    Unknown {
        /// Automerge scalar type code.
        type_code: u8,
        /// Opaque encoded value.
        bytes: Vec<u8>,
    },
}

/// Convergent structured container kind owned by the Logical-DB API.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObjectKind {
    /// String-keyed object.
    Map,
    /// Legacy Automerge table object, retained for lossless reads.
    Table,
    /// Ordered sequence.
    List,
    /// Collaborative text sequence.
    Text,
}

impl From<ObjectKind> for ObjType {
    fn from(value: ObjectKind) -> Self {
        match value {
            ObjectKind::Map => Self::Map,
            ObjectKind::Table => Self::Table,
            ObjectKind::List => Self::List,
            ObjectKind::Text => Self::Text,
        }
    }
}

impl From<ObjType> for ObjectKind {
    fn from(value: ObjType) -> Self {
        match value {
            ObjType::Map => Self::Map,
            ObjType::Table => Self::Table,
            ObjType::List => Self::List,
            ObjType::Text => Self::Text,
        }
    }
}

impl From<Scalar> for ScalarValue {
    fn from(value: Scalar) -> Self {
        match value {
            Scalar::Null => Self::Null,
            Scalar::Boolean(value) => Self::Boolean(value),
            Scalar::Integer(value) => Self::Int(value),
            Scalar::Unsigned(value) => Self::Uint(value),
            Scalar::Number(value) => Self::F64(value),
            Scalar::String(value) => Self::Str(value.into()),
            Scalar::Bytes(value) => Self::Bytes(value),
            Scalar::Counter(value) => Self::Counter(value.into()),
            Scalar::Timestamp(value) => Self::Timestamp(value),
            Scalar::Unknown { type_code, bytes } => Self::Unknown { type_code, bytes },
        }
    }
}

impl From<ScalarValue> for Scalar {
    fn from(value: ScalarValue) -> Self {
        match value {
            ScalarValue::Null => Self::Null,
            ScalarValue::Boolean(value) => Self::Boolean(value),
            ScalarValue::Int(value) => Self::Integer(value),
            ScalarValue::Uint(value) => Self::Unsigned(value),
            ScalarValue::F64(value) => Self::Number(value),
            ScalarValue::Str(value) => Self::String(value.to_string()),
            ScalarValue::Bytes(value) => Self::Bytes(value),
            ScalarValue::Counter(value) => Self::Counter(value.into()),
            ScalarValue::Timestamp(value) => Self::Timestamp(value),
            ScalarValue::Unknown { type_code, bytes } => Self::Unknown { type_code, bytes },
        }
    }
}

/// One immutable Automerge change ready for durable storage or transport.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EncodedChange {
    /// Native Automerge change identity.
    pub digest: ChangeDigest,
    /// Canonical binary change representation.
    pub bytes: Vec<u8>,
}

impl EncodedChange {
    /// Verifies size, native decoding, and digest without applying the change.
    ///
    /// # Errors
    ///
    /// Returns a typed size, decode, or digest error.
    pub fn validate(&self) -> Result<(), CrdtError> {
        if !is_nonempty_bounded(self.bytes.len(), MAX_CHANGE_BYTES) {
            eprintln!("logical-db-crdt: validate: InvalidChangeSize");
            return Err(CrdtError::InvalidChangeSize(self.bytes.len()));
        }
        decode_change(self).map(|_| ())
    }
}

/// A durable convergent document.
pub struct Document {
    inner: AutoCommit,
}

impl Document {
    /// Creates an empty document.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: AutoCommit::new_with_encoding(TextEncoding::UnicodeCodePoint),
        }
    }

    /// Loads a complete durable snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`CrdtError::Automerge`] when bytes are malformed or fail
    /// document verification.
    pub fn load(bytes: &[u8]) -> Result<Self, CrdtError> {
        if !is_nonempty_bounded(bytes.len(), MAX_SNAPSHOT_BYTES) {
            eprintln!("logical-db-crdt: load: InvalidSnapshotSize");
            return Err(CrdtError::InvalidSnapshotSize(bytes.len()));
        }
        Ok(Self {
            inner: AutoCommit::load_with_options(
                bytes,
                LoadOptions::new().text_encoding(TextEncoding::UnicodeCodePoint),
            )?,
        })
    }

    /// Serializes a verified complete snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`CrdtError::Automerge`] if internal document verification fails.
    pub fn save(&mut self) -> Result<Vec<u8>, CrdtError> {
        let snapshot = self.inner.save_and_verify()?;
        if !is_nonempty_bounded(snapshot.len(), MAX_SNAPSHOT_BYTES) {
            eprintln!("logical-db-crdt: save: InvalidSnapshotSize");
            return Err(CrdtError::InvalidSnapshotSize(snapshot.len()));
        }
        Ok(snapshot)
    }

    /// Returns the current causal frontier.
    pub fn heads(&mut self) -> Vec<ChangeDigest> {
        self.inner
            .get_heads()
            .into_iter()
            .map(hash_digest)
            .collect()
    }

    /// Borrows a read-only projection view that cannot stage edits.
    #[must_use]
    pub fn reader(&mut self) -> Reader<'_> {
        Reader {
            document: &mut self.inner,
        }
    }

    /// Applies one application edit and returns only newly committed changes.
    ///
    /// # Errors
    ///
    /// Returns the editor operation's typed failure or an Automerge failure.
    pub fn edit<T>(
        &mut self,
        operation: impl FnOnce(&mut Editor<'_>) -> Result<T, CrdtError>,
    ) -> Result<(T, Vec<EncodedChange>), CrdtError> {
        let before = self.inner.get_heads();
        let value = match operation(&mut Editor {
            document: &mut self.inner,
        }) {
            Ok(value) => value,
            Err(error) => {
                self.inner.rollback();
                eprintln!("logical-db-crdt: edit: error");
                return Err(error);
            }
        };
        self.inner.commit();
        let changes = self
            .inner
            .get_changes(&before)
            .into_iter()
            .map(|change| encode_change(&change))
            .collect();
        Ok((value, changes))
    }

    /// Applies untrusted immutable changes after digest validation.
    ///
    /// # Errors
    ///
    /// Returns a decode, digest, dependency, or Automerge application failure.
    pub fn apply(&mut self, changes: &[EncodedChange]) -> Result<(), CrdtError> {
        if !is_at_most(changes.len(), MAX_CHANGES_PER_PAGE) {
            eprintln!("logical-db-crdt: apply: ChangePageTooLarge");
            return Err(CrdtError::ChangePageTooLarge);
        }
        let mut aggregate_bytes = 0usize;
        for change in changes {
            if !is_nonempty_bounded(change.bytes.len(), MAX_CHANGE_BYTES) {
                eprintln!("logical-db-crdt: apply: InvalidChangeSize");
                return Err(CrdtError::InvalidChangeSize(change.bytes.len()));
            }
            aggregate_bytes = aggregate_bytes
                .checked_add(change.bytes.len())
                .ok_or(CrdtError::ChangePageTooLarge)?;
            if !is_at_most(aggregate_bytes, MAX_PAGE_BYTES) {
                eprintln!("logical-db-crdt: apply: ChangePageTooLarge");
                return Err(CrdtError::ChangePageTooLarge);
            }
        }
        let decoded = changes
            .iter()
            .map(decode_change)
            .collect::<Result<Vec<_>, CrdtError>>()?;
        self.inner.apply_changes(decoded)?;
        Ok(())
    }

    /// Merges another verified document into this one, retaining both histories.
    ///
    /// The merge is a CRDT union: concurrent edits from either side survive and
    /// converge to the same visible value on every replica. A recovered baseline
    /// is merged with the replica's current state rather than replacing it, so an
    /// unsent local edit is never dropped by recovery.
    ///
    /// # Errors
    ///
    /// Returns [`CrdtError::Automerge`] when the merge fails.
    pub fn merge(&mut self, other: &mut Self) -> Result<(), CrdtError> {
        self.inner.merge(&mut other.inner)?;
        Ok(())
    }

    /// Reads one scalar or object from a map property.
    ///
    /// # Errors
    ///
    /// Returns [`CrdtError::Automerge`] when the object or property access is
    /// invalid for the current document.
    pub fn get(&mut self, object: &Object, property: &str) -> Result<Option<Item>, CrdtError> {
        validate_property(property)?;
        Ok(self
            .inner
            .get(&object.0, property)?
            .map(|(value, object)| item_from(value, object)))
    }

    /// Stages a first-class tombstone marker and returns its immutable change.
    ///
    /// The marker is a monotonic counter, so every delete — including a repeat
    /// on an already deleted document — produces exactly one change that
    /// merges without resurrecting the value on any replica. The snapshot and
    /// its conflict candidates are retained; absence is a read rule enforced
    /// above this boundary.
    ///
    /// # Errors
    ///
    /// Returns [`CrdtError::Automerge`] when the marker mutation fails.
    pub fn delete(&mut self) -> Result<Vec<EncodedChange>, CrdtError> {
        let before = self.inner.get_heads();
        let present = self
            .inner
            .get(&ROOT, TOMBSTONE_PROPERTY)
            .map_err(CrdtError::from)?
            .is_some();
        let staged = if present {
            self.inner
                .increment(&ROOT, TOMBSTONE_PROPERTY, 1)
                .map_err(CrdtError::from)
        } else {
            self.inner
                .put(&ROOT, TOMBSTONE_PROPERTY, ScalarValue::Counter(1.into()))
                .map_err(CrdtError::from)
        };
        if let Err(error) = staged {
            self.inner.rollback();
            eprintln!("logical-db-crdt: delete: error");
            return Err(error);
        }
        self.inner.commit();
        Ok(self
            .inner
            .get_changes(&before)
            .into_iter()
            .map(|change| encode_change(&change))
            .collect())
    }

    /// Returns whether a tombstone marker is present.
    ///
    /// # Errors
    ///
    /// Returns [`CrdtError::Automerge`] when the marker cannot be read.
    pub fn is_deleted(&mut self) -> Result<bool, CrdtError> {
        Ok(self.inner.get(&ROOT, TOMBSTONE_PROPERTY)?.is_some())
    }
}

impl Default for Document {
    fn default() -> Self {
        Self::new()
    }
}

/// Value observed through the stable document boundary.
#[derive(Clone, Debug, PartialEq)]
pub enum Item {
    /// Scalar materialized value.
    Scalar(Scalar),
    /// Nested map, list, or text object.
    Object {
        /// Stable convergent container behavior.
        kind: ObjectKind,
        /// Opaque document-local handle.
        handle: Object,
    },
}

/// One causally distinct value retained for a conflicting property or index.
#[derive(Clone, Debug, PartialEq)]
pub struct ConflictValue {
    /// Scalar or nested object authored by one causal operation.
    pub value: Item,
    /// Stable opaque Automerge operation identity for UI reconciliation.
    pub causal_id: Vec<u8>,
}

/// Read-only document view used by application projections.
///
/// ```compile_fail
/// use logical_db_crdt::Document;
/// let mut document = Document::new();
/// let mut reader = document.reader();
/// reader.edit(|_| Ok(()));
/// ```
pub struct Reader<'document> {
    document: &'document mut AutoCommit,
}

impl Reader<'_> {
    /// Reads one scalar or object from a map property.
    ///
    /// # Errors
    ///
    /// Returns a property, object, or Automerge access failure.
    pub fn get(&mut self, object: &Object, property: &str) -> Result<Option<Item>, CrdtError> {
        validate_property(property)?;
        Ok(self
            .document
            .get(&object.0, property)?
            .map(|(value, object)| item_from(value, object)))
    }

    /// Reads one visible list or text-sequence value by index.
    ///
    /// # Errors
    ///
    /// Returns an object, index, or Automerge access failure.
    pub fn get_index(&mut self, object: &Object, index: usize) -> Result<Option<Item>, CrdtError> {
        Ok(self
            .document
            .get(&object.0, index)?
            .map(|(value, object)| item_from(value, object)))
    }

    /// Returns every causally concurrent value for a map property.
    ///
    /// A non-conflicting property returns zero or one value. Callers that show
    /// user-visible state can surface or reconcile multiple values explicitly.
    ///
    /// # Errors
    ///
    /// Returns a property, object, or Automerge access failure.
    pub fn conflicts(
        &mut self,
        object: &Object,
        property: &str,
    ) -> Result<Vec<ConflictValue>, CrdtError> {
        validate_property(property)?;
        Ok(self
            .document
            .get_all(&object.0, property)?
            .into_iter()
            .map(|(value, operation)| ConflictValue {
                value: item_from(value, operation.clone()),
                causal_id: operation.to_bytes(),
            })
            .collect())
    }

    /// Returns every causally concurrent value at one list or text index.
    ///
    /// # Errors
    ///
    /// Returns an object, index, or Automerge access failure.
    pub fn conflicts_at(
        &mut self,
        object: &Object,
        index: usize,
    ) -> Result<Vec<ConflictValue>, CrdtError> {
        Ok(self
            .document
            .get_all(&object.0, index)?
            .into_iter()
            .map(|(value, operation)| ConflictValue {
                value: item_from(value, operation.clone()),
                causal_id: operation.to_bytes(),
            })
            .collect())
    }

    /// Lists the visible keys of a map or visible element identifiers of a sequence.
    #[must_use]
    pub fn keys(&mut self, object: &Object) -> Vec<String> {
        self.document.keys(&object.0).collect()
    }

    /// Returns the visible length of a map, list, or text object.
    #[must_use]
    pub fn len(&mut self, object: &Object) -> usize {
        self.document.length(&object.0)
    }

    /// Returns whether a map, list, or text object has no visible entries.
    #[must_use]
    pub fn is_empty(&mut self, object: &Object) -> bool {
        self.len(object) == 0
    }

    /// Materializes a collaborative text object as UTF-8.
    ///
    /// # Errors
    ///
    /// Returns an object-kind or Automerge access failure.
    pub fn text(&mut self, object: &Object) -> Result<String, CrdtError> {
        Ok(self.document.text(&object.0)?)
    }

    /// Returns whether a tombstone marker is present on the read document.
    ///
    /// Conflict candidates stay inspectable through [`Reader::conflicts`]
    /// after deletion; this flag tells the caller above the boundary to
    /// report the document absent instead.
    ///
    /// # Errors
    ///
    /// Returns an Automerge access failure.
    pub fn is_deleted(&mut self) -> Result<bool, CrdtError> {
        Ok(self.document.get(&ROOT, TOMBSTONE_PROPERTY)?.is_some())
    }
}

/// Mutable view whose lifetime cannot outlive its document edit.
pub struct Editor<'document> {
    document: &'document mut AutoCommit,
}

impl Editor<'_> {
    /// Sets a scalar property on a map-like object.
    ///
    /// # Errors
    ///
    /// Returns a property, numeric portability, object, or Automerge failure.
    pub fn put(&mut self, object: &Object, property: &str, value: Scalar) -> Result<(), CrdtError> {
        validate_property(property)?;
        validate_writable_property(property)?;
        validate_scalar(&value)?;
        self.document
            .put(&object.0, property, ScalarValue::from(value))?;
        Ok(())
    }

    /// Deletes a property. Repeating the deletion is harmless.
    ///
    /// # Errors
    ///
    /// Returns a property, object, or Automerge failure.
    pub fn delete(&mut self, object: &Object, property: &str) -> Result<(), CrdtError> {
        validate_property(property)?;
        validate_writable_property(property)?;
        self.document.delete(&object.0, property)?;
        Ok(())
    }

    /// Creates or replaces a nested map, list, or text object.
    ///
    /// # Errors
    ///
    /// Returns a property, object, or Automerge failure.
    pub fn put_object(
        &mut self,
        object: &Object,
        property: &str,
        kind: ObjectKind,
    ) -> Result<Object, CrdtError> {
        validate_property(property)?;
        validate_writable_property(property)?;
        Ok(Object(self.document.put_object(
            &object.0,
            property,
            ObjType::from(kind),
        )?))
    }

    /// Inserts a scalar into a list.
    ///
    /// # Errors
    ///
    /// Returns a numeric portability, list-index, object, or Automerge failure.
    pub fn insert(&mut self, list: &Object, index: usize, value: Scalar) -> Result<(), CrdtError> {
        validate_scalar(&value)?;
        self.document
            .insert(&list.0, index, ScalarValue::from(value))?;
        Ok(())
    }

    /// Replaces a visible scalar at one list index.
    ///
    /// # Errors
    ///
    /// Returns a numeric portability, index, object, or Automerge failure.
    pub fn put_index(
        &mut self,
        list: &Object,
        index: usize,
        value: Scalar,
    ) -> Result<(), CrdtError> {
        validate_scalar(&value)?;
        self.document
            .put(&list.0, index, ScalarValue::from(value))?;
        Ok(())
    }

    /// Inserts a nested map, list, or text object into a list.
    ///
    /// # Errors
    ///
    /// Returns an index, object, or Automerge failure.
    pub fn insert_object(
        &mut self,
        list: &Object,
        index: usize,
        kind: ObjectKind,
    ) -> Result<Object, CrdtError> {
        Ok(Object(self.document.insert_object(
            &list.0,
            index,
            ObjType::from(kind),
        )?))
    }

    /// Deletes one visible list or text-sequence value.
    ///
    /// # Errors
    ///
    /// Returns an index, object, or Automerge failure.
    pub fn delete_index(&mut self, object: &Object, index: usize) -> Result<(), CrdtError> {
        self.document.delete(&object.0, index)?;
        Ok(())
    }

    /// Adds a signed delta to a map counter.
    ///
    /// # Errors
    ///
    /// Returns a property, type, object, or Automerge failure.
    pub fn increment(
        &mut self,
        object: &Object,
        property: &str,
        delta: i64,
    ) -> Result<(), CrdtError> {
        validate_property(property)?;
        validate_writable_property(property)?;
        self.document.increment(&object.0, property, delta)?;
        Ok(())
    }

    /// Adds a signed delta to a list counter.
    ///
    /// # Errors
    ///
    /// Returns an index, type, object, or Automerge failure.
    pub fn increment_index(
        &mut self,
        list: &Object,
        index: usize,
        delta: i64,
    ) -> Result<(), CrdtError> {
        self.document.increment(&list.0, index, delta)?;
        Ok(())
    }

    /// Replaces a bounded range of collaborative text using Unicode code-point offsets.
    ///
    /// # Errors
    ///
    /// Returns a text-size, range, object, or Automerge failure.
    pub fn splice_text(
        &mut self,
        text: &Object,
        position: usize,
        delete: usize,
        replacement: &str,
    ) -> Result<(), CrdtError> {
        if replacement.len() > MAX_CHANGE_BYTES {
            eprintln!("logical-db-crdt: splice_text: ScalarTooLarge");
            return Err(CrdtError::ScalarTooLarge(replacement.len()));
        }
        let delete = isize::try_from(delete).map_err(|error| {
            eprintln!("logical-db-crdt: CrdtError::InvalidSequenceRange: {error}");
            CrdtError::InvalidSequenceRange
        })?;
        self.document
            .splice_text(&text.0, position, delete, replacement)?;
        Ok(())
    }
}

/// Stable failures from the CRDT adapter.
#[derive(Debug, Error)]
pub enum CrdtError {
    /// Automerge rejected malformed state or an invalid operation.
    #[error("CRDT operation failed: {0}")]
    Automerge(#[from] AutomergeError),
    /// One immutable change failed decoding before application.
    #[error("CRDT change decoding failed: {0}")]
    ChangeDecode(#[from] LoadChangeError),
    /// Encoded bytes did not match their asserted native change hash.
    #[error("encoded change digest does not match its bytes")]
    DigestMismatch,
    /// One immutable change was empty or exceeded the envelope limit.
    #[error("CRDT change byte length {0} is outside protocol bounds")]
    InvalidChangeSize(usize),
    /// A complete snapshot was empty or exceeded its materialization ceiling.
    #[error("CRDT snapshot byte length {0} is outside local bounds")]
    InvalidSnapshotSize(usize),
    /// A change batch exceeded its count or aggregate byte ceiling.
    #[error("CRDT change page exceeds bounded protocol limits")]
    ChangePageTooLarge,
    /// Map property names are bounded and non-empty.
    #[error("property name is empty or exceeds 512 UTF-8 bytes")]
    InvalidProperty,
    /// NaN and infinity are rejected from portable structured state.
    #[error("non-finite numeric values are not portable")]
    NonFiniteNumber,
    /// One scalar alone exceeded the maximum change envelope.
    #[error("scalar byte length {0} exceeds the structured value limit")]
    ScalarTooLarge(usize),
    /// Unknown forward-compatible scalar encodings are read-only.
    #[error("unknown scalar encodings cannot be authored by this client")]
    UnknownScalarWrite,
    /// A sequence deletion could not be represented by the Automerge API.
    #[error("sequence deletion range exceeds the supported index domain")]
    InvalidSequenceRange,
}

fn item_from(value: Value<'_>, object: ObjId) -> Item {
    match value {
        Value::Object(kind) => Item::Object {
            kind: kind.into(),
            handle: Object(object),
        },
        Value::Scalar(value) => Item::Scalar(value.into_owned().into()),
    }
}

fn validate_property(property: &str) -> Result<(), CrdtError> {
    if is_nonempty_bounded(property.len(), 512) {
        Ok(())
    } else {
        Err(CrdtError::InvalidProperty)
    }
}

fn validate_writable_property(property: &str) -> Result<(), CrdtError> {
    if property == TOMBSTONE_PROPERTY {
        Err(CrdtError::InvalidProperty)
    } else {
        Ok(())
    }
}

fn validate_scalar(value: &Scalar) -> Result<(), CrdtError> {
    match value {
        Scalar::Number(number) if !number.is_finite() => Err(CrdtError::NonFiniteNumber),
        Scalar::String(value) if value.len() > MAX_CHANGE_BYTES => {
            Err(CrdtError::ScalarTooLarge(value.len()))
        }
        Scalar::Bytes(value) | Scalar::Unknown { bytes: value, .. }
            if value.len() > MAX_CHANGE_BYTES =>
        {
            Err(CrdtError::ScalarTooLarge(value.len()))
        }
        Scalar::Unknown { .. } => Err(CrdtError::UnknownScalarWrite),
        _ => Ok(()),
    }
}

fn is_nonempty_bounded(length: usize, maximum: usize) -> bool {
    (1..=maximum).contains(&length)
}

fn is_at_most(length: usize, maximum: usize) -> bool {
    length <= maximum
}

fn hash_digest(hash: ChangeHash) -> ChangeDigest {
    ChangeDigest::new(hash.0)
}

fn encode_change(change: &Change) -> EncodedChange {
    let digest = hash_digest(change.hash());
    EncodedChange {
        digest,
        bytes: change.raw_bytes().to_vec(),
    }
}

fn decode_change(encoded: &EncodedChange) -> Result<Change, CrdtError> {
    let change = Change::from_bytes(encoded.bytes.clone())?;
    if hash_digest(change.hash()) != encoded.digest {
        eprintln!("logical-db-crdt: decode_change: DigestMismatch");
        return Err(CrdtError::DigestMismatch);
    }
    Ok(change)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn one_change() -> EncodedChange {
        let mut document = Document::new();
        document
            .edit(|editor| editor.put(&Object::root(), "value", Scalar::Integer(1)))
            .unwrap()
            .1
            .pop()
            .unwrap()
    }

    #[test]
    fn concurrent_fields_converge_independent_of_delivery_order() {
        let mut origin = Document::new();
        origin
            .edit(|editor| editor.put(&Object::root(), "title", Scalar::String("base".into())))
            .unwrap();
        let snapshot = origin.save().unwrap();
        let mut first = Document::load(&snapshot).unwrap();
        let mut second = Document::load(&snapshot).unwrap();

        let ((), first_changes) = first
            .edit(|editor| editor.put(&Object::root(), "left", Scalar::Integer(1)))
            .unwrap();
        let ((), second_changes) = second
            .edit(|editor| editor.put(&Object::root(), "right", Scalar::Integer(2)))
            .unwrap();

        first.apply(&second_changes).unwrap();
        second.apply(&first_changes).unwrap();
        let mut first_heads = first.heads();
        let mut second_heads = second.heads();
        first_heads.sort_unstable();
        second_heads.sort_unstable();
        assert_eq!(first_heads, second_heads);
        assert_eq!(
            first.get(&Object::root(), "left").unwrap(),
            second.get(&Object::root(), "left").unwrap()
        );
        assert_eq!(
            first.get(&Object::root(), "right").unwrap(),
            second.get(&Object::root(), "right").unwrap()
        );
    }

    #[test]
    fn duplicate_change_delivery_is_idempotent() {
        let mut source = Document::new();
        let ((), changes) = source
            .edit(|editor| editor.put(&Object::root(), "value", Scalar::Integer(7)))
            .unwrap();
        let mut target = Document::new();
        target.apply(&changes).unwrap();
        let once = target.save().unwrap();
        target.apply(&changes).unwrap();
        assert_eq!(target.save().unwrap(), once);
    }

    #[test]
    fn concurrent_same_property_values_remain_explicitly_inspectable() {
        let mut origin = Document::new();
        origin
            .edit(|editor| editor.put(&Object::root(), "status", Scalar::String("base".into())))
            .unwrap();
        let snapshot = origin.save().unwrap();
        let mut first = Document::load(&snapshot).unwrap();
        let mut second = Document::load(&snapshot).unwrap();
        let first_change = first
            .edit(|editor| editor.put(&Object::root(), "status", Scalar::String("first".into())))
            .unwrap()
            .1;
        let second_change = second
            .edit(|editor| editor.put(&Object::root(), "status", Scalar::String("second".into())))
            .unwrap()
            .1;
        first.apply(&second_change).unwrap();
        second.apply(&first_change).unwrap();
        let mut values = first.reader().conflicts(&Object::root(), "status").unwrap();
        values.sort_by(|left, right| left.causal_id.cmp(&right.causal_id));
        assert_eq!(values.len(), 2);
        assert!(values.iter().all(|value| !value.causal_id.is_empty()));
        assert!(
            values
                .iter()
                .any(|value| { value.value == Item::Scalar(Scalar::String("first".into())) })
        );
        assert!(
            values
                .iter()
                .any(|value| { value.value == Item::Scalar(Scalar::String("second".into())) })
        );
    }

    #[test]
    fn failed_editor_operation_rolls_back_every_staged_mutation() {
        let mut document = Document::new();
        assert!(matches!(
            document.edit::<()>(|editor| {
                editor.put(&Object::root(), "leaked", Scalar::Integer(1))?;
                Err(CrdtError::InvalidProperty)
            }),
            Err(CrdtError::InvalidProperty)
        ));
        let ((), changes) = document
            .edit(|editor| editor.put(&Object::root(), "committed", Scalar::Integer(2)))
            .unwrap();
        assert_eq!(changes.len(), 1);
        assert_eq!(document.get(&Object::root(), "leaked").unwrap(), None);
        assert_eq!(
            document.get(&Object::root(), "committed").unwrap(),
            Some(Item::Scalar(Scalar::Integer(2)))
        );
    }

    #[test]
    fn apply_rejects_oversized_change_before_decoding() {
        let mut document = Document::new();
        let oversized = EncodedChange {
            digest: ChangeDigest::new([0; 32]),
            bytes: vec![0; MAX_CHANGE_BYTES + 1],
        };
        assert!(matches!(
            document.apply(&[oversized]),
            Err(CrdtError::InvalidChangeSize(size)) if size == MAX_CHANGE_BYTES + 1
        ));
    }

    #[test]
    fn snapshots_changes_heads_and_reader_validate_their_exact_boundaries() {
        let change = one_change();
        assert!(change.validate().is_ok());
        assert!(matches!(
            EncodedChange {
                digest: change.digest,
                bytes: vec![]
            }
            .validate(),
            Err(CrdtError::InvalidChangeSize(0))
        ));
        let mut forged = change;
        forged.digest = ChangeDigest::new([99; 32]);
        assert!(matches!(forged.validate(), Err(CrdtError::DigestMismatch)));
        let malformed = EncodedChange {
            digest: ChangeDigest::new([0; 32]),
            bytes: vec![1],
        };
        assert!(matches!(
            malformed.validate(),
            Err(CrdtError::ChangeDecode(_))
        ));

        let mut source = Document::new();
        source
            .edit(|editor| editor.put(&Object::root(), "answer", Scalar::Integer(42)))
            .unwrap();
        assert!(!source.heads().is_empty());
        let snapshot = source.save().unwrap();
        assert!(!snapshot.is_empty());
        let mut restored = Document::load(&snapshot).unwrap();
        assert_eq!(restored.heads(), source.heads());
        assert_eq!(
            restored.reader().get(&Object::root(), "answer").unwrap(),
            Some(Item::Scalar(Scalar::Integer(42)))
        );
        assert!(Document::load(b"not-an-automerge-snapshot").is_err());
    }

    #[test]
    fn editor_delete_insert_and_each_object_kind_change_materialized_state() {
        let mut document = Document::new();
        let kinds = [
            ("map", ObjectKind::Map),
            ("table", ObjectKind::Table),
            ("list", ObjectKind::List),
            ("text", ObjectKind::Text),
        ];
        let ((), changes) = document
            .edit(|editor| {
                editor.put(&Object::root(), "remove", Scalar::Boolean(true))?;
                for (property, kind) in kinds {
                    editor.put_object(&Object::root(), property, kind)?;
                }
                Ok(())
            })
            .unwrap();
        assert_eq!(changes.len(), 1);
        for (property, expected) in kinds {
            assert!(matches!(
                document.reader().get(&Object::root(), property).unwrap(),
                Some(Item::Object { kind, .. }) if kind == expected
            ));
        }
        let Some(Item::Object { handle: list, .. }) =
            document.get(&Object::root(), "list").unwrap()
        else {
            panic!("list object expected")
        };
        let ((), changes) = document
            .edit(|editor| editor.delete(&Object::root(), "remove"))
            .unwrap();
        assert_eq!(changes.len(), 1);
        assert_eq!(document.get(&Object::root(), "remove").unwrap(), None);
        let ((), changes) = document
            .edit(|editor| editor.insert(&list, 0, Scalar::String("first".into())))
            .unwrap();
        assert_eq!(changes.len(), 1);
    }

    #[test]
    fn lists_text_counters_and_discovery_are_available_through_stable_api() {
        let mut document = Document::new();
        let ((), changes) = document
            .edit(|editor| {
                let list = editor.put_object(&Object::root(), "items", ObjectKind::List)?;
                editor.insert(&list, 0, Scalar::String("old".into()))?;
                editor.put_index(&list, 0, Scalar::String("new".into()))?;
                editor.insert_object(&list, 1, ObjectKind::Map)?;
                editor.delete_index(&list, 1)?;
                editor.put(&Object::root(), "count", Scalar::Counter(1))?;
                editor.increment(&Object::root(), "count", 2)?;
                let text = editor.put_object(&Object::root(), "body", ObjectKind::Text)?;
                editor.splice_text(&text, 0, 0, "héllo")?;
                Ok(())
            })
            .unwrap();
        assert_eq!(changes.len(), 1);
        let Some(Item::Object { handle: list, .. }) =
            document.get(&Object::root(), "items").unwrap()
        else {
            panic!("list expected")
        };
        let Some(Item::Object { handle: text, .. }) =
            document.get(&Object::root(), "body").unwrap()
        else {
            panic!("text expected")
        };
        let mut reader = document.reader();
        assert!(reader.keys(&Object::root()).contains(&"items".to_owned()));
        assert_eq!(reader.len(&list), 1);
        assert!(!reader.is_empty(&list));
        assert_eq!(
            reader.get_index(&list, 0).unwrap(),
            Some(Item::Scalar(Scalar::String("new".into())))
        );
        assert_eq!(reader.text(&text).unwrap(), "héllo");
        assert_eq!(
            reader.get(&Object::root(), "count").unwrap(),
            Some(Item::Scalar(Scalar::Counter(3)))
        );
    }

    #[test]
    fn property_and_scalar_portability_rules_accept_edges_and_reject_violations() {
        assert!(!is_nonempty_bounded(0, MAX_CHANGE_BYTES));
        assert!(is_nonempty_bounded(MAX_CHANGE_BYTES, MAX_CHANGE_BYTES));
        assert!(!is_nonempty_bounded(MAX_CHANGE_BYTES + 1, MAX_CHANGE_BYTES));
        assert!(is_at_most(MAX_CHANGES_PER_PAGE, MAX_CHANGES_PER_PAGE));
        assert!(!is_at_most(MAX_CHANGES_PER_PAGE + 1, MAX_CHANGES_PER_PAGE));
        let maximum_property = "p".repeat(512);
        assert!(validate_property(&maximum_property).is_ok());
        assert!(matches!(
            validate_property(""),
            Err(CrdtError::InvalidProperty)
        ));
        assert!(matches!(
            validate_property(&format!("{maximum_property}p")),
            Err(CrdtError::InvalidProperty)
        ));
        for scalar in [
            Scalar::Number(f64::NAN),
            Scalar::Number(f64::INFINITY),
            Scalar::Number(f64::NEG_INFINITY),
        ] {
            assert!(matches!(
                validate_scalar(&scalar),
                Err(CrdtError::NonFiniteNumber)
            ));
        }
        for scalar in [
            Scalar::Number(1.25),
            Scalar::String("small".into()),
            Scalar::Bytes(vec![1]),
        ] {
            assert!(validate_scalar(&scalar).is_ok());
        }
        assert!(validate_scalar(&Scalar::String("x".repeat(MAX_CHANGE_BYTES))).is_ok());
        assert!(validate_scalar(&Scalar::Bytes(vec![1; MAX_CHANGE_BYTES])).is_ok());
        assert!(matches!(
            validate_scalar(&Scalar::String("x".repeat(MAX_CHANGE_BYTES + 1))),
            Err(CrdtError::ScalarTooLarge(_))
        ));
        assert!(matches!(
            validate_scalar(&Scalar::Bytes(vec![1; MAX_CHANGE_BYTES + 1])),
            Err(CrdtError::ScalarTooLarge(_))
        ));
        assert!(matches!(
            validate_scalar(&Scalar::Unknown {
                type_code: 7,
                bytes: vec![]
            }),
            Err(CrdtError::UnknownScalarWrite)
        ));
    }

    #[test]
    fn snapshot_decoder_rejects_empty_and_oversized_input_before_autoconstruction() {
        assert!(matches!(
            Document::load(&[]),
            Err(CrdtError::InvalidSnapshotSize(0))
        ));
        assert!(matches!(
            Document::load(&vec![0; MAX_SNAPSHOT_BYTES + 1]),
            Err(CrdtError::InvalidSnapshotSize(size)) if size == MAX_SNAPSHOT_BYTES + 1
        ));
    }

    #[test]
    fn apply_count_boundary_accepts_exact_maximum_and_rejects_successor() {
        let change = one_change();
        let exact = vec![change.clone(); MAX_CHANGES_PER_PAGE];
        let mut document = Document::new();
        document.apply(&exact).unwrap();
        let excessive = vec![change; MAX_CHANGES_PER_PAGE + 1];
        assert!(matches!(
            document.apply(&excessive),
            Err(CrdtError::ChangePageTooLarge)
        ));
    }

    #[test]
    fn reserved_tombstone_property_is_rejected_for_application_writes() {
        let mut document = Document::new();
        assert!(matches!(
            document.edit(|editor| {
                editor.put(&Object::root(), TOMBSTONE_PROPERTY, Scalar::Boolean(true))
            }),
            Err(CrdtError::InvalidProperty)
        ));
        assert!(matches!(
            document.edit(|editor| editor.delete(&Object::root(), TOMBSTONE_PROPERTY)),
            Err(CrdtError::InvalidProperty)
        ));
        assert!(matches!(
            document.edit(|editor| editor.put_object(
                &Object::root(),
                TOMBSTONE_PROPERTY,
                ObjectKind::Map
            )),
            Err(CrdtError::InvalidProperty)
        ));
        assert!(matches!(
            document.edit(|editor| editor.increment(&Object::root(), TOMBSTONE_PROPERTY, 1)),
            Err(CrdtError::InvalidProperty)
        ));
        assert!(!document.is_deleted().unwrap());
        let ((), changes) = document
            .edit(|editor| editor.put(&Object::root(), "ok", Scalar::Integer(1)))
            .unwrap();
        assert_eq!(changes.len(), 1);
    }

    #[test]
    fn document_delete_marks_tombstone_with_exactly_one_change() {
        let mut document = Document::new();
        assert!(!document.is_deleted().unwrap());
        assert!(!document.reader().is_deleted().unwrap());
        let changes = document.delete().unwrap();
        assert_eq!(changes.len(), 1);
        assert!(document.is_deleted().unwrap());
        assert!(document.reader().is_deleted().unwrap());
        let snapshot = document.save().unwrap();
        let mut restored = Document::load(&snapshot).unwrap();
        assert!(restored.is_deleted().unwrap());
        assert!(restored.reader().is_deleted().unwrap());
        let again = restored.delete().unwrap();
        assert_eq!(again.len(), 1);
        assert!(restored.is_deleted().unwrap());
    }

    #[test]
    fn tombstone_survives_merge_and_concurrent_edits_stay_inspectable() {
        let mut origin = Document::new();
        origin
            .edit(|editor| editor.put(&Object::root(), "title", Scalar::String("base".into())))
            .unwrap();
        let snapshot = origin.save().unwrap();
        let mut deleter = Document::load(&snapshot).unwrap();
        let mut editor_side = Document::load(&snapshot).unwrap();
        let tombstone_changes = deleter.delete().unwrap();
        let ((), edit_changes) = editor_side
            .edit(|editor| editor.put(&Object::root(), "title", Scalar::String("rival".into())))
            .unwrap();
        deleter.apply(&edit_changes).unwrap();
        editor_side.apply(&tombstone_changes).unwrap();
        assert!(deleter.is_deleted().unwrap());
        assert!(editor_side.is_deleted().unwrap());
        assert!(deleter.reader().is_deleted().unwrap());
        assert!(editor_side.reader().is_deleted().unwrap());
        for side in [&mut deleter, &mut editor_side] {
            let values = side.reader().conflicts(&Object::root(), "title").unwrap();
            assert!(
                values
                    .iter()
                    .any(|value| value.value == Item::Scalar(Scalar::String("rival".into())))
            );
        }
    }

    #[test]
    fn fresh_document_delete_applies_cleanly_to_an_empty_replica() {
        let mut deleter = Document::new();
        let tombstone_changes = deleter.delete().unwrap();
        let mut late = Document::new();
        assert!(!late.is_deleted().unwrap());
        late.apply(&tombstone_changes).unwrap();
        assert!(late.is_deleted().unwrap());
    }
}
