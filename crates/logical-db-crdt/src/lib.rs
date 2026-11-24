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
#[non_exhaustive]
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
#[non_exhaustive]
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
#[non_exhaustive]
pub struct EncodedChange {
    /// Native Automerge change identity.
    digest: ChangeDigest,
    /// Canonical binary change representation.
    bytes: Vec<u8>,
}

impl EncodedChange {
    /// Builds one encoded change from its native digest and canonical bytes.
    #[must_use]
    pub fn new(digest: ChangeDigest, bytes: Vec<u8>) -> Self {
        Self { digest, bytes }
    }

    /// Returns the native Automerge change identity.
    #[must_use]
    pub const fn digest(&self) -> ChangeDigest {
        self.digest
    }

    /// Borrows the canonical binary change representation.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Takes the canonical binary change representation.
    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    /// Verifies size, native decoding, and digest without applying the change.
    ///
    /// # Errors
    ///
    /// Returns a typed size, decode, or digest error.
    pub fn validate(&self) -> Result<(), CrdtError> {
        if !is_nonempty_bounded(self.bytes.len(), MAX_CHANGE_BYTES) {
            return Err(CrdtError::InvalidChangeSize(self.bytes.len()));
        }
        decode_change(self).map(|_| ())
    }
}

/// A durable convergent document.
pub struct Document {
    /// The native Automerge document carrying all convergent state.
    inner: AutoCommit,
}

impl std::fmt::Debug for Document {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("Document").finish_non_exhaustive()
    }
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
            return Err(CrdtError::ChangePageTooLarge);
        }
        let mut aggregate_bytes = 0usize;
        for change in changes {
            if !is_nonempty_bounded(change.bytes.len(), MAX_CHANGE_BYTES) {
                return Err(CrdtError::InvalidChangeSize(change.bytes.len()));
            }
            aggregate_bytes = aggregate_bytes
                .checked_add(change.bytes.len())
                .ok_or(CrdtError::ChangePageTooLarge)?;
            if !is_at_most(aggregate_bytes, MAX_PAGE_BYTES) {
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
#[non_exhaustive]
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
#[non_exhaustive]
pub struct ConflictValue {
    /// Scalar or nested object authored by one causal operation.
    value: Item,
    /// Stable opaque Automerge operation identity for UI reconciliation.
    causal_id: Vec<u8>,
}

impl ConflictValue {
    /// Builds one conflicting value from its item and causal operation identity.
    #[must_use]
    pub const fn new(value: Item, causal_id: Vec<u8>) -> Self {
        Self { value, causal_id }
    }

    /// Borrows the scalar or nested object authored by one causal operation.
    #[must_use]
    pub const fn value(&self) -> &Item {
        &self.value
    }

    /// Borrows the stable opaque Automerge operation identity.
    #[must_use]
    pub fn causal_id(&self) -> &[u8] {
        &self.causal_id
    }

    /// Takes the item and its causal operation identity.
    #[must_use]
    pub fn into_parts(self) -> (Item, Vec<u8>) {
        (self.value, self.causal_id)
    }
}

#[cfg(test)]
mod tests;
mod view;

pub use view::{Editor, Reader};

/// Stable failures from the CRDT adapter.
#[derive(Debug, Error)]
#[non_exhaustive]
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
    SequenceWidth(#[source] std::num::TryFromIntError),
}

/// Converts a native Automerge value and handle into a stable [`Item`].
fn item_from(value: Value<'_>, object: ObjId) -> Item {
    match value {
        Value::Object(kind) => Item::Object {
            kind: kind.into(),
            handle: Object(object),
        },
        Value::Scalar(value) => Item::Scalar(value.into_owned().into()),
    }
}

/// Rejects empty property names and names above the 512-byte bound.
fn validate_property(property: &str) -> Result<(), CrdtError> {
    if is_nonempty_bounded(property.len(), 512) {
        Ok(())
    } else {
        Err(CrdtError::InvalidProperty)
    }
}

/// Rejects application writes to the reserved tombstone property.
fn validate_writable_property(property: &str) -> Result<(), CrdtError> {
    if property == TOMBSTONE_PROPERTY {
        Err(CrdtError::InvalidProperty)
    } else {
        Ok(())
    }
}

/// Rejects non-finite numbers, oversize payloads, and unknown authored scalars.
fn validate_scalar(value: &Scalar) -> Result<(), CrdtError> {
    match *value {
        Scalar::Number(number) if !number.is_finite() => Err(CrdtError::NonFiniteNumber),
        Scalar::String(ref text) if text.len() > MAX_CHANGE_BYTES => {
            Err(CrdtError::ScalarTooLarge(text.len()))
        }
        Scalar::Bytes(ref bytes) | Scalar::Unknown { ref bytes, .. }
            if bytes.len() > MAX_CHANGE_BYTES =>
        {
            Err(CrdtError::ScalarTooLarge(bytes.len()))
        }
        Scalar::Unknown { .. } => Err(CrdtError::UnknownScalarWrite),
        Scalar::Null
        | Scalar::Boolean(_)
        | Scalar::Integer(_)
        | Scalar::Unsigned(_)
        | Scalar::Number(_)
        | Scalar::String(_)
        | Scalar::Bytes(_)
        | Scalar::Counter(_)
        | Scalar::Timestamp(_) => Ok(()),
    }
}

/// Reports whether `length` lies in the inclusive `1..=maximum` range.
fn is_nonempty_bounded(length: usize, maximum: usize) -> bool {
    (1..=maximum).contains(&length)
}

/// Reports whether `length` does not exceed `maximum`.
fn is_at_most(length: usize, maximum: usize) -> bool {
    length <= maximum
}

/// Wraps one native change hash as a stable content digest.
fn hash_digest(hash: ChangeHash) -> ChangeDigest {
    ChangeDigest::new(hash.0)
}

/// Encodes one committed native change with its hash and canonical bytes.
fn encode_change(change: &Change) -> EncodedChange {
    let digest = hash_digest(change.hash());
    EncodedChange {
        digest,
        bytes: change.raw_bytes().to_vec(),
    }
}

/// Decodes one change and verifies it against its asserted digest.
fn decode_change(encoded: &EncodedChange) -> Result<Change, CrdtError> {
    let change = Change::from_bytes(encoded.bytes.clone())?;
    if hash_digest(change.hash()) != encoded.digest {
        return Err(CrdtError::DigestMismatch);
    }
    Ok(change)
}
