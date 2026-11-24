//! Versioned, bounded state-bearing snapshot baseline records.

//! Content-addressed state-bearing baseline artifacts for safe log retention.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use crate::{DocumentId, ModelError, Scope};
use lgwks_std::hash::Hasher;

/// Current state-bearing baseline descriptor version.
pub const SNAPSHOT_BASELINE_VERSION: u16 = 2;
/// Legacy position-only baseline descriptor version.
pub const SNAPSHOT_BASELINE_LEGACY_VERSION: u16 = 1;
/// Encoded byte length of a v2 baseline descriptor.
pub const SNAPSHOT_BASELINE_BYTES: usize = 50;
/// Encoded byte length of a legacy v1 position descriptor.
pub const SNAPSHOT_BASELINE_LEGACY_BYTES: usize = 10;
/// Maximum documents in one baseline page or single-page artifact transfer.
pub const MAX_BASELINE_PAGE_DOCUMENTS: usize = 256;
/// Maximum documents in one complete sealed snapshot artifact.
///
/// The paging client consumes continuation pages, so the aggregate artifact may
/// exceed one page. Aggregate payload is independently bounded by
/// [`MAX_BASELINE_BYTES`]; this count bound keeps per-document identity and
/// buffer overhead proportional to the payload bound instead of leaving it
/// unbounded for tiny documents.
pub const MAX_BASELINE_DOCUMENTS: usize = 65_536;
/// Maximum aggregate snapshot payload bytes.
pub const MAX_BASELINE_BYTES: usize = 4 * 1024 * 1024;

/// Content digest of the canonical snapshot artifact.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct BaselineDigest([u8; 32]);

impl BaselineDigest {
    /// Wraps a canonical digest byte array.
    #[must_use]
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns the digest bytes.
    #[must_use]
    pub const fn into_bytes(self) -> [u8; 32] {
        self.0
    }
}

/// Fixed descriptor for one state-bearing snapshot artifact.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotBaseline {
    /// Descriptor layout version carried on the wire.
    version: u16,
    /// Log position the described artifact covers.
    position: u64,
    /// Canonical digest of the covered artifact.
    digest: BaselineDigest,
    /// Number of documents in the described artifact.
    document_count: u32,
    /// Aggregate snapshot payload byte length.
    byte_len: u32,
}

impl SnapshotBaseline {
    /// Constructs a v2 descriptor from canonical artifact metadata.
    #[must_use]
    pub const fn new(
        position: u64,
        digest: BaselineDigest,
        document_count: u32,
        byte_len: u32,
    ) -> Self {
        Self {
            version: SNAPSHOT_BASELINE_VERSION,
            position,
            digest,
            document_count,
            byte_len,
        }
    }

    /// Returns the descriptor version.
    #[must_use]
    pub const fn version(&self) -> u16 {
        self.version
    }
    /// Returns the covered log position.
    #[must_use]
    pub const fn position(&self) -> u64 {
        self.position
    }
    /// Returns the canonical artifact digest.
    #[must_use]
    pub const fn digest(&self) -> BaselineDigest {
        self.digest
    }
    /// Returns the ordered document count, bounded by [`MAX_BASELINE_DOCUMENTS`].
    #[must_use]
    pub const fn document_count(&self) -> u32 {
        self.document_count
    }
    /// Returns the aggregate snapshot byte length.
    #[must_use]
    pub const fn byte_len(&self) -> u32 {
        self.byte_len
    }

    /// Encodes the fixed v2 descriptor representation.
    #[must_use]
    pub fn encode(&self) -> [u8; SNAPSHOT_BASELINE_BYTES] {
        let mut out = [0; SNAPSHOT_BASELINE_BYTES];
        out[..2].copy_from_slice(&self.version.to_be_bytes());
        out[2..10].copy_from_slice(&self.position.to_be_bytes());
        out[10..42].copy_from_slice(&self.digest.into_bytes());
        out[42..46].copy_from_slice(&self.document_count.to_be_bytes());
        out[46..50].copy_from_slice(&self.byte_len.to_be_bytes());
        out
    }

    /// Decodes an explicit legacy v1 or current v2 descriptor.
    ///
    /// # Errors
    ///
    /// Returns a typed model error for malformed lengths or unsupported versions.
    pub fn decode(bytes: &[u8]) -> Result<SnapshotRecord, ModelError> {
        let mut rest = bytes;
        let version = u16::from_be_bytes(take_array::<2>(&mut rest)?);
        if bytes.len() == SNAPSHOT_BASELINE_LEGACY_BYTES {
            if version != SNAPSHOT_BASELINE_LEGACY_VERSION {
                return Err(ModelError::UnsupportedSnapshotVersion { actual: version });
            }
            let position = u64::from_be_bytes(take_array::<8>(&mut rest)?);
            return Ok(SnapshotRecord::LegacyPosition(position));
        }
        if bytes.len() != SNAPSHOT_BASELINE_BYTES {
            return Err(ModelError::InvalidSnapshotEncoding);
        }
        if version != SNAPSHOT_BASELINE_VERSION {
            return Err(ModelError::UnsupportedSnapshotVersion { actual: version });
        }
        let baseline = Self {
            version,
            position: u64::from_be_bytes(take_array::<8>(&mut rest)?),
            digest: BaselineDigest(take_array::<32>(&mut rest)?),
            document_count: u32::from_be_bytes(take_array::<4>(&mut rest)?),
            byte_len: u32::from_be_bytes(take_array::<4>(&mut rest)?),
        };
        if !within(baseline.document_count, MAX_BASELINE_DOCUMENTS) {
            return Err(ModelError::SnapshotTooManyDocuments);
        }
        if !within(baseline.byte_len, MAX_BASELINE_BYTES) {
            return Err(ModelError::SnapshotTooLarge);
        }
        Ok(SnapshotRecord::State(baseline))
    }
}

/// Result of decoding a versioned snapshot descriptor, bounds already checked.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SnapshotRecord {
    /// Legacy position-only descriptor; it cannot satisfy state recovery.
    LegacyPosition(u64),
    /// State-bearing v2 descriptor.
    State(SnapshotBaseline),
}

/// One bounded document state included in a snapshot artifact.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct SnapshotDocument {
    /// Stable document identity.
    document: DocumentId,
    /// Canonical serialized document state.
    snapshot: Vec<u8>,
    /// Whether the document is a tombstone.
    tombstone: bool,
}

impl SnapshotDocument {
    /// Builds one document entry from its identity, canonical bytes, and flag.
    #[must_use]
    pub const fn new(document: DocumentId, snapshot: Vec<u8>, tombstone: bool) -> Self {
        Self {
            document,
            snapshot,
            tombstone,
        }
    }

    /// Returns the stable document identity.
    #[must_use]
    pub const fn document(&self) -> DocumentId {
        self.document
    }

    /// Borrows the canonical serialized document state.
    #[must_use]
    pub fn snapshot(&self) -> &[u8] {
        &self.snapshot
    }

    /// Returns whether the document is a tombstone.
    #[must_use]
    pub const fn is_tombstone(&self) -> bool {
        self.tombstone
    }
}

/// A descriptor and its ordered, bounded document states.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct SnapshotArtifact {
    /// Descriptor computed from the artifact documents.
    baseline: SnapshotBaseline,
    /// Strictly ascending document list.
    documents: Vec<SnapshotDocument>,
}

impl SnapshotArtifact {
    /// Borrows the descriptor computed from the artifact documents.
    #[must_use]
    pub const fn baseline(&self) -> &SnapshotBaseline {
        &self.baseline
    }

    /// Borrows the strictly ascending document list.
    #[must_use]
    pub fn documents(&self) -> &[SnapshotDocument] {
        &self.documents
    }

    /// Takes the strictly ascending document list.
    #[must_use]
    pub fn into_documents(self) -> Vec<SnapshotDocument> {
        self.documents
    }

    /// Validates and constructs an artifact with a deterministic digest.
    ///
    /// # Errors
    ///
    /// Returns a model error when ordering or size bounds are violated.
    pub fn new(
        scope: Scope,
        position: u64,
        documents: Vec<SnapshotDocument>,
    ) -> Result<Self, ModelError> {
        if documents.len() > MAX_BASELINE_DOCUMENTS {
            return Err(ModelError::SnapshotTooManyDocuments);
        }
        let document_count = u32::try_from(documents.len()).map_err(ModelError::SnapshotWidth)?;
        let admitted_bytes = documents.iter().try_fold(0usize, |total, document| {
            total
                .checked_add(document.snapshot.len())
                .ok_or(ModelError::SnapshotTooLarge)
        })?;
        if admitted_bytes > MAX_BASELINE_BYTES {
            return Err(ModelError::SnapshotTooLarge);
        }
        let mut builder = SnapshotDigestBuilder::new(scope, position, u64::from(document_count));
        for document in &documents {
            builder.push(document)?;
        }
        let (digest, byte_len) = builder.finish()?;
        let byte_len = u32::try_from(byte_len).map_err(ModelError::SnapshotWidth)?;
        Ok(Self {
            baseline: SnapshotBaseline::new(position, digest, document_count, byte_len),
            documents,
        })
    }

    /// Recomputes and verifies the descriptor against its documents.
    ///
    /// # Errors
    ///
    /// Returns a model error when the artifact is malformed or its descriptor mismatches.
    pub fn validate(&self, scope: Scope) -> Result<(), ModelError> {
        let expected = Self::new(scope, self.baseline.position, self.documents.clone())?;
        if expected.baseline != self.baseline {
            return Err(ModelError::SnapshotDescriptorMismatch);
        }
        Ok(())
    }
}

#[cfg(test)]
thread_local! {
    static HASHED_BODY_BYTES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Incremental canonical digest over one ordered snapshot document sequence.
///
/// [`SnapshotArtifact::new`] and a paged recovery client fold the same sequence
/// through this builder, so a streamed artifact reproduces the exact digest of
/// the same sequence assembled in memory. The builder owns ordering,
/// per-document bounds, the running byte total, and the declared document count;
/// a sequence that ends short, long, or out of order cannot finalize.
pub struct SnapshotDigestBuilder {
    /// Running canonical digest over the folded sequence.
    digest: Hasher,
    /// Document count the finished stream must match.
    expected_count: u64,
    /// Documents folded so far.
    count: u64,
    /// Aggregate body bytes folded so far.
    byte_len: usize,
    /// Identity of the previously folded document, for ordering.
    previous: Option<DocumentId>,
    /// Whether a framed field length did not fit its prefix, so `finish` refuses.
    oversized: bool,
}

impl std::fmt::Debug for SnapshotDigestBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SnapshotDigestBuilder")
            .field("expected_count", &self.expected_count)
            .field("count", &self.count)
            .field("byte_len", &self.byte_len)
            .field("previous", &self.previous)
            .finish_non_exhaustive()
    }
}
impl SnapshotDigestBuilder {
    /// Starts a builder for exactly `document_count` documents at `position`.
    #[must_use]
    pub fn new(scope: Scope, position: u64, document_count: u64) -> Self {
        let mut builder = Self {
            digest: Hasher::new(),
            expected_count: document_count,
            count: 0,
            byte_len: 0,
            previous: None,
            oversized: false,
        };
        builder.fold(b"logical-db-baseline-v2");
        builder.fold(scope.account().as_uuid().as_bytes());
        builder.fold(scope.space().as_uuid().as_bytes());
        builder.fold(&position.to_be_bytes());
        builder.fold(&document_count.to_be_bytes());
        builder
    }

    /// Folds one framed field, recording a length that does not fit its prefix.
    fn fold(&mut self, bytes: &[u8]) {
        if frame(&mut self.digest, bytes).is_err() {
            self.oversized = true;
        }
    }

    /// Folds the next document into the running digest.
    ///
    /// # Errors
    ///
    /// Returns a typed model error when the document is empty, oversized, or not
    /// strictly ascending after the previously folded document.
    pub fn push(&mut self, document: &SnapshotDocument) -> Result<(), ModelError> {
        if self
            .previous
            .is_some_and(|previous| previous >= document.document)
        {
            return Err(ModelError::SnapshotDocumentsUnordered);
        }
        if document.snapshot.is_empty() || document.snapshot.len() > MAX_BASELINE_BYTES {
            return Err(ModelError::SnapshotDocumentInvalid);
        }
        self.byte_len = self
            .byte_len
            .checked_add(document.snapshot.len())
            .ok_or(ModelError::SnapshotTooLarge)?;
        self.count = self.count.saturating_add(1);
        self.previous = Some(document.document);
        self.fold(document.document.as_uuid().as_bytes());
        self.fold(&[u8::from(document.tombstone)]);
        #[cfg(test)]
        HASHED_BODY_BYTES
            .with(|bytes| bytes.set(bytes.get().saturating_add(document.snapshot.len())));
        self.fold(&document.snapshot);
        Ok(())
    }

    /// Consumes the builder and returns the descriptor digest and aggregate bytes.
    ///
    /// # Errors
    ///
    /// Returns a typed model error when the folded count differs from the declared count.
    pub fn finish(self) -> Result<(BaselineDigest, usize), ModelError> {
        if self.oversized {
            return Err(ModelError::SnapshotTooLarge);
        }
        if self.count != self.expected_count {
            return Err(ModelError::SnapshotDescriptorMismatch);
        }
        Ok((
            BaselineDigest(*self.digest.finalize().as_bytes()),
            self.byte_len,
        ))
    }
}

/// Frames a length prefix and body into the running canonical digest.
///
/// # Errors
///
/// Returns [`ModelError::SnapshotTooLarge`] when the length does not fit its
/// 64-bit prefix, so no truncated prefix is ever hashed.
fn frame(digest: &mut Hasher, bytes: &[u8]) -> Result<(), ModelError> {
    let Ok(length) = u64::try_from(bytes.len()) else {
        return Err(ModelError::SnapshotTooLarge);
    };
    digest.update(&length.to_be_bytes());
    digest.update(bytes);
    Ok(())
}

/// Splits the next `N` bytes off the front of `rest`, advancing it.
fn take_array<const N: usize>(rest: &mut &[u8]) -> Result<[u8; N], ModelError> {
    let Some((head, tail)) = rest.split_first_chunk::<N>() else {
        return Err(ModelError::InvalidSnapshotEncoding);
    };
    *rest = tail;
    Ok(*head)
}

/// Reports whether an encoded count fits `maximum`; an unrepresentable count does not.
fn within(value: u32, maximum: usize) -> bool {
    let Ok(count) = usize::try_from(value) else {
        return false;
    };
    count <= maximum
}

#[cfg(test)]
mod tests;
