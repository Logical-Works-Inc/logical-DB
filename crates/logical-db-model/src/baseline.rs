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
        if bytes.len() == SNAPSHOT_BASELINE_LEGACY_BYTES {
            let version = u16::from_be_bytes([bytes[0], bytes[1]]);
            if version != SNAPSHOT_BASELINE_LEGACY_VERSION {
                return Err(ModelError::UnsupportedSnapshotVersion { actual: version });
            }
            let mut position = [0; 8];
            position.copy_from_slice(&bytes[2..10]);
            return Ok(SnapshotRecord::LegacyPosition(u64::from_be_bytes(position)));
        }
        if bytes.len() != SNAPSHOT_BASELINE_BYTES {
            return Err(ModelError::InvalidSnapshotEncoding);
        }
        let version = u16::from_be_bytes([bytes[0], bytes[1]]);
        if version != SNAPSHOT_BASELINE_VERSION {
            return Err(ModelError::UnsupportedSnapshotVersion { actual: version });
        }
        let baseline = Self {
            version,
            position: decode_u64(&bytes[2..10]),
            digest: BaselineDigest(decode_array(&bytes[10..42])),
            document_count: decode_u32(&bytes[42..46]),
            byte_len: decode_u32(&bytes[46..50]),
        };
        if usize::try_from(baseline.document_count).unwrap_or(usize::MAX) > MAX_BASELINE_DOCUMENTS {
            return Err(ModelError::SnapshotTooManyDocuments);
        }
        if usize::try_from(baseline.byte_len).unwrap_or(usize::MAX) > MAX_BASELINE_BYTES {
            return Err(ModelError::SnapshotTooLarge);
        }
        Ok(SnapshotRecord::State(baseline))
    }
}

/// Result of decoding a versioned snapshot descriptor.
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
    pub document: DocumentId,
    /// Canonical serialized document state.
    pub snapshot: Vec<u8>,
    /// Whether the document is a tombstone.
    pub tombstone: bool,
}

impl SnapshotDocument {
    /// Builds one document entry from its identity, canonical bytes, and flag.
    ///
    /// This is the constructor `#[non_exhaustive]` requires: callers outside
    /// this crate cannot use a struct expression, so they compose through here.
    #[must_use]
    pub const fn new(document: DocumentId, snapshot: Vec<u8>, tombstone: bool) -> Self {
        Self {
            document,
            snapshot,
            tombstone,
        }
    }
}

/// A descriptor and its ordered, bounded document states.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct SnapshotArtifact {
    /// Descriptor computed from the artifact documents.
    pub baseline: SnapshotBaseline,
    /// Strictly ascending document list.
    pub documents: Vec<SnapshotDocument>,
}

impl SnapshotArtifact {
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
        let document_count =
            u32::try_from(documents.len()).map_err(|_error| ModelError::SnapshotTooLarge)?;
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
        let byte_len = u32::try_from(byte_len).map_err(|_error| ModelError::SnapshotTooLarge)?;
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
        let mut digest = Hasher::new();
        frame(&mut digest, b"logical-db-baseline-v2");
        frame(&mut digest, scope.account().as_uuid().as_bytes());
        frame(&mut digest, scope.space().as_uuid().as_bytes());
        frame(&mut digest, &position.to_be_bytes());
        frame(&mut digest, &document_count.to_be_bytes());
        Self {
            digest,
            expected_count: document_count,
            count: 0,
            byte_len: 0,
            previous: None,
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
        frame(&mut self.digest, document.document.as_uuid().as_bytes());
        frame(&mut self.digest, &[u8::from(document.tombstone)]);
        #[cfg(test)]
        HASHED_BODY_BYTES
            .with(|bytes| bytes.set(bytes.get().saturating_add(document.snapshot.len())));
        frame(&mut self.digest, &document.snapshot);
        Ok(())
    }

    /// Consumes the builder and returns the descriptor digest and aggregate bytes.
    ///
    /// # Errors
    ///
    /// Returns a typed model error when the folded count differs from the declared count.
    pub fn finish(self) -> Result<(BaselineDigest, usize), ModelError> {
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
fn frame(digest: &mut Hasher, bytes: &[u8]) {
    digest.update(&u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_be_bytes());
    digest.update(bytes);
}

/// Decodes a big-endian `u64` from exactly eight bytes.
fn decode_u64(bytes: &[u8]) -> u64 {
    let mut value = [0; 8];
    value.copy_from_slice(bytes);
    u64::from_be_bytes(value)
}

/// Decodes a big-endian `u32` from exactly four bytes.
fn decode_u32(bytes: &[u8]) -> u32 {
    let mut value = [0; 4];
    value.copy_from_slice(bytes);
    u32::from_be_bytes(value)
}

/// Copies exactly thirty-two digest bytes into a fixed array.
fn decode_array(bytes: &[u8]) -> [u8; 32] {
    let mut value = [0; 32];
    value.copy_from_slice(bytes);
    value
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AccountId, SpaceId};

    /// A fresh random-identifier scope for tests that only need one.
    fn scope() -> Scope {
        Scope::new(AccountId::new(), SpaceId::new())
    }
    /// A small three-byte non-tombstone document for generic cases.
    fn doc() -> SnapshotDocument {
        SnapshotDocument {
            document: DocumentId::new(),
            snapshot: vec![1, 2, 3],
            tombstone: false,
        }
    }
    /// A scope with fixed bytes, so digests are reproducible across runs.
    fn fixed_scope() -> Scope {
        Scope::new(
            AccountId::from_uuid(uuid::Uuid::from_bytes([0x11; 16])),
            SpaceId::from_uuid(uuid::Uuid::from_bytes([0x22; 16])),
        )
    }
    /// A document with a fixed identity so its digest is stable across runs.
    fn fixed_doc(byte: u8, snapshot: Vec<u8>, tombstone: bool) -> SnapshotDocument {
        SnapshotDocument {
            document: DocumentId::from_uuid(uuid::Uuid::from_bytes([byte; 16])),
            snapshot,
            tombstone,
        }
    }
    /// Folds a known-good sequence through the builder and returns its digest.
    ///
    /// Every rejection is propagated with `?`, so a test that feeds a valid
    /// sequence fails loudly if the builder ever refuses one.
    fn streamed_digest(
        scope: Scope,
        position: u64,
        documents: &[SnapshotDocument],
    ) -> Result<BaselineDigest, Box<dyn std::error::Error>> {
        let count = u64::try_from(documents.len()).unwrap_or(u64::MAX);
        let mut builder = SnapshotDigestBuilder::new(scope, position, count);
        for document in documents {
            builder.push(document)?;
        }
        Ok(builder.finish()?.0)
    }

    #[test]
    fn artifact_digest_is_reproducible_and_descriptor_validates()
    -> Result<(), Box<dyn std::error::Error>> {
        let scope = scope();
        let first = SnapshotArtifact::new(scope, 7, vec![doc()])?;
        let second = SnapshotArtifact::new(scope, 7, first.documents.clone())?;
        assert_eq!(first.baseline.digest(), second.baseline.digest());
        assert!(first.validate(scope).is_ok());
        let mut altered = first;
        altered.baseline = SnapshotBaseline::new(7, BaselineDigest::new([0; 32]), 1, 3);
        assert_eq!(
            altered.validate(scope),
            Err(ModelError::SnapshotDescriptorMismatch)
        );
        Ok(())
    }

    #[test]
    fn artifact_digest_has_a_portable_golden_encoding() -> Result<(), Box<dyn std::error::Error>> {
        let scope = Scope::new(
            AccountId::from_uuid(uuid::Uuid::from_bytes([0x11; 16])),
            SpaceId::from_uuid(uuid::Uuid::from_bytes([0x22; 16])),
        );
        let artifact = SnapshotArtifact::new(
            scope,
            7,
            vec![SnapshotDocument {
                document: DocumentId::from_uuid(uuid::Uuid::from_bytes([0x33; 16])),
                snapshot: vec![1, 2, 3],
                tombstone: false,
            }],
        )?;
        assert_eq!(
            artifact.baseline.digest().into_bytes(),
            // Re-pinned at the 2026-09-16 estate hash swap to BLAKE3.
            [
                41, 100, 14, 43, 176, 108, 83, 249, 216, 116, 41, 169, 65, 93, 202, 115, 211, 87,
                68, 48, 174, 151, 216, 133, 232, 209, 78, 155, 134, 56, 147, 69,
            ]
        );
        Ok(())
    }

    #[test]
    fn artifact_rejects_order_duplicates_and_bounds() {
        let first = doc();
        let second = SnapshotDocument {
            document: first.document,
            ..first.clone()
        };
        assert_eq!(
            SnapshotArtifact::new(scope(), 0, vec![first, second]),
            Err(ModelError::SnapshotDocumentsUnordered)
        );
        let mut too_many = Vec::new();
        for _ in 0..=MAX_BASELINE_DOCUMENTS {
            too_many.push(doc());
        }
        assert_eq!(
            SnapshotArtifact::new(scope(), 0, too_many),
            Err(ModelError::SnapshotTooManyDocuments)
        );
        assert_eq!(
            SnapshotArtifact::new(
                scope(),
                0,
                vec![SnapshotDocument {
                    document: DocumentId::new(),
                    snapshot: Vec::new(),
                    tombstone: true,
                }],
            ),
            Err(ModelError::SnapshotDocumentInvalid)
        );
    }

    #[test]
    fn streamed_documents_reproduce_the_in_memory_digest() -> Result<(), Box<dyn std::error::Error>>
    {
        let scope = scope();
        let documents = (0..200_u128)
            .map(|index| SnapshotDocument {
                document: DocumentId::from_uuid(uuid::Uuid::from_u128(index)),
                snapshot: vec![1, 2, 3],
                tombstone: index % 2 == 0,
            })
            .collect::<Vec<_>>();
        let artifact = SnapshotArtifact::new(scope, 9, documents.clone())?;
        let count = u64::try_from(documents.len()).unwrap_or(u64::MAX);
        let mut builder = SnapshotDigestBuilder::new(scope, 9, count);
        for document in &documents {
            builder.push(document)?;
        }
        let (digest, byte_len) = builder.finish()?;
        assert_eq!(digest, artifact.baseline.digest());
        assert_eq!(byte_len, 600);
        Ok(())
    }

    #[test]
    fn streamed_digest_folds_past_the_single_artifact_cap() -> Result<(), Box<dyn std::error::Error>>
    {
        let scope = scope();
        let declared = u64::try_from(MAX_BASELINE_DOCUMENTS)?;
        let mut builder = SnapshotDigestBuilder::new(scope, 0, declared.saturating_add(1));
        for index in 0..=MAX_BASELINE_DOCUMENTS {
            builder.push(&SnapshotDocument {
                document: DocumentId::from_uuid(uuid::Uuid::from_u128(u128::try_from(index)?)),
                snapshot: vec![7],
                tombstone: false,
            })?;
        }
        let (_, byte_len) = builder.finish()?;
        assert_eq!(byte_len, MAX_BASELINE_DOCUMENTS.saturating_add(1));
        Ok(())
    }

    // The pre-existing `artifact_digest_has_a_portable_golden_encoding` case is
    // the independent one-shot framing oracle. The vectors below were captured
    // from the pre-change `SnapshotArtifact::new` implementation and frozen
    // verbatim, so a shared framing bug cannot hide behind a test that compares
    // two paths both built from `SnapshotDigestBuilder`.

    #[test]
    fn empty_stream_digest_has_a_portable_golden_encoding() -> Result<(), Box<dyn std::error::Error>>
    {
        let expected = [
            71, 43, 172, 140, 216, 191, 236, 239, 92, 7, 42, 181, 192, 141, 96, 131, 199, 34, 103,
            0, 170, 35, 247, 111, 78, 137, 207, 157, 208, 248, 251, 238,
        ];
        let artifact = SnapshotArtifact::new(fixed_scope(), 7, Vec::new())?;
        assert_eq!(artifact.baseline.digest().into_bytes(), expected);
        assert_eq!(artifact.baseline.byte_len(), 0);
        assert_eq!(
            streamed_digest(fixed_scope(), 7, &[])?.into_bytes(),
            expected
        );
        Ok(())
    }

    #[test]
    fn tombstone_document_digest_has_a_portable_golden_encoding()
    -> Result<(), Box<dyn std::error::Error>> {
        let expected = [
            159, 212, 63, 55, 91, 223, 140, 196, 39, 31, 57, 185, 169, 62, 119, 131, 189, 52, 85,
            205, 204, 65, 198, 102, 214, 37, 114, 65, 221, 104, 136, 22,
        ];
        let documents = vec![fixed_doc(0x33, vec![1, 2, 3], true)];
        let artifact = SnapshotArtifact::new(fixed_scope(), 7, documents.clone())?;
        assert_eq!(artifact.baseline.digest().into_bytes(), expected);
        assert_eq!(artifact.baseline.byte_len(), 3);
        assert_eq!(
            streamed_digest(fixed_scope(), 7, &documents)?.into_bytes(),
            expected
        );
        Ok(())
    }

    #[test]
    fn multi_document_stream_digest_has_a_portable_golden_encoding()
    -> Result<(), Box<dyn std::error::Error>> {
        let expected = [
            16, 249, 242, 54, 53, 147, 16, 252, 104, 176, 157, 59, 236, 236, 174, 188, 61, 59, 175,
            174, 237, 67, 11, 175, 237, 255, 98, 131, 185, 20, 251, 201,
        ];
        let documents = vec![
            fixed_doc(0x33, vec![1, 2, 3], false),
            fixed_doc(0x44, vec![4, 5, 6], true),
            fixed_doc(0x55, vec![7, 8, 9], false),
        ];
        let artifact = SnapshotArtifact::new(fixed_scope(), 7, documents.clone())?;
        assert_eq!(artifact.baseline.digest().into_bytes(), expected);
        assert_eq!(artifact.baseline.byte_len(), 9);
        assert_eq!(
            streamed_digest(fixed_scope(), 7, &documents)?.into_bytes(),
            expected
        );
        Ok(())
    }

    // Falsifiers: each hostile shape must either change the digest or be refused.

    #[test]
    fn streamed_digest_refuses_overlong_streams() -> Result<(), Box<dyn std::error::Error>> {
        let scope = scope();
        let first = fixed_doc(0x33, vec![1], false);
        let second = fixed_doc(0x44, vec![2], false);
        let mut builder = SnapshotDigestBuilder::new(scope, 0, 1);
        builder.push(&first)?;
        builder.push(&second)?;
        assert_eq!(
            builder.finish(),
            Err(ModelError::SnapshotDescriptorMismatch)
        );
        Ok(())
    }

    #[test]
    fn streamed_digest_refuses_zero_count_with_a_document() -> Result<(), Box<dyn std::error::Error>>
    {
        let scope = scope();
        let mut builder = SnapshotDigestBuilder::new(scope, 0, 0);
        builder.push(&fixed_doc(0x33, vec![1], false))?;
        assert_eq!(
            builder.finish(),
            Err(ModelError::SnapshotDescriptorMismatch)
        );
        Ok(())
    }

    #[test]
    fn streamed_digest_refuses_empty_and_oversized_payloads() {
        let scope = scope();
        let mut builder = SnapshotDigestBuilder::new(scope, 0, 2);
        assert_eq!(
            builder.push(&fixed_doc(0x33, Vec::new(), false)),
            Err(ModelError::SnapshotDocumentInvalid)
        );
        assert_eq!(
            builder.push(&fixed_doc(
                0x44,
                vec![0; MAX_BASELINE_BYTES.saturating_add(1)],
                false
            )),
            Err(ModelError::SnapshotDocumentInvalid)
        );
        assert_eq!(
            builder.finish(),
            Err(ModelError::SnapshotDescriptorMismatch)
        );
    }

    #[test]
    fn streamed_digest_refuses_reverse_ordering_across_a_chunk_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let scope = scope();
        let higher = fixed_doc(0x55, vec![1], false);
        let lower = fixed_doc(0x44, vec![2], false);
        let mut builder = SnapshotDigestBuilder::new(scope, 0, 2);
        // The first chunk ends on the higher identity; the next chunk regresses.
        builder.push(&higher)?;
        assert_eq!(
            builder.push(&lower),
            Err(ModelError::SnapshotDocumentsUnordered)
        );
        Ok(())
    }

    #[test]
    fn streamed_digest_is_scope_position_and_tombstone_sensitive()
    -> Result<(), Box<dyn std::error::Error>> {
        let documents = [fixed_doc(0x33, vec![1, 2, 3], false)];
        let base = streamed_digest(fixed_scope(), 7, &documents)?;
        let other_scope = Scope::new(
            AccountId::from_uuid(uuid::Uuid::from_bytes([0x12; 16])),
            SpaceId::from_uuid(uuid::Uuid::from_bytes([0x22; 16])),
        );
        assert_ne!(base, streamed_digest(other_scope, 7, &documents)?);
        assert_ne!(base, streamed_digest(fixed_scope(), 8, &documents)?);
        let tombstones = [fixed_doc(0x33, vec![1, 2, 3], true)];
        assert_ne!(base, streamed_digest(fixed_scope(), 7, &tombstones)?);
        Ok(())
    }

    #[test]
    fn rejected_push_leaves_the_builder_usable_and_unchanged()
    -> Result<(), Box<dyn std::error::Error>> {
        let scope = scope();
        let first = fixed_doc(0x33, vec![1], false);
        let second = fixed_doc(0x44, vec![2], false);
        let mut builder = SnapshotDigestBuilder::new(scope, 0, 2);
        builder.push(&first)?;
        assert_eq!(
            builder.push(&first),
            Err(ModelError::SnapshotDocumentsUnordered)
        );
        assert_eq!(
            builder.push(&fixed_doc(0x44, Vec::new(), false)),
            Err(ModelError::SnapshotDocumentInvalid)
        );
        // A rejected push is not folded: the next valid push still observes
        // `first` as its predecessor and the builder finalizes the same stream
        // a fresh builder would.
        builder.push(&second)?;
        let mut fresh = SnapshotDigestBuilder::new(scope, 0, 2);
        fresh.push(&first)?;
        fresh.push(&second)?;
        assert_eq!(builder.finish()?, fresh.finish()?);
        Ok(())
    }

    #[test]
    fn caller_admits_aggregate_bytes_before_hashing_a_large_body()
    -> Result<(), Box<dyn std::error::Error>> {
        let scope = scope();
        let start = HASHED_BODY_BYTES.with(std::cell::Cell::get);
        SnapshotArtifact::new(scope, 0, vec![fixed_doc(0x33, vec![1, 2, 3], false)])?;
        let after_small = HASHED_BODY_BYTES.with(std::cell::Cell::get);
        assert_eq!(after_small, start.saturating_add(3));
        let documents = vec![
            fixed_doc(0x33, vec![0xAB; MAX_BASELINE_BYTES], false),
            fixed_doc(0x44, vec![0xCD; 1], false),
        ];
        assert_eq!(
            SnapshotArtifact::new(scope, 0, documents),
            Err(ModelError::SnapshotTooLarge)
        );
        assert_eq!(
            HASHED_BODY_BYTES.with(std::cell::Cell::get),
            after_small,
            "aggregate admission must run before any document body is hashed"
        );
        Ok(())
    }

    #[test]
    fn streamed_digest_refuses_short_streams_and_disorder() -> Result<(), Box<dyn std::error::Error>>
    {
        let scope = scope();
        let first = doc();
        let mut short = SnapshotDigestBuilder::new(scope, 0, 2);
        short.push(&first)?;
        assert_eq!(short.finish(), Err(ModelError::SnapshotDescriptorMismatch));
        let mut disordered = SnapshotDigestBuilder::new(scope, 0, 2);
        disordered.push(&first)?;
        assert_eq!(
            disordered.push(&first),
            Err(ModelError::SnapshotDocumentsUnordered)
        );
        Ok(())
    }

    #[test]
    fn artifact_accepts_a_baseline_larger_than_one_page() -> Result<(), Box<dyn std::error::Error>>
    {
        let scope = scope();
        let declared = u64::try_from(MAX_BASELINE_PAGE_DOCUMENTS)?;
        let documents = (0..=declared)
            .map(|index| {
                Ok(SnapshotDocument {
                    document: DocumentId::from_uuid(uuid::Uuid::from_u128(u128::from(index))),
                    snapshot: vec![7],
                    tombstone: false,
                })
            })
            .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
        let artifact = SnapshotArtifact::new(scope, 3, documents)?;
        assert_eq!(
            usize::try_from(artifact.baseline.document_count())?,
            MAX_BASELINE_PAGE_DOCUMENTS.saturating_add(1)
        );
        Ok(())
    }
}
