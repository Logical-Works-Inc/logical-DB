//! Versioned, bounded state-bearing snapshot baseline records.

//! Content-addressed state-bearing baseline artifacts for safe log retention.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use crate::{DocumentId, ModelError, Scope};
use sha2::{Digest, Sha256};

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
    version: u16,
    position: u64,
    digest: BaselineDigest,
    document_count: u32,
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
                let error = ModelError::UnsupportedSnapshotVersion { actual: version };
                eprintln!("logical-db-model: SnapshotBaseline::decode legacy version: {error}");
                return Err(error);
            }
            let mut position = [0; 8];
            position.copy_from_slice(&bytes[2..10]);
            return Ok(SnapshotRecord::LegacyPosition(u64::from_be_bytes(position)));
        }
        if bytes.len() != SNAPSHOT_BASELINE_BYTES {
            let error = ModelError::InvalidSnapshotEncoding;
            eprintln!("logical-db-model: SnapshotBaseline::decode length: {error}");
            return Err(error);
        }
        let version = u16::from_be_bytes([bytes[0], bytes[1]]);
        if version != SNAPSHOT_BASELINE_VERSION {
            let error = ModelError::UnsupportedSnapshotVersion { actual: version };
            eprintln!("logical-db-model: SnapshotBaseline::decode version: {error}");
            return Err(error);
        }
        let baseline = Self {
            version,
            position: decode_u64(&bytes[2..10]),
            digest: BaselineDigest(decode_array(&bytes[10..42])),
            document_count: decode_u32(&bytes[42..46]),
            byte_len: decode_u32(&bytes[46..50]),
        };
        if baseline.document_count as usize > MAX_BASELINE_DOCUMENTS {
            let error = ModelError::SnapshotTooManyDocuments;
            eprintln!("logical-db-model: SnapshotBaseline::decode document count: {error}");
            return Err(error);
        }
        if baseline.byte_len as usize > MAX_BASELINE_BYTES {
            let error = ModelError::SnapshotTooLarge;
            eprintln!("logical-db-model: SnapshotBaseline::decode byte length: {error}");
            return Err(error);
        }
        Ok(SnapshotRecord::State(baseline))
    }
}

/// Result of decoding a versioned snapshot descriptor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SnapshotRecord {
    /// Legacy position-only descriptor; it cannot satisfy state recovery.
    LegacyPosition(u64),
    /// State-bearing v2 descriptor.
    State(SnapshotBaseline),
}

/// One bounded document state included in a snapshot artifact.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotDocument {
    /// Stable document identity.
    pub document: DocumentId,
    /// Canonical serialized document state.
    pub snapshot: Vec<u8>,
    /// Whether the document is a tombstone.
    pub tombstone: bool,
}

/// A descriptor and its ordered, bounded document states.
#[derive(Clone, Debug, Eq, PartialEq)]
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
            let error = ModelError::SnapshotTooManyDocuments;
            eprintln!("logical-db-model: SnapshotArtifact::new count: {error}");
            return Err(error);
        }
        let document_count = u32::try_from(documents.len()).map_err(|error| {
            eprintln!("logical-db-model: SnapshotArtifact::new document count: {error}");
            ModelError::SnapshotTooLarge
        })?;
        let admitted_bytes = documents.iter().try_fold(0usize, |total, document| {
            total
                .checked_add(document.snapshot.len())
                .ok_or(ModelError::SnapshotTooLarge)
        })?;
        if admitted_bytes > MAX_BASELINE_BYTES {
            let error = ModelError::SnapshotTooLarge;
            eprintln!("logical-db-model: SnapshotArtifact::new aggregate admission: {error}");
            return Err(error);
        }
        let mut builder = SnapshotDigestBuilder::new(scope, position, u64::from(document_count));
        for document in &documents {
            builder.push(document)?;
        }
        let (digest, byte_len) = builder.finish()?;
        let byte_len = u32::try_from(byte_len).map_err(|error| {
            eprintln!("logical-db-model: SnapshotArtifact::new byte length: {error}");
            ModelError::SnapshotTooLarge
        })?;
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
            let error = ModelError::SnapshotDescriptorMismatch;
            eprintln!("logical-db-model: SnapshotArtifact::validate descriptor: {error}");
            return Err(error);
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
#[derive(Debug)]
pub struct SnapshotDigestBuilder {
    digest: Sha256,
    expected_count: u64,
    count: u64,
    byte_len: usize,
    previous: Option<DocumentId>,
}

impl SnapshotDigestBuilder {
    /// Starts a builder for exactly `document_count` documents at `position`.
    #[must_use]
    pub fn new(scope: Scope, position: u64, document_count: u64) -> Self {
        let mut digest = Sha256::new();
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
            let error = ModelError::SnapshotDocumentsUnordered;
            eprintln!("logical-db-model: SnapshotDigestBuilder ordering: {error}");
            return Err(error);
        }
        if document.snapshot.is_empty() || document.snapshot.len() > MAX_BASELINE_BYTES {
            let error = ModelError::SnapshotDocumentInvalid;
            eprintln!("logical-db-model: SnapshotDigestBuilder payload: {error}");
            return Err(error);
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
        HASHED_BODY_BYTES.with(|bytes| bytes.set(bytes.get() + document.snapshot.len()));
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
            let error = ModelError::SnapshotDescriptorMismatch;
            eprintln!("logical-db-model: SnapshotDigestBuilder count: {error}");
            return Err(error);
        }
        Ok((BaselineDigest(self.digest.finalize().into()), self.byte_len))
    }
}

fn frame(digest: &mut Sha256, bytes: &[u8]) {
    digest.update((bytes.len() as u64).to_be_bytes());
    digest.update(bytes);
}

fn decode_u64(bytes: &[u8]) -> u64 {
    let mut value = [0; 8];
    value.copy_from_slice(bytes);
    u64::from_be_bytes(value)
}

fn decode_u32(bytes: &[u8]) -> u32 {
    let mut value = [0; 4];
    value.copy_from_slice(bytes);
    u32::from_be_bytes(value)
}

fn decode_array(bytes: &[u8]) -> [u8; 32] {
    let mut value = [0; 32];
    value.copy_from_slice(bytes);
    value
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AccountId, SpaceId};

    fn scope() -> Scope {
        Scope::new(AccountId::new(), SpaceId::new())
    }
    fn doc() -> SnapshotDocument {
        SnapshotDocument {
            document: DocumentId::new(),
            snapshot: vec![1, 2, 3],
            tombstone: false,
        }
    }
    fn fixed_scope() -> Scope {
        Scope::new(
            AccountId::from_uuid(uuid::Uuid::from_bytes([0x11; 16])),
            SpaceId::from_uuid(uuid::Uuid::from_bytes([0x22; 16])),
        )
    }
    fn fixed_doc(byte: u8, snapshot: Vec<u8>, tombstone: bool) -> SnapshotDocument {
        SnapshotDocument {
            document: DocumentId::from_uuid(uuid::Uuid::from_bytes([byte; 16])),
            snapshot,
            tombstone,
        }
    }
    fn streamed_digest(
        scope: Scope,
        position: u64,
        documents: &[SnapshotDocument],
    ) -> BaselineDigest {
        let mut builder = SnapshotDigestBuilder::new(scope, position, documents.len() as u64);
        for document in documents {
            builder.push(document).unwrap();
        }
        builder.finish().unwrap().0
    }

    #[test]
    fn artifact_digest_is_reproducible_and_descriptor_validates() {
        let scope = scope();
        let first = SnapshotArtifact::new(scope, 7, vec![doc()]).unwrap();
        let second = SnapshotArtifact::new(scope, 7, first.documents.clone()).unwrap();
        assert_eq!(first.baseline.digest(), second.baseline.digest());
        assert!(first.validate(scope).is_ok());
        let mut altered = first.clone();
        altered.baseline = SnapshotBaseline::new(7, BaselineDigest::new([0; 32]), 1, 3);
        assert_eq!(
            altered.validate(scope),
            Err(ModelError::SnapshotDescriptorMismatch)
        );
    }

    #[test]
    fn artifact_digest_has_a_portable_golden_encoding() {
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
        )
        .unwrap();
        assert_eq!(
            artifact.baseline.digest().into_bytes(),
            [
                56, 231, 44, 53, 72, 56, 165, 65, 220, 47, 175, 20, 242, 220, 94, 89, 108, 99, 248,
                118, 214, 229, 194, 48, 144, 232, 24, 34, 204, 174, 36, 131,
            ]
        );
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
    fn streamed_documents_reproduce_the_in_memory_digest() {
        let scope = scope();
        let documents = (0..200_u128)
            .map(|index| SnapshotDocument {
                document: DocumentId::from_uuid(uuid::Uuid::from_u128(index)),
                snapshot: vec![1, 2, 3],
                tombstone: index % 2 == 0,
            })
            .collect::<Vec<_>>();
        let artifact = SnapshotArtifact::new(scope, 9, documents.clone()).unwrap();
        let mut builder = SnapshotDigestBuilder::new(scope, 9, documents.len() as u64);
        for document in &documents {
            builder.push(document).unwrap();
        }
        let (digest, byte_len) = builder.finish().unwrap();
        assert_eq!(digest, artifact.baseline.digest());
        assert_eq!(byte_len, 200 * 3);
    }

    #[test]
    fn streamed_digest_folds_past_the_single_artifact_cap() {
        let scope = scope();
        let declared = u64::try_from(MAX_BASELINE_DOCUMENTS + 1).unwrap();
        let mut builder = SnapshotDigestBuilder::new(scope, 0, declared);
        for index in 0..=MAX_BASELINE_DOCUMENTS {
            builder
                .push(&SnapshotDocument {
                    document: DocumentId::from_uuid(uuid::Uuid::from_u128(index as u128)),
                    snapshot: vec![7],
                    tombstone: false,
                })
                .unwrap();
        }
        let (_, byte_len) = builder.finish().unwrap();
        assert_eq!(byte_len, MAX_BASELINE_DOCUMENTS + 1);
    }

    // The pre-existing `artifact_digest_has_a_portable_golden_encoding` case is
    // the independent one-shot framing oracle. The vectors below were captured
    // from the pre-change `SnapshotArtifact::new` implementation and frozen
    // verbatim, so a shared framing bug cannot hide behind a test that compares
    // two paths both built from `SnapshotDigestBuilder`.

    #[test]
    fn empty_stream_digest_has_a_portable_golden_encoding() {
        let expected = [
            130, 230, 239, 157, 28, 146, 21, 237, 58, 130, 114, 37, 199, 141, 221, 3, 67, 32, 213,
            139, 84, 177, 203, 137, 131, 231, 122, 95, 237, 122, 239, 110,
        ];
        let artifact = SnapshotArtifact::new(fixed_scope(), 7, Vec::new()).unwrap();
        assert_eq!(artifact.baseline.digest().into_bytes(), expected);
        assert_eq!(artifact.baseline.byte_len(), 0);
        assert_eq!(
            streamed_digest(fixed_scope(), 7, &[]).into_bytes(),
            expected
        );
    }

    #[test]
    fn tombstone_document_digest_has_a_portable_golden_encoding() {
        let expected = [
            5, 66, 153, 159, 247, 113, 46, 90, 156, 32, 147, 29, 186, 33, 186, 16, 110, 102, 164,
            94, 143, 39, 240, 138, 131, 152, 248, 246, 240, 30, 33, 239,
        ];
        let documents = vec![fixed_doc(0x33, vec![1, 2, 3], true)];
        let artifact = SnapshotArtifact::new(fixed_scope(), 7, documents.clone()).unwrap();
        assert_eq!(artifact.baseline.digest().into_bytes(), expected);
        assert_eq!(artifact.baseline.byte_len(), 3);
        assert_eq!(
            streamed_digest(fixed_scope(), 7, &documents).into_bytes(),
            expected
        );
    }

    #[test]
    fn multi_document_stream_digest_has_a_portable_golden_encoding() {
        let expected = [
            54, 246, 36, 252, 204, 121, 238, 36, 81, 30, 162, 254, 219, 217, 50, 184, 253, 205,
            145, 59, 5, 110, 244, 119, 114, 56, 3, 163, 157, 236, 92, 220,
        ];
        let documents = vec![
            fixed_doc(0x33, vec![1, 2, 3], false),
            fixed_doc(0x44, vec![4, 5, 6], true),
            fixed_doc(0x55, vec![7, 8, 9], false),
        ];
        let artifact = SnapshotArtifact::new(fixed_scope(), 7, documents.clone()).unwrap();
        assert_eq!(artifact.baseline.digest().into_bytes(), expected);
        assert_eq!(artifact.baseline.byte_len(), 9);
        assert_eq!(
            streamed_digest(fixed_scope(), 7, &documents).into_bytes(),
            expected
        );
    }

    // Falsifiers: each hostile shape must either change the digest or be refused.

    #[test]
    fn streamed_digest_refuses_overlong_streams() {
        let scope = scope();
        let first = fixed_doc(0x33, vec![1], false);
        let second = fixed_doc(0x44, vec![2], false);
        let mut builder = SnapshotDigestBuilder::new(scope, 0, 1);
        builder.push(&first).unwrap();
        builder.push(&second).unwrap();
        assert_eq!(
            builder.finish(),
            Err(ModelError::SnapshotDescriptorMismatch)
        );
    }

    #[test]
    fn streamed_digest_refuses_zero_count_with_a_document() {
        let scope = scope();
        let mut builder = SnapshotDigestBuilder::new(scope, 0, 0);
        builder.push(&fixed_doc(0x33, vec![1], false)).unwrap();
        assert_eq!(
            builder.finish(),
            Err(ModelError::SnapshotDescriptorMismatch)
        );
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
            builder.push(&fixed_doc(0x44, vec![0; MAX_BASELINE_BYTES + 1], false)),
            Err(ModelError::SnapshotDocumentInvalid)
        );
        assert_eq!(
            builder.finish(),
            Err(ModelError::SnapshotDescriptorMismatch)
        );
    }

    #[test]
    fn streamed_digest_refuses_reverse_ordering_across_a_chunk_boundary() {
        let scope = scope();
        let higher = fixed_doc(0x55, vec![1], false);
        let lower = fixed_doc(0x44, vec![2], false);
        let mut builder = SnapshotDigestBuilder::new(scope, 0, 2);
        // The first chunk ends on the higher identity; the next chunk regresses.
        builder.push(&higher).unwrap();
        assert_eq!(
            builder.push(&lower),
            Err(ModelError::SnapshotDocumentsUnordered)
        );
    }

    #[test]
    fn streamed_digest_is_scope_position_and_tombstone_sensitive() {
        let documents = [fixed_doc(0x33, vec![1, 2, 3], false)];
        let base = streamed_digest(fixed_scope(), 7, &documents);
        let other_scope = Scope::new(
            AccountId::from_uuid(uuid::Uuid::from_bytes([0x12; 16])),
            SpaceId::from_uuid(uuid::Uuid::from_bytes([0x22; 16])),
        );
        assert_ne!(base, streamed_digest(other_scope, 7, &documents));
        assert_ne!(base, streamed_digest(fixed_scope(), 8, &documents));
        let tombstones = [fixed_doc(0x33, vec![1, 2, 3], true)];
        assert_ne!(base, streamed_digest(fixed_scope(), 7, &tombstones));
    }

    #[test]
    fn rejected_push_leaves_the_builder_usable_and_unchanged() {
        let scope = scope();
        let first = fixed_doc(0x33, vec![1], false);
        let second = fixed_doc(0x44, vec![2], false);
        let mut builder = SnapshotDigestBuilder::new(scope, 0, 2);
        builder.push(&first).unwrap();
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
        builder.push(&second).unwrap();
        let mut fresh = SnapshotDigestBuilder::new(scope, 0, 2);
        fresh.push(&first).unwrap();
        fresh.push(&second).unwrap();
        assert_eq!(builder.finish().unwrap(), fresh.finish().unwrap());
    }

    #[test]
    fn caller_admits_aggregate_bytes_before_hashing_a_large_body() {
        let scope = scope();
        let start = HASHED_BODY_BYTES.with(std::cell::Cell::get);
        SnapshotArtifact::new(scope, 0, vec![fixed_doc(0x33, vec![1, 2, 3], false)]).unwrap();
        let after_small = HASHED_BODY_BYTES.with(std::cell::Cell::get);
        assert_eq!(after_small, start + 3);
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
    }

    #[test]
    fn streamed_digest_refuses_short_streams_and_disorder() {
        let scope = scope();
        let first = doc();
        let mut short = SnapshotDigestBuilder::new(scope, 0, 2);
        short.push(&first).unwrap();
        assert_eq!(short.finish(), Err(ModelError::SnapshotDescriptorMismatch));
        let mut disordered = SnapshotDigestBuilder::new(scope, 0, 2);
        disordered.push(&first).unwrap();
        assert_eq!(
            disordered.push(&first),
            Err(ModelError::SnapshotDocumentsUnordered)
        );
    }

    #[test]
    fn artifact_accepts_a_baseline_larger_than_one_page() {
        let scope = scope();
        let documents = (0..=MAX_BASELINE_PAGE_DOCUMENTS)
            .map(|index| SnapshotDocument {
                document: DocumentId::from_uuid(uuid::Uuid::from_u128(index as u128)),
                snapshot: vec![7],
                tombstone: false,
            })
            .collect::<Vec<_>>();
        let artifact = SnapshotArtifact::new(scope, 3, documents).unwrap();
        assert_eq!(
            artifact.baseline.document_count() as usize,
            MAX_BASELINE_PAGE_DOCUMENTS + 1
        );
    }
}
