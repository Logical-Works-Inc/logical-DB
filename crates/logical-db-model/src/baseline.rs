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
/// Maximum number of documents in one snapshot artifact.
pub const MAX_BASELINE_DOCUMENTS: usize = 256;
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
        validate_documents(&documents)?;
        let byte_len = documents.iter().try_fold(0usize, |total, doc| {
            total
                .checked_add(doc.snapshot.len())
                .ok_or(ModelError::SnapshotTooLarge)
        })?;
        let digest = BaselineDigest(compute_digest(scope, position, &documents));
        let document_count = u32::try_from(documents.len()).map_err(|error| {
            eprintln!("logical-db-model: SnapshotArtifact::new document count: {error}");
            ModelError::SnapshotTooLarge
        })?;
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
        validate_documents(&self.documents)?;
        let expected = Self::new(scope, self.baseline.position, self.documents.clone())?;
        if expected.baseline != self.baseline {
            let error = ModelError::SnapshotDescriptorMismatch;
            eprintln!("logical-db-model: SnapshotArtifact::validate descriptor: {error}");
            return Err(error);
        }
        Ok(())
    }
}

fn validate_documents(documents: &[SnapshotDocument]) -> Result<(), ModelError> {
    if documents.len() > MAX_BASELINE_DOCUMENTS {
        let error = ModelError::SnapshotTooManyDocuments;
        eprintln!("logical-db-model: validate_documents count: {error}");
        return Err(error);
    }
    let mut total = 0usize;
    let mut previous = None;
    for document in documents {
        if previous.is_some_and(|id| id >= document.document) {
            let error = ModelError::SnapshotDocumentsUnordered;
            eprintln!("logical-db-model: validate_documents ordering: {error}");
            return Err(error);
        }
        previous = Some(document.document);
        if document.snapshot.is_empty() || document.snapshot.len() > MAX_BASELINE_BYTES {
            let error = ModelError::SnapshotDocumentInvalid;
            eprintln!("logical-db-model: validate_documents payload: {error}");
            return Err(error);
        }
        total = total
            .checked_add(document.snapshot.len())
            .ok_or(ModelError::SnapshotTooLarge)?;
    }
    if total > MAX_BASELINE_BYTES {
        let error = ModelError::SnapshotTooLarge;
        eprintln!("logical-db-model: validate_documents aggregate bytes: {error}");
        return Err(error);
    }
    Ok(())
}

fn compute_digest(scope: Scope, position: u64, documents: &[SnapshotDocument]) -> [u8; 32] {
    let mut digest = Sha256::new();
    frame(&mut digest, b"logical-db-baseline-v2");
    frame(&mut digest, scope.account().as_uuid().as_bytes());
    frame(&mut digest, scope.space().as_uuid().as_bytes());
    frame(&mut digest, &position.to_be_bytes());
    frame(&mut digest, &(documents.len() as u64).to_be_bytes());
    for document in documents {
        frame(&mut digest, document.document.as_uuid().as_bytes());
        frame(&mut digest, &[u8::from(document.tombstone)]);
        frame(&mut digest, &document.snapshot);
    }
    digest.finalize().into()
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
}
