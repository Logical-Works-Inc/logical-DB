//! Tamper-evident projection snapshot envelopes (#119 row 4).
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.
//!
//! This absorbs the kernel snapshot contract (`tape-store::snapshot`, U1): a
//! deterministic, content-addressed envelope seals one projection state at
//! one frontier. Identity is the logical-DB [`Scope`] plus an [`IndexId`]
//! instead of the kernel tenant-route string, and every identity field is
//! fixed-width, so the NUL-injection ambiguity class the kernel defends
//! against cannot be constructed here. Encoding is length-prefixed raw bytes,
//! not CBOR: the envelope hash covers length-delimited fields, so the stored
//! bytes are self-describing without a serialization framework on this path.
//!
//! The backend trait, its redb implementation, and restore-or-replay live in
//! [`super::snapshot_backend`] and [`super::snapshot_restore`].

use logical_db_model::{AccountId, IndexId, MAX_SNAPSHOT_BYTES, Scope, SpaceId};
use thiserror::Error;

/// Current snapshot schema version. Older readers refuse newer envelopes.
pub(crate) const SNAPSHOT_SCHEMA_VERSION: u32 = 2;
/// Fixed envelope header: account, space, projection, version, semantics,
/// checkpoint, last row, last sequence, creation time, blob length.
const ENVELOPE_HEADER_BYTES: usize = 16 + 16 + 16 + 4 + 32 + 32 + 32 + 8 + 8 + 8;

/// Typed refusal from the envelope, backend, or restore path.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SnapshotEnvelopeError {
    /// A byte layout, length, or identity check failed.
    #[error("snapshot envelope malformed: {0}")]
    Malformed(String),
    /// Integrity verification failed. `at` names which boundary fired.
    #[error("snapshot tampered at {at}: {reason}")]
    Tampered {
        /// Which check fired.
        at: &'static str,
        /// What mismatched.
        reason: String,
    },
    /// The envelope schema version is not the current one.
    #[error("snapshot version mismatch: found {found}, expected {expected}")]
    VersionMismatch {
        /// The envelope's version.
        found: u32,
        /// The version this build reads.
        expected: u32,
    },
    /// The backend refused persistence.
    #[error("snapshot backend failed at {at}: {reason}")]
    BackendFailed {
        /// The call site.
        at: &'static str,
        /// What refused.
        reason: String,
    },
}

/// The whole frontier one envelope seals, as one value so a half-specified
/// envelope cannot exist.
///
/// Identity first, then the sealed payload, then the frontier the payload
/// covers: the state precedes the frontier fields it is sealed under.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub(crate) struct SnapshotSeal {
    /// Owning scope.
    pub scope: Scope,
    /// The projected index identity.
    pub projection: IndexId,
    /// Serialized projection state.
    pub(crate) state: Vec<u8>,
    /// Creation time in Unix seconds, caller-supplied.
    pub created_at_secs: u64,
    /// Opaque projection semantics fingerprint bound at seal.
    pub semantics: [u8; 32],
    /// Checkpoint of the sealed state.
    pub checkpoint: [u8; 32],
    /// Last row hash covered by the snapshot.
    pub last_row: [u8; 32],
    /// Sequence number of the last covered row (tail-replay locator).
    pub last_seq: u64,
}

/// Encodes the state length the hash and the storage form both cover, so the
/// overflow refusal exists exactly once.
fn state_blob_len(state: &[u8]) -> Result<u64, SnapshotEnvelopeError> {
    u64::try_from(state.len()).map_err(|error| {
        SnapshotEnvelopeError::Malformed(format!("snapshot state length overflow: {error}"))
    })
}

/// A sealed projection snapshot: the projection state at one frontier with
/// its own content address embedded.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct SnapshotEnvelope {
    /// Owning account the seal binds.
    pub account: AccountId,
    /// Owning application space the seal binds.
    pub space: SpaceId,
    /// The projected index identity.
    pub projection: IndexId,
    /// Envelope schema version, always [`SNAPSHOT_SCHEMA_VERSION`] at seal.
    pub schema_version: u32,
    /// Opaque projection semantics fingerprint the seal binds.
    pub semantics: [u8; 32],
    /// Checkpoint of the sealed state, re-derived on restore.
    pub checkpoint: [u8; 32],
    /// Last row hash the snapshot covers.
    pub last_row: [u8; 32],
    /// Sequence number of the last covered row, the tail-replay locator.
    pub last_seq: u64,
    /// Sealed projection state bytes.
    pub(crate) state: Vec<u8>,
    /// Creation time in Unix seconds, caller-supplied.
    pub created_at_secs: u64,
    /// BLAKE3 over every other field, computed at seal.
    pub hash: [u8; 32],
}

impl SnapshotEnvelope {
    /// Seals a whole frontier, embedding its content address.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotEnvelopeError::Malformed`] when a section length
    /// overflows its field, which cannot happen on a supported target.
    pub(crate) fn seal(seal: SnapshotSeal) -> Result<Self, SnapshotEnvelopeError> {
        let mut envelope = Self {
            account: seal.scope.account(),
            space: seal.scope.space(),
            projection: seal.projection,
            schema_version: SNAPSHOT_SCHEMA_VERSION,
            semantics: seal.semantics,
            checkpoint: seal.checkpoint,
            last_row: seal.last_row,
            last_seq: seal.last_seq,
            state: seal.state,
            created_at_secs: seal.created_at_secs,
            hash: [0_u8; 32],
        };
        envelope.hash = envelope.compute_hash()?;
        Ok(envelope)
    }

    /// Computes the deterministic content address over every field but `hash`.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotEnvelopeError::Malformed`] when a section length
    /// overflows its field.
    pub fn compute_hash(&self) -> Result<[u8; 32], SnapshotEnvelopeError> {
        let mut hasher = lgwks_std::hash::Hasher::new();
        hasher.update(self.account.as_uuid().as_bytes().as_slice());
        hasher.update(self.space.as_uuid().as_bytes().as_slice());
        hasher.update(self.projection.as_uuid().as_bytes().as_slice());
        hasher.update(&self.schema_version.to_le_bytes());
        hasher.update(&self.semantics);
        hasher.update(&self.checkpoint);
        hasher.update(&self.last_row);
        hasher.update(&self.last_seq.to_le_bytes());
        hasher.update(&self.created_at_secs.to_le_bytes());
        let blob_len = state_blob_len(&self.state)?;
        hasher.update(&blob_len.to_le_bytes());
        hasher.update(&self.state);
        Ok(*hasher.finalize().as_bytes())
    }

    /// Verifies the embedded hash recomputes.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotEnvelopeError::Tampered`] on any mismatch and
    /// [`SnapshotEnvelopeError::Malformed`] when the hash cannot be recomputed.
    pub fn verify(&self) -> Result<(), SnapshotEnvelopeError> {
        if self.compute_hash()? == self.hash {
            Ok(())
        } else {
            Err(SnapshotEnvelopeError::Tampered {
                at: "envelope_verify",
                reason: String::from("hash mismatch on envelope integrity check"),
            })
        }
    }

    /// Refuses an envelope whose schema version is not current.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotEnvelopeError::VersionMismatch`] otherwise.
    pub(crate) fn check_version(&self) -> Result<(), SnapshotEnvelopeError> {
        if self.schema_version == SNAPSHOT_SCHEMA_VERSION {
            Ok(())
        } else {
            Err(SnapshotEnvelopeError::VersionMismatch {
                found: self.schema_version,
                expected: SNAPSHOT_SCHEMA_VERSION,
            })
        }
    }

    /// Encodes the envelope for storage: `(hash, bytes)`.
    ///
    /// # Errors
    ///
    /// Refuses an envelope that does not verify, and a state blob past
    /// [`MAX_SNAPSHOT_BYTES`].
    pub(crate) fn serialize_for_storage(
        &self,
    ) -> Result<(Vec<u8>, Vec<u8>), SnapshotEnvelopeError> {
        self.verify()?;
        if self.state.len() > MAX_SNAPSHOT_BYTES {
            return Err(SnapshotEnvelopeError::Malformed(format!(
                "snapshot state of {} bytes exceeds {MAX_SNAPSHOT_BYTES}",
                self.state.len()
            )));
        }
        Ok((self.hash.to_vec(), self.encode()?))
    }

    /// Decodes storage bytes, checking the key hash, identity, and content.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotEnvelopeError`] for a key mismatch, a version
    /// mismatch, or failed integrity.
    pub(crate) fn deserialize_from_storage(
        hash_bytes: &[u8],
        bytes: &[u8],
    ) -> Result<Self, SnapshotEnvelopeError> {
        let envelope = Self::decode(bytes)?;
        if hash_bytes != envelope.hash {
            return Err(SnapshotEnvelopeError::Tampered {
                at: "deserialize_key_hash_mismatch",
                reason: String::from("storage key hash does not match embedded hash"),
            });
        }
        envelope.verify()?;
        Ok(envelope)
    }

    /// Length-prefixed encoding of every field including `hash`.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotEnvelopeError::Malformed`] when a section length
    /// overflows its field.
    fn encode(&self) -> Result<Vec<u8>, SnapshotEnvelopeError> {
        let mut out = Vec::with_capacity(ENVELOPE_HEADER_BYTES.saturating_add(self.state.len()));
        out.extend_from_slice(self.account.as_uuid().as_bytes().as_slice());
        out.extend_from_slice(self.space.as_uuid().as_bytes().as_slice());
        out.extend_from_slice(self.projection.as_uuid().as_bytes().as_slice());
        out.extend_from_slice(&self.schema_version.to_le_bytes());
        out.extend_from_slice(&self.semantics);
        out.extend_from_slice(&self.checkpoint);
        out.extend_from_slice(&self.last_row);
        out.extend_from_slice(&self.last_seq.to_le_bytes());
        out.extend_from_slice(&self.created_at_secs.to_le_bytes());
        let blob_len = state_blob_len(&self.state)?;
        out.extend_from_slice(&blob_len.to_le_bytes());
        out.extend_from_slice(&self.state);
        out.extend_from_slice(&self.hash);
        Ok(out)
    }

    /// Decodes [`Self::encode`] bytes with full bounds checks.
    fn decode(bytes: &[u8]) -> Result<Self, SnapshotEnvelopeError> {
        let malformed = |message: &str| SnapshotEnvelopeError::Malformed(String::from(message));
        if bytes.len() < ENVELOPE_HEADER_BYTES.saturating_add(32) {
            return Err(malformed("envelope shorter than header plus hash"));
        }
        let mut cursor = 0_usize;
        let account: [u8; 16] = decode_array(bytes, &mut cursor)?;
        let space: [u8; 16] = decode_array(bytes, &mut cursor)?;
        let projection: [u8; 16] = decode_array(bytes, &mut cursor)?;
        let schema_version = decode_u32(bytes, &mut cursor)?;
        let semantics = decode_array(bytes, &mut cursor)?;
        let checkpoint = decode_array(bytes, &mut cursor)?;
        let last_row = decode_array(bytes, &mut cursor)?;
        let last_seq = decode_u64(bytes, &mut cursor)?;
        let created_at_secs = decode_u64(bytes, &mut cursor)?;
        let blob_len = decode_u64(bytes, &mut cursor)?;
        let Ok(blob_len) = usize::try_from(blob_len) else {
            return Err(malformed("envelope blob length overflow"));
        };
        let Some(blob_end) = cursor.checked_add(blob_len) else {
            return Err(malformed("envelope blob overflow"));
        };
        let Some(state) = bytes.get(cursor..blob_end) else {
            return Err(malformed("envelope blob truncated"));
        };
        let state = state.to_vec();
        cursor = blob_end;
        let hash: [u8; 32] = decode_array(bytes, &mut cursor)?;
        if cursor != bytes.len() {
            return Err(malformed("envelope carries trailing bytes"));
        }
        Ok(Self {
            account: AccountId::from_uuid(decode_id(&account)?),
            space: SpaceId::from_uuid(decode_id(&space)?),
            projection: IndexId::from_uuid(decode_id(&projection)?),
            schema_version,
            semantics,
            checkpoint,
            last_row,
            last_seq,
            state,
            created_at_secs,
            hash,
        })
    }
}

/// Decodes one identifier field, refusing malformed bytes as a malformed
/// envelope. The three identity fields share this funnel instead of
/// repeating the mapping.
fn decode_id<T>(bytes: &[u8]) -> Result<T, SnapshotEnvelopeError>
where
    T: for<'de> lgwks_std::json::Deserialize<'de>,
{
    super::codec::decode_uuid(bytes)
        .map_err(|error| SnapshotEnvelopeError::Malformed(error.to_string()))
}

/// Reads one fixed array, advancing the cursor past it.
fn decode_array<const N: usize>(
    bytes: &[u8],
    cursor: &mut usize,
) -> Result<[u8; N], SnapshotEnvelopeError> {
    let malformed = |message: &str| SnapshotEnvelopeError::Malformed(String::from(message));
    let Some(end) = cursor.checked_add(N) else {
        return Err(malformed("envelope offset overflow"));
    };
    let Some(slice) = bytes.get(*cursor..end) else {
        return Err(malformed("envelope truncated"));
    };
    let mut fixed = [0_u8; N];
    fixed.copy_from_slice(slice);
    *cursor = end;
    Ok(fixed)
}

/// Reads one little-endian `u32`, advancing the cursor past it.
fn decode_u32(bytes: &[u8], cursor: &mut usize) -> Result<u32, SnapshotEnvelopeError> {
    let malformed = |message: &str| SnapshotEnvelopeError::Malformed(String::from(message));
    let Some(end) = cursor.checked_add(4) else {
        return Err(malformed("envelope offset overflow"));
    };
    let Some(slice) = bytes.get(*cursor..end) else {
        return Err(malformed("envelope truncated"));
    };
    let mut buffer = [0_u8; 4];
    buffer.copy_from_slice(slice);
    *cursor = end;
    Ok(u32::from_le_bytes(buffer))
}

/// Reads one little-endian `u64`, advancing the cursor past it.
fn decode_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64, SnapshotEnvelopeError> {
    let malformed = |message: &str| SnapshotEnvelopeError::Malformed(String::from(message));
    let Some(end) = cursor.checked_add(8) else {
        return Err(malformed("envelope offset overflow"));
    };
    let Some(slice) = bytes.get(*cursor..end) else {
        return Err(malformed("envelope truncated"));
    };
    let mut buffer = [0_u8; 8];
    buffer.copy_from_slice(slice);
    *cursor = end;
    Ok(u64::from_le_bytes(buffer))
}

/// Snapshot metadata for listing and retention.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct SnapshotMetadata {
    /// Owning account.
    pub account: AccountId,
    /// Owning space.
    pub space: SpaceId,
    /// The projected index.
    pub projection: IndexId,
    /// Content address of the snapshot.
    pub hash: [u8; 32],
    /// Creation time in Unix seconds.
    pub created_at_secs: u64,
    /// Last row covered.
    pub last_row: [u8; 32],
}

impl SnapshotMetadata {
    /// Builds metadata from an envelope the caller holds.
    #[must_use]
    pub const fn new(
        account: AccountId,
        space: SpaceId,
        projection: IndexId,
        hash: [u8; 32],
        created_at_secs: u64,
        last_row: [u8; 32],
    ) -> Self {
        Self {
            account,
            space,
            projection,
            hash,
            created_at_secs,
            last_row,
        }
    }
}

/// Durable snapshot storage behind a typed boundary.
pub trait SnapshotStore {
    /// Persists one sealed envelope and moves the latest pointer to it.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotEnvelopeError`] when persistence fails.
    fn save_snapshot(&self, snapshot: &SnapshotEnvelope) -> Result<(), SnapshotEnvelopeError>;

    /// Loads the latest snapshot for a scope and projection, or `None`
    /// before the first save.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotEnvelopeError`] when integrity fails.
    fn load_latest_snapshot(
        &self,
        scope: Scope,
        projection: IndexId,
    ) -> Result<Option<SnapshotEnvelope>, SnapshotEnvelopeError>;

    /// Lists verified snapshots for a scope and projection, newest first.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotEnvelopeError`] past the listing ceiling or on any
    /// corrupt record.
    fn list_snapshots(
        &self,
        scope: Scope,
        projection: IndexId,
    ) -> Result<Vec<SnapshotMetadata>, SnapshotEnvelopeError>;

    /// Deletes one snapshot by hash, clearing a latest pointer that names it.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotEnvelopeError`] when persistence fails.
    fn delete_snapshot(
        &self,
        scope: Scope,
        projection: IndexId,
        hash: &[u8; 32],
    ) -> Result<bool, SnapshotEnvelopeError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds one sealed envelope over a blob.
    fn sealed(blob: Vec<u8>) -> Result<SnapshotEnvelope, SnapshotEnvelopeError> {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        SnapshotEnvelope::seal(SnapshotSeal {
            scope,
            projection: IndexId::new(),
            semantics: [3_u8; 32],
            checkpoint: [1_u8; 32],
            last_row: [2_u8; 32],
            last_seq: 7,
            state: blob,
            created_at_secs: 1_700_000_000,
        })
    }

    #[test]
    fn sealed_envelope_verifies() -> Result<(), Box<dyn std::error::Error>> {
        let envelope = sealed(vec![3, 4, 5])?;
        envelope.verify()?;
        assert_eq!(
            envelope.compute_hash()?,
            envelope.hash,
            "seal must embed the recomputed hash"
        );
        Ok(())
    }

    #[test]
    fn tampered_blob_detection() -> Result<(), Box<dyn std::error::Error>> {
        let mut envelope = sealed(vec![3, 4, 5])?;
        envelope.state[0] ^= 0xFF;
        let error = envelope.verify();
        assert!(
            matches!(error, Err(SnapshotEnvelopeError::Tampered { .. })),
            "blob tamper must fail verification, got {error:?}"
        );
        Ok(())
    }

    #[test]
    fn tampered_checkpoint_detection() -> Result<(), Box<dyn std::error::Error>> {
        let mut envelope = sealed(vec![3, 4, 5])?;
        envelope.checkpoint[0] ^= 0xFF;
        assert!(
            envelope.verify().is_err(),
            "checkpoint tamper must fail verification"
        );
        Ok(())
    }

    #[test]
    fn tampered_last_seq_detection() -> Result<(), Box<dyn std::error::Error>> {
        let mut envelope = sealed(vec![3, 4, 5])?;
        envelope.last_seq ^= 0xFFFF;
        assert!(
            envelope.verify().is_err(),
            "last_seq tamper must fail verification"
        );
        Ok(())
    }

    #[test]
    fn tampered_state_detection() -> Result<(), Box<dyn std::error::Error>> {
        let mut envelope = sealed(vec![3, 4, 5])?;
        envelope.state.push(9);
        assert!(
            envelope.verify().is_err(),
            "state tamper must fail verification"
        );
        assert!(
            envelope.serialize_for_storage().is_err(),
            "tampered state must never persist"
        );
        Ok(())
    }

    #[test]
    fn version_mismatch_refused() -> Result<(), Box<dyn std::error::Error>> {
        let mut envelope = sealed(vec![3, 4, 5])?;
        envelope.schema_version = 999;
        let error = envelope.check_version();
        assert!(
            matches!(error, Err(SnapshotEnvelopeError::VersionMismatch { .. })),
            "a future version must be refused, got {error:?}"
        );
        assert!(
            envelope.verify().is_err(),
            "a version mutation breaks the seal too"
        );
        Ok(())
    }

    #[test]
    fn unsealed_envelope_never_persisted() -> Result<(), Box<dyn std::error::Error>> {
        let mut envelope = sealed(vec![3, 4, 5])?;
        envelope.state.push(9);
        let error = envelope.serialize_for_storage();
        assert!(
            error.is_err(),
            "a mutated-after-seal envelope must never serialize, got {error:?}"
        );
        Ok(())
    }

    #[test]
    fn serialize_deserialize_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
        let envelope = sealed(vec![1, 2, 3, 4, 5])?;
        let (hash_bytes, bytes) = envelope.serialize_for_storage()?;
        let restored = SnapshotEnvelope::deserialize_from_storage(&hash_bytes, &bytes)?;
        assert_eq!(envelope, restored, "roundtrip must preserve the envelope");
        restored.verify()?;
        Ok(())
    }

    #[test]
    fn corrupted_key_hash_rejected() -> Result<(), Box<dyn std::error::Error>> {
        let envelope = sealed(vec![5, 6, 7])?;
        let (_, bytes) = envelope.serialize_for_storage()?;
        let mut corrupted = [0_u8; 32];
        corrupted[0] = 0xFF;
        let error = SnapshotEnvelope::deserialize_from_storage(&corrupted, &bytes);
        assert!(
            error.is_err(),
            "a wrong storage key must be refused, got {error:?}"
        );
        Ok(())
    }

    #[test]
    fn truncated_storage_refused() -> Result<(), Box<dyn std::error::Error>> {
        let envelope = sealed(vec![5, 6, 7, 8])?;
        let (hash_bytes, bytes) = envelope.serialize_for_storage()?;
        let half = bytes.len().checked_div(2).ok_or("test split")?;
        assert!(
            SnapshotEnvelope::deserialize_from_storage(&hash_bytes, &bytes[..half]).is_err(),
            "truncated storage must be refused"
        );
        Ok(())
    }

    #[test]
    fn extended_storage_refused() -> Result<(), Box<dyn std::error::Error>> {
        let envelope = sealed(vec![5, 6, 7, 8])?;
        let (hash_bytes, bytes) = envelope.serialize_for_storage()?;
        let mut extended = bytes;
        extended.push(0xFF);
        assert!(
            SnapshotEnvelope::deserialize_from_storage(&hash_bytes, &extended).is_err(),
            "extended storage must be refused"
        );
        Ok(())
    }
}
