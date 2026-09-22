//! Tenant-bound, portable encrypted archive contracts.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20. Source URLs are in `evidence/SOURCES.md`.
//!
//! The port deliberately accepts only ciphertext.  A physical KMS or object
//! store remains an adapter concern, but every adapter must preserve the exact
//! scope, residency, customer key version, object class, and content receipt.

use lgwks_std::hash::blake3;
use logical_db_model::Scope;
use thiserror::Error;

/// Maximum bytes in a portable archive identity or declared region.
const MAX_ARCHIVE_ID_BYTES: usize = 128;
/// Maximum ciphertext carried in a single archive object.
const MAX_ARCHIVE_CIPHERTEXT_BYTES: usize = 64 * 1024 * 1024;

/// One declared geographic residency boundary.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Region(String);

impl Region {
    /// Creates one stable residency identity.
    ///
    /// # Errors
    /// Returns [`ArchiveError::InvalidIdentity`] for an empty or oversized value.
    pub fn new(value: impl Into<String>) -> Result<Self, ArchiveError> {
        let value = value.into();
        validate_identity(&value, "region")?;
        Ok(Self(value))
    }

    /// Returns the declared residency identity.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Customer-controlled key identity; key material never crosses this port.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct CustomerKeyId(String);

impl CustomerKeyId {
    /// Creates one stable customer key identity.
    ///
    /// # Errors
    /// Returns [`ArchiveError::InvalidIdentity`] for an empty or oversized value.
    pub fn new(value: impl Into<String>) -> Result<Self, ArchiveError> {
        let value = value.into();
        validate_identity(&value, "customer key")?;
        Ok(Self(value))
    }

    /// Returns the opaque key identity.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Nonzero version of a customer-controlled wrapping key.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct CustomerKeyVersion(u64);

impl CustomerKeyVersion {
    /// Creates one non-sentinel key version.
    ///
    /// # Errors
    /// Returns [`ArchiveError::ReservedKeyVersion`] for zero.
    pub const fn new(value: u64) -> Result<Self, ArchiveError> {
        if value == 0 {
            return Err(ArchiveError::ReservedKeyVersion);
        }
        Ok(Self(value))
    }

    /// Returns the portable version number.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Stable opaque identity of one immutable archive object.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ArchiveObjectId(String);

impl ArchiveObjectId {
    /// Creates one archive object identity.
    ///
    /// # Errors
    /// Returns [`ArchiveError::InvalidIdentity`] for an empty or oversized value.
    pub fn new(value: impl Into<String>) -> Result<Self, ArchiveError> {
        let value = value.into();
        validate_identity(&value, "archive object")?;
        Ok(Self(value))
    }

    /// Returns the opaque object identity.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Logical role of an immutable archive object.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ArchiveKind {
    /// State-bearing recovery baseline.
    Baseline,
    /// Durable attachment bytes.
    Attachment,
    /// Portable logical export.
    Export,
}

/// Ciphertext and its declared security context, never plaintext or key material.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArchiveObject {
    /// Immutable archive identity.
    id: ArchiveObjectId,
    /// Scope that owns this ciphertext.
    scope: Scope,
    /// Residency boundary of the ciphertext.
    region: Region,
    /// Customer-controlled wrapping-key identity.
    key: CustomerKeyId,
    /// Customer-controlled wrapping-key revision.
    key_version: CustomerKeyVersion,
    /// Purpose for which the ciphertext is retained.
    kind: ArchiveKind,
    /// Encrypted bytes; plaintext is never represented here.
    ciphertext: Vec<u8>,
    /// BLAKE3 digest of the encrypted bytes.
    digest: [u8; 32],
}

impl ArchiveObject {
    /// Builds a bounded encrypted archive object and calculates its receipt digest.
    ///
    /// # Errors
    /// Returns [`ArchiveError::InvalidCiphertext`] for an empty or oversized payload.
    pub fn new(
        id: ArchiveObjectId,
        scope: Scope,
        region: Region,
        key: CustomerKeyId,
        key_version: CustomerKeyVersion,
        kind: ArchiveKind,
        ciphertext: Vec<u8>,
    ) -> Result<Self, ArchiveError> {
        if ciphertext.is_empty() || ciphertext.len() > MAX_ARCHIVE_CIPHERTEXT_BYTES {
            return Err(ArchiveError::InvalidCiphertext);
        }
        let digest = *blake3(&ciphertext).as_bytes();
        Ok(Self {
            id,
            scope,
            region,
            key,
            key_version,
            kind,
            ciphertext,
            digest,
        })
    }

    /// Returns the immutable object identity.
    #[must_use]
    pub fn id(&self) -> &ArchiveObjectId {
        &self.id
    }
    /// Returns the owning scope.
    #[must_use]
    pub const fn scope(&self) -> Scope {
        self.scope
    }
    /// Returns the declared residency boundary.
    #[must_use]
    pub fn region(&self) -> &Region {
        &self.region
    }
    /// Returns the customer key identity.
    #[must_use]
    pub fn key(&self) -> &CustomerKeyId {
        &self.key
    }
    /// Returns the customer key version used to wrap this object.
    #[must_use]
    pub const fn key_version(&self) -> CustomerKeyVersion {
        self.key_version
    }
    /// Returns the logical archive role.
    #[must_use]
    pub const fn kind(&self) -> ArchiveKind {
        self.kind
    }
    /// Returns ciphertext only.
    #[must_use]
    pub fn ciphertext(&self) -> &[u8] {
        &self.ciphertext
    }
    /// Returns the BLAKE3 ciphertext receipt digest.
    #[must_use]
    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

/// Exact immutable write authorization for one archive object.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArchiveWrite {
    /// Immutable object accepted for a one-shot write.
    object: ArchiveObject,
}

impl ArchiveWrite {
    /// Wraps an object for a one-shot immutable write.
    #[must_use]
    pub const fn new(object: ArchiveObject) -> Self {
        Self { object }
    }
    /// Returns the immutable object being written.
    #[must_use]
    pub const fn object(&self) -> &ArchiveObject {
        &self.object
    }
}

/// Receipt retained for audit and verified export/import handoff.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArchiveReceipt {
    /// Receipt-bound archive identity.
    id: ArchiveObjectId,
    /// Receipt-bound owning scope.
    scope: Scope,
    /// Receipt-bound residency boundary.
    region: Region,
    /// Receipt-bound customer key identity.
    key: CustomerKeyId,
    /// Receipt-bound customer key revision.
    key_version: CustomerKeyVersion,
    /// Receipt-bound logical archive role.
    kind: ArchiveKind,
    /// Receipt-bound ciphertext digest.
    digest: [u8; 32],
}

impl ArchiveReceipt {
    /// Derives the exact receipt an archive adapter returns after accepting an object.
    ///
    /// This only describes ciphertext the adapter was asked to store; it does
    /// not confer a read capability. [`SovereignArchive::restore`] still
    /// requires the adapter to return a matching object in its own residency.
    #[must_use]
    pub fn from_object(object: &ArchiveObject) -> Self {
        Self {
            id: object.id.clone(),
            scope: object.scope,
            region: object.region.clone(),
            key: object.key.clone(),
            key_version: object.key_version,
            kind: object.kind,
            digest: object.digest,
        }
    }

    /// Returns the archived object identity.
    #[must_use]
    pub fn id(&self) -> &ArchiveObjectId {
        &self.id
    }
    /// Returns the owning scope.
    #[must_use]
    pub const fn scope(&self) -> Scope {
        self.scope
    }
    /// Returns the residency boundary attested by this receipt.
    #[must_use]
    pub fn region(&self) -> &Region {
        &self.region
    }
    /// Returns the attested customer key identity.
    #[must_use]
    pub fn key(&self) -> &CustomerKeyId {
        &self.key
    }
    /// Returns the attested customer key version.
    #[must_use]
    pub const fn key_version(&self) -> CustomerKeyVersion {
        self.key_version
    }
    /// Returns the attested object class.
    #[must_use]
    pub const fn kind(&self) -> ArchiveKind {
        self.kind
    }
    /// Returns the immutable ciphertext digest.
    #[must_use]
    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }

    /// Checks every security-relevant object field against this receipt.
    fn verifies(&self, object: &ArchiveObject) -> bool {
        self.id == object.id
            && self.scope == object.scope
            && self.region == object.region
            && self.key == object.key
            && self.key_version == object.key_version
            && self.kind == object.kind
            && self.digest == object.digest
            && object.digest == *blake3(&object.ciphertext).as_bytes()
    }
}

/// Exact restore/import request that prevents a cross-scope or cross-region read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArchiveRestore {
    /// Scope permitted to restore this archive object.
    scope: Scope,
    /// Residency boundary required for the restore.
    region: Region,
    /// Exact immutable archive receipt being restored.
    receipt: ArchiveReceipt,
}

impl ArchiveRestore {
    /// Creates one restore request bound to the receipt's original scope and region.
    ///
    /// # Errors
    /// Returns [`ArchiveError::ReceiptMismatch`] for a cross-scope or cross-region request.
    pub fn new(
        scope: Scope,
        region: Region,
        receipt: ArchiveReceipt,
    ) -> Result<Self, ArchiveError> {
        if receipt.scope != scope || receipt.region != region {
            return Err(ArchiveError::ReceiptMismatch);
        }
        Ok(Self {
            scope,
            region,
            receipt,
        })
    }

    /// Returns the requesting scope.
    #[must_use]
    pub const fn scope(&self) -> Scope {
        self.scope
    }
    /// Returns the required residency boundary.
    #[must_use]
    pub fn region(&self) -> &Region {
        &self.region
    }
    /// Returns the receipt whose exact object may be restored.
    #[must_use]
    pub fn receipt(&self) -> &ArchiveReceipt {
        &self.receipt
    }
}

/// Provider-neutral immutable archive port. Implementations must reject writes
/// outside [`Self::residency`] and return only an object matching the request.
pub trait ArchiveObjectStore {
    /// Declared physical residency of this adapter.
    fn residency(&self) -> &Region;
    /// Persists one immutable ciphertext object and returns its exact receipt.
    fn put(&mut self, write: ArchiveWrite) -> Result<ArchiveReceipt, ArchiveError>;
    /// Reads one ciphertext object only after exact receipt verification.
    fn get(&self, restore: &ArchiveRestore) -> Result<ArchiveObject, ArchiveError>;
}

/// Guarded archive façade that prevents callers from silently crossing a port's region.
pub struct SovereignArchive<S> {
    /// Region-bound adapter hidden behind the fail-closed façade.
    store: S,
}

impl<S> SovereignArchive<S>
where
    S: ArchiveObjectStore,
{
    /// Owns one region-bound archive adapter.
    #[must_use]
    pub const fn new(store: S) -> Self {
        Self { store }
    }

    /// Writes only to the object-declared residency boundary.
    ///
    /// # Errors
    /// Returns [`ArchiveError::ResidencyMismatch`] before the port is called.
    pub fn archive(&mut self, write: ArchiveWrite) -> Result<ArchiveReceipt, ArchiveError> {
        if self.store.residency() != write.object.region() {
            return Err(ArchiveError::ResidencyMismatch);
        }
        let receipt = self.store.put(write)?;
        Ok(receipt)
    }

    /// Restores only from the receipt-declared residency boundary and verifies
    /// the returned object before exposing it to import/recovery callers.
    ///
    /// # Errors
    /// Returns [`ArchiveError::ResidencyMismatch`] before the port is called or
    /// [`ArchiveError::ReceiptMismatch`] for tampering or an incorrect object.
    pub fn restore(&self, restore: &ArchiveRestore) -> Result<ArchiveObject, ArchiveError> {
        if self.store.residency() != restore.region() {
            return Err(ArchiveError::ResidencyMismatch);
        }
        let object = self.store.get(restore)?;
        if !restore.receipt.verifies(&object) {
            return Err(ArchiveError::ReceiptMismatch);
        }
        Ok(object)
    }
}

/// Fail-closed archive contract errors.
#[derive(Debug, Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum ArchiveError {
    /// A stable identity was empty or exceeded its bounded representation.
    #[error("invalid {kind} identity")]
    InvalidIdentity { kind: &'static str },
    /// Zero cannot name a valid customer key version.
    #[error("customer key version zero is reserved")]
    ReservedKeyVersion,
    /// Ciphertext was empty or exceeded the contract bound.
    #[error("archive ciphertext is empty or exceeds its bound")]
    InvalidCiphertext,
    /// A caller tried to use a port outside the declared residency boundary.
    #[error("archive residency does not match the declared object residency")]
    ResidencyMismatch,
    /// A restore/import request or returned object differs from its receipt.
    #[error("archive receipt does not match the requested object")]
    ReceiptMismatch,
}

/// Validates a bounded portable archive identity.
fn validate_identity(value: &str, kind: &'static str) -> Result<(), ArchiveError> {
    if value.is_empty() || value.len() > MAX_ARCHIVE_ID_BYTES {
        return Err(ArchiveError::InvalidIdentity { kind });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use logical_db_model::{AccountId, SpaceId};

    use super::*;

    struct MemoryArchive {
        region: Region,
        objects: BTreeMap<ArchiveObjectId, ArchiveObject>,
        tamper_on_read: bool,
    }

    impl ArchiveObjectStore for MemoryArchive {
        fn residency(&self) -> &Region {
            &self.region
        }

        fn put(&mut self, write: ArchiveWrite) -> Result<ArchiveReceipt, ArchiveError> {
            if write.object.region != self.region {
                return Err(ArchiveError::ResidencyMismatch);
            }
            let receipt = ArchiveReceipt::from_object(&write.object);
            self.objects.insert(write.object.id.clone(), write.object);
            Ok(receipt)
        }

        fn get(&self, restore: &ArchiveRestore) -> Result<ArchiveObject, ArchiveError> {
            let mut object = self
                .objects
                .get(restore.receipt.id())
                .cloned()
                .ok_or(ArchiveError::ReceiptMismatch)?;
            if self.tamper_on_read {
                object.ciphertext.push(9);
            }
            Ok(object)
        }
    }

    fn fixture(region: &str) -> Result<ArchiveObject, ArchiveError> {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        ArchiveObject::new(
            ArchiveObjectId::new("baseline-1")?,
            scope,
            Region::new(region)?,
            CustomerKeyId::new("customer-kek")?,
            CustomerKeyVersion::new(2)?,
            ArchiveKind::Baseline,
            vec![1, 2, 3],
        )
    }

    #[test]
    fn cross_region_archive_is_refused_before_the_port_is_called() -> Result<(), ArchiveError> {
        let object = fixture("ca-central")?;
        let mut archive = SovereignArchive::new(MemoryArchive {
            region: Region::new("us-east")?,
            objects: BTreeMap::new(),
            tamper_on_read: false,
        });
        assert_eq!(
            archive.archive(ArchiveWrite::new(object)),
            Err(ArchiveError::ResidencyMismatch)
        );
        assert!(
            archive.store.objects.is_empty(),
            "a rejected write must not reach the archive port"
        );
        Ok(())
    }

    #[test]
    fn restore_rejects_tampered_ciphertext_even_when_the_object_id_matches()
    -> Result<(), ArchiveError> {
        let object = fixture("ca-central")?;
        let scope = object.scope();
        let region = object.region().clone();
        let mut archive = SovereignArchive::new(MemoryArchive {
            region: region.clone(),
            objects: BTreeMap::new(),
            tamper_on_read: false,
        });
        let receipt = archive.archive(ArchiveWrite::new(object))?;
        archive.store.tamper_on_read = true;
        let restore = ArchiveRestore::new(scope, region, receipt)?;
        assert_eq!(
            archive.restore(&restore),
            Err(ArchiveError::ReceiptMismatch)
        );
        Ok(())
    }
}
