//! Stable identities, authority decisions, limits, and errors for Logical-DB.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

#![forbid(unsafe_code)]

mod baseline;
mod capability;
pub use baseline::{
    BaselineDigest, MAX_BASELINE_BYTES, MAX_BASELINE_DOCUMENTS, MAX_BASELINE_PAGE_DOCUMENTS,
    SNAPSHOT_BASELINE_BYTES, SNAPSHOT_BASELINE_LEGACY_BYTES, SNAPSHOT_BASELINE_LEGACY_VERSION,
    SNAPSHOT_BASELINE_VERSION, SnapshotArtifact, SnapshotBaseline, SnapshotDigestBuilder,
    SnapshotDocument, SnapshotRecord,
};
pub use capability::{
    BackendClass, CAPABILITY_PROFILE_BYTES, CAPABILITY_PROFILE_VERSION, Capability,
    CapabilityProfile, common_guarantees,
};

use std::{collections::BTreeMap, fmt};

use lgwks_std::json::{Deserialize, Serialize};
use thiserror::Error;

mod namespace_wire;
use namespace_wire::{take, take_checkpoint, take_namespace, take_u16, take_u32};

/// The estate's named owner for RFC 4122 identifiers is `lgwks_std::id`.
///
/// That module supplies the `UUIDv4` layout and the OS entropy this crate
/// builds time-ordered `UUIDv7` values on top of, which the standard library
/// cannot provide. See `uuid_v7` for the layout and the `uuid` entry in
/// `contract/APPROVED.toml` (`capability = "identity.uuid-v7"`) for the
/// registered approval of the external parse contract.
use lgwks_std::id::Uuid;

/// Maximum UTF-8 bytes in a user-controlled display-independent name.
pub const MAX_NAME_BYTES: usize = 512;
/// Maximum causal change bytes accepted in one envelope.
pub const MAX_CHANGE_BYTES: usize = 1_048_576;
/// Maximum changes accepted in one protocol page.
pub const MAX_CHANGES_PER_PAGE: usize = 256;
/// Maximum aggregate bytes accepted in one protocol page.
pub const MAX_PAGE_BYTES: usize = 8_388_608;
/// Maximum encoded state for one materialized document snapshot.
pub const MAX_SNAPSHOT_BYTES: usize = 16_777_216;

/// Milliseconds since the Unix epoch used to order time-ordered identifiers.
fn unix_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}

/// Builds one time-ordered `UUIDv7` from the estate's OS entropy.
///
/// `lgwks_std::id` owns the RFC 4122 layout and the entropy source but
/// generates v4 only, so the v7 prefix is composed here: a 48-bit big-endian
/// Unix millisecond timestamp, the RFC 9562 version nibble `7`, and the
/// variant bits `10`. The remaining 74 random bits keep identifiers unique
/// within a millisecond.
#[must_use]
pub fn uuid_v7() -> Uuid {
    let mut raw: [u8; 16] = lgwks_std::random::bytes().unwrap_or_default();
    raw[..6].copy_from_slice(&unix_millis().to_be_bytes()[2..]);
    raw[6] = (raw[6] & 0x0F) | 0x70;
    raw[8] = (raw[8] & 0x3F) | 0x80;
    Uuid::from_bytes(raw)
}

/// Declares a UUID-backed newtype identifier with the shared model surface.
///
/// Each invocation emits the transparent newtype, its `now_v7` constructor,
/// UUID accessors, `Default`, and `Display`, so every logical identity is a
/// distinct type rather than a bare `Uuid`.
macro_rules! entity_id {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(Uuid);

        impl $name {
            /// Generates a time-ordered `UUIDv7` identifier.
            #[must_use]
            pub fn new() -> Self {
                Self(uuid_v7())
            }

            /// Wraps a previously validated UUID.
            #[must_use]
            pub const fn from_uuid(value: Uuid) -> Self {
                Self(value)
            }

            /// Exposes the portable UUID value without exposing storage bytes.
            #[must_use]
            pub const fn as_uuid(self) -> Uuid {
                self.0
            }

            /// Exposes the identifier's 16 big-endian bytes, for key layouts
            /// whose backend tuple codec owns its own element types.
            #[must_use]
            pub fn as_bytes(self) -> [u8; 16] {
                *self.0.as_bytes()
            }

            /// Parses the canonical 8-4-4-4-12 hyphenated identifier form.
            pub fn parse_canonical(text: &str) -> Result<Self, lgwks_std::id::ParseError> {
                Uuid::parse(text).map(Self)
            }

            /// The RFC 4122 variant nibble, or `None` when unrecognised.
            #[must_use]
            pub fn variant_bits(self) -> Option<u8> {
                let bytes = *self.0.as_bytes();
                match bytes[8] & 0xC0 {
                    0x80 => Some(bytes[8] >> 6),
                    0xC0 => Some((bytes[8] >> 4) & 0b11),
                    _ => None,
                }
            }

            /// The 48-bit Unix millisecond prefix of a time-ordered `v7`
            /// identifier, or `None` for any other version.
            #[must_use]
            pub fn unix_millis(self) -> Option<u64> {
                let bytes = *self.0.as_bytes();
                if bytes[6] >> 4 != 7 || self.variant_bits() != Some(0b10) {
                    return None;
                }
                let mut prefix = [0_u8; 8];
                prefix[2..].copy_from_slice(&bytes[..6]);
                Some(u64::from_be_bytes(prefix))
            }

            /// Rehydrates an identifier from exactly 16 durable bytes.
            ///
            /// This crate owns identifier construction for the whole
            /// workspace, so every other crate decodes durable identifiers
            /// through here rather than building them itself.
            pub fn from_durable_bytes(bytes: &[u8; 16]) -> Self {
                Self(Uuid::from_bytes(*bytes))
            }

            /// Builds a deterministic identifier from a 128-bit fixture value.
            #[must_use]
            pub fn from_fixture_u128(value: u128) -> Self {
                Self(Uuid::from_bytes(value.to_be_bytes()))
            }

            /// A fresh time-ordered identifier rendered for a unique path.
            #[must_use]
            pub fn unique_suffix() -> String {
                Self::new().to_string()
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                let bytes = self.0.as_bytes();
                for (index, byte) in bytes.iter().enumerate() {
                    if matches!(index, 4 | 6 | 8 | 10) {
                        formatter.write_str("-")?;
                    }
                    write!(formatter, "{byte:02x}")?;
                }
                Ok(())
            }
        }

        impl Serialize for $name {
            fn serialize<S: lgwks_std::json::serde::Serializer>(
                &self,
                serializer: S,
            ) -> Result<S::Ok, S::Error> {
                let bytes = self.as_bytes();
                serializer.serialize_bytes(bytes.as_slice())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: lgwks_std::json::serde::Deserializer<'de>>(
                deserializer: D,
            ) -> Result<Self, D::Error> {
                struct ByteVisitor;

                impl<'de> lgwks_std::json::serde::de::Visitor<'de> for ByteVisitor {
                    type Value = [u8; 16];

                    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                        formatter.write_str("16 durable identifier bytes")
                    }

                    fn visit_bytes<E: lgwks_std::json::serde::de::Error>(
                        self,
                        value: &[u8],
                    ) -> Result<Self::Value, E> {
                        value
                            .try_into()
                            .map_err(|_| E::custom("identifier is not 16 bytes"))
                    }

                    fn visit_seq<A: lgwks_std::json::serde::de::SeqAccess<'de>>(
                        self,
                        mut seq: A,
                    ) -> Result<Self::Value, A::Error> {
                        let mut raw = [0_u8; 16];
                        for slot in &mut raw {
                            *slot = seq.next_element()?.ok_or_else(|| {
                                lgwks_std::json::serde::de::Error::custom(
                                    "identifier ended before 16 bytes",
                                )
                            })?;
                        }
                        Ok(raw)
                    }
                }

                let raw = deserializer.deserialize_bytes(ByteVisitor)?;
                Ok(Self(Uuid::from_bytes(raw)))
            }
        }
    };
}

entity_id!(AccountId, "Identifies one person's logical account.");
entity_id!(
    SpaceId,
    "Identifies an application-owned namespace within an account."
);
entity_id!(DocumentId, "Identifies a convergent structured document.");
entity_id!(IndexId, "Identifies one schema-declared secondary index.");
entity_id!(
    ReplicaId,
    "Identifies one durable local replica installation."
);
entity_id!(OperationId, "Identifies one retryable logical operation.");
entity_id!(PeerId, "Identifies one synchronization service or peer.");

/// Portable scalar accepted by an ordered secondary index.
///
/// Floating-point and CRDT container values are deliberately excluded: their
/// ordering and conflict semantics must be made explicit by an application
/// projection before they enter a durable query key.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(crate = "lgwks_std::json::serde")]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
#[non_exhaustive]
pub enum IndexValue {
    /// Boolean ordering is `false`, then `true`.
    Boolean(bool),
    /// Signed integer ordered by mathematical value.
    Integer(i64),
    /// Unsigned integer ordered by mathematical value.
    Unsigned(u64),
    /// UTF-8 text ordered by encoded byte sequence.
    Text(String),
    /// Opaque bytes ordered lexicographically.
    Bytes(Vec<u8>),
    /// Milliseconds since the Unix epoch, ordered chronologically.
    Timestamp(i64),
}

/// One materialized secondary-index value for a document.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(crate = "lgwks_std::json::serde")]
#[non_exhaustive]
pub struct IndexEntry {
    /// Stable schema identity of the projection.
    pub index: IndexId,
    /// Current projected value. Absence is represented by no entry.
    pub value: IndexValue,
}

impl IndexEntry {
    /// Builds one index projection from its schema identity and value.
    ///
    /// This is the constructor `#[non_exhaustive]` requires: callers outside
    /// this crate cannot use a struct expression, so they compose through here.
    #[must_use]
    pub const fn new(index: IndexId, value: IndexValue) -> Self {
        Self { index, value }
    }
}

/// Monotonic optimistic-concurrency marker for one materialized document.
///
/// This is deliberately independent from a CRDT change hash: one snapshot can
/// incorporate several concurrent changes and therefore has multiple causal
/// heads rather than one privileged last change.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(crate = "lgwks_std::json::serde")]
#[serde(transparent)]
pub struct DocumentRevision(u64);

impl DocumentRevision {
    /// Constructs a revision read from durable storage.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the portable numeric revision.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns the checked successor.
    #[must_use]
    pub const fn next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }
}

/// Content identity of immutable CRDT change bytes.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(crate = "lgwks_std::json::serde")]
#[serde(transparent)]
pub struct ChangeDigest([u8; 32]);

impl ChangeDigest {
    /// Wraps a digest computed by the CRDT boundary.
    #[must_use]
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns the stable digest bytes.
    #[must_use]
    pub const fn into_bytes(self) -> [u8; 32] {
        self.0
    }
}

/// Canonical identity of an action recomputed at a trusted admission boundary.
///
/// This is not an application retry label. Stores must recompute the action
/// from its fields before accepting a grant carrying this identity.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(crate = "lgwks_std::json::serde")]
#[serde(transparent)]
pub struct ActionDigest([u8; 32]);

impl ActionDigest {
    /// Wraps the output of the versioned canonical action encoder.
    #[must_use]
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns the stable digest bytes.
    #[must_use]
    pub const fn into_bytes(self) -> [u8; 32] {
        self.0
    }
}

/// Canonical identity of the caller's requested logical mutation.
///
/// This is distinct from [`ChangeDigest`]: retries bind to what the caller
/// requested, while the change digest identifies the CRDT result.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(crate = "lgwks_std::json::serde")]
#[serde(transparent)]
pub struct OperationDigest([u8; 32]);

impl OperationDigest {
    /// Wraps a digest produced from the application's canonical request bytes.
    #[must_use]
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns the stable digest bytes.
    #[must_use]
    pub const fn into_bytes(self) -> [u8; 32] {
        self.0
    }
}

/// Reports whether a checkpoint position fell behind the retention floor.
///
/// A behind-floor checkpoint no longer addresses servable log history and must
/// take the explicit full-recovery transition instead of a partial page.
#[must_use]
pub const fn is_behind_retention(position: u64, floor: u64) -> bool {
    position < floor
}

/// Reports whether a retention floor addresses committed history.
///
/// The floor may equal the frontier (everything prunable is pruned) but never
/// pass it; a floor past the frontier addresses uncommitted positions.
#[must_use]
pub const fn retention_floor_valid(floor: u64, frontier: u64) -> bool {
    floor <= frontier
}

/// Maximum sync pages processed for one namespace in a single cycle.
///
/// This mirrors the sync engine ceiling so a namespace policy can never
/// promise more pages than the driver below it will run.
pub const MAX_NAMESPACE_PAGES_PER_CYCLE: u8 = 32;
/// Maximum opaque service cursor bytes carried by one namespace checkpoint.
///
/// This matches the protocol and store checkpoint bounds so every cursor a
/// namespace records stays acceptable to the pull request and to the durable
/// slot that multiplexes it.
pub const MAX_NAMESPACE_CURSOR_BYTES: usize = 4_096;
/// Maximum encoded bytes of one multiplexed namespace checkpoint map.
///
/// The map shares the single durable checkpoint slot bound to one upstream,
/// so the whole multiplex must fit where one global cursor fits.
pub const MAX_NAMESPACE_MAP_BYTES: usize = 4_096;
/// Layout version of one encoded namespace checkpoint.
pub const NAMESPACE_CHECKPOINT_VERSION: u16 = 1;
/// Layout version of one encoded namespace checkpoint map.
pub const NAMESPACE_CHECKPOINT_MAP_VERSION: u16 = 1;
/// Freshness horizon for a stored namespace cursor in milliseconds.
///
/// A cursor older than a day forces the explicit full-recovery pull rather
/// than trusting a position the service may have expired while the device
/// slept. Every installed page refreshes the horizon.
pub const NAMESPACE_CHECKPOINT_TTL_MS: u64 = 86_400_000;
/// Magic prefix rejecting foreign slot bytes before version checks.
const NAMESPACE_MAP_MAGIC: [u8; 6] = *b"LDBNS1";

/// Network condition gating a namespace sync domain.
///
/// [`NetworkCondition::Offline`] defers all work without touching the
/// transport; [`NetworkCondition::Constrained`] runs one page per cycle so a
/// metered link still makes progress; [`NetworkCondition::Online`] runs the
/// full policy budget.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(crate = "lgwks_std::json::serde")]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum NetworkCondition {
    /// Run the full page budget every cycle.
    #[default]
    Online,
    /// Run exactly one page per cycle.
    Constrained,
    /// Defer all work and report it as deferred.
    Offline,
}

/// Per-namespace sync policy: page budget plus network condition.
///
/// Each namespace carries its own budget so a chatty namespace cannot spend
/// the quiet namespace's pages: the driver runs every domain with its own
/// limits against its own checkpoint.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(crate = "lgwks_std::json::serde")]
pub struct NamespacePolicy {
    /// Maximum changes requested in each pull page.
    changes_per_page: u16,
    /// Maximum encoded change bytes requested in each pull page.
    bytes_per_page: u32,
    /// Maximum pull pages before the driver yields to the host.
    pages_per_cycle: u8,
    /// Network condition gating this sync domain.
    network: NetworkCondition,
}

impl NamespacePolicy {
    /// Builds a validated policy mirroring the protocol page bounds.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError::PageTooLarge`] for a zero or over-limit budget.
    pub fn new(
        changes_per_page: u16,
        bytes_per_page: u32,
        pages_per_cycle: u8,
        network: NetworkCondition,
    ) -> Result<Self, ModelError> {
        if changes_per_page == 0
            || usize::from(changes_per_page) > MAX_CHANGES_PER_PAGE
            || bytes_per_page == 0
            || usize::try_from(bytes_per_page).unwrap_or(usize::MAX) > MAX_PAGE_BYTES
            || pages_per_cycle == 0
            || pages_per_cycle > MAX_NAMESPACE_PAGES_PER_CYCLE
        {
            return Err(ModelError::PageTooLarge);
        }
        Ok(Self {
            changes_per_page,
            bytes_per_page,
            pages_per_cycle,
            network,
        })
    }

    /// Returns the maximum changes requested in each page.
    #[must_use]
    pub const fn changes_per_page(self) -> u16 {
        self.changes_per_page
    }

    /// Returns the maximum encoded change bytes requested in each page.
    #[must_use]
    pub const fn bytes_per_page(self) -> u32 {
        self.bytes_per_page
    }

    /// Returns the maximum pull pages before yielding to the host.
    #[must_use]
    pub const fn pages_per_cycle(self) -> u8 {
        self.pages_per_cycle
    }

    /// Returns the network condition gating this domain.
    #[must_use]
    pub const fn network(self) -> NetworkCondition {
        self.network
    }
}

impl Default for NamespacePolicy {
    fn default() -> Self {
        Self {
            changes_per_page: 128,
            bytes_per_page: 4 * 1_048_576,
            pages_per_cycle: 16,
            network: NetworkCondition::Online,
        }
    }
}

/// Validates a cache namespace name.
///
/// # Errors
///
/// Returns [`ModelError::InvalidIdentifierLength`] for an empty or oversized
/// name.
pub fn validate_namespace(namespace: &str) -> Result<(), ModelError> {
    if namespace.is_empty() || namespace.len() > MAX_NAME_BYTES {
        return Err(ModelError::InvalidIdentifierLength {
            kind: "namespace",
            context: "validate_namespace",
            actual: namespace.len(),
            maximum: MAX_NAME_BYTES,
        });
    }
    Ok(())
}

/// One namespace's durable sync position with an explicit expiry.
///
/// The cursor is the opaque service position for this namespace alone; the
/// expiry is the local freshness horizon. An expired cursor takes the same
/// explicit full-recovery transition as an expired global checkpoint instead
/// of a partial page.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(crate = "lgwks_std::json::serde")]
pub struct NamespaceCheckpoint {
    /// Checkpoint layout version carried on the wire.
    version: u16,
    /// Exclusive local freshness horizon in milliseconds since the Unix epoch.
    expires_at_unix_ms: u64,
    /// Opaque service cursor bytes for this namespace alone.
    cursor: Vec<u8>,
}

impl NamespaceCheckpoint {
    /// Records a cursor with a nonzero exclusive expiry.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError::InvalidIdentifierLength`] for an empty or
    /// oversized cursor and [`ModelError::InvalidAuthorityExpiry`] for the
    /// reserved zero expiry.
    pub fn new(cursor: Vec<u8>, expires_at_unix_ms: u64) -> Result<Self, ModelError> {
        if cursor.is_empty() || cursor.len() > MAX_NAMESPACE_CURSOR_BYTES {
            return Err(ModelError::InvalidIdentifierLength {
                kind: "namespace cursor",
                context: "NamespaceCheckpoint::new",
                actual: cursor.len(),
                maximum: MAX_NAMESPACE_CURSOR_BYTES,
            });
        }
        if expires_at_unix_ms == 0 {
            return Err(ModelError::InvalidAuthorityExpiry);
        }
        Ok(Self {
            version: NAMESPACE_CHECKPOINT_VERSION,
            expires_at_unix_ms,
            cursor,
        })
    }

    /// Returns the checkpoint layout version.
    #[must_use]
    pub const fn version(&self) -> u16 {
        self.version
    }

    /// Returns the exclusive expiry in milliseconds since the Unix epoch.
    #[must_use]
    pub const fn expires_at_unix_ms(&self) -> u64 {
        self.expires_at_unix_ms
    }

    /// Returns the opaque service cursor bytes.
    #[must_use]
    pub fn cursor(&self) -> &[u8] {
        &self.cursor
    }

    /// Encodes the checkpoint as version plus expiry plus cursor.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut output = Vec::with_capacity(14_usize.saturating_add(self.cursor.len()));
        output.extend_from_slice(&self.version.to_be_bytes());
        output.extend_from_slice(&self.expires_at_unix_ms.to_be_bytes());
        let cursor_len = u32::try_from(self.cursor.len()).unwrap_or(u32::MAX);
        output.extend_from_slice(&cursor_len.to_be_bytes());
        output.extend_from_slice(&self.cursor);
        output
    }

    /// Decodes checkpoint bytes, refusing bad lengths and versions.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError::InvalidNamespaceCheckpointEncoding`] for a bad
    /// length and [`ModelError::UnsupportedNamespaceCheckpointVersion`] for a
    /// foreign version.
    pub fn decode(bytes: &[u8]) -> Result<Self, ModelError> {
        if bytes.len() < 14 {
            return Err(ModelError::InvalidNamespaceCheckpointEncoding);
        }
        let version = u16::from_be_bytes([bytes[0], bytes[1]]);
        if version != NAMESPACE_CHECKPOINT_VERSION {
            return Err(ModelError::UnsupportedNamespaceCheckpointVersion { actual: version });
        }
        let expires_at_unix_ms = u64::from_be_bytes([
            bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7], bytes[8], bytes[9],
        ]);
        if expires_at_unix_ms == 0 {
            return Err(ModelError::InvalidNamespaceCheckpointEncoding);
        }
        let cursor_len = u32::from_be_bytes([bytes[10], bytes[11], bytes[12], bytes[13]]);
        let cursor_len = usize::try_from(cursor_len)
            .map_err(|_error| ModelError::InvalidNamespaceCheckpointEncoding)?;
        if cursor_len == 0 || cursor_len > MAX_NAMESPACE_CURSOR_BYTES {
            return Err(ModelError::InvalidNamespaceCheckpointEncoding);
        }
        if bytes.len() != 14_usize.saturating_add(cursor_len) {
            return Err(ModelError::InvalidNamespaceCheckpointEncoding);
        }
        Ok(Self {
            version,
            expires_at_unix_ms,
            cursor: bytes[14..].to_vec(),
        })
    }
}

/// Reports whether a namespace cursor passed its freshness horizon.
///
/// An expired cursor no longer addresses trusted log history and must take
/// the explicit full-recovery transition instead of a partial page.
#[must_use]
pub const fn is_namespace_checkpoint_expired(
    checkpoint: &NamespaceCheckpoint,
    now_unix_ms: u64,
) -> bool {
    now_unix_ms >= checkpoint.expires_at_unix_ms
}

/// Multiplexed per-namespace checkpoints sharing one durable slot.
///
/// One upstream binding keeps one checkpoint slot; this map partitions that
/// slot so every namespace carries its own cursor and expiry while the
/// binding stays singular. Encoding is canonical (namespaces sort by name)
/// and bounded to the slot limit, so it installs through the same atomic
/// page path as any other checkpoint.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(crate = "lgwks_std::json::serde")]
pub struct NamespaceCheckpoints {
    /// Per-namespace checkpoints in canonical name order.
    entries: BTreeMap<String, NamespaceCheckpoint>,
}

impl NamespaceCheckpoints {
    /// Creates an empty map for a peer with no namespace state yet.
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
        }
    }

    /// Returns whether the map holds no namespace.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns how many namespaces carry checkpoints.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns the checkpoint recorded for a namespace, if any.
    #[must_use]
    pub fn get(&self, namespace: &str) -> Option<&NamespaceCheckpoint> {
        self.entries.get(namespace)
    }

    /// Returns every namespaced checkpoint holder in canonical order.
    #[must_use]
    pub fn namespaces(&self) -> Vec<String> {
        self.entries.keys().cloned().collect()
    }

    /// Records a namespace checkpoint, replacing any previous entry.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError::InvalidIdentifierLength`] for an empty or
    /// oversized namespace.
    pub fn insert(
        &mut self,
        namespace: &str,
        checkpoint: NamespaceCheckpoint,
    ) -> Result<(), ModelError> {
        validate_namespace(namespace)?;
        self.entries.insert(namespace.to_owned(), checkpoint);
        Ok(())
    }

    /// Drops a namespace checkpoint, reporting whether one was stored.
    pub fn remove(&mut self, namespace: &str) -> bool {
        self.entries.remove(namespace).is_some()
    }

    /// Encodes the map as magic plus version plus canonical entries.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError::NamespaceCheckpointMapTooLarge`] when the map
    /// would not fit the single durable slot.
    pub fn encode(&self) -> Result<Vec<u8>, ModelError> {
        let mut output = Vec::new();
        output.extend_from_slice(&NAMESPACE_MAP_MAGIC);
        output.extend_from_slice(&NAMESPACE_CHECKPOINT_MAP_VERSION.to_be_bytes());
        let count = u32::try_from(self.entries.len()).unwrap_or(u32::MAX);
        output.extend_from_slice(&count.to_be_bytes());
        for (namespace, checkpoint) in &self.entries {
            let name_len = u32::try_from(namespace.len()).unwrap_or(u32::MAX);
            output.extend_from_slice(&name_len.to_be_bytes());
            output.extend_from_slice(namespace.as_bytes());
            let encoded = checkpoint.encode();
            let checkpoint_len = u32::try_from(encoded.len()).unwrap_or(u32::MAX);
            output.extend_from_slice(&checkpoint_len.to_be_bytes());
            output.extend_from_slice(&encoded);
        }
        if output.len() > MAX_NAMESPACE_MAP_BYTES {
            return Err(ModelError::NamespaceCheckpointMapTooLarge {
                actual: output.len(),
                maximum: MAX_NAMESPACE_MAP_BYTES,
            });
        }
        Ok(output)
    }

    /// Decodes map bytes, refusing foreign, truncated, or non-canonical input.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError::InvalidNamespaceCheckpointEncoding`] for a bad
    /// structure and [`ModelError::UnsupportedNamespaceCheckpointVersion`]
    /// for a foreign map version.
    pub fn decode(bytes: &[u8]) -> Result<Self, ModelError> {
        let mut cursor = 0_usize;
        if take(bytes, &mut cursor, NAMESPACE_MAP_MAGIC.len())? != NAMESPACE_MAP_MAGIC {
            return Err(ModelError::InvalidNamespaceCheckpointEncoding);
        }
        let version = take_u16(bytes, &mut cursor)?;
        if version != NAMESPACE_CHECKPOINT_MAP_VERSION {
            return Err(ModelError::UnsupportedNamespaceCheckpointVersion { actual: version });
        }
        let count = take_u32(bytes, &mut cursor)?;
        let mut entries = BTreeMap::new();
        for _ in 0..count {
            let namespace = take_namespace(bytes, &mut cursor)?;
            let checkpoint = take_checkpoint(bytes, &mut cursor)?;
            if entries.insert(namespace.to_owned(), checkpoint).is_some() {
                return Err(ModelError::InvalidNamespaceCheckpointEncoding);
            }
        }
        if cursor != bytes.len() {
            return Err(ModelError::InvalidNamespaceCheckpointEncoding);
        }
        Ok(Self { entries })
    }
}

/// Logical ownership scope independent from physical backend keys.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(crate = "lgwks_std::json::serde")]
pub struct Scope {
    /// Owning account identity.
    account: AccountId,
    /// Application space within the account.
    space: SpaceId,
}

impl Scope {
    /// Creates a scope from typed identifiers.
    #[must_use]
    pub const fn new(account: AccountId, space: SpaceId) -> Self {
        Self { account, space }
    }

    /// Returns the owning account.
    #[must_use]
    pub const fn account(self) -> AccountId {
        self.account
    }

    /// Returns the application space.
    #[must_use]
    pub const fn space(self) -> SpaceId {
        self.space
    }
}

/// Effects understood by the core authorization boundary.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(crate = "lgwks_std::json::serde")]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Effect {
    /// Read materialized document state.
    Read,
    /// Commit a local or replicated document change.
    Write,
    /// Read a bounded query or change page.
    Scan,
    /// Manage synchronization state for the scope.
    Sync,
}

/// Minimal authority result accepted by the core.
///
/// A host authority adapter may produce this value, but no verifier runtime
/// type crosses the model or storage boundary.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(crate = "lgwks_std::json::serde")]
pub struct AuthorityDecision {
    /// Scope the decision authorizes.
    scope: Scope,
    /// Effect the decision authorizes.
    effect: Effect,
    /// Canonical digest of the requested logical mutation.
    operation: OperationDigest,
    /// Exclusive expiry in milliseconds since the Unix epoch.
    expires_at_unix_ms: u64,
    /// Audit identity assigned by the authority adapter.
    decision_id: String,
}

impl AuthorityDecision {
    /// Builds a bounded decision after the host has authenticated its source.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError::InvalidIdentifierLength`] for an empty or
    /// oversized decision identity and [`ModelError::InvalidAuthorityExpiry`]
    /// for the reserved zero expiry.
    pub fn new(
        scope: Scope,
        effect: Effect,
        operation: OperationDigest,
        expires_at_unix_ms: u64,
        decision_id: impl Into<String>,
    ) -> Result<Self, ModelError> {
        let decision_id = decision_id.into();
        if decision_id.is_empty() || decision_id.len() > MAX_NAME_BYTES {
            return Err(ModelError::InvalidIdentifierLength {
                kind: "authority decision",
                context: "AuthorityDecision::new",
                actual: decision_id.len(),
                maximum: MAX_NAME_BYTES,
            });
        }
        if expires_at_unix_ms == 0 {
            return Err(ModelError::InvalidAuthorityExpiry);
        }
        Ok(Self {
            scope,
            effect,
            operation,
            expires_at_unix_ms,
            decision_id,
        })
    }

    /// Checks exact scope, effect, request digest, and exclusive expiry.
    #[must_use]
    pub fn permits(
        &self,
        scope: Scope,
        effect: Effect,
        operation: OperationDigest,
        now_unix_ms: u64,
    ) -> bool {
        self.scope == scope
            && self.effect == effect
            && self.operation == operation
            && now_unix_ms < self.expires_at_unix_ms
    }

    /// Returns the audit identity assigned by the authority adapter.
    #[must_use]
    pub fn decision_id(&self) -> &str {
        &self.decision_id
    }
}

/// Stable model and validation failures.
#[derive(Debug, Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum ModelError {
    /// A caller-controlled identifier violated its encoded bound.
    #[error("{context}: {kind} length {actual} is outside the maximum {maximum}")]
    InvalidIdentifierLength {
        /// Semantic identifier class.
        kind: &'static str,
        /// Boundary that rejected the identifier.
        context: &'static str,
        /// Observed byte count.
        actual: usize,
        /// Accepted byte count.
        maximum: usize,
    },
    /// An authority decision used a sentinel rather than a real expiry.
    #[error("authority decision expiry must be nonzero")]
    InvalidAuthorityExpiry,
    /// The supplied decision did not cover an operation.
    #[error("authority decision does not cover the requested scope and effect")]
    AuthorityDenied,
    /// One encoded change exceeded the bounded envelope.
    #[error("change length {actual} exceeds maximum {maximum}")]
    ChangeTooLarge {
        /// Observed encoded byte count.
        actual: usize,
        /// Accepted encoded byte count.
        maximum: usize,
    },
    /// A protocol page exceeded a count or byte limit.
    #[error("page exceeds bounded protocol limits")]
    PageTooLarge,
    /// A snapshot baseline record violated its fixed versioned encoding.
    #[error("snapshot baseline encoding is malformed")]
    InvalidSnapshotEncoding,
    /// A snapshot baseline record used a foreign layout version.
    #[error("snapshot baseline version {actual} is unsupported")]
    UnsupportedSnapshotVersion {
        /// Observed baseline layout version.
        actual: u16,
    },
    /// The artifact exceeded the document count bound.
    #[error("snapshot contains too many documents")]
    SnapshotTooManyDocuments,
    /// Document identifiers were not strictly ascending and unique.
    #[error("snapshot document ordering or uniqueness is invalid")]
    SnapshotDocumentsUnordered,
    /// A document snapshot violated its per-document bound.
    #[error("snapshot document is empty or exceeds its bound")]
    SnapshotDocumentInvalid,
    /// The aggregate snapshot payload exceeded its byte bound.
    #[error("snapshot exceeds its aggregate byte bound")]
    SnapshotTooLarge,
    /// Descriptor metadata or digest did not match the artifact documents.
    #[error("snapshot descriptor does not match its documents")]
    SnapshotDescriptorMismatch,
    /// A namespace checkpoint record violated its versioned encoding.
    #[error("namespace checkpoint encoding is malformed")]
    InvalidNamespaceCheckpointEncoding,
    /// A namespace checkpoint record used a foreign layout version.
    #[error("namespace checkpoint version {actual} is unsupported")]
    UnsupportedNamespaceCheckpointVersion {
        /// Observed checkpoint layout version.
        actual: u16,
    },
    /// A multiplexed namespace checkpoint map exceeded the durable slot bound.
    #[error("namespace checkpoint map length {actual} exceeds maximum {maximum}")]
    NamespaceCheckpointMapTooLarge {
        /// Observed encoded byte count.
        actual: usize,
        /// Accepted encoded byte count.
        maximum: usize,
    },
    /// A capability profile could not satisfy a required guarantee.
    #[error(
        "backend class {class} does not provide {} required guarantees",
        .missing.len()
    )]
    UnsupportedGuarantees {
        /// Declared backend class that failed the request.
        class: BackendClass,
        /// Every required guarantee the class does not provide, in request order.
        missing: Vec<Capability>,
    },
    /// A capability profile identity violated its fixed versioned encoding.
    #[error("capability profile length {actual} is not {CAPABILITY_PROFILE_BYTES}")]
    InvalidCapabilityProfileEncoding {
        /// Observed encoded byte count.
        actual: usize,
    },
    /// A capability profile identity used a foreign layout version.
    #[error("capability profile version {actual} is unsupported")]
    UnsupportedCapabilityProfileVersion {
        /// Observed profile layout version.
        actual: u16,
    },
    /// A capability profile identity named an unknown backend class.
    #[error("backend class discriminant {actual} is unsupported")]
    UnsupportedBackendClass {
        /// Observed class discriminant.
        actual: u8,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_round_trip_as_typed_uuid_values() {
        let id = DocumentId::new();
        assert_eq!(DocumentId::from_uuid(id.as_uuid()), id);
    }

    #[test]
    fn authority_is_exactly_scope_bound() -> Result<(), Box<dyn std::error::Error>> {
        let account = AccountId::new();
        let first = Scope::new(account, SpaceId::new());
        let second = Scope::new(account, SpaceId::new());
        let operation = OperationDigest::new([7; 32]);
        let decision = AuthorityDecision::new(first, Effect::Read, operation, 100, "local-policy")?;
        assert!(decision.permits(first, Effect::Read, operation, 99));
        assert!(!decision.permits(first, Effect::Write, operation, 99));
        assert!(!decision.permits(second, Effect::Read, operation, 99));
        assert!(!decision.permits(first, Effect::Read, OperationDigest::new([8; 32]), 99));
        assert!(!decision.permits(first, Effect::Read, operation, 100));
        assert_eq!(decision.decision_id(), "local-policy");
        Ok(())
    }

    #[test]
    fn revision_digest_and_resource_values_preserve_every_exact_bit() {
        assert_eq!(MAX_PAGE_BYTES, 8_388_608);
        let revision = DocumentRevision::new(41);
        assert_eq!(revision.get(), 41);
        assert_eq!(revision.next(), Some(DocumentRevision::new(42)));
        assert_eq!(DocumentRevision::new(u64::MAX).next(), None);
        let change = ChangeDigest::new([17; 32]);
        let operation = OperationDigest::new([29; 32]);
        assert_eq!(change.into_bytes(), [17; 32]);
        assert_eq!(operation.into_bytes(), [29; 32]);
    }

    #[test]
    fn snapshot_baseline_codec_is_versioned_and_fail_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let account = AccountId::new();
        let space = SpaceId::new();
        let scope = Scope::new(account, space);
        let artifact = SnapshotArtifact::new(
            scope,
            41,
            vec![SnapshotDocument {
                document: DocumentId::new(),
                snapshot: vec![1, 2],
                tombstone: false,
            }],
        )?;
        let baseline = artifact.baseline.clone();
        assert_eq!(baseline.version(), SNAPSHOT_BASELINE_VERSION);
        assert_eq!(baseline.position(), 41);
        assert_eq!(
            SnapshotBaseline::decode(&baseline.encode()),
            Ok(SnapshotRecord::State(baseline))
        );
        assert_eq!(
            SnapshotBaseline::decode(&[0, 1, 0, 0, 0, 0, 0, 0, 0, 41]),
            Ok(SnapshotRecord::LegacyPosition(41))
        );
        assert_eq!(
            SnapshotBaseline::decode(&[0; SNAPSHOT_BASELINE_BYTES]),
            Err(ModelError::UnsupportedSnapshotVersion { actual: 0 })
        );
        let mut too_many = artifact.baseline.encode();
        too_many[42..46].copy_from_slice(
            &(u32::try_from(MAX_BASELINE_DOCUMENTS).unwrap_or(u32::MAX) + 1).to_be_bytes(),
        );
        assert_eq!(
            SnapshotBaseline::decode(&too_many),
            Err(ModelError::SnapshotTooManyDocuments)
        );
        let mut too_large = artifact.baseline.encode();
        too_large[46..50].copy_from_slice(
            &(u32::try_from(MAX_BASELINE_BYTES).unwrap_or(u32::MAX) + 1).to_be_bytes(),
        );
        assert_eq!(
            SnapshotBaseline::decode(&too_large),
            Err(ModelError::SnapshotTooLarge)
        );
        assert!(artifact.validate(scope).is_ok());
        Ok(())
    }

    #[test]
    fn retention_floor_predicates_gate_prune_and_recovery() {
        assert!(is_behind_retention(0, 1));
        assert!(!is_behind_retention(1, 1));
        assert!(!is_behind_retention(2, 1));
        assert!(retention_floor_valid(0, 0));
        assert!(retention_floor_valid(2, 3));
        assert!(!retention_floor_valid(4, 3));
    }

    #[test]
    fn namespace_checkpoint_codec_round_trips_and_fails_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let checkpoint = NamespaceCheckpoint::new(vec![9, 8, 7], 1_000)?;
        assert_eq!(checkpoint.version(), NAMESPACE_CHECKPOINT_VERSION);
        assert_eq!(checkpoint.expires_at_unix_ms(), 1_000);
        assert_eq!(checkpoint.cursor(), &[9, 8, 7]);
        let encoded = checkpoint.encode();
        assert_eq!(NamespaceCheckpoint::decode(&encoded), Ok(checkpoint));
        assert_eq!(
            NamespaceCheckpoint::decode(&[]),
            Err(ModelError::InvalidNamespaceCheckpointEncoding)
        );
        assert_eq!(
            NamespaceCheckpoint::decode(&encoded[..encoded.len() - 1]),
            Err(ModelError::InvalidNamespaceCheckpointEncoding)
        );
        let mut foreign_version = encoded.clone();
        foreign_version[0..2].copy_from_slice(&(NAMESPACE_CHECKPOINT_VERSION + 1).to_be_bytes());
        assert_eq!(
            NamespaceCheckpoint::decode(&foreign_version),
            Err(ModelError::UnsupportedNamespaceCheckpointVersion {
                actual: NAMESPACE_CHECKPOINT_VERSION + 1,
            })
        );
        let mut trailing = encoded;
        trailing.push(0);
        assert_eq!(
            NamespaceCheckpoint::decode(&trailing),
            Err(ModelError::InvalidNamespaceCheckpointEncoding)
        );
        assert!(matches!(
            NamespaceCheckpoint::new(Vec::new(), 1),
            Err(ModelError::InvalidIdentifierLength {
                kind: "namespace cursor",
                ..
            })
        ));
        assert!(matches!(
            NamespaceCheckpoint::new(vec![0; MAX_NAMESPACE_CURSOR_BYTES + 1], 1),
            Err(ModelError::InvalidIdentifierLength { .. })
        ));
        assert_eq!(
            NamespaceCheckpoint::new(vec![1], 0),
            Err(ModelError::InvalidAuthorityExpiry)
        );
        Ok(())
    }

    #[test]
    fn namespace_checkpoint_expiry_is_exclusive() -> Result<(), Box<dyn std::error::Error>> {
        let checkpoint = NamespaceCheckpoint::new(vec![1], 100)?;
        assert!(!is_namespace_checkpoint_expired(&checkpoint, 99));
        assert!(is_namespace_checkpoint_expired(&checkpoint, 100));
        assert!(is_namespace_checkpoint_expired(&checkpoint, 101));
        Ok(())
    }

    #[test]
    fn namespace_policy_validation_mirrors_page_bounds() -> Result<(), Box<dyn std::error::Error>> {
        let policy = NamespacePolicy::new(128, 4 * 1_048_576, 16, NetworkCondition::Online)?;
        assert_eq!(policy.changes_per_page(), 128);
        assert_eq!(policy.bytes_per_page(), 4 * 1_048_576);
        assert_eq!(policy.pages_per_cycle(), 16);
        assert_eq!(policy.network(), NetworkCondition::Online);
        assert_eq!(NamespacePolicy::default(), policy);
        for invalid in [
            NamespacePolicy::new(0, 1_024, 1, NetworkCondition::Online),
            NamespacePolicy::new(
                u16::try_from(MAX_CHANGES_PER_PAGE).unwrap_or(u16::MAX) + 1,
                1_024,
                1,
                NetworkCondition::Online,
            ),
            NamespacePolicy::new(1, 0, 1, NetworkCondition::Online),
            NamespacePolicy::new(
                1,
                u32::try_from(MAX_PAGE_BYTES).unwrap_or(u32::MAX) + 1,
                1,
                NetworkCondition::Online,
            ),
            NamespacePolicy::new(1, 1_024, 0, NetworkCondition::Online),
            NamespacePolicy::new(
                1,
                1_024,
                MAX_NAMESPACE_PAGES_PER_CYCLE + 1,
                NetworkCondition::Online,
            ),
        ] {
            assert_eq!(invalid, Err(ModelError::PageTooLarge));
        }
        Ok(())
    }

    #[test]
    fn namespace_map_partitions_checkpoints_within_one_binding()
    -> Result<(), Box<dyn std::error::Error>> {
        assert!(matches!(
            validate_namespace(""),
            Err(ModelError::InvalidIdentifierLength {
                kind: "namespace",
                ..
            })
        ));
        assert!(validate_namespace("sessions").is_ok());
        let mut map = NamespaceCheckpoints::new();
        assert!(map.is_empty());
        assert_eq!(map.len(), 0);
        map.insert("hot", NamespaceCheckpoint::new(vec![1], 100)?)?;
        map.insert("cold", NamespaceCheckpoint::new(vec![2], 200)?)?;
        assert_eq!(map.len(), 2);
        assert_eq!(map.namespaces(), vec!["cold".to_owned(), "hot".to_owned()]);
        let encoded = map.encode()?;
        assert!(!encoded.is_empty());
        assert!(encoded.len() <= MAX_NAMESPACE_MAP_BYTES);
        let decoded = NamespaceCheckpoints::decode(&encoded)?;
        assert_eq!(
            decoded.get("hot").map(NamespaceCheckpoint::cursor),
            Some(&[1][..])
        );
        assert_eq!(
            decoded.get("cold").map(NamespaceCheckpoint::cursor),
            Some(&[2][..])
        );
        let mut advanced = decoded;
        advanced.insert("hot", NamespaceCheckpoint::new(vec![3], 300)?)?;
        assert_eq!(
            advanced.get("hot").map(NamespaceCheckpoint::cursor),
            Some(&[3][..])
        );
        assert_eq!(
            advanced.get("cold").map(NamespaceCheckpoint::cursor),
            Some(&[2][..])
        );
        assert!(advanced.remove("cold"));
        assert!(!advanced.remove("cold"));
        assert!(advanced.get("cold").is_none());
        let empty_bytes = NamespaceCheckpoints::new().encode()?;
        assert!(!empty_bytes.is_empty());
        assert!(NamespaceCheckpoints::decode(&empty_bytes)?.is_empty());
        assert_eq!(
            NamespaceCheckpoints::decode(b"legacy-global-cursor"),
            Err(ModelError::InvalidNamespaceCheckpointEncoding)
        );
        assert_eq!(
            NamespaceCheckpoints::decode(&[]),
            Err(ModelError::InvalidNamespaceCheckpointEncoding)
        );
        let probe = NamespaceCheckpoint::new(vec![1], 1)?;
        assert!(matches!(
            map.insert("", probe),
            Err(ModelError::InvalidIdentifierLength { .. })
        ));
        let mut oversized = NamespaceCheckpoints::new();
        for name in ["first", "second"] {
            oversized.insert(
                name,
                NamespaceCheckpoint::new(vec![7; MAX_NAMESPACE_CURSOR_BYTES], u64::MAX)?,
            )?;
        }
        assert!(matches!(
            oversized.encode(),
            Err(ModelError::NamespaceCheckpointMapTooLarge { .. })
        ));
        Ok(())
    }
}
