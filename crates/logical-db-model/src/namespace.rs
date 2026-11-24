//! Namespace sync policy and bounded per-namespace checkpoints.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use std::collections::BTreeMap;

use lgwks_std::json::{Deserialize, Serialize};

use crate::namespace_wire::{take, take_checkpoint, take_namespace, take_u16, take_u32, take_u64};
use crate::{ModelError, validate_namespace};

/// Maximum opaque service cursor bytes carried by one namespace checkpoint.
///
/// This matches the protocol and store checkpoint bounds so every cursor a
/// namespace records stays acceptable to the pull request and to the durable
/// slot that multiplexes it.
const MAX_NAMESPACE_CURSOR_BYTES: usize = 4_096;
/// Maximum encoded bytes of one multiplexed namespace checkpoint map.
///
/// The map shares the single durable checkpoint slot bound to one upstream,
/// so the whole multiplex must fit where one global cursor fits.
const MAX_NAMESPACE_MAP_BYTES: usize = 4_096;
/// Layout version of one encoded namespace checkpoint.
const NAMESPACE_CHECKPOINT_VERSION: u16 = 1;
/// Layout version of one encoded namespace checkpoint map.
const NAMESPACE_CHECKPOINT_MAP_VERSION: u16 = 1;
/// Magic prefix rejecting foreign slot bytes before version checks.
const NAMESPACE_MAP_MAGIC: [u8; 6] = *b"LDBNS1";
/// Fixed encoded bytes before the cursor: version, expiry, cursor length.
const CHECKPOINT_HEADER_BYTES: usize = 14;

/// Narrows an encoded length to its `u32` wire field, or reports `error`.
fn length_field(length: usize, error: ModelError) -> Result<u32, ModelError> {
    let Ok(field) = u32::try_from(length) else {
        return Err(error);
    };
    Ok(field)
}

/// The error for a multiplexed map whose encoding passed the durable slot.
const fn map_too_large(actual: usize) -> ModelError {
    ModelError::NamespaceCheckpointMapTooLarge {
        actual,
        maximum: MAX_NAMESPACE_MAP_BYTES,
    }
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
    ///
    /// # Errors
    ///
    /// Returns [`ModelError::InvalidIdentifierLength`] when the cursor length
    /// does not fit its wire field.
    pub fn encode(&self) -> Result<Vec<u8>, ModelError> {
        let cursor_len = length_field(
            self.cursor.len(),
            ModelError::InvalidIdentifierLength {
                kind: "namespace cursor",
                context: "NamespaceCheckpoint::encode",
                actual: self.cursor.len(),
                maximum: MAX_NAMESPACE_CURSOR_BYTES,
            },
        )?;
        let mut output =
            Vec::with_capacity(CHECKPOINT_HEADER_BYTES.saturating_add(self.cursor.len()));
        output.extend_from_slice(&self.version.to_be_bytes());
        output.extend_from_slice(&self.expires_at_unix_ms.to_be_bytes());
        output.extend_from_slice(&cursor_len.to_be_bytes());
        output.extend_from_slice(&self.cursor);
        Ok(output)
    }

    /// Decodes checkpoint bytes, refusing bad lengths and versions.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError::InvalidNamespaceCheckpointEncoding`] for a bad
    /// length and [`ModelError::UnsupportedNamespaceCheckpointVersion`] for a
    /// foreign version.
    pub fn decode(bytes: &[u8]) -> Result<Self, ModelError> {
        let mut position = 0_usize;
        let version = take_u16(bytes, &mut position)?;
        if version != NAMESPACE_CHECKPOINT_VERSION {
            return Err(ModelError::UnsupportedNamespaceCheckpointVersion { actual: version });
        }
        let expires_at_unix_ms = take_u64(bytes, &mut position)?;
        if expires_at_unix_ms == 0 {
            return Err(ModelError::InvalidNamespaceCheckpointEncoding);
        }
        let cursor_len = take_u32(bytes, &mut position)?;
        if cursor_len == 0 || cursor_len > MAX_NAMESPACE_CURSOR_BYTES {
            return Err(ModelError::InvalidNamespaceCheckpointEncoding);
        }
        let cursor = take(bytes, &mut position, cursor_len)?;
        if position != bytes.len() {
            return Err(ModelError::InvalidNamespaceCheckpointEncoding);
        }
        Ok(Self {
            version,
            expires_at_unix_ms,
            cursor: cursor.to_vec(),
        })
    }
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
        let count = length_field(self.entries.len(), map_too_large(self.entries.len()))?;
        output.extend_from_slice(&count.to_be_bytes());
        for (namespace, checkpoint) in &self.entries {
            let name_len = length_field(namespace.len(), map_too_large(namespace.len()))?;
            output.extend_from_slice(&name_len.to_be_bytes());
            output.extend_from_slice(namespace.as_bytes());
            let encoded = checkpoint.encode()?;
            let checkpoint_len = length_field(encoded.len(), map_too_large(encoded.len()))?;
            output.extend_from_slice(&checkpoint_len.to_be_bytes());
            output.extend_from_slice(&encoded);
        }
        if output.len() > MAX_NAMESPACE_MAP_BYTES {
            return Err(map_too_large(output.len()));
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespace_checkpoint_codec_round_trips_and_fails_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let checkpoint = NamespaceCheckpoint::new(vec![9, 8, 7], 1_000)?;
        assert_eq!(checkpoint.version(), NAMESPACE_CHECKPOINT_VERSION);
        assert_eq!(checkpoint.expires_at_unix_ms(), 1_000);
        assert_eq!(checkpoint.cursor(), &[9, 8, 7]);
        let encoded = checkpoint.encode()?;
        assert_eq!(NamespaceCheckpoint::decode(&encoded), Ok(checkpoint));
        assert_eq!(
            NamespaceCheckpoint::decode(&[]),
            Err(ModelError::InvalidNamespaceCheckpointEncoding)
        );
        let (_, truncated) = encoded
            .split_last()
            .ok_or(ModelError::InvalidNamespaceCheckpointEncoding)?;
        assert_eq!(
            NamespaceCheckpoint::decode(truncated),
            Err(ModelError::InvalidNamespaceCheckpointEncoding)
        );
        let foreign = NAMESPACE_CHECKPOINT_VERSION.saturating_add(1);
        let mut foreign_version = foreign.to_be_bytes().to_vec();
        foreign_version.extend_from_slice(
            encoded
                .get(2..)
                .ok_or(ModelError::InvalidNamespaceCheckpointEncoding)?,
        );
        assert_eq!(
            NamespaceCheckpoint::decode(&foreign_version),
            Err(ModelError::UnsupportedNamespaceCheckpointVersion { actual: foreign })
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
            NamespaceCheckpoint::new(vec![0; MAX_NAMESPACE_CURSOR_BYTES.saturating_add(1)], 1),
            Err(ModelError::InvalidIdentifierLength { .. })
        ));
        assert_eq!(
            NamespaceCheckpoint::new(vec![1], 0),
            Err(ModelError::InvalidAuthorityExpiry)
        );
        Ok(())
    }

    #[test]
    fn namespace_map_partitions_checkpoints_within_one_binding()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut map = NamespaceCheckpoints::new();
        assert!(map.is_empty());
        assert_eq!(map.len(), 0);
        map.insert("hot", NamespaceCheckpoint::new(vec![1], 100)?)?;
        map.insert("cold", NamespaceCheckpoint::new(vec![2], 200)?)?;
        assert_eq!(map.len(), 2);
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
