//! Serde wire format of the estate UUID identifiers.
//!
//! The format is the one the `uuid` crate wrote before the estate type
//! replaced it, so stored JSON and CBOR still decode: human-readable formats
//! carry the canonical hyphenated string, binary formats carry sixteen raw
//! bytes.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use core::fmt;

use lgwks_std::id::Uuid;
use lgwks_std::json::serde::de::{self, SeqAccess, Visitor};
use lgwks_std::json::serde::{Deserializer, Serializer};

/// Encoded byte length of one identifier.
const IDENTIFIER_BYTES: usize = 16;

/// Serializes one identifier as a string or as sixteen raw bytes.
pub(crate) fn serialize<S: Serializer>(uuid: &Uuid, serializer: S) -> Result<S::Ok, S::Error> {
    if serializer.is_human_readable() {
        serializer.serialize_str(&uuid.to_string())
    } else {
        serializer.serialize_bytes(uuid.as_bytes())
    }
}

/// Deserializes one identifier from a string, bytes, or a sixteen-element sequence.
pub(crate) fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Uuid, D::Error> {
    if deserializer.is_human_readable() {
        deserializer.deserialize_any(IdentifierVisitor)
    } else {
        deserializer.deserialize_bytes(IdentifierVisitor)
    }
}

/// Accepts every shape the previous wire format could carry.
#[derive(Debug)]
struct IdentifierVisitor;

impl<'de> Visitor<'de> for IdentifierVisitor {
    type Value = Uuid;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a UUID string or sixteen bytes")
    }

    fn visit_str<E: de::Error>(self, value: &str) -> Result<Uuid, E> {
        Uuid::parse(value).map_err(E::custom)
    }

    fn visit_bytes<E: de::Error>(self, value: &[u8]) -> Result<Uuid, E> {
        let Ok(bytes) = <[u8; IDENTIFIER_BYTES]>::try_from(value) else {
            return Err(E::invalid_length(value.len(), &self));
        };
        Ok(Uuid::from(bytes))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Uuid, A::Error> {
        let mut bytes = [0_u8; IDENTIFIER_BYTES];
        for (index, slot) in bytes.iter_mut().enumerate() {
            let Some(byte) = seq.next_element::<u8>()? else {
                return Err(de::Error::invalid_length(index, &self));
            };
            *slot = byte;
        }
        if seq.next_element::<u8>()?.is_some() {
            return Err(de::Error::invalid_length(
                IDENTIFIER_BYTES.saturating_add(1),
                &self,
            ));
        }
        Ok(Uuid::from(bytes))
    }
}
