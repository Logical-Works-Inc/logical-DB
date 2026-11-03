//! Durable single-identity sidecar for library-managed replicas.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use super::error::DatabaseError;

/// Decodes one model identifier from its sixteen network-order bytes through
/// the model's Deserialize contract, without naming the rival UUID crate.
///
/// The bytes ride a quoted simple-hex JSON string into the estate JSON
/// decoder; the target type is inferred at each call site. The identity
/// sidecar and every layer above the facade (the key-value cache's derived
/// document, operation and index identities) build identifiers here, so there
/// is one byte-to-identity path above the store.
///
/// # Errors
///
/// Returns [`DatabaseError::Io`] with `InvalidData` when the bytes do not
/// decode as the requested identifier.
pub fn decode_identity_id<T>(bytes: &[u8]) -> Result<T, DatabaseError>
where
    T: for<'de> lgwks_std::json::Deserialize<'de>,
{
    let text = format!("\"{}\"", lgwks_std::hex::encode(bytes));
    lgwks_std::json::from_str(&text).map_err(|error| {
        DatabaseError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("logical-db identity bytes are corrupt: {error}"),
        ))
    })
}
