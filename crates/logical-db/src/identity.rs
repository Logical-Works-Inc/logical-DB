//! Durable single-identity sidecar for library-managed replicas.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use super::error::DatabaseError;

/// Decodes one model identifier from its sixteen network-order bytes through
/// the model's Deserialize contract, without naming the rival UUID crate.
///
/// The bytes ride a quoted RFC 9562 hyphenated JSON string (the only text
/// form `lgwks_std::id::Uuid` 5 accepts) into the estate JSON decoder; the
/// target type is inferred at each call site. The identity
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
    let corrupt = |detail: String| {
        DatabaseError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("logical-db identity bytes are corrupt: {detail}"),
        ))
    };
    let octets = <[u8; 16]>::try_from(bytes)
        .map_err(|error| corrupt(format!("{} bytes, expected 16: {error}", bytes.len())))?;
    let text = format!("\"{}\"", lgwks_std::id::Uuid::from(octets));
    lgwks_std::json::from_str(&text).map_err(|error| corrupt(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::decode_identity_id;
    use logical_db_model::DocumentId;

    /// RFC 9562 §4: the sixteen network-order bytes are the identity, so a
    /// decode followed by `as_uuid` must hand back the same bytes, and any
    /// other length is refused rather than padded or truncated.
    #[test]
    fn sixteen_bytes_round_trip_and_other_lengths_are_refused() -> Result<(), String> {
        let bytes = 0x0190_0000_0000_7000_8000_0000_0000_0001_u128.to_be_bytes();
        let document =
            decode_identity_id::<DocumentId>(&bytes).map_err(|error| error.to_string())?;
        assert_eq!(
            *document.as_uuid().as_bytes(),
            bytes,
            "the decoded identity must keep its bytes"
        );
        assert!(
            decode_identity_id::<DocumentId>(&[0_u8; 15]).is_err(),
            "fifteen bytes must be refused"
        );
        assert!(
            decode_identity_id::<DocumentId>(&[0_u8; 17]).is_err(),
            "seventeen bytes must be refused"
        );
        Ok(())
    }
}
