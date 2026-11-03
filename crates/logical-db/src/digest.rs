//! Content digests for the put-value sugar.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use logical_db_crdt::Scalar;
use logical_db_model::{DocumentId, OperationDigest};

/// Derives one operation request digest from canonical content bytes.
///
/// The digest is BLAKE3 through the estate substrate `lgwks_std::hash` over
/// the document identity, the property key, and a tagged scalar encoding, so
/// equal content yields equal request identity and any content difference
/// yields a different digest. This sugar starts at BLAKE3 rather than the
/// workspace's legacy SHA-2 surfaces: new identity code follows the estate
/// dependency doctrine, and legacy v1 digest surfaces stay versioned until
/// their own migration.
#[must_use]
pub fn content_digest(document: DocumentId, key: &str, value: &Scalar) -> OperationDigest {
    OperationDigest::new(
        *lgwks_std::hash::blake3(&canonical_value_bytes(document, key, value)).as_bytes(),
    )
}

/// Canonical value bytes
fn canonical_value_bytes(document: DocumentId, key: &str, value: &Scalar) -> Vec<u8> {
    /// Push bounded field
    fn push_bounded_field(bytes: &mut Vec<u8>, field: &[u8]) {
        if let Ok(length) = u32::try_from(field.len()) {
            bytes.extend_from_slice(&length.to_be_bytes());
            bytes.extend_from_slice(field);
            return;
        }
        bytes.extend_from_slice(&u32::MAX.to_be_bytes());
        if let Ok(width) = usize::try_from(u32::MAX) {
            let take = width.min(field.len());
            bytes.extend_from_slice(&field[..take]);
        }
    }
    let mut bytes = Vec::new();
    bytes.extend_from_slice(document.as_uuid().as_bytes());
    push_bounded_field(&mut bytes, key.as_bytes());
    match *value {
        Scalar::Null => bytes.push(0),
        Scalar::Boolean(inner) => {
            bytes.push(1);
            bytes.push(u8::from(inner));
        }
        Scalar::Integer(inner) => {
            bytes.push(2);
            bytes.extend_from_slice(&inner.to_be_bytes());
        }
        Scalar::Unsigned(inner) => {
            bytes.push(3);
            bytes.extend_from_slice(&inner.to_be_bytes());
        }
        Scalar::Number(inner) => {
            bytes.push(4);
            bytes.extend_from_slice(&inner.to_bits().to_be_bytes());
        }
        Scalar::String(ref inner) => {
            bytes.push(5);
            push_bounded_field(&mut bytes, inner.as_bytes());
        }
        Scalar::Bytes(ref inner) => {
            bytes.push(6);
            push_bounded_field(&mut bytes, inner);
        }
        Scalar::Counter(inner) => {
            bytes.push(7);
            bytes.extend_from_slice(&inner.to_be_bytes());
        }
        Scalar::Timestamp(inner) => {
            bytes.push(8);
            bytes.extend_from_slice(&inner.to_be_bytes());
        }
        Scalar::Unknown {
            type_code,
            bytes: ref inner,
        } => {
            bytes.push(9);
            bytes.push(type_code);
            push_bounded_field(&mut bytes, inner);
        }
        // Non_exhaustive scalar: an unrecognized form cannot contribute to a
        // content digest under an existing tag, so fail closed with a distinct
        // tag rather than colliding with a known encoding.
        _ => bytes.push(u8::MAX),
    }
    bytes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_digest_is_deterministic_and_content_sensitive()
    -> Result<(), Box<dyn std::error::Error>> {
        let document = DocumentId::new();
        let first = content_digest(document, "key", &Scalar::String("v".into()));
        let again = content_digest(document, "key", &Scalar::String("v".into()));
        assert_eq!(first, again);
        assert_ne!(
            first,
            content_digest(document, "key", &Scalar::String("w".into()))
        );
        assert_ne!(
            first,
            content_digest(document, "other", &Scalar::String("v".into()))
        );
        assert_ne!(
            first,
            content_digest(DocumentId::new(), "key", &Scalar::String("v".into()))
        );
        Ok(())
    }
}
