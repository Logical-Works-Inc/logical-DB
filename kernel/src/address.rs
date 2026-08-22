//! Owns content addressing: the BLAKE3 content id of a payload. This
//! is the identity primitive later phases use for dedup and for
//! claim-class provenance rows.

// ── Content ids ──────────────────────────────────────────────

/// BLAKE3 over `payload`; 32 bytes, stable across processes and
/// machines, SIMD-backed so content hashing never becomes the
/// bottleneck of an ingest path.
pub fn content_id(payload: &[u8]) -> [u8; 32] {
    blake3::hash(payload).into()
}

// ── Tests ────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_id_matches_the_official_blake3_vector() {
        // Official BLAKE3 test vector (test_vectors.json, input len 0).
        assert_eq!(
            hex(content_id(b"")),
            "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"
        );
    }

    #[test]
    fn distinct_payloads_get_distinct_content_ids() {
        assert_ne!(content_id(b"hello"), content_id(b"hellp"));
        assert_ne!(content_id(b""), [0u8; 32]);
    }

    #[test]
    fn content_id_is_deterministic_across_calls() {
        assert_eq!(content_id(b"stable"), content_id(b"stable"));
    }

    fn hex(bytes: [u8; 32]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }
}
