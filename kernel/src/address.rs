//! Owns content addressing: the BLAKE3 content id of a payload. This
//! is the identity primitive later phases use for dedup and for
//! claim-class provenance rows.

use lgwks_std::hash::{self, Digest};

// ── Content ids ──────────────────────────────────────────────

/// BLAKE3 over `payload`; 32 bytes, stable across processes and
/// machines, SIMD-backed so content hashing never becomes the
/// bottleneck of an ingest path.
pub fn content_id(payload: &[u8]) -> Digest {
    hash::blake3(payload)
}

// ── Tests ────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_id_matches_the_official_blake3_vector() {
        assert_eq!(
            content_id(b"").to_hex(),
            "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"
        );
    }

    #[test]
    fn distinct_payloads_get_distinct_content_ids() {
        assert_ne!(content_id(b"hello"), content_id(b"hellp"));
    }

    #[test]
    fn content_id_is_deterministic_across_calls() {
        assert_eq!(content_id(b"stable"), content_id(b"stable"));
    }
}
