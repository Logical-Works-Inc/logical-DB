//! Owns content addressing: the sha256 content id of a payload. This
//! is the identity primitive later phases use for dedup and for
//! claim-class provenance rows.

use sha2::{Digest, Sha256};

// ── Content ids ──────────────────────────────────────────────

/// sha256 over `payload`; stable across processes and machines.
pub fn content_id(payload: &[u8]) -> [u8; 32] {
    Sha256::digest(payload).into()
}

// ── Tests ────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_id_matches_the_sha256_of_the_payload() {
        // Known vector: sha256("hello") per FIPS 180-4.
        assert_eq!(
            hex(content_id(b"hello")),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }

    #[test]
    fn empty_payload_has_a_distinct_content_id() {
        assert_ne!(content_id(b""), [0u8; 32]);
    }

    fn hex(bytes: [u8; 32]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }
}
