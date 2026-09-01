//! Braid-owned content identity at the Logical-DB boundary.
//!
//! Logical-DB deliberately does not define a digest/CID type or unframed hash
//! algorithm. Callers supply their domain and receive Braid's canonical `Cid`.

pub use braid_ir::Cid;

/// Domain-separated, length-framed content identity from the constellation's
/// sole CID authority. Domain ownership remains with the calling layer.
pub fn content_id(domain: &[u8], payload: &[u8]) -> Cid {
    Cid::compute(domain, payload)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_id_is_exactly_braid_identity() {
        let domain = b"lw.logical-db.test.v0";
        assert_eq!(
            content_id(domain, b"payload"),
            Cid::compute(domain, b"payload")
        );
    }

    #[test]
    fn distinct_payloads_get_distinct_content_ids() {
        assert_ne!(
            content_id(b"lw.logical-db.test.v0", b"hello"),
            content_id(b"lw.logical-db.test.v0", b"hellp")
        );
    }

    #[test]
    fn domains_cannot_alias_the_same_payload() {
        assert_ne!(
            content_id(b"domain-a", b"same"),
            content_id(b"domain-b", b"same")
        );
    }
}
