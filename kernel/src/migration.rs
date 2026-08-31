//! Explicit v1 row-key to hybrid-v2 migration primitives.
//!
//! Migration is copy/verify/cutover. This module never deletes a source key
//! and never guesses a destination scope; the operator must supply the exact
//! account/container/zone that owns the legacy store.

use crate::model::{split_key, Class};
use crate::tuple::{Element, KeyFamily, Scope, Tuple};
use crate::Result;

const LEGACY_ROW_MARKER: &[u8] = b"logical-db/v1-row";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LegacyV1Point {
    pub class: Class,
    pub id: Vec<u8>,
    pub timestamp: u64,
}

impl LegacyV1Point {
    pub fn decode(source_key: &[u8]) -> Result<Self> {
        let (class, id, timestamp) = split_key(source_key)?;
        Ok(Self {
            class,
            id,
            timestamp,
        })
    }

    /// Lossless destination tuple. Timestamp remains unsigned big-endian
    /// bytes because the tuple's signed integer axis cannot represent u64.
    pub fn destination_tuple(&self) -> Tuple {
        Tuple::new(vec![
            Element::Bytes(LEGACY_ROW_MARKER.to_vec()),
            Element::Bytes(vec![self.class.tag()]),
            Element::Bytes(self.id.clone()),
            Element::Bytes(self.timestamp.to_be_bytes().to_vec()),
        ])
    }

    pub fn destination_key(&self, scope: &Scope) -> Vec<u8> {
        scope.pack(KeyFamily::Record, &self.destination_tuple())
    }

    #[cfg(feature = "braid")]
    pub fn copy_into(
        &self,
        batch: &mut crate::admitted::WriteBatch,
        source_value: Vec<u8>,
    ) -> Result<()> {
        batch.set(KeyFamily::Record, &self.destination_tuple(), source_value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::point_key;
    use crate::tuple::{AccountId, ContainerId, ScopedKey, ZoneId};

    fn scope() -> Scope {
        Scope::new(
            AccountId::new(b"account".to_vec()).unwrap(),
            ContainerId::new(b"legacy-import".to_vec()).unwrap(),
            ZoneId::new(b"v1".to_vec()).unwrap(),
        )
    }

    #[test]
    fn every_v1_class_maps_losslessly_into_one_explicit_scope() {
        for class in [
            Class::Record,
            Class::Term,
            Class::Edge,
            Class::VecCode,
            Class::Claim,
        ] {
            let source = point_key(class, &[0, 0xFF, class.tag()], u64::MAX);
            let legacy = LegacyV1Point::decode(&source).unwrap();
            let destination = ScopedKey::decode(&legacy.destination_key(&scope())).unwrap();
            assert_eq!(destination.scope, scope());
            assert_eq!(destination.family, KeyFamily::Record);
            assert_eq!(destination.tuple, legacy.destination_tuple());
        }
    }

    #[test]
    fn malformed_source_is_never_guessed() {
        assert!(LegacyV1Point::decode(b"not-a-v1-point-key").is_err());
    }
}
