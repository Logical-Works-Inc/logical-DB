//! Canonical tuple layer — issue #11 F2, ADR-002.
//!
//! One estate namespace, stable subspaces as newtypes, canonical
//! order-preserving tuple codec, and the tenant-prefix contract
//! `INV-TENANT-PREFIX`. This is the FoundationDB layer philosophy:
//! namespaces/subspaces + one order-preserving encoding, not a
//! second physical model.
//!
//! Raw estate/subspace/tenant prefix bytes are never public surface —
//! consumers hold a subspace newtype and a [`TenantId`] and can only
//! construct keys through [`KernelSubspace::pack`] et al. Content IDs
//! stay payload identity (INV-CONTENT-IS-PAYLOAD); directory
//! indirection is logical only (filesystem truth belongs to LEM).
//!
//! Codec is `#[forbid(unsafe_code)]` and fail-closed on decode.
//! Ordering is total and preserved in the encoded bytes
//! (`Tuple`'s `Ord` equals lexicographic order of `encode()`).

use crate::{KernelError, Result};

// ── Estate ────────────────────────────────────────────────────

/// Estate-level prefix. Stable, versioned, and never exposed raw
/// outside this module — every key begins with it, so redb-era
/// `Class` tags (`0x01`…`0x05`) can never collide after migration.
const ESTATE_PREFIX: &[u8] = &[0xFE, b'L', b'D', b'B'];

/// Hybrid application format. `0xF2` is outside every v1 product-subspace
/// tag (`0x01..=0x05`), so old keys and new account-first keys are disjoint
/// without consulting a migration table.
const HYBRID_V2_PREFIX: &[u8] = &[0xFE, b'L', b'D', b'B', 0xF2];
pub const MAX_SCOPE_ID_BYTES: usize = 1_024;

// ── Tenant ────────────────────────────────────────────────────

/// Tenant / authority scope. Opaque bytes; arbitrary `[u8]` is
/// accepted (existing arbitrary-byte corpus already in tree).
/// Display is redacted — bytes never appear in logs.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TenantId(Vec<u8>);

impl TenantId {
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Self(bytes.into())
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

// ── Hybrid application scope (ADR-003) ───────────────────────

fn validate_scope_id(axis: &'static str, bytes: Vec<u8>) -> Result<Vec<u8>> {
    if bytes.is_empty() || bytes.len() > MAX_SCOPE_ID_BYTES {
        return Err(KernelError::ScopeIdInvalid {
            axis,
            len: bytes.len(),
            limit: MAX_SCOPE_ID_BYTES,
        });
    }
    Ok(bytes)
}

macro_rules! define_scope_id {
    ($name:ident, $axis:literal) => {
        #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(Vec<u8>);

        impl $name {
            pub fn new(bytes: impl Into<Vec<u8>>) -> Result<Self> {
                validate_scope_id($axis, bytes.into()).map(Self)
            }

            pub fn as_bytes(&self) -> &[u8] {
                &self.0
            }
        }
    };
}

define_scope_id!(AccountId, "account");
define_scope_id!(ContainerId, "container");
define_scope_id!(ZoneId, "zone");

/// The stable application ownership/atomicity scope. Account comes first in
/// every key so export, erasure, backup, and quota scans are contiguous.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Scope {
    account: AccountId,
    container: ContainerId,
    zone: ZoneId,
}

impl Scope {
    pub fn new(account: AccountId, container: ContainerId, zone: ZoneId) -> Self {
        Self {
            account,
            container,
            zone,
        }
    }

    pub fn account(&self) -> &AccountId {
        &self.account
    }

    pub fn container(&self) -> &ContainerId {
        &self.container
    }

    pub fn zone(&self) -> &ZoneId {
        &self.zone
    }

    pub fn account_prefix(&self) -> Vec<u8> {
        let mut out = HYBRID_V2_PREFIX.to_vec();
        encode_component(self.account.as_bytes(), &mut out);
        out
    }

    pub fn container_prefix(&self) -> Vec<u8> {
        let mut out = self.account_prefix();
        encode_component(self.container.as_bytes(), &mut out);
        out
    }

    pub fn zone_prefix(&self) -> Vec<u8> {
        let mut out = self.container_prefix();
        encode_component(self.zone.as_bytes(), &mut out);
        out
    }

    /// Pack one logical family key. Physical providers receive only these
    /// bytes; embedded and outboard profiles therefore cannot drift in scope
    /// or ordering semantics.
    pub fn try_pack(&self, family: KeyFamily, tuple: &Tuple) -> Result<Vec<u8>> {
        let mut out = self.family_prefix(family);
        let encoded_len = tuple.encoded_len();
        let packed_len = out.len().saturating_add(encoded_len);
        if packed_len > crate::fdb::MAX_KEY_BYTES {
            return Err(KernelError::KeyTooLarge {
                len: packed_len,
                limit: crate::fdb::MAX_KEY_BYTES,
            });
        }
        out.extend_from_slice(&tuple.encode());
        Ok(out)
    }

    #[cfg(test)]
    pub(crate) fn pack(&self, family: KeyFamily, tuple: &Tuple) -> Vec<u8> {
        self.try_pack(family, tuple)
            .expect("internal hybrid key components are bounded")
    }

    pub(crate) fn family_prefix(&self, family: KeyFamily) -> Vec<u8> {
        let mut out = self.zone_prefix();
        out.push(family.tag());
        out
    }

    pub(crate) fn system_prefix(&self, family: SystemFamily) -> Vec<u8> {
        let mut out = self.zone_prefix();
        out.push(family.tag());
        out
    }

    pub(crate) fn pack_system(&self, family: SystemFamily, tuple: &Tuple) -> Vec<u8> {
        let mut out = self.system_prefix(family);
        // Every system tuple is assembled from already-bounded scope,
        // operation, and proof identifiers inside admitted orchestration.
        out.extend_from_slice(&tuple.encode());
        out
    }
}

/// App-owned data families under one zone. Change rows and idempotency markers
/// are deliberately absent: only admitted orchestration can name them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum KeyFamily {
    Record,
    Revision,
    Index,
    BlobRef,
}

impl KeyFamily {
    const fn tag(self) -> u8 {
        match self {
            Self::Record => 0x10,
            Self::Revision => 0x11,
            Self::Index => 0x13,
            Self::BlobRef => 0x14,
        }
    }

    fn from_tag(tag: u8) -> Result<Self> {
        match tag {
            0x10 => Ok(Self::Record),
            0x11 => Ok(Self::Revision),
            0x13 => Ok(Self::Index),
            0x14 => Ok(Self::BlobRef),
            _ => Err(KernelError::MalformedKey),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum SystemFamily {
    Change,
    Outbox,
    Frontier,
}

impl SystemFamily {
    const fn tag(self) -> u8 {
        match self {
            Self::Change => 0x12,
            Self::Outbox => 0x15,
            Self::Frontier => 0x16,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScopedKey {
    pub scope: Scope,
    pub family: KeyFamily,
    pub tuple: Tuple,
}

impl ScopedKey {
    pub fn decode(key: &[u8]) -> Result<Self> {
        crate::fdb::validate_key(key)?;
        if !key.starts_with(HYBRID_V2_PREFIX) {
            let version = key.get(ESTATE_PREFIX.len()).copied().unwrap_or_default();
            return if key.starts_with(ESTATE_PREFIX) {
                Err(KernelError::UnsupportedFormatVersion(version))
            } else {
                Err(KernelError::MalformedKey)
            };
        }
        let mut cursor = &key[HYBRID_V2_PREFIX.len()..];
        let account = AccountId::new(decode_component(&mut cursor)?)?;
        let container = ContainerId::new(decode_component(&mut cursor)?)?;
        let zone = ZoneId::new(decode_component(&mut cursor)?)?;
        let (family, tuple_bytes) = cursor.split_first().ok_or(KernelError::MalformedKey)?;
        Ok(Self {
            scope: Scope::new(account, container, zone),
            family: KeyFamily::from_tag(*family)?,
            tuple: Tuple::decode(tuple_bytes)?,
        })
    }
}

fn encode_component(bytes: &[u8], out: &mut Vec<u8>) {
    for &byte in bytes {
        if byte == 0x00 {
            out.extend_from_slice(&[0x00, 0xFF]);
        } else {
            out.push(byte);
        }
    }
    out.extend_from_slice(&[0x00, 0x00]);
}

fn decode_component(cursor: &mut &[u8]) -> Result<Vec<u8>> {
    let mut decoded = Vec::new();
    loop {
        let (byte, rest) = cursor.split_first().ok_or(KernelError::MalformedKey)?;
        *cursor = rest;
        if *byte != 0x00 {
            decoded.push(*byte);
        } else {
            let (next, rest) = cursor.split_first().ok_or(KernelError::MalformedKey)?;
            *cursor = rest;
            match *next {
                0xFF => decoded.push(0x00),
                0x00 => return Ok(decoded),
                _ => return Err(KernelError::MalformedKey),
            }
        }
        if decoded.len() > MAX_SCOPE_ID_BYTES {
            return Err(KernelError::ScopeIdInvalid {
                axis: "encoded scope",
                len: decoded.len(),
                limit: MAX_SCOPE_ID_BYTES,
            });
        }
    }
}

// ── Tuple ─────────────────────────────────────────────────────

/// One element of the canonical tuple. Ordering is the encoded
/// ordering: variant discriminant first, then inner value.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Element {
    Bytes(Vec<u8>),
    Int(i64),
    Bool(bool),
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Tuple(Vec<Element>);

impl Tuple {
    pub fn new(elements: Vec<Element>) -> Self {
        Self(elements)
    }

    pub fn elements(&self) -> &[Element] {
        &self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn encoded_len(&self) -> usize {
        self.0.iter().fold(0usize, |total, element| {
            let element_len = match element {
                Element::Bytes(bytes) => 2usize
                    .saturating_add(bytes.len())
                    .saturating_add(bytes.iter().filter(|byte| **byte == 0).count()),
                Element::Int(_) => 9,
                Element::Bool(_) => 1,
            };
            total.saturating_add(element_len)
        })
    }

    /// Order-preserving encode. Each element is self-delimiting:
    /// `Bytes` carries an escaped `0x00` terminator, `Int` is fixed
    /// 9 bytes (type code + sign-flipped BE), `Bool` is one byte.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        for el in &self.0 {
            match el {
                Element::Bytes(b) => {
                    out.push(CODE_BYTES);
                    for &byte in b {
                        if byte == 0x00 {
                            out.extend_from_slice(&[0x00, 0xFF]);
                        } else {
                            out.push(byte);
                        }
                    }
                    out.push(0x00);
                }
                Element::Int(v) => {
                    out.push(CODE_INT);
                    let flipped = (*v as u64) ^ 0x8000_0000_0000_0000u64;
                    out.extend_from_slice(&flipped.to_be_bytes());
                }
                Element::Bool(b) => {
                    out.push(if *b { CODE_BOOL_TRUE } else { CODE_BOOL_FALSE });
                }
            }
        }
        out
    }

    /// Fail-closed decode. Any truncation, unknown type code, or
    /// unterminated `Bytes` is `MalformedTuple` — never a silent
    /// fallback.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut out = Vec::new();
        let mut cursor = 0usize;
        while cursor < bytes.len() {
            let code = bytes[cursor];
            cursor += 1;
            match code {
                CODE_BYTES => {
                    let mut val = Vec::new();
                    let mut terminated = false;
                    while cursor < bytes.len() {
                        let b = bytes[cursor];
                        if b == 0x00 {
                            if cursor + 1 < bytes.len() && bytes[cursor + 1] == 0xFF {
                                val.push(0x00);
                                cursor += 2;
                            } else {
                                cursor += 1;
                                terminated = true;
                                break;
                            }
                        } else {
                            val.push(b);
                            cursor += 1;
                        }
                    }
                    if !terminated {
                        return Err(KernelError::MalformedTuple);
                    }
                    out.push(Element::Bytes(val));
                }
                CODE_INT => {
                    if cursor + 8 > bytes.len() {
                        return Err(KernelError::MalformedTuple);
                    }
                    let mut arr = [0u8; 8];
                    arr.copy_from_slice(&bytes[cursor..cursor + 8]);
                    cursor += 8;
                    let flipped = u64::from_be_bytes(arr);
                    let v = (flipped ^ 0x8000_0000_0000_0000u64) as i64;
                    out.push(Element::Int(v));
                }
                CODE_BOOL_FALSE => out.push(Element::Bool(false)),
                CODE_BOOL_TRUE => out.push(Element::Bool(true)),
                _ => return Err(KernelError::MalformedTuple),
            }
        }
        Ok(Self(out))
    }
}

// Type codes — chosen to give the type ordering
// Bytes < Int < Bool, and distinct from subspace tags.
const CODE_BYTES: u8 = 0x01;
const CODE_INT: u8 = 0x14;
const CODE_BOOL_FALSE: u8 = 0x26;
const CODE_BOOL_TRUE: u8 = 0x27;

// ── Subspace machinery ──────────────────────────────────────

fn escape_tenant(id: &[u8], out: &mut Vec<u8>) {
    for &b in id {
        if b == 0x00 {
            out.extend_from_slice(&[0x00, 0xFF]);
        } else {
            out.push(b);
        }
    }
}

fn encode_tenant_prefix(tag: u8, tenant: &TenantId) -> Vec<u8> {
    let mut out = Vec::with_capacity(ESTATE_PREFIX.len() + 1 + tenant.0.len() * 2 + 2);
    out.extend_from_slice(ESTATE_PREFIX);
    out.push(tag);
    escape_tenant(&tenant.0, &mut out);
    out.extend_from_slice(&[0x00, 0x00]);
    out
}

fn decode_tenant_after_estate(key: &[u8], expected_tag: u8) -> Result<(TenantId, usize)> {
    if key.len() < ESTATE_PREFIX.len() + 1 + 2 {
        return Err(KernelError::MalformedKey);
    }
    if &key[..ESTATE_PREFIX.len()] != ESTATE_PREFIX {
        return Err(KernelError::MalformedKey);
    }
    if key[ESTATE_PREFIX.len()] != expected_tag {
        return Err(KernelError::TenantPrefixViolation);
    }
    let mut cursor = ESTATE_PREFIX.len() + 1;
    let mut tenant = Vec::new();
    // Walk until 0x00 0x00 terminator (escaped 0x00 is 0x00 0xFF).
    loop {
        if cursor + 1 >= key.len() {
            return Err(KernelError::MalformedKey);
        }
        let b = key[cursor];
        if b == 0x00 {
            let nxt = key[cursor + 1];
            if nxt == 0xFF {
                tenant.push(0x00);
                cursor += 2;
            } else if nxt == 0x00 {
                cursor += 2;
                break;
            } else {
                return Err(KernelError::MalformedKey);
            }
        } else {
            tenant.push(b);
            cursor += 1;
        }
    }
    Ok((TenantId(tenant), cursor))
}

fn pack_inner(tag: u8, tenant: &TenantId, tuple: &Tuple) -> Vec<u8> {
    let mut out = encode_tenant_prefix(tag, tenant);
    out.extend_from_slice(&tuple.encode());
    out
}

fn unpack_inner(tag: u8, key: &[u8]) -> Result<(TenantId, Tuple)> {
    let (tenant, at) = decode_tenant_after_estate(key, tag)?;
    let tuple = Tuple::decode(&key[at..])?;
    Ok((tenant, tuple))
}

// ── Subspace newtypes (raw prefixes are not public) ─────────

macro_rules! define_subspace {
    ($name:ident, $tag:expr, $doc:expr) => {
        #[doc = $doc]
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
        pub struct $name;

        impl $name {
            pub fn new() -> Self {
                Self
            }

            /// Tenant-scoped prefix for `scan_prefix`-style ranges.
            /// A transaction holding this prefix by construction cannot
            /// address another tenant's range (`INV-TENANT-PREFIX`).
            pub fn tenant_prefix(&self, tenant: &TenantId) -> Vec<u8> {
                encode_tenant_prefix($tag, tenant)
            }

            /// Pack a tuple into the canonical FDB key
            /// `estate / subspace / tenant / tuple`.
            pub fn pack(&self, tenant: &TenantId, tuple: &Tuple) -> Vec<u8> {
                pack_inner($tag, tenant, tuple)
            }

            /// Inverse of [`Self::pack`]; fail-closed on any framing
            /// break and on subspace/tag mismatch.
            pub fn unpack(&self, key: &[u8]) -> Result<(TenantId, Tuple)> {
                unpack_inner($tag, key)
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }
    };
}

define_subspace!(
    KernelSubspace,
    0x01,
    "Kernel subspace — core rows, never raw bytes in public API."
);
define_subspace!(
    LemSubspace,
    0x02,
    "LEM subspace — filesystem projection (filesystem truth belongs to LEM)."
);
define_subspace!(
    CairnSubspace,
    0x03,
    "Cairn subspace — indexing and retrieval."
);
define_subspace!(
    MooSubspace,
    0x04,
    "MOO subspace — orchestration and scheduling."
);
define_subspace!(
    ForgeSubspace,
    0x05,
    "Forge subspace — build and harness state."
);

/// The one estate namespace. Exists so call-sites name the estate
/// once; it carries no extra bytes beyond [`ESTATE_PREFIX`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Estate;

impl Estate {
    pub fn prefix() -> Vec<u8> {
        ESTATE_PREFIX.to_vec()
    }
}

// ── Tests ─────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn scope(account: &[u8], container: &[u8], zone: &[u8]) -> Scope {
        Scope::new(
            AccountId::new(account.to_vec()).unwrap(),
            ContainerId::new(container.to_vec()).unwrap(),
            ZoneId::new(zone.to_vec()).unwrap(),
        )
    }

    #[test]
    fn hybrid_v2_round_trips_arbitrary_scoped_bytes() {
        let original = ScopedKey {
            scope: scope(b"person\0one", b"app\0notes", b"private\0zone"),
            family: KeyFamily::Record,
            tuple: Tuple::new(vec![
                Element::Bytes(b"object\0id".to_vec()),
                Element::Int(-7),
                Element::Bool(true),
            ]),
        };
        let encoded = original.scope.pack(original.family, &original.tuple);
        assert_eq!(ScopedKey::decode(&encoded).unwrap(), original);
    }

    #[test]
    fn tuple_wire_bytes_and_scope_component_boundaries_are_pinned() {
        assert_eq!(Tuple::new(vec![]).encode(), Vec::<u8>::new());
        assert_eq!(
            Tuple::new(vec![Element::Bytes(b"a\0b".to_vec())]).encode(),
            vec![CODE_BYTES, b'a', 0x00, 0xFF, b'b', 0x00]
        );
        assert_eq!(
            Tuple::new(vec![Element::Int(0)]).encode(),
            vec![CODE_INT, 0x80, 0, 0, 0, 0, 0, 0, 0]
        );
        assert_eq!(
            Tuple::new(vec![Element::Bool(false), Element::Bool(true)]).encode(),
            vec![CODE_BOOL_FALSE, CODE_BOOL_TRUE]
        );

        let empty = [0x00, 0x00];
        let mut empty_cursor = empty.as_slice();
        assert_eq!(
            decode_component(&mut empty_cursor).unwrap(),
            Vec::<u8>::new()
        );
        assert!(empty_cursor.is_empty());

        let mut exact = vec![b'x'; MAX_SCOPE_ID_BYTES];
        exact.extend_from_slice(&[0x00, 0x00]);
        let mut exact_cursor = exact.as_slice();
        assert_eq!(
            decode_component(&mut exact_cursor).unwrap(),
            vec![b'x'; MAX_SCOPE_ID_BYTES]
        );
        assert!(exact_cursor.is_empty());

        let mut oversized = vec![b'x'; MAX_SCOPE_ID_BYTES + 1];
        oversized.extend_from_slice(&[0x00, 0x00]);
        let mut oversized_cursor = oversized.as_slice();
        assert!(matches!(
            decode_component(&mut oversized_cursor),
            Err(KernelError::ScopeIdInvalid {
                axis: "encoded scope",
                len,
                limit: MAX_SCOPE_ID_BYTES
            }) if len == MAX_SCOPE_ID_BYTES + 1
        ));
    }

    #[test]
    fn account_is_the_leftmost_dynamic_axis() {
        let a = scope(b"account-a", b"same-container", b"same-zone");
        let b = scope(b"account-b", b"same-container", b"same-zone");
        let tuple = Tuple::new(vec![Element::Bytes(b"object".to_vec())]);
        let key_a = a.pack(KeyFamily::Record, &tuple);
        let key_b = b.pack(KeyFamily::Record, &tuple);
        assert!(key_a < key_b);
        assert!(key_a.starts_with(&a.account_prefix()));
        assert!(!key_b.starts_with(&a.account_prefix()));
    }

    #[test]
    fn containers_zones_and_families_are_disjoint() {
        let base = scope(b"account", b"container-a", b"zone-a");
        let other_container = scope(b"account", b"container-b", b"zone-a");
        let other_zone = scope(b"account", b"container-a", b"zone-b");
        let tuple = Tuple::new(vec![Element::Bytes(b"same-object".to_vec())]);
        let record = base.pack(KeyFamily::Record, &tuple);
        assert_ne!(record, other_container.pack(KeyFamily::Record, &tuple));
        assert_ne!(record, other_zone.pack(KeyFamily::Record, &tuple));
        assert_ne!(record, base.pack(KeyFamily::Revision, &tuple));

        for family in [
            KeyFamily::Record,
            KeyFamily::Revision,
            KeyFamily::Index,
            KeyFamily::BlobRef,
        ] {
            let decoded = ScopedKey::decode(&base.pack(family, &tuple)).unwrap();
            assert_eq!(decoded.scope, base);
            assert_eq!(decoded.family, family);
            assert_eq!(decoded.tuple, tuple);
        }
        for family in [
            SystemFamily::Change,
            SystemFamily::Outbox,
            SystemFamily::Frontier,
        ] {
            let system_key = base.pack_system(family, &tuple);
            assert!(matches!(
                ScopedKey::decode(&system_key),
                Err(KernelError::MalformedKey)
            ));
        }
    }

    #[test]
    fn hybrid_v2_never_aliases_a_v1_product_subspace() {
        let s = scope(b"account", b"container", b"zone");
        let v2 = s.pack(
            KeyFamily::Record,
            &Tuple::new(vec![Element::Bytes(b"x".to_vec())]),
        );
        for v1 in [
            KernelSubspace::new().pack(&TenantId::new(b"account"), &Tuple::new(vec![])),
            LemSubspace::new().pack(&TenantId::new(b"account"), &Tuple::new(vec![])),
            CairnSubspace::new().pack(&TenantId::new(b"account"), &Tuple::new(vec![])),
            MooSubspace::new().pack(&TenantId::new(b"account"), &Tuple::new(vec![])),
            ForgeSubspace::new().pack(&TenantId::new(b"account"), &Tuple::new(vec![])),
        ] {
            assert!(!v2.starts_with(&v1));
            assert!(!v1.starts_with(HYBRID_V2_PREFIX));
        }
    }

    #[test]
    fn scope_ids_fail_closed_at_empty_and_oversized_boundaries() {
        assert!(matches!(
            AccountId::new(Vec::new()),
            Err(KernelError::ScopeIdInvalid {
                axis: "account",
                ..
            })
        ));
        assert!(ContainerId::new(vec![b'x'; MAX_SCOPE_ID_BYTES]).is_ok());
        assert!(matches!(
            ZoneId::new(vec![b'x'; MAX_SCOPE_ID_BYTES + 1]),
            Err(KernelError::ScopeIdInvalid { axis: "zone", .. })
        ));
    }

    fn arb_bytes_cases() -> Vec<Vec<u8>> {
        vec![
            vec![],
            vec![0x00],
            vec![0x00, 0xFF],
            vec![0xFF],
            vec![0xFF, 0x00, 0xFF],
            b"hello".to_vec(),
            b"hello\x00world".to_vec(),
            vec![0xFE, b'L', b'D', b'B'],
            vec![0x00, 0x00],
            (0..=255u16).map(|v| v as u8).collect(),
        ]
    }

    #[test]
    fn tuple_round_trips_and_preserves_order() {
        // Round-trip over arbitrary bytes and type mix, including
        // every byte value and empty components.
        let byte_cases = arb_bytes_cases();
        let mut tuples = Vec::new();
        for b in &byte_cases {
            tuples.push(Tuple::new(vec![Element::Bytes(b.clone())]));
            tuples.push(Tuple::new(vec![Element::Bytes(b.clone()), Element::Int(0)]));
            tuples.push(Tuple::new(vec![
                Element::Bytes(b.clone()),
                Element::Bytes(b.clone()),
            ]));
        }
        tuples.push(Tuple::new(vec![]));
        tuples.push(Tuple::new(vec![Element::Int(i64::MIN)]));
        tuples.push(Tuple::new(vec![Element::Int(-1)]));
        tuples.push(Tuple::new(vec![Element::Int(0)]));
        tuples.push(Tuple::new(vec![Element::Int(1)]));
        tuples.push(Tuple::new(vec![Element::Int(i64::MAX)]));
        tuples.push(Tuple::new(vec![Element::Bool(false)]));
        tuples.push(Tuple::new(vec![Element::Bool(true)]));
        tuples.push(Tuple::new(vec![
            Element::Bytes(b"a".to_vec()),
            Element::Int(42),
            Element::Bool(true),
        ]));
        tuples.push(Tuple::new(vec![
            Element::Bytes(vec![0x00]),
            Element::Bytes(vec![0x00, 0x00]),
        ]));

        for t in &tuples {
            let enc = t.encode();
            let back = Tuple::decode(&enc).expect("round-trip must succeed");
            assert_eq!(&back, t, "round-trip failed for {t:?} -> {enc:?}");
        }

        // Total ordering: Tuple's Ord must equal lexicographic order
        // of the encoded bytes.
        let mut sorted_by_tuple = tuples.clone();
        sorted_by_tuple.sort();

        let mut pairs: Vec<(Tuple, Vec<u8>)> = tuples
            .into_iter()
            .map(|t| {
                let e = t.encode();
                (t, e)
            })
            .collect();
        pairs.sort_by(|a, b| a.1.cmp(&b.1));
        let sorted_by_bytes: Vec<Tuple> = pairs.into_iter().map(|(t, _)| t).collect();
        assert_eq!(
            sorted_by_tuple, sorted_by_bytes,
            "tuple ordering must be preserved by encoded bytes"
        );

        // Explicit ordering witnesses across types (Bytes < Int < Bool
        // by type code).
        let b = Tuple::new(vec![Element::Bytes(vec![0xFF])]);
        let i = Tuple::new(vec![Element::Int(0)]);
        let f = Tuple::new(vec![Element::Bool(false)]);
        assert!(b < i, "Bytes must sort before Int");
        assert!(i < f, "Int must sort before Bool");
        assert!(b.encode() < i.encode());
        assert!(i.encode() < f.encode());

        // Within Bytes, lexicographic ordering preserved including
        // embedded 0x00.
        let a = Tuple::new(vec![Element::Bytes(b"a".to_vec())]);
        let a_nul = Tuple::new(vec![Element::Bytes(b"a\x00".to_vec())]);
        let a_nul_b = Tuple::new(vec![Element::Bytes(b"a\x00b".to_vec())]);
        assert!(a < a_nul);
        assert!(a_nul < a_nul_b);
        assert!(a.encode() < a_nul.encode());
        assert!(a_nul.encode() < a_nul_b.encode());

        // Int ordering preserved including sign flip.
        let neg = Tuple::new(vec![Element::Int(-100)]);
        let zero = Tuple::new(vec![Element::Int(0)]);
        let pos = Tuple::new(vec![Element::Int(100)]);
        assert!(neg < zero);
        assert!(zero < pos);
        assert!(neg.encode() < zero.encode());
        assert!(zero.encode() < pos.encode());

        // Malformed inputs are fail-closed.
        assert!(Tuple::decode(&[CODE_BYTES]).is_err(), "unterminated Bytes");
        assert!(
            Tuple::decode(&[CODE_BYTES, 0x00, 0xFF]).is_err(),
            "escaped NUL without terminator"
        );
        assert!(Tuple::decode(&[0xFF]).is_err(), "unknown type code");
        assert!(
            Tuple::decode(&[CODE_INT, 0x01, 0x02]).is_err(),
            "truncated Int"
        );
        assert!(
            Tuple::decode(&[CODE_BYTES, b'a', 0x00, CODE_INT]).is_err(),
            "trailing truncated Int after Bytes"
        );
        // Empty encoding is the empty tuple, not an error.
        assert_eq!(Tuple::decode(&[]).unwrap(), Tuple::new(vec![]));
    }

    #[test]
    fn cross_tenant_prefix_is_isolated() {
        let kernel = KernelSubspace::new();
        let lem = LemSubspace::new();
        let tenants = [
            TenantId::new(b"alice".to_vec()),
            TenantId::new(b"bob".to_vec()),
            TenantId::new(vec![0x00]),
            TenantId::new(b"a\x00b".to_vec()),
            TenantId::new(vec![0x00, 0x00]),
            TenantId::new(vec![0xFF, 0x00, 0xFF]),
            TenantId::new(b"".to_vec()),
        ];
        let adversarial_tuples = vec![
            Tuple::new(vec![Element::Bytes(vec![0x00, 0x00])]),
            Tuple::new(vec![Element::Bytes(vec![0x00, 0xFF])]),
            Tuple::new(vec![Element::Bytes(ESTATE_PREFIX.to_vec())]),
            Tuple::new(vec![Element::Bytes(vec![0xFE, b'L', b'D', b'B', 0x01])]),
            Tuple::new(vec![Element::Bytes(b"bob".to_vec())]),
            Tuple::new(vec![Element::Bytes(b"\x00\x00".to_vec())]),
            Tuple::new(vec![Element::Bytes(vec![])]),
            Tuple::new(vec![Element::Bytes(b"alice\x00\x00bob".to_vec())]),
            Tuple::new(vec![]),
        ];

        // Every tenant's prefix is exactly what `tenant_prefix` returns
        // and is the prefix of its packed keys.
        for tenant in &tenants {
            let prefix = kernel.tenant_prefix(tenant);
            for tup in &adversarial_tuples {
                let key = kernel.pack(tenant, tup);
                assert!(
                    key.starts_with(&prefix),
                    "key for {tenant:?} must start with its own prefix"
                );
            }
        }

        // No key packed for tenant A may be decoded as tenant B, and
        // no A key may carry B's prefix — even when the tuple
        // contains bytes that look like a tenant name or terminator.
        for a in &tenants {
            for b in &tenants {
                if a == b {
                    continue;
                }
                let b_prefix = kernel.tenant_prefix(b);
                for tup in &adversarial_tuples {
                    let key_a = kernel.pack(a, tup);
                    assert!(
                        !key_a.starts_with(&b_prefix),
                        "tenant {a:?} key must not have prefix of tenant {b:?} even with adversarial tuple {tup:?}"
                    );
                    // Unpacking with the wrong tenant's subspace must not
                    // yield b's tenant id.
                    let (got_tenant, got_tuple) = kernel.unpack(&key_a).unwrap();
                    assert_eq!(got_tenant, *a);
                    assert_eq!(got_tuple, *tup);
                    assert_ne!(got_tenant, *b);
                    // Cross-subspace isolation too: a kernel key never
                    // unpacks as LEM.
                    assert!(lem.unpack(&key_a).is_err());
                }
            }
        }

        // Direct content-id-as-key confusion is rejected: content IDs
        // are payload (tuple element bytes), never a physical prefix.
        // Crafting a tuple whose bytes equal another tenant's id does
        // not make the key live under that tenant.
        let alice = TenantId::new(b"alice".to_vec());
        let bob = TenantId::new(b"bob".to_vec());
        let evil = Tuple::new(vec![Element::Bytes(b"bob".to_vec())]);
        let key = kernel.pack(&alice, &evil);
        assert!(!key.starts_with(&kernel.tenant_prefix(&bob)));
        let (tenant_back, tuple_back) = kernel.unpack(&key).unwrap();
        assert_eq!(tenant_back, alice);
        assert_eq!(tuple_back, evil);

        // Malformed tenant framing is fail-closed, not a silent
        // alias to another tenant.
        let mut bad = kernel.pack(&alice, &Tuple::new(vec![]));
        // Corrupt the tenant terminator.
        let term_pos = ESTATE_PREFIX.len() + 1 + b"alice".len();
        bad[term_pos] = 0xFF;
        assert!(kernel.unpack(&bad).is_err());
        // Truncated key.
        assert!(kernel.unpack(&bad[..ESTATE_PREFIX.len()]).is_err());
        assert!(kernel.unpack(&[]).is_err());
    }

    #[test]
    fn tenant_ids_with_arbitrary_bytes_are_accepted_and_isolated() {
        let s = KernelSubspace::new();
        for id in arb_bytes_cases() {
            let tenant = TenantId::new(id.clone());
            let tup = Tuple::new(vec![Element::Bytes(b"payload".to_vec())]);
            let key = s.pack(&tenant, &tup);
            let (back_tenant, back_tup) = s.unpack(&key).unwrap();
            assert_eq!(back_tenant.as_bytes(), id.as_slice());
            assert_eq!(back_tup, tup);
        }
    }

    #[test]
    fn subspaces_are_distinct_types_with_stable_prefixes() {
        let k = KernelSubspace::new();
        let l = LemSubspace::new();
        let c = CairnSubspace::new();
        let m = MooSubspace::new();
        let f = ForgeSubspace::new();
        let tenant = TenantId::new(b"t".to_vec());
        let tup = Tuple::new(vec![Element::Bytes(b"x".to_vec())]);
        let keys = [
            k.pack(&tenant, &tup),
            l.pack(&tenant, &tup),
            c.pack(&tenant, &tup),
            m.pack(&tenant, &tup),
            f.pack(&tenant, &tup),
        ];
        for i in 0..keys.len() {
            for j in 0..keys.len() {
                if i == j {
                    continue;
                }
                assert_ne!(keys[i], keys[j], "subspaces must be distinct");
                assert!(
                    !keys[i].starts_with(&keys[j][..ESTATE_PREFIX.len() + 1]),
                    "no subspace key may have another subspace prefix"
                );
            }
        }
        assert_eq!(Estate::prefix(), ESTATE_PREFIX.to_vec());
    }
}
