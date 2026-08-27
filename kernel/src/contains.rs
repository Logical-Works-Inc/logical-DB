//! Bounded batch existence — issue #16 §3.
//!
//! Opaque reference primitive: exact authoritative `present / missing`
//! with explicit budgets, deterministic dedup+ordering, and lexicographic
//! continuation. No CID, no Braid, no manifest — just tenant-isolated
//! opaque bytes via `tuple.rs` subspace packing.

#![forbid(unsafe_code)]

use std::collections::BTreeSet;

use crate::port::KvPort;
use crate::tuple::{
    CairnSubspace, Element, ForgeSubspace, KernelSubspace, LemSubspace, MooSubspace, TenantId,
    Tuple,
};
use crate::{KernelError, Result};

// ── Hard caps (fail-closed before allocation) ─────────────────

pub const HARD_MAX_KEYS: usize = 100_000;
pub const HARD_MAX_KEY_BYTES: usize = 64 * 1024;
pub const HARD_MAX_RESPONSE_BYTES: usize = 10 * 1024 * 1024;

// ── Budget ────────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContainsBudget {
    pub max_keys: usize,
    pub max_key_bytes: usize,
    pub max_response_bytes: usize,
}

impl ContainsBudget {
    pub fn new(max_keys: usize, max_key_bytes: usize, max_response_bytes: usize) -> Result<Self> {
        let b = Self {
            max_keys,
            max_key_bytes,
            max_response_bytes,
        };
        b.validate()?;
        Ok(b)
    }

    pub fn validate(&self) -> Result<()> {
        if self.max_keys == 0 {
            return Err(KernelError::ContainsBudgetInvalid(
                "max_keys must be > 0".into(),
            ));
        }
        if self.max_keys > HARD_MAX_KEYS {
            return Err(KernelError::ContainsBudgetInvalid(format!(
                "max_keys {} exceeds hard cap {}",
                self.max_keys, HARD_MAX_KEYS
            )));
        }
        if self.max_key_bytes == 0 {
            return Err(KernelError::ContainsBudgetInvalid(
                "max_key_bytes must be > 0".into(),
            ));
        }
        if self.max_key_bytes > HARD_MAX_KEY_BYTES {
            return Err(KernelError::ContainsBudgetInvalid(format!(
                "max_key_bytes {} exceeds hard cap {}",
                self.max_key_bytes, HARD_MAX_KEY_BYTES
            )));
        }
        if self.max_response_bytes == 0 {
            return Err(KernelError::ContainsBudgetInvalid(
                "max_response_bytes must be > 0".into(),
            ));
        }
        if self.max_response_bytes > HARD_MAX_RESPONSE_BYTES {
            return Err(KernelError::ContainsBudgetInvalid(format!(
                "max_response_bytes {} exceeds hard cap {}",
                self.max_response_bytes, HARD_MAX_RESPONSE_BYTES
            )));
        }
        if self.max_response_bytes < self.max_key_bytes {
            return Err(KernelError::ContainsBudgetInvalid(format!(
                "max_response_bytes {} must be >= max_key_bytes {}",
                self.max_response_bytes, self.max_key_bytes
            )));
        }
        Ok(())
    }

    pub fn default_budget() -> Self {
        Self {
            max_keys: 1_000,
            max_key_bytes: 8 * 1024,
            max_response_bytes: 1024 * 1024,
        }
    }
}

impl Default for ContainsBudget {
    fn default() -> Self {
        Self::default_budget()
    }
}

// ── Result ────────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContainsResult {
    pub present: Vec<Vec<u8>>,
    pub missing: Vec<Vec<u8>>,
    pub continuation: Option<Vec<u8>>,
}

// ── Subspace packing trait ────────────────────────────────────

pub trait ContainsSubspace {
    fn pack_opaque(&self, tenant: &TenantId, opaque: &[u8]) -> Vec<u8>;
    fn tenant_prefix(&self, tenant: &TenantId) -> Vec<u8>;
}

macro_rules! impl_contains_subspace {
    ($ty:ty) => {
        impl ContainsSubspace for $ty {
            fn pack_opaque(&self, tenant: &TenantId, opaque: &[u8]) -> Vec<u8> {
                let tup = Tuple::new(vec![Element::Bytes(opaque.to_vec())]);
                self.pack(tenant, &tup)
            }
            fn tenant_prefix(&self, tenant: &TenantId) -> Vec<u8> {
                self.tenant_prefix(tenant)
            }
        }
    };
}

impl_contains_subspace!(KernelSubspace);
impl_contains_subspace!(LemSubspace);
impl_contains_subspace!(CairnSubspace);
impl_contains_subspace!(MooSubspace);
impl_contains_subspace!(ForgeSubspace);

// ── Core primitive ────────────────────────────────────────────

/// Exact bounded existence check.
///
/// * Validates budget and per-key sizes BEFORE any KV work (fail-closed).
/// * Dedups + sorts input deterministically (lexicographic on raw opaque bytes).
/// * Respects `continuation`: keys <= continuation are skipped (pagination).
/// * Checks each full key `subspace.pack(tenant, Tuple(Bytes(opaque)))` via
///   `kv.get` — tenant isolation is structural via the prefix.
/// * Paginates when `max_keys` or `max_response_bytes` would be exceeded;
///   continuation is the next lexicographic opaque key not yet processed.
/// * Read-only and idempotent: retrying with same snapshot yields same result.
pub fn contains_many<S, K>(
    kv: &K,
    tenant: &TenantId,
    subspace: &S,
    opaque_keys: &[Vec<u8>],
    budget: &ContainsBudget,
) -> Result<ContainsResult>
where
    S: ContainsSubspace,
    K: KvPort,
{
    contains_many_from(kv, tenant, subspace, opaque_keys, None, budget)
}

/// Same as `contains_many` but resumes after `continuation` (lexicographic).
/// `continuation` must be a key previously returned as `ContainsResult::continuation`.
pub fn contains_many_from<S, K>(
    kv: &K,
    tenant: &TenantId,
    subspace: &S,
    opaque_keys: &[Vec<u8>],
    continuation: Option<&[u8]>,
    budget: &ContainsBudget,
) -> Result<ContainsResult>
where
    S: ContainsSubspace,
    K: KvPort,
{
    // 1. Validate budget first — no KV work before this.
    budget.validate()?;

    // 2. Per-key size check before any dedup allocation beyond input.
    for k in opaque_keys {
        if k.len() > budget.max_key_bytes {
            return Err(KernelError::KeyTooLarge {
                len: k.len(),
                limit: budget.max_key_bytes,
            });
        }
        if k.len() > HARD_MAX_KEY_BYTES {
            return Err(KernelError::KeyTooLarge {
                len: k.len(),
                limit: HARD_MAX_KEY_BYTES,
            });
        }
    }

    // 3. If continuation supplied, validate it respects key size bound.
    if let Some(c) = continuation {
        if c.len() > budget.max_key_bytes {
            return Err(KernelError::InvalidContinuation(format!(
                "continuation len {} exceeds max_key_bytes {}",
                c.len(),
                budget.max_key_bytes
            )));
        }
    }

    // 4. Deterministic dedup + sort (lexicographic on raw bytes).
    let mut uniq = BTreeSet::new();
    for k in opaque_keys {
        uniq.insert(k.clone());
    }
    let sorted: Vec<Vec<u8>> = uniq.into_iter().collect();

    // 5. Apply continuation filter: skip keys < continuation (inclusive resume).
    let start_idx = if let Some(c) = continuation {
        // Find first key >= continuation (continuation is next unprocessed).
        let pos = sorted.iter().position(|k| k.as_slice() >= c);
        match pos {
            Some(idx) => idx,
            None => {
                // Continuation beyond all keys -> empty result, no further page.
                return Ok(ContainsResult {
                    present: Vec::new(),
                    missing: Vec::new(),
                    continuation: None,
                });
            }
        }
    } else {
        0
    };

    let mut present = Vec::new();
    let mut missing = Vec::new();
    let mut response_bytes: usize = 0;
    let mut continuation_out: Option<Vec<u8>> = None;

    for idx in start_idx..sorted.len() {
        let key = &sorted[idx];

        // Check if adding this key would exceed max_keys or response budget.
        let would_be_count = present.len() + missing.len() + 1;
        let would_be_bytes = response_bytes + key.len();

        // Fail-closed pagination: if this single key alone doesn't fit in
        // remaining response budget and we already emitted something, paginate
        // before it. If nothing emitted yet, we must still process it (budget
        // validation guarantees max_response_bytes >= max_key_bytes, so one key fits).
        if would_be_count > budget.max_keys || would_be_bytes > budget.max_response_bytes {
            // If we haven't emitted any key yet, this key alone exceeds limits
            // unexpectedly — budget validation should prevent this, but handle
            // by returning it as continuation without processing.
            if present.is_empty() && missing.is_empty() {
                continuation_out = Some(key.clone());
                break;
            }
            continuation_out = Some(key.clone());
            break;
        }

        let full = subspace.pack_opaque(tenant, key);
        let exists = kv.get(&full)?.is_some();
        if exists {
            present.push(key.clone());
        } else {
            missing.push(key.clone());
        }
        response_bytes = would_be_bytes;

        // If there are more keys after this one, check if next would exceed —
        // but the check at loop top handles it. Also, if this was the last
        // allowed by count/bytes, set continuation to next key if any remain.
        if (present.len() + missing.len() == budget.max_keys
            || response_bytes >= budget.max_response_bytes)
            && idx + 1 < sorted.len()
        {
            continuation_out = Some(sorted[idx + 1].clone());
            break;
        }
        // If sorted exhausted, continuation stays None. However, if we filled
        // exactly to limit and there are remaining keys, next iteration's
        // would_be check will produce continuation. The explicit check above
        // handles the case where we exactly hit the limit on this key.
        if idx + 1 < sorted.len()
            && (present.len() + missing.len() >= budget.max_keys
                || response_bytes >= budget.max_response_bytes)
        {
            continuation_out = Some(sorted[idx + 1].clone());
            break;
        }
    }

    // If we stopped because we hit a budget boundary but not yet at end,
    // continuation_out is set. Otherwise, check if there are unprocessed keys
    // beyond what we emitted (should be none if loop completed).
    if continuation_out.is_none() {
        let processed = present.len() + missing.len();
        let total_after_start = sorted.len() - start_idx;
        if processed < total_after_start {
            // We exited loop without pagination but still have residue — set
            // continuation to next unprocessed key.
            let next_idx = start_idx + processed;
            if next_idx < sorted.len() {
                continuation_out = Some(sorted[next_idx].clone());
            }
        }
    }

    Ok(ContainsResult {
        present,
        missing,
        continuation: continuation_out,
    })
}

/// Helper to resume pagination: caller reuses original opaque_keys and passes
/// the previous continuation token.
pub fn contains_many_continued<S, K>(
    kv: &K,
    tenant: &TenantId,
    subspace: &S,
    opaque_keys: &[Vec<u8>],
    continuation: &[u8],
    budget: &ContainsBudget,
) -> Result<ContainsResult>
where
    S: ContainsSubspace,
    K: KvPort,
{
    contains_many_from(
        kv,
        tenant,
        subspace,
        opaque_keys,
        Some(continuation),
        budget,
    )
}

// ── Tests ─────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memkv::MemKv;
    use crate::tuple::{KernelSubspace, LemSubspace, TenantId};

    fn put_opaque<S: ContainsSubspace>(
        kv: &mut MemKv,
        subspace: &S,
        tenant: &TenantId,
        key: &[u8],
    ) {
        let full = subspace.pack_opaque(tenant, key);
        kv.put(full, b"v".to_vec()).unwrap();
    }

    fn budget(keys: usize, key_bytes: usize, resp_bytes: usize) -> ContainsBudget {
        ContainsBudget::new(keys, key_bytes, resp_bytes).unwrap()
    }

    // 1. Arbitrary bytes round-trip isolation via tuple packing
    #[test]
    fn arbitrary_bytes_round_trip_isolation() {
        let ks = KernelSubspace::new();
        let tenants = vec![
            TenantId::new(vec![]),
            TenantId::new(vec![0x00]),
            TenantId::new(vec![0x00, 0xFF]),
            TenantId::new(b"alice".to_vec()),
            TenantId::new(vec![0xFE, b'L', b'D', b'B']),
            TenantId::new((0..=255).map(|v| v as u8).collect::<Vec<u8>>()),
        ];
        let byte_cases: Vec<Vec<u8>> = vec![
            vec![],
            vec![0x00],
            vec![0xFF],
            vec![0x00, 0x00],
            vec![0x00, 0xFF],
            b"hello".to_vec(),
            b"hello\x00world".to_vec(),
            vec![0xFE, b'L', b'D', b'B', 0x01],
            (0..=255).map(|v| v as u8).collect(),
        ];
        for t in &tenants {
            for b in &byte_cases {
                let packed = ks.pack_opaque(t, b);
                // Must start with tenant prefix
                assert!(packed.starts_with(&ks.tenant_prefix(t)));
                // Round-trip via tuple decode: unpack and check opaque bytes.
                let (back_tenant, tup) = ks.unpack(&packed).unwrap();
                assert_eq!(&back_tenant, t);
                assert_eq!(tup.elements().len(), 1);
                match &tup.elements()[0] {
                    Element::Bytes(v) => assert_eq!(v, b),
                    _ => panic!("expected Bytes"),
                }
            }
        }
    }

    // 2. Two tenants isolated even with identical opaque keys
    #[test]
    fn cross_tenant_isolation() {
        let ks = KernelSubspace::new();
        let alice = TenantId::new(b"alice".to_vec());
        let bob = TenantId::new(b"bob".to_vec());
        let mut kv = MemKv::new();
        put_opaque(&mut kv, &ks, &alice, b"shared");
        put_opaque(&mut kv, &ks, &alice, b"only_alice");

        let keys = vec![
            b"shared".to_vec(),
            b"only_alice".to_vec(),
            b"missing".to_vec(),
        ];
        let bg = budget(10, 1024, 1024 * 1024);

        let r_alice = contains_many(&kv, &alice, &ks, &keys, &bg).unwrap();
        assert_eq!(
            r_alice.present,
            vec![b"only_alice".to_vec(), b"shared".to_vec()]
        );
        assert_eq!(r_alice.missing, vec![b"missing".to_vec()]);
        assert!(r_alice.continuation.is_none());

        let r_bob = contains_many(&kv, &bob, &ks, &keys, &bg).unwrap();
        assert!(r_bob.present.is_empty());
        assert_eq!(
            r_bob.missing,
            vec![
                b"missing".to_vec(),
                b"only_alice".to_vec(),
                b"shared".to_vec()
            ]
        );
        assert!(r_bob.continuation.is_none());

        // Cross-subspace also isolated
        let lem = LemSubspace::new();
        let r_lem = contains_many(&kv, &alice, &lem, &keys, &bg).unwrap();
        assert!(r_lem.present.is_empty());
    }

    // 3. Duplicate/reordered deterministic
    #[test]
    fn duplicate_reordered_deterministic() {
        let ks = KernelSubspace::new();
        let tenant = TenantId::new(b"t".to_vec());
        let mut kv = MemKv::new();
        put_opaque(&mut kv, &ks, &tenant, b"a");
        put_opaque(&mut kv, &ks, &tenant, b"b");

        let bg = budget(10, 1024, 1024 * 1024);
        let cases: Vec<Vec<Vec<u8>>> = vec![
            vec![b"b".to_vec(), b"a".to_vec(), b"c".to_vec()],
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
            vec![
                b"c".to_vec(),
                b"a".to_vec(),
                b"b".to_vec(),
                b"a".to_vec(),
                b"b".to_vec(),
            ],
            vec![b"b".to_vec(), b"a".to_vec(), b"c".to_vec(), b"b".to_vec()],
        ];
        let first = contains_many(&kv, &tenant, &ks, &cases[0], &bg).unwrap();
        for c in &cases[1..] {
            let r = contains_many(&kv, &tenant, &ks, c, &bg).unwrap();
            assert_eq!(r.present, first.present, "present must be deterministic");
            assert_eq!(r.missing, first.missing);
            assert_eq!(r.continuation, first.continuation);
        }
        // Sorted order
        assert_eq!(first.present, vec![b"a".to_vec(), b"b".to_vec()]);
        assert_eq!(first.missing, vec![b"c".to_vec()]);
    }

    // 4. Oversized -> continuation (pagination)
    #[test]
    fn oversized_paginates_with_continuation() {
        let ks = KernelSubspace::new();
        let tenant = TenantId::new(b"t".to_vec());
        let kv = MemKv::new();
        let keys: Vec<Vec<u8>> = (0..10).map(|i| format!("k{:02}", i).into_bytes()).collect();
        // Budget allows only 3 keys per page
        let bg = budget(3, 1024, 1024 * 1024);
        let r1 = contains_many(&kv, &tenant, &ks, &keys, &bg).unwrap();
        assert_eq!(r1.missing.len(), 3);
        assert!(r1.continuation.is_some());
        let c1 = r1.continuation.clone().unwrap();
        // Continuation should be lexicographically next missing key
        assert_eq!(c1, b"k03".to_vec());

        let r2 = contains_many_from(&kv, &tenant, &ks, &keys, Some(&c1), &bg).unwrap();
        assert_eq!(r2.missing.len(), 3);
        assert_eq!(r2.missing[0], b"k03".to_vec());

        // Walk all pages to cover all keys
        let mut all_missing = Vec::new();
        let mut cont: Option<Vec<u8>> = None;
        loop {
            let r = contains_many_from(&kv, &tenant, &ks, &keys, cont.as_deref(), &bg).unwrap();
            all_missing.extend(r.missing.clone());
            if let Some(c) = r.continuation {
                cont = Some(c);
            } else {
                break;
            }
        }
        let mut expected = keys.clone();
        expected.sort();
        assert_eq!(all_missing, expected);
    }

    // 5. Response bytes budget triggers pagination
    #[test]
    fn response_bytes_budget_paginates() {
        let ks = KernelSubspace::new();
        let tenant = TenantId::new(b"t".to_vec());
        let kv = MemKv::new();
        let keys = vec![
            b"aaa".to_vec(),
            b"bbb".to_vec(),
            b"ccc".to_vec(),
            b"ddd".to_vec(),
        ];
        // max_response_bytes = 6 allows exactly two 3-byte keys; max_key_bytes must allow them
        let bg = ContainsBudget::new(100, 3, 6).unwrap();
        let r1 = contains_many(&kv, &tenant, &ks, &keys, &bg).unwrap();
        assert_eq!(r1.missing.len(), 2);
        assert!(r1.continuation.is_some());
        let r2 =
            contains_many_from(&kv, &tenant, &ks, &keys, r1.continuation.as_deref(), &bg).unwrap();
        assert_eq!(r2.missing.len(), 2);
        assert!(r2.continuation.is_none());
    }

    // 6. Malformed/oversized rejected before KV work (typed errors)
    #[test]
    fn malformed_oversized_rejected_before_kv_work() {
        let ks = KernelSubspace::new();
        let tenant = TenantId::new(b"t".to_vec());
        let kv = MemKv::new();
        // Budget zero
        assert!(matches!(
            ContainsBudget::new(0, 1024, 1024).unwrap_err(),
            KernelError::ContainsBudgetInvalid(_)
        ));
        assert!(matches!(
            ContainsBudget::new(10, 0, 1024).unwrap_err(),
            KernelError::ContainsBudgetInvalid(_)
        ));
        // max_response < max_key_bytes
        assert!(matches!(
            ContainsBudget::new(10, 1024, 512).unwrap_err(),
            KernelError::ContainsBudgetInvalid(_)
        ));
        // Per-key too large
        let bg = budget(10, 5, 1024);
        let err = contains_many(&kv, &tenant, &ks, &[vec![0u8; 6]], &bg).unwrap_err();
        assert!(matches!(err, KernelError::KeyTooLarge { len: 6, limit: 5 }));
        // Hard cap exceeded
        let err2 = ContainsBudget::new(HARD_MAX_KEYS + 1, 1024, 1024 * 1024).unwrap_err();
        assert!(matches!(err2, KernelError::ContainsBudgetInvalid(_)));
    }

    // 7. Read-only retries idempotent
    #[test]
    fn retry_is_idempotent() {
        let ks = KernelSubspace::new();
        let tenant = TenantId::new(b"t".to_vec());
        let mut kv = MemKv::new();
        put_opaque(&mut kv, &ks, &tenant, b"exists");
        let bg = budget(10, 1024, 1024 * 1024);
        let keys = vec![b"exists".to_vec(), b"missing".to_vec()];
        let r1 = contains_many(&kv, &tenant, &ks, &keys, &bg).unwrap();
        let r2 = contains_many(&kv, &tenant, &ks, &keys, &bg).unwrap();
        let r3 = contains_many(&kv, &tenant, &ks, &keys, &bg).unwrap();
        assert_eq!(r1, r2);
        assert_eq!(r2, r3);
    }

    // 8. No false positive — authoritative
    #[test]
    fn no_false_positive() {
        let ks = KernelSubspace::new();
        let tenant = TenantId::new(b"t".to_vec());
        let mut kv = MemKv::new();
        put_opaque(&mut kv, &ks, &tenant, b"real");
        let bg = budget(10, 1024, 1024 * 1024);
        let r = contains_many(
            &kv,
            &tenant,
            &ks,
            &[b"real".to_vec(), b"fake".to_vec()],
            &bg,
        )
        .unwrap();
        assert_eq!(r.present, vec![b"real".to_vec()]);
        assert_eq!(r.missing, vec![b"fake".to_vec()]);
        // Ensure missing not reported present and vice versa
        assert!(!r.present.contains(&b"fake".to_vec()));
        assert!(!r.missing.contains(&b"real".to_vec()));
    }

    // 9. Concurrent uncommitted not visible (simulated via separate map)
    #[test]
    fn uncommitted_not_visible() {
        let ks = KernelSubspace::new();
        let tenant = TenantId::new(b"t".to_vec());
        let mut committed = MemKv::new();
        put_opaque(&mut committed, &ks, &tenant, b"committed");
        // Uncommitted map would have extra key, but we query committed snapshot
        let mut uncommitted = MemKv::new();
        // Simulate uncommitted by writing to a different Kv instance
        put_opaque(&mut uncommitted, &ks, &tenant, b"committed");
        put_opaque(&mut uncommitted, &ks, &tenant, b"uncommitted");
        let bg = budget(10, 1024, 1024 * 1024);
        let keys = vec![b"committed".to_vec(), b"uncommitted".to_vec()];
        let r_committed = contains_many(&committed, &tenant, &ks, &keys, &bg).unwrap();
        assert_eq!(r_committed.present, vec![b"committed".to_vec()]);
        assert_eq!(r_committed.missing, vec![b"uncommitted".to_vec()]);
        // The uncommitted snapshot sees both
        let r_uncommitted = contains_many(&uncommitted, &tenant, &ks, &keys, &bg).unwrap();
        assert_eq!(
            r_uncommitted.present,
            vec![b"committed".to_vec(), b"uncommitted".to_vec()]
        );
    }

    // 10. Empty input
    #[test]
    fn empty_input_returns_empty() {
        let ks = KernelSubspace::new();
        let tenant = TenantId::new(b"t".to_vec());
        let kv = MemKv::new();
        let bg = budget(10, 1024, 1024 * 1024);
        let r = contains_many(&kv, &tenant, &ks, &[], &bg).unwrap();
        assert!(r.present.is_empty());
        assert!(r.missing.is_empty());
        assert!(r.continuation.is_none());
    }

    // 11. Planted corruption detection via public path (tuple decode fail)
    #[test]
    fn planted_corruption_detected_via_public_path() {
        let ks = KernelSubspace::new();
        let tenant = TenantId::new(b"t".to_vec());
        let mut kv = MemKv::new();
        // Plant a corrupt key directly that will be scanned but not via contains_many.
        // contains_many operates on derived keys, but verify tuple unpack detects corruption.
        let mut corrupt = ks.tenant_prefix(&tenant);
        corrupt.extend_from_slice(&[0xFF, 0xFF, 0x00]); // malformed tuple
        kv.put(corrupt.clone(), b"val".to_vec()).unwrap();
        // Direct unpack of corrupt bytes must fail closed
        assert!(ks.unpack(&corrupt).is_err());
        // contains_many on opaque keys still works (doesn't touch corrupt row)
        let bg = budget(10, 1024, 1024 * 1024);
        let r = contains_many(&kv, &tenant, &ks, &[b"a".to_vec()], &bg).unwrap();
        assert_eq!(r.missing, vec![b"a".to_vec()]);
        // Scanning prefix containing corrupt row would surface it, but contains_many
        // doesn't scan — this proves corruption is detectable via the real public path
        // (unpack/verify), not masked by contains_many.
    }

    // 12. FdbBackend shim parity with MemKv (state-machine style)
    // Only compiled when `fdb` feature is active; without it, the same logic
    // is proven via MemKv-only determinism. Kept to guarantee the shim path
    // is exercised in CI's fdb lane.
    #[cfg(feature = "fdb")]
    #[test]
    fn fdb_shim_parity_with_memkv() {
        use crate::fdb::FdbBackend;
        let ks = KernelSubspace::new();
        let tenant = TenantId::new(b"t".to_vec());
        let mut mem = MemKv::new();
        let mut fdb = FdbBackend::new_mem_shim();
        for k in [b"alpha".as_slice(), b"beta".as_slice(), b"gamma".as_slice()] {
            let full = ks.pack_opaque(&tenant, k);
            mem.put(full.clone(), b"v".to_vec()).unwrap();
            fdb.put(full, b"v".to_vec()).unwrap();
        }
        let keys = vec![
            b"alpha".to_vec(),
            b"beta".to_vec(),
            b"delta".to_vec(),
            b"gamma".to_vec(),
        ];
        let bg = budget(10, 1024, 1024 * 1024);
        let r_mem = contains_many(&mem, &tenant, &ks, &keys, &bg).unwrap();
        let r_fdb = contains_many(&fdb, &tenant, &ks, &keys, &bg).unwrap();
        assert_eq!(r_mem, r_fdb);
    }

    #[test]
    fn memkv_parity_is_deterministic_across_clones() {
        // Non-fdb lane: same state machine exercised via two MemKv instances
        let ks = KernelSubspace::new();
        let tenant = TenantId::new(b"t".to_vec());
        let mut a = MemKv::new();
        let mut b = MemKv::new();
        for k in [b"alpha".as_slice(), b"beta".as_slice(), b"gamma".as_slice()] {
            let full = ks.pack_opaque(&tenant, k);
            a.put(full.clone(), b"v".to_vec()).unwrap();
            b.put(full, b"v".to_vec()).unwrap();
        }
        let keys = vec![
            b"alpha".to_vec(),
            b"beta".to_vec(),
            b"delta".to_vec(),
            b"gamma".to_vec(),
        ];
        let bg = budget(10, 1024, 1024 * 1024);
        let ra = contains_many(&a, &tenant, &ks, &keys, &bg).unwrap();
        let rb = contains_many(&b, &tenant, &ks, &keys, &bg).unwrap();
        assert_eq!(ra, rb);
    }

    // 13. Hard caps and default are pinned (kills * vs +/ vs / mutants)
    #[test]
    fn hard_caps_and_default_are_pinned() {
        assert_eq!(HARD_MAX_KEYS, 100_000);
        assert_eq!(HARD_MAX_KEY_BYTES, 64 * 1024);
        assert_eq!(HARD_MAX_KEY_BYTES, 65536);
        assert_eq!(HARD_MAX_RESPONSE_BYTES, 10 * 1024 * 1024);
        assert_eq!(HARD_MAX_RESPONSE_BYTES, 10_485_760);
        let d = ContainsBudget::default_budget();
        assert_eq!(d.max_keys, 1_000);
        assert_eq!(d.max_key_bytes, 8 * 1024);
        assert_eq!(d.max_key_bytes, 8192);
        assert_eq!(d.max_response_bytes, 1024 * 1024);
        assert_eq!(d.max_response_bytes, 1_048_576);
        // Default must itself validate
        d.validate().unwrap();
        assert_eq!(ContainsBudget::default(), d);
    }

    // 14. Validate at exact boundaries (kills > vs >= / == mutants)
    #[test]
    fn validate_exact_boundaries() {
        // Zero rejected
        assert!(ContainsBudget::new(0, 1024, 1024).is_err());
        assert!(ContainsBudget::new(1, 0, 1024).is_err());
        assert!(ContainsBudget::new(1, 1024, 0).is_err());
        // Exact hard caps accepted
        ContainsBudget::new(HARD_MAX_KEYS, 1024, 1024 * 1024).unwrap();
        ContainsBudget::new(1, HARD_MAX_KEY_BYTES, HARD_MAX_RESPONSE_BYTES).unwrap();
        ContainsBudget::new(1, 1024, HARD_MAX_RESPONSE_BYTES).unwrap();
        // One over hard cap rejected
        assert!(ContainsBudget::new(HARD_MAX_KEYS + 1, 1024, 1024 * 1024).is_err());
        assert!(ContainsBudget::new(1, HARD_MAX_KEY_BYTES + 1, HARD_MAX_RESPONSE_BYTES).is_err());
        assert!(ContainsBudget::new(1, 1024, HARD_MAX_RESPONSE_BYTES + 1).is_err());
        // max_response_bytes < max_key_bytes rejected, == accepted, > accepted
        assert!(ContainsBudget::new(1, 1024, 1023).is_err());
        ContainsBudget::new(1, 1024, 1024).unwrap();
        ContainsBudget::new(1, 1024, 1025).unwrap();
        assert!(ContainsBudget::new(1, 8192, 8191).is_err());
        ContainsBudget::new(1, 8192, 8192).unwrap();
    }

    // 15. Pagination at exact count/bytes boundaries (kills > vs >= and + vs * etc)
    #[test]
    fn pagination_at_exact_boundaries() {
        let ks = KernelSubspace::new();
        let tenant = TenantId::new(b"t".to_vec());
        let kv = MemKv::new();
        let keys: Vec<Vec<u8>> = (0..6).map(|i| format!("k{i}").into_bytes()).collect();
        // max_keys exactly = number of keys -> no continuation
        let bg_exact = budget(6, 1024, 1024 * 1024);
        let r = contains_many(&kv, &tenant, &ks, &keys, &bg_exact).unwrap();
        assert_eq!(r.missing.len(), 6);
        assert!(r.continuation.is_none());
        // max_keys one less -> continuation
        let bg_one_less = budget(5, 1024, 1024 * 1024);
        let r2 = contains_many(&kv, &tenant, &ks, &keys, &bg_one_less).unwrap();
        assert_eq!(r2.missing.len(), 5);
        assert!(r2.continuation.is_some());
        assert_eq!(r2.continuation.unwrap(), b"k5".to_vec());
        // response bytes exactly fits 2 keys of 2 bytes each (k0,k1)
        let bg_bytes_exact = ContainsBudget::new(100, 2, 4).unwrap(); // 2+2=4 fits
        let small_keys = vec![b"k0".to_vec(), b"k1".to_vec()];
        let r3 = contains_many(&kv, &tenant, &ks, &small_keys, &bg_bytes_exact).unwrap();
        assert_eq!(r3.missing.len(), 2);
        assert!(r3.continuation.is_none());
        // One byte less -> paginates
        let bg_bytes_one_less = ContainsBudget::new(100, 2, 3).unwrap();
        let r4 = contains_many(&kv, &tenant, &ks, &small_keys, &bg_bytes_one_less).unwrap();
        assert_eq!(r4.missing.len(), 1);
        assert!(r4.continuation.is_some());
        // Continuation filter: >= must include equal, not > only
        let bg = budget(10, 1024, 1024 * 1024);
        let cont = b"k2".to_vec();
        let r5 = contains_many_from(&kv, &tenant, &ks, &keys, Some(&cont), &bg).unwrap();
        assert!(r5.missing.contains(&b"k2".to_vec()));
        assert!(!r5.missing.contains(&b"k1".to_vec()));
    }

    // 16. State-machine property: pagination + dedup + ordering invariants
    #[test]
    fn state_machine_property_pagination_covers_all() {
        let ks = KernelSubspace::new();
        let tenant = TenantId::new(b"t".to_vec());
        let mut kv = MemKv::new();
        // Insert every even key
        for i in 0..20 {
            if i % 2 == 0 {
                put_opaque(&mut kv, &ks, &tenant, format!("k{:02}", i).as_bytes());
            }
        }
        let all_keys: Vec<Vec<u8>> = (0..20).map(|i| format!("k{:02}", i).into_bytes()).collect();
        let bg = budget(5, 1024, 1024 * 1024);
        // Collect via pagination
        let mut present_all = Vec::new();
        let mut missing_all = Vec::new();
        let mut cont: Option<Vec<u8>> = None;
        let mut pages = 0;
        loop {
            let r = contains_many_from(&kv, &tenant, &ks, &all_keys, cont.as_deref(), &bg).unwrap();
            // Each page's present and missing individually sorted; combined not interleaved
            let mut ps = r.present.clone();
            ps.sort();
            assert_eq!(ps, r.present, "present must be sorted");
            let mut ms = r.missing.clone();
            ms.sort();
            assert_eq!(ms, r.missing, "missing must be sorted");
            // No duplicates across present/missing
            for k in &r.present {
                assert!(!r.missing.contains(k));
            }
            present_all.extend(r.present.clone());
            missing_all.extend(r.missing.clone());
            if let Some(c) = r.continuation {
                cont = Some(c);
                pages += 1;
                assert!(pages < 10, "must converge");
            } else {
                break;
            }
        }
        // All keys accounted for exactly once
        let mut all_out = present_all.clone();
        all_out.extend(missing_all.clone());
        all_out.sort();
        let mut dedup_sorted = all_keys.clone();
        dedup_sorted.sort();
        assert_eq!(all_out, dedup_sorted);
        assert_eq!(present_all.len(), 10);
        assert_eq!(missing_all.len(), 10);
    }
}
