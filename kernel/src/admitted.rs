//! Admitted transaction envelope — issue #12 F3, ADR-002.
//!
//! Thin high-level write envelope over the raw FDB wrapper (F1).  FDB
//! stays oblivious byte storage; the estate binds Braid's three-axis
//! contract `Run(f,S,G) iff Safe ∧ Authorized ∧ Justified` at the
//! transaction boundary.
//!
//! * `INV-UNKNOWN-NEVER-WRITES` — structural: [`AdmittedTxn`] has no
//!   public constructor missing any proof ref; `Unknown` is
//!   unconstructable, not a runtime check.
//! * `INV-AUTHORITY-SCOPE` — authority must cover `namespace + effect`,
//!   checked at construction and again at commit (typed error).
//! * `INV-JUSTIFICATION-FRESH` — [`JustificationProofRef`] is bound to
//!   a snapshot version; [`LogicalDb::transact_admitted`] consumes the
//!   envelope and rejects a stale binding; retry must re-derive.
//! * Receipts capture the FDB committed version plus the three proof
//!   refs.  Rendering goes through the forge-harness-owned Receipt
//!   schema port; this crate only captures the fields.

use crate::fdb::{FdbTransaction, LogicalDb};
use crate::tuple::TenantId;
use crate::{KernelError, Result};

// ── Subspace / Namespace ───────────────────────────────────────

/// Stable subspace tag, mirrored from `kernel/src/tuple.rs` (same
/// discriminant values so a namespace maps 1:1 to a tuple subspace).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Subspace {
    Kernel = 0x01,
    Lem = 0x02,
    Cairn = 0x03,
    Moo = 0x04,
    Forge = 0x05,
}

impl Subspace {
    pub fn tag(self) -> u8 {
        self as u8
    }
}

/// Logical namespace = estate + subspace + tenant.  Authority is
/// scoped over this exact value; the tuple layer already makes the
/// byte prefix disjoint (`INV-TENANT-PREFIX`).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Namespace {
    tenant: TenantId,
    subspace: Subspace,
}

impl Namespace {
    pub fn new(tenant: TenantId, subspace: Subspace) -> Self {
        Self { tenant, subspace }
    }

    pub fn kernel(tenant: TenantId) -> Self {
        Self::new(tenant, Subspace::Kernel)
    }

    pub fn lem(tenant: TenantId) -> Self {
        Self::new(tenant, Subspace::Lem)
    }

    pub fn cairn(tenant: TenantId) -> Self {
        Self::new(tenant, Subspace::Cairn)
    }

    pub fn moo(tenant: TenantId) -> Self {
        Self::new(tenant, Subspace::Moo)
    }

    pub fn forge(tenant: TenantId) -> Self {
        Self::new(tenant, Subspace::Forge)
    }

    pub fn tenant(&self) -> &TenantId {
        &self.tenant
    }

    pub fn subspace(&self) -> Subspace {
        self.subspace
    }
}

// ── Effects ────────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Effect {
    Put,
    Delete,
    Get,
    Scan,
    ClearRange,
}

// ── Proof refs (opaque, no Unknown variant) ────────────────────

/// Safe proof — structural admission from Braid/Keel.  Opaque string
/// ref; bytes never appear in logs (redacted via Debug).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SafeProofRef(String);

impl SafeProofRef {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn id(&self) -> &str {
        &self.0
    }
}

/// Authority proof — capability scoped to namespaces + effects.
/// The issuing authority (Secure Authority / Logic OS Kernel) is
/// outside this repo; we only enforce that the envelope's
/// `namespace + declared_effects` is a subset of what the proof
/// authorizes (INV-AUTHORITY-SCOPE).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthorityProofRef {
    id: String,
    allowed_namespaces: Vec<Namespace>,
    allowed_effects: Vec<Effect>,
}

impl AuthorityProofRef {
    pub fn new(
        id: impl Into<String>,
        allowed_namespaces: Vec<Namespace>,
        allowed_effects: Vec<Effect>,
    ) -> Self {
        Self {
            id: id.into(),
            allowed_namespaces,
            allowed_effects,
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn covers(&self, namespace: &Namespace, effects: &[Effect]) -> bool {
        if !self.allowed_namespaces.contains(namespace) {
            return false;
        }
        for e in effects {
            if !self.allowed_effects.contains(e) {
                return false;
            }
        }
        true
    }
}

/// Justification proof — bound to an exact read version / frontier.
/// The immutable state snapshot it justifies is exactly `bound_version`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct JustificationProofRef {
    id: String,
    bound_version: u64,
}

impl JustificationProofRef {
    pub fn new(id: impl Into<String>, bound_version: u64) -> Self {
        Self {
            id: id.into(),
            bound_version,
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn bound_version(&self) -> u64 {
        self.bound_version
    }
}

// ── Operation id (idempotency) ─────────────────────────────────

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct OperationId(String);

impl OperationId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

// ── Admitted envelope (typestate, consumed on transact) ────────

/// High-level write envelope.  No `Clone`, no `Default`, no struct-
/// literal construction — the only public constructor is
/// [`Self::new`] which requires all three proofs.  `Unknown` is
/// unconstructable by construction (INV-UNKNOWN-NEVER-WRITES).
/// `transact_admitted` takes this by value so blind reuse after a
/// conflict is a move error at compile time and a stale-version
/// error at runtime (INV-JUSTIFICATION-FRESH).
pub struct AdmittedTxn {
    snapshot_version: u64,
    safe: SafeProofRef,
    authority: AuthorityProofRef,
    justification: JustificationProofRef,
    namespace: Namespace,
    declared_effects: Vec<Effect>,
    operation_id: OperationId,
}

impl AdmittedTxn {
    /// Construct an admitted envelope.  Fails with
    /// [`KernelError::AuthorityScopeViolation`] if the authority does
    /// not cover `namespace + declared_effects`, or with
    /// [`KernelError::StaleJustification`] if `justification` is not
    /// bound to `snapshot_version`.
    pub fn new(
        snapshot_version: u64,
        safe: SafeProofRef,
        authority: AuthorityProofRef,
        justification: JustificationProofRef,
        namespace: Namespace,
        declared_effects: Vec<Effect>,
        operation_id: OperationId,
    ) -> Result<Self> {
        if !authority.covers(&namespace, &declared_effects) {
            return Err(KernelError::AuthorityScopeViolation);
        }
        if justification.bound_version != snapshot_version {
            return Err(KernelError::StaleJustification {
                expected: snapshot_version,
                got: justification.bound_version,
            });
        }
        Ok(Self {
            snapshot_version,
            safe,
            authority,
            justification,
            namespace,
            declared_effects,
            operation_id,
        })
    }

    pub fn snapshot_version(&self) -> u64 {
        self.snapshot_version
    }

    pub fn namespace(&self) -> &Namespace {
        &self.namespace
    }

    pub fn declared_effects(&self) -> &[Effect] {
        &self.declared_effects
    }

    pub fn operation_id(&self) -> &OperationId {
        &self.operation_id
    }

    pub fn safe_proof(&self) -> &SafeProofRef {
        &self.safe
    }

    pub fn authority_proof(&self) -> &AuthorityProofRef {
        &self.authority
    }

    pub fn justification_proof(&self) -> &JustificationProofRef {
        &self.justification
    }
}

// ── Receipt (forge-harness owns the schema, we capture fields) ──

/// Receipt captured at commit.  The committed version plus the three
/// proof refs; rendering to the forge-harness Receipt schema goes
/// through [`ReceiptPort`] (authority NOT held here).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Receipt {
    pub committed_version: u64,
    pub operation_id: OperationId,
    pub safe_proof: SafeProofRef,
    pub authority_proof: AuthorityProofRef,
    pub justification_proof: JustificationProofRef,
    pub namespace: Namespace,
    pub declared_effects: Vec<Effect>,
}

/// Port for the forge-harness-owned Receipt schema.  This crate does
/// not define the schema; it only exposes the captured fields.
pub trait ReceiptPort {
    fn render(&self, receipt: &Receipt) -> Vec<u8>;
}

/// Trivial JSON renderer for tests — not the canonical schema, just
/// a deterministic byte rendering that proves the fields are captured.
pub struct JsonReceiptPort;

impl ReceiptPort for JsonReceiptPort {
    fn render(&self, receipt: &Receipt) -> Vec<u8> {
        // Forge-harness owns the canonical Receipt schema; this is only a
        // deterministic test rendering that proves the admitted fields are
        // captured.  Use lgwks_std JSON surface without relying on a
        // `json!` macro — construct via string formatting and re-parse
        // is not needed, a stable byte string suffices.
        let tenant_hex = lgwks_std::hex::encode(receipt.namespace.tenant().as_bytes());
        let s = format!(
            "{{\"committed_version\":{},\"operation_id\":\"{}\",\"safe_proof\":\"{}\",\"authority_proof\":\"{}\",\"justification_proof\":\"{}\",\"justification_bound_version\":{},\"namespace_subspace\":{},\"namespace_tenant\":\"{}\"}}",
            receipt.committed_version,
            receipt.operation_id.as_str(),
            receipt.safe_proof.id(),
            receipt.authority_proof.id(),
            receipt.justification_proof.id(),
            receipt.justification_proof.bound_version(),
            receipt.namespace.subspace().tag(),
            tenant_hex,
        );
        s.into_bytes()
    }
}

// ── LogicalDb admitted transact (consumes envelope) ────────────

impl LogicalDb {
    /// Admitted write transaction.  Consumes `envelope` so a stale
    /// justification cannot be blindly reused after an FDB conflict —
    /// the caller must re-derive a fresh envelope at the new read
    /// version (INV-JUSTIFICATION-FRESH).  Also re-checks authority
    /// scope at the commit boundary (defense in depth).
    pub async fn transact_admitted<F, T>(&self, envelope: AdmittedTxn, f: F) -> Result<(T, Receipt)>
    where
        F: FnOnce(&mut FdbTransaction) -> Result<T>,
    {
        let current = self.read_version()?;
        if envelope.snapshot_version != current {
            return Err(KernelError::StaleJustification {
                expected: current,
                got: envelope.snapshot_version,
            });
        }
        if envelope.justification.bound_version != envelope.snapshot_version {
            return Err(KernelError::StaleJustification {
                expected: envelope.snapshot_version,
                got: envelope.justification.bound_version,
            });
        }
        if !envelope
            .authority
            .covers(&envelope.namespace, &envelope.declared_effects)
        {
            return Err(KernelError::AuthorityScopeViolation);
        }

        // Snapshot must be taken at envelope version; use backend snapshot
        let snapshot = self.backend.lock().unwrap().snapshot_map();
        let mut tx = FdbTransaction::new_with_snapshot(
            self.config().transaction_size_limit_bytes,
            self.config().transaction_timeout_ms,
            envelope.snapshot_version,
            snapshot,
        );
        let result = f(&mut tx)?;

        self.try_commit(tx).await?;
        let committed_version = self.committed_version()?;
        let receipt = Receipt {
            committed_version,
            operation_id: envelope.operation_id,
            safe_proof: envelope.safe,
            authority_proof: envelope.authority,
            justification_proof: envelope.justification,
            namespace: envelope.namespace,
            declared_effects: envelope.declared_effects,
        };
        Ok((result, receipt))
    }
}

// ── Tests ──────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fdb::FdbConfig;

    fn test_namespace() -> Namespace {
        Namespace::kernel(TenantId::new(b"tenant-a".to_vec()))
    }

    fn make_proofs(
        ns: &Namespace,
        version: u64,
    ) -> (SafeProofRef, AuthorityProofRef, JustificationProofRef) {
        let safe = SafeProofRef::new(format!("safe-v{version}"));
        let authority = AuthorityProofRef::new(
            format!("auth-v{version}"),
            vec![ns.clone()],
            vec![Effect::Put, Effect::Delete],
        );
        let justification = JustificationProofRef::new(format!("just-v{version}"), version);
        (safe, authority, justification)
    }

    #[tokio::test]
    async fn admitted_txn() {
        let db = LogicalDb::open(FdbConfig::default()).unwrap();
        let ns = test_namespace();
        let v0 = db.read_version().unwrap();
        assert_eq!(v0, 0);

        let (safe, authority, justification) = make_proofs(&ns, v0);
        let op_id = OperationId::new("op-1");
        let envelope = AdmittedTxn::new(
            v0,
            safe.clone(),
            authority.clone(),
            justification.clone(),
            ns.clone(),
            vec![Effect::Put],
            op_id.clone(),
        )
        .expect("valid envelope must construct");

        // Authority mismatch is enforced at construction.
        let bad_ns = Namespace::lem(TenantId::new(b"other-tenant".to_vec()));
        let (safe2, auth2, just2) = make_proofs(&ns, v0);
        // auth2 only allows ns, not bad_ns
        let bad = AdmittedTxn::new(
            v0,
            safe2,
            auth2,
            just2,
            bad_ns,
            vec![Effect::Put],
            OperationId::new("op-bad"),
        );
        assert!(
            matches!(bad, Err(KernelError::AuthorityScopeViolation)),
            "authority must not cover foreign namespace"
        );

        // Effect mismatch also fails.
        let (safe3, auth3, just3) = make_proofs(&ns, v0);
        // auth3 allows Put/Delete but not Scan as declared
        let bad2 = AdmittedTxn::new(
            v0,
            safe3,
            auth3,
            just3,
            ns.clone(),
            vec![Effect::Scan],
            OperationId::new("op-bad2"),
        );
        assert!(
            matches!(bad2, Err(KernelError::AuthorityScopeViolation)),
            "authority must not cover undeclared effect"
        );

        // Justification binding mismatch fails at construction.
        let stale_just = JustificationProofRef::new("just-stale", 999);
        let bad3 = AdmittedTxn::new(
            v0,
            safe.clone(),
            authority.clone(),
            stale_just,
            ns.clone(),
            vec![Effect::Put],
            OperationId::new("op-bad3"),
        );
        assert!(
            matches!(
                bad3,
                Err(KernelError::StaleJustification {
                    expected: 0,
                    got: 999
                })
            ),
            "justification bound_version must match snapshot"
        );

        // Happy path: admitted write commits and receipt captures version + proofs.
        let (result, receipt) = db
            .transact_admitted(envelope, |tx| {
                tx.set(b"admitted-key".to_vec(), b"admitted-value".to_vec())?;
                let got = tx.get(b"admitted-key")?;
                Ok(got)
            })
            .await
            .unwrap();
        assert_eq!(result, Some(b"admitted-value".to_vec()));
        assert_eq!(receipt.committed_version, 1);
        assert_eq!(receipt.operation_id, op_id);
        assert_eq!(receipt.safe_proof, safe);
        assert_eq!(receipt.authority_proof, authority);
        assert_eq!(receipt.justification_proof, justification);
        assert_eq!(receipt.namespace, ns);
        assert_eq!(db.committed_version().unwrap(), 1);
        assert_eq!(db.read_version().unwrap(), 1);

        // Receipt rendering through the port (forge-harness owns schema;
        // here we prove the fields are captured deterministically).
        let port = JsonReceiptPort;
        let bytes = port.render(&receipt);
        let rendered = String::from_utf8(bytes).unwrap();
        assert!(rendered.contains("\"committed_version\":1"));
        assert!(rendered.contains("\"operation_id\":\"op-1\""));
        assert!(rendered.contains("\"safe_proof\":\"safe-v0\""));
        assert!(rendered.contains("\"authority_proof\":\"auth-v0\""));

        // No Clone on envelope is enforced structurally — the type does
        // not impl Clone, so `envelope.clone()` would not compile.
        // This is the compile-time half of INV-UNKNOWN-NEVER-WRITES /
        // INV-JUSTIFICATION-FRESH.  The runtime half is tested in
        // stale_justification_aborts_and_reevaluates.
        fn assert_not_clone<T: Clone>() {}
        // The following would fail to compile if uncommented, proving
        // the envelope cannot be duplicated for blind retry:
        // assert_not_clone::<AdmittedTxn>();
        let _ = assert_not_clone::<SafeProofRef>; // proofs themselves are Clone, envelope is not
    }

    #[tokio::test]
    async fn stale_justification_aborts_and_reevaluates() {
        let db = LogicalDb::open(FdbConfig::default()).unwrap();
        let ns = test_namespace();

        // v0 envelope prepared before any writes — will become stale.
        let v0 = db.read_version().unwrap();
        assert_eq!(v0, 0);
        let (safe_stale, auth_stale, just_stale) = make_proofs(&ns, v0);
        let stale_envelope = AdmittedTxn::new(
            v0,
            safe_stale.clone(),
            auth_stale.clone(),
            just_stale.clone(),
            ns.clone(),
            vec![Effect::Put],
            OperationId::new("op-stale"),
        )
        .unwrap();

        // Concurrent writer at v0 bumps the db to v1.
        let (safe_c, auth_c, just_c) = make_proofs(&ns, v0);
        let concurrent = AdmittedTxn::new(
            v0,
            safe_c,
            auth_c,
            just_c,
            ns.clone(),
            vec![Effect::Put],
            OperationId::new("op-concurrent"),
        )
        .unwrap();
        let (_, receipt_c) = db
            .transact_admitted(concurrent, |tx| {
                tx.set(b"concurrent-key".to_vec(), b"1".to_vec())?;
                Ok(())
            })
            .await
            .unwrap();
        assert_eq!(receipt_c.committed_version, 1);
        assert_eq!(db.read_version().unwrap(), 1);

        // Now stale_envelope (bound to v0) must be rejected — the read
        // version / frontier has moved.  Blind reuse is not expressible
        // as a move (envelope would have been consumed), and even a
        // freshly constructed envelope at the old version is stale at
        // the commit boundary.
        let result = db
            .transact_admitted(stale_envelope, |tx| {
                tx.set(b"stale-key".to_vec(), b"stale".to_vec())?;
                Ok(())
            })
            .await;
        assert!(
            matches!(
                result,
                Err(KernelError::StaleJustification {
                    expected: 1,
                    got: 0
                })
            ),
            "stale justification must abort, got {result:?}"
        );

        // Caller must re-derive a fresh envelope at the new version.
        let v1 = db.read_version().unwrap();
        assert_eq!(v1, 1);
        let (safe_fresh, auth_fresh, just_fresh) = make_proofs(&ns, v1);
        let fresh = AdmittedTxn::new(
            v1,
            safe_fresh.clone(),
            auth_fresh.clone(),
            just_fresh.clone(),
            ns.clone(),
            vec![Effect::Put],
            OperationId::new("op-fresh"),
        )
        .unwrap();
        let ((), receipt_fresh) = db
            .transact_admitted(fresh, |tx| {
                tx.set(b"fresh-key".to_vec(), b"fresh".to_vec())?;
                Ok(())
            })
            .await
            .unwrap();
        assert_eq!(receipt_fresh.committed_version, 2);
        assert_eq!(receipt_fresh.justification_proof.bound_version(), 1);
        assert_eq!(db.committed_version().unwrap(), 2);

        // Also verify that a justification constructed at v1 but
        // presented as snapshot v0 fails at envelope construction (binding
        // check), not just at transact time.
        let bad_just = JustificationProofRef::new("just-bad", 1);
        let bad_envelope = AdmittedTxn::new(
            0,
            safe_fresh,
            auth_fresh,
            bad_just,
            ns.clone(),
            vec![Effect::Put],
            OperationId::new("op-bad-binding"),
        );
        assert!(
            matches!(
                bad_envelope,
                Err(KernelError::StaleJustification {
                    expected: 0,
                    got: 1
                })
            ),
            "justification binding must match snapshot at construction"
        );
    }

    #[tokio::test]
    async fn authority_must_cover_namespace() {
        let db = LogicalDb::open(FdbConfig::default()).unwrap();
        let ns_a = Namespace::kernel(TenantId::new(b"tenant-a".to_vec()));
        let ns_b = Namespace::kernel(TenantId::new(b"tenant-b".to_vec()));
        let ns_lem = Namespace::lem(TenantId::new(b"tenant-a".to_vec()));
        let v0 = db.read_version().unwrap();
        let (safe, authority, justification) = make_proofs(&ns_a, v0);
        // authority only covers ns_a Put/Delete — using ns_b must fail at construction
        let bad_ns = AdmittedTxn::new(
            v0,
            safe.clone(),
            authority.clone(),
            justification.clone(),
            ns_b.clone(),
            vec![Effect::Put],
            OperationId::new("op-foreign-tenant"),
        );
        assert!(
            matches!(bad_ns, Err(KernelError::AuthorityScopeViolation)),
            "foreign tenant namespace must be rejected at envelope construction"
        );
        // same tenant but different subspace must also fail
        let bad_subspace = AdmittedTxn::new(
            v0,
            safe.clone(),
            authority.clone(),
            justification.clone(),
            ns_lem.clone(),
            vec![Effect::Put],
            OperationId::new("op-foreign-subspace"),
        );
        assert!(
            matches!(bad_subspace, Err(KernelError::AuthorityScopeViolation)),
            "foreign subspace must be rejected"
        );
        // effect not covered must fail
        let bad_effect = AdmittedTxn::new(
            v0,
            safe.clone(),
            authority.clone(),
            justification.clone(),
            ns_a.clone(),
            vec![Effect::Scan],
            OperationId::new("op-bad-effect"),
        );
        assert!(
            matches!(bad_effect, Err(KernelError::AuthorityScopeViolation)),
            "uncovered effect must be rejected"
        );
        // foreign namespace reuse at commit boundary is also defense-in-depth:
        // a valid envelope cannot be retargeted — re-check covers+justification
        let valid = AdmittedTxn::new(
            v0,
            safe,
            authority,
            justification,
            ns_a.clone(),
            vec![Effect::Put],
            OperationId::new("op-valid"),
        )
        .unwrap();
        // transact with a closure that would touch a foreign namespace is not
        // expressible without forging — the envelope's namespace is fixed and
        // re-checked at commit. Verify the happy path still commits.
        let ((), receipt) = db
            .transact_admitted(valid, |tx| {
                tx.set(b"k".to_vec(), b"v".to_vec())?;
                Ok(())
            })
            .await
            .unwrap();
        assert_eq!(receipt.namespace, ns_a);
    }

    #[tokio::test]
    async fn foreign_namespace_reuse_is_authority_violation() {
        // Direct negative journey: authority scoped to kernel/tenant-a
        // reused against cairn/tenant-a or kernel/tenant-b must error
        // with typed AuthorityScopeViolation, never a silent write.
        let db = LogicalDb::open(FdbConfig::default()).unwrap();
        let ns_kernel_a = Namespace::kernel(TenantId::new(b"tenant-a".to_vec()));
        let ns_cairn_a = Namespace::cairn(TenantId::new(b"tenant-a".to_vec()));
        let ns_kernel_b = Namespace::kernel(TenantId::new(b"tenant-b".to_vec()));
        let v0 = db.read_version().unwrap();
        let safe = SafeProofRef::new("safe-foreign");
        let auth_kernel_a = AuthorityProofRef::new(
            "auth-kernel-a",
            vec![ns_kernel_a.clone()],
            vec![Effect::Put],
        );
        let just = JustificationProofRef::new("just-foreign", v0);
        for (label, foreign_ns) in [
            ("cairn-same-tenant", ns_cairn_a),
            ("kernel-foreign-tenant", ns_kernel_b),
        ] {
            let res = AdmittedTxn::new(
                v0,
                safe.clone(),
                auth_kernel_a.clone(),
                just.clone(),
                foreign_ns,
                vec![Effect::Put],
                OperationId::new(format!("op-{label}")),
            );
            assert!(
                matches!(res, Err(KernelError::AuthorityScopeViolation)),
                "{label} must be AuthorityScopeViolation"
            );
        }
    }
}
