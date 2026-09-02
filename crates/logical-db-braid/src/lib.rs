//! Braid runnable invocations bound to exact Logical-DB requests and frontiers.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
//! S33 S34 S35 S36 S40 S41. Source URLs are in `evidence/SOURCES.md`.

#![forbid(unsafe_code)]

use std::{collections::BTreeMap, fmt::Write as _};

use braid_capability::Capability;
use braid_flow_ir::{FactRef, Predicate, ValueExpr};
use braid_flow_plan::{CompletionMap, FlowSnapshot};
use braid_ir::{
    Capsule, TermRegistry, Value,
    term::{EffectClass, Exposure, TermSpec, TypeTag},
};
use braid_run::{JustificationProof, RunnableInvocation};
use braid_sdk::Builder;
use braid_verify::{Verdict, admit};
use logical_db_model::{Effect, OperationDigest, Scope};
use logical_db_service::{Authority, ExecutionGrant};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// Braid capability prefix authorizing structured DB reads.
pub const READ_CAPABILITY: &str = "logical_db.read";
/// Braid capability prefix authorizing structured DB writes.
pub const WRITE_CAPABILITY: &str = "logical_db.write";
/// Braid capability prefix authorizing replica synchronization.
pub const SYNC_CAPABILITY: &str = "logical_db.sync";

const SCOPE_FACT: &str = "logical_db.scope";
const EFFECT_FACT: &str = "logical_db.effect";
const REQUEST_FACT: &str = "logical_db.request";
const FRONTIER_FACT: &str = "logical_db.frontier";

/// Exact logical facts and expiry supplied to Braid preparation as one value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RequestBinding {
    scope: Scope,
    effect: Effect,
    request: OperationDigest,
    frontier: u64,
    expires_at_unix_ms: u64,
}

impl RequestBinding {
    /// Binds one request to its observed durable frontier and exclusive expiry.
    #[must_use]
    pub const fn new(
        scope: Scope,
        effect: Effect,
        request: OperationDigest,
        frontier: u64,
        expires_at_unix_ms: u64,
    ) -> Self {
        Self {
            scope,
            effect,
            request,
            frontier,
            expires_at_unix_ms,
        }
    }
}

/// Constructs the exact Braid capability for one DB request.
#[must_use]
pub fn capability_for(scope: Scope, effect: Effect, request: OperationDigest) -> Capability {
    let prefix = match effect {
        Effect::Read | Effect::Scan => READ_CAPABILITY,
        Effect::Write => WRITE_CAPABILITY,
        Effect::Sync => SYNC_CAPABILITY,
    };
    let mut request_hex = String::with_capacity(64);
    for byte in request.into_bytes() {
        write!(&mut request_hex, "{byte:02x}").expect("writing to String is infallible");
    }
    Capability::new(format!(
        "{prefix}.{}.{}.{request_hex}",
        scope.account(),
        scope.space()
    ))
}

/// Opaque verifier/planner product passed unchanged into the store transaction.
#[derive(Clone, Debug)]
pub struct BraidGrant {
    runnable: RunnableInvocation,
    scope: Scope,
    effect: Effect,
    request: OperationDigest,
    frontier: u64,
    expires_at_unix_ms: u64,
    invocation_id: [u8; 32],
}

impl BraidGrant {
    /// Returns the actual verifier-owned invocation retained by this grant.
    #[must_use]
    pub const fn runnable(&self) -> &RunnableInvocation {
        &self.runnable
    }

    fn permits(
        &self,
        scope: Scope,
        effect: Effect,
        request: OperationDigest,
        frontier: u64,
        now_unix_ms: u64,
    ) -> bool {
        self.scope == scope
            && self.effect == effect
            && self.request == request
            && self.frontier == frontier
            && now_unix_ms < self.expires_at_unix_ms
    }
}

impl ExecutionGrant for BraidGrant {
    fn scope(&self) -> Scope {
        self.scope
    }
    fn effect(&self) -> Effect {
        self.effect
    }
    fn request(&self) -> OperationDigest {
        self.request
    }
    fn frontier(&self) -> u64 {
        self.frontier
    }
    fn invocation_id(&self) -> [u8; 32] {
        self.invocation_id
    }
}

/// Turns independently supplied canonical Braid material into an exact runnable grant.
///
/// The proof snapshot binds scope, strongest effect, request digest, and the durable
/// tuple-space frontier. Any change to those facts changes the invocation identity.
///
/// # Errors
///
/// Refuses failed admission, noncanonical/mismatched capsule material, an
/// unrepresentable frontier, or any failed snapshot justification/binding.
pub fn prepare_request(
    capsule_bytes: &[u8],
    registry: &TermRegistry,
    ambient: &[Capability],
    binding: RequestBinding,
) -> Result<BraidGrant, BraidAdapterError> {
    let RequestBinding {
        scope,
        effect,
        request,
        frontier,
        expires_at_unix_ms,
    } = binding;
    let admission = admit(capsule_bytes, registry, ambient).map_err(BraidAdapterError::Rejected)?;
    let capsule = Capsule::from_bytes(capsule_bytes).map_err(BraidAdapterError::Capsule)?;
    let required = capability_for(scope, effect, request);
    if !capsule.grants.iter().any(|grant| grant == &required) {
        return Err(BraidAdapterError::MissingExactGrant);
    }
    let frontier_value =
        i64::try_from(frontier).map_err(|_| BraidAdapterError::Frontier(frontier))?;
    let facts = request_facts(scope, effect, request, frontier_value);
    let snapshot = FlowSnapshot::new(facts.clone());
    let predicate = request_predicate(&facts)?;
    let justification =
        JustificationProof::prove(&capsule, &predicate, &snapshot, &CompletionMap::new())
            .map_err(BraidAdapterError::Execution)?;
    let runnable = RunnableInvocation::prepare(
        capsule,
        registry,
        admission,
        justification,
        ambient,
        &snapshot,
    )
    .map_err(BraidAdapterError::Execution)?;
    let invocation_id = invocation_id(&runnable);
    Ok(BraidGrant {
        runnable,
        scope,
        effect,
        request,
        frontier,
        expires_at_unix_ms,
        invocation_id,
    })
}

fn request_facts(
    scope: Scope,
    effect: Effect,
    request: OperationDigest,
    frontier: i64,
) -> BTreeMap<String, Value> {
    let mut scope_bytes = Vec::with_capacity(32);
    scope_bytes.extend_from_slice(scope.account().as_uuid().as_bytes());
    scope_bytes.extend_from_slice(scope.space().as_uuid().as_bytes());
    BTreeMap::from([
        (SCOPE_FACT.into(), Value::Bytes(scope_bytes)),
        (EFFECT_FACT.into(), Value::Text(effect_name(effect).into())),
        (
            REQUEST_FACT.into(),
            Value::Bytes(request.into_bytes().to_vec()),
        ),
        (FRONTIER_FACT.into(), Value::Int(frontier)),
    ])
}

fn request_predicate(facts: &BTreeMap<String, Value>) -> Result<Predicate, BraidAdapterError> {
    let mut clauses = Vec::with_capacity(facts.len());
    for (name, value) in facts {
        let fact = FactRef::new(name).map_err(BraidAdapterError::Fact)?;
        clauses.push(Predicate::Eq(
            ValueExpr::SnapshotFact(fact),
            ValueExpr::Literal(value.clone()),
        ));
    }
    Ok(Predicate::And(clauses))
}

fn invocation_id(runnable: &RunnableInvocation) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"logical-db/braid-invocation/v1\0");
    digest.update(runnable.capsule_cid().0);
    digest.update(runnable.registry_cid().0);
    digest.update(runnable.authority_cid().0);
    digest.update(runnable.justification().snapshot_cid().0);
    digest.update(runnable.justification().predicate_cid().0);
    digest.finalize().into()
}

fn effect_name(effect: Effect) -> &'static str {
    match effect {
        Effect::Read => "read",
        Effect::Scan => "scan",
        Effect::Write => "write",
        Effect::Sync => "sync",
    }
}

/// Generates the minimal one-term registry and capsule used by the standalone host.
///
/// This is not an allow-boolean shortcut: every call creates, admits, justifies,
/// and prepares a real [`RunnableInvocation`] over exact request/frontier facts.
///
/// # Errors
///
/// Returns a typed registry, builder, admission, fact, or runnable-proof failure.
pub fn prepare_local_request(
    scope: Scope,
    effect: Effect,
    request: OperationDigest,
    frontier: u64,
    expires_at_unix_ms: u64,
) -> Result<BraidGrant, BraidAdapterError> {
    let capability = capability_for(scope, effect, request);
    let mut registry = TermRegistry::new(1);
    registry
        .insert(TermSpec {
            id: "logical_db.invoke".into(),
            inputs: Vec::new(),
            output: TypeTag::Bool,
            capability: Some(capability.clone()),
            effect: match effect {
                Effect::Write => EffectClass::ReversibleWrite,
                Effect::Read | Effect::Scan | Effect::Sync => EffectClass::Read,
            },
            source_exposure: Exposure::Internal,
            egress_ceiling: None,
            cost: 1,
        })
        .map_err(BraidAdapterError::Registry)?;
    let mut builder = Builder::new(&registry, "execute exact Logical-DB request");
    let result = builder
        .strand("logical_db.invoke", &[])
        .map_err(BraidAdapterError::Build)?;
    builder.output(result).budget_tight();
    let capsule = builder.build().map_err(BraidAdapterError::Build)?;
    let bytes = capsule.to_bytes();
    prepare_request(
        &bytes,
        &registry,
        &[capability],
        RequestBinding::new(scope, effect, request, frontier, expires_at_unix_ms),
    )
}

/// Authenticated scope policy that produces a new exact Braid invocation per request.
pub struct LocalBraidAuthority {
    scope: Scope,
    expires_at_unix_ms: u64,
}

impl LocalBraidAuthority {
    /// Limits locally generated invocations to one authenticated scope and expiry.
    #[must_use]
    pub const fn new(scope: Scope, expires_at_unix_ms: u64) -> Self {
        Self {
            scope,
            expires_at_unix_ms,
        }
    }
}

impl Authority for LocalBraidAuthority {
    type Grant = BraidGrant;

    fn authorize(
        &self,
        scope: Scope,
        effect: Effect,
        request: OperationDigest,
        frontier: u64,
        now_unix_ms: u64,
    ) -> Option<Self::Grant> {
        if scope != self.scope
            || !matches!(effect, Effect::Write | Effect::Sync)
            || now_unix_ms >= self.expires_at_unix_ms
        {
            return None;
        }
        prepare_local_request(scope, effect, request, frontier, self.expires_at_unix_ms).ok()
    }
}

/// Bounded externally prepared runnable grant collection.
pub struct BraidAuthority {
    grants: Vec<BraidGrant>,
}

impl BraidAuthority {
    /// Refuses an empty or excessive invocation set.
    ///
    /// # Errors
    ///
    /// Returns [`BraidAdapterError::GrantCount`] outside `1..=256`.
    pub fn new(grants: Vec<BraidGrant>) -> Result<Self, BraidAdapterError> {
        if grants.is_empty() || grants.len() > 256 {
            return Err(BraidAdapterError::GrantCount(grants.len()));
        }
        Ok(Self { grants })
    }
}

impl Authority for BraidAuthority {
    type Grant = BraidGrant;

    fn authorize(
        &self,
        scope: Scope,
        effect: Effect,
        request: OperationDigest,
        frontier: u64,
        now_unix_ms: u64,
    ) -> Option<Self::Grant> {
        self.grants
            .iter()
            .find(|grant| grant.permits(scope, effect, request, frontier, now_unix_ms))
            .cloned()
    }
}

/// Fail-closed Braid integration failures.
#[derive(Debug, Error)]
pub enum BraidAdapterError {
    /// Independent admission rejected the canonical capsule.
    #[error("Braid admission rejected the capsule: {0:?}")]
    Rejected(Verdict),
    /// Canonical capsule bytes did not decode.
    #[error("Braid capsule decoding failed: {0}")]
    Capsule(braid_ir::capsule::CapsuleError),
    /// Capsule grant set omitted the exact scope/effect/request capability.
    #[error("Braid capsule lacks the exact scoped request capability")]
    MissingExactGrant,
    /// Frontier exceeded Braid's canonical fixed integer domain.
    #[error("Braid snapshot frontier {0} exceeds its canonical integer domain")]
    Frontier(u64),
    /// Snapshot fact name violated Braid identifier rules.
    #[error("Braid snapshot fact name is invalid: {0}")]
    Fact(braid_flow_ir::IdentifierError),
    /// Minimal standalone registry violated Braid invariants.
    #[error("Braid registry construction failed: {0}")]
    Registry(braid_ir::term::RegistryError),
    /// Minimal standalone capsule violated Braid invariants.
    #[error("Braid capsule construction failed: {0}")]
    Build(braid_sdk::BuildError),
    /// Justification or runnable preparation failed.
    #[error("Braid runnable proof failed: {0}")]
    Execution(braid_run::ExecutionError),
    /// External grant set violated its resource bound.
    #[error("Braid grant count {0} is outside 1..=256")]
    GrantCount(usize),
}

#[cfg(test)]
mod tests {
    use logical_db_model::{AccountId, SpaceId};

    use super::*;

    #[test]
    fn real_runnable_is_bound_to_scope_effect_request_frontier_and_expiry() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let request = OperationDigest::new([9; 32]);
        let grant = prepare_local_request(scope, Effect::Write, request, 7, 100).unwrap();
        assert_eq!(ExecutionGrant::scope(&grant), scope);
        assert_eq!(ExecutionGrant::effect(&grant), Effect::Write);
        assert_eq!(ExecutionGrant::request(&grant), request);
        assert_eq!(ExecutionGrant::frontier(&grant), 7);
        assert!(grant.permits(scope, Effect::Write, request, 7, 99));
        assert!(!grant.permits(scope, Effect::Write, request, 8, 99));
        assert!(!grant.permits(scope, Effect::Sync, request, 7, 99));
        assert!(!grant.permits(scope, Effect::Write, OperationDigest::new([8; 32]), 7, 99));
        assert!(!grant.permits(scope, Effect::Write, request, 7, 100));
    }

    #[test]
    fn invocation_identity_changes_with_frontier_and_request() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let first =
            prepare_local_request(scope, Effect::Write, OperationDigest::new([1; 32]), 1, 9)
                .unwrap();
        let frontier =
            prepare_local_request(scope, Effect::Write, OperationDigest::new([1; 32]), 2, 9)
                .unwrap();
        let request =
            prepare_local_request(scope, Effect::Write, OperationDigest::new([2; 32]), 1, 9)
                .unwrap();
        assert_ne!(first.invocation_id(), frontier.invocation_id());
        assert_ne!(first.invocation_id(), request.invocation_id());
    }

    #[test]
    fn local_policy_generates_only_exact_authenticated_runnable_grants() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let policy = LocalBraidAuthority::new(scope, 20);
        let request = OperationDigest::new([4; 32]);
        assert!(
            policy
                .authorize(scope, Effect::Write, request, 3, 19)
                .is_some()
        );
        assert!(
            policy
                .authorize(scope, Effect::Read, request, 3, 19)
                .is_none()
        );
        assert!(
            policy
                .authorize(scope, Effect::Write, request, 3, 20)
                .is_none()
        );
    }

    #[test]
    fn effect_fact_names_and_external_authority_lookup_are_exact() {
        assert_eq!(effect_name(Effect::Read), "read");
        assert_eq!(effect_name(Effect::Scan), "scan");
        assert_eq!(effect_name(Effect::Write), "write");
        assert_eq!(effect_name(Effect::Sync), "sync");

        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let request = OperationDigest::new([6; 32]);
        let grant = prepare_local_request(scope, Effect::Write, request, 11, 30).unwrap();
        let expected_id = grant.invocation_id();
        let authority = BraidAuthority::new(vec![grant]).unwrap();
        assert_eq!(
            authority
                .authorize(scope, Effect::Write, request, 11, 29)
                .unwrap()
                .invocation_id(),
            expected_id
        );
        assert!(
            authority
                .authorize(scope, Effect::Write, request, 12, 29)
                .is_none()
        );
    }
}
