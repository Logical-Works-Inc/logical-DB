//! Braid runnable invocations bound to exact Logical-DB requests and frontiers.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
//! S33 S34 S35 S36 S40 S41. Source URLs are in `evidence/SOURCES.md`.

#![forbid(unsafe_code)]

use std::collections::BTreeMap;

use braid_capability::Capability;
use braid_flow_ir::{FactRef, Predicate, ValueExpr};
use braid_flow_plan::{CompletionMap, FlowSnapshot};
use braid_ir::{
    Capsule, ConfirmPolicy, TermRegistry, Value,
    term::{EffectClass, Exposure, TermSpec, TypeTag},
};
use braid_run::{JustificationProof, RunnableInvocation};
use braid_sdk::Builder;
use braid_verify::{Verdict, admit};
use lgwks_std::hash::Hasher;
use logical_db_model::{ActionDigest, Effect, Scope};
use logical_db_service::{Authority, ExecutionGrant};
use thiserror::Error;

/// Braid capability prefix authorizing structured DB reads.
pub const READ_CAPABILITY: &str = "logical_db.read";
/// Braid capability prefix authorizing structured DB scans.
pub const SCAN_CAPABILITY: &str = "logical_db.scan";
/// Braid capability prefix authorizing structured DB writes.
pub const WRITE_CAPABILITY: &str = "logical_db.write";
/// Braid capability prefix authorizing replica synchronization.
pub const SYNC_CAPABILITY: &str = "logical_db.sync";

const SCOPE_FACT: &str = "logical_db.scope";
const EFFECT_FACT: &str = "logical_db.effect";
const ACTION_FACT: &str = "logical_db.action.v1";
const FRONTIER_FACT: &str = "logical_db.frontier";
const EXPIRY_FACT: &str = "logical_db.expires_at_unix_ms";

/// Exact logical facts and expiry supplied to Braid preparation as one value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ActionBinding {
    scope: Scope,
    effect: Effect,
    action: ActionDigest,
    frontier: u64,
    expires_at_unix_ms: u64,
}

impl ActionBinding {
    /// Binds one action to its observed durable frontier and exclusive expiry.
    #[must_use]
    pub const fn new(
        scope: Scope,
        effect: Effect,
        action: ActionDigest,
        frontier: u64,
        expires_at_unix_ms: u64,
    ) -> Self {
        Self {
            scope,
            effect,
            action,
            frontier,
            expires_at_unix_ms,
        }
    }
}

/// Constructs the exact Braid capability for one DB action.
#[must_use]
pub fn capability_for(scope: Scope, effect: Effect, action: ActionDigest) -> Capability {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let prefix = match effect {
        Effect::Read => READ_CAPABILITY,
        Effect::Scan => SCAN_CAPABILITY,
        Effect::Write => WRITE_CAPABILITY,
        Effect::Sync => SYNC_CAPABILITY,
    };
    let mut action_hex = String::with_capacity(64);
    for byte in action.into_bytes() {
        action_hex.push(HEX[usize::from(byte >> 4)] as char);
        action_hex.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    Capability::new(format!(
        "{prefix}.{}.{}.{action_hex}",
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
    action: ActionDigest,
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
        action: ActionDigest,
        frontier: u64,
        now_unix_ms: u64,
    ) -> bool {
        self.scope == scope
            && self.effect == effect
            && self.action == action
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
    fn action(&self) -> ActionDigest {
        self.action
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
/// The proof snapshot binds scope, strongest effect, action digest, and the durable
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
    binding: ActionBinding,
) -> Result<BraidGrant, BraidAdapterError> {
    let ActionBinding {
        scope,
        effect,
        action,
        frontier,
        expires_at_unix_ms,
    } = binding;
    let admission = admit(capsule_bytes, registry, ambient).map_err(BraidAdapterError::Rejected)?;
    let capsule = Capsule::from_bytes(capsule_bytes).map_err(BraidAdapterError::Capsule)?;
    let required = capability_for(scope, effect, action);
    if !capsule.grants.iter().any(|grant| grant == &required) {
        eprintln!("logical-db-braid: prepare_request: MissingExactGrant");
        return Err(BraidAdapterError::MissingExactGrant);
    }
    validate_operation_policy(&capsule, registry, &required, effect)?;
    let frontier_value = i64::try_from(frontier).map_err(|error| {
        eprintln!("logical-db-braid: BraidAdapterError::Frontier: {error}");
        BraidAdapterError::Frontier(frontier)
    })?;
    let expiry_value = i64::try_from(expires_at_unix_ms).map_err(|error| {
        eprintln!("logical-db-braid: BraidAdapterError::Expiry: {error}");
        BraidAdapterError::Expiry(expires_at_unix_ms)
    })?;
    let facts = action_facts(scope, effect, action, frontier_value, expiry_value);
    let snapshot = FlowSnapshot::new(facts.clone());
    let predicate = action_predicate(&facts)?;
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
        action,
        frontier,
        expires_at_unix_ms,
        invocation_id,
    })
}

fn validate_operation_policy(
    capsule: &Capsule,
    registry: &TermRegistry,
    required: &Capability,
    effect: Effect,
) -> Result<(), BraidAdapterError> {
    let (required_effect, required_ceiling) = operation_policy(effect);
    if matches!(
        required_effect,
        EffectClass::Irreversible | EffectClass::Egress
    ) && capsule.confirm != ConfirmPolicy::HumanConfirm
    {
        eprintln!("logical-db-braid: validate_operation_policy: EffectPolicyMismatch");
        return Err(BraidAdapterError::EffectPolicyMismatch);
    }
    let mut exposures = Vec::with_capacity(capsule.braid.strands.len());
    let mut matched = false;
    for strand in &capsule.braid.strands {
        let spec = registry
            .get(&strand.term)
            .ok_or(BraidAdapterError::EffectPolicyMismatch)?;
        let incoming = strand
            .inputs
            .iter()
            .try_fold(Exposure::Public, |value, index| {
                exposures
                    .get(*index as usize)
                    .copied()
                    .map(|exposure| value.max(exposure))
                    .ok_or(BraidAdapterError::EffectPolicyMismatch)
            })?;
        if spec.capability.as_ref() != Some(required) {
            exposures.push(spec.source_exposure.max(incoming));
            continue;
        }
        if spec.effect != required_effect
            || spec.egress_ceiling != required_ceiling
            || incoming != Exposure::Vault
        {
            eprintln!("logical-db-braid: validate_operation_policy: EffectPolicyMismatch");
            return Err(BraidAdapterError::EffectPolicyMismatch);
        }
        matched = true;
        exposures.push(spec.source_exposure.max(incoming));
    }
    if matched {
        Ok(())
    } else {
        Err(BraidAdapterError::EffectPolicyMismatch)
    }
}

fn action_facts(
    scope: Scope,
    effect: Effect,
    action: ActionDigest,
    frontier: i64,
    expires_at_unix_ms: i64,
) -> BTreeMap<String, Value> {
    let mut scope_bytes = Vec::with_capacity(32);
    scope_bytes.extend_from_slice(scope.account().as_uuid().as_bytes());
    scope_bytes.extend_from_slice(scope.space().as_uuid().as_bytes());
    BTreeMap::from([
        (SCOPE_FACT.into(), Value::Bytes(scope_bytes)),
        (EFFECT_FACT.into(), Value::Text(effect_name(effect).into())),
        (
            ACTION_FACT.into(),
            Value::Bytes(action.into_bytes().to_vec()),
        ),
        (FRONTIER_FACT.into(), Value::Int(frontier)),
        (EXPIRY_FACT.into(), Value::Int(expires_at_unix_ms)),
    ])
}

fn action_predicate(facts: &BTreeMap<String, Value>) -> Result<Predicate, BraidAdapterError> {
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
    let mut digest = Hasher::new();
    digest.update(b"logical-db/braid-invocation/v1\0");
    digest.update(&runnable.capsule_cid().0);
    digest.update(&runnable.registry_cid().0);
    digest.update(&runnable.authority_cid().0);
    digest.update(&runnable.justification().snapshot_cid().0);
    digest.update(&runnable.justification().predicate_cid().0);
    *digest.finalize().as_bytes()
}

fn effect_name(effect: Effect) -> &'static str {
    match effect {
        Effect::Read => "read",
        Effect::Scan => "scan",
        Effect::Write => "write",
        Effect::Sync => "sync",
    }
}

/// Generates minimal local material for non-dangerous read and scan effects.
///
/// This is not an allow-boolean shortcut: successful calls create, admit,
/// justify, and prepare a real [`RunnableInvocation`] over exact facts.
/// Irreversible and egress effects fail because this function cannot supply
/// independently prepared confirmation evidence.
///
/// # Errors
///
/// Returns a typed registry, builder, admission, fact, or runnable-proof failure.
pub fn prepare_local_request(
    scope: Scope,
    effect: Effect,
    action: ActionDigest,
    frontier: u64,
    expires_at_unix_ms: u64,
) -> Result<BraidGrant, BraidAdapterError> {
    let capability = capability_for(scope, effect, action);
    let mut registry = TermRegistry::new(1);
    registry
        .insert(TermSpec {
            id: "logical_db.bound_request".into(),
            inputs: Vec::new(),
            output: TypeTag::Bool,
            capability: None,
            effect: EffectClass::Pure,
            source_exposure: Exposure::Vault,
            egress_ceiling: None,
            cost: 1,
        })
        .map_err(BraidAdapterError::Registry)?;
    let (operation_effect, egress_ceiling) = operation_policy(effect);
    registry
        .insert(TermSpec {
            id: "logical_db.invoke".into(),
            inputs: vec![TypeTag::Bool],
            output: TypeTag::Bool,
            capability: Some(capability.clone()),
            effect: operation_effect,
            source_exposure: Exposure::Vault,
            egress_ceiling,
            cost: 1,
        })
        .map_err(BraidAdapterError::Registry)?;
    let mut builder = Builder::new(&registry, "execute exact Logical-DB action");
    let bound_request = builder
        .strand("logical_db.bound_request", &[])
        .map_err(BraidAdapterError::Build)?;
    let result = builder
        .strand("logical_db.invoke", &[bound_request])
        .map_err(BraidAdapterError::Build)?;
    builder.output(result).budget_tight();
    let capsule = builder.build().map_err(BraidAdapterError::Build)?;
    let bytes = capsule.to_bytes();
    prepare_request(
        &bytes,
        &registry,
        &[capability],
        ActionBinding::new(scope, effect, action, frontier, expires_at_unix_ms),
    )
}

fn operation_policy(effect: Effect) -> (EffectClass, Option<Exposure>) {
    match effect {
        Effect::Read | Effect::Scan => (EffectClass::Read, None),
        Effect::Write => (EffectClass::Irreversible, Some(Exposure::Vault)),
        Effect::Sync => (EffectClass::Egress, Some(Exposure::Vault)),
    }
}

/// Local authority for non-dangerous operations only.
///
/// Braid's pinned confirmation model carries no independently verifiable
/// runtime evidence. This authority therefore refuses irreversible writes and
/// egress synchronization. Hosts must supply externally prepared grants through
/// [`BraidAuthority`] for those effects.
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
        action: ActionDigest,
        frontier: u64,
        now_unix_ms: u64,
    ) -> Option<Self::Grant> {
        if scope != self.scope
            || !matches!(effect, Effect::Read | Effect::Scan)
            || now_unix_ms >= self.expires_at_unix_ms
        {
            return None;
        }
        prepare_local_request(scope, effect, action, frontier, self.expires_at_unix_ms).ok()
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
            eprintln!("logical-db-braid: new: GrantCount");
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
        action: ActionDigest,
        frontier: u64,
        now_unix_ms: u64,
    ) -> Option<Self::Grant> {
        self.grants
            .iter()
            .find(|grant| grant.permits(scope, effect, action, frontier, now_unix_ms))
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
    /// Capsule grant set omitted the exact scope/effect/action capability.
    #[error("Braid capsule lacks the exact scoped action capability")]
    MissingExactGrant,
    /// Exact capability was not attached to a vault-tainted term with the
    /// required effect, ceiling, and dangerous-operation confirmation policy.
    #[error("Braid capsule does not model the required Logical-DB effect and data exposure")]
    EffectPolicyMismatch,
    /// Frontier exceeded Braid's canonical fixed integer domain.
    #[error("Braid snapshot frontier {0} exceeds its canonical integer domain")]
    Frontier(u64),
    /// Expiry exceeded Braid's canonical fixed integer domain.
    #[error("Braid snapshot expiry {0} exceeds its canonical integer domain")]
    Expiry(u64),
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
    fn real_runnable_is_bound_to_scope_effect_action_frontier_and_expiry() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let action = ActionDigest::new([9; 32]);
        let grant = prepare_local_request(scope, Effect::Read, action, 7, 100).unwrap();
        assert_eq!(ExecutionGrant::scope(&grant), scope);
        assert_eq!(ExecutionGrant::effect(&grant), Effect::Read);
        assert_eq!(ExecutionGrant::action(&grant), action);
        assert_eq!(ExecutionGrant::frontier(&grant), 7);
        assert!(grant.permits(scope, Effect::Read, action, 7, 99));
        assert!(!grant.permits(scope, Effect::Read, action, 8, 99));
        assert!(!grant.permits(scope, Effect::Sync, action, 7, 99));
        assert!(!grant.permits(scope, Effect::Read, ActionDigest::new([8; 32]), 7, 99));
        assert!(!grant.permits(scope, Effect::Read, action, 7, 100));
    }

    #[test]
    fn invocation_identity_changes_with_frontier_action_and_expiry() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let first =
            prepare_local_request(scope, Effect::Read, ActionDigest::new([1; 32]), 1, 9).unwrap();
        let frontier =
            prepare_local_request(scope, Effect::Read, ActionDigest::new([1; 32]), 2, 9).unwrap();
        let action =
            prepare_local_request(scope, Effect::Read, ActionDigest::new([2; 32]), 1, 9).unwrap();
        let expiry =
            prepare_local_request(scope, Effect::Read, ActionDigest::new([1; 32]), 1, 10).unwrap();
        assert_ne!(first.invocation_id(), frontier.invocation_id());
        assert_ne!(first.invocation_id(), action.invocation_id());
        assert_ne!(first.invocation_id(), expiry.invocation_id());
    }

    #[test]
    fn scan_and_point_read_require_distinct_capabilities() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let action = ActionDigest::new([3; 32]);
        assert_ne!(
            capability_for(scope, Effect::Read, action),
            capability_for(scope, Effect::Scan, action)
        );
    }

    #[test]
    fn durable_and_network_effects_are_never_downgraded() {
        assert_eq!(
            operation_policy(Effect::Write),
            (EffectClass::Irreversible, Some(Exposure::Vault))
        );
        assert_eq!(
            operation_policy(Effect::Sync),
            (EffectClass::Egress, Some(Exposure::Vault))
        );
        assert_eq!(operation_policy(Effect::Read), (EffectClass::Read, None));
        assert_eq!(operation_policy(Effect::Scan), (EffectClass::Read, None));
    }

    #[test]
    fn external_capsule_cannot_attach_write_capability_to_a_weaker_effect() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let action = ActionDigest::new([5; 32]);
        let capability = capability_for(scope, Effect::Write, action);
        let mut registry = TermRegistry::new(1);
        registry
            .insert(TermSpec {
                id: "action".into(),
                inputs: vec![],
                output: TypeTag::Bool,
                capability: None,
                effect: EffectClass::Pure,
                source_exposure: Exposure::Vault,
                egress_ceiling: None,
                cost: 1,
            })
            .unwrap();
        registry
            .insert(TermSpec {
                id: "downgraded_write".into(),
                inputs: vec![TypeTag::Bool],
                output: TypeTag::Bool,
                capability: Some(capability.clone()),
                effect: EffectClass::ReversibleWrite,
                source_exposure: Exposure::Vault,
                egress_ceiling: Some(Exposure::Vault),
                cost: 1,
            })
            .unwrap();
        let mut builder = Builder::new(&registry, "attempt write downgrade");
        let source = builder.strand("action", &[]).unwrap();
        let sink = builder.strand("downgraded_write", &[source]).unwrap();
        builder
            .output(sink)
            .budget_tight()
            .confirm(ConfirmPolicy::HumanConfirm);
        let capsule = builder.build().unwrap();
        assert!(matches!(
            prepare_request(
                &capsule.to_bytes(),
                &registry,
                &[capability],
                ActionBinding::new(scope, Effect::Write, action, 0, 1),
            ),
            Err(BraidAdapterError::EffectPolicyMismatch)
        ));
    }

    #[test]
    fn externally_confirmed_exact_write_capsule_is_accepted() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let action = ActionDigest::new([6; 32]);
        let capability = capability_for(scope, Effect::Write, action);
        let mut registry = TermRegistry::new(1);
        registry
            .insert(TermSpec {
                id: "action".into(),
                inputs: vec![],
                output: TypeTag::Bool,
                capability: None,
                effect: EffectClass::Pure,
                source_exposure: Exposure::Vault,
                egress_ceiling: None,
                cost: 1,
            })
            .unwrap();
        registry
            .insert(TermSpec {
                id: "write".into(),
                inputs: vec![TypeTag::Bool],
                output: TypeTag::Bool,
                capability: Some(capability.clone()),
                effect: EffectClass::Irreversible,
                source_exposure: Exposure::Vault,
                egress_ceiling: Some(Exposure::Vault),
                cost: 1,
            })
            .unwrap();
        let mut builder = Builder::new(&registry, "externally confirmed write");
        let source = builder.strand("action", &[]).unwrap();
        let sink = builder.strand("write", &[source]).unwrap();
        builder
            .output(sink)
            .budget_tight()
            .confirm(ConfirmPolicy::HumanConfirm);
        let capsule = builder.build().unwrap();
        let grant = prepare_request(
            &capsule.to_bytes(),
            &registry,
            &[capability],
            ActionBinding::new(scope, Effect::Write, action, 0, 1),
        )
        .unwrap();
        assert_eq!(grant.effect(), Effect::Write);
    }

    #[test]
    fn external_read_capsule_cannot_change_the_required_ceiling_shape() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let action = ActionDigest::new([7; 32]);
        let capability = capability_for(scope, Effect::Read, action);
        let mut registry = TermRegistry::new(1);
        registry
            .insert(TermSpec {
                id: "action".into(),
                inputs: vec![],
                output: TypeTag::Bool,
                capability: None,
                effect: EffectClass::Pure,
                source_exposure: Exposure::Vault,
                egress_ceiling: None,
                cost: 1,
            })
            .unwrap();
        registry
            .insert(TermSpec {
                id: "read".into(),
                inputs: vec![TypeTag::Bool],
                output: TypeTag::Bool,
                capability: Some(capability.clone()),
                effect: EffectClass::Read,
                source_exposure: Exposure::Vault,
                egress_ceiling: Some(Exposure::Vault),
                cost: 1,
            })
            .unwrap();
        let mut builder = Builder::new(&registry, "attempt read policy drift");
        let source = builder.strand("action", &[]).unwrap();
        let sink = builder.strand("read", &[source]).unwrap();
        builder.output(sink).budget_tight();
        let capsule = builder.build().unwrap();
        assert!(matches!(
            prepare_request(
                &capsule.to_bytes(),
                &registry,
                &[capability],
                ActionBinding::new(scope, Effect::Read, action, 0, 1),
            ),
            Err(BraidAdapterError::EffectPolicyMismatch)
        ));
    }

    #[test]
    fn external_sync_capsule_must_route_vault_tainted_request_material() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let action = ActionDigest::new([8; 32]);
        let capability = capability_for(scope, Effect::Sync, action);
        let mut registry = TermRegistry::new(1);
        registry
            .insert(TermSpec {
                id: "public_request".into(),
                inputs: vec![],
                output: TypeTag::Bool,
                capability: None,
                effect: EffectClass::Pure,
                source_exposure: Exposure::Public,
                egress_ceiling: None,
                cost: 1,
            })
            .unwrap();
        registry
            .insert(TermSpec {
                id: "sync".into(),
                inputs: vec![TypeTag::Bool],
                output: TypeTag::Bool,
                capability: Some(capability.clone()),
                effect: EffectClass::Egress,
                source_exposure: Exposure::Vault,
                egress_ceiling: Some(Exposure::Vault),
                cost: 1,
            })
            .unwrap();
        let mut builder = Builder::new(&registry, "attempt untainted sync");
        let source = builder.strand("public_request", &[]).unwrap();
        let sink = builder.strand("sync", &[source]).unwrap();
        builder
            .output(sink)
            .budget_tight()
            .confirm(ConfirmPolicy::HumanConfirm);
        let capsule = builder.build().unwrap();
        assert!(matches!(
            prepare_request(
                &capsule.to_bytes(),
                &registry,
                &[capability],
                ActionBinding::new(scope, Effect::Sync, action, 0, 1),
            ),
            Err(BraidAdapterError::EffectPolicyMismatch)
        ));
    }

    #[test]
    fn local_policy_generates_only_exact_authenticated_runnable_grants() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let policy = LocalBraidAuthority::new(scope, 20);
        let action = ActionDigest::new([4; 32]);
        assert!(
            policy
                .authorize(scope, Effect::Read, action, 3, 19)
                .is_some()
        );
        assert!(
            policy
                .authorize(scope, Effect::Write, action, 3, 19)
                .is_none()
        );
        assert!(
            policy
                .authorize(scope, Effect::Read, action, 3, 20)
                .is_none()
        );
        assert!(matches!(
            prepare_local_request(scope, Effect::Write, action, 3, 20),
            Err(BraidAdapterError::Build(
                braid_sdk::BuildError::ConfirmRequired { .. }
            ))
        ));
    }

    #[test]
    fn effect_fact_names_and_external_authority_lookup_are_exact() {
        assert_eq!(effect_name(Effect::Read), "read");
        assert_eq!(effect_name(Effect::Scan), "scan");
        assert_eq!(effect_name(Effect::Write), "write");
        assert_eq!(effect_name(Effect::Sync), "sync");

        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let action = ActionDigest::new([6; 32]);
        let grant = prepare_local_request(scope, Effect::Read, action, 11, 30).unwrap();
        let expected_id = grant.invocation_id();
        let authority = BraidAuthority::new(vec![grant]).unwrap();
        assert_eq!(
            authority
                .authorize(scope, Effect::Read, action, 11, 29)
                .unwrap()
                .invocation_id(),
            expected_id
        );
        assert!(
            authority
                .authorize(scope, Effect::Read, action, 12, 29)
                .is_none()
        );
    }

    fn confirmed_dangerous_material(
        scope: Scope,
        effect: Effect,
        action: ActionDigest,
    ) -> (Vec<u8>, TermRegistry, Capability) {
        let (operation_effect, _) = operation_policy(effect);
        let capability = capability_for(scope, effect, action);
        let mut registry = TermRegistry::new(1);
        registry
            .insert(TermSpec {
                id: "action".into(),
                inputs: vec![],
                output: TypeTag::Bool,
                capability: None,
                effect: EffectClass::Pure,
                source_exposure: Exposure::Vault,
                egress_ceiling: None,
                cost: 1,
            })
            .unwrap();
        registry
            .insert(TermSpec {
                id: "invoke".into(),
                inputs: vec![TypeTag::Bool],
                output: TypeTag::Bool,
                capability: Some(capability.clone()),
                effect: operation_effect,
                source_exposure: Exposure::Vault,
                egress_ceiling: Some(Exposure::Vault),
                cost: 1,
            })
            .unwrap();
        let mut builder = Builder::new(&registry, "externally confirmed action");
        let source = builder.strand("action", &[]).unwrap();
        let sink = builder.strand("invoke", &[source]).unwrap();
        builder
            .output(sink)
            .budget_tight()
            .confirm(ConfirmPolicy::HumanConfirm);
        let capsule = builder.build().unwrap();
        (capsule.to_bytes(), registry, capability)
    }

    #[test]
    fn fabricated_confirmation_is_rejected_for_write_evidence() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let action = ActionDigest::new([11; 32]);
        let (bytes, registry, capability) =
            confirmed_dangerous_material(scope, Effect::Write, action);
        let mut tampered = Capsule::from_bytes(&bytes).unwrap();
        tampered.confirm = ConfirmPolicy::None;
        let result = prepare_request(
            &tampered.to_bytes(),
            &registry,
            &[capability],
            ActionBinding::new(scope, Effect::Write, action, 0, 1),
        );
        assert!(
            matches!(result, Err(BraidAdapterError::Rejected(_))),
            "independent admission must refuse a write strand without human confirm"
        );
    }

    #[test]
    fn local_fabrication_cannot_mint_sync_evidence() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let action = ActionDigest::new([12; 32]);
        assert!(matches!(
            prepare_local_request(scope, Effect::Sync, action, 3, 20),
            Err(BraidAdapterError::Build(
                braid_sdk::BuildError::ConfirmRequired { .. }
            ))
        ));
        let policy = LocalBraidAuthority::new(scope, 20);
        assert!(
            policy
                .authorize(scope, Effect::Sync, action, 3, 19)
                .is_none()
        );
    }

    #[test]
    fn external_write_grant_lookup_is_exact_across_scope_effect_and_expiry() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let action = ActionDigest::new([13; 32]);
        let (bytes, registry, capability) =
            confirmed_dangerous_material(scope, Effect::Write, action);
        let grant = prepare_request(
            &bytes,
            &registry,
            &[capability],
            ActionBinding::new(scope, Effect::Write, action, 5, 100),
        )
        .unwrap();
        let expected_id = grant.invocation_id();
        let authority = BraidAuthority::new(vec![grant]).unwrap();
        assert_eq!(
            authority
                .authorize(scope, Effect::Write, action, 5, 99)
                .unwrap()
                .invocation_id(),
            expected_id
        );
        assert!(
            authority
                .authorize(scope, Effect::Write, action, 5, 100)
                .is_none()
        );
        assert!(
            authority
                .authorize(
                    Scope::new(AccountId::new(), SpaceId::new()),
                    Effect::Write,
                    action,
                    5,
                    99
                )
                .is_none()
        );
        assert!(
            authority
                .authorize(scope, Effect::Sync, action, 5, 99)
                .is_none()
        );
        assert!(
            authority
                .authorize(scope, Effect::Write, ActionDigest::new([14; 32]), 5, 99)
                .is_none()
        );
        assert!(
            authority
                .authorize(scope, Effect::Write, action, 6, 99)
                .is_none()
        );
    }

    #[test]
    fn external_sync_grant_authorizes_exact_sync_evidence() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let action = ActionDigest::new([15; 32]);
        let (bytes, registry, capability) =
            confirmed_dangerous_material(scope, Effect::Sync, action);
        let grant = prepare_request(
            &bytes,
            &registry,
            &[capability],
            ActionBinding::new(scope, Effect::Sync, action, 5, 100),
        )
        .unwrap();
        let authority = BraidAuthority::new(vec![grant]).unwrap();
        assert!(
            authority
                .authorize(scope, Effect::Sync, action, 5, 99)
                .is_some()
        );
        assert!(
            authority
                .authorize(scope, Effect::Sync, action, 5, 100)
                .is_none()
        );
        assert!(
            authority
                .authorize(scope, Effect::Write, action, 5, 99)
                .is_none()
        );
    }
}
