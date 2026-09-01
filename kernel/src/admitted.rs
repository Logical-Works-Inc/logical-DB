//! Braid-bound scoped transactions — ADR-003.
//!
//! This is the shared processing boundary for embedded and outboard profiles.
//! It accepts only verifier/planner-created [`RunnableInvocation`] values,
//! binds their immutable Flow snapshot to the exact database read version,
//! scopes every physical key, and durably deduplicates operation identifiers.

use braid_flow_ir::{FactRef, Predicate, ValueExpr};
use braid_flow_plan::FlowSnapshot;
use braid_ir::{Capsule, Cid, EffectClass, TermRegistry, Value};
use braid_run::RunnableInvocation;

use crate::fdb::LogicalDb;
use crate::port::DurableKv;
use crate::tuple::{Element, KeyFamily, Scope, SystemFamily, Tuple};
use crate::{KernelError, Result};

pub const DATABASE_VERSION_FACT: &str = "logical_db.read_version";
pub const DATABASE_REQUEST_FACT: &str = "logical_db.request_cid";
pub const MAX_PROOF_ID_BYTES: usize = 1_024;
pub const MAX_OPERATION_ID_BYTES: usize = 1_024;
pub const MAX_DECLARED_EFFECTS: usize = 16;
pub const MAX_AUTHORITY_SCOPES: usize = 128;
pub const MAX_PUBLIC_RANGE_ROWS: usize = 100;
pub const MAX_CHANGE_PAGE_BYTES: usize = 11_000_000;

const OPERATION_DOMAIN: &[u8] = b"lw.logical-db.operation.v2";
const REQUEST_DOMAIN: &[u8] = b"lw.logical-db.request.v2";
const BRAID_PREDICATE_DOMAIN: &[u8] = b"lw.braid.justification.predicate.v0";
const VERSIONSTAMP_BYTES: usize = 10;
const TOKEN_SCOPE_BYTES: usize = 32;
const MIN_CHANGE_SUFFIX_BYTES: usize = 13;
const MAX_CHANGE_SUFFIX_BYTES: usize = 1_036;
const MIN_CHANGE_TOKEN_BYTES: usize = 45;
const MAX_CHANGE_TOKEN_BYTES: usize = 1_068;
const OPERATION_MARKER_BYTES: usize = 32 + VERSIONSTAMP_BYTES;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Effect {
    Put,
    Delete,
    Get,
    Scan,
}

impl Effect {
    const fn tag(self) -> u8 {
        match self {
            Self::Put => 1,
            Self::Delete => 2,
            Self::Get => 3,
            Self::Scan => 4,
        }
    }
}

/// Inspection proof for the exact Braid capsule and registry that produced a
/// runnable invocation. Its fields are private so callers cannot assert an
/// effect independently of Braid's content-addressed graph.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BraidExecutionBinding {
    capsule_cid: Cid,
    registry_cid: Cid,
    strongest_effect: EffectClass,
    capabilities: Vec<String>,
}

impl BraidExecutionBinding {
    pub fn inspect(capsule: &Capsule, registry: &TermRegistry) -> Result<Self> {
        if capsule.registry_cid != registry.cid() {
            return Err(KernelError::BraidExecutionBindingMismatch);
        }
        let mut strongest_effect = EffectClass::Pure;
        let mut capabilities = Vec::new();
        for strand in &capsule.braid.strands {
            let spec = registry
                .get(&strand.term)
                .ok_or(KernelError::BraidExecutionBindingMismatch)?;
            strongest_effect = strongest_effect.max(spec.effect);
            if let Some(capability) = &spec.capability {
                capabilities.push(capability.as_str().to_owned());
            }
        }
        capabilities.sort_unstable();
        capabilities.dedup();
        Ok(Self {
            capsule_cid: capsule.cid(),
            registry_cid: registry.cid(),
            strongest_effect,
            capabilities,
        })
    }
}

/// External namespace authorization remains owned by Kernel/Secure Authority.
/// This value is a scoped assertion consumed at the DB boundary, not a new
/// principal or capability-token authority. Construct it only from a trusted
/// in-process authority decision; never deserialize one from an app request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthorityProofRef {
    id: String,
    allowed_scopes: Vec<Scope>,
    allowed_effects: Vec<Effect>,
}

impl AuthorityProofRef {
    pub fn new(
        id: impl Into<String>,
        allowed_scopes: Vec<Scope>,
        allowed_effects: Vec<Effect>,
    ) -> Result<Self> {
        let id = id.into();
        if id.is_empty() || id.len() > MAX_PROOF_ID_BYTES {
            return Err(KernelError::ScopeIdInvalid {
                axis: "authority proof",
                len: id.len(),
                limit: MAX_PROOF_ID_BYTES,
            });
        }
        if allowed_scopes.is_empty() || allowed_scopes.len() > MAX_AUTHORITY_SCOPES {
            return Err(KernelError::AuthorityScopeViolation);
        }
        if allowed_effects.is_empty() || allowed_effects.len() > MAX_DECLARED_EFFECTS {
            return Err(KernelError::AuthorityScopeViolation);
        }
        Ok(Self {
            id,
            allowed_scopes,
            allowed_effects,
        })
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn covers(&self, scope: &Scope, effects: &[Effect]) -> bool {
        self.allowed_scopes.contains(scope)
            && effects
                .iter()
                .all(|effect| self.allowed_effects.contains(effect))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct OperationId(Vec<u8>);

impl OperationId {
    pub fn new(bytes: impl Into<Vec<u8>>) -> Result<Self> {
        let bytes = bytes.into();
        if bytes.is_empty() || bytes.len() > MAX_OPERATION_ID_BYTES {
            return Err(KernelError::ScopeIdInvalid {
                axis: "operation",
                len: bytes.len(),
                limit: MAX_OPERATION_ID_BYTES,
            });
        }
        Ok(Self(bytes))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// Exact Braid identities captured from an opaque runnable invocation. These
/// fields are observations of Braid-owned proofs; none is constructible input
/// to an admitted transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BraidProofBinding {
    pub capsule_cid: Cid,
    pub registry_cid: Cid,
    pub authority_cid: Cid,
    pub snapshot_cid: Cid,
    pub predicate_cid: Cid,
}

impl BraidProofBinding {
    fn from_runnable(runnable: &RunnableInvocation) -> Self {
        Self {
            capsule_cid: runnable.capsule_cid(),
            registry_cid: runnable.registry_cid(),
            authority_cid: runnable.authority_cid(),
            snapshot_cid: runnable.justification().snapshot_cid(),
            predicate_cid: runnable.justification().predicate_cid(),
        }
    }
}

/// Binds Braid's immutable semantic snapshot to one exact DB read version.
/// The fact is checked here; naming a snapshot CID beside a version is not
/// sufficient evidence that the snapshot actually contained that version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StateBinding {
    database_version: u64,
    request_cid: Cid,
    snapshot_cid: Cid,
}

impl StateBinding {
    pub fn new(database_version: u64, snapshot: &FlowSnapshot) -> Result<Self> {
        let fact_version = i64::try_from(database_version)
            .map_err(|_| KernelError::DatabaseVersionOutOfRange(database_version))?;
        if !matches!(
            snapshot.get_by_str(DATABASE_VERSION_FACT),
            Some(Value::Int(version)) if *version == fact_version
        ) {
            return Err(KernelError::MissingDatabaseVersionFact);
        }
        let request_cid = match snapshot.get_by_str(DATABASE_REQUEST_FACT) {
            Some(Value::Bytes(bytes)) => Cid(bytes
                .as_slice()
                .try_into()
                .map_err(|_| KernelError::MissingDatabaseRequestFact)?),
            _ => return Err(KernelError::MissingDatabaseRequestFact),
        };
        Ok(Self {
            database_version,
            request_cid,
            snapshot_cid: snapshot.cid(),
        })
    }

    pub fn database_version(&self) -> u64 {
        self.database_version
    }

    pub fn snapshot_cid(&self) -> Cid {
        self.snapshot_cid
    }

    pub fn request_cid(&self) -> Cid {
        self.request_cid
    }
}

/// Canonical request identity that Braid must prove in its immutable snapshot.
/// Build this before creating the `RunnableInvocation`; Logical-DB recomputes
/// it from the actual request at execution and refuses any mismatch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RequestBinding(Cid);

impl RequestBinding {
    pub fn cid(self) -> Cid {
        self.0
    }

    pub fn snapshot_fact(self) -> (&'static str, Value) {
        (DATABASE_REQUEST_FACT, Value::Bytes(self.0 .0.to_vec()))
    }

    pub fn predicate(self) -> Result<Predicate> {
        let fact = FactRef::new(DATABASE_REQUEST_FACT)
            .map_err(|error| KernelError::Backend(format!("request fact: {error}")))?;
        Ok(Predicate::Eq(
            ValueExpr::SnapshotFact(fact),
            ValueExpr::Literal(Value::Bytes(self.0 .0.to_vec())),
        ))
    }
}

/// Unforgeable high-level transaction authority. No `Clone` is intentional:
/// conflict recovery must produce a new Braid runnable against new state.
pub struct AdmittedTxn {
    runnable: RunnableInvocation,
    braid: BraidProofBinding,
    state: StateBinding,
    authority: AuthorityProofRef,
    scope: Scope,
    declared_effects: Vec<Effect>,
    operation_id: OperationId,
}

impl AdmittedTxn {
    pub fn new(
        runnable: RunnableInvocation,
        state: StateBinding,
        execution: BraidExecutionBinding,
        authority: AuthorityProofRef,
        scope: Scope,
        mut declared_effects: Vec<Effect>,
        operation_id: OperationId,
    ) -> Result<Self> {
        normalize_effects(&mut declared_effects)?;
        if !authority.covers(&scope, &declared_effects) {
            return Err(KernelError::AuthorityScopeViolation);
        }
        let braid = BraidProofBinding::from_runnable(&runnable);
        if braid.snapshot_cid != state.snapshot_cid {
            return Err(KernelError::BraidSnapshotMismatch);
        }
        if braid.predicate_cid != request_predicate_cid(state.request_cid)? {
            return Err(KernelError::BraidRequestPredicateMismatch);
        }
        if runnable.capsule_cid() != execution.capsule_cid
            || runnable.registry_cid() != execution.registry_cid
        {
            return Err(KernelError::BraidExecutionBindingMismatch);
        }
        if execution.strongest_effect != required_braid_effect(&declared_effects) {
            return Err(KernelError::BraidEffectMismatch);
        }
        if declared_effects.iter().any(|effect| {
            execution
                .capabilities
                .binary_search(&database_capability(*effect).to_owned())
                .is_err()
        }) {
            return Err(KernelError::BraidEffectMismatch);
        }
        Ok(Self {
            runnable,
            braid,
            state,
            authority,
            scope,
            declared_effects,
            operation_id,
        })
    }

    pub fn state(&self) -> &StateBinding {
        &self.state
    }

    pub fn scope(&self) -> &Scope {
        &self.scope
    }

    pub fn declared_effects(&self) -> &[Effect] {
        &self.declared_effects
    }

    pub fn operation_id(&self) -> &OperationId {
        &self.operation_id
    }

    pub fn braid_proof(&self) -> &BraidProofBinding {
        &self.braid
    }
}

fn required_braid_effect(effects: &[Effect]) -> EffectClass {
    if effects
        .iter()
        .any(|effect| matches!(effect, Effect::Put | Effect::Delete))
    {
        EffectClass::ReversibleWrite
    } else {
        EffectClass::Read
    }
}

const fn database_capability(effect: Effect) -> &'static str {
    match effect {
        Effect::Put => "logical-db.record.write",
        Effect::Delete => "logical-db.record.delete",
        Effect::Get => "logical-db.record.read",
        Effect::Scan => "logical-db.record.scan",
    }
}

fn normalize_effects(effects: &mut Vec<Effect>) -> Result<()> {
    effects.sort_unstable();
    effects.dedup();
    match effects.as_slice() {
        [] => Err(KernelError::AuthorityScopeViolation),
        _ => Ok(()),
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Receipt {
    pub committed_version: u64,
    pub operation_id: OperationId,
    pub braid: BraidProofBinding,
    pub authority_proof_id: String,
    pub state: StateBinding,
    pub scope: Scope,
    pub declared_effects: Vec<Effect>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AdmittedOutcome<T> {
    Applied { value: T, receipt: Receipt },
    Duplicate { receipt: Receipt },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum WriteMutation {
    Set { key: Vec<u8>, value: Vec<u8> },
    Clear { key: Vec<u8> },
}

/// Backend-neutral, bounded write plan produced by the thin client layer.
///
/// The scope and allowed effects are captured from an [`AdmittedTxn`]; callers
/// can name only a logical key family and tuple. Physical keys remain private,
/// making the same value safe to execute in-process or through the native FDB
/// client without duplicating namespace logic.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteBatch {
    scope: Scope,
    effects: Vec<Effect>,
    mutations: Vec<WriteMutation>,
    encoded_bytes: usize,
}

/// Opaque, zone-local position in the durable change log. Its bytes are a
/// storage cursor, not a timestamp or an application identifier.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChangeToken(Vec<u8>);

impl ChangeToken {
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self> {
        if !(MIN_CHANGE_TOKEN_BYTES..=MAX_CHANGE_TOKEN_BYTES).contains(&bytes.len()) {
            return Err(KernelError::MalformedKey);
        }
        validate_change_suffix(&bytes[TOKEN_SCOPE_BYTES..])?;
        Ok(Self(bytes))
    }

    fn for_scope(scope: &Scope, suffix: &[u8]) -> Result<Self> {
        validate_change_suffix(suffix)?;
        let mut bytes = change_token_scope(scope).0.to_vec();
        bytes.extend_from_slice(suffix);
        Ok(Self(bytes))
    }

    fn suffix_for<'a>(&'a self, scope: &Scope) -> Result<&'a [u8]> {
        if self.0[..TOKEN_SCOPE_BYTES] != change_token_scope(scope).0 {
            return Err(KernelError::TenantPrefixViolation);
        }
        Ok(&self.0[TOKEN_SCOPE_BYTES..])
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

fn validate_change_suffix(bytes: &[u8]) -> Result<()> {
    if !(MIN_CHANGE_SUFFIX_BYTES..=MAX_CHANGE_SUFFIX_BYTES).contains(&bytes.len()) {
        return Err(KernelError::MalformedKey);
    }
    let operation_len = u16::from_be_bytes(
        bytes[VERSIONSTAMP_BYTES..VERSIONSTAMP_BYTES + 2]
            .try_into()
            .map_err(|_| KernelError::MalformedKey)?,
    ) as usize;
    if operation_len == 0 || bytes.len() != VERSIONSTAMP_BYTES + 2 + operation_len {
        return Err(KernelError::MalformedKey);
    }
    Ok(())
}

fn change_token_scope(scope: &Scope) -> Cid {
    Cid::compute(b"lw.logical-db.change-token.scope.v2", &scope.zone_prefix())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChangeMutation {
    Set { key: crate::tuple::ScopedKey },
    Clear { key: crate::tuple::ScopedKey },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChangeEvent {
    pub token: ChangeToken,
    pub operation_id: OperationId,
    pub fingerprint: Cid,
    pub mutations: Vec<ChangeMutation>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChangePage {
    pub events: Vec<ChangeEvent>,
    pub next_token: Option<ChangeToken>,
    pub more: bool,
}

impl WriteBatch {
    pub fn new(scope: Scope, mut effects: Vec<Effect>) -> Result<Self> {
        normalize_effects(&mut effects)?;
        Ok(Self {
            scope,
            effects,
            mutations: Vec::new(),
            encoded_bytes: 0,
        })
    }

    pub fn request_binding(&self, operation_id: &OperationId) -> RequestBinding {
        RequestBinding(request_cid(
            &self.scope,
            &self.effects,
            operation_id,
            1,
            &self.canonical_bytes(),
        ))
    }

    fn require(&self, effect: Effect) -> Result<()> {
        if self.effects.contains(&effect) {
            Ok(())
        } else {
            Err(KernelError::AuthorityScopeViolation)
        }
    }

    fn charge(&mut self, bytes: usize) -> Result<()> {
        let size = self.encoded_bytes.saturating_add(bytes);
        if size > crate::fdb::MAX_TRANSACTION_BYTES {
            return Err(KernelError::TransactionTooLarge {
                size,
                limit: crate::fdb::MAX_TRANSACTION_BYTES,
            });
        }
        if self.mutations.len() >= crate::fdb::MAX_TRANSACTION_KEYS {
            return Err(KernelError::TransactionTooLarge {
                size: self.mutations.len() + 1,
                limit: crate::fdb::MAX_TRANSACTION_KEYS,
            });
        }
        self.encoded_bytes = size;
        Ok(())
    }

    pub fn set(&mut self, family: KeyFamily, tuple: &Tuple, value: Vec<u8>) -> Result<()> {
        self.require(Effect::Put)?;
        crate::fdb::validate_value(&value)?;
        let key = self.scope.try_pack(family, tuple)?;
        self.charge(key.len().saturating_add(value.len()))?;
        self.mutations.push(WriteMutation::Set { key, value });
        Ok(())
    }

    pub fn clear(&mut self, family: KeyFamily, tuple: &Tuple) -> Result<()> {
        self.require(Effect::Delete)?;
        let key = self.scope.try_pack(family, tuple)?;
        self.charge(key.len())?;
        self.mutations.push(WriteMutation::Clear { key });
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.mutations.len()
    }

    pub fn is_empty(&self) -> bool {
        self.mutations.is_empty()
    }

    fn validate_for(&self, envelope: &AdmittedTxn, backend_limit: usize) -> Result<()> {
        if self.scope != envelope.scope || self.effects != envelope.declared_effects {
            return Err(KernelError::AuthorityScopeViolation);
        }
        let change_mutations = self.change_mutation_bytes();
        let change_value_bytes = change_value_len(&envelope.operation_id, &change_mutations);
        if change_value_bytes > crate::fdb::MAX_VALUE_BYTES {
            return Err(KernelError::ValueTooLarge {
                len: change_value_bytes,
                limit: crate::fdb::MAX_VALUE_BYTES,
            });
        }
        let total = self
            .encoded_bytes
            .saturating_add(operation_key(&envelope.scope, &envelope.operation_id).len())
            .saturating_add(OPERATION_MARKER_BYTES + 4)
            .saturating_add(invocation_key(envelope).len())
            .saturating_add(OPERATION_MARKER_BYTES + 4)
            .saturating_add(frontier_key(&envelope.scope).len() + 32)
            .saturating_add(embedded_change_key(&envelope.scope, &envelope.operation_id).len() + 4)
            .saturating_add(change_value_bytes);
        if total > backend_limit {
            return Err(KernelError::TransactionTooLarge {
                size: total,
                limit: backend_limit,
            });
        }
        Ok(())
    }

    fn canonical_bytes(&self) -> Vec<u8> {
        let mut encoded = Vec::new();
        for mutation in &self.mutations {
            match mutation {
                WriteMutation::Set { key, value } => {
                    encoded.push(1);
                    append_framed(&mut encoded, key);
                    append_framed(&mut encoded, value);
                }
                WriteMutation::Clear { key } => {
                    encoded.push(2);
                    append_framed(&mut encoded, key);
                }
            }
        }
        encoded
    }

    fn change_mutation_bytes(&self) -> Vec<u8> {
        let mut encoded = Vec::new();
        for mutation in &self.mutations {
            match mutation {
                WriteMutation::Set { key, .. } => {
                    encoded.push(1);
                    append_framed(&mut encoded, key);
                }
                WriteMutation::Clear { key } => {
                    encoded.push(2);
                    append_framed(&mut encoded, key);
                }
            }
        }
        encoded
    }
}

impl RequestBinding {
    pub fn for_get(
        scope: &Scope,
        effects: &[Effect],
        operation_id: &OperationId,
        family: KeyFamily,
        tuple: &Tuple,
    ) -> Result<Self> {
        let mut normalized = effects.to_vec();
        normalize_effects(&mut normalized)?;
        let key = scope.try_pack(family, tuple)?;
        Ok(Self(request_cid(scope, &normalized, operation_id, 2, &key)))
    }

    pub fn for_changes(
        scope: &Scope,
        effects: &[Effect],
        operation_id: &OperationId,
        after: Option<&ChangeToken>,
        limit: usize,
    ) -> Result<Self> {
        if !(1..=MAX_PUBLIC_RANGE_ROWS).contains(&limit) {
            return Err(KernelError::RangeLimitInvalid {
                requested: limit,
                limit: MAX_PUBLIC_RANGE_ROWS,
            });
        }
        let mut normalized = effects.to_vec();
        normalize_effects(&mut normalized)?;
        let mut payload = Vec::new();
        match after {
            Some(token) => append_framed(&mut payload, token.as_bytes()),
            None => append_framed(&mut payload, &[]),
        }
        payload.extend_from_slice(&(limit as u64).to_be_bytes());
        Ok(Self(request_cid(
            scope,
            &normalized,
            operation_id,
            3,
            &payload,
        )))
    }
}

pub trait ReceiptPort {
    fn render(&self, receipt: &Receipt) -> Result<Vec<u8>>;
}

pub struct JsonReceiptPort;

impl ReceiptPort for JsonReceiptPort {
    fn render(&self, receipt: &Receipt) -> Result<Vec<u8>> {
        #[derive(serde::Serialize)]
        struct JsonReceipt<'a> {
            committed_version: u64,
            operation_id: String,
            capsule_cid: String,
            registry_cid: String,
            braid_authority_cid: String,
            snapshot_cid: String,
            predicate_cid: String,
            authority_proof: &'a str,
            account: String,
            container: String,
            zone: String,
            effects: Vec<u8>,
        }

        let effects = receipt
            .declared_effects
            .iter()
            .map(|effect| effect.tag())
            .collect();
        lgwks_std::json::to_vec(&JsonReceipt {
            committed_version: receipt.committed_version,
            operation_id: lgwks_std::hex::encode(receipt.operation_id.as_bytes()),
            capsule_cid: receipt.braid.capsule_cid.to_hex(),
            registry_cid: receipt.braid.registry_cid.to_hex(),
            braid_authority_cid: receipt.braid.authority_cid.to_hex(),
            snapshot_cid: receipt.braid.snapshot_cid.to_hex(),
            predicate_cid: receipt.braid.predicate_cid.to_hex(),
            authority_proof: &receipt.authority_proof_id,
            account: lgwks_std::hex::encode(receipt.scope.account().as_bytes()),
            container: lgwks_std::hex::encode(receipt.scope.container().as_bytes()),
            zone: lgwks_std::hex::encode(receipt.scope.zone().as_bytes()),
            effects,
        })
        .map_err(|error| KernelError::Backend(format!("receipt JSON: {error}")))
    }
}

async fn lock_embedded_until_deadline(
    db: &LogicalDb,
) -> Result<std::sync::MutexGuard<'_, crate::fdb::EmbeddedBackend>> {
    let started = std::time::Instant::now();
    let limit_ms = db.config().transaction_timeout_ms;
    loop {
        if let Some(backend) = db.try_lock_backend()? {
            return Ok(backend);
        }
        let elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        if elapsed_ms >= limit_ms {
            return Err(KernelError::TransactionTimedOut {
                elapsed_ms,
                limit_ms,
            });
        }
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
}

impl LogicalDb {
    /// Execute a stable, backend-neutral write plan. This is the canonical
    /// hybrid API; unlike a retryable closure, its exact mutation bytes are
    /// known before unknown-outcome deduplication is evaluated.
    pub async fn commit_admitted(
        &self,
        envelope: AdmittedTxn,
        batch: WriteBatch,
    ) -> Result<AdmittedOutcome<()>> {
        batch.validate_for(&envelope, self.config().transaction_size_limit_bytes)?;
        validate_request_binding(&envelope, batch.request_binding(&envelope.operation_id))?;
        let marker_key = operation_key(&envelope.scope, &envelope.operation_id);
        let invocation_key = invocation_key(&envelope);
        let mutation_bytes = batch.canonical_bytes();
        let change_mutations = batch.change_mutation_bytes();
        let fingerprint = operation_fingerprint(&envelope, &mutation_bytes);
        // Embedded work is abandoned only before it enters the durable
        // critical section. Once mutation begins, it always returns the real
        // commit outcome and never manufactures a timeout after success.
        let mut backend = lock_embedded_until_deadline(self).await?;
        let current = backend.committed_version()?;

        if let Some(marker) = backend.get_point(&marker_key)? {
            let committed_version = decode_operation_marker(&marker, fingerprint)?;
            return Ok(AdmittedOutcome::Duplicate {
                receipt: receipt_from(&envelope, committed_version),
            });
        }
        if backend.get_point(&invocation_key)?.is_some() {
            return Err(KernelError::BraidInvocationConsumed);
        }
        validate_fresh_envelope(&envelope, current)?;
        let committed_version = current
            .checked_add(1)
            .ok_or_else(|| KernelError::Backend("committed version overflow".into()))?;
        let mut puts = Vec::with_capacity(batch.mutations.len().saturating_add(3));
        let mut deletes = Vec::new();
        for mutation in batch.mutations {
            match mutation {
                WriteMutation::Set { key, value } => puts.push((key, value)),
                WriteMutation::Clear { key } => deletes.push(key),
            }
        }
        puts.push((
            resolved_change_key(&envelope.scope, &envelope.operation_id, committed_version),
            change_value(&envelope.operation_id, fingerprint, &change_mutations),
        ));
        let marker = resolved_operation_marker(fingerprint, committed_version);
        puts.push((marker_key, marker.clone()));
        puts.push((invocation_key, marker));
        backend.apply_transaction(&puts, &deletes, committed_version)?;
        Ok(AdmittedOutcome::Applied {
            value: (),
            receipt: receipt_from(&envelope, committed_version),
        })
    }

    pub async fn get_admitted(
        &self,
        envelope: AdmittedTxn,
        family: KeyFamily,
        tuple: &Tuple,
    ) -> Result<Option<Vec<u8>>> {
        if !envelope.declared_effects.contains(&Effect::Get) {
            return Err(KernelError::AuthorityScopeViolation);
        }
        validate_request_binding(
            &envelope,
            RequestBinding::for_get(
                &envelope.scope,
                &envelope.declared_effects,
                &envelope.operation_id,
                family,
                tuple,
            )?,
        )?;
        let key = envelope.scope.try_pack(family, tuple)?;
        let backend = lock_embedded_until_deadline(self).await?;
        let current = backend.committed_version()?;
        validate_fresh_envelope(&envelope, current)?;
        backend.get_point(&key)
    }

    pub async fn changes_admitted(
        &self,
        envelope: AdmittedTxn,
        after: Option<&ChangeToken>,
        limit: usize,
    ) -> Result<ChangePage> {
        validate_change_request(&envelope, limit)?;
        validate_request_binding(
            &envelope,
            RequestBinding::for_changes(
                &envelope.scope,
                &envelope.declared_effects,
                &envelope.operation_id,
                after,
                limit,
            )?,
        )?;
        let backend = lock_embedded_until_deadline(self).await?;
        let current = backend.committed_version()?;
        validate_fresh_envelope(&envelope, current)?;
        let prefix = change_prefix(&envelope.scope);
        let end = exclusive_prefix_end(&prefix).ok_or(KernelError::MalformedKey)?;
        let start_key = after
            .map(|token| physical_change_key(&prefix, &envelope.scope, token))
            .transpose()?;
        let (start, exclusive) = start_key
            .as_deref()
            .map_or((prefix.as_slice(), false), |key| (key, true));
        let mut rows =
            backend.scan_range_limited(start, exclusive, &end, limit.saturating_add(1))?;
        let more = rows.len() > limit;
        rows.truncate(limit);
        change_page(&envelope.scope, rows, more)
    }
}

/// One opaque app-facing database handle for both deployment profiles. The
/// logical request model is identical; only the final transaction driver
/// changes. Provider identity is deliberately private so an application cannot
/// unwrap this handle and bypass the admitted transaction boundary.
///
/// ```compile_fail
/// use logical_db::admitted::HybridDb;
///
/// fn unwrap_provider(db: HybridDb) {
///     let HybridDb::Embedded(_) = db;
/// }
/// ```
pub struct HybridDb {
    provider: HybridProvider,
}

enum HybridProvider {
    Embedded(LogicalDb),
    #[cfg(feature = "fdb")]
    Outboard(OutboardDb),
}

impl HybridDb {
    /// Open a zero-daemon, process-local database. This is useful for tests and
    /// ephemeral app state; use [`Self::embedded_at`] for durable embedding.
    pub fn embedded_memory(options: crate::DatabaseOptions) -> Result<Self> {
        Ok(Self {
            provider: HybridProvider::Embedded(LogicalDb::open(options)?),
        })
    }

    /// Open a durable database inside the application process. The admitted
    /// processing and wire contracts are identical to the outboard profile.
    pub fn embedded_at(
        path: impl AsRef<std::path::Path>,
        options: crate::DatabaseOptions,
    ) -> Result<Self> {
        Ok(Self {
            provider: HybridProvider::Embedded(LogicalDb::open_at(path, options)?),
        })
    }

    #[cfg(feature = "fdb")]
    pub fn outboard(db: OutboardDb) -> Self {
        Self {
            provider: HybridProvider::Outboard(db),
        }
    }

    /// Open the thin native client after the host has started FoundationDB's
    /// one-per-process network thread and retained its guard.
    #[cfg(feature = "fdb")]
    pub fn outboard_after_network_boot(options: crate::DatabaseOptions) -> Result<Self> {
        Ok(Self {
            provider: HybridProvider::Outboard(OutboardDb::open_after_network_boot(options)?),
        })
    }

    pub async fn read_version(&self) -> Result<u64> {
        match &self.provider {
            HybridProvider::Embedded(db) => db.read_version(),
            #[cfg(feature = "fdb")]
            HybridProvider::Outboard(db) => db.read_version().await,
        }
    }

    pub async fn commit_admitted(
        &self,
        envelope: AdmittedTxn,
        batch: WriteBatch,
    ) -> Result<AdmittedOutcome<()>> {
        match &self.provider {
            HybridProvider::Embedded(db) => db.commit_admitted(envelope, batch).await,
            #[cfg(feature = "fdb")]
            HybridProvider::Outboard(db) => db.commit_admitted(envelope, batch).await,
        }
    }

    pub async fn get_admitted(
        &self,
        envelope: AdmittedTxn,
        family: KeyFamily,
        tuple: &Tuple,
    ) -> Result<Option<Vec<u8>>> {
        match &self.provider {
            HybridProvider::Embedded(db) => db.get_admitted(envelope, family, tuple).await,
            #[cfg(feature = "fdb")]
            HybridProvider::Outboard(db) => db.get_admitted(envelope, family, tuple).await,
        }
    }

    pub async fn changes_admitted(
        &self,
        envelope: AdmittedTxn,
        after: Option<&ChangeToken>,
        limit: usize,
    ) -> Result<ChangePage> {
        match &self.provider {
            HybridProvider::Embedded(db) => db.changes_admitted(envelope, after, limit).await,
            #[cfg(feature = "fdb")]
            HybridProvider::Outboard(db) => db.changes_admitted(envelope, after, limit).await,
        }
    }
}

#[cfg(feature = "fdb")]
pub struct OutboardDb {
    database: std::sync::Arc<foundationdb::Database>,
    options: crate::DatabaseOptions,
}

#[cfg(feature = "fdb")]
impl OutboardDb {
    /// Attach the logical layer to a native FDB handle. The host process owns
    /// the one-per-process FoundationDB network guard and must keep it alive
    /// longer than this value.
    pub fn from_database(
        database: foundationdb::Database,
        options: crate::DatabaseOptions,
    ) -> Result<Self> {
        crate::fdb::validate_database_options(&options)?;
        Ok(Self {
            database: std::sync::Arc::new(database),
            options,
        })
    }

    /// Open the configured cluster after the host has booted the FDB network.
    /// Logical-DB deliberately does not hide the unsafe global network guard.
    pub fn open_after_network_boot(options: crate::DatabaseOptions) -> Result<Self> {
        let database = foundationdb::Database::new(options.cluster_file.as_deref())
            .map_err(native_fdb_error)?;
        Self::from_database(database, options)
    }

    pub async fn read_version(&self) -> Result<u64> {
        let timeout = native_timeout(&self.options)?;
        let version = self
            .database
            .run(move |transaction, _| async move {
                transaction
                    .set_option(foundationdb::options::TransactionOption::Timeout(timeout))?;
                transaction.set_option(foundationdb::options::TransactionOption::RetryLimit(
                    MAX_NATIVE_RETRIES,
                ))?;
                Ok(transaction.get_read_version().await?)
            })
            .await
            .map_err(native_backend_error)?;
        native_version(version)
    }

    pub async fn commit_admitted(
        &self,
        envelope: AdmittedTxn,
        batch: WriteBatch,
    ) -> Result<AdmittedOutcome<()>> {
        batch.validate_for(&envelope, self.options.transaction_size_limit_bytes)?;
        validate_request_binding(&envelope, batch.request_binding(&envelope.operation_id))?;
        validate_envelope_authority(&envelope)?;
        let mutation_bytes = batch.canonical_bytes();
        let change_mutations = batch.change_mutation_bytes();
        let fingerprint = operation_fingerprint(&envelope, &mutation_bytes);
        let marker_key = operation_key(&envelope.scope, &envelope.operation_id);
        let invocation_key = invocation_key(&envelope);
        let frontier_key = frontier_key(&envelope.scope);
        let change_key = native_change_key(&envelope.scope, &envelope.operation_id);
        let change_value = change_value(&envelope.operation_id, fingerprint, &change_mutations);
        let expected_version = envelope.state.database_version;
        let timeout = native_timeout(&self.options)?;
        let batch = std::sync::Arc::new(batch);
        let retry_marker_key = std::sync::Arc::new(marker_key.clone());
        let retry_invocation_key = std::sync::Arc::new(invocation_key);
        let retry_frontier_key = std::sync::Arc::new(frontier_key);
        let retry_change_key = std::sync::Arc::new(change_key);
        let retry_change_value = std::sync::Arc::new(change_value);

        let attempt = self
            .database
            .run(move |transaction, maybe_committed| {
                let batch = std::sync::Arc::clone(&batch);
                let marker_key = std::sync::Arc::clone(&retry_marker_key);
                let invocation_key = std::sync::Arc::clone(&retry_invocation_key);
                let frontier_key = std::sync::Arc::clone(&retry_frontier_key);
                let change_key = std::sync::Arc::clone(&retry_change_key);
                let change_value = std::sync::Arc::clone(&retry_change_value);
                let retried_after_uncertain_commit: bool = maybe_committed.into();
                async move {
                    transaction
                        .set_option(foundationdb::options::TransactionOption::Timeout(timeout))?;
                    transaction.set_option(
                        foundationdb::options::TransactionOption::RetryLimit(MAX_NATIVE_RETRIES),
                    )?;
                    if let Some(marker) = transaction.get(marker_key.as_slice(), false).await? {
                        return Ok(NativeWriteAttempt::Duplicate(marker.to_vec()));
                    }
                    if transaction
                        .get(invocation_key.as_slice(), false)
                        .await?
                        .is_some()
                    {
                        return Ok(NativeWriteAttempt::InvocationConsumed);
                    }
                    // The read/write frontier makes every write in one zone
                    // conflict with another write prepared at the same state.
                    // This is the FDB equivalent of the embedded commit lock.
                    let _frontier = transaction.get(frontier_key.as_slice(), false).await?;
                    let current = transaction.get_read_version().await?;
                    if current < 0 || current as u64 != expected_version {
                        return Ok(NativeWriteAttempt::Stale(current));
                    }
                    for mutation in &batch.mutations {
                        match mutation {
                            WriteMutation::Set { key, value } => transaction.set(key, value),
                            WriteMutation::Clear { key } => transaction.clear(key),
                        }
                    }
                    transaction.set(frontier_key.as_slice(), fingerprint.0.as_slice());
                    transaction.atomic_op(
                        change_key.as_slice(),
                        change_value.as_slice(),
                        foundationdb::options::MutationType::SetVersionstampedKey,
                    );
                    transaction.atomic_op(
                        marker_key.as_slice(),
                        &native_marker_template(fingerprint),
                        foundationdb::options::MutationType::SetVersionstampedValue,
                    );
                    transaction.atomic_op(
                        invocation_key.as_slice(),
                        &native_marker_template(fingerprint),
                        foundationdb::options::MutationType::SetVersionstampedValue,
                    );
                    // `Database::run` reports whether a prior commit may have
                    // succeeded. The marker checks above resolve that state;
                    // this read prevents the signal from being silently lost.
                    let _ = retried_after_uncertain_commit;
                    Ok(NativeWriteAttempt::Applied)
                }
            })
            .await
            .map_err(native_backend_error)?;

        let (duplicate, marker) = match attempt {
            NativeWriteAttempt::Applied => (
                false,
                self.read_marker(&marker_key).await?.ok_or_else(|| {
                    KernelError::Backend("committed FDB operation marker is missing".into())
                })?,
            ),
            NativeWriteAttempt::Duplicate(marker) => (true, marker),
            NativeWriteAttempt::InvocationConsumed => {
                return Err(KernelError::BraidInvocationConsumed);
            }
            NativeWriteAttempt::Stale(current) => {
                return Err(KernelError::StaleJustification {
                    expected: native_version(current)?,
                    got: expected_version,
                });
            }
        };
        let committed_version = decode_operation_marker(&marker, fingerprint)?;
        let receipt = receipt_from(&envelope, committed_version);
        if duplicate {
            Ok(AdmittedOutcome::Duplicate { receipt })
        } else {
            Ok(AdmittedOutcome::Applied { value: (), receipt })
        }
    }

    pub async fn get_admitted(
        &self,
        envelope: AdmittedTxn,
        family: KeyFamily,
        tuple: &Tuple,
    ) -> Result<Option<Vec<u8>>> {
        if !envelope.declared_effects.contains(&Effect::Get) {
            return Err(KernelError::AuthorityScopeViolation);
        }
        validate_request_binding(
            &envelope,
            RequestBinding::for_get(
                &envelope.scope,
                &envelope.declared_effects,
                &envelope.operation_id,
                family,
                tuple,
            )?,
        )?;
        validate_envelope_authority(&envelope)?;
        let key = envelope.scope.try_pack(family, tuple)?;
        let expected_version = envelope.state.database_version;
        let timeout = native_timeout(&self.options)?;
        let attempt = self
            .database
            .run(move |transaction, _| {
                let key = key.clone();
                async move {
                    transaction
                        .set_option(foundationdb::options::TransactionOption::Timeout(timeout))?;
                    transaction.set_option(
                        foundationdb::options::TransactionOption::RetryLimit(MAX_NATIVE_RETRIES),
                    )?;
                    let current = transaction.get_read_version().await?;
                    if current < 0 || current as u64 != expected_version {
                        return Ok(NativeReadAttempt::Stale(current));
                    }
                    let value = transaction
                        .get(&key, false)
                        .await?
                        .map(|value| value.to_vec());
                    Ok(NativeReadAttempt::Value(value))
                }
            })
            .await
            .map_err(native_backend_error)?;
        match attempt {
            NativeReadAttempt::Value(value) => Ok(value),
            NativeReadAttempt::Stale(current) => Err(KernelError::StaleJustification {
                expected: native_version(current)?,
                got: expected_version,
            }),
        }
    }

    // Deliberate: explicit assignments keep the native selector fields
    // observable to source-transform gates and the live-FDB receipt.
    #[allow(clippy::field_reassign_with_default)]
    pub async fn changes_admitted(
        &self,
        envelope: AdmittedTxn,
        after: Option<&ChangeToken>,
        limit: usize,
    ) -> Result<ChangePage> {
        validate_change_request(&envelope, limit)?;
        validate_request_binding(
            &envelope,
            RequestBinding::for_changes(
                &envelope.scope,
                &envelope.declared_effects,
                &envelope.operation_id,
                after,
                limit,
            )?,
        )?;
        validate_envelope_authority(&envelope)?;
        let scope = envelope.scope.clone();
        let prefix = change_prefix(&scope);
        let end = exclusive_prefix_end(&prefix).ok_or(KernelError::MalformedKey)?;
        let begin = after
            .map(|token| {
                physical_change_key(&prefix, &scope, token)
                    .map(foundationdb::KeySelector::first_greater_than)
            })
            .transpose()?
            .unwrap_or_else(|| foundationdb::KeySelector::first_greater_or_equal(prefix.clone()));
        let expected_version = envelope.state.database_version;
        let timeout = native_timeout(&self.options)?;
        let attempt = self
            .database
            .run(move |transaction, _| {
                let begin = begin.clone();
                let end = end.clone();
                async move {
                    transaction
                        .set_option(foundationdb::options::TransactionOption::Timeout(timeout))?;
                    transaction.set_option(
                        foundationdb::options::TransactionOption::RetryLimit(MAX_NATIVE_RETRIES),
                    )?;
                    let current = transaction.get_read_version().await?;
                    if current < 0 || current as u64 != expected_version {
                        return Ok(NativeChangesAttempt::Stale(current));
                    }
                    // Assign after construction so each required selector is
                    // explicit even in source transforms used by the default
                    // mutation lane (the live-FDB lane observes the values).
                    let mut options = foundationdb::RangeOption::default();
                    options.begin = begin;
                    options.end = foundationdb::KeySelector::first_greater_or_equal(end);
                    options.limit = Some(limit);
                    options.target_bytes = MAX_CHANGE_PAGE_BYTES;
                    let values = transaction.get_range(&options, 1, false).await?;
                    let more = values.more();
                    let rows = values
                        .iter()
                        .map(|row| (row.key().to_vec(), row.value().to_vec()))
                        .collect();
                    Ok(NativeChangesAttempt::Page(rows, more))
                }
            })
            .await
            .map_err(native_backend_error)?;
        match attempt {
            NativeChangesAttempt::Page(rows, more) => change_page(&scope, rows, more),
            NativeChangesAttempt::Stale(current) => Err(KernelError::StaleJustification {
                expected: native_version(current)?,
                got: expected_version,
            }),
        }
    }

    async fn read_marker(&self, marker_key: &[u8]) -> Result<Option<Vec<u8>>> {
        let timeout = native_timeout(&self.options)?;
        let marker_key = marker_key.to_vec();
        self.database
            .run(move |transaction, _| {
                let marker_key = marker_key.clone();
                async move {
                    transaction
                        .set_option(foundationdb::options::TransactionOption::Timeout(timeout))?;
                    transaction.set_option(
                        foundationdb::options::TransactionOption::RetryLimit(MAX_NATIVE_RETRIES),
                    )?;
                    Ok(transaction
                        .get(&marker_key, false)
                        .await?
                        .map(|value| value.to_vec()))
                }
            })
            .await
            .map_err(native_backend_error)
    }
}

#[cfg(feature = "fdb")]
const MAX_NATIVE_RETRIES: i32 = 5;

#[cfg(feature = "fdb")]
enum NativeWriteAttempt {
    Applied,
    Duplicate(Vec<u8>),
    InvocationConsumed,
    Stale(i64),
}

#[cfg(feature = "fdb")]
enum NativeReadAttempt {
    Value(Option<Vec<u8>>),
    Stale(i64),
}

#[cfg(feature = "fdb")]
enum NativeChangesAttempt {
    Page(Vec<(Vec<u8>, Vec<u8>)>, bool),
    Stale(i64),
}

#[cfg(feature = "fdb")]
fn native_timeout(options: &crate::DatabaseOptions) -> Result<i32> {
    i32::try_from(options.transaction_timeout_ms)
        .map_err(|_| KernelError::ConfigurationInvalid("FDB timeout must fit i32"))
}

#[cfg(feature = "fdb")]
fn native_version(version: i64) -> Result<u64> {
    u64::try_from(version)
        .map_err(|_| KernelError::Backend(format!("FDB returned invalid version {version}")))
}

#[cfg(feature = "fdb")]
fn native_backend_error(error: foundationdb::FdbBindingError) -> KernelError {
    match error.get_fdb_error() {
        Some(error) => native_fdb_error(error),
        None => KernelError::Backend(format!("native FDB binding: {error}")),
    }
}

#[cfg(feature = "fdb")]
fn native_fdb_error(error: foundationdb::FdbError) -> KernelError {
    let code = error.code();
    if error.is_maybe_committed() || matches!(code, 1021 | 1022 | 1030) {
        return KernelError::CommitOutcomeUnknown { code };
    }
    match code {
        1020 => KernelError::TransactionConflict,
        1004 | 1031 => KernelError::BackendTimedOut { code },
        1025 | 1101 => KernelError::BackendCancelled,
        1000 | 1007 | 1037 | 1038 | 1510..=1515 | 2008 => KernelError::BackendUnavailable { code },
        1039 | 2100 | 2200..=2203 => KernelError::BackendVersionSkew { code },
        _ => KernelError::Backend(format!("native FDB error {code}: {error}")),
    }
}

#[cfg(feature = "fdb")]
fn native_marker_template(fingerprint: Cid) -> Vec<u8> {
    let mut marker = operation_marker_template(fingerprint);
    marker.extend_from_slice(&(32_u32).to_le_bytes());
    marker
}

fn change_prefix(scope: &Scope) -> Vec<u8> {
    scope.system_prefix(SystemFamily::Change)
}

fn frontier_key(scope: &Scope) -> Vec<u8> {
    scope.pack_system(
        SystemFamily::Frontier,
        &Tuple::new(vec![Element::Bytes(b"zone-frontier".to_vec())]),
    )
}

fn embedded_change_key(scope: &Scope, operation_id: &OperationId) -> Vec<u8> {
    let mut key = change_prefix(scope);
    key.extend_from_slice(&[0xFF; VERSIONSTAMP_BYTES]);
    key.extend_from_slice(&(operation_id.as_bytes().len() as u16).to_be_bytes());
    key.extend_from_slice(operation_id.as_bytes());
    key
}

fn resolved_change_key(scope: &Scope, operation_id: &OperationId, version: u64) -> Vec<u8> {
    let mut key = change_prefix(scope);
    key.extend_from_slice(&logical_versionstamp(version));
    key.extend_from_slice(&(operation_id.as_bytes().len() as u16).to_be_bytes());
    key.extend_from_slice(operation_id.as_bytes());
    key
}

fn logical_versionstamp(version: u64) -> [u8; VERSIONSTAMP_BYTES] {
    let mut stamp = [0; VERSIONSTAMP_BYTES];
    stamp[..8].copy_from_slice(&version.to_be_bytes());
    stamp
}

fn resolved_operation_marker(fingerprint: Cid, version: u64) -> Vec<u8> {
    let mut marker = operation_marker_template(fingerprint);
    marker[32..].copy_from_slice(&logical_versionstamp(version));
    marker
}

#[cfg(feature = "fdb")]
fn native_change_key(scope: &Scope, operation_id: &OperationId) -> Vec<u8> {
    let mut key = embedded_change_key(scope, operation_id);
    let offset = u32::try_from(change_prefix(scope).len())
        .expect("bounded scope prefix always fits the FDB versionstamp offset");
    key.extend_from_slice(&offset.to_le_bytes());
    key
}

fn physical_change_key(prefix: &[u8], scope: &Scope, token: &ChangeToken) -> Result<Vec<u8>> {
    let mut key = prefix.to_vec();
    key.extend_from_slice(token.suffix_for(scope)?);
    Ok(key)
}

fn exclusive_prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last != 0xFF {
            end.push(last + 1);
            return Some(end);
        }
    }
    None
}

fn change_value(operation_id: &OperationId, fingerprint: Cid, mutations: &[u8]) -> Vec<u8> {
    let mut value = vec![1];
    value.extend_from_slice(&fingerprint.0);
    append_framed(&mut value, operation_id.as_bytes());
    append_framed(&mut value, mutations);
    value
}

fn change_value_len(operation_id: &OperationId, mutations: &[u8]) -> usize {
    1_usize
        .saturating_add(32)
        .saturating_add(8)
        .saturating_add(operation_id.as_bytes().len())
        .saturating_add(8)
        .saturating_add(mutations.len())
}

fn validate_change_request(envelope: &AdmittedTxn, limit: usize) -> Result<()> {
    if !envelope.declared_effects.contains(&Effect::Scan) {
        return Err(KernelError::AuthorityScopeViolation);
    }
    if limit == 0 || limit > MAX_PUBLIC_RANGE_ROWS {
        return Err(KernelError::RangeLimitInvalid {
            requested: limit,
            limit: MAX_PUBLIC_RANGE_ROWS,
        });
    }
    Ok(())
}

fn change_page(scope: &Scope, rows: Vec<(Vec<u8>, Vec<u8>)>, more: bool) -> Result<ChangePage> {
    for (key, value) in &rows {
        crate::fdb::validate_key(key)?;
        crate::fdb::validate_value(value)?;
    }
    let prefix = change_prefix(scope);
    let mut events = Vec::with_capacity(rows.len());
    for (key, value) in rows {
        let suffix = key
            .strip_prefix(prefix.as_slice())
            .ok_or(KernelError::TenantPrefixViolation)?;
        let token = ChangeToken::for_scope(scope, suffix)?;
        events.push(decode_change_event(scope, token, &value)?);
    }
    let next_token = events.last().map(|event| event.token.clone());
    Ok(ChangePage {
        events,
        next_token,
        more,
    })
}

fn take_framed<'a>(bytes: &mut &'a [u8]) -> Result<&'a [u8]> {
    let len_bytes = bytes.get(..8).ok_or(KernelError::MalformedKey)?;
    let len = u64::from_be_bytes(
        len_bytes
            .try_into()
            .map_err(|_| KernelError::MalformedKey)?,
    );
    *bytes = &bytes[8..];
    let len = usize::try_from(len).map_err(|_| KernelError::MalformedKey)?;
    let value = bytes.get(..len).ok_or(KernelError::MalformedKey)?;
    *bytes = &bytes[len..];
    Ok(value)
}

fn decode_change_event(scope: &Scope, token: ChangeToken, value: &[u8]) -> Result<ChangeEvent> {
    if value.len() < 33 || value[0] != 1 {
        return Err(KernelError::MalformedKey);
    }
    let mut fingerprint = [0; 32];
    fingerprint.copy_from_slice(&value[1..33]);
    let mut cursor = &value[33..];
    let operation_id = OperationId::new(take_framed(&mut cursor)?.to_vec())?;
    let mut encoded_mutations = take_framed(&mut cursor)?;
    if !cursor.is_empty() {
        return Err(KernelError::MalformedKey);
    }
    let mut mutations = Vec::new();
    while let Some((tag, rest)) = encoded_mutations.split_first() {
        encoded_mutations = rest;
        let key = crate::tuple::ScopedKey::decode(take_framed(&mut encoded_mutations)?)?;
        if &key.scope != scope {
            return Err(KernelError::TenantPrefixViolation);
        }
        mutations.push(match tag {
            1 => ChangeMutation::Set { key },
            2 => ChangeMutation::Clear { key },
            _ => return Err(KernelError::MalformedKey),
        });
    }
    Ok(ChangeEvent {
        token,
        operation_id,
        fingerprint: Cid(fingerprint),
        mutations,
    })
}

fn validate_fresh_envelope(envelope: &AdmittedTxn, current: u64) -> Result<()> {
    if envelope.state.database_version != current {
        return Err(KernelError::StaleJustification {
            expected: current,
            got: envelope.state.database_version,
        });
    }
    validate_envelope_authority(envelope)
}

fn validate_envelope_authority(envelope: &AdmittedTxn) -> Result<()> {
    if !envelope
        .authority
        .covers(&envelope.scope, &envelope.declared_effects)
    {
        return Err(KernelError::AuthorityScopeViolation);
    }
    // Keep the opaque Braid proof alive through every validation. Moving the
    // envelope into the API ensures it cannot authorize a second operation.
    let _runnable = &envelope.runnable;
    Ok(())
}

fn operation_key(scope: &Scope, operation_id: &OperationId) -> Vec<u8> {
    scope.pack_system(
        SystemFamily::Outbox,
        &Tuple::new(vec![Element::Bytes(operation_id.as_bytes().to_vec())]),
    )
}

fn invocation_key(envelope: &AdmittedTxn) -> Vec<u8> {
    let mut identity = Vec::with_capacity(32 * 5);
    identity.extend_from_slice(&envelope.braid.capsule_cid.0);
    identity.extend_from_slice(&envelope.braid.registry_cid.0);
    identity.extend_from_slice(&envelope.braid.authority_cid.0);
    identity.extend_from_slice(&envelope.braid.snapshot_cid.0);
    identity.extend_from_slice(&envelope.braid.predicate_cid.0);
    envelope.scope.pack_system(
        SystemFamily::Outbox,
        &Tuple::new(vec![
            Element::Bytes(b"braid-invocation".to_vec()),
            Element::Bytes(identity),
        ]),
    )
}

fn append_framed(bytes: &mut Vec<u8>, value: &[u8]) {
    bytes.extend_from_slice(&(value.len() as u64).to_be_bytes());
    bytes.extend_from_slice(value);
}

fn request_cid(
    scope: &Scope,
    effects: &[Effect],
    operation_id: &OperationId,
    request_tag: u8,
    payload: &[u8],
) -> Cid {
    let mut bytes = scope.zone_prefix();
    append_framed(&mut bytes, operation_id.as_bytes());
    bytes.push(request_tag);
    bytes.extend_from_slice(&(effects.len() as u16).to_be_bytes());
    bytes.extend(effects.iter().map(|effect| effect.tag()));
    append_framed(&mut bytes, payload);
    Cid::compute(REQUEST_DOMAIN, &bytes)
}

fn request_predicate_cid(request: Cid) -> Result<Cid> {
    let predicate = RequestBinding(request).predicate()?;
    Ok(Cid::compute(
        BRAID_PREDICATE_DOMAIN,
        &braid_ir::encode(&predicate.to_canon()),
    ))
}

fn validate_request_binding(envelope: &AdmittedTxn, actual: RequestBinding) -> Result<()> {
    if envelope.state.request_cid == actual.cid() {
        Ok(())
    } else {
        Err(KernelError::RequestPlanMismatch)
    }
}

fn operation_fingerprint(envelope: &AdmittedTxn, mutation_bytes: &[u8]) -> Cid {
    let mut bytes = envelope.scope.zone_prefix();
    append_framed(&mut bytes, envelope.operation_id.as_bytes());
    bytes.extend_from_slice(&envelope.braid.capsule_cid.0);
    bytes.extend_from_slice(&envelope.braid.registry_cid.0);
    bytes.extend_from_slice(&envelope.braid.authority_cid.0);
    bytes.extend_from_slice(&envelope.braid.snapshot_cid.0);
    bytes.extend_from_slice(&envelope.braid.predicate_cid.0);
    bytes.extend_from_slice(&envelope.state.database_version.to_be_bytes());
    append_framed(&mut bytes, envelope.authority.id.as_bytes());
    bytes.extend(envelope.declared_effects.iter().map(|effect| effect.tag()));
    append_framed(&mut bytes, mutation_bytes);
    Cid::compute(OPERATION_DOMAIN, &bytes)
}

fn operation_marker_template(fingerprint: Cid) -> Vec<u8> {
    let mut marker = Vec::with_capacity(OPERATION_MARKER_BYTES);
    marker.extend_from_slice(&fingerprint.0);
    marker.extend_from_slice(&[0; VERSIONSTAMP_BYTES]);
    marker
}

fn decode_operation_marker(marker: &[u8], expected: Cid) -> Result<u64> {
    if marker.len() != OPERATION_MARKER_BYTES || marker[..32] != expected.0 {
        return Err(KernelError::OperationIdConflict);
    }
    let version = marker[32..40]
        .try_into()
        .map(u64::from_be_bytes)
        .map_err(|_| KernelError::OperationIdConflict)?;
    // The final two versionstamp bytes are FoundationDB's transaction-local
    // batch order. They are zero in the embedded codec but may be non-zero in
    // a native transaction, so they are intentionally not part of the logical
    // committed version.
    Ok(version)
}

fn receipt_from(envelope: &AdmittedTxn, committed_version: u64) -> Receipt {
    Receipt {
        committed_version,
        operation_id: envelope.operation_id.clone(),
        braid: envelope.braid.clone(),
        authority_proof_id: envelope.authority.id.clone(),
        state: envelope.state.clone(),
        scope: envelope.scope.clone(),
        declared_effects: envelope.declared_effects.clone(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use braid_capability::Capability;
    use braid_flow_plan::{CompletionMap, FlowSnapshot};
    use braid_ir::{
        Braid, Capsule, ConfirmPolicy, EffectClass, Exposure, Strand, TermRegistry, TermSpec,
        TypeTag, Value, IR_VERSION,
    };
    use braid_run::{JustificationProof, RunnableInvocation};

    use super::*;
    use crate::fdb::FdbConfig;
    use crate::tuple::{AccountId, ContainerId, ZoneId};

    fn scope(name: &[u8]) -> Scope {
        Scope::new(
            AccountId::new(name.to_vec()).unwrap(),
            ContainerId::new(b"notes".to_vec()).unwrap(),
            ZoneId::new(b"private".to_vec()).unwrap(),
        )
    }

    fn registry_and_capsule_terms(
        mut terms: Vec<(&'static str, EffectClass)>,
    ) -> (TermRegistry, Capsule, Vec<Capability>) {
        terms.sort_unstable_by_key(|(term, _)| *term);
        let mut registry = TermRegistry::new(1);
        let ambient: Vec<Capability> = terms
            .iter()
            .map(|(term, _)| Capability::new(*term))
            .collect();
        for ((_, effect), capability) in terms.iter().zip(&ambient) {
            registry
                .insert(TermSpec {
                    id: capability.as_str().into(),
                    inputs: vec![],
                    output: TypeTag::Bool,
                    capability: Some(capability.clone()),
                    effect: *effect,
                    source_exposure: Exposure::Public,
                    egress_ceiling: None,
                    cost: 1,
                })
                .unwrap();
        }
        let capsule = Capsule {
            ir_version: IR_VERSION,
            vocab_version: registry.vocab_version,
            registry_cid: registry.cid(),
            intent: "execute one exact scoped logical-db request".into(),
            grants: ambient.clone(),
            braid: Braid {
                strands: ambient
                    .iter()
                    .map(|capability| Strand {
                        term: capability.as_str().into(),
                        inputs: vec![],
                    })
                    .collect(),
                outputs: (0..u32::try_from(ambient.len()).unwrap()).collect(),
            },
            budget: ambient.len() as u64,
            confirm: ConfirmPolicy::None,
            evidence: vec![],
        };
        (registry, capsule, ambient)
    }

    fn registry_and_capsule(effects: &[Effect]) -> (TermRegistry, Capsule, Vec<Capability>) {
        registry_and_capsule_terms(
            effects
                .iter()
                .map(|effect| {
                    (
                        database_capability(*effect),
                        match effect {
                            Effect::Put | Effect::Delete => EffectClass::ReversibleWrite,
                            Effect::Get | Effect::Scan => EffectClass::Read,
                        },
                    )
                })
                .collect(),
        )
    }

    fn prepare_runnable(
        version: u64,
        binding: RequestBinding,
        registry: TermRegistry,
        capsule: Capsule,
        ambient: Vec<Capability>,
    ) -> (RunnableInvocation, FlowSnapshot, BraidExecutionBinding) {
        let execution = BraidExecutionBinding::inspect(&capsule, &registry).unwrap();
        let (request_name, request_value) = binding.snapshot_fact();
        let snapshot = FlowSnapshot::new(BTreeMap::from([
            (
                DATABASE_VERSION_FACT.to_string(),
                Value::Int(version as i64),
            ),
            (request_name.to_string(), request_value),
        ]));
        let predicate = binding.predicate().unwrap();
        let justification =
            JustificationProof::prove(&capsule, &predicate, &snapshot, &CompletionMap::new())
                .unwrap();
        let admission = braid_verify::admit(&capsule.to_bytes(), &registry, &ambient).unwrap();
        let runnable = RunnableInvocation::prepare(
            capsule,
            &registry,
            admission,
            justification,
            &ambient,
            &snapshot,
        )
        .unwrap();
        (runnable, snapshot, execution)
    }

    fn runnable(
        version: u64,
        binding: RequestBinding,
        effects: &[Effect],
    ) -> (RunnableInvocation, FlowSnapshot, BraidExecutionBinding) {
        let (registry, capsule, ambient) = registry_and_capsule(effects);
        prepare_runnable(version, binding, registry, capsule, ambient)
    }

    fn admitted(
        version: u64,
        scope: Scope,
        effects: Vec<Effect>,
        operation: OperationId,
        binding: RequestBinding,
    ) -> AdmittedTxn {
        let (runnable, snapshot, execution) = runnable(version, binding, &effects);
        let state = StateBinding::new(version, &snapshot).unwrap();
        let authority =
            AuthorityProofRef::new("kernel-capability-1", vec![scope.clone()], effects.clone())
                .unwrap();
        AdmittedTxn::new(
            runnable, state, execution, authority, scope, effects, operation,
        )
        .unwrap()
    }

    fn new_write_batch(scope: &Scope, effects: Vec<Effect>) -> WriteBatch {
        WriteBatch::new(scope.clone(), effects).unwrap()
    }

    fn write_envelope(version: u64, batch: &WriteBatch, operation: &[u8]) -> AdmittedTxn {
        let operation = OperationId::new(operation.to_vec()).unwrap();
        admitted(
            version,
            batch.scope.clone(),
            batch.effects.clone(),
            operation.clone(),
            batch.request_binding(&operation),
        )
    }

    fn envelope(version: u64, scope: Scope, operation: &[u8]) -> AdmittedTxn {
        let batch = new_write_batch(&scope, vec![Effect::Put, Effect::Get]);
        write_envelope(version, &batch, operation)
    }

    fn read_envelope(
        version: u64,
        scope: Scope,
        operation: &[u8],
        family: KeyFamily,
        tuple: &Tuple,
    ) -> AdmittedTxn {
        let operation = OperationId::new(operation.to_vec()).unwrap();
        let effects = vec![Effect::Get];
        let binding = RequestBinding::for_get(&scope, &effects, &operation, family, tuple).unwrap();
        admitted(version, scope, effects, operation, binding)
    }

    fn scan_envelope(
        version: u64,
        scope: Scope,
        operation: &[u8],
        after: Option<&ChangeToken>,
        limit: usize,
    ) -> AdmittedTxn {
        let operation = OperationId::new(operation.to_vec()).unwrap();
        let effects = vec![Effect::Scan];
        let binding =
            RequestBinding::for_changes(&scope, &effects, &operation, after, limit).unwrap();
        admitted(version, scope, effects, operation, binding)
    }

    fn change_token(scope: &Scope, operation: &[u8]) -> ChangeToken {
        let mut suffix = vec![0; VERSIONSTAMP_BYTES];
        suffix.extend_from_slice(&(operation.len() as u16).to_be_bytes());
        suffix.extend_from_slice(operation);
        ChangeToken::for_scope(scope, &suffix).unwrap()
    }

    #[tokio::test]
    async fn real_braid_runnable_gates_a_scoped_embedded_transaction() {
        let db = LogicalDb::open(FdbConfig::default()).unwrap();
        let account = scope(b"account-a");
        let object = Tuple::new(vec![Element::Bytes(b"object-1".to_vec())]);
        let mut batch = new_write_batch(&account, vec![Effect::Put]);
        batch
            .set(KeyFamily::Record, &object, b"value".to_vec())
            .unwrap();
        let write = write_envelope(0, &batch, b"op-1");
        let outcome = db.commit_admitted(write, batch).await.unwrap();
        match outcome {
            AdmittedOutcome::Applied { receipt, .. } => {
                assert_eq!(receipt.committed_version, 1);
                assert_ne!(receipt.braid.capsule_cid.0, [0; 32]);
            }
            AdmittedOutcome::Duplicate { .. } => panic!("first operation cannot be duplicate"),
        }
        assert_eq!(
            db.get_admitted(
                read_envelope(1, account, b"read-1", KeyFamily::Record, &object),
                KeyFamily::Record,
                &object,
            )
            .await
            .unwrap(),
            Some(b"value".to_vec())
        );
    }

    #[tokio::test]
    async fn retry_after_unknown_outcome_returns_the_original_commit() {
        let db = LogicalDb::open(FdbConfig::default()).unwrap();
        let account = scope(b"account-a");
        let object = Tuple::new(vec![Element::Bytes(b"object".to_vec())]);
        let mut first_batch = new_write_batch(&account, vec![Effect::Put]);
        first_batch
            .set(KeyFamily::Record, &object, b"value".to_vec())
            .unwrap();
        let first_envelope = write_envelope(0, &first_batch, b"same-op");
        let first = db
            .commit_admitted(first_envelope, first_batch)
            .await
            .unwrap();
        assert!(matches!(first, AdmittedOutcome::Applied { .. }));

        let mut retry_batch = new_write_batch(&account, vec![Effect::Put]);
        retry_batch
            .set(KeyFamily::Record, &object, b"value".to_vec())
            .unwrap();
        let retry_envelope = write_envelope(0, &retry_batch, b"same-op");
        let retry = db
            .commit_admitted(retry_envelope, retry_batch)
            .await
            .unwrap();
        match retry {
            AdmittedOutcome::Duplicate { receipt } => {
                assert_eq!(receipt.committed_version, 1)
            }
            AdmittedOutcome::Applied { .. } => panic!("retry must deduplicate"),
        }
    }

    #[tokio::test]
    async fn operation_id_reuse_with_different_mutation_bytes_fails_closed() {
        let db = LogicalDb::open(FdbConfig::default()).unwrap();
        let account = scope(b"account-a");
        let object = Tuple::new(vec![Element::Bytes(b"object".to_vec())]);
        let mut first_batch = new_write_batch(&account, vec![Effect::Put]);
        first_batch
            .set(KeyFamily::Record, &object, b"first".to_vec())
            .unwrap();
        let first = write_envelope(0, &first_batch, b"same-op");
        db.commit_admitted(first, first_batch).await.unwrap();

        let mut conflicting_batch = new_write_batch(&account, vec![Effect::Put]);
        conflicting_batch
            .set(KeyFamily::Record, &object, b"second".to_vec())
            .unwrap();
        let conflicting = write_envelope(0, &conflicting_batch, b"same-op");
        assert!(matches!(
            db.commit_admitted(conflicting, conflicting_batch).await,
            Err(KernelError::OperationIdConflict)
        ));
    }

    #[tokio::test]
    async fn exact_braid_request_cannot_authorize_a_different_write_plan() {
        let db = LogicalDb::open(FdbConfig::default()).unwrap();
        let account = scope(b"account-a");
        let first_object = Tuple::new(vec![Element::Bytes(b"first".to_vec())]);
        let mut first_batch = new_write_batch(&account, vec![Effect::Put]);
        first_batch
            .set(KeyFamily::Record, &first_object, b"one".to_vec())
            .unwrap();
        let first = write_envelope(0, &first_batch, b"operation-one");
        let second_object = Tuple::new(vec![Element::Bytes(b"second".to_vec())]);
        let mut second_batch = new_write_batch(&account, vec![Effect::Put]);
        second_batch
            .set(KeyFamily::Record, &second_object, b"two".to_vec())
            .unwrap();
        assert!(matches!(
            db.commit_admitted(first, second_batch).await,
            Err(KernelError::RequestPlanMismatch)
        ));
    }

    #[test]
    fn foreign_braid_capabilities_cannot_authorize_a_put_request() {
        let account = scope(b"effect-axis");
        for (foreign, suffix) in [
            (Effect::Get, b"read".as_slice()),
            (Effect::Delete, b"delete"),
        ] {
            let operation =
                OperationId::new([b"effect-axis-".as_slice(), suffix].concat()).unwrap();
            let batch = new_write_batch(&account, vec![Effect::Put]);
            let (runnable, snapshot, execution) =
                runnable(0, batch.request_binding(&operation), &[foreign]);
            let result = AdmittedTxn::new(
                runnable,
                StateBinding::new(0, &snapshot).unwrap(),
                execution,
                AuthorityProofRef::new(
                    "effect-axis-authority",
                    vec![account.clone()],
                    vec![Effect::Put],
                )
                .unwrap(),
                account.clone(),
                vec![Effect::Put],
                operation,
            );
            assert!(matches!(result, Err(KernelError::BraidEffectMismatch)));
        }

        let operation = OperationId::new(b"effect-axis-unrelated".to_vec()).unwrap();
        let batch = new_write_batch(&account, vec![Effect::Put]);
        let (registry, capsule, ambient) = registry_and_capsule_terms(vec![(
            "filesystem.record.write",
            EffectClass::ReversibleWrite,
        )]);
        let (runnable, snapshot, execution) = prepare_runnable(
            0,
            batch.request_binding(&operation),
            registry,
            capsule,
            ambient,
        );
        let result = AdmittedTxn::new(
            runnable,
            StateBinding::new(0, &snapshot).unwrap(),
            execution,
            AuthorityProofRef::new(
                "effect-axis-authority",
                vec![account.clone()],
                vec![Effect::Put],
            )
            .unwrap(),
            account,
            vec![Effect::Put],
            operation,
        );
        assert!(matches!(result, Err(KernelError::BraidEffectMismatch)));
    }

    #[tokio::test]
    async fn legacy_copy_uses_the_admitted_batch_and_preserves_value() {
        let db = LogicalDb::open(FdbConfig::default()).unwrap();
        let account = scope(b"account-a");
        let legacy = crate::migration::LegacyV1Point::decode(&crate::model::point_key(
            crate::model::Class::Claim,
            b"legacy-id",
            u64::MAX,
        ))
        .unwrap();
        let mut batch = new_write_batch(&account, vec![Effect::Put]);
        legacy
            .copy_into(&mut batch, b"legacy-value".to_vec())
            .unwrap();
        let write = write_envelope(0, &batch, b"legacy-copy");
        db.commit_admitted(write, batch).await.unwrap();

        let destination = legacy.destination_tuple();
        let read = read_envelope(1, account, b"legacy-read", KeyFamily::Record, &destination);
        assert_eq!(
            db.get_admitted(read, KeyFamily::Record, &destination)
                .await
                .unwrap(),
            Some(b"legacy-value".to_vec())
        );
    }

    #[tokio::test]
    async fn durable_hybrid_entry_point_survives_app_restart() {
        let path = std::env::temp_dir().join(format!(
            "logical-db-hybrid-{}-{}.redb",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let account = scope(b"embedded-app");
        let object = Tuple::new(vec![Element::Bytes(b"document".to_vec())]);
        {
            let db = HybridDb::embedded_at(&path, FdbConfig::default()).unwrap();
            let mut batch = new_write_batch(&account, vec![Effect::Put]);
            batch
                .set(KeyFamily::Record, &object, b"durable".to_vec())
                .unwrap();
            let write = write_envelope(0, &batch, b"app-write");
            assert!(matches!(
                db.commit_admitted(write, batch).await.unwrap(),
                AdmittedOutcome::Applied { .. }
            ));
        }
        {
            let reopened = HybridDb::embedded_at(&path, FdbConfig::default()).unwrap();
            assert_eq!(reopened.read_version().await.unwrap(), 1);
            assert_eq!(
                reopened
                    .get_admitted(
                        read_envelope(1, account, b"app-read", KeyFamily::Record, &object,),
                        KeyFamily::Record,
                        &object,
                    )
                    .await
                    .unwrap(),
                Some(b"durable".to_vec())
            );
        }
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn zone_change_feed_is_durable_bounded_and_token_resumable() {
        let db = LogicalDb::open(FdbConfig::default()).unwrap();
        let account = scope(b"account-a");
        for (version, name) in [b"one".as_slice(), b"two".as_slice()]
            .into_iter()
            .enumerate()
        {
            let mut batch = new_write_batch(&account, vec![Effect::Put]);
            batch
                .set(
                    KeyFamily::Record,
                    &Tuple::new(vec![Element::Bytes(name.to_vec())]),
                    name.to_vec(),
                )
                .unwrap();
            let write = write_envelope(version as u64, &batch, name);
            db.commit_admitted(write, batch).await.unwrap();
        }

        let first_envelope = scan_envelope(2, account.clone(), b"scan-1", None, 1);
        let first = db.changes_admitted(first_envelope, None, 1).await.unwrap();
        assert_eq!(first.events.len(), 1);
        assert!(first.more);
        assert!(matches!(
            first.events[0].mutations.as_slice(),
            [ChangeMutation::Set { .. }]
        ));

        let second_envelope =
            scan_envelope(2, account.clone(), b"scan-2", first.next_token.as_ref(), 1);
        let second = db
            .changes_admitted(second_envelope, first.next_token.as_ref(), 1)
            .await
            .unwrap();
        assert_eq!(second.events.len(), 1);
        assert!(!second.more);
        assert_ne!(first.events[0].operation_id, second.events[0].operation_id);
    }

    #[test]
    fn state_binding_requires_the_exact_database_fact() {
        let snapshot = FlowSnapshot::new(BTreeMap::new());
        assert!(matches!(
            StateBinding::new(0, &snapshot),
            Err(KernelError::MissingDatabaseVersionFact)
        ));
    }

    #[test]
    fn state_binding_rejects_versions_braid_cannot_represent() {
        let snapshot = FlowSnapshot::new(BTreeMap::new());
        assert!(matches!(
            StateBinding::new(i64::MAX as u64 + 1, &snapshot),
            Err(KernelError::DatabaseVersionOutOfRange(_))
        ));
    }

    #[test]
    fn state_binding_reports_the_nonzero_version_it_proved() {
        let account = scope(b"binding");
        let operation = OperationId::new(b"binding-op".to_vec()).unwrap();
        let batch = new_write_batch(&account, vec![Effect::Put]);
        let (_, snapshot, _) = runnable(41, batch.request_binding(&operation), &[Effect::Put]);
        let binding = StateBinding::new(41, &snapshot).unwrap();
        assert_eq!(binding.database_version(), 41);
        assert_eq!(binding.snapshot_cid(), snapshot.cid());
    }

    #[test]
    fn batch_limits_are_exact_and_include_transaction_metadata() {
        let account = scope(b"account-limits");
        let batch = new_write_batch(&account, vec![Effect::Put, Effect::Get]);
        let transaction = write_envelope(0, &batch, b"limit-operation");

        assert_eq!(MAX_CHANGE_PAGE_BYTES, 11_000_000);
        assert!(matches!(
            WriteBatch::new(account.clone(), vec![]),
            Err(KernelError::AuthorityScopeViolation)
        ));
        let normalized = WriteBatch::new(
            account.clone(),
            vec![Effect::Scan, Effect::Put, Effect::Put],
        )
        .unwrap();
        assert_eq!(normalized.effects, vec![Effect::Put, Effect::Scan]);

        let mut charged = batch.clone();
        charged.encoded_bytes = crate::fdb::MAX_TRANSACTION_BYTES - 2;
        charged.charge(2).unwrap();
        assert_eq!(charged.encoded_bytes, crate::fdb::MAX_TRANSACTION_BYTES);
        assert!(matches!(
            charged.charge(1),
            Err(KernelError::TransactionTooLarge { .. })
        ));

        let mut small_charge = batch.clone();
        small_charge.encoded_bytes = 2;
        small_charge.charge(3).unwrap();
        assert_eq!(small_charge.encoded_bytes, 5);

        let placeholder = WriteMutation::Clear { key: vec![1] };
        let mut keys = batch.clone();
        keys.mutations = vec![placeholder.clone(); crate::fdb::MAX_TRANSACTION_KEYS - 1];
        keys.charge(0).unwrap();
        keys.mutations.push(placeholder);
        match keys.charge(0) {
            Err(KernelError::TransactionTooLarge { size, limit }) => {
                assert_eq!(size, crate::fdb::MAX_TRANSACTION_KEYS + 1);
                assert_eq!(limit, crate::fdb::MAX_TRANSACTION_KEYS);
            }
            result => panic!("expected exact key-count rejection, got {result:?}"),
        }

        let change_mutations = batch.change_mutation_bytes();
        let exact_total = batch.encoded_bytes
            + operation_key(&transaction.scope, &transaction.operation_id).len()
            + OPERATION_MARKER_BYTES
            + 4
            + invocation_key(&transaction).len()
            + OPERATION_MARKER_BYTES
            + 4
            + frontier_key(&transaction.scope).len()
            + 32
            + embedded_change_key(&transaction.scope, &transaction.operation_id).len()
            + 4
            + change_value_len(&transaction.operation_id, &change_mutations);
        batch.validate_for(&transaction, exact_total).unwrap();
        assert!(matches!(
            batch.validate_for(&transaction, exact_total - 1),
            Err(KernelError::TransactionTooLarge { .. })
        ));

        let mut exact_change = batch.clone();
        let empty_change_len = change_value_len(&transaction.operation_id, &[]);
        let key_len = crate::fdb::MAX_VALUE_BYTES - empty_change_len - 9;
        exact_change.mutations = vec![WriteMutation::Clear {
            key: vec![1; key_len],
        }];
        exact_change.validate_for(&transaction, usize::MAX).unwrap();
        exact_change.mutations[0] = WriteMutation::Clear {
            key: vec![1; key_len + 1],
        };
        assert!(matches!(
            exact_change.validate_for(&transaction, usize::MAX),
            Err(KernelError::ValueTooLarge {
                len,
                limit: crate::fdb::MAX_VALUE_BYTES
            }) if len == crate::fdb::MAX_VALUE_BYTES + 1
        ));

        let mut wrong_scope = batch.clone();
        wrong_scope.scope = scope(b"other-account");
        assert!(matches!(
            wrong_scope.validate_for(&transaction, usize::MAX),
            Err(KernelError::AuthorityScopeViolation)
        ));
        let mut wrong_effects = batch;
        wrong_effects.effects = vec![Effect::Get];
        assert!(matches!(
            wrong_effects.validate_for(&transaction, usize::MAX),
            Err(KernelError::AuthorityScopeViolation)
        ));
    }

    #[tokio::test]
    async fn hybrid_read_version_and_stale_or_revoked_envelopes_fail_closed() {
        let db = HybridDb::embedded_memory(FdbConfig::default()).unwrap();
        assert_eq!(db.read_version().await.unwrap(), 0);

        let account = scope(b"account-validation");
        let stale = envelope(1, account.clone(), b"stale");
        assert!(matches!(
            validate_fresh_envelope(&stale, 0),
            Err(KernelError::StaleJustification {
                expected: 0,
                got: 1
            })
        ));

        let mut revoked = envelope(0, account, b"revoked");
        revoked.authority.allowed_scopes.clear();
        assert!(matches!(
            validate_envelope_authority(&revoked),
            Err(KernelError::AuthorityScopeViolation)
        ));
    }

    #[test]
    fn change_codec_covers_prefix_size_and_clear_events() {
        let account = scope(b"account-codec");
        let prefix = change_prefix(&account);
        assert_eq!(prefix, account.system_prefix(SystemFamily::Change));
        assert_ne!(prefix, vec![0]);
        assert_ne!(prefix, vec![1]);

        let expected_frontier = account.pack_system(
            SystemFamily::Frontier,
            &Tuple::new(vec![Element::Bytes(b"zone-frontier".to_vec())]),
        );
        assert_eq!(frontier_key(&account), expected_frontier);

        let operation = OperationId::new(b"codec-operation".to_vec()).unwrap();
        let mut expected_embedded = prefix.clone();
        expected_embedded.extend_from_slice(&[0xFF; VERSIONSTAMP_BYTES]);
        expected_embedded.extend_from_slice(&(operation.as_bytes().len() as u16).to_be_bytes());
        expected_embedded.extend_from_slice(operation.as_bytes());
        assert_eq!(embedded_change_key(&account, &operation), expected_embedded);
        let tuple = Tuple::new(vec![Element::Bytes(b"record".to_vec())]);
        let key = account.pack(KeyFamily::Record, &tuple);
        let mut mutations = vec![2];
        append_framed(&mut mutations, &key);
        let fingerprint = Cid::compute(b"logical-db.test", b"codec");
        let encoded = change_value(&operation, fingerprint, &mutations);
        assert_eq!(change_value_len(&operation, &mutations), encoded.len());

        let event =
            decode_change_event(&account, change_token(&account, b"codec"), &encoded).unwrap();
        assert!(matches!(
            event.mutations.as_slice(),
            [ChangeMutation::Clear { key: changed }] if changed.family == KeyFamily::Record
        ));
    }

    #[test]
    fn native_marker_accepts_nonzero_fdb_batch_order() {
        let fingerprint = Cid::compute(b"logical-db.test", b"marker");
        let mut marker = operation_marker_template(fingerprint);
        marker[32..40].copy_from_slice(&42_u64.to_be_bytes());
        marker[40..42].copy_from_slice(&7_u16.to_be_bytes());
        assert_eq!(decode_operation_marker(&marker, fingerprint).unwrap(), 42);
    }

    #[test]
    fn namespace_authority_cannot_be_transplanted_between_accounts() {
        let a = scope(b"account-a");
        let b = scope(b"account-b");
        let authority = AuthorityProofRef::new("auth", vec![a], vec![Effect::Put]).unwrap();
        assert!(!authority.covers(&b, &[Effect::Put]));
    }

    #[test]
    fn batch_cannot_use_an_undeclared_effect() {
        let account = scope(b"account");
        let mut batch = new_write_batch(&account, vec![Effect::Get]);
        assert!(matches!(
            batch.set(KeyFamily::Record, &Tuple::new(vec![]), vec![]),
            Err(KernelError::AuthorityScopeViolation)
        ));
    }

    #[test]
    fn admitted_contract_exposes_the_exact_braid_and_authority_binding() {
        let account = scope(b"account-contract");
        let transaction = envelope(0, account.clone(), b"contract-operation");

        assert_eq!(transaction.state().database_version(), 0);
        assert_eq!(
            transaction.state().snapshot_cid(),
            transaction.braid_proof().snapshot_cid
        );
        assert_eq!(transaction.scope(), &account);
        assert_eq!(transaction.declared_effects(), &[Effect::Put, Effect::Get]);
        assert_eq!(transaction.operation_id().as_bytes(), b"contract-operation");
        assert_eq!(transaction.authority.id(), "kernel-capability-1");
        assert_ne!(transaction.braid_proof().capsule_cid.0, [0; 32]);
        assert_ne!(transaction.braid_proof().registry_cid.0, [0; 32]);
        assert_ne!(transaction.braid_proof().authority_cid.0, [0; 32]);
        assert_ne!(transaction.braid_proof().predicate_cid.0, [0; 32]);
    }

    #[test]
    fn receipt_port_preserves_every_audit_dimension() {
        let account = scope(b"account-receipt");
        let transaction = envelope(0, account, b"receipt-operation");
        let receipt = receipt_from(&transaction, 41);
        let rendered = String::from_utf8(JsonReceiptPort.render(&receipt).unwrap()).unwrap();

        assert!(rendered.contains("\"committed_version\":41"));
        assert!(rendered.contains("\"operation_id\":\"726563656970742d6f7065726174696f6e\""));
        assert!(rendered.contains("\"authority_proof\":\"kernel-capability-1\""));
        assert!(rendered.contains("\"effects\":[1,3]"));
        assert!(rendered.contains(&receipt.braid.capsule_cid.to_hex()));
        assert!(rendered.contains(&receipt.braid.registry_cid.to_hex()));
        assert!(rendered.contains(&receipt.braid.authority_cid.to_hex()));
        assert!(rendered.contains(&receipt.braid.snapshot_cid.to_hex()));
        assert!(rendered.contains(&receipt.braid.predicate_cid.to_hex()));
        assert_eq!(Effect::Delete.tag(), 2);
        assert_eq!(Effect::Scan.tag(), 4);

        let mut hostile = receipt;
        hostile.authority_proof_id = "\"},\n\"forged\":true".into();
        let encoded = JsonReceiptPort.render(&hostile).unwrap();
        let value: lgwks_std::json::Value = lgwks_std::json::from_slice(&encoded).unwrap();
        assert_eq!(
            value
                .get("authority_proof")
                .and_then(|value| value.as_str()),
            Some(hostile.authority_proof_id.as_str())
        );
        assert!(value.get("forged").is_none());
    }

    #[test]
    fn change_token_enforces_both_wire_bounds() {
        let account = scope(b"token-scope");
        assert!(matches!(
            ChangeToken::from_bytes(Vec::new()),
            Err(KernelError::MalformedKey)
        ));
        let minimum = change_token(&account, b"x");
        assert_eq!(minimum.as_bytes().len(), MIN_CHANGE_TOKEN_BYTES);
        assert_eq!(
            minimum.suffix_for(&account).unwrap().len(),
            MIN_CHANGE_SUFFIX_BYTES
        );
        let maximum = change_token(&account, &vec![7; MAX_OPERATION_ID_BYTES]);
        assert_eq!(maximum.as_bytes().len(), MAX_CHANGE_TOKEN_BYTES);
        assert!(ChangeToken::from_bytes(maximum.as_bytes().to_vec()).is_ok());
        assert!(matches!(
            ChangeToken::from_bytes(vec![7; TOKEN_SCOPE_BYTES + MAX_OPERATION_ID_BYTES + 13]),
            Err(KernelError::MalformedKey)
        ));
        assert!(matches!(
            ChangeToken::from_bytes(vec![7; MIN_CHANGE_TOKEN_BYTES - 1]),
            Err(KernelError::MalformedKey)
        ));
        let mut zero_operation = minimum.as_bytes().to_vec();
        zero_operation[TOKEN_SCOPE_BYTES + VERSIONSTAMP_BYTES..][..2]
            .copy_from_slice(&0_u16.to_be_bytes());
        assert!(matches!(
            ChangeToken::from_bytes(zero_operation),
            Err(KernelError::MalformedKey)
        ));
        let mut mismatched_operation = minimum.as_bytes().to_vec();
        mismatched_operation[TOKEN_SCOPE_BYTES + VERSIONSTAMP_BYTES..][..2]
            .copy_from_slice(&2_u16.to_be_bytes());
        assert!(matches!(
            ChangeToken::from_bytes(mismatched_operation),
            Err(KernelError::MalformedKey)
        ));
        assert!(matches!(
            minimum.suffix_for(&scope(b"other-token-scope")),
            Err(KernelError::TenantPrefixViolation)
        ));
        assert!(matches!(
            physical_change_key(
                &change_prefix(&scope(b"other-token-scope")),
                &scope(b"other-token-scope"),
                &minimum,
            ),
            Err(KernelError::TenantPrefixViolation)
        ));
    }

    #[cfg(feature = "fdb")]
    #[test]
    fn native_errors_preserve_actionable_categories() {
        assert!(matches!(
            native_fdb_error(foundationdb::FdbError::from_code(1020)),
            KernelError::TransactionConflict
        ));
        assert!(matches!(
            native_fdb_error(foundationdb::FdbError::from_code(1031)),
            KernelError::BackendTimedOut { code: 1031 }
        ));
        assert!(matches!(
            native_fdb_error(foundationdb::FdbError::from_code(2008)),
            KernelError::BackendUnavailable { code: 2008 }
        ));
        assert!(matches!(
            native_fdb_error(foundationdb::FdbError::from_code(2203)),
            KernelError::BackendVersionSkew { code: 2203 }
        ));
        for code in [1021, 1022, 1030] {
            assert!(matches!(
                native_fdb_error(foundationdb::FdbError::from_code(code)),
                KernelError::CommitOutcomeUnknown { code: actual } if actual == code
            ));
        }
    }

    #[tokio::test]
    async fn embedded_admitted_wait_honors_timeout_before_storage_begins() {
        let options = FdbConfig {
            transaction_timeout_ms: 1,
            ..FdbConfig::default()
        };
        let db = LogicalDb::open(options).unwrap();
        let account = scope(b"embedded-timeout");
        let object = Tuple::new(vec![Element::Bytes(b"record".to_vec())]);
        let read = read_envelope(0, account, b"blocked-read", KeyFamily::Record, &object);
        let backend = std::sync::Arc::clone(&db.backend);
        let (held_tx, held_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let holder = std::thread::spawn(move || {
            let _guard = backend.lock().unwrap();
            held_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });
        held_rx.recv().unwrap();
        let result = db.get_admitted(read, KeyFamily::Record, &object).await;
        release_tx.send(()).unwrap();
        holder.join().unwrap();
        assert!(matches!(
            result,
            Err(KernelError::TransactionTimedOut {
                elapsed_ms,
                limit_ms: 1,
            }) if elapsed_ms >= 1
        ));
    }

    #[test]
    fn write_batch_accounts_for_set_clear_and_canonical_bytes() {
        let account = scope(b"account-batch");
        let set_tuple = Tuple::new(vec![Element::Bytes(b"set".to_vec())]);
        let clear_tuple = Tuple::new(vec![Element::Bytes(b"clear".to_vec())]);
        let mut batch = new_write_batch(&account, vec![Effect::Delete, Effect::Put]);

        assert!(batch.is_empty());
        assert_eq!(batch.len(), 0);
        batch
            .set(KeyFamily::Record, &set_tuple, b"value".to_vec())
            .unwrap();
        batch.clear(KeyFamily::Index, &clear_tuple).unwrap();
        assert!(!batch.is_empty());
        assert_eq!(batch.len(), 2);

        let set_key = account.pack(KeyFamily::Record, &set_tuple);
        let clear_key = account.pack(KeyFamily::Index, &clear_tuple);
        assert_eq!(
            batch.encoded_bytes,
            set_key.len() + b"value".len() + clear_key.len()
        );
        let mut expected = vec![1];
        append_framed(&mut expected, &set_key);
        append_framed(&mut expected, b"value");
        expected.push(2);
        append_framed(&mut expected, &clear_key);
        assert_eq!(batch.canonical_bytes(), expected);
    }

    #[test]
    fn change_request_and_page_fail_closed_at_each_boundary() {
        let account = scope(b"account-change-boundary");
        assert!(matches!(
            validate_change_request(&envelope(0, account.clone(), b"not-scan"), 1),
            Err(KernelError::AuthorityScopeViolation)
        ));
        assert!(matches!(
            validate_change_request(&scan_envelope(0, account.clone(), b"zero", None, 1), 0,),
            Err(KernelError::RangeLimitInvalid { .. })
        ));
        assert!(validate_change_request(
            &scan_envelope(0, account.clone(), b"maximum", None, MAX_PUBLIC_RANGE_ROWS,),
            MAX_PUBLIC_RANGE_ROWS
        )
        .is_ok());
        assert!(matches!(
            validate_change_request(
                &scan_envelope(0, account.clone(), b"too-many", None, 1),
                MAX_PUBLIC_RANGE_ROWS + 1
            ),
            Err(KernelError::RangeLimitInvalid { .. })
        ));

        let token = change_token(&account, b"page");
        let operation = OperationId::new(b"change-operation".to_vec()).unwrap();
        let fingerprint = Cid::compute(b"logical-db.test", b"change");
        let tuple = Tuple::new(vec![Element::Bytes(b"record".to_vec())]);
        let key = account.pack(KeyFamily::Record, &tuple);
        let mutations = {
            let mut bytes = vec![1];
            append_framed(&mut bytes, &key);
            bytes
        };
        let value = change_value(&operation, fingerprint, &mutations);
        let mut physical_key = change_prefix(&account);
        physical_key.extend_from_slice(token.suffix_for(&account).unwrap());
        let page = change_page(&account, vec![(physical_key, value)], true).unwrap();
        assert_eq!(page.events.len(), 1);
        assert_eq!(page.next_token, Some(token));
        assert!(page.more);
        assert_eq!(page.events[0].operation_id, operation);
        assert_eq!(page.events[0].fingerprint, fingerprint);
        assert!(matches!(
            page.events[0].mutations.as_slice(),
            [ChangeMutation::Set { key: changed }] if changed.family == KeyFamily::Record
        ));

        assert!(matches!(
            decode_change_event(&account, change_token(&account, b"bad-short"), &[1; 32]),
            Err(KernelError::MalformedKey)
        ));
        let mut wrong_tag = vec![2];
        wrong_tag.extend_from_slice(&[0; 32]);
        assert!(matches!(
            decode_change_event(&account, change_token(&account, b"bad-tag"), &wrong_tag),
            Err(KernelError::MalformedKey)
        ));
    }
}
