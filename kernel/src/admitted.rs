//! Braid-bound scoped transactions — ADR-003.
//!
//! This is the shared processing boundary for embedded and outboard profiles.
//! It accepts only verifier/planner-created [`RunnableInvocation`] values,
//! binds their immutable Flow snapshot to the exact database read version,
//! scopes every physical key, and durably deduplicates operation identifiers.

use std::ops::Bound;

use braid_flow_plan::FlowSnapshot;
use braid_ir::{Cid, Value};
use braid_run::RunnableInvocation;

use crate::fdb::{FdbTransaction, LogicalDb};
use crate::tuple::{Element, KeyFamily, Scope, Tuple};
use crate::{KernelError, Result};

pub const DATABASE_VERSION_FACT: &str = "logical_db.read_version";
pub const MAX_PROOF_ID_BYTES: usize = 1_024;
pub const MAX_OPERATION_ID_BYTES: usize = 1_024;
pub const MAX_DECLARED_EFFECTS: usize = 16;
pub const MAX_AUTHORITY_SCOPES: usize = 128;
pub const MAX_PUBLIC_RANGE_ROWS: usize = 10_000;

const OPERATION_DOMAIN: &[u8] = b"lw.logical-db.operation.v2";
const VERSIONSTAMP_BYTES: usize = 10;
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

/// External namespace authorization remains owned by Kernel/Secure Authority.
/// This value is a scoped assertion consumed at the DB boundary, not a new
/// principal or capability-token authority.
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
    snapshot_cid: Cid,
}

impl StateBinding {
    pub fn new(database_version: u64, snapshot: &FlowSnapshot) -> Result<Self> {
        let fact_version = i64::try_from(database_version)
            .map_err(|_| KernelError::DatabaseVersionOutOfRange(database_version))?;
        match snapshot.get_by_str(DATABASE_VERSION_FACT) {
            Some(Value::Int(version)) if *version == fact_version => Ok(Self {
                database_version,
                snapshot_cid: snapshot.cid(),
            }),
            _ => Err(KernelError::MissingDatabaseVersionFact),
        }
    }

    pub fn database_version(&self) -> u64 {
        self.database_version
    }

    pub fn snapshot_cid(&self) -> Cid {
        self.snapshot_cid
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
        authority: AuthorityProofRef,
        scope: Scope,
        mut declared_effects: Vec<Effect>,
        operation_id: OperationId,
    ) -> Result<Self> {
        declared_effects.sort_unstable();
        declared_effects.dedup();
        if declared_effects.is_empty() || declared_effects.len() > MAX_DECLARED_EFFECTS {
            return Err(KernelError::AuthorityScopeViolation);
        }
        if !authority.covers(&scope, &declared_effects) {
            return Err(KernelError::AuthorityScopeViolation);
        }
        let braid = BraidProofBinding::from_runnable(&runnable);
        if braid.snapshot_cid != state.snapshot_cid {
            return Err(KernelError::BraidSnapshotMismatch);
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
        if bytes.is_empty() || bytes.len() > MAX_OPERATION_ID_BYTES + 12 {
            return Err(KernelError::MalformedKey);
        }
        Ok(Self(bytes))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
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

impl AdmittedTxn {
    pub fn write_batch(&self) -> WriteBatch {
        WriteBatch {
            scope: self.scope.clone(),
            effects: self.declared_effects.clone(),
            mutations: Vec::new(),
            encoded_bytes: 0,
        }
    }
}

impl WriteBatch {
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
        let key = self.scope.pack(family, tuple);
        self.charge(key.len().saturating_add(value.len()))?;
        self.mutations.push(WriteMutation::Set { key, value });
        Ok(())
    }

    pub fn clear(&mut self, family: KeyFamily, tuple: &Tuple) -> Result<()> {
        self.require(Effect::Delete)?;
        let key = self.scope.pack(family, tuple);
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
        let total = self
            .encoded_bytes
            .saturating_add(operation_key(&envelope.scope, &envelope.operation_id).len())
            .saturating_add(OPERATION_MARKER_BYTES + 4)
            .saturating_add(invocation_key(envelope).len())
            .saturating_add(OPERATION_MARKER_BYTES + 4)
            .saturating_add(embedded_change_key(&envelope.scope, &envelope.operation_id).len() + 4)
            .saturating_add(change_value_len(&envelope.operation_id, &change_mutations));
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

    fn apply_embedded(&self, tx: &mut FdbTransaction) -> Result<()> {
        for mutation in &self.mutations {
            match mutation {
                WriteMutation::Set { key, value } => tx.set(key.clone(), value.clone())?,
                WriteMutation::Clear { key } => tx.clear(key)?,
            }
        }
        Ok(())
    }
}

pub trait ReceiptPort {
    fn render(&self, receipt: &Receipt) -> Vec<u8>;
}

pub struct JsonReceiptPort;

impl ReceiptPort for JsonReceiptPort {
    fn render(&self, receipt: &Receipt) -> Vec<u8> {
        let effects = receipt
            .declared_effects
            .iter()
            .map(|effect| effect.tag().to_string())
            .collect::<Vec<_>>()
            .join(",");
        format!(
            concat!(
                "{{\"committed_version\":{},\"operation_id\":\"{}\",",
                "\"capsule_cid\":\"{}\",\"registry_cid\":\"{}\",",
                "\"braid_authority_cid\":\"{}\",\"snapshot_cid\":\"{}\",",
                "\"predicate_cid\":\"{}\",\"authority_proof\":\"{}\",",
                "\"account\":\"{}\",\"container\":\"{}\",\"zone\":\"{}\",",
                "\"effects\":[{}]}}"
            ),
            receipt.committed_version,
            lgwks_std::hex::encode(receipt.operation_id.as_bytes()),
            receipt.braid.capsule_cid.to_hex(),
            receipt.braid.registry_cid.to_hex(),
            receipt.braid.authority_cid.to_hex(),
            receipt.braid.snapshot_cid.to_hex(),
            receipt.braid.predicate_cid.to_hex(),
            receipt.authority_proof_id,
            lgwks_std::hex::encode(receipt.scope.account().as_bytes()),
            lgwks_std::hex::encode(receipt.scope.container().as_bytes()),
            lgwks_std::hex::encode(receipt.scope.zone().as_bytes()),
            effects,
        )
        .into_bytes()
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
        let (current, snapshot) = self.versioned_snapshot()?;
        let marker_key = operation_key(&envelope.scope, &envelope.operation_id);
        let invocation_key = invocation_key(&envelope);
        let existing_marker = snapshot.get(&marker_key).cloned();
        let mutation_bytes = batch.canonical_bytes();
        let change_mutations = batch.change_mutation_bytes();
        let fingerprint = operation_fingerprint(&envelope, &mutation_bytes);

        if let Some(marker) = existing_marker.as_deref() {
            let committed_version = decode_operation_marker(marker, fingerprint)?;
            return Ok(AdmittedOutcome::Duplicate {
                receipt: receipt_from(&envelope, committed_version),
            });
        }
        if snapshot.contains_key(&invocation_key) {
            return Err(KernelError::BraidInvocationConsumed);
        }
        validate_fresh_envelope(&envelope, current)?;

        let mut tx = FdbTransaction::new_with_snapshot(
            self.config().transaction_size_limit_bytes,
            self.config().transaction_timeout_ms,
            envelope.state.database_version,
            snapshot,
        );
        tx.require_exact_read_version();
        batch.apply_embedded(&mut tx)?;
        tx.set_versionstamped_key(
            embedded_change_key(&envelope.scope, &envelope.operation_id),
            change_value(&envelope.operation_id, fingerprint, &change_mutations),
        )?;
        tx.set_versionstamped_value(marker_key, operation_marker_template(fingerprint))?;
        tx.set_versionstamped_value(invocation_key, operation_marker_template(fingerprint))?;

        let committed_version = self.try_commit(tx).await?;
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
        let (current, snapshot) = self.versioned_snapshot()?;
        validate_fresh_envelope(&envelope, current)?;
        Ok(snapshot.get(&envelope.scope.pack(family, tuple)).cloned())
    }

    pub async fn changes_admitted(
        &self,
        envelope: AdmittedTxn,
        after: Option<&ChangeToken>,
        limit: usize,
    ) -> Result<ChangePage> {
        validate_change_request(&envelope, limit)?;
        let (current, snapshot) = self.versioned_snapshot()?;
        validate_fresh_envelope(&envelope, current)?;
        let prefix = change_prefix(&envelope.scope);
        let end = exclusive_prefix_end(&prefix).ok_or(KernelError::MalformedKey)?;
        let start_key = after.map(|token| physical_change_key(&prefix, token));
        let start = start_key
            .as_deref()
            .map_or(Bound::Included(prefix.as_slice()), Bound::Excluded);
        let mut rows = snapshot
            .range::<[u8], _>((start, Bound::Excluded(end.as_slice())))
            .take(limit.saturating_add(1))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<Vec<_>>();
        let more = rows.len() > limit;
        rows.truncate(limit);
        change_page(&envelope.scope, rows, more)
    }
}

/// One app-facing database handle for both deployment profiles. The logical
/// request model is identical; only the final transaction driver changes.
pub enum HybridDb {
    Embedded(LogicalDb),
    #[cfg(feature = "fdb")]
    Outboard(OutboardDb),
}

impl HybridDb {
    /// Open a zero-daemon, process-local database. This is useful for tests and
    /// ephemeral app state; use [`Self::embedded_at`] for durable embedding.
    pub fn embedded_memory(options: crate::fdb::DatabaseOptions) -> Result<Self> {
        Ok(Self::Embedded(LogicalDb::open(options)?))
    }

    /// Open a durable database inside the application process. The admitted
    /// processing and wire contracts are identical to the outboard profile.
    pub fn embedded_at(
        path: impl AsRef<std::path::Path>,
        options: crate::fdb::DatabaseOptions,
    ) -> Result<Self> {
        Ok(Self::Embedded(LogicalDb::open_at(path, options)?))
    }

    pub fn embedded(db: LogicalDb) -> Self {
        Self::Embedded(db)
    }

    #[cfg(feature = "fdb")]
    pub fn outboard(db: OutboardDb) -> Self {
        Self::Outboard(db)
    }

    /// Open the thin native client after the host has started FoundationDB's
    /// one-per-process network thread and retained its guard.
    #[cfg(feature = "fdb")]
    pub fn outboard_after_network_boot(options: crate::fdb::DatabaseOptions) -> Result<Self> {
        Ok(Self::Outboard(OutboardDb::open_after_network_boot(
            options,
        )?))
    }

    pub async fn read_version(&self) -> Result<u64> {
        match self {
            Self::Embedded(db) => db.read_version(),
            #[cfg(feature = "fdb")]
            Self::Outboard(db) => db.read_version().await,
        }
    }

    pub async fn commit_admitted(
        &self,
        envelope: AdmittedTxn,
        batch: WriteBatch,
    ) -> Result<AdmittedOutcome<()>> {
        match self {
            Self::Embedded(db) => db.commit_admitted(envelope, batch).await,
            #[cfg(feature = "fdb")]
            Self::Outboard(db) => db.commit_admitted(envelope, batch).await,
        }
    }

    pub async fn get_admitted(
        &self,
        envelope: AdmittedTxn,
        family: KeyFamily,
        tuple: &Tuple,
    ) -> Result<Option<Vec<u8>>> {
        match self {
            Self::Embedded(db) => db.get_admitted(envelope, family, tuple).await,
            #[cfg(feature = "fdb")]
            Self::Outboard(db) => db.get_admitted(envelope, family, tuple).await,
        }
    }

    pub async fn changes_admitted(
        &self,
        envelope: AdmittedTxn,
        after: Option<&ChangeToken>,
        limit: usize,
    ) -> Result<ChangePage> {
        match self {
            Self::Embedded(db) => db.changes_admitted(envelope, after, limit).await,
            #[cfg(feature = "fdb")]
            Self::Outboard(db) => db.changes_admitted(envelope, after, limit).await,
        }
    }
}

#[cfg(feature = "fdb")]
pub struct OutboardDb {
    database: std::sync::Arc<foundationdb::Database>,
    options: crate::fdb::DatabaseOptions,
}

#[cfg(feature = "fdb")]
impl OutboardDb {
    /// Attach the logical layer to a native FDB handle. The host process owns
    /// the one-per-process FoundationDB network guard and must keep it alive
    /// longer than this value.
    pub fn from_database(
        database: foundationdb::Database,
        options: crate::fdb::DatabaseOptions,
    ) -> Result<Self> {
        if options.transaction_size_limit_bytes > crate::fdb::MAX_TRANSACTION_BYTES {
            return Err(KernelError::TransactionTooLarge {
                size: options.transaction_size_limit_bytes,
                limit: crate::fdb::MAX_TRANSACTION_BYTES,
            });
        }
        i32::try_from(options.transaction_timeout_ms)
            .map_err(|_| KernelError::Backend("FDB timeout does not fit i32".into()))?;
        Ok(Self {
            database: std::sync::Arc::new(database),
            options,
        })
    }

    /// Open the configured cluster after the host has booted the FDB network.
    /// Logical-DB deliberately does not hide the unsafe global network guard.
    pub fn open_after_network_boot(options: crate::fdb::DatabaseOptions) -> Result<Self> {
        let database = foundationdb::Database::new(options.cluster_file.as_deref())
            .map_err(native_backend_error)?;
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
        validate_envelope_authority(&envelope)?;
        let mutation_bytes = batch.canonical_bytes();
        let change_mutations = batch.change_mutation_bytes();
        let fingerprint = operation_fingerprint(&envelope, &mutation_bytes);
        let marker_key = operation_key(&envelope.scope, &envelope.operation_id);
        let invocation_key = invocation_key(&envelope);
        let change_key = native_change_key(&envelope.scope, &envelope.operation_id);
        let change_value = change_value(&envelope.operation_id, fingerprint, &change_mutations);
        let expected_version = envelope.state.database_version;
        let timeout = native_timeout(&self.options)?;
        let batch = std::sync::Arc::new(batch);
        let retry_marker_key = std::sync::Arc::new(marker_key.clone());
        let retry_invocation_key = std::sync::Arc::new(invocation_key);
        let retry_change_key = std::sync::Arc::new(change_key);
        let retry_change_value = std::sync::Arc::new(change_value);

        let attempt = self
            .database
            .run(move |transaction, _maybe_committed| {
                let batch = std::sync::Arc::clone(&batch);
                let marker_key = std::sync::Arc::clone(&retry_marker_key);
                let invocation_key = std::sync::Arc::clone(&retry_invocation_key);
                let change_key = std::sync::Arc::clone(&retry_change_key);
                let change_value = std::sync::Arc::clone(&retry_change_value);
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
        validate_envelope_authority(&envelope)?;
        let key = envelope.scope.pack(family, tuple);
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

    pub async fn changes_admitted(
        &self,
        envelope: AdmittedTxn,
        after: Option<&ChangeToken>,
        limit: usize,
    ) -> Result<ChangePage> {
        validate_change_request(&envelope, limit)?;
        validate_envelope_authority(&envelope)?;
        let scope = envelope.scope.clone();
        let prefix = change_prefix(&scope);
        let end = exclusive_prefix_end(&prefix).ok_or(KernelError::MalformedKey)?;
        let begin = after
            .map(|token| {
                foundationdb::KeySelector::first_greater_than(physical_change_key(&prefix, token))
            })
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
                    let options = foundationdb::RangeOption {
                        begin,
                        end: foundationdb::KeySelector::first_greater_or_equal(end),
                        limit: Some(limit),
                        ..foundationdb::RangeOption::default()
                    };
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
fn native_timeout(options: &crate::fdb::DatabaseOptions) -> Result<i32> {
    i32::try_from(options.transaction_timeout_ms)
        .map_err(|_| KernelError::Backend("FDB timeout does not fit i32".into()))
}

#[cfg(feature = "fdb")]
fn native_version(version: i64) -> Result<u64> {
    u64::try_from(version)
        .map_err(|_| KernelError::Backend(format!("FDB returned invalid version {version}")))
}

#[cfg(feature = "fdb")]
fn native_backend_error(error: impl std::fmt::Display) -> KernelError {
    KernelError::Backend(format!("native FDB: {error}"))
}

#[cfg(feature = "fdb")]
fn native_marker_template(fingerprint: Cid) -> Vec<u8> {
    let mut marker = operation_marker_template(fingerprint);
    marker.extend_from_slice(&(32_u32).to_le_bytes());
    marker
}

fn change_prefix(scope: &Scope) -> Vec<u8> {
    scope.family_prefix(KeyFamily::Change)
}

fn embedded_change_key(scope: &Scope, operation_id: &OperationId) -> Vec<u8> {
    let mut key = change_prefix(scope);
    key.extend_from_slice(&[0xFF; VERSIONSTAMP_BYTES]);
    key.extend_from_slice(&(operation_id.as_bytes().len() as u16).to_be_bytes());
    key.extend_from_slice(operation_id.as_bytes());
    key
}

#[cfg(feature = "fdb")]
fn native_change_key(scope: &Scope, operation_id: &OperationId) -> Vec<u8> {
    let mut key = embedded_change_key(scope, operation_id);
    let offset = u32::try_from(change_prefix(scope).len())
        .expect("bounded scope prefix always fits the FDB versionstamp offset");
    key.extend_from_slice(&offset.to_le_bytes());
    key
}

fn physical_change_key(prefix: &[u8], token: &ChangeToken) -> Vec<u8> {
    let mut key = prefix.to_vec();
    key.extend_from_slice(token.as_bytes());
    key
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
    let prefix = change_prefix(scope);
    let mut events = Vec::with_capacity(rows.len());
    for (key, value) in rows {
        let suffix = key
            .strip_prefix(prefix.as_slice())
            .ok_or(KernelError::TenantPrefixViolation)?;
        let token = ChangeToken::from_bytes(suffix.to_vec())?;
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
    scope.pack(
        KeyFamily::Outbox,
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
    envelope.scope.pack(
        KeyFamily::Outbox,
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
    use braid_flow_ir::Predicate;
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

    fn registry_and_capsule() -> (TermRegistry, Capsule, Vec<Capability>) {
        let capability = Capability::new("logical-db.record.write");
        let mut registry = TermRegistry::new(1);
        registry
            .insert(TermSpec {
                id: "logical-db.record.write".into(),
                inputs: vec![],
                output: TypeTag::Bool,
                capability: Some(capability.clone()),
                effect: EffectClass::ReversibleWrite,
                source_exposure: Exposure::Public,
                egress_ceiling: None,
                cost: 1,
            })
            .unwrap();
        let capsule = Capsule {
            ir_version: IR_VERSION,
            vocab_version: registry.vocab_version,
            registry_cid: registry.cid(),
            intent: "write one scoped logical-db record".into(),
            grants: vec![capability.clone()],
            braid: Braid {
                strands: vec![Strand {
                    term: "logical-db.record.write".into(),
                    inputs: vec![],
                }],
                outputs: vec![0],
            },
            budget: 1,
            confirm: ConfirmPolicy::None,
            evidence: vec![],
        };
        (registry, capsule, vec![capability])
    }

    fn runnable(version: u64) -> (RunnableInvocation, FlowSnapshot) {
        let (registry, capsule, ambient) = registry_and_capsule();
        let snapshot = FlowSnapshot::new(BTreeMap::from([(
            DATABASE_VERSION_FACT.to_string(),
            Value::Int(version as i64),
        )]));
        let justification = JustificationProof::prove(
            &capsule,
            &Predicate::Const(true),
            &snapshot,
            &CompletionMap::new(),
        )
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
        (runnable, snapshot)
    }

    fn envelope(version: u64, scope: Scope, operation: &[u8]) -> AdmittedTxn {
        let (runnable, snapshot) = runnable(version);
        let state = StateBinding::new(version, &snapshot).unwrap();
        let authority = AuthorityProofRef::new(
            "kernel-capability-1",
            vec![scope.clone()],
            vec![Effect::Put, Effect::Get],
        )
        .unwrap();
        AdmittedTxn::new(
            runnable,
            state,
            authority,
            scope,
            vec![Effect::Put, Effect::Get],
            OperationId::new(operation.to_vec()).unwrap(),
        )
        .unwrap()
    }

    fn scan_envelope(version: u64, scope: Scope, operation: &[u8]) -> AdmittedTxn {
        let (runnable, snapshot) = runnable(version);
        let state = StateBinding::new(version, &snapshot).unwrap();
        let authority =
            AuthorityProofRef::new("kernel-scan-1", vec![scope.clone()], vec![Effect::Scan])
                .unwrap();
        AdmittedTxn::new(
            runnable,
            state,
            authority,
            scope,
            vec![Effect::Scan],
            OperationId::new(operation.to_vec()).unwrap(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn real_braid_runnable_gates_a_scoped_embedded_transaction() {
        let db = LogicalDb::open(FdbConfig::default()).unwrap();
        let account = scope(b"account-a");
        let object = Tuple::new(vec![Element::Bytes(b"object-1".to_vec())]);
        let write = envelope(0, account.clone(), b"op-1");
        let mut batch = write.write_batch();
        batch
            .set(KeyFamily::Record, &object, b"value".to_vec())
            .unwrap();
        let outcome = db.commit_admitted(write, batch).await.unwrap();
        match outcome {
            AdmittedOutcome::Applied { receipt, .. } => {
                assert_eq!(receipt.committed_version, 1);
                assert_ne!(receipt.braid.capsule_cid.0, [0; 32]);
            }
            AdmittedOutcome::Duplicate { .. } => panic!("first operation cannot be duplicate"),
        }
        assert_eq!(
            db.get_admitted(envelope(1, account, b"read-1"), KeyFamily::Record, &object)
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
        let first_envelope = envelope(0, account.clone(), b"same-op");
        let mut first_batch = first_envelope.write_batch();
        first_batch
            .set(KeyFamily::Record, &object, b"value".to_vec())
            .unwrap();
        let first = db
            .commit_admitted(first_envelope, first_batch)
            .await
            .unwrap();
        assert!(matches!(first, AdmittedOutcome::Applied { .. }));

        let retry_envelope = envelope(0, account, b"same-op");
        let mut retry_batch = retry_envelope.write_batch();
        retry_batch
            .set(KeyFamily::Record, &object, b"value".to_vec())
            .unwrap();
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
        let first = envelope(0, account.clone(), b"same-op");
        let mut first_batch = first.write_batch();
        first_batch
            .set(KeyFamily::Record, &object, b"first".to_vec())
            .unwrap();
        db.commit_admitted(first, first_batch).await.unwrap();

        let conflicting = envelope(0, account, b"same-op");
        let mut conflicting_batch = conflicting.write_batch();
        conflicting_batch
            .set(KeyFamily::Record, &object, b"second".to_vec())
            .unwrap();
        assert!(matches!(
            db.commit_admitted(conflicting, conflicting_batch).await,
            Err(KernelError::OperationIdConflict)
        ));
    }

    #[tokio::test]
    async fn one_braid_invocation_cannot_authorize_two_operations() {
        let db = LogicalDb::open(FdbConfig::default()).unwrap();
        let account = scope(b"account-a");
        let first_object = Tuple::new(vec![Element::Bytes(b"first".to_vec())]);
        let first = envelope(0, account.clone(), b"operation-one");
        let mut first_batch = first.write_batch();
        first_batch
            .set(KeyFamily::Record, &first_object, b"one".to_vec())
            .unwrap();
        db.commit_admitted(first, first_batch).await.unwrap();

        // The helper reconstructs the same content-addressed Braid invocation
        // at version zero. A different operation ID must not turn that proof
        // into a reusable bearer capability.
        let second_object = Tuple::new(vec![Element::Bytes(b"second".to_vec())]);
        let second = envelope(0, account, b"operation-two");
        let mut second_batch = second.write_batch();
        second_batch
            .set(KeyFamily::Record, &second_object, b"two".to_vec())
            .unwrap();
        assert!(matches!(
            db.commit_admitted(second, second_batch).await,
            Err(KernelError::BraidInvocationConsumed)
        ));
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
        let write = envelope(0, account.clone(), b"legacy-copy");
        let mut batch = write.write_batch();
        legacy
            .copy_into(&mut batch, b"legacy-value".to_vec())
            .unwrap();
        db.commit_admitted(write, batch).await.unwrap();

        let read = envelope(1, account, b"legacy-read");
        assert_eq!(
            db.get_admitted(read, KeyFamily::Record, &legacy.destination_tuple())
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
            let write = envelope(0, account.clone(), b"app-write");
            let mut batch = write.write_batch();
            batch
                .set(KeyFamily::Record, &object, b"durable".to_vec())
                .unwrap();
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
                        envelope(1, account, b"app-read"),
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
            let write = envelope(version as u64, account.clone(), name);
            let mut batch = write.write_batch();
            batch
                .set(
                    KeyFamily::Record,
                    &Tuple::new(vec![Element::Bytes(name.to_vec())]),
                    name.to_vec(),
                )
                .unwrap();
            db.commit_admitted(write, batch).await.unwrap();
        }

        let first = db
            .changes_admitted(scan_envelope(2, account.clone(), b"scan-1"), None, 1)
            .await
            .unwrap();
        assert_eq!(first.events.len(), 1);
        assert!(first.more);
        assert!(matches!(
            first.events[0].mutations.as_slice(),
            [ChangeMutation::Set { .. }]
        ));

        let second = db
            .changes_admitted(
                scan_envelope(2, account, b"scan-2"),
                first.next_token.as_ref(),
                1,
            )
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
        let (runnable, snapshot) = runnable(0);
        let write = AdmittedTxn::new(
            runnable,
            StateBinding::new(0, &snapshot).unwrap(),
            AuthorityProofRef::new("auth", vec![account.clone()], vec![Effect::Get]).unwrap(),
            account,
            vec![Effect::Get],
            OperationId::new(b"read-only".to_vec()).unwrap(),
        )
        .unwrap();
        let mut batch = write.write_batch();
        assert!(matches!(
            batch.set(KeyFamily::Record, &Tuple::new(vec![]), vec![]),
            Err(KernelError::AuthorityScopeViolation)
        ));
    }
}
