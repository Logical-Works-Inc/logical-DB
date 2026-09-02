//! Multi-tenant protocol service with authenticated opaque checkpoints.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
//! S33. Source URLs are in `evidence/SOURCES.md`.

#![forbid(unsafe_code)]

use std::{future::Future, sync::Arc};

use hmac::{Hmac, KeyInit, Mac};
use logical_db_crdt::EncodedChange;
use logical_db_model::{Effect, OperationDigest, Scope};
use logical_db_protocol::{
    ChangeEnvelope, Checkpoint, PROTOCOL_VERSION, ProtocolError, PullPage, PullRequest,
    PushOutcome, PushRequest, PushResponse, RejectionCode,
};
use sha2::Sha256;
use thiserror::Error;

const CHECKPOINT_MAGIC: &[u8; 4] = b"LCP\x01";
const CHECKPOINT_BODY_BYTES: usize = 52;
const CHECKPOINT_BYTES: usize = CHECKPOINT_BODY_BYTES + 32;

/// Verifier-produced execution proof consumed by a storage transaction.
pub trait ExecutionGrant: Send + Sync {
    /// Scope proven by the invocation.
    fn scope(&self) -> Scope;
    /// Strongest database effect proven by the invocation.
    fn effect(&self) -> Effect;
    /// Canonical request digest bound into the invocation snapshot.
    fn request(&self) -> OperationDigest;
    /// Exact durable scope frontier justified before transaction execution.
    fn frontier(&self) -> u64;
    /// Stable content identity atomically consumed with a new write.
    fn invocation_id(&self) -> [u8; 32];
}

/// Host authority boundary. Production composition uses a Braid runnable invocation.
pub trait Authority {
    /// Concrete opaque proof forwarded into the durable transaction.
    type Grant: ExecutionGrant;

    /// Produces a proof for the exact request and observed durable frontier.
    fn authorize(
        &self,
        scope: Scope,
        effect: Effect,
        request: OperationDigest,
        frontier: u64,
        now_unix_ms: u64,
    ) -> Option<Self::Grant>;
}

/// Durable outcome of accepting one immutable change.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AcceptResult {
    /// Newly appended to the scoped change log.
    Accepted,
    /// Exact operation and bytes were already appended.
    Duplicate,
    /// A prior attempt may have committed and exact durable identity proved it did.
    RecoveredUnknownCommit,
    /// Operation identity exists with different request or change bytes.
    Conflict,
    /// Durable frontier changed after the invocation was justified.
    StaleAuthority,
    /// The invocation identity was atomically consumed by another operation.
    InvocationConsumed,
}

/// Bounded page returned by a service storage adapter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredPage {
    /// Changes strictly after the requested position.
    pub changes: Vec<ChangeEnvelope>,
    /// Durable position covering every returned change.
    pub position: u64,
    /// Whether another page is immediately available.
    pub more: bool,
}

/// Durable outcome of executing one frontier-bound page invocation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PageResult {
    /// The transaction observed the justified frontier and returned a page.
    Page(StoredPage),
    /// The scope frontier changed before the read transaction executed.
    StaleAuthority,
}

/// Service-side durable storage contract implemented by `FoundationDB`.
pub trait ServiceStore: Send + Sync {
    /// Adapter-specific failure.
    type Error;

    /// Idempotently accepts one validated immutable change.
    ///
    /// # Errors
    ///
    /// Returns a typed backend transaction or corruption failure.
    fn accept(
        &self,
        change: &ChangeEnvelope,
        grant: &impl ExecutionGrant,
    ) -> impl Future<Output = Result<AcceptResult, Self::Error>> + Send;

    /// Reads the current monotonic scoped frontier used for Braid justification.
    fn frontier(&self, scope: Scope) -> impl Future<Output = Result<u64, Self::Error>> + Send;

    /// Reads a bounded ordered page after a durable scoped position.
    ///
    /// # Errors
    ///
    /// Returns a typed backend read, decode, or corruption failure.
    fn page(
        &self,
        scope: Scope,
        request: OperationDigest,
        after: u64,
        max_changes: u16,
        max_bytes: u32,
        grant: &impl ExecutionGrant,
    ) -> impl Future<Output = Result<PageResult, Self::Error>> + Send;
}

impl<Store> ServiceStore for Arc<Store>
where
    Store: ServiceStore + ?Sized,
{
    type Error = Store::Error;

    fn accept(
        &self,
        change: &ChangeEnvelope,
        grant: &impl ExecutionGrant,
    ) -> impl Future<Output = Result<AcceptResult, Self::Error>> + Send {
        (**self).accept(change, grant)
    }

    fn frontier(&self, scope: Scope) -> impl Future<Output = Result<u64, Self::Error>> + Send {
        (**self).frontier(scope)
    }

    fn page(
        &self,
        scope: Scope,
        request: OperationDigest,
        after: u64,
        max_changes: u16,
        max_bytes: u32,
        grant: &impl ExecutionGrant,
    ) -> impl Future<Output = Result<PageResult, Self::Error>> + Send {
        (**self).page(scope, request, after, max_changes, max_bytes, grant)
    }
}

/// Constant-time authenticated codec for opaque, expiring scoped positions.
#[derive(Clone)]
pub struct CheckpointCodec {
    key: [u8; 32],
    ttl_ms: u64,
}

impl CheckpointCodec {
    /// Constructs a codec from host-managed key material and a nonzero TTL.
    ///
    /// # Errors
    ///
    /// Refuses an all-zero key or zero TTL.
    pub fn new(key: [u8; 32], ttl_ms: u64) -> Result<Self, CheckpointError> {
        if key == [0; 32] {
            return Err(CheckpointError::WeakKey);
        }
        if ttl_ms == 0 {
            return Err(CheckpointError::InvalidTtl);
        }
        Ok(Self { key, ttl_ms })
    }

    fn issue(
        &self,
        scope: Scope,
        position: u64,
        now_unix_ms: u64,
    ) -> Result<Checkpoint, CheckpointError> {
        let expires = now_unix_ms
            .checked_add(self.ttl_ms)
            .ok_or(CheckpointError::TimeOverflow)?;
        let mut body = [0; CHECKPOINT_BODY_BYTES];
        body[..4].copy_from_slice(CHECKPOINT_MAGIC);
        body[4..20].copy_from_slice(scope.account().as_uuid().as_bytes());
        body[20..36].copy_from_slice(scope.space().as_uuid().as_bytes());
        body[36..44].copy_from_slice(&position.to_be_bytes());
        body[44..52].copy_from_slice(&expires.to_be_bytes());
        let tag = self.tag(&body);
        let mut token = Vec::with_capacity(CHECKPOINT_BYTES);
        token.extend_from_slice(&body);
        token.extend_from_slice(&tag);
        Checkpoint::from_wire(token).map_err(CheckpointError::Protocol)
    }

    fn open(
        &self,
        scope: Scope,
        token: &Checkpoint,
        now_unix_ms: u64,
    ) -> Result<u64, CheckpointError> {
        let bytes = token.as_wire();
        if bytes.len() != CHECKPOINT_BYTES || &bytes[..4] != CHECKPOINT_MAGIC {
            return Err(CheckpointError::Malformed);
        }
        let mut mac = self.mac();
        mac.update(&bytes[..CHECKPOINT_BODY_BYTES]);
        mac.verify_slice(&bytes[CHECKPOINT_BODY_BYTES..])
            .map_err(|_| CheckpointError::InvalidMac)?;
        if bytes[4..20] != *scope.account().as_uuid().as_bytes()
            || bytes[20..36] != *scope.space().as_uuid().as_bytes()
        {
            return Err(CheckpointError::WrongScope);
        }
        let position = decode_u64(&bytes[36..44])?;
        let expires = decode_u64(&bytes[44..52])?;
        if now_unix_ms >= expires {
            return Err(CheckpointError::Expired);
        }
        Ok(position)
    }

    fn tag(&self, body: &[u8]) -> [u8; 32] {
        let mut mac = self.mac();
        mac.update(body);
        mac.finalize().into_bytes().into()
    }

    fn mac(&self) -> Hmac<Sha256> {
        <Hmac<Sha256> as KeyInit>::new_from_slice(&self.key).expect("HMAC accepts every key length")
    }
}

/// Opaque checkpoint validation failures.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum CheckpointError {
    /// Key material is an obvious unconfigured sentinel.
    #[error("checkpoint key must not be all zero")]
    WeakKey,
    /// Checkpoint TTL must be nonzero.
    #[error("checkpoint TTL must be nonzero")]
    InvalidTtl,
    /// Expiry arithmetic overflowed.
    #[error("checkpoint expiry overflowed")]
    TimeOverflow,
    /// Token length, magic, or integer encoding is malformed.
    #[error("checkpoint is malformed")]
    Malformed,
    /// Authentication tag did not verify.
    #[error("checkpoint authentication failed")]
    InvalidMac,
    /// Token belongs to another account or application space.
    #[error("checkpoint belongs to another scope")]
    WrongScope,
    /// Token's exclusive expiration has passed.
    #[error("checkpoint expired")]
    Expired,
    /// Protocol checkpoint constructor rejected the encoded token.
    #[error("checkpoint protocol encoding failed: {0}")]
    Protocol(ProtocolError),
}

/// Pull response distinguishes expiry from invalid or unauthorized input.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ServicePull {
    /// Valid bounded page.
    Page(PullPage),
    /// Valid authenticated checkpoint reached its expiry.
    CheckpointExpired,
}

/// Stateless service orchestration over a durable storage adapter.
pub struct ProtocolService<Store, Policy> {
    store: Store,
    policy: Policy,
    checkpoints: CheckpointCodec,
}

impl<Store, Policy> ProtocolService<Store, Policy>
where
    Store: ServiceStore,
    Policy: Authority,
{
    /// Constructs a service from explicit durable, authority, and token ports.
    #[must_use]
    pub const fn new(store: Store, policy: Policy, checkpoints: CheckpointCodec) -> Self {
        Self {
            store,
            policy,
            checkpoints,
        }
    }

    /// Validates and idempotently accepts a bounded upload page.
    ///
    /// # Errors
    ///
    /// Returns a page-level protocol or backend failure. Per-change validation,
    /// authority, and conflict failures are stable outcomes in request order.
    pub async fn push(
        &self,
        request: &PushRequest,
        now_unix_ms: u64,
    ) -> Result<PushResponse, ServiceError<Store::Error>> {
        request.validate().map_err(ServiceError::Protocol)?;
        let mut outcomes = Vec::with_capacity(request.changes.len());
        for change in &request.changes {
            let operation = change.operation;
            let frontier = self
                .store
                .frontier(change.scope)
                .await
                .map_err(ServiceError::Store)?;
            let Some(grant) = self.policy.authorize(
                change.scope,
                Effect::Write,
                change.request,
                frontier,
                now_unix_ms,
            ) else {
                outcomes.push(PushOutcome::Rejected {
                    operation,
                    code: RejectionCode::Forbidden,
                });
                continue;
            };
            if (EncodedChange {
                digest: change.digest,
                bytes: change.bytes.clone(),
            })
            .validate()
            .is_err()
            {
                outcomes.push(PushOutcome::Rejected {
                    operation,
                    code: RejectionCode::DigestMismatch,
                });
                continue;
            }
            let outcome = match self
                .store
                .accept(change, &grant)
                .await
                .map_err(ServiceError::Store)?
            {
                AcceptResult::Accepted => PushOutcome::Accepted { operation },
                AcceptResult::Duplicate | AcceptResult::RecoveredUnknownCommit => {
                    PushOutcome::Duplicate { operation }
                }
                AcceptResult::Conflict => PushOutcome::Rejected {
                    operation,
                    code: RejectionCode::OperationConflict,
                },
                AcceptResult::StaleAuthority => PushOutcome::Rejected {
                    operation,
                    code: RejectionCode::StaleAuthority,
                },
                AcceptResult::InvocationConsumed => PushOutcome::Rejected {
                    operation,
                    code: RejectionCode::InvocationConsumed,
                },
            };
            outcomes.push(outcome);
        }
        Ok(PushResponse {
            protocol: PROTOCOL_VERSION,
            outcomes,
        })
    }

    /// Reads one authorized page after an authenticated opaque checkpoint.
    ///
    /// # Errors
    ///
    /// Returns protocol, authority, checkpoint-authentication, backend, or
    /// cross-scope corruption failures. Expiry is a typed non-error response.
    pub async fn pull(
        &self,
        request: &PullRequest,
        now_unix_ms: u64,
    ) -> Result<ServicePull, ServiceError<Store::Error>> {
        request.validate().map_err(ServiceError::Protocol)?;
        let frontier = self
            .store
            .frontier(request.scope)
            .await
            .map_err(ServiceError::Store)?;
        let Some(grant) = self.policy.authorize(
            request.scope,
            Effect::Sync,
            request.request,
            frontier,
            now_unix_ms,
        ) else {
            return Err(ServiceError::Forbidden);
        };
        let after = match &request.after {
            Some(token) => match self.checkpoints.open(request.scope, token, now_unix_ms) {
                Ok(position) => position,
                Err(CheckpointError::Expired) => return Ok(ServicePull::CheckpointExpired),
                Err(error) => return Err(ServiceError::Checkpoint(error)),
            },
            None => 0,
        };
        let result = self
            .store
            .page(
                request.scope,
                request.request,
                after,
                request.max_changes,
                request.max_bytes,
                &grant,
            )
            .await
            .map_err(ServiceError::Store)?;
        let PageResult::Page(stored) = result else {
            return Err(ServiceError::StaleAuthority);
        };
        if !valid_stored_page(after, frontier, &stored) {
            return Err(ServiceError::CorruptStoreData);
        }
        if stored
            .changes
            .iter()
            .any(|change| change.scope != request.scope)
        {
            return Err(ServiceError::CrossScopeStoreData);
        }
        for change in &stored.changes {
            EncodedChange {
                digest: change.digest,
                bytes: change.bytes.clone(),
            }
            .validate()
            .map_err(|_| ServiceError::CorruptStoreData)?;
        }
        let checkpoint = self
            .checkpoints
            .issue(request.scope, stored.position, now_unix_ms)
            .map_err(ServiceError::Checkpoint)?;
        let page = PullPage {
            protocol: PROTOCOL_VERSION,
            changes: stored.changes,
            checkpoint,
            more: stored.more,
        };
        page.validate(request.scope)
            .map_err(ServiceError::Protocol)?;
        Ok(ServicePull::Page(page))
    }
}

fn valid_stored_page(after: u64, frontier: u64, page: &StoredPage) -> bool {
    let Ok(count) = u64::try_from(page.changes.len()) else {
        return false;
    };
    after
        .checked_add(count)
        .is_some_and(|position| position == page.position)
        && page.position <= frontier
        && page.more == (page.position < frontier)
}

/// Service boundary failures.
#[derive(Debug, Error)]
pub enum ServiceError<StoreError> {
    /// Protocol validation failed.
    #[error("service protocol rejected input: {0}")]
    Protocol(ProtocolError),
    /// Pull authority was denied.
    #[error("service authority denied the pull request")]
    Forbidden,
    /// Durable state changed after pull authority was justified.
    #[error("service authority became stale before the pull transaction")]
    StaleAuthority,
    /// Opaque checkpoint was malformed, forged, or cross-scoped.
    #[error("service checkpoint rejected input: {0}")]
    Checkpoint(CheckpointError),
    /// Durable adapter failed.
    #[error("service storage failed")]
    Store(StoreError),
    /// Durable adapter violated account/space isolation.
    #[error("service storage returned cross-scope material")]
    CrossScopeStoreData,
    /// Durable bytes failed CRDT-native decoding or digest verification.
    #[error("service storage returned corrupt change material")]
    CorruptStoreData,
}

fn decode_u64(bytes: &[u8]) -> Result<u64, CheckpointError> {
    bytes
        .try_into()
        .map(u64::from_be_bytes)
        .map_err(|_| CheckpointError::Malformed)
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, convert::Infallible, sync::Mutex};

    use logical_db_crdt::{Document, Object, Scalar};
    use logical_db_model::{AccountId, ChangeDigest, DocumentId, OperationId, ReplicaId, SpaceId};

    use super::*;

    struct Allow;
    struct Stale;
    struct Grant {
        scope: Scope,
        effect: Effect,
        request: OperationDigest,
        frontier: u64,
    }

    impl ExecutionGrant for Grant {
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
            self.request.into_bytes()
        }
    }

    impl Authority for Allow {
        type Grant = Grant;

        fn authorize(
            &self,
            scope: Scope,
            effect: Effect,
            request: OperationDigest,
            frontier: u64,
            _now: u64,
        ) -> Option<Self::Grant> {
            Some(Grant {
                scope,
                effect,
                request,
                frontier,
            })
        }
    }

    impl Authority for Stale {
        type Grant = Grant;

        fn authorize(
            &self,
            scope: Scope,
            effect: Effect,
            request: OperationDigest,
            frontier: u64,
            _now: u64,
        ) -> Option<Self::Grant> {
            Some(Grant {
                scope,
                effect,
                request,
                frontier: frontier.checked_sub(1).unwrap_or(1),
            })
        }
    }

    #[derive(Default)]
    struct Memory {
        changes: Mutex<Vec<ChangeEnvelope>>,
        operations: Mutex<HashMap<OperationId, (OperationDigest, ChangeDigest)>>,
    }

    #[allow(clippy::unused_async_trait_impl)]
    impl ServiceStore for Memory {
        type Error = Infallible;
        async fn accept(
            &self,
            change: &ChangeEnvelope,
            _grant: &impl ExecutionGrant,
        ) -> Result<AcceptResult, Self::Error> {
            let mut operations = self.operations.lock().unwrap();
            if let Some(existing) = operations.get(&change.operation) {
                return Ok(if *existing == (change.request, change.digest) {
                    AcceptResult::Duplicate
                } else {
                    AcceptResult::Conflict
                });
            }
            operations.insert(change.operation, (change.request, change.digest));
            self.changes.lock().unwrap().push(change.clone());
            Ok(AcceptResult::Accepted)
        }
        async fn frontier(&self, scope: Scope) -> Result<u64, Self::Error> {
            Ok(self
                .changes
                .lock()
                .unwrap()
                .iter()
                .filter(|change| change.scope == scope)
                .count() as u64)
        }
        async fn page(
            &self,
            scope: Scope,
            request: OperationDigest,
            after: u64,
            max_changes: u16,
            max_bytes: u32,
            grant: &impl ExecutionGrant,
        ) -> Result<PageResult, Self::Error> {
            if grant.scope() != scope
                || grant.effect() != Effect::Sync
                || grant.request() != request
            {
                return Ok(PageResult::StaleAuthority);
            }
            let changes = self.changes.lock().unwrap();
            if grant.frontier()
                != changes
                    .iter()
                    .filter(|change| change.scope == scope)
                    .count() as u64
            {
                return Ok(PageResult::StaleAuthority);
            }
            let mut bytes = 0usize;
            let page = changes
                .iter()
                .filter(|change| change.scope == scope)
                .skip(usize::try_from(after).unwrap_or(usize::MAX))
                .take(usize::from(max_changes))
                .take_while(|change| {
                    bytes += change.bytes.len();
                    bytes <= max_bytes as usize
                })
                .cloned()
                .collect::<Vec<_>>();
            let position = after + page.len() as u64;
            let more = changes
                .iter()
                .filter(|change| change.scope == scope)
                .count()
                > usize::try_from(position).unwrap_or(usize::MAX);
            Ok(PageResult::Page(StoredPage {
                changes: page,
                position,
                more,
            }))
        }
    }

    fn change(scope: Scope, replica: ReplicaId, operation: OperationId) -> ChangeEnvelope {
        let mut document = Document::new();
        let ((), mut changes) = document
            .edit(|editor| editor.put(&Object::root(), "x", Scalar::Integer(1)))
            .unwrap();
        let encoded = changes.pop().unwrap();
        ChangeEnvelope {
            scope,
            document: DocumentId::new(),
            replica,
            operation,
            request: OperationDigest::new([4; 32]),
            digest: encoded.digest,
            bytes: encoded.bytes,
        }
    }

    #[test]
    fn exact_retry_is_duplicate_and_forged_checkpoint_fails_closed() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let operation = OperationId::new();
        let service = ProtocolService::new(
            Memory::default(),
            Allow,
            CheckpointCodec::new([7; 32], 100).unwrap(),
        );
        let request = PushRequest {
            protocol: PROTOCOL_VERSION,
            replica,
            changes: vec![change(scope, replica, operation)],
        };
        assert!(matches!(
            futures::executor::block_on(service.push(&request, 10))
                .unwrap()
                .outcomes[0],
            PushOutcome::Accepted { .. }
        ));
        assert!(matches!(
            futures::executor::block_on(service.push(&request, 10))
                .unwrap()
                .outcomes[0],
            PushOutcome::Duplicate { .. }
        ));
        let pull = PullRequest {
            protocol: PROTOCOL_VERSION,
            scope,
            replica,
            request: OperationDigest::new([9; 32]),
            after: None,
            max_changes: 10,
            max_bytes: 1024,
        };
        let ServicePull::Page(page) = futures::executor::block_on(service.pull(&pull, 10)).unwrap()
        else {
            panic!("page expected")
        };
        let mut forged = page.checkpoint.as_wire().to_vec();
        forged[40] ^= 1;
        let forged = PullRequest {
            after: Some(Checkpoint::from_wire(forged).unwrap()),
            ..pull
        };
        assert!(matches!(
            futures::executor::block_on(service.pull(&forged, 11)),
            Err(ServiceError::Checkpoint(CheckpointError::InvalidMac))
        ));
    }

    #[test]
    fn valid_expired_checkpoint_requests_explicit_full_resync() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let service = ProtocolService::new(
            Memory::default(),
            Allow,
            CheckpointCodec::new([8; 32], 5).unwrap(),
        );
        let pull = PullRequest {
            protocol: PROTOCOL_VERSION,
            scope,
            replica,
            request: OperationDigest::new([1; 32]),
            after: None,
            max_changes: 10,
            max_bytes: 1024,
        };
        let ServicePull::Page(page) = futures::executor::block_on(service.pull(&pull, 10)).unwrap()
        else {
            panic!("page expected")
        };
        let expired = PullRequest {
            after: Some(page.checkpoint),
            ..pull
        };
        assert_eq!(
            futures::executor::block_on(service.pull(&expired, 15)).unwrap(),
            ServicePull::CheckpointExpired
        );
    }

    #[test]
    fn pull_store_consumes_exact_grant_and_rejects_stale_frontier() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let service = ProtocolService::new(
            Memory::default(),
            Stale,
            CheckpointCodec::new([8; 32], 50).unwrap(),
        );
        let pull = PullRequest {
            protocol: PROTOCOL_VERSION,
            scope,
            replica,
            request: OperationDigest::new([3; 32]),
            after: None,
            max_changes: 1,
            max_bytes: 1024,
        };
        assert!(matches!(
            futures::executor::block_on(service.pull(&pull, 10)),
            Err(ServiceError::StaleAuthority)
        ));
    }

    #[test]
    fn checkpoint_shape_scope_axes_and_integer_decode_fail_independently() {
        let account = AccountId::new();
        let space = SpaceId::new();
        let scope = Scope::new(account, space);
        let codec = CheckpointCodec::new([13; 32], 100).unwrap();
        let token = codec.issue(scope, 2, 10).unwrap();
        assert_eq!(codec.open(scope, &token, 11), Ok(2));
        assert_eq!(decode_u64(&2_u64.to_be_bytes()), Ok(2));
        assert_eq!(decode_u64(&[0; 7]), Err(CheckpointError::Malformed));

        let mut wrong_magic = token.as_wire().to_vec();
        wrong_magic[..4].copy_from_slice(b"NOPE");
        let wrong_magic = Checkpoint::from_wire(wrong_magic).unwrap();
        assert_eq!(
            codec.open(scope, &wrong_magic, 11),
            Err(CheckpointError::Malformed)
        );
        let short = Checkpoint::from_wire(CHECKPOINT_MAGIC.to_vec()).unwrap();
        assert_eq!(
            codec.open(scope, &short, 11),
            Err(CheckpointError::Malformed)
        );
        assert_eq!(
            codec.open(Scope::new(AccountId::new(), space), &token, 11),
            Err(CheckpointError::WrongScope)
        );
        assert_eq!(
            codec.open(Scope::new(account, SpaceId::new()), &token, 11),
            Err(CheckpointError::WrongScope)
        );
    }

    #[test]
    fn stored_page_shape_cannot_skip_or_lie_about_the_bound_frontier() {
        let page = StoredPage {
            changes: Vec::new(),
            position: 4,
            more: true,
        };
        assert!(valid_stored_page(4, 5, &page));
        let skipped = StoredPage {
            position: 5,
            ..page.clone()
        };
        assert!(!valid_stored_page(4, 5, &skipped));
        let false_end = StoredPage {
            more: false,
            ..page.clone()
        };
        assert!(!valid_stored_page(4, 5, &false_end));
        let beyond = StoredPage {
            position: 6,
            more: false,
            ..page
        };
        assert!(!valid_stored_page(4, 5, &beyond));
    }
}
