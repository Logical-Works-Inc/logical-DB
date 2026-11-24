//! Push, pull, checkpoint and page-shape contracts.

use super::*;

#[test]
fn recovered_and_refused_admissions_keep_their_wire_meaning() {
    let operation = OperationId::new();
    let recovered = AcceptResult::RecoveredUnknownCommit;
    let stale = AcceptResult::StaleAuthority;
    let consumed = AcceptResult::InvocationConsumed;
    assert_eq!(
        push_outcome(operation, recovered),
        PushOutcome::Duplicate { operation },
        "recovered unknown commits must map to a duplicate wire outcome"
    );
    assert_eq!(
        push_outcome(operation, stale),
        PushOutcome::Rejected {
            operation,
            code: RejectionCode::StaleAuthority
        },
        "stale authority must map to the stale-authority rejection code"
    );
    assert_eq!(
        push_outcome(operation, consumed),
        PushOutcome::Rejected {
            operation,
            code: RejectionCode::InvocationConsumed
        },
        "consumed invocation must map to the invocation-consumed rejection code"
    );
}

#[test]
fn exact_retry_is_duplicate_and_forged_checkpoint_fails_closed() -> Result<(), Box<dyn Error>> {
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let replica = ReplicaId::new();
    let operation = OperationId::new();
    let service = ProtocolService::new(
        Memory::default(),
        Allow,
        CheckpointCodec::new([7; 32], 100)?,
    );
    let request = PushRequest::new(
        PROTOCOL_VERSION,
        replica,
        vec![change(scope, replica, operation)?],
    );
    assert!(
        matches!(
            lgwks_std::task::block_on(service.push(&request, 10))?
                .outcomes()
                .first(),
            Some(&PushOutcome::Accepted { .. })
        ),
        "first admission must be accepted"
    );
    assert!(
        matches!(
            lgwks_std::task::block_on(service.push(&request, 10))?
                .outcomes()
                .first(),
            Some(&PushOutcome::Duplicate { .. })
        ),
        "exact retry must be a duplicate"
    );
    let pull = PullRequest::new(
        PROTOCOL_VERSION,
        scope,
        replica,
        OperationDigest::new([9; 32]),
        None,
        10,
        1024,
    );
    let ServicePull::Page(page) = lgwks_std::task::block_on(service.pull(&pull, 10))? else {
        return Err("page expected".into());
    };
    let mut forged = page.checkpoint().as_wire().to_vec();
    *forged
        .get_mut(40)
        .ok_or("checkpoint wire form is shorter than its MAC offset")? ^= 1;
    let forged = PullRequest::new(
        pull.protocol,
        pull.scope,
        pull.replica,
        pull.request,
        Some(Checkpoint::from_wire(forged)?),
        pull.max_changes,
        pull.max_bytes,
    );
    assert!(
        matches!(
            lgwks_std::task::block_on(service.pull(&forged, 11)),
            Err(ServiceError::Checkpoint(CheckpointError::InvalidMac))
        ),
        "forged checkpoint must fail the MAC check"
    );
    Ok(())
}

#[test]
fn valid_expired_checkpoint_requests_explicit_full_resync() -> Result<(), Box<dyn Error>> {
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let replica = ReplicaId::new();
    let service = ProtocolService::new(Memory::default(), Allow, CheckpointCodec::new([8; 32], 5)?);
    let pull = PullRequest::new(
        PROTOCOL_VERSION,
        scope,
        replica,
        OperationDigest::new([1; 32]),
        None,
        10,
        1024,
    );
    let ServicePull::Page(page) = lgwks_std::task::block_on(service.pull(&pull, 10))? else {
        return Err("page expected".into());
    };
    let expired = PullRequest::new(
        pull.protocol,
        pull.scope,
        pull.replica,
        pull.request,
        Some(page.checkpoint().clone()),
        pull.max_changes,
        pull.max_bytes,
    );
    assert_eq!(
        lgwks_std::task::block_on(service.pull(&expired, 15))?,
        ServicePull::CheckpointExpired,
        "valid expired checkpoint must request explicit full resync"
    );
    Ok(())
}

#[test]
fn push_distinguishes_malformed_change_from_digest_mismatch() -> Result<(), Box<dyn Error>> {
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let replica = ReplicaId::new();
    let service = ProtocolService::new(
        Memory::default(),
        Allow,
        CheckpointCodec::new([5; 32], 100)?,
    );
    let valid = change(scope, replica, OperationId::new())?;
    let malformed = forged(&valid, valid.request(), valid.digest(), vec![1]);
    let valid = change(scope, replica, OperationId::new())?;
    let mismatched = forged(
        &valid,
        valid.request(),
        ChangeDigest::new([0; 32]),
        valid.bytes().to_vec(),
    );
    let response = lgwks_std::task::block_on(service.push(
        &PushRequest::new(PROTOCOL_VERSION, replica, vec![malformed, mismatched]),
        10,
    ))?;
    assert!(
        matches!(
            response.outcomes().first(),
            Some(&PushOutcome::Rejected {
                code: RejectionCode::InvalidChange,
                ..
            })
        ),
        "malformed change bytes must reject as invalid change"
    );
    assert!(
        matches!(
            response.outcomes().get(1),
            Some(&PushOutcome::Rejected {
                code: RejectionCode::DigestMismatch,
                ..
            })
        ),
        "digest mismatch must reject as digest mismatch"
    );
    Ok(())
}

#[test]
fn pull_store_consumes_exact_grant_and_rejects_stale_frontier() -> Result<(), Box<dyn Error>> {
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let replica = ReplicaId::new();
    let service =
        ProtocolService::new(Memory::default(), Stale, CheckpointCodec::new([8; 32], 50)?);
    let pull = PullRequest::new(
        PROTOCOL_VERSION,
        scope,
        replica,
        OperationDigest::new([3; 32]),
        None,
        1,
        1024,
    );
    assert!(
        matches!(
            lgwks_std::task::block_on(service.pull(&pull, 10)),
            Err(ServiceError::StaleAuthority)
        ),
        "stale frontier grant must be rejected"
    );
    Ok(())
}

#[test]
fn checkpoint_shape_scope_axes_and_integer_decode_fail_independently() -> Result<(), Box<dyn Error>>
{
    let account = AccountId::new();
    let space = SpaceId::new();
    let scope = Scope::new(account, space);
    let codec = CheckpointCodec::new([13; 32], 100)?;
    let token = codec.issue(scope, 2, 10)?;
    assert_eq!(
        codec.open(scope, &token, 11),
        Ok(2),
        "well-formed checkpoint must decode to its position"
    );
    let mut wrong_magic = token.as_wire().to_vec();
    wrong_magic
        .get_mut(..4)
        .ok_or("checkpoint wire form is shorter than its magic")?
        .copy_from_slice(b"NOPE");
    let wrong_magic = Checkpoint::from_wire(wrong_magic)?;
    assert_eq!(
        codec.open(scope, &wrong_magic, 11),
        Err(CheckpointError::Malformed),
        "wrong magic must be malformed"
    );
    let short = Checkpoint::from_wire(CHECKPOINT_MAGIC.to_vec())?;
    assert_eq!(
        codec.open(scope, &short, 11),
        Err(CheckpointError::Malformed),
        "short token must be malformed"
    );
    assert_eq!(
        codec.open(Scope::new(AccountId::new(), space), &token, 11),
        Err(CheckpointError::WrongScope),
        "account axis must be authenticated"
    );
    assert_eq!(
        codec.open(Scope::new(account, SpaceId::new()), &token, 11),
        Err(CheckpointError::WrongScope),
        "space axis must be authenticated"
    );
    Ok(())
}

#[test]
fn stored_page_shape_cannot_skip_or_lie_about_the_bound_frontier() -> Result<(), Box<dyn Error>> {
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let replica = ReplicaId::new();
    let page = StoredPage {
        changes: vec![change(scope, replica, OperationId::new())?],
        position: 5,
        more: true,
    };
    assert!(
        valid_stored_page(4, 6, &page),
        "valid page must pass shape check"
    );
    let stalled = StoredPage {
        changes: Vec::new(),
        position: 4,
        more: true,
    };
    assert!(
        !valid_stored_page(4, 6, &stalled),
        "empty page claiming more must fail shape check"
    );
    let skipped = StoredPage {
        position: 6,
        ..page.clone()
    };
    assert!(
        !valid_stored_page(4, 6, &skipped),
        "skipped position must fail shape check"
    );
    let false_end = StoredPage {
        more: false,
        ..page.clone()
    };
    assert!(
        !valid_stored_page(4, 6, &false_end),
        "false end claim must fail shape check"
    );
    let beyond = StoredPage {
        position: 6,
        more: false,
        ..page
    };
    assert!(
        !valid_stored_page(4, 6, &beyond),
        "position past frontier must fail shape check"
    );
    Ok(())
}

/// Authority fixture that always refuses authorization.
pub(super) struct Deny;

impl Authority for Deny {
    type Grant = Grant;

    fn authorize(
        &self,
        _scope: Scope,
        _effect: Effect,
        _action: ActionDigest,
        _frontier: u64,
        _now: u64,
    ) -> Option<Self::Grant> {
        None
    }
}

/// Rebuilds one envelope with the given request, digest and payload; every
/// other identity is the base envelope's.
pub(super) fn forged(
    base: &ChangeEnvelope,
    request: OperationDigest,
    digest: ChangeDigest,
    bytes: Vec<u8>,
) -> ChangeEnvelope {
    ChangeEnvelope::new(
        base.scope(),
        base.document(),
        base.replica(),
        base.operation(),
        request,
        digest,
        bytes,
    )
}

/// Builds a fixture envelope with an explicit request digest byte.
pub(super) fn envelope_with(
    scope: Scope,
    replica: ReplicaId,
    operation: OperationId,
    request_byte: u8,
) -> Result<ChangeEnvelope, Box<dyn Error>> {
    let base = change(scope, replica, operation)?;
    Ok(forged(
        &base,
        OperationDigest::new([request_byte; 32]),
        base.digest(),
        base.bytes().to_vec(),
    ))
}

/// Builds a fixture envelope whose payload is a document tombstone.
pub(super) fn tombstone_envelope(
    scope: Scope,
    replica: ReplicaId,
    operation: OperationId,
    request_byte: u8,
) -> Result<ChangeEnvelope, Box<dyn Error>> {
    let mut document = Document::new();
    document.edit(|editor| editor.put(&Object::root(), "k", Scalar::Integer(1)))?;
    let mut staged = document.delete()?;
    let encoded = staged.pop().ok_or("delete must stage exactly one change")?;
    assert!(staged.is_empty(), "delete must stage exactly one change");
    Ok(ChangeEnvelope::new(
        scope,
        DocumentId::new(),
        replica,
        operation,
        OperationDigest::new([request_byte; 32]),
        encoded.digest(),
        encoded.into_bytes(),
    ))
}

/// Pushes one bounded page of fixture changes through the service.
pub(super) fn push_all<Store, Policy>(
    service: &ProtocolService<Store, Policy>,
    replica: ReplicaId,
    changes: Vec<ChangeEnvelope>,
) -> Result<PushResponse, Box<dyn Error>>
where
    Store: ServiceStore,
    Store::Error: std::fmt::Debug,
    Policy: Authority,
{
    let request = PushRequest::new(PROTOCOL_VERSION, replica, changes);
    Ok(lgwks_std::task::block_on(service.push(&request, 10))
        .map_err(|error| format!("push failed: {error:?}"))?)
}

/// Pulls one bounded page after an optional checkpoint.
pub(super) fn pull_after<Store, Policy>(
    service: &ProtocolService<Store, Policy>,
    scope: Scope,
    replica: ReplicaId,
    after: Option<Checkpoint>,
    now: u64,
) -> Result<ServicePull, Box<dyn Error>>
where
    Store: ServiceStore,
    Store::Error: std::fmt::Debug,
    Policy: Authority,
{
    let request = PullRequest::new(
        PROTOCOL_VERSION,
        scope,
        replica,
        OperationDigest::new([91; 32]),
        after,
        10,
        1_048_576,
    );
    Ok(lgwks_std::task::block_on(service.pull(&request, now))
        .map_err(|error| format!("pull failed: {error:?}"))?)
}

/// Requests one baseline page after a checkpoint and an exclusive document cursor.
pub(super) fn recover_at<Store, Policy>(
    service: &ProtocolService<Store, Policy>,
    scope: Scope,
    replica: ReplicaId,
    after: Option<Checkpoint>,
    after_document: Option<DocumentId>,
    now: u64,
) -> Result<ServiceBaseline, Box<dyn Error>>
where
    Store: RetentionStore,
    Store::Error: std::fmt::Debug,
    Policy: Authority,
{
    let request = BaselineRequest::new(
        PROTOCOL_VERSION,
        scope,
        replica,
        OperationDigest::new([92; 32]),
        after,
        after_document,
    );
    Ok(lgwks_std::task::block_on(service.recover(&request, now))
        .map_err(|error| format!("recover failed: {error:?}"))?)
}
