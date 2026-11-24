//! Recovery, sealing, consumption and pruning contracts.

use super::protocol::{
    Deny, envelope_with, forged, pull_after, push_all, recover_at, tombstone_envelope,
};
use super::*;

#[test]
fn a_recovery_grant_for_one_cursor_cannot_authorize_another() -> Result<(), Box<dyn Error>> {
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let replica = ReplicaId::new();
    let intent = OperationDigest::new([92; 32]);
    let cursor = DocumentId::new();
    let authorized = recover_action(scope, replica, intent, None, None);
    let captured = recover_action(scope, replica, intent, None, Some(&cursor));
    assert_ne!(
        authorized, captured,
        "the exclusive cursor must change the recover action digest"
    );

    let service = ProtocolService::new(
        Memory::default(),
        Fixture::Exact {
            scope,
            action: authorized,
            frontier: 0,
        },
        CheckpointCodec::new([6; 32], 100)?,
    );
    let other = BaselineRequest::new(PROTOCOL_VERSION, scope, replica, intent, None, Some(cursor));
    assert!(
        matches!(
            lgwks_std::task::block_on(service.recover(&other, 10)),
            Err(ServiceError::Forbidden)
        ),
        "cursor mismatch must refuse the recovery grant"
    );
    let exact = BaselineRequest::new(PROTOCOL_VERSION, scope, replica, intent, None, None);
    assert!(
        matches!(
            lgwks_std::task::block_on(service.recover(&exact, 10)),
            Ok(ServiceBaseline::Unavailable)
        ),
        "exact grant without a sealed baseline must report unavailable"
    );
    Ok(())
}

#[test]
fn recovery_pages_a_baseline_larger_than_one_page() -> Result<(), Box<dyn Error>> {
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let replica = ReplicaId::new();
    let service = ProtocolService::new(
        Memory::default(),
        Allow,
        CheckpointCodec::new([6; 32], 100)?,
    );
    let total = logical_db_model::MAX_BASELINE_PAGE_DOCUMENTS + 44;
    let changes = (0..=u8::MAX)
        .cycle()
        .take(total)
        .map(|value| envelope_with(scope, replica, OperationId::new(), value))
        .collect::<Result<Vec<_>, _>>()?;
    for chunk in changes.chunks(logical_db_model::MAX_CHANGES_PER_PAGE) {
        push_all(&service, replica, chunk.to_vec())?;
    }
    assert!(
        matches!(
            seal_at(&service, scope, u64::try_from(total)?, 10)?,
            SealResult::Sealed { .. }
        ),
        "multi-page baseline must seal"
    );

    let ServiceBaseline::Artifact(first) = recover_at(&service, scope, replica, None, None, 11)?
    else {
        return Err("sealed baseline must be available".into());
    };
    assert!(first.more(), "first page must report more documents");
    assert_eq!(
        first.documents().len(),
        logical_db_model::MAX_BASELINE_PAGE_DOCUMENTS,
        "first page must be full"
    );
    let cursor = first
        .next_document()
        .ok_or("continued page carries a cursor")?;
    let ServiceBaseline::Artifact(second) =
        recover_at(&service, scope, replica, None, Some(cursor), 12)?
    else {
        return Err("continued baseline page must be available".into());
    };
    assert!(!second.more(), "final page must not report more documents");
    assert_eq!(
        second.documents().len(),
        44,
        "final page must hold the tail"
    );
    assert_eq!(
        second.descriptor(),
        first.descriptor(),
        "pages must share one descriptor"
    );
    assert!(
        second.next_document().is_none(),
        "final page must not carry a cursor"
    );

    let mut accumulator =
        logical_db_protocol::BaselineAccumulator::new(scope, first.validate_page(scope)?);
    assert!(
        !accumulator.push(&first)?,
        "first page must not complete the accumulator"
    );
    assert!(
        accumulator.push(&second)?,
        "second page must complete the accumulator"
    );
    assert_eq!(
        accumulator.finish()?.documents().len(),
        total,
        "accumulator must reassemble every document"
    );
    Ok(())
}

fn seal_at<Store, Policy>(
    service: &ProtocolService<Store, Policy>,
    scope: Scope,
    position: u64,
    now: u64,
) -> Result<SealResult, Box<dyn Error>>
where
    Store: RetentionStore,
    Store::Error: std::fmt::Debug,
    Policy: Authority,
{
    Ok(lgwks_std::task::block_on(service.seal_snapshot(
        scope,
        position,
        OperationDigest::new([61; 32]),
        now,
    ))
    .map_err(|error| format!("seal failed: {error:?}"))?)
}

fn consume_through<Store, Policy>(
    service: &ProtocolService<Store, Policy>,
    scope: Scope,
    through: u64,
    now: u64,
) -> Result<ConsumedResult, Box<dyn Error>>
where
    Store: RetentionStore,
    Store::Error: std::fmt::Debug,
    Policy: Authority,
{
    Ok(lgwks_std::task::block_on(service.mark_consumed(
        scope,
        through,
        OperationDigest::new([62; 32]),
        now,
    ))
    .map_err(|error| format!("mark_consumed failed: {error:?}"))?)
}

fn collect<Store, Policy>(
    service: &ProtocolService<Store, Policy>,
    scope: Scope,
    up_to: u64,
    min_live: Option<u64>,
    now: u64,
) -> Result<PruneResult, Box<dyn Error>>
where
    Store: RetentionStore,
    Store::Error: std::fmt::Debug,
    Policy: Authority,
{
    Ok(lgwks_std::task::block_on(service.prune(
        scope,
        up_to,
        min_live,
        OperationDigest::new([63; 32]),
        now,
    ))
    .map_err(|error| format!("prune failed: {error:?}"))?)
}

#[test]
fn prune_refuses_a_prefix_still_referenced_by_a_live_checkpoint() -> Result<(), Box<dyn Error>> {
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let replica = ReplicaId::new();
    let service = ProtocolService::new(
        Memory::default(),
        Allow,
        CheckpointCodec::new([21; 32], 10_000)?,
    );
    let changes = (30u8..33)
        .map(|request| envelope_with(scope, replica, OperationId::new(), request))
        .collect::<Result<Vec<_>, _>>()?;
    let response = push_all(&service, replica, changes)?;
    assert!(
        response
            .outcomes()
            .iter()
            .all(|outcome| matches!(outcome, PushOutcome::Accepted { .. })),
        "every fixture change must be accepted"
    );
    assert_eq!(
        consume_through(&service, scope, 3, 11)?,
        ConsumedResult::Advanced {
            consumed_through: 3
        },
        "watermark must advance through the prefix"
    );
    assert_eq!(
        collect(&service, scope, 3, Some(1), 12)?,
        PruneResult::Refused(PruneRefusal::LiveCheckpoint { min_live: 1 }),
        "live checkpoint must refuse collection"
    );
    assert_eq!(
        collect(&service, scope, 0, None, 13)?,
        PruneResult::Pruned(PruneReceipt {
            pruned_through: 0,
            floor: 0
        }),
        "request at or below the floor is an idempotent no-op"
    );
    let ServicePull::Page(page) = pull_after(&service, scope, replica, None, 14)? else {
        return Err("page expected".into());
    };
    assert_eq!(
        page.changes().len(),
        3,
        "page must serve every retained change"
    );
    Ok(())
}

#[test]
fn prune_refuses_a_prefix_with_unconsumed_operation_records() -> Result<(), Box<dyn Error>> {
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let replica = ReplicaId::new();
    let service = ProtocolService::new(
        Memory::default(),
        Allow,
        CheckpointCodec::new([22; 32], 10_000)?,
    );
    let changes = (40u8..43)
        .map(|request| envelope_with(scope, replica, OperationId::new(), request))
        .collect::<Result<Vec<_>, _>>()?;
    push_all(&service, replica, changes)?;
    assert_eq!(
        collect(&service, scope, 2, None, 11)?,
        PruneResult::Refused(PruneRefusal::PendingOperations {
            consumed_through: 0
        }),
        "unconsumed operations must refuse collection"
    );
    assert_eq!(
        consume_through(&service, scope, 2, 12)?,
        ConsumedResult::Advanced {
            consumed_through: 2
        },
        "watermark must advance"
    );
    assert_eq!(
        collect(&service, scope, 2, None, 13)?,
        PruneResult::Refused(PruneRefusal::MissingBaseline),
        "missing baseline must refuse collection"
    );
    assert_eq!(
        seal_at(&service, scope, 2, 14)?,
        SealResult::Sealed { position: 2 },
        "baseline must seal at position two"
    );
    assert_eq!(
        collect(&service, scope, 2, None, 15)?,
        PruneResult::Pruned(PruneReceipt {
            pruned_through: 2,
            floor: 2
        }),
        "covered prefix must collect"
    );
    assert_eq!(
        collect(&service, scope, 3, None, 16)?,
        PruneResult::Refused(PruneRefusal::PendingOperations {
            consumed_through: 2
        }),
        "prefix past the watermark must refuse"
    );
    Ok(())
}

#[test]
fn prune_refuses_a_prefix_beyond_the_state_bearing_baseline() -> Result<(), Box<dyn Error>> {
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let replica = ReplicaId::new();
    let service = ProtocolService::new(
        Memory::default(),
        Allow,
        CheckpointCodec::new([29; 32], 10_000)?,
    );
    let changes = (70u8..73)
        .map(|request| envelope_with(scope, replica, OperationId::new(), request))
        .collect::<Result<Vec<_>, _>>()?;
    push_all(&service, replica, changes)?;
    assert_eq!(
        seal_at(&service, scope, 2, 11)?,
        SealResult::Sealed { position: 2 },
        "baseline must seal at position two"
    );
    assert_eq!(
        consume_through(&service, scope, 3, 12)?,
        ConsumedResult::Advanced {
            consumed_through: 3
        },
        "watermark must advance past the baseline"
    );
    assert_eq!(
        collect(&service, scope, 3, None, 13)?,
        PruneResult::Refused(PruneRefusal::BaselineBehind { position: 2 }),
        "prefix past the baseline must refuse"
    );
    assert_eq!(
        collect(&service, scope, 2, None, 14)?,
        PruneResult::Pruned(PruneReceipt {
            pruned_through: 2,
            floor: 2
        }),
        "prefix covered by the baseline must collect"
    );
    Ok(())
}

#[test]
fn behind_floor_checkpoint_requires_baseline_then_suffix_completes() -> Result<(), Box<dyn Error>> {
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let replica = ReplicaId::new();
    let service = ProtocolService::new(
        Memory::default(),
        Allow,
        CheckpointCodec::new([23; 32], 10_000)?,
    );
    let first = envelope_with(scope, replica, OperationId::new(), 50)?;
    push_all(&service, replica, vec![first])?;
    let ServicePull::Page(first_page) = pull_after(&service, scope, replica, None, 11)? else {
        return Err("page expected".into());
    };
    assert_eq!(
        first_page.changes().len(),
        1,
        "first page must hold one change"
    );
    let stale_checkpoint = first_page.checkpoint().clone();
    let second = envelope_with(scope, replica, OperationId::new(), 51)?;
    let tombstone = tombstone_envelope(scope, replica, OperationId::new(), 52)?;
    let tombstone_bytes = tombstone.bytes().to_vec();
    let fourth = envelope_with(scope, replica, OperationId::new(), 53)?;
    push_all(&service, replica, vec![second, tombstone, fourth])?;
    assert_eq!(
        seal_at(&service, scope, 2, 12)?,
        SealResult::Sealed { position: 2 },
        "baseline must seal at position two"
    );
    assert_eq!(
        consume_through(&service, scope, 4, 13)?,
        ConsumedResult::Advanced {
            consumed_through: 4
        },
        "watermark must advance through the prefix"
    );
    assert_eq!(
        collect(&service, scope, 2, None, 14)?,
        PruneResult::Pruned(PruneReceipt {
            pruned_through: 2,
            floor: 2
        }),
        "covered prefix must collect"
    );
    assert_eq!(
        pull_after(&service, scope, replica, None, 15)?,
        ServicePull::BaselineRequired { floor: 2 },
        "fresh replica facing a nonzero floor must require baseline recovery"
    );
    let checkpoint = recover_two_document_baseline(&service, scope, replica, stale_checkpoint)?;
    let ServicePull::Page(recovered) = pull_after(&service, scope, replica, Some(checkpoint), 17)?
    else {
        return Err("recovery suffix page expected".into());
    };
    assert_eq!(
        recovered.changes().len(),
        2,
        "suffix must hold the tail changes"
    );
    assert_eq!(
        recovered.changes().first().map(ChangeEnvelope::bytes),
        Some(tombstone_bytes.as_slice()),
        "suffix must replay the tombstone payload"
    );
    assert!(!recovered.more(), "suffix page must be final");
    Ok(())
}

#[test]
fn chunk_manifest_and_invocation_references_stay_consistent_after_collection()
-> Result<(), Box<dyn Error>> {
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let replica = ReplicaId::new();
    let service = ProtocolService::new(
        Memory::default(),
        Allow,
        CheckpointCodec::new([24; 32], 10_000)?,
    );
    let operations = [OperationId::new(), OperationId::new(), OperationId::new()];
    let changes = operations
        .iter()
        .zip(60u8..63)
        .map(|(operation, request)| envelope_with(scope, replica, *operation, request))
        .collect::<Result<Vec<_>, _>>()?;
    push_all(&service, replica, changes.clone())?;
    assert_eq!(
        seal_at(&service, scope, 2, 11)?,
        SealResult::Sealed { position: 2 },
        "baseline must seal at position two"
    );
    assert_eq!(
        consume_through(&service, scope, 3, 12)?,
        ConsumedResult::Advanced {
            consumed_through: 3
        },
        "watermark must advance"
    );
    assert_eq!(
        collect(&service, scope, 2, None, 13)?,
        PruneResult::Pruned(PruneReceipt {
            pruned_through: 2,
            floor: 2
        }),
        "covered prefix must collect"
    );
    let (Some(first), Some(second)) = (changes.first(), changes.get(1)) else {
        return Err("fixture must hold at least two changes".into());
    };
    let duplicate = push_all(&service, replica, vec![first.clone()])?;
    assert!(
        matches!(
            duplicate.outcomes().first(),
            Some(&PushOutcome::Duplicate { .. })
        ),
        "exact retry after collection must remain a duplicate"
    );
    let conflicting = forged(
        second,
        OperationDigest::new([99; 32]),
        second.digest(),
        second.bytes().to_vec(),
    );
    let conflict = push_all(&service, replica, vec![conflicting])?;
    assert!(
        matches!(
            conflict.outcomes().first(),
            Some(&PushOutcome::Rejected {
                code: RejectionCode::OperationConflict,
                ..
            })
        ),
        "conflicting request bytes must reject as operation conflict"
    );
    let ServiceBaseline::Artifact(baseline) = recover_at(&service, scope, replica, None, None, 13)?
    else {
        return Err("state-bearing baseline expected".into());
    };
    let ServicePull::Page(page) = pull_after(
        &service,
        scope,
        replica,
        Some(baseline.checkpoint().clone()),
        13,
    )?
    else {
        return Err("page expected".into());
    };
    assert_eq!(page.changes().len(), 1, "suffix must hold one change");
    assert_eq!(
        page.changes().first().map(ChangeEnvelope::operation),
        operations.get(2).copied(),
        "suffix must carry the uncollected operation"
    );
    assert!(!page.more(), "suffix page must be final");
    Ok(())
}

#[test]
fn retention_floor_and_baseline_survive_service_restart() -> Result<(), Box<dyn Error>> {
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let replica = ReplicaId::new();
    let store = std::sync::Arc::new(Memory::default());
    let changes = (70u8..73)
        .map(|request| envelope_with(scope, replica, OperationId::new(), request))
        .collect::<Result<Vec<_>, _>>()?;
    let stale_checkpoint = {
        let service = ProtocolService::new(
            Arc::clone(&store),
            Allow,
            CheckpointCodec::new([25; 32], 10_000)?,
        );
        let (head, tail) = changes
            .split_first()
            .ok_or("fixture must hold at least one change")?;
        push_all(&service, replica, vec![head.clone()])?;
        let ServicePull::Page(first) = pull_after(&service, scope, replica, None, 11)? else {
            return Err("page expected".into());
        };
        push_all(&service, replica, tail.to_vec())?;
        assert_eq!(
            seal_at(&service, scope, 2, 12)?,
            SealResult::Sealed { position: 2 },
            "baseline must seal at position two"
        );
        assert_eq!(
            consume_through(&service, scope, 3, 13)?,
            ConsumedResult::Advanced {
                consumed_through: 3
            },
            "watermark must advance"
        );
        assert_eq!(
            collect(&service, scope, 2, None, 14)?,
            PruneResult::Pruned(PruneReceipt {
                pruned_through: 2,
                floor: 2
            }),
            "covered prefix must collect"
        );
        first.checkpoint().clone()
    };
    let service = ProtocolService::new(
        Arc::clone(&store),
        Allow,
        CheckpointCodec::new([25; 32], 10_000)?,
    );
    assert_eq!(
        lgwks_std::task::block_on(store.retention_floor(scope))?,
        2,
        "retention floor must survive restart"
    );
    assert!(
        matches!(
            lgwks_std::task::block_on(store.snapshot_baseline(scope))?,
            Some(SnapshotRecord::State(ref baseline)) if baseline.position() == 2
        ),
        "sealed baseline must survive restart"
    );
    recover_two_document_baseline(&service, scope, replica, stale_checkpoint)?;
    Ok(())
}

/// Proves a checkpoint behind the floor recovers the two-document baseline at
/// position two, and returns the checkpoint that resumes after it.
fn recover_two_document_baseline<Store, Policy>(
    service: &ProtocolService<Store, Policy>,
    scope: Scope,
    replica: ReplicaId,
    stale: Checkpoint,
) -> Result<Checkpoint, Box<dyn Error>>
where
    Store: RetentionStore,
    Store::Error: std::fmt::Debug,
    Policy: Authority,
{
    assert_eq!(
        pull_after(service, scope, replica, Some(stale.clone()), 15)?,
        ServicePull::BaselineRequired { floor: 2 },
        "checkpoint behind the floor must require baseline recovery"
    );
    let ServiceBaseline::Artifact(baseline) =
        recover_at(service, scope, replica, Some(stale), None, 16)?
    else {
        return Err("state-bearing baseline expected".into());
    };
    let artifact = baseline.validate(scope)?;
    assert_eq!(
        artifact.baseline().position(),
        2,
        "recovered baseline must cover position two"
    );
    assert_eq!(
        artifact.documents().len(),
        2,
        "recovered baseline must hold two documents"
    );
    Ok(baseline.checkpoint().clone())
}

#[test]
fn prune_decision_rejects_every_uncollectible_edge() {
    assert_eq!(
        prune_decision(10, 2, 2, None, 5),
        Ok(2),
        "request at floor is a no-op"
    );
    assert_eq!(
        prune_decision(10, 2, 1, None, 5),
        Ok(2),
        "request below floor is a no-op"
    );
    assert_eq!(
        prune_decision(10, 2, 5, None, 5),
        Ok(5),
        "covered prefix advances"
    );
    assert_eq!(
        prune_decision(10, 2, 5, Some(5), 9),
        Ok(5),
        "prefix at the live checkpoint advances"
    );
    assert_eq!(
        prune_decision(3, 0, 4, None, 4),
        Err(PruneRefusal::BeyondFrontier { frontier: 3 }),
        "prefix past the frontier must refuse"
    );
    assert_eq!(
        prune_decision(10, 0, 5, Some(4), 9),
        Err(PruneRefusal::LiveCheckpoint { min_live: 4 }),
        "prefix past a live checkpoint must refuse"
    );
    assert_eq!(
        prune_decision(10, 0, 5, None, 4),
        Err(PruneRefusal::PendingOperations {
            consumed_through: 4
        }),
        "prefix past the consumption watermark must refuse"
    );
}

#[test]
fn retention_writes_require_fresh_authority() -> Result<(), Box<dyn Error>> {
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let stale = ProtocolService::new(
        Memory::default(),
        Stale,
        CheckpointCodec::new([26; 32], 10_000)?,
    );
    assert_eq!(
        lgwks_std::task::block_on(stale.seal_snapshot(
            scope,
            0,
            OperationDigest::new([61; 32]),
            10,
        ))?,
        SealResult::StaleAuthority,
        "seal must require fresh authority"
    );
    assert_eq!(
        lgwks_std::task::block_on(stale.mark_consumed(
            scope,
            0,
            OperationDigest::new([62; 32]),
            10,
        ))?,
        ConsumedResult::StaleAuthority,
        "mark_consumed must require fresh authority"
    );
    assert_eq!(
        lgwks_std::task::block_on(
            stale.prune(scope, 0, None, OperationDigest::new([63; 32]), 10,)
        )?,
        PruneResult::StaleAuthority,
        "prune must require fresh authority"
    );
    let denied = ProtocolService::new(
        Memory::default(),
        Deny,
        CheckpointCodec::new([27; 32], 10_000)?,
    );
    assert!(
        matches!(
            lgwks_std::task::block_on(denied.prune(
                scope,
                0,
                None,
                OperationDigest::new([63; 32]),
                10,
            )),
            Err(ServiceError::Forbidden)
        ),
        "denied authority must fail closed"
    );
    Ok(())
}
