//! The protocol service: push, pull, recovery and retention over a store.

use logical_db_crdt::{CrdtError, EncodedChange};
use logical_db_model::{
    Effect, OperationDigest, OperationId, Scope, SnapshotDocument, SnapshotRecord,
    is_behind_retention,
};
use logical_db_protocol::{
    BaselineDocument, BaselinePage, BaselineRequest, PROTOCOL_VERSION, PageAction, PullPage,
    PullRequest, PushOutcome, PushRequest, PushResponse, RecoveryAction, RejectionCode,
    mark_consumed_action, prune_action, seal_snapshot_action, write_action,
};

use crate::{
    ServiceError,
    authority::{Authority, ExecutionGrant},
    checkpoint::{CheckpointCodec, CheckpointError},
    store::{
        AcceptResult, BaselinePageResult, ConsumedResult, PageResult, PruneResult, RetentionStore,
        SealResult, ServiceStore, StoredPage,
    },
};

/// Pull response distinguishes expiry from invalid or unauthorized input.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ServicePull {
    /// Valid bounded page.
    Page(PullPage),
    /// Valid authenticated checkpoint reached its expiry.
    CheckpointExpired,
    /// The client cannot resume from the log prefix alone.
    ///
    /// The retained prefix below `floor` was collected into a state-bearing
    /// baseline, so a fresh or expired replica must install that artifact
    /// before any suffix pull. A position is not a recoverable snapshot.
    BaselineRequired {
        /// Durable retention floor the client must recover through.
        floor: u64,
    },
}

/// Response to one recovery request: the sealed artifact or its absence.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ServiceBaseline {
    /// A sealed state-bearing artifact covers the retained prefix.
    Artifact(Box<BaselinePage>),
    /// No state-bearing baseline exists, so the durable log still covers every
    /// position and a full pull from the floor is complete.
    Unavailable,
}

/// Stateless service orchestration over a durable storage adapter.
pub struct ProtocolService<Store, Policy> {
    /// Durable storage adapter.
    store: Store,
    /// Host authority boundary used to mint execution grants.
    policy: Policy,
    /// Authenticated opaque checkpoint codec.
    checkpoints: CheckpointCodec,
}

impl<Store, Policy> std::fmt::Debug for ProtocolService<Store, Policy> {
    /// Names the service and its token codec; adapters stay opaque.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProtocolService")
            .field("checkpoints", &self.checkpoints)
            .finish_non_exhaustive()
    }
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
        let mut outcomes = Vec::with_capacity(request.changes().len());
        for change in request.changes() {
            let operation = change.operation();
            let validation =
                EncodedChange::new(change.digest(), change.bytes().to_vec()).validate();
            if let Err(error) = validation {
                outcomes.push(PushOutcome::Rejected {
                    operation,
                    code: if matches!(error, CrdtError::DigestMismatch) {
                        RejectionCode::DigestMismatch
                    } else {
                        RejectionCode::InvalidChange
                    },
                });
                continue;
            }
            let frontier = self
                .store
                .frontier(change.scope())
                .await
                .map_err(ServiceError::Store)?;
            let Some(grant) = self.policy.authorize(
                change.scope(),
                Effect::Write,
                write_action(change),
                frontier,
                now_unix_ms,
            ) else {
                outcomes.push(PushOutcome::Rejected {
                    operation,
                    code: RejectionCode::Forbidden,
                });
                continue;
            };
            let accepted = self
                .store
                .accept(change, &grant)
                .await
                .map_err(ServiceError::Store)?;
            let outcome = push_outcome(operation, accepted);
            outcomes.push(outcome);
        }
        Ok(PushResponse::new(PROTOCOL_VERSION, outcomes))
    }

    /// Reads one authorized page after an authenticated opaque checkpoint.
    ///
    /// A checkpoint behind the durable retention floor, an expired checkpoint
    /// once collection has advanced the floor, and a fresh replica facing a
    /// nonzero floor all take the explicit [`ServicePull::BaselineRequired`]
    /// transition: the collected prefix exists only as a state-bearing
    /// baseline, so no partial log prefix may be applied.
    ///
    /// # Errors
    ///
    /// Returns protocol, authority, checkpoint-authentication, backend, or
    /// cross-scope corruption failures. Expiry and required recovery are typed
    /// non-error responses.
    pub async fn pull(
        &self,
        request: &PullRequest,
        now_unix_ms: u64,
    ) -> Result<ServicePull, ServiceError<Store::Error>> {
        request.validate().map_err(ServiceError::Protocol)?;
        let floor = self
            .store
            .retention_floor(request.scope)
            .await
            .map_err(ServiceError::Store)?;
        let after = match request.after.as_ref() {
            Some(token) => match self.checkpoints.open(request.scope, token, now_unix_ms) {
                Ok(position) if is_behind_retention(position, floor) => {
                    return Ok(ServicePull::BaselineRequired { floor });
                }
                Ok(position) => position,
                Err(CheckpointError::Expired) if floor > 0 => {
                    return Ok(ServicePull::BaselineRequired { floor });
                }
                Err(CheckpointError::Expired) => return Ok(ServicePull::CheckpointExpired),
                Err(error) => return Err(ServiceError::Checkpoint(error)),
            },
            None if floor > 0 => {
                return Ok(ServicePull::BaselineRequired { floor });
            }
            None => floor,
        };
        let frontier = self
            .store
            .frontier(request.scope)
            .await
            .map_err(ServiceError::Store)?;
        self.page_after(request, after, frontier, now_unix_ms).await
    }

    /// Serves one authorized page and issues the replacement checkpoint.
    async fn page_after(
        &self,
        request: &PullRequest,
        after: u64,
        frontier: u64,
        now_unix_ms: u64,
    ) -> Result<ServicePull, ServiceError<Store::Error>> {
        let action = PageAction::new(
            request.scope,
            request.replica,
            request.request,
            after,
            request.max_changes,
            request.max_bytes,
        );
        let Some(grant) = self.policy.authorize(
            request.scope,
            Effect::Sync,
            action.digest(),
            frontier,
            now_unix_ms,
        ) else {
            return Err(ServiceError::Forbidden);
        };
        let result = self
            .store
            .page(action, &grant)
            .await
            .map_err(ServiceError::Store)?;
        let stored = match result {
            PageResult::Page(stored) => stored,
            PageResult::BehindFloor => return Ok(ServicePull::CheckpointExpired),
            PageResult::StaleAuthority => return Err(ServiceError::StaleAuthority),
        };
        if !valid_stored_page(after, frontier, &stored) {
            return Err(ServiceError::CorruptStoreData);
        }
        if stored
            .changes
            .iter()
            .any(|change| change.scope() != request.scope)
        {
            return Err(ServiceError::CrossScopeStoreData);
        }
        for change in &stored.changes {
            EncodedChange::new(change.digest(), change.bytes().to_vec())
                .validate()
                .map_err(ServiceError::CorruptChange)?;
        }
        let checkpoint = self
            .checkpoints
            .issue(request.scope, stored.position, now_unix_ms)
            .map_err(ServiceError::Checkpoint)?;
        let page = PullPage::new(PROTOCOL_VERSION, stored.changes, checkpoint, stored.more);
        page.validate(request.scope)
            .map_err(ServiceError::Protocol)?;
        Ok(ServicePull::Page(page))
    }
}

impl<Store, Policy> ProtocolService<Store, Policy>
where
    Store: RetentionStore,
    Policy: Authority,
{
    /// Durably records a versioned snapshot baseline for full recovery.
    ///
    /// # Errors
    ///
    /// Returns authority or backend failures. A baseline past the frontier is
    /// a [`SealResult::BeyondFrontier`] refusal, never a silent commit.
    pub async fn seal_snapshot(
        &self,
        scope: Scope,
        through: u64,
        request: OperationDigest,
        now_unix_ms: u64,
    ) -> Result<SealResult, ServiceError<Store::Error>> {
        let frontier = self
            .store
            .frontier(scope)
            .await
            .map_err(ServiceError::Store)?;
        if through > frontier {
            return Ok(SealResult::BeyondFrontier { frontier });
        }
        let artifact = self
            .store
            .build_snapshot(scope, through)
            .await
            .map_err(ServiceError::Store)?;
        let Some(grant) = self.policy.authorize(
            scope,
            Effect::Sync,
            seal_snapshot_action(scope, request, artifact.baseline()),
            frontier,
            now_unix_ms,
        ) else {
            return Err(ServiceError::Forbidden);
        };
        self.store
            .seal_snapshot(scope, &artifact, request, &grant)
            .await
            .map_err(ServiceError::Store)
    }

    /// Returns one bounded page of the sealed state-bearing baseline a
    /// recovering client must install.
    ///
    /// The durable descriptor is read once per request and the ordered document
    /// slice is read after `request.after_document`, so a baseline larger than
    /// one page is delivered without materializing the whole artifact. The
    /// client verifies the aggregate descriptor digest across the pages.
    ///
    /// # Errors
    ///
    /// Returns protocol, authority, backend, or cross-scope corruption
    /// failures. A scope with no sealed state-bearing baseline reports
    /// [`ServiceBaseline::Unavailable`] instead of an empty artifact.
    pub async fn recover(
        &self,
        request: &BaselineRequest,
        now_unix_ms: u64,
    ) -> Result<ServiceBaseline, ServiceError<Store::Error>> {
        request.validate().map_err(ServiceError::Protocol)?;
        let frontier = self
            .store
            .frontier(request.scope)
            .await
            .map_err(ServiceError::Store)?;
        let action = RecoveryAction::new(
            request.scope,
            request.replica,
            request.request,
            request.after.clone(),
            request.after_document,
        );
        let digest = action.digest();
        let Some(grant) =
            self.policy
                .authorize(request.scope, Effect::Sync, digest, frontier, now_unix_ms)
        else {
            return Err(ServiceError::Forbidden);
        };
        if grant.scope() != request.scope
            || grant.effect() != Effect::Sync
            || grant.action() != digest
            || grant.frontier() != frontier
        {
            return Err(ServiceError::Forbidden);
        }
        let sealed = self
            .store
            .snapshot_baseline(request.scope)
            .await
            .map_err(ServiceError::Store)?;
        let Some(SnapshotRecord::State(descriptor)) = sealed else {
            return Ok(ServiceBaseline::Unavailable);
        };
        let slice = match self
            .store
            .read_baseline_page(&action, &descriptor, &grant)
            .await
            .map_err(ServiceError::Store)?
        {
            BaselinePageResult::Page(slice) => slice,
            BaselinePageResult::StaleAuthority => return Err(ServiceError::StaleAuthority),
        };
        if slice.more && slice.documents.is_empty() {
            return Err(ServiceError::CorruptStoreData);
        }
        let checkpoint = self
            .checkpoints
            .issue(request.scope, descriptor.position(), now_unix_ms)
            .map_err(ServiceError::Checkpoint)?;
        let next_document = if slice.more {
            slice.documents.last().map(SnapshotDocument::document)
        } else {
            None
        };
        let page = BaselinePage::new(
            PROTOCOL_VERSION,
            descriptor.encode().to_vec(),
            slice
                .documents
                .iter()
                .map(|document| {
                    BaselineDocument::new(
                        document.document(),
                        document.is_tombstone(),
                        document.snapshot().to_vec(),
                    )
                })
                .collect(),
            checkpoint,
            slice.more,
            next_document,
        );
        page.validate_page(request.scope)
            .map_err(ServiceError::Protocol)?;
        Ok(ServiceBaseline::Artifact(Box::new(page)))
    }

    /// Advances the peer-consumption watermark guarding collection.
    ///
    /// # Errors
    ///
    /// Returns authority or backend failures. A watermark past the frontier is
    /// a [`ConsumedResult::BeyondFrontier`] refusal, never a silent advance.
    pub async fn mark_consumed(
        &self,
        scope: Scope,
        through: u64,
        request: OperationDigest,
        now_unix_ms: u64,
    ) -> Result<ConsumedResult, ServiceError<Store::Error>> {
        let frontier = self
            .store
            .frontier(scope)
            .await
            .map_err(ServiceError::Store)?;
        let Some(grant) = self.policy.authorize(
            scope,
            Effect::Sync,
            mark_consumed_action(scope, request, through),
            frontier,
            now_unix_ms,
        ) else {
            return Err(ServiceError::Forbidden);
        };
        self.store
            .mark_consumed(scope, through, request, &grant)
            .await
            .map_err(ServiceError::Store)
    }

    /// Collects the log prefix the peer-aware prune decision allows.
    ///
    /// `min_live` is the minimum position across live unexpired checkpoints;
    /// `None` asserts no live peer holds history the floor does not cover.
    ///
    /// # Errors
    ///
    /// Returns authority or backend failures. A prefix a live peer still needs
    /// is a [`PruneResult::Refused`] refusal, never a partial collection.
    pub async fn prune(
        &self,
        scope: Scope,
        up_to: u64,
        min_live: Option<u64>,
        request: OperationDigest,
        now_unix_ms: u64,
    ) -> Result<PruneResult, ServiceError<Store::Error>> {
        let frontier = self
            .store
            .frontier(scope)
            .await
            .map_err(ServiceError::Store)?;
        let Some(grant) = self.policy.authorize(
            scope,
            Effect::Sync,
            prune_action(scope, request, up_to, min_live),
            frontier,
            now_unix_ms,
        ) else {
            return Err(ServiceError::Forbidden);
        };
        self.store
            .prune(scope, up_to, min_live, request, &grant)
            .await
            .map_err(ServiceError::Store)
    }
}

/// Maps one durable accept outcome onto its stable wire representation.
pub(crate) fn push_outcome(operation: OperationId, accepted: AcceptResult) -> PushOutcome {
    match accepted {
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
    }
}

/// Checks page shape against the justified prefix and durable frontier.
pub(crate) fn valid_stored_page(after: u64, frontier: u64, page: &StoredPage) -> bool {
    let Ok(count) = u64::try_from(page.changes.len()) else {
        return false;
    };
    after.checked_add(count) == Some(page.position)
        && page.position <= frontier
        && page.more == (page.position < frontier)
        && (!page.more || !page.changes.is_empty())
}
