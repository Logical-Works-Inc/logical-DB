//! Shared live-`FoundationDB` recovery fixture for the composition journeys.
//!
//! Included by name (never a test target itself) so the successful recovery
//! journey and the process-kill journey observe one canonical seed, seal, and
//! prune implementation.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32 S33
//! S34 S35 S36 S37 S38 S39. Source URLs are in `evidence/SOURCES.md`.

use std::error::Error;

use logical_db::{OperationDigest, Scope};
use logical_db_model::{ActionDigest, Effect};
use logical_db_protocol::{mark_consumed_action, prune_action, seal_snapshot_action};
use logical_db_server::{AuthenticationError, Authenticator};
use logical_db_service::{
    Authority, ConsumedResult, ExecutionGrant, PruneReceipt, PruneResult, RetentionStore,
    SealResult, ServiceStore,
};
use logical_db_store_fdb::FdbServiceStore;

/// Bearer token the fixture authenticator accepts.
pub const BEARER: &[u8] = b"Bearer recovery-fixture";

/// Admission policy that grants every well-formed action.
pub struct Allow;

/// One fixture execution grant.
pub struct Grant {
    /// Authorized scope.
    pub scope: Scope,
    /// Authorized effect.
    pub effect: Effect,
    /// Authorized action identity.
    pub action: ActionDigest,
    /// Observed frontier.
    pub frontier: u64,
}

impl ExecutionGrant for Grant {
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
        self.action.into_bytes()
    }
}

impl Authority for Allow {
    type Grant = Grant;

    fn authorize(
        &self,
        scope: Scope,
        effect: Effect,
        action: ActionDigest,
        frontier: u64,
        _now_unix_ms: u64,
    ) -> Option<Self::Grant> {
        Some(Grant {
            scope,
            effect,
            action,
            frontier,
        })
    }
}

/// Fixture authenticator accepting exactly one bearer token.
pub struct FixtureAuthenticator;

impl Authenticator for FixtureAuthenticator {
    type Policy = Allow;

    fn authenticate(
        &self,
        authorization: &axum::http::HeaderValue,
        _now_unix_ms: u64,
    ) -> Result<Self::Policy, AuthenticationError> {
        if authorization.as_bytes() == BEARER {
            Ok(Allow)
        } else {
            Err(AuthenticationError::Unauthorized)
        }
    }
}

/// Builds one fixture grant.
pub fn grant(scope: Scope, effect: Effect, action: ActionDigest, frontier: u64) -> Grant {
    Grant {
        scope,
        effect,
        action,
        frontier,
    }
}

/// Seals the covering baseline at `through`, consumes it, and prunes the prefix.
pub async fn seal_consume_and_prune(
    store: &FdbServiceStore,
    scope: Scope,
    through: u64,
) -> Result<(), Box<dyn Error>> {
    let artifact = store.build_snapshot(scope, through).await?;
    let frontier = store.frontier(scope).await?;
    let seal_request = OperationDigest::new([0x52; 32]);
    if store
        .seal_snapshot(
            scope,
            &artifact,
            seal_request,
            &grant(
                scope,
                Effect::Sync,
                seal_snapshot_action(scope, seal_request, &artifact.baseline),
                frontier,
            ),
        )
        .await?
        != (SealResult::Sealed { position: through })
    {
        return Err(std::io::Error::other("baseline did not seal").into());
    }
    let consumed_request = OperationDigest::new([0x53; 32]);
    if store
        .mark_consumed(
            scope,
            through,
            consumed_request,
            &grant(
                scope,
                Effect::Sync,
                mark_consumed_action(scope, consumed_request, through),
                frontier,
            ),
        )
        .await?
        != (ConsumedResult::Advanced {
            consumed_through: through,
        })
    {
        return Err(std::io::Error::other("consumption did not advance").into());
    }
    let prune_request = OperationDigest::new([0x54; 32]);
    if store
        .prune(
            scope,
            through,
            None,
            prune_request,
            &grant(
                scope,
                Effect::Sync,
                prune_action(scope, prune_request, through, None),
                frontier,
            ),
        )
        .await?
        != (PruneResult::Pruned(PruneReceipt::new(through, through)))
    {
        return Err(std::io::Error::other("prune did not collect the log prefix").into());
    }
    Ok(())
}

/// Reads the durable frontier with a bounded retry budget for transient FDB errors.
pub async fn read_frontier(store: &FdbServiceStore, scope: Scope) -> Result<u64, Box<dyn Error>> {
    let mut last_error = None;
    for _retry in 0..128 {
        match store.frontier(scope).await {
            Ok(frontier) => return Ok(frontier),
            Err(error) => last_error = Some(error),
        }
    }
    Err(std::io::Error::other(format!("frontier retry budget exhausted: {last_error:?}")).into())
}

/// Reads the durable retention floor with a bounded retry budget.
pub async fn read_floor(store: &FdbServiceStore, scope: Scope) -> Result<u64, Box<dyn Error>> {
    let mut last_error = None;
    for _retry in 0..128 {
        match store.retention_floor(scope).await {
            Ok(floor) => return Ok(floor),
            Err(error) => last_error = Some(error),
        }
    }
    Err(std::io::Error::other(format!(
        "retention-floor retry budget exhausted: {last_error:?}"
    ))
    .into())
}
