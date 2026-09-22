//! Versioned, fail-closed Cloud partition-routing contracts.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20. Source URLs are in `evidence/SOURCES.md`.
//!
//! The current FDB relay remains a scope-wide baseline. This crate deliberately
//! owns no database handle and selects no physical endpoint: a Cloud authority
//! publishes a bounded route map, while callers can only present the epoch they
//! observed and receive a recovery transition if it has gone stale.

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};

use logical_db_model::{DocumentId, OperationDigest, OperationId, Scope};
use thiserror::Error;

mod view;
pub use view::{
    AuthorizedView, ViewCursor, ViewError, ViewGrant, ViewId, ViewLimits, ViewRequest, ViewVersion,
};

/// Maximum UTF-8 bytes in a stable routing or recovery authority identity.
const MAX_ROUTING_ID_BYTES: usize = 128;
/// Maximum transfer receipts accepted for one bounded route-map transition.
const MAX_HANDOFFS_PER_TRANSITION: usize = 4_096;

/// Identifies one logical partition inside a [`Scope`].
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PartitionId(u32);

impl PartitionId {
    /// Creates a non-sentinel partition identity.
    ///
    /// # Errors
    ///
    /// Returns [`RoutingError::ReservedPartition`] when `value` is zero.
    pub const fn new(value: u32) -> Result<Self, RoutingError> {
        if value == 0 {
            return Err(RoutingError::ReservedPartition);
        }
        Ok(Self(value))
    }

    /// Returns the portable partition number.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// Monotonic version of one complete authoritative routing map.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RoutingEpoch(u64);

impl RoutingEpoch {
    /// Creates a non-sentinel epoch.
    ///
    /// # Errors
    /// Returns [`RoutingError::ReservedEpoch`] when `value` is zero.
    pub const fn new(value: u64) -> Result<Self, RoutingError> {
        if value == 0 {
            return Err(RoutingError::ReservedEpoch);
        }
        Ok(Self(value))
    }

    /// Returns the portable epoch number.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Declares the strongest ordering an operation is entitled to assume.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum OrderingDomain {
    /// Operations sharing a document identity have one causal order.
    PerDocument,
    /// Operations in one logical partition have one causal order.
    PerPartition,
    /// Every operation in the scope has one total order.
    ScopeWide,
}

/// Server-owned recovery authority for a partition after a route transition.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct RecoveryAuthority(String);

impl RecoveryAuthority {
    /// Validates an opaque stable recovery authority identity.
    ///
    /// # Errors
    /// Returns [`RoutingError::InvalidIdentity`] for empty or oversized values.
    pub fn new(value: impl Into<String>) -> Result<Self, RoutingError> {
        let value = value.into();
        validate_identity(&value, "recovery authority")?;
        Ok(Self(value))
    }

    /// Returns the opaque authority identity.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// One server-authorized route, with no physical-store or endpoint identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PartitionRoute {
    /// Logical partition being routed.
    partition: PartitionId,
    /// Inclusive lower UUID key boundary owned by this partition.
    start: u128,
    /// Inclusive upper UUID key boundary owned by this partition.
    end: u128,
    /// Region placement label asserted by the routing authority.
    region: String,
    /// Ordering domain preserved by every transition of this partition.
    ordering: OrderingDomain,
    /// Authority that owns recovery after a route transition.
    recovery: RecoveryAuthority,
}

impl PartitionRoute {
    /// Creates one typed route record.
    ///
    /// # Errors
    /// Returns [`RoutingError::InvalidIdentity`] for an invalid region label.
    pub fn new(
        partition: PartitionId,
        start: u128,
        end: u128,
        region: impl Into<String>,
        ordering: OrderingDomain,
        recovery: RecoveryAuthority,
    ) -> Result<Self, RoutingError> {
        if start > end {
            return Err(RoutingError::InvalidRange { start, end });
        }
        let region = region.into();
        validate_identity(&region, "region")?;
        Ok(Self {
            partition,
            start,
            end,
            region,
            ordering,
            recovery,
        })
    }

    /// Returns the route's logical partition.
    #[must_use]
    pub const fn partition(&self) -> PartitionId {
        self.partition
    }

    /// Returns this route's inclusive lower UUID key boundary.
    #[must_use]
    pub const fn start(&self) -> u128 {
        self.start
    }

    /// Returns this route's inclusive upper UUID key boundary.
    #[must_use]
    pub const fn end(&self) -> u128 {
        self.end
    }

    /// Returns the authority-declared placement label.
    #[must_use]
    pub fn region(&self) -> &str {
        &self.region
    }

    /// Returns the route's declared ordering domain.
    #[must_use]
    pub const fn ordering(&self) -> OrderingDomain {
        self.ordering
    }

    /// Returns the authority that owns recovery.
    #[must_use]
    pub const fn recovery(&self) -> &RecoveryAuthority {
        &self.recovery
    }
}

/// Immutable receipt that authorizes one predecessor-to-successor range handoff.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RouteHandoff {
    /// Partition that released a contiguous key range.
    predecessor: PartitionId,
    /// Partition that accepted that range in the next epoch.
    successor: PartitionId,
    /// Durable operation receipt for the state transfer through its watermark.
    receipt: OperationDigest,
}

impl RouteHandoff {
    /// Creates one authority-issued route handoff receipt.
    #[must_use]
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "handoff construction is reserved for in-crate authority adapters; external callers consume RoutingAuthority"
        )
    )]
    pub(crate) const fn new(
        predecessor: PartitionId,
        successor: PartitionId,
        receipt: OperationDigest,
    ) -> Self {
        Self {
            predecessor,
            successor,
            receipt,
        }
    }

    /// Returns the partition that released the range.
    #[must_use]
    pub const fn predecessor(&self) -> PartitionId {
        self.predecessor
    }

    /// Returns the partition that accepted the range.
    #[must_use]
    pub const fn successor(&self) -> PartitionId {
        self.successor
    }

    /// Returns the durable transfer receipt digest.
    #[must_use]
    pub const fn receipt(&self) -> OperationDigest {
        self.receipt
    }
}

/// Narrow server-side authority boundary for resolving Cloud routes.
///
/// Callers receive a resolution or a typed stale-map transition. They never
/// receive authority to construct a map or select a physical partition.
pub trait RoutingAuthority {
    /// Resolves a scope-bound retry against the authority's current map.
    ///
    /// # Errors
    /// Returns a typed refusal for wrong scope, stale epoch, or absent route.
    fn resolve_route(&self, request: RouteRequest) -> Result<RouteResolution, RoutingError>;
}

/// A caller's stable document key and the epoch it used to route an operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RouteRequest {
    /// Tenant/application scope the downstream executor must recheck.
    scope: Scope,
    /// Document whose partition must be chosen by the authoritative map.
    document: DocumentId,
    /// Retry-stable operation identity.
    operation: OperationId,
    /// Canonical digest of the operation authorized for this retry.
    action: OperationDigest,
    /// Complete-map epoch the caller observed before issuing the operation.
    epoch: RoutingEpoch,
}

impl RouteRequest {
    /// Creates a route request without accepting a client-selected partition.
    #[must_use]
    pub const fn new(
        scope: Scope,
        document: DocumentId,
        operation: OperationId,
        action: OperationDigest,
        epoch: RoutingEpoch,
    ) -> Self {
        Self {
            scope,
            document,
            operation,
            action,
            epoch,
        }
    }

    /// Returns the tenant/application scope bound to this request.
    #[must_use]
    pub const fn scope(&self) -> Scope {
        self.scope
    }
}

/// The opaque recovery transition returned for a stale map epoch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RoutingTransition {
    /// Current authoritative epoch the caller must acquire before retrying.
    current_epoch: RoutingEpoch,
    /// Recovery authority that can issue the current map.
    recovery: RecoveryAuthority,
}

impl RoutingTransition {
    /// Returns the current route-map epoch.
    #[must_use]
    pub const fn current_epoch(&self) -> RoutingEpoch {
        self.current_epoch
    }

    /// Returns the authority that owns map recovery.
    #[must_use]
    pub const fn recovery(&self) -> &RecoveryAuthority {
        &self.recovery
    }
}

/// Result of one current-epoch route resolution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RouteResolution {
    /// Scope carried through to the downstream executor.
    scope: Scope,
    /// Retry-stable operation identity carried through resolution.
    operation: OperationId,
    /// Authorized action digest carried through resolution.
    action: OperationDigest,
    /// Epoch whose membership was checked before returning this route.
    epoch: RoutingEpoch,
    /// Route selected by the server-owned deterministic partition map.
    route: PartitionRoute,
}

impl RouteResolution {
    /// Returns the scope the downstream executor must recheck.
    #[must_use]
    pub const fn scope(&self) -> Scope {
        self.scope
    }

    /// Returns the retry-stable operation identity.
    #[must_use]
    pub const fn operation(&self) -> OperationId {
        self.operation
    }

    /// Returns the action digest bound to this resolution.
    #[must_use]
    pub const fn action(&self) -> OperationDigest {
        self.action
    }
    /// Returns the verified route-map epoch.
    #[must_use]
    pub const fn epoch(&self) -> RoutingEpoch {
        self.epoch
    }

    /// Returns the selected logical route.
    #[must_use]
    pub const fn route(&self) -> &PartitionRoute {
        &self.route
    }
}

/// Bounded, immutable routing state for one account/application scope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PartitionMap {
    /// Scope that owns every route in this map.
    scope: Scope,
    /// Current complete-map epoch.
    epoch: RoutingEpoch,
    /// Authority that issues a full-map recovery after a stale epoch.
    recovery: RecoveryAuthority,
    /// Stable route membership in deterministic partition order.
    routes: BTreeMap<PartitionId, PartitionRoute>,
}

impl PartitionMap {
    /// Builds a complete authoritative partition map.
    ///
    /// # Errors
    /// Returns [`RoutingError::EmptyMap`] for no routes and
    /// [`RoutingError::DuplicatePartition`] for duplicate identities.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "construction is reserved for in-crate authority adapters; external callers use RoutingAuthority"
        )
    )]
    pub(crate) fn new(
        scope: Scope,
        epoch: RoutingEpoch,
        recovery: RecoveryAuthority,
        routes: impl IntoIterator<Item = PartitionRoute>,
    ) -> Result<Self, RoutingError> {
        let mut indexed = BTreeMap::new();
        for route in routes {
            let partition = route.partition();
            if indexed.insert(partition, route).is_some() {
                return Err(RoutingError::DuplicatePartition { partition });
            }
        }
        if indexed.is_empty() {
            return Err(RoutingError::EmptyMap);
        }
        validate_complete_ranges(&indexed)?;
        Ok(Self {
            scope,
            epoch,
            recovery,
            routes: indexed,
        })
    }

    /// Returns the scope authorized by this complete map.
    #[must_use]
    pub const fn scope(&self) -> Scope {
        self.scope
    }

    /// Returns the current complete-map epoch.
    #[must_use]
    pub const fn epoch(&self) -> RoutingEpoch {
        self.epoch
    }

    /// Returns the authority that issues complete route-map recovery.
    #[must_use]
    pub const fn recovery(&self) -> &RecoveryAuthority {
        &self.recovery
    }

    /// Resolves a document only if the caller presents the current epoch.
    ///
    /// The map, not the caller, selects a partition. A stale caller gets a
    /// recovery transition and no route, preventing a stale write from being
    /// silently redirected into a newly split, merged, or evacuated partition.
    pub fn resolve(&self, request: RouteRequest) -> Result<RouteResolution, RoutingError> {
        if request.scope != self.scope {
            return Err(RoutingError::RequestScopeMismatch);
        }
        if request.epoch != self.epoch {
            return Err(RoutingError::StaleEpoch {
                transition: self.transition(),
            });
        }
        let key = u128::from_be_bytes(request.document.as_uuid().into_bytes());
        let route = self
            .routes
            .values()
            .find(|route| route.start <= key && key <= route.end)
            .cloned()
            .ok_or(RoutingError::UnroutableDocument)?;
        Ok(RouteResolution {
            scope: request.scope,
            operation: request.operation,
            action: request.action,
            epoch: self.epoch,
            route,
        })
    }

    /// Advances this map to a replacement after validating scope, epoch, and
    /// ordering-domain preservation for retained partitions.
    ///
    /// # Errors
    /// Returns a typed refusal when the replacement is for another scope, does
    /// not advance the epoch, or weakens the ordering entitlement of a retained
    /// partition.
    pub fn replace_with(
        &self,
        replacement: Self,
        handoffs: &[RouteHandoff],
    ) -> Result<Self, RoutingError> {
        if replacement.scope != self.scope {
            return Err(RoutingError::ScopeMismatch);
        }
        if replacement.epoch <= self.epoch {
            return Err(RoutingError::EpochNotAdvanced {
                current: self.epoch,
                replacement: replacement.epoch,
            });
        }
        validate_handoffs(&self.routes, &replacement.routes, handoffs)?;
        Ok(replacement)
    }

    /// Returns every declared partition in deterministic order.
    #[must_use]
    pub fn partitions(&self) -> BTreeSet<PartitionId> {
        self.routes.keys().copied().collect()
    }

    /// Constructs the current fail-closed recovery transition.
    fn transition(&self) -> RoutingTransition {
        RoutingTransition {
            current_epoch: self.epoch,
            recovery: self.recovery.clone(),
        }
    }
}

impl RoutingAuthority for PartitionMap {
    fn resolve_route(&self, request: RouteRequest) -> Result<RouteResolution, RoutingError> {
        self.resolve(request)
    }
}

/// Errors from validating or resolving routing contracts.
#[derive(Debug, Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum RoutingError {
    /// A route's inclusive UUID key boundaries are inverted.
    #[error("partition key range {start}..={end} is inverted")]
    InvalidRange {
        /// Inclusive lower boundary supplied by the authority.
        start: u128,
        /// Inclusive upper boundary supplied by the authority.
        end: u128,
    },
    /// A partition identity used the reserved zero sentinel.
    #[error("partition identity zero is reserved")]
    ReservedPartition,
    /// A routing epoch used the reserved zero sentinel.
    #[error("routing epoch zero is reserved")]
    ReservedEpoch,
    /// A caller-controlled routing identity was empty or oversized.
    #[error("{kind} identity length {actual} exceeds the allowed range")]
    InvalidIdentity {
        /// Identity class that failed validation.
        kind: &'static str,
        /// Observed UTF-8 length.
        actual: usize,
    },
    /// A partition map contained no selectable partitions.
    #[error("partition map must contain at least one route")]
    EmptyMap,
    /// A map repeated one logical partition identity.
    #[error("partition {partition:?} appears more than once")]
    DuplicatePartition {
        /// Repeated partition identity.
        partition: PartitionId,
    },
    /// A replacement map crossed tenant/application scope.
    #[error("replacement partition map belongs to another scope")]
    ScopeMismatch,
    /// A request tried to resolve against another tenant/application scope.
    #[error("route request scope does not match the authoritative map")]
    RequestScopeMismatch,
    /// No authority-declared range covered the request document.
    #[error("no authoritative partition range covers the document")]
    UnroutableDocument,
    /// A map leaves a UUID key gap or contains overlapping route ranges.
    #[error("partition routes must cover the UUID keyspace exactly once")]
    IncompleteRouteMap,
    /// A transition supplied more receipts than its bounded contract permits.
    #[error("route transition has {actual} handoffs, above maximum {maximum}")]
    TooManyHandoffs {
        /// Observed receipt count.
        actual: usize,
        /// Fixed maximum receipt count.
        maximum: usize,
    },
    /// A handoff referred to a predecessor or successor absent from its maps.
    #[error("route handoff refers to an unknown partition")]
    UnknownHandoffPartition,
    /// A handoff did not cover an overlapping predecessor/successor range.
    #[error("route handoff does not cover an overlapping range")]
    InvalidHandoffRange,
    /// A transition omitted the receipt required to transfer one route range.
    #[error("route transition lacks a handoff receipt for an overlapping range")]
    MissingHandoff,
    /// A replacement did not advance the map version.
    #[error("replacement epoch {replacement:?} must exceed current epoch {current:?}")]
    EpochNotAdvanced {
        /// Current authoritative epoch.
        current: RoutingEpoch,
        /// Candidate replacement epoch.
        replacement: RoutingEpoch,
    },
    /// A retained partition tried to change its declared ordering domain.
    #[error("partition {partition:?} changed its declared ordering domain")]
    OrderingChanged {
        /// Partition whose ordering contract weakened or changed.
        partition: PartitionId,
    },
    /// A caller presented an old route-map epoch and must recover first.
    #[error("routing epoch is stale; recover the current map before retrying")]
    StaleEpoch {
        /// Opaque transition that identifies the recovery authority and epoch.
        transition: RoutingTransition,
    },
}

/// Validates a caller-controlled opaque identity.
fn validate_identity(value: &str, kind: &'static str) -> Result<(), RoutingError> {
    if value.is_empty() || value.len() > MAX_ROUTING_ID_BYTES {
        return Err(RoutingError::InvalidIdentity {
            kind,
            actual: value.len(),
        });
    }
    Ok(())
}

/// Proves that a map partitions every UUID key exactly once.
fn validate_complete_ranges(
    routes: &BTreeMap<PartitionId, PartitionRoute>,
) -> Result<(), RoutingError> {
    let mut ordered: Vec<&PartitionRoute> = routes.values().collect();
    ordered.sort_by_key(|route| route.start());
    let Some(first) = ordered.first() else {
        return Err(RoutingError::EmptyMap);
    };
    if first.start() != 0 {
        return Err(RoutingError::IncompleteRouteMap);
    }
    let mut previous_end = first.end();
    for route in ordered.into_iter().skip(1) {
        if previous_end.checked_add(1) != Some(route.start()) {
            return Err(RoutingError::IncompleteRouteMap);
        }
        previous_end = route.end();
    }
    if previous_end != u128::MAX {
        return Err(RoutingError::IncompleteRouteMap);
    }
    Ok(())
}

/// Proves every overlapping old/new range has a typed transfer receipt.
fn validate_handoffs(
    previous: &BTreeMap<PartitionId, PartitionRoute>,
    next: &BTreeMap<PartitionId, PartitionRoute>,
    handoffs: &[RouteHandoff],
) -> Result<(), RoutingError> {
    if handoffs.len() > MAX_HANDOFFS_PER_TRANSITION {
        return Err(RoutingError::TooManyHandoffs {
            actual: handoffs.len(),
            maximum: MAX_HANDOFFS_PER_TRANSITION,
        });
    }
    for handoff in handoffs {
        let old = previous
            .get(&handoff.predecessor())
            .ok_or(RoutingError::UnknownHandoffPartition)?;
        let replacement = next
            .get(&handoff.successor())
            .ok_or(RoutingError::UnknownHandoffPartition)?;
        if !ranges_overlap(old, replacement) {
            return Err(RoutingError::InvalidHandoffRange);
        }
        if old.ordering() != replacement.ordering() {
            return Err(RoutingError::OrderingChanged {
                partition: old.partition(),
            });
        }
    }
    for old in previous.values() {
        for replacement in next.values() {
            if ranges_overlap(old, replacement)
                && !handoffs.iter().any(|handoff| {
                    handoff.predecessor() == old.partition()
                        && handoff.successor() == replacement.partition()
                })
            {
                return Err(RoutingError::MissingHandoff);
            }
        }
    }
    Ok(())
}

/// Returns whether two inclusive UUID ranges share at least one key.
fn ranges_overlap(left: &PartitionRoute, right: &PartitionRoute) -> bool {
    left.start() <= right.end() && right.start() <= left.end()
}

#[cfg(test)]
mod tests {
    use logical_db_model::{AccountId, OperationDigest, OperationId, SpaceId};
    use uuid::Uuid;

    use super::{
        DocumentId, OrderingDomain, PartitionId, PartitionMap, PartitionRoute, RecoveryAuthority,
        RouteHandoff, RouteRequest, RoutingEpoch, RoutingError, Scope,
    };

    /// Builds a deterministic document key for routing tests.
    fn document(value: u128) -> DocumentId {
        DocumentId::from_uuid(Uuid::from_u128(value))
    }

    /// Builds a scope-bound retry request with deterministic operation identity.
    fn request(
        scope: Scope,
        document: DocumentId,
        epoch: u64,
    ) -> Result<RouteRequest, RoutingError> {
        Ok(RouteRequest::new(
            scope,
            document,
            OperationId::from_uuid(Uuid::from_u128(99)),
            OperationDigest::new([7; 32]),
            RoutingEpoch::new(epoch)?,
        ))
    }

    /// Builds the two transfer receipts required by an unchanged two-way map.
    fn identity_handoffs() -> Result<[RouteHandoff; 2], RoutingError> {
        Ok([
            RouteHandoff::new(
                PartitionId::new(1)?,
                PartitionId::new(1)?,
                OperationDigest::new([1; 32]),
            ),
            RouteHandoff::new(
                PartitionId::new(2)?,
                PartitionId::new(2)?,
                OperationDigest::new([2; 32]),
            ),
        ])
    }

    /// Builds a deterministic scope for routing tests.
    fn scope(value: u128) -> Scope {
        let space = match value.checked_add(1) {
            Some(next) => next,
            None => value,
        };
        Scope::new(
            AccountId::from_uuid(Uuid::from_u128(value)),
            SpaceId::from_uuid(Uuid::from_u128(space)),
        )
    }

    /// Builds a complete two-partition map for transition tests.
    fn map(scope: Scope, epoch: u64) -> Result<PartitionMap, RoutingError> {
        let recovery = RecoveryAuthority::new("route-authority")?;
        PartitionMap::new(
            scope,
            RoutingEpoch::new(epoch)?,
            recovery.clone(),
            [
                PartitionRoute::new(
                    PartitionId::new(1)?,
                    0,
                    170141183460469231731687303715884105727,
                    "ca-central",
                    OrderingDomain::PerDocument,
                    recovery.clone(),
                )?,
                PartitionRoute::new(
                    PartitionId::new(2)?,
                    170141183460469231731687303715884105728,
                    u128::MAX,
                    "ca-central",
                    OrderingDomain::PerDocument,
                    recovery,
                )?,
            ],
        )
    }

    #[test]
    fn stale_epoch_fails_closed_with_a_recovery_transition() -> Result<(), RoutingError> {
        let routes = map(scope(10), 2)?;
        let result = routes.resolve(request(scope(10), document(4), 1)?);
        assert!(
            matches!(&result, Err(RoutingError::StaleEpoch { .. })),
            "a stale map must not receive a route"
        );
        let transition = match result {
            Err(RoutingError::StaleEpoch { transition }) => transition,
            _ => return Ok(()),
        };
        assert_eq!(transition.current_epoch().get(), 2);
        assert_eq!(transition.recovery().as_str(), "route-authority");
        Ok(())
    }

    #[test]
    fn retry_with_the_same_epoch_selects_the_same_partition() -> Result<(), RoutingError> {
        let current_scope = scope(20);
        let routes = map(current_scope, 1)?;
        let request = request(current_scope, document(7), 1)?;
        let first = routes.resolve(request)?;
        let retry = routes.resolve(request)?;
        assert_eq!(first, retry, "exact retry must retain its partition");
        assert_eq!(
            first.scope(),
            current_scope,
            "resolution must retain tenant scope"
        );
        assert_eq!(first.action(), OperationDigest::new([7; 32]));
        Ok(())
    }

    #[test]
    fn a_cross_tenant_request_cannot_use_another_scope_map() -> Result<(), RoutingError> {
        let routes = map(scope(21), 1)?;
        assert_eq!(
            routes.resolve(request(scope(22), document(7), 1)?),
            Err(RoutingError::RequestScopeMismatch)
        );
        Ok(())
    }

    #[test]
    fn split_or_rebalance_cannot_weaken_a_retained_ordering_domain() -> Result<(), RoutingError> {
        let current_scope = scope(30);
        let current = map(current_scope, 1)?;
        let recovery = RecoveryAuthority::new("route-authority")?;
        let replacement = PartitionMap::new(
            current_scope,
            RoutingEpoch::new(2)?,
            recovery.clone(),
            [
                PartitionRoute::new(
                    PartitionId::new(1)?,
                    0,
                    170141183460469231731687303715884105727,
                    "eu-central",
                    OrderingDomain::PerPartition,
                    recovery.clone(),
                )?,
                PartitionRoute::new(
                    PartitionId::new(2)?,
                    170141183460469231731687303715884105728,
                    u128::MAX,
                    "ca-central",
                    OrderingDomain::PerDocument,
                    recovery,
                )?,
            ],
        )?;
        assert_eq!(
            current.replace_with(replacement, &identity_handoffs()?),
            Err(RoutingError::OrderingChanged {
                partition: PartitionId::new(1)?
            })
        );
        Ok(())
    }

    #[test]
    fn split_requires_receipts_for_every_predecessor_successor_overlap() -> Result<(), RoutingError>
    {
        let current_scope = scope(35);
        let current = map(current_scope, 1)?;
        let recovery = RecoveryAuthority::new("route-authority")?;
        let replacement = PartitionMap::new(
            current_scope,
            RoutingEpoch::new(2)?,
            recovery.clone(),
            [
                PartitionRoute::new(
                    PartitionId::new(1)?,
                    0,
                    85070591730234615865843651857942052863,
                    "ca-central",
                    OrderingDomain::PerDocument,
                    recovery.clone(),
                )?,
                PartitionRoute::new(
                    PartitionId::new(3)?,
                    85070591730234615865843651857942052864,
                    170141183460469231731687303715884105727,
                    "ca-central",
                    OrderingDomain::PerDocument,
                    recovery.clone(),
                )?,
                PartitionRoute::new(
                    PartitionId::new(2)?,
                    170141183460469231731687303715884105728,
                    u128::MAX,
                    "ca-central",
                    OrderingDomain::PerDocument,
                    recovery,
                )?,
            ],
        )?;
        let handoffs = [
            RouteHandoff::new(
                PartitionId::new(1)?,
                PartitionId::new(1)?,
                OperationDigest::new([1; 32]),
            ),
            RouteHandoff::new(
                PartitionId::new(1)?,
                PartitionId::new(3)?,
                OperationDigest::new([2; 32]),
            ),
            RouteHandoff::new(
                PartitionId::new(2)?,
                PartitionId::new(2)?,
                OperationDigest::new([3; 32]),
            ),
        ];
        assert_eq!(
            current.replace_with(replacement.clone(), &[]),
            Err(RoutingError::MissingHandoff)
        );
        let advanced = current.replace_with(replacement, &handoffs)?;
        let stale = advanced.resolve(request(current_scope, document(7), 1)?);
        assert!(matches!(stale, Err(RoutingError::StaleEpoch { .. })));
        Ok(())
    }

    #[test]
    fn replacement_requires_the_same_scope_and_an_advanced_epoch() -> Result<(), RoutingError> {
        let current_scope = scope(40);
        let current = map(current_scope, 2)?;
        assert_eq!(
            current.replace_with(map(scope(50), 3)?, &[]),
            Err(RoutingError::ScopeMismatch)
        );
        assert_eq!(
            current.replace_with(map(current_scope, 2)?, &[]),
            Err(RoutingError::EpochNotAdvanced {
                current: RoutingEpoch::new(2)?,
                replacement: RoutingEpoch::new(2)?,
            })
        );
        Ok(())
    }
}
