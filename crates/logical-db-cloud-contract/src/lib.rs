//! Versioned, fail-closed Cloud partition-routing contracts.
//!
//! The current FDB relay remains a scope-wide baseline. This crate deliberately
//! owns no database handle and selects no physical endpoint: a Cloud authority
//! publishes a bounded route map, while callers can only present the epoch they
//! observed and receive a recovery transition if it has gone stale.

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};

use logical_db_model::{DocumentId, Scope};
use thiserror::Error;

/// Maximum UTF-8 bytes in a stable routing or recovery authority identity.
const MAX_ROUTING_ID_BYTES: usize = 128;

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
        region: impl Into<String>,
        ordering: OrderingDomain,
        recovery: RecoveryAuthority,
    ) -> Result<Self, RoutingError> {
        let region = region.into();
        validate_identity(&region, "region")?;
        Ok(Self {
            partition,
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

/// A caller's stable document key and the epoch it used to route an operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RouteRequest {
    /// Document whose partition must be chosen by the authoritative map.
    document: DocumentId,
    /// Complete-map epoch the caller observed before issuing the operation.
    epoch: RoutingEpoch,
}

impl RouteRequest {
    /// Creates a route request without accepting a client-selected partition.
    #[must_use]
    pub const fn new(document: DocumentId, epoch: RoutingEpoch) -> Self {
        Self { document, epoch }
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
    /// Epoch whose membership was checked before returning this route.
    epoch: RoutingEpoch,
    /// Route selected by the server-owned deterministic partition map.
    route: PartitionRoute,
}

impl RouteResolution {
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
    pub fn new(
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
        if request.epoch != self.epoch {
            return Err(RoutingError::StaleEpoch {
                transition: self.transition(),
            });
        }
        let index = document_index(request.document, self.routes.len())?;
        let route = self
            .routes
            .values()
            .nth(index)
            .cloned()
            .ok_or(RoutingError::EmptyMap)?;
        Ok(RouteResolution {
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
    pub fn replace_with(&self, replacement: Self) -> Result<Self, RoutingError> {
        if replacement.scope != self.scope {
            return Err(RoutingError::ScopeMismatch);
        }
        if replacement.epoch <= self.epoch {
            return Err(RoutingError::EpochNotAdvanced {
                current: self.epoch,
                replacement: replacement.epoch,
            });
        }
        for (partition, existing) in &self.routes {
            if let Some(next) = replacement.routes.get(partition)
                && next.ordering != existing.ordering
            {
                return Err(RoutingError::OrderingChanged {
                    partition: *partition,
                });
            }
        }
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

/// Errors from validating or resolving routing contracts.
#[derive(Debug, Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum RoutingError {
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

/// Selects a stable route index from a document UUID without client control.
fn document_index(document: DocumentId, route_count: usize) -> Result<usize, RoutingError> {
    let bytes = document.as_uuid().into_bytes();
    let mut prefix = [0; 4];
    prefix.copy_from_slice(&bytes[..4]);
    let value = usize::try_from(u32::from_be_bytes(prefix)).map_err(|_| RoutingError::EmptyMap)?;
    value.checked_rem(route_count).ok_or(RoutingError::EmptyMap)
}

#[cfg(test)]
mod tests {
    use logical_db_model::{AccountId, SpaceId};
    use uuid::Uuid;

    use super::{
        DocumentId, OrderingDomain, PartitionId, PartitionMap, PartitionRoute, RecoveryAuthority,
        RouteRequest, RoutingEpoch, RoutingError, Scope,
    };

    /// Builds a deterministic document key for routing tests.
    fn document(value: u128) -> DocumentId {
        DocumentId::from_uuid(Uuid::from_u128(value))
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
                    "ca-central",
                    OrderingDomain::PerDocument,
                    recovery.clone(),
                )?,
                PartitionRoute::new(
                    PartitionId::new(2)?,
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
        let result = routes.resolve(RouteRequest::new(document(4), RoutingEpoch::new(1)?));
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
        let routes = map(scope(20), 1)?;
        let request = RouteRequest::new(document(7), RoutingEpoch::new(1)?);
        let first = routes.resolve(request)?;
        let retry = routes.resolve(request)?;
        assert_eq!(first, retry, "exact retry must retain its partition");
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
                    "eu-central",
                    OrderingDomain::PerPartition,
                    recovery.clone(),
                )?,
                PartitionRoute::new(
                    PartitionId::new(2)?,
                    "ca-central",
                    OrderingDomain::PerDocument,
                    recovery,
                )?,
            ],
        )?;
        assert_eq!(
            current.replace_with(replacement),
            Err(RoutingError::OrderingChanged {
                partition: PartitionId::new(1)?
            })
        );
        Ok(())
    }

    #[test]
    fn replacement_requires_the_same_scope_and_an_advanced_epoch() -> Result<(), RoutingError> {
        let current_scope = scope(40);
        let current = map(current_scope, 2)?;
        assert_eq!(
            current.replace_with(map(scope(50), 3)?),
            Err(RoutingError::ScopeMismatch)
        );
        assert_eq!(
            current.replace_with(map(current_scope, 2)?),
            Err(RoutingError::EpochNotAdvanced {
                current: RoutingEpoch::new(2)?,
                replacement: RoutingEpoch::new(2)?,
            })
        );
        Ok(())
    }
}
