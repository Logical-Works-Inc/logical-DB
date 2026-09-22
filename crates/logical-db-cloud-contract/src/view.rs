//! Server-authorized, bounded replication-view descriptors.

use std::collections::{BTreeMap, BTreeSet};

use logical_db_model::Scope;
use thiserror::Error;

use crate::{PartitionId, RecoveryAuthority};

/// Maximum partitions that one authorized view can address.
const MAX_VIEW_PARTITIONS: usize = 256;
/// Maximum bytes in one opaque per-partition cursor.
const MAX_VIEW_CURSOR_BYTES: usize = 1024;
/// Maximum UTF-8 bytes in a stable view or grant identity.
const MAX_VIEW_ID_BYTES: usize = 128;
/// Maximum response bytes permitted by one view page.
const MAX_VIEW_PAGE_BYTES: u32 = 8 * 1024 * 1024;

/// Opaque server-issued identity of one replication view.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ViewId(String);

impl ViewId {
    /// Validates a stable view identity.
    ///
    /// # Errors
    /// Returns [`ViewError::InvalidIdentity`] for empty or oversized values.
    pub fn new(value: impl Into<String>) -> Result<Self, ViewError> {
        let value = value.into();
        validate_identity(&value, "view")?;
        Ok(Self(value))
    }

    /// Returns the opaque view identity.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Monotonic schema version of a view definition.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ViewVersion(u64);

impl ViewVersion {
    /// Creates a nonzero view version.
    ///
    /// # Errors
    /// Returns [`ViewError::ReservedVersion`] for the zero sentinel.
    pub const fn new(value: u64) -> Result<Self, ViewError> {
        if value == 0 {
            return Err(ViewError::ReservedVersion);
        }
        Ok(Self(value))
    }

    /// Returns the portable version number.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Server-issued authorization identity for one exact view revision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ViewGrant(String);

impl ViewGrant {
    /// Validates an opaque grant identity.
    ///
    /// # Errors
    /// Returns [`ViewError::InvalidIdentity`] for empty or oversized values.
    pub fn new(value: impl Into<String>) -> Result<Self, ViewError> {
        let value = value.into();
        validate_identity(&value, "view grant")?;
        Ok(Self(value))
    }

    /// Returns the opaque grant identity.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Explicit page and byte bounds for one authorized view.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ViewLimits {
    /// Maximum records the server may return in one page.
    max_records: u16,
    /// Maximum encoded bytes the server may return in one page.
    max_bytes: u32,
}

impl ViewLimits {
    /// Builds fixed nonzero page and byte bounds.
    ///
    /// # Errors
    /// Returns [`ViewError::InvalidLimits`] for a zero or oversized bound.
    pub const fn new(max_records: u16, max_bytes: u32) -> Result<Self, ViewError> {
        if max_records == 0 || max_bytes == 0 || max_bytes > MAX_VIEW_PAGE_BYTES {
            return Err(ViewError::InvalidLimits);
        }
        Ok(Self {
            max_records,
            max_bytes,
        })
    }

    /// Returns the record-count bound.
    #[must_use]
    pub const fn max_records(self) -> u16 {
        self.max_records
    }

    /// Returns the encoded-byte bound.
    #[must_use]
    pub const fn max_bytes(self) -> u32 {
        self.max_bytes
    }
}

/// One opaque cursor in an exact authorized partition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ViewCursor {
    /// Partition whose position this cursor represents.
    partition: PartitionId,
    /// Opaque server-issued position.
    cursor: Vec<u8>,
}

impl ViewCursor {
    /// Builds one bounded opaque cursor.
    ///
    /// # Errors
    /// Returns [`ViewError::InvalidCursor`] for empty or oversized cursors.
    pub fn new(partition: PartitionId, cursor: Vec<u8>) -> Result<Self, ViewError> {
        if cursor.is_empty() || cursor.len() > MAX_VIEW_CURSOR_BYTES {
            return Err(ViewError::InvalidCursor);
        }
        Ok(Self { partition, cursor })
    }

    /// Returns the cursor partition.
    #[must_use]
    pub const fn partition(&self) -> PartitionId {
        self.partition
    }

    /// Returns opaque cursor bytes without granting a mutable handle.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.cursor
    }
}

/// Request to consume a view using its exact authorization and cursor vector.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ViewRequest {
    /// Tenant/application scope requested by the caller.
    scope: Scope,
    /// Requested view identity.
    view: ViewId,
    /// Requested view definition version.
    version: ViewVersion,
    /// Server-issued authorization identity.
    grant: ViewGrant,
    /// Caller-held cursor vector; it must name each authorized partition once.
    cursors: Vec<ViewCursor>,
}

impl ViewRequest {
    /// Creates a bounded request for one exact authorized view.
    ///
    /// # Errors
    /// Returns [`ViewError::TooManyPartitions`] for an unbounded vector.
    pub fn new(
        scope: Scope,
        view: ViewId,
        version: ViewVersion,
        grant: ViewGrant,
        cursors: Vec<ViewCursor>,
    ) -> Result<Self, ViewError> {
        if cursors.len() > MAX_VIEW_PARTITIONS {
            return Err(ViewError::TooManyPartitions);
        }
        Ok(Self {
            scope,
            view,
            version,
            grant,
            cursors,
        })
    }
}

/// Immutable server-authorized materialization boundary for a replication view.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthorizedView {
    /// Scope that owns all material served by the view.
    scope: Scope,
    /// Stable identity of the view definition.
    view: ViewId,
    /// Definition version that scopes grant and cursors.
    version: ViewVersion,
    /// Exact server-issued authorization identity.
    grant: ViewGrant,
    /// Region placement declared by the authority.
    region: String,
    /// Authority that owns revocation and cursor-expiry recovery.
    recovery: RecoveryAuthority,
    /// Complete authorized partition membership.
    partitions: BTreeSet<PartitionId>,
    /// Fixed serving bounds.
    limits: ViewLimits,
    /// Revocation is explicit and fail-closed.
    revoked: bool,
}

impl AuthorizedView {
    /// Constructs a server-owned view descriptor.
    ///
    /// This is crate-private so clients can consume descriptors through a view
    /// authority but cannot manufacture membership or grants locally.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "construction is reserved for in-crate view authority adapters"
        )
    )]
    #[expect(
        clippy::too_many_arguments,
        reason = "the descriptor fields are explicit authority inputs and are not ambient configuration"
    )]
    pub(crate) fn new(
        scope: Scope,
        view: ViewId,
        version: ViewVersion,
        grant: ViewGrant,
        region: impl Into<String>,
        recovery: RecoveryAuthority,
        partitions: impl IntoIterator<Item = PartitionId>,
        limits: ViewLimits,
    ) -> Result<Self, ViewError> {
        let region = region.into();
        validate_identity(&region, "region")?;
        let partitions: BTreeSet<_> = partitions.into_iter().collect();
        if partitions.is_empty() || partitions.len() > MAX_VIEW_PARTITIONS {
            return Err(ViewError::TooManyPartitions);
        }
        Ok(Self {
            scope,
            view,
            version,
            grant,
            region,
            recovery,
            partitions,
            limits,
            revoked: false,
        })
    }

    /// Revokes the view before any cursor is accepted.
    #[must_use]
    pub fn revoke(mut self) -> Self {
        self.revoked = true;
        self
    }

    /// Validates a consumption request without granting a store handle.
    ///
    /// # Errors
    /// Returns a typed refusal for cross-scope, stale, revoked, or incomplete requests.
    pub fn validate(&self, request: &ViewRequest) -> Result<(), ViewError> {
        if self.revoked {
            return Err(ViewError::Revoked {
                recovery: self.recovery.clone(),
            });
        }
        if request.scope != self.scope {
            return Err(ViewError::ScopeMismatch);
        }
        if request.view != self.view
            || request.version != self.version
            || request.grant != self.grant
        {
            return Err(ViewError::AuthorizationMismatch);
        }
        let mut cursors = BTreeMap::new();
        for cursor in &request.cursors {
            if cursors.insert(cursor.partition(), cursor).is_some() {
                return Err(ViewError::DuplicateCursor);
            }
        }
        if cursors.len() != self.partitions.len()
            || !self
                .partitions
                .iter()
                .all(|partition| cursors.contains_key(partition))
        {
            return Err(ViewError::IncompleteCursorVector {
                recovery: self.recovery.clone(),
            });
        }
        Ok(())
    }

    /// Returns the exact placement label the cache and delivery layer must honor.
    #[must_use]
    pub fn region(&self) -> &str {
        &self.region
    }

    /// Returns fixed response bounds.
    #[must_use]
    pub const fn limits(&self) -> ViewLimits {
        self.limits
    }
}

/// Errors from view authorization and cursor-vector validation.
#[derive(Debug, Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum ViewError {
    /// A view identity was empty or exceeded its fixed bound.
    #[error("{kind} identity is empty or too long")]
    InvalidIdentity {
        /// Identity class that failed validation.
        kind: &'static str,
    },
    /// Version zero is reserved.
    #[error("view version zero is reserved")]
    ReservedVersion,
    /// A page bound was zero or too large.
    #[error("view limits are invalid")]
    InvalidLimits,
    /// A cursor was empty or too large.
    #[error("view cursor is invalid")]
    InvalidCursor,
    /// Partition or cursor-vector count exceeded the fixed bound.
    #[error("view has too many partitions or cursors")]
    TooManyPartitions,
    /// A request named another tenant/application scope.
    #[error("view request scope does not match")]
    ScopeMismatch,
    /// Identity, version, or grant did not match the exact authorized view.
    #[error("view authorization does not match")]
    AuthorizationMismatch,
    /// A request repeated one partition cursor.
    #[error("view cursor vector repeats a partition")]
    DuplicateCursor,
    /// A request omitted an authorized partition cursor.
    #[error("view cursor vector is incomplete; recover through {recovery:?}")]
    IncompleteCursorVector {
        /// Recovery authority for a renewed view.
        recovery: RecoveryAuthority,
    },
    /// The server revoked this exact view.
    #[error("view is revoked; recover through {recovery:?}")]
    Revoked {
        /// Recovery authority for a renewed view.
        recovery: RecoveryAuthority,
    },
}

/// Validates an opaque server identity.
fn validate_identity(value: &str, kind: &'static str) -> Result<(), ViewError> {
    if value.is_empty() || value.len() > MAX_VIEW_ID_BYTES {
        return Err(ViewError::InvalidIdentity { kind });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use logical_db_model::{AccountId, SpaceId};
    use uuid::Uuid;

    use super::{
        AuthorizedView, ViewCursor, ViewError, ViewGrant, ViewId, ViewLimits, ViewRequest,
        ViewVersion,
    };
    use crate::{PartitionId, RecoveryAuthority, Scope};

    /// Builds a deterministic scope for view-contract tests.
    fn scope() -> Scope {
        Scope::new(
            AccountId::from_uuid(Uuid::from_u128(1)),
            SpaceId::from_uuid(Uuid::from_u128(2)),
        )
    }

    /// Builds a two-partition authorized view.
    fn view() -> Result<AuthorizedView, ViewError> {
        AuthorizedView::new(
            scope(),
            ViewId::new("team-view")?,
            ViewVersion::new(1)?,
            ViewGrant::new("grant-1")?,
            "ca-central",
            RecoveryAuthority::new("view-authority").map_err(|_| ViewError::InvalidIdentity {
                kind: "recovery authority",
            })?,
            [
                PartitionId::new(1).map_err(|_| ViewError::InvalidLimits)?,
                PartitionId::new(2).map_err(|_| ViewError::InvalidLimits)?,
            ],
            ViewLimits::new(64, 4096)?,
        )
    }

    /// Builds a complete cursor vector for the exact view identity.
    fn request() -> Result<ViewRequest, ViewError> {
        ViewRequest::new(
            scope(),
            ViewId::new("team-view")?,
            ViewVersion::new(1)?,
            ViewGrant::new("grant-1")?,
            vec![
                ViewCursor::new(
                    PartitionId::new(1).map_err(|_| ViewError::InvalidLimits)?,
                    vec![1],
                )?,
                ViewCursor::new(
                    PartitionId::new(2).map_err(|_| ViewError::InvalidLimits)?,
                    vec![2],
                )?,
            ],
        )
    }

    #[test]
    fn exact_authorized_view_requires_the_complete_cursor_vector() -> Result<(), ViewError> {
        view()?.validate(&request()?)?;
        let incomplete = ViewRequest::new(
            scope(),
            ViewId::new("team-view")?,
            ViewVersion::new(1)?,
            ViewGrant::new("grant-1")?,
            vec![ViewCursor::new(
                PartitionId::new(1).map_err(|_| ViewError::InvalidLimits)?,
                vec![1],
            )?],
        )?;
        assert!(matches!(
            view()?.validate(&incomplete),
            Err(ViewError::IncompleteCursorVector { .. })
        ));
        Ok(())
    }

    #[test]
    fn revoked_view_fails_closed_before_cursor_consumption() -> Result<(), ViewError> {
        assert!(matches!(
            view()?.revoke().validate(&request()?),
            Err(ViewError::Revoked { .. })
        ));
        Ok(())
    }
}
