//! Authority seam: execution grants and the local scope authority.

use logical_db_model::{ActionDigest, Effect, Scope};

/// Verifier-produced execution proof consumed by a storage transaction.
pub trait ExecutionGrant: Send + Sync {
    /// Scope proven by the invocation.
    fn scope(&self) -> Scope;
    /// Strongest database effect proven by the invocation.
    fn effect(&self) -> Effect;
    /// Canonical action digest bound into the invocation snapshot.
    fn action(&self) -> ActionDigest;
    /// Exact durable scope frontier justified before transaction execution.
    fn frontier(&self) -> u64;
    /// Stable content identity atomically consumed with a new write.
    fn invocation_id(&self) -> [u8; 32];
}

/// Host authority boundary. Production composition supplies an [`ExecutionGrant`]
/// through this trait; no verifier runtime type crosses it.
pub trait Authority {
    /// Concrete opaque proof forwarded into the durable transaction.
    type Grant: ExecutionGrant;

    /// Produces a proof for the exact request and the durable frontier the
    /// service observed, judged at `at_unix_ms`.
    fn authorize(
        &self,
        requested_scope: Scope,
        requested_effect: Effect,
        requested_action: ActionDigest,
        observed_frontier: u64,
        at_unix_ms: u64,
    ) -> Option<Self::Grant>;
}

/// A boxed grant is a grant, so an authority may hand back a proof whose
/// concrete type it keeps private.
impl ExecutionGrant for Box<dyn ExecutionGrant> {
    fn scope(&self) -> Scope {
        self.as_ref().scope()
    }

    fn effect(&self) -> Effect {
        self.as_ref().effect()
    }

    fn action(&self) -> ActionDigest {
        self.as_ref().action()
    }

    fn frontier(&self) -> u64 {
        self.as_ref().frontier()
    }

    fn invocation_id(&self) -> [u8; 32] {
        self.as_ref().invocation_id()
    }
}

/// Host-local execution proof for one authenticated read or scan.
///
/// The invocation identity is the keyed digest of scope, effect, action,
/// frontier, and expiry. Any change to those facts changes the identity and
/// therefore the idempotency key of a write that consumes it.
#[derive(Clone, Debug)]
struct LocalGrant {
    /// Scope the grant authorizes.
    granted_scope: Scope,
    /// Strongest effect the grant authorizes.
    granted_effect: Effect,
    /// Canonical action digest bound into the grant.
    bound_action: ActionDigest,
    /// Durable scope frontier observed when the grant was issued.
    issued_at_frontier: u64,
    /// Stable content identity of the bound facts.
    identity: [u8; 32],
}

impl LocalGrant {
    /// Binds one grant to the exact request facts and observed frontier.
    fn new(
        scope: Scope,
        effect: Effect,
        action: ActionDigest,
        frontier: u64,
        expires_at_unix_ms: u64,
    ) -> Self {
        let mut digest = lgwks_std::hash::Hasher::new();
        digest.update(b"logical-db/local-grant/v1\0");
        digest.update(scope.account().as_uuid().as_bytes());
        digest.update(scope.space().as_uuid().as_bytes());
        digest.update(&[match effect {
            Effect::Read => 1,
            Effect::Scan => 2,
            Effect::Write => 3,
            Effect::Sync => 4,
            // `Effect` is non-exhaustive at the model boundary; an unknown
            // variant must still get a distinct invocation identity.
            _ => 0,
        }]);
        digest.update(&action.into_bytes());
        digest.update(&frontier.to_be_bytes());
        digest.update(&expires_at_unix_ms.to_be_bytes());
        Self {
            granted_scope: scope,
            granted_effect: effect,
            bound_action: action,
            issued_at_frontier: frontier,
            identity: *digest.finalize().as_bytes(),
        }
    }
}

impl ExecutionGrant for LocalGrant {
    fn scope(&self) -> Scope {
        self.granted_scope
    }

    fn effect(&self) -> Effect {
        self.granted_effect
    }

    fn action(&self) -> ActionDigest {
        self.bound_action
    }

    fn frontier(&self) -> u64 {
        self.issued_at_frontier
    }

    fn invocation_id(&self) -> [u8; 32] {
        self.identity
    }
}

/// Host-local authority for non-egress operations inside one authenticated scope.
///
/// Refuses [`Effect::Write`] and [`Effect::Sync`] and every request outside the
/// bound scope or past the exclusive expiry. Hosts that must write or
/// synchronize supply an externally prepared [`Authority`] instead; this type
/// is the one local implementation and never admits those effects.
#[derive(Clone, Copy, Debug)]
pub struct LocalScopeAuthority {
    /// Exact account and application space this authority may authorize.
    scope: Scope,
    /// Exclusive expiry in Unix milliseconds.
    expires_at_unix_ms: u64,
}

impl LocalScopeAuthority {
    /// Limits locally generated grants to one authenticated scope and expiry.
    #[must_use]
    pub const fn new(scope: Scope, expires_at_unix_ms: u64) -> Self {
        Self {
            scope,
            expires_at_unix_ms,
        }
    }
}

impl Authority for LocalScopeAuthority {
    type Grant = Box<dyn ExecutionGrant>;

    fn authorize(
        &self,
        scope: Scope,
        effect: Effect,
        action: ActionDigest,
        frontier: u64,
        now: u64,
    ) -> Option<Box<dyn ExecutionGrant>> {
        let local = matches!(effect, Effect::Read | Effect::Scan);
        let live = now < self.expires_at_unix_ms;
        (scope == self.scope && local && live).then(|| -> Box<dyn ExecutionGrant> {
            Box::new(LocalGrant::new(
                scope,
                effect,
                action,
                frontier,
                self.expires_at_unix_ms,
            ))
        })
    }
}
