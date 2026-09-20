//! Explicit backend capability profiles for the two declared deployment
//! classes.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.
//!
//! The recorded product decision (Director ruling, 2026-09-16, issue #8)
//! splits the backend: redb serves local device state and `FoundationDB` serves
//! distributed transaction requirements. The upper key, namespace, and event
//! contracts must stay compatible, but the two backends do not promise the
//! same guarantees. A consumer therefore publishes which class it is bound to
//! and may rely only on the *common subset* of guarantees unless it explicitly
//! requests the stronger profile.
//!
//! This module makes that rule mechanical instead of documentary:
//!
//! * [`BackendClass`] is the two declared classes, `EmbeddedLocal` and
//!   `DistributedTransactional`.
//! * [`Capability`] is one guarantee an application may rely on. It is a sum
//!   type, so an unknown guarantee name cannot be constructed or encoded.
//! * [`CapabilityProfile`] binds a class to its fixed guarantee set. The set is
//!   not caller-supplied: [`CapabilityProfile::embedded_local`] and
//!   [`CapabilityProfile::distributed_transactional`] are the only
//!   constructors, and each carries the guarantees that backend actually
//!   provides.
//!
//! Because the guarantee set is fixed per class, a caller cannot claim a
//! stronger profile than its backend provides. [`CapabilityProfile::require`]
//! fails closed and names every unmet guarantee, and
//! [`common_guarantees`] resolves the shared subset for a pair of profiles.

use lgwks_std::json::{Deserialize, Serialize};

use super::ModelError;

/// Layout version of the encoded capability-profile identity.
///
/// A profile identity may cross a replication or negotiation boundary, so its
/// wire form is versioned like every other durable Logical-DB identity.
pub const CAPABILITY_PROFILE_VERSION: u16 = 1;

/// Encoded byte length of one capability-profile identity.
///
/// Fixed so decoding never allocates and never guesses at a length prefix.
pub const CAPABILITY_PROFILE_BYTES: usize = 3;

/// One backend guarantee an application may rely on.
///
/// The variants deliberately mirror the recorded ruling: the first group is
/// guaranteed by `EmbeddedLocal`, the second group is guaranteed only by
/// `DistributedTransactional`. A guarantee that neither backend provides is
/// not a variant here, which keeps an unsupported claim unrepresentable.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(crate = "lgwks_std::json::serde")]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Capability {
    /// A committed local edit survives process loss without destructor
    /// execution.
    DurableLocalCommit,
    /// Concurrent replicas converge on one deterministic document state.
    CausalConvergence,
    /// Synchronization proceeds in bounded, resumable, idempotent pages.
    BoundedSync,
    /// Schema and projection identity changes are bounded and resumable.
    SchemaMigration,
    /// Local state exports to a versioned payload and restores from one.
    ExportRestore,
    /// Transactions are serializable across independent service processes.
    DistributedSerializableTransactions,
    /// A replicated cluster keeps serving after a process or node loss.
    ReplicatedAvailability,
    /// Ordered watches and versionstamps observe committed cluster history.
    WatchVersionstamps,
    /// A cluster recovers its committed state after coordinator loss.
    ClusterRecovery,
}

impl Capability {
    /// Stable lowercase fact name for logs, receipts, and diagnostics.
    ///
    /// This is a wire fact, so it is spelled once here rather than derived
    /// from `Debug`.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::DurableLocalCommit => "durable_local_commit",
            Self::CausalConvergence => "causal_convergence",
            Self::BoundedSync => "bounded_sync",
            Self::SchemaMigration => "schema_migration",
            Self::ExportRestore => "export_restore",
            Self::DistributedSerializableTransactions => "distributed_serializable_transactions",
            Self::ReplicatedAvailability => "replicated_availability",
            Self::WatchVersionstamps => "watch_versionstamps",
            Self::ClusterRecovery => "cluster_recovery",
        }
    }

    /// Reports whether the guarantee is provided by `EmbeddedLocal`.
    #[must_use]
    pub const fn is_embedded_local(self) -> bool {
        matches!(
            self,
            Self::DurableLocalCommit
                | Self::CausalConvergence
                | Self::BoundedSync
                | Self::SchemaMigration
                | Self::ExportRestore
        )
    }

    /// Reports whether the guarantee is provided by
    /// `DistributedTransactional`.
    #[must_use]
    pub const fn is_distributed_transactional(self) -> bool {
        matches!(
            self,
            Self::DistributedSerializableTransactions
                | Self::ReplicatedAvailability
                | Self::WatchVersionstamps
                | Self::ClusterRecovery
        )
    }
}

/// The two declared backend classes.
///
/// The classes are not interchangeable: they publish different guarantees over
/// compatible key, namespace, and event contracts.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(crate = "lgwks_std::json::serde")]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum BackendClass {
    /// A local redb replica owned by one application installation.
    EmbeddedLocal,
    /// A FoundationDB-backed service holding distributed transaction
    /// authority.
    DistributedTransactional,
}

impl BackendClass {
    /// Stable class name for a profile identity and diagnostics.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::EmbeddedLocal => "EmbeddedLocal",
            Self::DistributedTransactional => "DistributedTransactional",
        }
    }

    /// Encoded class discriminant inside a profile identity.
    #[must_use]
    const fn tag(self) -> u8 {
        match self {
            Self::EmbeddedLocal => 0,
            Self::DistributedTransactional => 1,
        }
    }

    /// Decodes a class discriminant, refusing an unknown class.
    fn from_tag(tag: u8) -> Result<Self, ModelError> {
        match tag {
            0 => Ok(Self::EmbeddedLocal),
            1 => Ok(Self::DistributedTransactional),
            other => Err(ModelError::UnsupportedBackendClass { actual: other }),
        }
    }

    /// The guarantees this class provides.
    ///
    /// The set is exhaustive over [`Capability`]: every variant appears in
    /// exactly one of the two arms, so a newly added guarantee forces this
    /// match to be revisited rather than silently defaulting to unsupported.
    #[must_use]
    pub const fn guarantees(self) -> &'static [Capability] {
        match self {
            Self::EmbeddedLocal => &[
                Capability::DurableLocalCommit,
                Capability::CausalConvergence,
                Capability::BoundedSync,
                Capability::SchemaMigration,
                Capability::ExportRestore,
            ],
            Self::DistributedTransactional => &[
                Capability::DistributedSerializableTransactions,
                Capability::ReplicatedAvailability,
                Capability::WatchVersionstamps,
                Capability::ClusterRecovery,
            ],
        }
    }
}

/// A declared backend class together with the guarantees it provides.
///
/// A value can only be produced by the class constructors, so a profile can
/// never advertise a guarantee its class does not provide.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CapabilityProfile {
    /// Declared backend class this profile publishes guarantees for.
    class: BackendClass,
}

impl CapabilityProfile {
    /// The profile of the embedded redb replica.
    #[must_use]
    pub const fn embedded_local() -> Self {
        Self {
            class: BackendClass::EmbeddedLocal,
        }
    }

    /// The profile of the `FoundationDB` distributed transaction authority.
    #[must_use]
    pub const fn distributed_transactional() -> Self {
        Self {
            class: BackendClass::DistributedTransactional,
        }
    }

    /// Returns the profile of a declared class.
    #[must_use]
    pub const fn of(class: BackendClass) -> Self {
        Self { class }
    }

    /// Returns the declared class.
    #[must_use]
    pub const fn class(self) -> BackendClass {
        self.class
    }

    /// Returns the guarantees this profile provides.
    #[must_use]
    pub const fn guarantees(self) -> &'static [Capability] {
        self.class.guarantees()
    }

    /// Reports whether this profile provides one guarantee.
    #[must_use]
    pub const fn supports(self, guarantee: Capability) -> bool {
        match self.class {
            BackendClass::EmbeddedLocal => guarantee.is_embedded_local(),
            BackendClass::DistributedTransactional => guarantee.is_distributed_transactional(),
        }
    }

    /// Requires every listed guarantee, failing closed on the unmet ones.
    ///
    /// This is the mechanical form of "consumers may only rely on the common
    /// subset unless they explicitly request the stronger profile": the
    /// caller states the guarantees it needs, and a profile that does not
    /// provide one returns every missing guarantee instead of proceeding.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError::UnsupportedGuarantees`] listing the guarantees
    /// this profile does not provide, in caller-supplied order.
    pub fn require(self, required: &[Capability]) -> Result<(), ModelError> {
        let mut missing = Vec::new();
        for guarantee in required {
            if !self.supports(*guarantee) {
                missing.push(*guarantee);
            }
        }
        if missing.is_empty() {
            Ok(())
        } else {
            Err(ModelError::UnsupportedGuarantees {
                class: self.class,
                missing,
            })
        }
    }

    /// Encodes the fixed-length, versioned profile identity.
    #[must_use]
    pub fn encode(self) -> [u8; CAPABILITY_PROFILE_BYTES] {
        let tag = self.class.tag();
        let version = CAPABILITY_PROFILE_VERSION.to_be_bytes();
        [version[0], version[1], tag]
    }

    /// Decodes a profile identity, refusing unknown versions and classes.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError::InvalidCapabilityProfileEncoding`] for a length
    /// that is not exactly [`CAPABILITY_PROFILE_BYTES`],
    /// [`ModelError::UnsupportedCapabilityProfileVersion`] for a foreign
    /// version, and [`ModelError::UnsupportedBackendClass`] for an unknown
    /// class discriminant.
    pub fn decode(bytes: &[u8]) -> Result<Self, ModelError> {
        if bytes.len() != CAPABILITY_PROFILE_BYTES {
            return Err(ModelError::InvalidCapabilityProfileEncoding {
                actual: bytes.len(),
            });
        }
        let version = u16::from_be_bytes([bytes[0], bytes[1]]);
        if version != CAPABILITY_PROFILE_VERSION {
            return Err(ModelError::UnsupportedCapabilityProfileVersion { actual: version });
        }
        Ok(Self::of(BackendClass::from_tag(bytes[2])?))
    }
}

impl Default for CapabilityProfile {
    /// The embedded replica is the default surface of this product.
    fn default() -> Self {
        Self::embedded_local()
    }
}

/// Resolves the guarantees two profiles share.
///
/// Two components that each declare their own profile can rely on exactly
/// this subset without either one requesting the stronger profile.
#[must_use]
pub fn common_guarantees(first: &CapabilityProfile, second: &CapabilityProfile) -> Vec<Capability> {
    let mut shared = Vec::new();
    for guarantee in first.guarantees() {
        if second.supports(*guarantee) {
            shared.push(*guarantee);
        }
    }
    shared
}

impl std::fmt::Display for Capability {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.name())
    }
}

impl std::fmt::Display for BackendClass {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(BackendClass::name(*self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EMBEDDED: CapabilityProfile = CapabilityProfile::embedded_local();
    const DISTRIBUTED: CapabilityProfile = CapabilityProfile::distributed_transactional();

    #[test]
    fn each_class_provides_its_declared_guarantees_and_no_other() {
        assert_eq!(EMBEDDED.class(), BackendClass::EmbeddedLocal);
        assert_eq!(EMBEDDED.guarantees().len(), 5);
        assert!(EMBEDDED.supports(Capability::DurableLocalCommit));
        assert!(EMBEDDED.supports(Capability::ExportRestore));
        assert!(!EMBEDDED.supports(Capability::DistributedSerializableTransactions));
        assert!(!EMBEDDED.supports(Capability::ReplicatedAvailability));

        assert_eq!(DISTRIBUTED.class(), BackendClass::DistributedTransactional);
        assert_eq!(DISTRIBUTED.guarantees().len(), 4);
        assert!(DISTRIBUTED.supports(Capability::DistributedSerializableTransactions));
        assert!(DISTRIBUTED.supports(Capability::ClusterRecovery));
        assert!(!DISTRIBUTED.supports(Capability::DurableLocalCommit));
        assert!(!DISTRIBUTED.supports(Capability::ExportRestore));
    }

    #[test]
    fn every_guarantee_belongs_to_exactly_one_class() {
        let all = [
            Capability::DurableLocalCommit,
            Capability::CausalConvergence,
            Capability::BoundedSync,
            Capability::SchemaMigration,
            Capability::ExportRestore,
            Capability::DistributedSerializableTransactions,
            Capability::ReplicatedAvailability,
            Capability::WatchVersionstamps,
            Capability::ClusterRecovery,
        ];
        for guarantee in all {
            assert_eq!(
                guarantee.is_embedded_local(),
                !guarantee.is_distributed_transactional(),
                "guarantee {} must belong to exactly one class",
                guarantee.name()
            );
        }
    }

    #[test]
    fn require_fails_closed_and_names_every_unmet_guarantee() {
        assert_eq!(
            EMBEDDED.require(&[Capability::DurableLocalCommit, Capability::ExportRestore]),
            Ok(())
        );
        assert_eq!(
            DISTRIBUTED.require(&[Capability::DistributedSerializableTransactions]),
            Ok(())
        );
        assert_eq!(
            EMBEDDED.require(&[
                Capability::DurableLocalCommit,
                Capability::ReplicatedAvailability,
                Capability::WatchVersionstamps,
            ]),
            Err(ModelError::UnsupportedGuarantees {
                class: BackendClass::EmbeddedLocal,
                missing: vec![
                    Capability::ReplicatedAvailability,
                    Capability::WatchVersionstamps,
                ],
            })
        );
        assert_eq!(
            DISTRIBUTED.require(&[Capability::ExportRestore]),
            Err(ModelError::UnsupportedGuarantees {
                class: BackendClass::DistributedTransactional,
                missing: vec![Capability::ExportRestore],
            })
        );
    }

    #[test]
    fn common_subset_is_empty_between_the_two_declared_classes() {
        assert_eq!(common_guarantees(&EMBEDDED, &DISTRIBUTED), Vec::new());
        assert_eq!(common_guarantees(&DISTRIBUTED, &EMBEDDED), Vec::new());
        assert_eq!(
            common_guarantees(&EMBEDDED, &EMBEDDED),
            EMBEDDED.guarantees().to_vec()
        );
    }

    #[test]
    fn profile_identity_round_trips_and_fails_closed() {
        assert_eq!(CapabilityProfile::decode(&EMBEDDED.encode()), Ok(EMBEDDED));
        assert_eq!(
            CapabilityProfile::decode(&DISTRIBUTED.encode()),
            Ok(DISTRIBUTED)
        );
        assert_eq!(
            EMBEDDED.encode(),
            [
                0,
                u8::try_from(CAPABILITY_PROFILE_VERSION).unwrap_or(u8::MAX),
                0
            ]
        );
        assert_eq!(
            CapabilityProfile::decode(&[0, 1]),
            Err(ModelError::InvalidCapabilityProfileEncoding { actual: 2 })
        );
        assert_eq!(
            CapabilityProfile::decode(&[0, 2, 0]),
            Err(ModelError::UnsupportedCapabilityProfileVersion { actual: 2 })
        );
        assert_eq!(
            CapabilityProfile::decode(&[0, 1, 9]),
            Err(ModelError::UnsupportedBackendClass { actual: 9 })
        );
    }

    #[test]
    fn default_profile_is_the_embedded_replica() {
        assert_eq!(CapabilityProfile::default(), EMBEDDED);
        assert_eq!(BackendClass::EmbeddedLocal.name(), "EmbeddedLocal");
        assert_eq!(
            BackendClass::DistributedTransactional.name(),
            "DistributedTransactional"
        );
    }
}
