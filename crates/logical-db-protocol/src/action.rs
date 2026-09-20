//! Canonical action identities, independent from application retry labels.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use lgwks_std::hash::Hasher;
use logical_db_model::{
    ActionDigest, DocumentId, OperationDigest, ReplicaId, Scope, SnapshotBaseline,
};

use crate::{ChangeEnvelope, Checkpoint, PROTOCOL_VERSION};

/// Server-normalized page parameters after authenticated checkpoint decoding.
///
/// The store receives these actual parameters and independently recomputes
/// their digest; a caller cannot substitute limits or position behind a grant.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PageAction {
    /// Account and application whose data will be returned.
    pub scope: Scope,
    /// Receiving replica identified by the wire request.
    pub replica: ReplicaId,
    /// Caller-provided intent identity, never itself an authorization digest.
    pub intent: OperationDigest,
    /// Authenticated, retention-normalized starting position.
    pub after: u64,
    /// Actual record limit used by the store.
    pub max_changes: u16,
    /// Actual byte limit used by the store.
    pub max_bytes: u32,
}

impl PageAction {
    /// Hashes every normalized page parameter under the page action domain.
    #[must_use]
    pub fn digest(self) -> ActionDigest {
        let mut digest = action_hash(b"page", self.scope, self.intent);
        frame(&mut digest, self.replica.as_uuid().as_bytes());
        frame(&mut digest, &self.after.to_be_bytes());
        frame(&mut digest, &self.max_changes.to_be_bytes());
        frame(&mut digest, &self.max_bytes.to_be_bytes());
        ActionDigest::new(*digest.finalize().as_bytes())
    }
}

/// Server-normalized recovery parameters after authenticated request decoding.
///
/// The store receives these actual parameters and independently recomputes
/// their action digest, so a caller cannot preserve the intent while
/// substituting the checkpoint or the exclusive document cursor behind a grant.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveryAction {
    /// Scope whose recovery baseline is read.
    pub scope: Scope,
    /// Recovering replica bound into the invocation.
    pub replica: ReplicaId,
    /// Caller-provided intent identity, never itself an authorization digest.
    pub intent: OperationDigest,
    /// Validated triggering checkpoint, when the caller held one.
    pub after: Option<Checkpoint>,
    /// Exclusive document cursor selecting this page, when resuming.
    pub after_document: Option<DocumentId>,
}

impl RecoveryAction {
    /// Recomputes the canonical action digest from the actual parameters.
    #[must_use]
    pub fn digest(&self) -> ActionDigest {
        recover_action(
            self.scope,
            self.replica,
            self.intent,
            self.after.as_ref(),
            self.after_document.as_ref(),
        )
    }
}

/// Hashes every change field, including intent and the exact payload bytes.
///
/// Admission must validate protocol bounds and native CRDT bytes first. The
/// fixed action domain identifies the write effect; no caller effect is trusted.
#[must_use]
pub fn write_action(change: &ChangeEnvelope) -> ActionDigest {
    let mut digest = action_hash(b"write", change.scope, change.request);
    frame(&mut digest, change.document.as_uuid().as_bytes());
    frame(&mut digest, change.operation.as_uuid().as_bytes());
    frame(&mut digest, change.replica.as_uuid().as_bytes());
    frame(&mut digest, &change.digest.into_bytes());
    frame(&mut digest, &change.bytes);
    ActionDigest::new(*digest.finalize().as_bytes())
}

/// Binds a seal operation to the complete state-bearing artifact descriptor.
#[must_use]
pub fn seal_snapshot_action(
    scope: Scope,
    intent: OperationDigest,
    baseline: &SnapshotBaseline,
) -> ActionDigest {
    let mut digest = action_hash(b"seal-snapshot", scope, intent);
    frame(&mut digest, &baseline.encode());
    ActionDigest::new(*digest.finalize().as_bytes())
}

/// Binds a recovery fetch to the scope, replica, intent, triggering checkpoint,
/// and exclusive document cursor.
#[must_use]
pub fn recover_action(
    scope: Scope,
    replica: ReplicaId,
    intent: OperationDigest,
    after: Option<&Checkpoint>,
    after_document: Option<&DocumentId>,
) -> ActionDigest {
    let mut digest = action_hash(b"recover-baseline", scope, intent);
    frame(&mut digest, replica.as_uuid().as_bytes());
    match after {
        Some(checkpoint) => {
            frame(&mut digest, &[1]);
            frame(&mut digest, checkpoint.as_wire());
        }
        None => frame(&mut digest, &[0]),
    }
    match after_document {
        Some(document) => {
            frame(&mut digest, &[1]);
            frame(&mut digest, document.as_uuid().as_bytes());
        }
        None => frame(&mut digest, &[0]),
    }
    ActionDigest::new(*digest.finalize().as_bytes())
}

/// Binds consumption acknowledgement to its exact upper position.
#[must_use]
pub fn mark_consumed_action(scope: Scope, intent: OperationDigest, through: u64) -> ActionDigest {
    let mut digest = action_hash(b"mark-consumed", scope, intent);
    frame(&mut digest, &through.to_be_bytes());
    ActionDigest::new(*digest.finalize().as_bytes())
}

/// Binds collection to its upper position and explicit optional live checkpoint.
#[must_use]
pub fn prune_action(
    scope: Scope,
    intent: OperationDigest,
    through: u64,
    min_live: Option<u64>,
) -> ActionDigest {
    let mut digest = action_hash(b"prune", scope, intent);
    frame(&mut digest, &through.to_be_bytes());
    frame(&mut digest, &[u8::from(min_live.is_some())]);
    if let Some(position) = min_live {
        frame(&mut digest, &position.to_be_bytes());
    }
    ActionDigest::new(*digest.finalize().as_bytes())
}

fn action_hash(kind: &[u8], scope: Scope, intent: OperationDigest) -> Hasher {
    let mut digest = Hasher::new();
    frame(&mut digest, b"logical-db-action-v1");
    frame(&mut digest, &PROTOCOL_VERSION.to_be_bytes());
    frame(&mut digest, kind);
    frame(&mut digest, scope.account().as_uuid().as_bytes());
    frame(&mut digest, scope.space().as_uuid().as_bytes());
    frame(&mut digest, &intent.into_bytes());
    digest
}

fn frame(digest: &mut Hasher, bytes: &[u8]) {
    digest.update(&(bytes.len() as u64).to_be_bytes());
    digest.update(bytes);
}

#[cfg(test)]
mod tests {
    use super::*;
    use logical_db_model::{AccountId, ChangeDigest, DocumentId, OperationId, SpaceId};

    fn change() -> ChangeEnvelope {
        ChangeEnvelope {
            scope: Scope::new(AccountId::new(), SpaceId::new()),
            document: DocumentId::new(),
            replica: ReplicaId::new(),
            operation: OperationId::new(),
            request: OperationDigest::new([1; 32]),
            digest: ChangeDigest::new([2; 32]),
            bytes: vec![3, 4],
        }
    }

    #[test]
    fn write_action_v1_has_a_portable_golden_encoding() {
        let change = ChangeEnvelope {
            scope: Scope::new(
                AccountId::from_uuid("01010101-0101-0101-0101-010101010101".parse().unwrap()),
                SpaceId::from_uuid("02020202-0202-0202-0202-020202020202".parse().unwrap()),
            ),
            document: DocumentId::from_uuid(
                "03030303-0303-0303-0303-030303030303".parse().unwrap(),
            ),
            operation: OperationId::from_uuid(
                "04040404-0404-0404-0404-040404040404".parse().unwrap(),
            ),
            replica: ReplicaId::from_uuid("05050505-0505-0505-0505-050505050505".parse().unwrap()),
            request: OperationDigest::new([6; 32]),
            digest: ChangeDigest::new([7; 32]),
            bytes: vec![0, 1, 255],
        };
        // Golden re-pinned at the 2026-09-16 estate hash swap: the digest
        // primitive is BLAKE3 through `lgwks_std::hash`, framing unchanged.
        assert_eq!(
            write_action(&change).into_bytes(),
            [
                0xbf, 0x4a, 0x28, 0x73, 0xe9, 0x91, 0x12, 0x87, 0xf4, 0xb6, 0x48, 0x36, 0xce, 0x3e,
                0x84, 0xf2, 0x66, 0xd9, 0x68, 0x5b, 0x49, 0x74, 0xb8, 0x68, 0x29, 0xe4, 0x52, 0xef,
                0xc7, 0x15, 0xa4, 0x87,
            ]
        );
    }

    #[test]
    fn write_identity_changes_for_every_authorized_field() {
        let original = change();
        let expected = write_action(&original);
        assert_eq!(expected, write_action(&original));
        let mut variants = vec![original.clone(); 8];
        variants[0].scope = Scope::new(AccountId::new(), original.scope.space());
        variants[1].scope = Scope::new(original.scope.account(), SpaceId::new());
        variants[2].document = DocumentId::new();
        variants[3].operation = OperationId::new();
        variants[4].replica = ReplicaId::new();
        variants[5].request = OperationDigest::new([5; 32]);
        variants[6].digest = ChangeDigest::new([6; 32]);
        variants[7].bytes.push(7);
        for variant in variants {
            assert_ne!(expected, write_action(&variant));
        }
    }

    #[test]
    fn page_identity_binds_actor_position_and_limits() {
        let original = change();
        let page = PageAction {
            scope: original.scope,
            replica: original.replica,
            intent: original.request,
            after: 0,
            max_changes: 1,
            max_bytes: 100,
        };
        let variants = [
            PageAction {
                replica: ReplicaId::new(),
                ..page
            },
            PageAction { after: 1, ..page },
            PageAction {
                max_changes: 2,
                ..page
            },
            PageAction {
                max_bytes: 101,
                ..page
            },
        ];
        for variant in variants {
            assert_ne!(page.digest(), variant.digest());
        }
    }

    #[test]
    fn retention_actions_are_domain_separated_and_bind_bounds() {
        use logical_db_model::BaselineDigest;

        let original = change();
        let (scope, intent) = (original.scope, original.request);
        let consumed = mark_consumed_action(scope, intent, 1);
        assert_ne!(consumed, mark_consumed_action(scope, intent, 2));
        assert_ne!(consumed, prune_action(scope, intent, 1, None));
        assert_ne!(
            prune_action(scope, intent, 1, None),
            prune_action(scope, intent, 1, Some(0))
        );
        let baseline = SnapshotBaseline::new(1, BaselineDigest::new([1; 32]), 1, 10);
        for rebound in [
            SnapshotBaseline::new(2, baseline.digest(), 1, 10),
            SnapshotBaseline::new(1, BaselineDigest::new([2; 32]), 1, 10),
            SnapshotBaseline::new(1, baseline.digest(), 2, 10),
            SnapshotBaseline::new(1, baseline.digest(), 1, 11),
        ] {
            assert_ne!(
                seal_snapshot_action(scope, intent, &baseline),
                seal_snapshot_action(scope, intent, &rebound)
            );
        }
    }
}
