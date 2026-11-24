//! Wire-contract tests for the protocol crate.

mod baseline;

use std::error::Error;

use logical_db_model::{AccountId, SpaceId};

use super::*;
use crate::wire::{deserialize_bounded_bytes, deserialize_bounded_vec, deserialize_change_bytes};

/// Builds one valid change envelope for the given sender and scope.
fn envelope(replica: ReplicaId, scope: Scope) -> ChangeEnvelope {
    ChangeEnvelope {
        scope,
        document: DocumentId::new(),
        replica,
        operation: OperationId::new(),
        request: OperationDigest::new([3; 32]),
        digest: ChangeDigest::new([7; 32]),
        bytes: vec![1, 2, 3],
    }
}

/// Decodes a valid one-byte checkpoint for fixtures.
fn checkpoint() -> Result<Checkpoint, Box<dyn Error>> {
    Ok(Checkpoint::from_wire(vec![1])?)
}

#[test]
fn mixed_push_outcomes_preserve_operation_correlation() {
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let replica = ReplicaId::new();
    let first = envelope(replica, scope);
    let second = envelope(replica, scope);
    let duplicate = PushOutcome::Duplicate {
        operation: first.operation,
    };
    let rejected = PushOutcome::Rejected {
        operation: second.operation,
        code: RejectionCode::Forbidden,
    };
    let request = PushRequest {
        protocol: PROTOCOL_VERSION,
        replica,
        changes: vec![first, second],
    };
    let mut response = PushResponse {
        protocol: PROTOCOL_VERSION,
        outcomes: vec![duplicate, rejected],
    };
    assert_eq!(
        response.validate(&request),
        Ok(()),
        "correlated outcomes must validate"
    );
    response.outcomes.swap(0, 1);
    assert_eq!(
        response.validate(&request),
        Err(ProtocolError::OutcomeMismatch),
        "swapped outcomes must be refused"
    );
}

#[test]
fn push_rejects_cross_replica_attribution() {
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let sender = ReplicaId::new();
    let request = PushRequest {
        protocol: PROTOCOL_VERSION,
        replica: sender,
        changes: vec![envelope(ReplicaId::new(), scope)],
    };
    assert_eq!(
        request.validate(),
        Err(ProtocolError::ReplicaMismatch),
        "a foreign sender must be refused"
    );
}

#[test]
fn pull_rejects_cross_scope_material() -> Result<(), Box<dyn Error>> {
    let first = Scope::new(AccountId::new(), SpaceId::new());
    let second = Scope::new(AccountId::new(), SpaceId::new());
    let page = PullPage {
        protocol: PROTOCOL_VERSION,
        changes: vec![envelope(ReplicaId::new(), second)],
        checkpoint: checkpoint()?,
        more: false,
    };
    assert_eq!(
        page.validate(first),
        Err(ProtocolError::ScopeMismatch),
        "material from another scope must be refused"
    );
    Ok(())
}

#[test]
fn pull_rejects_a_non_progress_page() -> Result<(), Box<dyn Error>> {
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let page = PullPage {
        protocol: PROTOCOL_VERSION,
        changes: Vec::new(),
        checkpoint: checkpoint()?,
        more: true,
    };
    assert_eq!(
        page.validate(scope),
        Err(ProtocolError::NonProgressPage),
        "a more=true empty page must be refused"
    );
    Ok(())
}

#[test]
fn push_response_rejects_missing_and_reordered_operation_results() {
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let replica = ReplicaId::new();
    let first = envelope(replica, scope);
    let second = envelope(replica, scope);
    let request = PushRequest {
        protocol: PROTOCOL_VERSION,
        replica,
        changes: vec![first.clone(), second.clone()],
    };
    let missing = PushResponse {
        protocol: PROTOCOL_VERSION,
        outcomes: vec![PushOutcome::Accepted {
            operation: first.operation,
        }],
    };
    assert_eq!(
        missing.validate(&request),
        Err(ProtocolError::OutcomeMismatch),
        "an omitted outcome must be refused"
    );
    let reordered = PushResponse {
        protocol: PROTOCOL_VERSION,
        outcomes: vec![
            PushOutcome::Accepted {
                operation: second.operation,
            },
            PushOutcome::Accepted {
                operation: first.operation,
            },
        ],
    };
    assert_eq!(
        reordered.validate(&request),
        Err(ProtocolError::OutcomeMismatch),
        "reordered outcomes must be refused"
    );
}

#[test]
fn hostile_sequence_hint_cannot_preallocate_past_change_limit() {
    use lgwks_std::json::serde::de::value::{Error as ValueError, SeqDeserializer};

    let oversized = std::iter::repeat_n(0u8, MAX_CHANGE_BYTES.saturating_add(1));
    let deserializer = SeqDeserializer::<_, ValueError>::new(oversized);
    assert!(
        deserialize_change_bytes(deserializer).is_err(),
        "a sequence above the change bound must be refused"
    );
}

#[test]
fn checkpoint_deserialization_cannot_bypass_constructor_bounds() {
    use lgwks_std::json::serde::de::value::{Error as ValueError, SeqDeserializer};

    let empty = SeqDeserializer::<_, ValueError>::new(std::iter::empty::<u8>());
    assert!(
        Checkpoint::deserialize(empty).is_err(),
        "an empty checkpoint must be refused"
    );
    let oversized = SeqDeserializer::<_, ValueError>::new(std::iter::repeat_n(
        0u8,
        MAX_CHECKPOINT_BYTES.saturating_add(1),
    ));
    assert!(
        Checkpoint::deserialize(oversized).is_err(),
        "an oversized checkpoint must be refused"
    );
}

#[test]
fn checkpoint_and_change_bounds_accept_exact_edges_only() -> Result<(), Box<dyn Error>> {
    assert_eq!(
        Checkpoint::from_wire(Vec::new()),
        Err(ProtocolError::InvalidCheckpoint),
        "an empty checkpoint must be refused"
    );
    let one = Checkpoint::from_wire(vec![9])?;
    assert_eq!(one.as_wire(), &[9], "the checkpoint must round-trip");
    let maximum = vec![7; MAX_CHECKPOINT_BYTES];
    assert_eq!(
        Checkpoint::from_wire(maximum.clone())?.as_wire(),
        maximum,
        "a maximum-length checkpoint must round-trip"
    );
    assert_eq!(
        Checkpoint::from_wire(vec![0; MAX_CHECKPOINT_BYTES.saturating_add(1)]),
        Err(ProtocolError::InvalidCheckpoint),
        "one byte above the checkpoint bound must be refused"
    );

    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let replica = ReplicaId::new();
    let mut change = envelope(replica, scope);
    change.bytes.clear();
    assert_eq!(
        change.validate(),
        Err(ProtocolError::InvalidChangeSize(0)),
        "an empty change must be refused"
    );
    change.bytes = vec![1];
    assert_eq!(change.validate(), Ok(()), "a one-byte change is valid");
    change.bytes = vec![1; MAX_CHANGE_BYTES];
    assert_eq!(
        change.validate(),
        Ok(()),
        "a maximum-length change is valid"
    );
    change.bytes.push(1);
    assert_eq!(
        change.validate(),
        Err(ProtocolError::InvalidChangeSize(
            MAX_CHANGE_BYTES.saturating_add(1)
        )),
        "one byte above the change bound must be refused"
    );
    Ok(())
}

#[test]
fn every_protocol_and_page_boundary_has_an_exact_rejection_edge() -> Result<(), Box<dyn Error>> {
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let replica = ReplicaId::new();
    let max_changes = u16::try_from(MAX_CHANGES_PER_PAGE)?;
    let max_bytes = u32::try_from(MAX_PAGE_BYTES)?;
    let pull = |protocol, max_changes, max_bytes| PullRequest {
        protocol,
        scope,
        replica,
        request: OperationDigest::new([4; 32]),
        after: None,
        max_changes,
        max_bytes,
    };
    assert!(
        matches!(
            pull(PROTOCOL_VERSION.saturating_add(1), 1, 1).validate(),
            Err(ProtocolError::UnsupportedVersion { .. })
        ),
        "a future protocol version must be refused"
    );
    assert_eq!(
        pull(PROTOCOL_VERSION, 0, 1).validate(),
        Err(ProtocolError::PageLimit),
        "a zero count limit must be refused"
    );
    assert_eq!(
        pull(PROTOCOL_VERSION, 1, 1).validate(),
        Ok(()),
        "a one/one limit is valid"
    );
    assert_eq!(
        pull(PROTOCOL_VERSION, max_changes, max_bytes).validate(),
        Ok(()),
        "the exact maximum limits are valid"
    );
    assert_eq!(
        pull(PROTOCOL_VERSION, max_changes.saturating_add(1), 1).validate(),
        Err(ProtocolError::PageLimit),
        "one above the count limit must be refused"
    );
    assert_eq!(
        pull(PROTOCOL_VERSION, 1, 0).validate(),
        Err(ProtocolError::PageLimit),
        "a zero byte limit must be refused"
    );
    assert_eq!(
        pull(PROTOCOL_VERSION, 1, max_bytes.saturating_add(1)).validate(),
        Err(ProtocolError::PageLimit),
        "one above the byte limit must be refused"
    );

    let capacity = MAX_PAGE_BYTES.div_euclid(MAX_CHANGE_BYTES);
    let mut exact_bytes = envelope(replica, scope);
    exact_bytes.bytes = vec![1; MAX_CHANGE_BYTES];
    let exact_page = PushRequest {
        protocol: PROTOCOL_VERSION,
        replica,
        changes: vec![exact_bytes; capacity],
    };
    assert_eq!(
        exact_page.validate(),
        Ok(()),
        "the exact aggregate byte maximum is valid"
    );
    let mut oversized_page = exact_page;
    oversized_page.changes.push(envelope(replica, scope));
    assert_eq!(
        oversized_page.validate(),
        Err(ProtocolError::PageLimit),
        "one change above the byte maximum must be refused"
    );

    let count_page = PushRequest {
        protocol: PROTOCOL_VERSION,
        replica,
        changes: vec![envelope(replica, scope); MAX_CHANGES_PER_PAGE.saturating_add(1)],
    };
    assert_eq!(
        count_page.validate(),
        Err(ProtocolError::PageLimit),
        "one above the count maximum must be refused"
    );
    let exact_count = PushRequest {
        protocol: PROTOCOL_VERSION,
        replica,
        changes: vec![envelope(replica, scope); MAX_CHANGES_PER_PAGE],
    };
    assert_eq!(
        exact_count.validate(),
        Ok(()),
        "the exact count maximum is valid"
    );
    Ok(())
}

#[test]
fn bounded_deserializers_preserve_values_and_reject_the_first_excess_item()
-> Result<(), Box<dyn Error>> {
    use lgwks_std::json::serde::de::value::{
        BorrowedBytesDeserializer, Error as ValueError, SeqDeserializer,
    };

    let source = [1_u8, 2, 3];
    let bytes = deserialize_bounded_bytes(
        BorrowedBytesDeserializer::<ValueError>::new(&source),
        source.len(),
        "test bytes",
    )?;
    assert_eq!(bytes, source, "borrowed bytes must pass through unchanged");

    let exact = SeqDeserializer::<_, ValueError>::new(0_u8..4);
    assert_eq!(
        deserialize_bounded_vec::<_, u8>(exact, 4, "test values")?,
        vec![0, 1, 2, 3],
        "an exact-length sequence must pass through unchanged"
    );
    let excessive = SeqDeserializer::<_, ValueError>::new(0_u8..5);
    assert!(
        deserialize_bounded_vec::<_, u8>(excessive, 4, "test values").is_err(),
        "one item above the bound must be refused"
    );

    let exact_bytes = SeqDeserializer::<_, ValueError>::new(0_u8..4);
    assert_eq!(
        deserialize_bounded_bytes(exact_bytes, 4, "test bytes")?,
        vec![0, 1, 2, 3],
        "an exact-length byte sequence must pass through unchanged"
    );
    let shorter_bytes = SeqDeserializer::<_, ValueError>::new(0_u8..3);
    assert_eq!(
        deserialize_bounded_bytes(shorter_bytes, 4, "test bytes")?,
        vec![0, 1, 2],
        "a shorter byte sequence must pass through unchanged"
    );
    let excessive_bytes = SeqDeserializer::<_, ValueError>::new(0_u8..5);
    assert!(
        deserialize_bounded_bytes(excessive_bytes, 4, "test bytes").is_err(),
        "one byte above the bound must be refused"
    );
    Ok(())
}

#[test]
fn cbor_wire_visitors_preserve_exact_pages_and_reject_first_excess() -> Result<(), Box<dyn Error>> {
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let replica = ReplicaId::new();
    let request = PushRequest {
        protocol: PROTOCOL_VERSION,
        replica,
        changes: vec![envelope(replica, scope); MAX_CHANGES_PER_PAGE],
    };
    let mut wire = Vec::new();
    ciborium::into_writer(&request, &mut wire)?;
    let decoded: PushRequest = ciborium::from_reader(wire.as_slice())?;
    assert_eq!(decoded, request, "a full push page must round-trip");
    assert_eq!(decoded.validate(), Ok(()), "the decoded page must validate");

    let mut excessive = request.clone();
    excessive.changes.push(envelope(replica, scope));
    wire.clear();
    ciborium::into_writer(&excessive, &mut wire)?;
    assert!(
        ciborium::from_reader::<PushRequest, _>(wire.as_slice()).is_err(),
        "a push page above the count bound must be refused"
    );

    let response = PushResponse {
        protocol: PROTOCOL_VERSION,
        outcomes: request
            .changes
            .iter()
            .map(|change| PushOutcome::Accepted {
                operation: change.operation,
            })
            .collect(),
    };
    wire.clear();
    ciborium::into_writer(&response, &mut wire)?;
    let decoded: PushResponse = ciborium::from_reader(wire.as_slice())?;
    assert_eq!(decoded, response, "a full response page must round-trip");

    let mut excessive = response;
    excessive.outcomes.push(PushOutcome::Accepted {
        operation: OperationId::new(),
    });
    wire.clear();
    ciborium::into_writer(&excessive, &mut wire)?;
    assert!(
        ciborium::from_reader::<PushResponse, _>(wire.as_slice()).is_err(),
        "a response page above the count bound must be refused"
    );

    let capacity = MAX_PAGE_BYTES.div_euclid(MAX_CHANGE_BYTES);
    let mut exact_bytes = envelope(replica, scope);
    exact_bytes.bytes = vec![1; MAX_CHANGE_BYTES];
    let exact = PushRequest {
        protocol: PROTOCOL_VERSION,
        replica,
        changes: vec![exact_bytes; capacity],
    };
    wire.clear();
    ciborium::into_writer(&exact, &mut wire)?;
    let decoded: PushRequest = ciborium::from_reader(wire.as_slice())?;
    assert_eq!(
        decoded.changes.len(),
        exact.changes.len(),
        "an exact aggregate-byte page must round-trip"
    );
    let mut excessive = exact;
    excessive.changes.push(envelope(replica, scope));
    wire.clear();
    ciborium::into_writer(&excessive, &mut wire)?;
    assert!(
        ciborium::from_reader::<PushRequest, _>(wire.as_slice()).is_err(),
        "a page above the byte bound must be refused"
    );
    Ok(())
}

#[test]
fn valid_push_response_and_pull_page_still_validate_their_versions() -> Result<(), Box<dyn Error>> {
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let replica = ReplicaId::new();
    let change = envelope(replica, scope);
    let request = PushRequest {
        protocol: PROTOCOL_VERSION,
        replica,
        changes: vec![change.clone()],
    };
    let valid = PushResponse {
        protocol: PROTOCOL_VERSION,
        outcomes: vec![PushOutcome::Duplicate {
            operation: change.operation,
        }],
    };
    assert_eq!(
        valid.validate(&request),
        Ok(()),
        "a matching response must validate"
    );
    assert!(
        matches!(
            PushResponse {
                protocol: PROTOCOL_VERSION.saturating_add(1),
                ..valid
            }
            .validate(&request),
            Err(ProtocolError::UnsupportedVersion { .. })
        ),
        "a future response version must be refused"
    );
    let page = PullPage {
        protocol: PROTOCOL_VERSION.saturating_add(1),
        changes: vec![change],
        checkpoint: checkpoint()?,
        more: false,
    };
    assert!(
        matches!(
            page.validate(scope),
            Err(ProtocolError::UnsupportedVersion { .. })
        ),
        "a future page version must be refused"
    );
    Ok(())
}
