//! Descriptor, digest, and streaming-builder behaviour of the snapshot baseline.

use super::*;
use crate::{AccountId, SpaceId};
use lgwks_std::id::Uuid;

/// A fresh random-identifier scope for tests that only need one.
fn scope() -> Scope {
    Scope::new(AccountId::new(), SpaceId::new())
}

/// A small three-byte non-tombstone document for generic cases.
fn doc() -> SnapshotDocument {
    SnapshotDocument::new(DocumentId::new(), vec![1, 2, 3], false)
}

/// A scope with fixed bytes, so digests are reproducible across runs.
fn fixed_scope() -> Scope {
    Scope::new(
        AccountId::from_uuid(Uuid::from([0x11; 16])),
        SpaceId::from_uuid(Uuid::from([0x22; 16])),
    )
}

/// A document with a fixed identity so its digest is stable across runs.
fn fixed_doc(byte: u8, snapshot: Vec<u8>, tombstone: bool) -> SnapshotDocument {
    SnapshotDocument::new(
        DocumentId::from_uuid(Uuid::from([byte; 16])),
        snapshot,
        tombstone,
    )
}

/// Folds a known-good sequence through the builder and returns its digest.
///
/// Every rejection is propagated with `?`, so a test that feeds a valid
/// sequence fails loudly if the builder ever refuses one.
fn streamed_digest(
    scope: Scope,
    position: u64,
    documents: &[SnapshotDocument],
) -> Result<BaselineDigest, Box<dyn std::error::Error>> {
    let count = u64::try_from(documents.len())?;
    let mut builder = SnapshotDigestBuilder::new(scope, position, count);
    for document in documents {
        builder.push(document)?;
    }
    Ok(builder.finish()?.0)
}

#[test]
fn artifact_digest_is_reproducible_and_descriptor_validates()
-> Result<(), Box<dyn std::error::Error>> {
    let scope = scope();
    let first = SnapshotArtifact::new(scope, 7, vec![doc()])?;
    let second = SnapshotArtifact::new(scope, 7, first.documents().to_vec())?;
    assert_eq!(first.baseline().digest(), second.baseline().digest());
    assert!(first.validate(scope).is_ok());
    // The digest binds the scope, so another scope cannot validate it.
    assert_eq!(
        first.validate(fixed_scope()),
        Err(ModelError::SnapshotDescriptorMismatch)
    );
    Ok(())
}

#[test]
fn artifact_digest_has_a_portable_golden_encoding() -> Result<(), Box<dyn std::error::Error>> {
    let artifact = SnapshotArtifact::new(
        fixed_scope(),
        7,
        vec![fixed_doc(0x33, vec![1, 2, 3], false)],
    )?;
    assert_eq!(
        artifact.baseline().digest().into_bytes(),
        // Re-pinned at the 2026-09-16 estate hash swap to BLAKE3.
        [
            41, 100, 14, 43, 176, 108, 83, 249, 216, 116, 41, 169, 65, 93, 202, 115, 211, 87, 68,
            48, 174, 151, 216, 133, 232, 209, 78, 155, 134, 56, 147, 69,
        ]
    );
    Ok(())
}

#[test]
fn artifact_rejects_order_duplicates_and_bounds() {
    let first = doc();
    let second = first.clone();
    assert_eq!(
        SnapshotArtifact::new(scope(), 0, vec![first, second]),
        Err(ModelError::SnapshotDocumentsUnordered)
    );
    let mut too_many = Vec::new();
    for _ in 0..=MAX_BASELINE_DOCUMENTS {
        too_many.push(doc());
    }
    assert_eq!(
        SnapshotArtifact::new(scope(), 0, too_many),
        Err(ModelError::SnapshotTooManyDocuments)
    );
    assert_eq!(
        SnapshotArtifact::new(
            scope(),
            0,
            vec![SnapshotDocument::new(DocumentId::new(), Vec::new(), true)],
        ),
        Err(ModelError::SnapshotDocumentInvalid)
    );
}

#[test]
fn streamed_documents_reproduce_the_in_memory_digest() -> Result<(), Box<dyn std::error::Error>> {
    let scope = scope();
    let documents = (0..200_u128)
        .map(|index| {
            SnapshotDocument::new(
                DocumentId::from_uuid(Uuid::from(index.to_be_bytes())),
                vec![1, 2, 3],
                index % 2 == 0,
            )
        })
        .collect::<Vec<_>>();
    let artifact = SnapshotArtifact::new(scope, 9, documents.clone())?;
    let count = u64::try_from(documents.len())?;
    let mut builder = SnapshotDigestBuilder::new(scope, 9, count);
    for document in &documents {
        builder.push(document)?;
    }
    let (digest, byte_len) = builder.finish()?;
    assert_eq!(digest, artifact.baseline().digest());
    assert_eq!(byte_len, 600);
    Ok(())
}

#[test]
fn streamed_digest_folds_past_the_single_artifact_cap() -> Result<(), Box<dyn std::error::Error>> {
    let scope = scope();
    let declared = u64::try_from(MAX_BASELINE_DOCUMENTS)?;
    let mut builder = SnapshotDigestBuilder::new(scope, 0, declared.saturating_add(1));
    for index in 0..=MAX_BASELINE_DOCUMENTS {
        builder.push(&SnapshotDocument::new(
            DocumentId::from_uuid(Uuid::from(u128::try_from(index)?.to_be_bytes())),
            vec![7],
            false,
        ))?;
    }
    let (_, byte_len) = builder.finish()?;
    assert_eq!(byte_len, MAX_BASELINE_DOCUMENTS.saturating_add(1));
    Ok(())
}

// The pre-existing `artifact_digest_has_a_portable_golden_encoding` case is
// the independent one-shot framing oracle. The vectors below were captured
// from the pre-change `SnapshotArtifact::new` implementation and frozen
// verbatim, so a shared framing bug cannot hide behind a test that compares
// two paths both built from `SnapshotDigestBuilder`.

#[test]
fn empty_stream_digest_has_a_portable_golden_encoding() -> Result<(), Box<dyn std::error::Error>> {
    let expected = [
        71, 43, 172, 140, 216, 191, 236, 239, 92, 7, 42, 181, 192, 141, 96, 131, 199, 34, 103, 0,
        170, 35, 247, 111, 78, 137, 207, 157, 208, 248, 251, 238,
    ];
    let artifact = SnapshotArtifact::new(fixed_scope(), 7, Vec::new())?;
    assert_eq!(artifact.baseline().digest().into_bytes(), expected);
    assert_eq!(artifact.baseline().byte_len(), 0);
    assert_eq!(
        streamed_digest(fixed_scope(), 7, &[])?.into_bytes(),
        expected
    );
    Ok(())
}

#[test]
fn tombstone_document_digest_has_a_portable_golden_encoding()
-> Result<(), Box<dyn std::error::Error>> {
    let expected = [
        159, 212, 63, 55, 91, 223, 140, 196, 39, 31, 57, 185, 169, 62, 119, 131, 189, 52, 85, 205,
        204, 65, 198, 102, 214, 37, 114, 65, 221, 104, 136, 22,
    ];
    let documents = vec![fixed_doc(0x33, vec![1, 2, 3], true)];
    let artifact = SnapshotArtifact::new(fixed_scope(), 7, documents.clone())?;
    assert_eq!(artifact.baseline().digest().into_bytes(), expected);
    assert_eq!(artifact.baseline().byte_len(), 3);
    assert_eq!(
        streamed_digest(fixed_scope(), 7, &documents)?.into_bytes(),
        expected
    );
    Ok(())
}

#[test]
fn multi_document_stream_digest_has_a_portable_golden_encoding()
-> Result<(), Box<dyn std::error::Error>> {
    let expected = [
        16, 249, 242, 54, 53, 147, 16, 252, 104, 176, 157, 59, 236, 236, 174, 188, 61, 59, 175,
        174, 237, 67, 11, 175, 237, 255, 98, 131, 185, 20, 251, 201,
    ];
    let documents = vec![
        fixed_doc(0x33, vec![1, 2, 3], false),
        fixed_doc(0x44, vec![4, 5, 6], true),
        fixed_doc(0x55, vec![7, 8, 9], false),
    ];
    let artifact = SnapshotArtifact::new(fixed_scope(), 7, documents.clone())?;
    assert_eq!(artifact.baseline().digest().into_bytes(), expected);
    assert_eq!(artifact.baseline().byte_len(), 9);
    assert_eq!(
        streamed_digest(fixed_scope(), 7, &documents)?.into_bytes(),
        expected
    );
    Ok(())
}

// Falsifiers: each hostile shape must either change the digest or be refused.

#[test]
fn streamed_digest_refuses_overlong_streams() -> Result<(), Box<dyn std::error::Error>> {
    let mut builder = SnapshotDigestBuilder::new(scope(), 0, 1);
    builder.push(&fixed_doc(0x33, vec![1], false))?;
    builder.push(&fixed_doc(0x44, vec![2], false))?;
    assert_eq!(
        builder.finish(),
        Err(ModelError::SnapshotDescriptorMismatch)
    );
    Ok(())
}

#[test]
fn streamed_digest_refuses_zero_count_with_a_document() -> Result<(), Box<dyn std::error::Error>> {
    let mut builder = SnapshotDigestBuilder::new(scope(), 0, 0);
    builder.push(&fixed_doc(0x33, vec![1], false))?;
    assert_eq!(
        builder.finish(),
        Err(ModelError::SnapshotDescriptorMismatch)
    );
    Ok(())
}

#[test]
fn streamed_digest_refuses_empty_and_oversized_payloads() {
    let mut builder = SnapshotDigestBuilder::new(scope(), 0, 2);
    assert_eq!(
        builder.push(&fixed_doc(0x33, Vec::new(), false)),
        Err(ModelError::SnapshotDocumentInvalid)
    );
    assert_eq!(
        builder.push(&fixed_doc(
            0x44,
            vec![0; MAX_BASELINE_BYTES.saturating_add(1)],
            false
        )),
        Err(ModelError::SnapshotDocumentInvalid)
    );
    assert_eq!(
        builder.finish(),
        Err(ModelError::SnapshotDescriptorMismatch)
    );
}

#[test]
fn streamed_digest_refuses_reverse_ordering_across_a_chunk_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let mut builder = SnapshotDigestBuilder::new(scope(), 0, 2);
    // The first chunk ends on the higher identity; the next chunk regresses.
    builder.push(&fixed_doc(0x55, vec![1], false))?;
    assert_eq!(
        builder.push(&fixed_doc(0x44, vec![2], false)),
        Err(ModelError::SnapshotDocumentsUnordered)
    );
    Ok(())
}

#[test]
fn streamed_digest_is_scope_position_and_tombstone_sensitive()
-> Result<(), Box<dyn std::error::Error>> {
    let documents = [fixed_doc(0x33, vec![1, 2, 3], false)];
    let base = streamed_digest(fixed_scope(), 7, &documents)?;
    let other_scope = Scope::new(
        AccountId::from_uuid(Uuid::from([0x12; 16])),
        SpaceId::from_uuid(Uuid::from([0x22; 16])),
    );
    assert_ne!(base, streamed_digest(other_scope, 7, &documents)?);
    assert_ne!(base, streamed_digest(fixed_scope(), 8, &documents)?);
    let tombstones = [fixed_doc(0x33, vec![1, 2, 3], true)];
    assert_ne!(base, streamed_digest(fixed_scope(), 7, &tombstones)?);
    Ok(())
}

#[test]
fn rejected_push_leaves_the_builder_usable_and_unchanged() -> Result<(), Box<dyn std::error::Error>>
{
    let scope = scope();
    let first = fixed_doc(0x33, vec![1], false);
    let second = fixed_doc(0x44, vec![2], false);
    let mut builder = SnapshotDigestBuilder::new(scope, 0, 2);
    builder.push(&first)?;
    assert_eq!(
        builder.push(&first),
        Err(ModelError::SnapshotDocumentsUnordered)
    );
    assert_eq!(
        builder.push(&fixed_doc(0x44, Vec::new(), false)),
        Err(ModelError::SnapshotDocumentInvalid)
    );
    // A rejected push is not folded: the next valid push still observes
    // `first` as its predecessor and the builder finalizes the same stream
    // a fresh builder would.
    builder.push(&second)?;
    let mut fresh = SnapshotDigestBuilder::new(scope, 0, 2);
    fresh.push(&first)?;
    fresh.push(&second)?;
    assert_eq!(builder.finish()?, fresh.finish()?);
    Ok(())
}

#[test]
fn caller_admits_aggregate_bytes_before_hashing_a_large_body()
-> Result<(), Box<dyn std::error::Error>> {
    let scope = scope();
    let start = HASHED_BODY_BYTES.with(std::cell::Cell::get);
    SnapshotArtifact::new(scope, 0, vec![fixed_doc(0x33, vec![1, 2, 3], false)])?;
    let after_small = HASHED_BODY_BYTES.with(std::cell::Cell::get);
    assert_eq!(after_small, start.saturating_add(3));
    let documents = vec![
        fixed_doc(0x33, vec![0xAB; MAX_BASELINE_BYTES], false),
        fixed_doc(0x44, vec![0xCD; 1], false),
    ];
    assert_eq!(
        SnapshotArtifact::new(scope, 0, documents),
        Err(ModelError::SnapshotTooLarge)
    );
    assert_eq!(
        HASHED_BODY_BYTES.with(std::cell::Cell::get),
        after_small,
        "aggregate admission must run before any document body is hashed"
    );
    Ok(())
}

#[test]
fn streamed_digest_refuses_short_streams_and_disorder() -> Result<(), Box<dyn std::error::Error>> {
    let scope = scope();
    let first = doc();
    let mut short = SnapshotDigestBuilder::new(scope, 0, 2);
    short.push(&first)?;
    assert_eq!(short.finish(), Err(ModelError::SnapshotDescriptorMismatch));
    let mut disordered = SnapshotDigestBuilder::new(scope, 0, 2);
    disordered.push(&first)?;
    assert_eq!(
        disordered.push(&first),
        Err(ModelError::SnapshotDocumentsUnordered)
    );
    Ok(())
}

#[test]
fn artifact_accepts_a_baseline_larger_than_one_page() -> Result<(), Box<dyn std::error::Error>> {
    let declared = u64::try_from(MAX_BASELINE_PAGE_DOCUMENTS)?;
    let documents = (0..=declared)
        .map(|index| {
            SnapshotDocument::new(
                DocumentId::from_uuid(Uuid::from(u128::from(index).to_be_bytes())),
                vec![7],
                false,
            )
        })
        .collect::<Vec<_>>();
    let artifact = SnapshotArtifact::new(scope(), 3, documents)?;
    assert_eq!(
        usize::try_from(artifact.baseline().document_count())?,
        MAX_BASELINE_PAGE_DOCUMENTS.saturating_add(1)
    );
    Ok(())
}
