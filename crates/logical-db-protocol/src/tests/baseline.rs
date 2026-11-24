//! Recovery baseline page, accumulator, and bound tests.

use logical_db_model::{
    MAX_BASELINE_BYTES, MAX_BASELINE_PAGE_DOCUMENTS, ModelError, SNAPSHOT_BASELINE_LEGACY_VERSION,
    SnapshotArtifact, SnapshotBaseline, SnapshotDocument,
};

use super::*;

/// Builds ordered state documents from `(snapshot, tombstone)` fixtures,
/// sorting by document identity so ordering is deterministic.
fn ordered_documents(snapshots: &[(Vec<u8>, bool)]) -> Vec<SnapshotDocument> {
    let mut documents = snapshots
        .iter()
        .map(|entry| SnapshotDocument::new(DocumentId::new(), entry.0.clone(), entry.1))
        .collect::<Vec<_>>();
    documents.sort_by_key(|document| *document.document().as_uuid().as_bytes());
    documents
}

/// Builds a valid single-page baseline carrying the supplied documents.
fn baseline_page(
    scope: Scope,
    position: u64,
    documents: Vec<SnapshotDocument>,
) -> Result<BaselinePage, Box<dyn Error>> {
    let artifact = SnapshotArtifact::new(scope, position, documents)?;
    Ok(BaselinePage {
        protocol: PROTOCOL_VERSION,
        descriptor: artifact.baseline().encode().to_vec(),
        documents: artifact
            .documents()
            .iter()
            .map(|document| {
                BaselineDocument::new(
                    document.document(),
                    document.is_tombstone(),
                    document.snapshot().to_vec(),
                )
            })
            .collect(),
        checkpoint: Checkpoint::from_wire(vec![7; 8])?,
        more: false,
        next_document: None,
    })
}

#[test]
fn baseline_page_round_trips_and_reproduces_its_descriptor() -> Result<(), Box<dyn Error>> {
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let artifact = SnapshotArtifact::new(
        scope,
        7,
        ordered_documents(&[(vec![1, 2, 3], false), (vec![9], true)]),
    )?;
    let page = baseline_page(scope, 7, artifact.documents().to_vec())?;
    let encoded = lgwks_std::json::to_vec(&page)?;
    let decoded: BaselinePage = lgwks_std::json::from_slice(&encoded)?;
    assert_eq!(
        decoded.validate(scope),
        Ok(artifact.clone()),
        "a JSON round-trip must reproduce the artifact"
    );
    let mut wire = Vec::new();
    ciborium::into_writer(&page, &mut wire)?;
    let decoded: BaselinePage = ciborium::from_reader(wire.as_slice())?;
    assert_eq!(
        decoded.validate(scope),
        Ok(artifact),
        "a CBOR round-trip must reproduce the artifact"
    );
    Ok(())
}

#[test]
fn baseline_page_refuses_substituted_truncated_reordered_and_legacy_artifacts()
-> Result<(), Box<dyn Error>> {
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let page = baseline_page(
        scope,
        3,
        ordered_documents(&[(vec![1, 2, 3], false), (vec![4], false)]),
    )?;

    let mut substituted = page.clone();
    let first = substituted
        .documents
        .first_mut()
        .ok_or("the fixture page carries documents")?;
    first.snapshot = vec![9, 9, 9];
    assert_eq!(
        substituted.validate(scope),
        Err(ProtocolError::BaselineMismatch),
        "substituted content must be refused"
    );

    let mut truncated = page.clone();
    truncated.documents.pop();
    assert_eq!(
        truncated.validate(scope),
        Err(ProtocolError::BaselineMismatch),
        "a truncated artifact must be refused"
    );

    let mut reordered = page.clone();
    reordered.documents.reverse();
    assert_eq!(
        reordered.validate(scope),
        Err(ProtocolError::RejectedDocuments(
            ModelError::SnapshotDocumentsUnordered
        )),
        "a reordered artifact must be refused"
    );

    let mut legacy = page;
    legacy.descriptor = [
        SNAPSHOT_BASELINE_LEGACY_VERSION.to_be_bytes().as_slice(),
        7_u64.to_be_bytes().as_slice(),
    ]
    .concat();
    assert_eq!(
        legacy.validate(scope),
        Err(ProtocolError::InvalidBaselineDescriptor),
        "a legacy descriptor must be refused"
    );
    Ok(())
}

#[test]
fn baseline_document_count_is_bounded_during_decode() -> Result<(), Box<dyn Error>> {
    let mut documents = Vec::new();
    for _ in 0..=MAX_BASELINE_PAGE_DOCUMENTS {
        documents.push(BaselineDocument {
            document: DocumentId::new(),
            tombstone: false,
            snapshot: vec![1],
        });
    }
    let page = BaselinePage {
        protocol: PROTOCOL_VERSION,
        descriptor: SnapshotBaseline::new(0, logical_db_model::BaselineDigest::new([0; 32]), 0, 0)
            .encode()
            .to_vec(),
        documents,
        checkpoint: Checkpoint::from_wire(vec![1])?,
        more: false,
        next_document: None,
    };
    let encoded = lgwks_std::json::to_vec(&page)?;
    assert!(
        lgwks_std::json::from_slice::<BaselinePage>(&encoded).is_err(),
        "a page above the per-page document bound must fail during decode"
    );
    Ok(())
}

#[test]
fn baseline_page_validator_refuses_typed_pages_over_its_bounds() -> Result<(), Box<dyn Error>> {
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let descriptor = SnapshotBaseline::new(0, logical_db_model::BaselineDigest::new([0; 32]), 0, 0)
        .encode()
        .to_vec();
    let checkpoint = Checkpoint::from_wire(vec![1])?;
    let counted = BaselinePage {
        protocol: PROTOCOL_VERSION,
        descriptor: descriptor.clone(),
        documents: (0..=MAX_BASELINE_PAGE_DOCUMENTS)
            .map(|_| BaselineDocument {
                document: DocumentId::new(),
                tombstone: false,
                snapshot: vec![1],
            })
            .collect(),
        checkpoint: checkpoint.clone(),
        more: false,
        next_document: None,
    };
    assert_eq!(
        counted.validate_page(scope),
        Err(ProtocolError::InvalidBaselineDocuments),
        "a page above the document count bound must be refused"
    );
    let half = MAX_BASELINE_BYTES.div_euclid(2).saturating_add(1);
    let oversized = BaselinePage {
        protocol: PROTOCOL_VERSION,
        descriptor,
        documents: vec![
            BaselineDocument {
                document: DocumentId::new(),
                tombstone: false,
                snapshot: vec![0; half],
            },
            BaselineDocument {
                document: DocumentId::new(),
                tombstone: false,
                snapshot: vec![0; half],
            },
        ],
        checkpoint,
        more: false,
        next_document: None,
    };
    assert_eq!(
        oversized.validate_page(scope),
        Err(ProtocolError::InvalidBaselineDocuments),
        "a page above the aggregate byte bound must be refused"
    );
    Ok(())
}

/// Builds `count` distinct, ascending state documents via the model
/// constructor so the descriptor can be reproduced.
fn ordered_states(count: usize) -> Result<Vec<SnapshotDocument>, Box<dyn Error>> {
    let mut documents = (0..count)
        .map(|index| {
            let payload = u8::try_from(index.rem_euclid(251))?.saturating_add(1);
            Ok(SnapshotDocument::new(
                DocumentId::new(),
                vec![payload],
                false,
            ))
        })
        .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
    documents.sort_by_key(|document| *document.document().as_uuid().as_bytes());
    Ok(documents)
}

/// Builds one page of an artifact from `slice`, advertising continuation.
fn paged_page(
    artifact: &SnapshotArtifact,
    slice: &[SnapshotDocument],
    more: bool,
) -> Result<BaselinePage, Box<dyn Error>> {
    Ok(BaselinePage {
        protocol: PROTOCOL_VERSION,
        descriptor: artifact.baseline().encode().to_vec(),
        documents: slice
            .iter()
            .map(|document| {
                BaselineDocument::new(
                    document.document(),
                    document.is_tombstone(),
                    document.snapshot().to_vec(),
                )
            })
            .collect(),
        checkpoint: Checkpoint::from_wire(vec![9; 8])?,
        more,
        next_document: if more {
            slice.last().map(SnapshotDocument::document)
        } else {
            None
        },
    })
}

#[test]
fn accumulator_folds_a_multi_page_baseline_and_refuses_gaps_and_shortfalls()
-> Result<(), Box<dyn Error>> {
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let documents = ordered_states(MAX_BASELINE_PAGE_DOCUMENTS.saturating_add(44))?;
    let artifact = SnapshotArtifact::new(scope, 11, documents.clone())?;
    let (first, rest) = documents.split_at(MAX_BASELINE_PAGE_DOCUMENTS);
    let first_page = paged_page(&artifact, first, true)?;
    let second_page = paged_page(&artifact, rest, false)?;
    let mut accumulator = BaselineAccumulator::new(scope, artifact.baseline().clone());
    assert!(
        !accumulator.push(&first_page)?,
        "a non-terminal page must report incomplete"
    );
    assert!(
        accumulator.push(&second_page)?,
        "the terminal page must report complete"
    );
    assert_eq!(
        accumulator.finish()?,
        artifact,
        "the folded artifact must match the original"
    );

    let mut gap = BaselineAccumulator::new(scope, artifact.baseline().clone());
    assert!(
        !gap.push(&first_page)?,
        "the first page must still be accepted"
    );
    assert_eq!(
        gap.push(&first_page),
        Err(ProtocolError::BaselineMismatch),
        "a replayed page must be refused"
    );

    let mut short = BaselineAccumulator::new(scope, artifact.baseline().clone());
    assert!(
        !short.push(&first_page)?,
        "the first page must still be accepted"
    );
    assert_eq!(
        short.finish(),
        Err(ProtocolError::BaselineMismatch),
        "an incomplete fold must be refused at finish"
    );
    Ok(())
}
