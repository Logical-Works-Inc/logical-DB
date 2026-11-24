use std::error::Error;

use super::*;

/// Extracts the sole change produced by the enclosing edit, failing loudly
/// when the edit committed a different number of changes.
fn only_change(changes: Vec<EncodedChange>) -> Result<EncodedChange, Box<dyn Error>> {
    let mut iter = changes.into_iter();
    let change = iter.next().ok_or("edit committed no changes")?;
    if iter.next().is_some() {
        return Err("edit committed more than one change".into());
    }
    Ok(change)
}

/// Builds one immutable change for the `value` property.
fn one_change() -> Result<EncodedChange, Box<dyn Error>> {
    let mut document = Document::new();
    let ((), changes) =
        document.edit(|editor| editor.put(&Object::root(), "value", Scalar::Integer(1)))?;
    only_change(changes)
}

#[test]
fn concurrent_fields_converge_independent_of_delivery_order() -> Result<(), Box<dyn Error>> {
    let mut origin = Document::new();
    origin.edit(|editor| editor.put(&Object::root(), "title", Scalar::String("base".into())))?;
    let snapshot = origin.save()?;
    let mut first = Document::load(&snapshot)?;
    let mut second = Document::load(&snapshot)?;

    let ((), first_changes) =
        first.edit(|editor| editor.put(&Object::root(), "left", Scalar::Integer(1)))?;
    let ((), second_changes) =
        second.edit(|editor| editor.put(&Object::root(), "right", Scalar::Integer(2)))?;

    first.apply(&second_changes)?;
    second.apply(&first_changes)?;
    let mut first_heads = first.heads();
    let mut second_heads = second.heads();
    first_heads.sort_unstable();
    second_heads.sort_unstable();
    assert_eq!(first_heads, second_heads, "replicas must share a frontier");
    assert_eq!(
        first.get(&Object::root(), "left")?,
        second.get(&Object::root(), "left")?,
        "converged left value must match"
    );
    assert_eq!(
        first.get(&Object::root(), "right")?,
        second.get(&Object::root(), "right")?,
        "converged right value must match"
    );
    Ok(())
}

#[test]
fn duplicate_change_delivery_is_idempotent() -> Result<(), Box<dyn Error>> {
    let mut source = Document::new();
    let ((), changes) =
        source.edit(|editor| editor.put(&Object::root(), "value", Scalar::Integer(7)))?;
    let mut target = Document::new();
    target.apply(&changes)?;
    let once = target.save()?;
    target.apply(&changes)?;
    assert_eq!(
        target.save()?,
        once,
        "re-applying a change must not alter the snapshot"
    );
    Ok(())
}

#[test]
fn concurrent_same_property_values_remain_explicitly_inspectable() -> Result<(), Box<dyn Error>> {
    let mut origin = Document::new();
    origin.edit(|editor| editor.put(&Object::root(), "status", Scalar::String("base".into())))?;
    let snapshot = origin.save()?;
    let mut first = Document::load(&snapshot)?;
    let mut second = Document::load(&snapshot)?;
    let first_change = first
        .edit(|editor| editor.put(&Object::root(), "status", Scalar::String("first".into())))?
        .1;
    let second_change = second
        .edit(|editor| editor.put(&Object::root(), "status", Scalar::String("second".into())))?
        .1;
    first.apply(&second_change)?;
    second.apply(&first_change)?;
    let mut values = first.reader().conflicts(&Object::root(), "status")?;
    values.sort_by(|left, right| left.causal_id.cmp(&right.causal_id));
    assert_eq!(values.len(), 2, "both concurrent authors must be retained");
    assert!(
        values.iter().all(|value| !value.causal_id.is_empty()),
        "every retained value must carry a causal identity"
    );
    assert!(
        values
            .iter()
            .any(|value| { value.value == Item::Scalar(Scalar::String("first".into())) }),
        "the first concurrent value must remain inspectable"
    );
    assert!(
        values
            .iter()
            .any(|value| { value.value == Item::Scalar(Scalar::String("second".into())) }),
        "the second concurrent value must remain inspectable"
    );
    Ok(())
}

#[test]
fn failed_editor_operation_rolls_back_every_staged_mutation() -> Result<(), Box<dyn Error>> {
    let mut document = Document::new();
    assert!(
        matches!(
            document.edit::<()>(|editor| {
                editor.put(&Object::root(), "leaked", Scalar::Integer(1))?;
                Err(CrdtError::InvalidProperty)
            }),
            Err(CrdtError::InvalidProperty)
        ),
        "a failed edit must surface its own error"
    );
    let ((), changes) =
        document.edit(|editor| editor.put(&Object::root(), "committed", Scalar::Integer(2)))?;
    assert_eq!(changes.len(), 1, "the retry must commit exactly one change");
    assert_eq!(
        document.get(&Object::root(), "leaked")?,
        None,
        "the rolled-back mutation must not be visible"
    );
    assert_eq!(
        document.get(&Object::root(), "committed")?,
        Some(Item::Scalar(Scalar::Integer(2))),
        "the committed mutation must be visible"
    );
    Ok(())
}

#[test]
fn apply_rejects_oversized_change_before_decoding() {
    let mut document = Document::new();
    let oversized = EncodedChange {
        digest: ChangeDigest::new([0; 32]),
        bytes: vec![0; MAX_CHANGE_BYTES + 1],
    };
    assert!(
        matches!(
            document.apply(&[oversized]),
            Err(CrdtError::InvalidChangeSize(size)) if size == MAX_CHANGE_BYTES + 1
        ),
        "an oversized change must be rejected with its actual length"
    );
}

#[test]
fn snapshots_changes_heads_and_reader_validate_their_exact_boundaries() -> Result<(), Box<dyn Error>>
{
    let change = one_change()?;
    assert!(change.validate().is_ok(), "a real change must validate");
    assert!(
        matches!(
            EncodedChange {
                digest: change.digest,
                bytes: vec![]
            }
            .validate(),
            Err(CrdtError::InvalidChangeSize(0))
        ),
        "an empty change must be rejected as size zero"
    );
    let mut forged = change;
    forged.digest = ChangeDigest::new([99; 32]);
    assert!(
        matches!(forged.validate(), Err(CrdtError::DigestMismatch)),
        "a forged digest must be refused"
    );
    let malformed = EncodedChange {
        digest: ChangeDigest::new([0; 32]),
        bytes: vec![1],
    };
    assert!(
        matches!(malformed.validate(), Err(CrdtError::ChangeDecode(_))),
        "malformed bytes must fail decoding"
    );

    let mut source = Document::new();
    source.edit(|editor| editor.put(&Object::root(), "answer", Scalar::Integer(42)))?;
    assert!(
        !source.heads().is_empty(),
        "an edit must advance the frontier"
    );
    let snapshot = source.save()?;
    assert!(!snapshot.is_empty(), "a saved snapshot must carry bytes");
    let mut restored = Document::load(&snapshot)?;
    assert_eq!(
        restored.heads(),
        source.heads(),
        "the restored frontier must match the source"
    );
    assert_eq!(
        restored.reader().get(&Object::root(), "answer")?,
        Some(Item::Scalar(Scalar::Integer(42))),
        "the restored scalar must match the source"
    );
    assert!(
        Document::load(b"not-an-automerge-snapshot").is_err(),
        "a non-snapshot must be refused"
    );
    Ok(())
}

#[test]
fn editor_delete_insert_and_each_object_kind_change_materialized_state()
-> Result<(), Box<dyn Error>> {
    let mut document = Document::new();
    let kinds = [
        ("map", ObjectKind::Map),
        ("table", ObjectKind::Table),
        ("list", ObjectKind::List),
        ("text", ObjectKind::Text),
    ];
    let ((), changes) = document.edit(|editor| {
        editor.put(&Object::root(), "remove", Scalar::Boolean(true))?;
        for (property, kind) in kinds {
            editor.put_object(&Object::root(), property, kind)?;
        }
        Ok(())
    })?;
    assert_eq!(changes.len(), 1, "one edit must commit exactly one change");
    for (property, expected) in kinds {
        assert!(
            matches!(
                document.reader().get(&Object::root(), property)?,
                Some(Item::Object { kind, .. }) if kind == expected
            ),
            "property {property} must materialize as {expected:?}"
        );
    }
    let Some(Item::Object { handle: list, .. }) = document.get(&Object::root(), "list")? else {
        return Err("list object expected".into());
    };
    let ((), changes) = document.edit(|editor| editor.delete(&Object::root(), "remove"))?;
    assert_eq!(changes.len(), 1, "the delete must commit one change");
    assert_eq!(
        document.get(&Object::root(), "remove")?,
        None,
        "the deleted property must be absent"
    );
    let ((), changes) =
        document.edit(|editor| editor.insert(&list, 0, Scalar::String("first".into())))?;
    assert_eq!(changes.len(), 1, "the insert must commit one change");
    Ok(())
}

#[test]
fn lists_text_counters_and_discovery_are_available_through_stable_api() -> Result<(), Box<dyn Error>>
{
    let mut document = Document::new();
    let ((), changes) = document.edit(|editor| {
        let list = editor.put_object(&Object::root(), "items", ObjectKind::List)?;
        editor.insert(&list, 0, Scalar::String("old".into()))?;
        editor.put_index(&list, 0, Scalar::String("new".into()))?;
        editor.insert_object(&list, 1, ObjectKind::Map)?;
        editor.delete_index(&list, 1)?;
        editor.put(&Object::root(), "count", Scalar::Counter(1))?;
        editor.increment(&Object::root(), "count", 2)?;
        let text = editor.put_object(&Object::root(), "body", ObjectKind::Text)?;
        editor.splice_text(&text, 0, 0, "héllo")?;
        Ok(())
    })?;
    assert_eq!(changes.len(), 1, "one edit must commit exactly one change");
    let Some(Item::Object { handle: list, .. }) = document.get(&Object::root(), "items")? else {
        return Err("list expected".into());
    };
    let Some(Item::Object { handle: text, .. }) = document.get(&Object::root(), "body")? else {
        return Err("text expected".into());
    };
    let mut reader = document.reader();
    assert!(
        reader.keys(&Object::root()).contains(&"items".to_owned()),
        "the root must expose the items key"
    );
    assert_eq!(reader.len(&list), 1, "the list must hold one element");
    assert!(!reader.is_empty(&list), "the list must not be empty");
    assert_eq!(
        reader.get_index(&list, 0)?,
        Some(Item::Scalar(Scalar::String("new".into()))),
        "put_index must replace the visible element"
    );
    assert_eq!(
        reader.text(&text)?,
        "héllo",
        "spliced text must round-trip by code point"
    );
    assert_eq!(
        reader.get(&Object::root(), "count")?,
        Some(Item::Scalar(Scalar::Counter(3))),
        "an incremented counter must read back at its new value"
    );
    Ok(())
}

#[test]
fn property_and_scalar_portability_rules_accept_edges_and_reject_violations() {
    assert!(!is_nonempty_bounded(0, MAX_CHANGE_BYTES), "zero is empty");
    assert!(
        is_nonempty_bounded(MAX_CHANGE_BYTES, MAX_CHANGE_BYTES),
        "the exact maximum is accepted"
    );
    assert!(
        !is_nonempty_bounded(MAX_CHANGE_BYTES + 1, MAX_CHANGE_BYTES),
        "one above the maximum is rejected"
    );
    assert!(
        is_at_most(MAX_CHANGES_PER_PAGE, MAX_CHANGES_PER_PAGE),
        "the exact maximum is accepted"
    );
    assert!(
        !is_at_most(MAX_CHANGES_PER_PAGE + 1, MAX_CHANGES_PER_PAGE),
        "one above the maximum is rejected"
    );
    let maximum_property = "p".repeat(512);
    assert!(
        validate_property(&maximum_property).is_ok(),
        "a 512-byte property is accepted"
    );
    assert!(
        matches!(validate_property(""), Err(CrdtError::InvalidProperty)),
        "an empty property is rejected"
    );
    assert!(
        matches!(
            validate_property(&format!("{maximum_property}p")),
            Err(CrdtError::InvalidProperty)
        ),
        "a 513-byte property is rejected"
    );
    for scalar in [
        Scalar::Number(f64::NAN),
        Scalar::Number(f64::INFINITY),
        Scalar::Number(f64::NEG_INFINITY),
    ] {
        assert!(
            matches!(validate_scalar(&scalar), Err(CrdtError::NonFiniteNumber)),
            "non-finite numbers must be rejected"
        );
    }
    for scalar in [
        Scalar::Number(1.25),
        Scalar::String("small".into()),
        Scalar::Bytes(vec![1]),
    ] {
        assert!(
            validate_scalar(&scalar).is_ok(),
            "portable scalars must be accepted"
        );
    }
    assert!(
        validate_scalar(&Scalar::String("x".repeat(MAX_CHANGE_BYTES))).is_ok(),
        "the exact maximum string length is accepted"
    );
    assert!(
        validate_scalar(&Scalar::Bytes(vec![1; MAX_CHANGE_BYTES])).is_ok(),
        "the exact maximum byte payload is accepted"
    );
    assert!(
        matches!(
            validate_scalar(&Scalar::String("x".repeat(MAX_CHANGE_BYTES + 1))),
            Err(CrdtError::ScalarTooLarge(_))
        ),
        "an oversize string must be rejected"
    );
    assert!(
        matches!(
            validate_scalar(&Scalar::Bytes(vec![1; MAX_CHANGE_BYTES + 1])),
            Err(CrdtError::ScalarTooLarge(_))
        ),
        "an oversize byte payload must be rejected"
    );
    assert!(
        matches!(
            validate_scalar(&Scalar::Unknown {
                type_code: 7,
                bytes: vec![]
            }),
            Err(CrdtError::UnknownScalarWrite)
        ),
        "an unknown scalar must be read-only"
    );
}

#[test]
fn snapshot_decoder_rejects_empty_and_oversized_input_before_autoconstruction() {
    assert!(
        matches!(Document::load(&[]), Err(CrdtError::InvalidSnapshotSize(0))),
        "an empty snapshot must be rejected as size zero"
    );
    assert!(
        matches!(
            Document::load(&vec![0; MAX_SNAPSHOT_BYTES + 1]),
            Err(CrdtError::InvalidSnapshotSize(size)) if size == MAX_SNAPSHOT_BYTES + 1
        ),
        "an oversized snapshot must be rejected with its actual length"
    );
}

#[test]
fn apply_count_boundary_accepts_exact_maximum_and_rejects_successor() -> Result<(), Box<dyn Error>>
{
    let change = one_change()?;
    let exact = vec![change.clone(); MAX_CHANGES_PER_PAGE];
    let mut document = Document::new();
    document.apply(&exact)?;
    let excessive = vec![change; MAX_CHANGES_PER_PAGE + 1];
    assert!(
        matches!(
            document.apply(&excessive),
            Err(CrdtError::ChangePageTooLarge)
        ),
        "one above the count maximum must be rejected"
    );
    Ok(())
}

#[test]
fn reserved_tombstone_property_is_rejected_for_application_writes() -> Result<(), Box<dyn Error>> {
    let mut document = Document::new();
    assert!(
        matches!(
            document.edit(|editor| {
                editor.put(&Object::root(), TOMBSTONE_PROPERTY, Scalar::Boolean(true))
            }),
            Err(CrdtError::InvalidProperty)
        ),
        "put must refuse the reserved property"
    );
    assert!(
        matches!(
            document.edit(|editor| editor.delete(&Object::root(), TOMBSTONE_PROPERTY)),
            Err(CrdtError::InvalidProperty)
        ),
        "delete must refuse the reserved property"
    );
    assert!(
        matches!(
            document.edit(|editor| editor.put_object(
                &Object::root(),
                TOMBSTONE_PROPERTY,
                ObjectKind::Map
            )),
            Err(CrdtError::InvalidProperty)
        ),
        "put_object must refuse the reserved property"
    );
    assert!(
        matches!(
            document.edit(|editor| editor.increment(&Object::root(), TOMBSTONE_PROPERTY, 1)),
            Err(CrdtError::InvalidProperty)
        ),
        "increment must refuse the reserved property"
    );
    assert!(
        !document.is_deleted()?,
        "a refused write must not delete the document"
    );
    let ((), changes) =
        document.edit(|editor| editor.put(&Object::root(), "ok", Scalar::Integer(1)))?;
    assert_eq!(changes.len(), 1, "the allowed write must commit one change");
    Ok(())
}

#[test]
fn document_delete_marks_tombstone_with_exactly_one_change() -> Result<(), Box<dyn Error>> {
    let mut document = Document::new();
    assert!(!document.is_deleted()?, "a fresh document is not deleted");
    assert!(!document.reader().is_deleted()?, "the reader agrees");
    let changes = document.delete()?;
    assert_eq!(changes.len(), 1, "delete must commit exactly one change");
    assert!(document.is_deleted()?, "delete must set the tombstone");
    assert!(
        document.reader().is_deleted()?,
        "the reader must observe the tombstone"
    );
    let snapshot = document.save()?;
    let mut restored = Document::load(&snapshot)?;
    assert!(
        restored.is_deleted()?,
        "the tombstone must survive a snapshot round-trip"
    );
    assert!(
        restored.reader().is_deleted()?,
        "the restored reader must observe the tombstone"
    );
    let again = restored.delete()?;
    assert_eq!(
        again.len(),
        1,
        "a repeated delete must still commit exactly one change"
    );
    assert!(
        restored.is_deleted()?,
        "the repeated delete must keep the tombstone"
    );
    Ok(())
}

#[test]
fn tombstone_survives_merge_and_concurrent_edits_stay_inspectable() -> Result<(), Box<dyn Error>> {
    let mut origin = Document::new();
    origin.edit(|editor| editor.put(&Object::root(), "title", Scalar::String("base".into())))?;
    let snapshot = origin.save()?;
    let mut deleter = Document::load(&snapshot)?;
    let mut editor_side = Document::load(&snapshot)?;
    let tombstone_changes = deleter.delete()?;
    let ((), edit_changes) = editor_side
        .edit(|editor| editor.put(&Object::root(), "title", Scalar::String("rival".into())))?;
    deleter.apply(&edit_changes)?;
    editor_side.apply(&tombstone_changes)?;
    assert!(
        deleter.is_deleted()?,
        "the deleter must observe its tombstone"
    );
    assert!(
        editor_side.is_deleted()?,
        "the tombstone must propagate to the editing replica"
    );
    assert!(deleter.reader().is_deleted()?, "the deleter reader agrees");
    assert!(
        editor_side.reader().is_deleted()?,
        "the editing reader observes the merged tombstone"
    );
    for side in [&mut deleter, &mut editor_side] {
        let values = side.reader().conflicts(&Object::root(), "title")?;
        assert!(
            values
                .iter()
                .any(|value| value.value == Item::Scalar(Scalar::String("rival".into()))),
            "the concurrent edit must stay inspectable after deletion"
        );
    }
    Ok(())
}

#[test]
fn fresh_document_delete_applies_cleanly_to_an_empty_replica() -> Result<(), Box<dyn Error>> {
    let mut deleter = Document::new();
    let tombstone_changes = deleter.delete()?;
    let mut late = Document::new();
    assert!(!late.is_deleted()?, "the late replica starts undeleted");
    late.apply(&tombstone_changes)?;
    assert!(
        late.is_deleted()?,
        "applying the tombstone must delete the late replica"
    );
    Ok(())
}
