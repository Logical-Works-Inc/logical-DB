//! Episode-to-document semantics journey without daemon or network.
//!
//! Builds one [`Episode`] in account/space [`Uuid`] scope, appends it to an
//! [`OriginLog`], derives the Cairn [`Observation`] from its [`EpisodeId`],
//! persists the CBOR snapshot bytes as a facade [`Database`] document, then
//! reloads and re-verifies identity, digest, and the [`LocalWorldRecord`]
//! evidence-subset guard after reopen.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
//! S33 S34 S35 S36 S37 S38 S39. Source URLs are in `evidence/SOURCES.md`.

#![forbid(unsafe_code)]

use std::fs;

use ciborium::value::Value;
use logical_db::{
    AccountId, Database, DocumentId, Item, Object, OperationDigest, OperationId, ReplicaId, Scalar,
    Scope, SpaceId,
};
use logical_db_cairn::{Actionability, EpisodeRef, Modality, Observation, TokenKind};
use logical_db_episode::{
    Episode, EpisodeError, EpisodeId, EpisodeTime, LocalWorldRecord, OriginLog, content_id,
};
use uuid::Uuid;

#[test]
fn episode_snapshot_roundtrips_through_facade_document_with_evidence_guard() {
    let account = Uuid::now_v7();
    let space = Uuid::now_v7();
    let payload = b"semantics journey payload".to_vec();

    let episode = Episode::new(
        0,
        TokenKind::Event,
        EpisodeTime::default(),
        Vec::new(),
        Vec::new(),
        payload.clone(),
        Some((account, space)),
    )
    .unwrap();
    assert_eq!(episode.id.into_bytes(), content_id(&episode));

    let mut log = OriginLog::default();
    let id = log.append(episode.clone()).unwrap();
    assert_eq!(id, episode.id);
    log.verify_log().unwrap();

    let reference = EpisodeRef::try_from(id).unwrap();
    let observation = Observation {
        id: reference,
        kind: episode.kind,
        modality: Modality::Text,
        uncertainty: 0.1,
        actionability: Actionability::Inspect,
        value: episode.payload.clone(),
        links: Vec::new(),
    };
    observation.validate().unwrap();
    assert_eq!(observation.content_hash(), observation.content_hash());

    let mut snapshot = Vec::new();
    ciborium::into_writer(&episode, &mut snapshot).unwrap();

    let path = std::env::temp_dir().join(format!("logical-db-semantics-{}.redb", Uuid::now_v7()));
    let scope = Scope::new(AccountId::from_uuid(account), SpaceId::from_uuid(space));
    let replica = ReplicaId::new();
    let document = DocumentId::new();
    let database = Database::open(&path, scope, replica).unwrap();
    database
        .edit(
            document,
            OperationId::new(),
            OperationDigest::new([0x5E; 32]),
            |editor| editor.put(&Object::root(), "snapshot", Scalar::Bytes(snapshot.clone())),
        )
        .unwrap();

    let stored = database
        .read(document, |reader| reader.get(&Object::root(), "snapshot"))
        .unwrap();
    let Some(Some(Item::Scalar(Scalar::Bytes(bytes)))) = stored else {
        panic!("facade must return the stored snapshot bytes");
    };
    let reloaded: Episode = ciborium::from_reader(bytes.as_slice()).unwrap();
    assert_eq!(reloaded, episode);
    reloaded.validate().unwrap();

    let via_facade = logical_db::logical_db_cairn::EpisodeRef::try_from(id).unwrap();
    assert_eq!(reference, via_facade);

    let record = logical_db::logical_db_episode::LocalWorldRecord::new(
        0,
        vec![id],
        vec![id],
        vec![id],
        Vec::new(),
        b"state".to_vec(),
        Some((account, space)),
    )
    .unwrap();
    assert_eq!(record.source_heads, vec![id]);
    let uncovered = LocalWorldRecord::new(
        0,
        vec![id],
        vec![id],
        vec![EpisodeId::new([9u8; 32]).unwrap()],
        Vec::new(),
        b"state".to_vec(),
        Some((account, space)),
    );
    assert_eq!(uncovered, Err(EpisodeError::UnprovenReference));

    drop(database);
    let reopened = Database::open(&path, scope, replica).unwrap();
    let stored = reopened
        .read(document, |reader| reader.get(&Object::root(), "snapshot"))
        .unwrap();
    let Some(Some(Item::Scalar(Scalar::Bytes(bytes)))) = stored else {
        panic!("snapshot must survive reopen");
    };
    let reloaded: Episode = ciborium::from_reader(bytes.as_slice()).unwrap();
    assert_eq!(reloaded.id, id);
    reloaded.validate().unwrap();

    drop(reopened);
    fs::remove_file(path).unwrap();
}

#[test]
fn semantic_cbor_rejects_zero_ids_unknown_versions_and_forged_indexes() {
    let mut zero = Vec::new();
    ciborium::into_writer(&[0u8; 32], &mut zero).unwrap();
    assert!(ciborium::from_reader::<EpisodeId, _>(zero.as_slice()).is_err());
    assert!(ciborium::from_reader::<EpisodeRef, _>(zero.as_slice()).is_err());

    let episode = Episode::new(
        0,
        TokenKind::Event,
        EpisodeTime::default(),
        Vec::new(),
        Vec::new(),
        b"versioned snapshot".to_vec(),
        None,
    )
    .unwrap();
    let mut encoded = Vec::new();
    ciborium::into_writer(&episode, &mut encoded).unwrap();
    let mut value: Value = ciborium::from_reader(encoded.as_slice()).unwrap();
    let Value::Map(fields) = &mut value else {
        panic!("episode snapshot must be a CBOR map");
    };
    let version = fields
        .iter_mut()
        .find(|(key, _)| key == &Value::Text("version".into()))
        .unwrap();
    version.1 = Value::Integer(2.into());
    encoded.clear();
    ciborium::into_writer(&value, &mut encoded).unwrap();
    assert!(ciborium::from_reader::<Episode, _>(encoded.as_slice()).is_err());

    let mut log = OriginLog::default();
    log.append(episode.clone()).unwrap();
    let mut valid = Vec::new();
    ciborium::into_writer(&log, &mut valid).unwrap();
    let rebuilt: OriginLog = ciborium::from_reader(valid.as_slice()).unwrap();
    assert_eq!(rebuilt.get(episode.id), Ok(&episode));
    let mut forged: Value = ciborium::from_reader(valid.as_slice()).unwrap();
    let Value::Map(fields) = &mut forged else {
        panic!("origin-log snapshot must be a CBOR map");
    };
    fields.push((Value::Text("index".into()), Value::Map(Vec::new())));
    let mut forged_bytes = Vec::new();
    ciborium::into_writer(&forged, &mut forged_bytes).unwrap();
    assert!(ciborium::from_reader::<OriginLog, _>(forged_bytes.as_slice()).is_err());

    let valid_log: Value = ciborium::from_reader(valid.as_slice()).unwrap();
    let Value::Map(log_fields) = valid_log else {
        panic!("origin-log snapshot must be a CBOR map");
    };
    let (_, Value::Array(episodes)) = log_fields
        .into_iter()
        .find(|(key, _)| key == &Value::Text("episodes".into()))
        .unwrap()
    else {
        panic!("origin-log snapshot must carry episodes");
    };
    let hostile = Value::Map(vec![
        (Value::Text("version".into()), Value::Integer(1.into())),
        (
            Value::Text("episodes".into()),
            Value::Array(vec![episodes[0].clone(); 257]),
        ),
    ]);
    let mut hostile_bytes = Vec::new();
    ciborium::into_writer(&hostile, &mut hostile_bytes).unwrap();
    assert!(ciborium::from_reader::<OriginLog, _>(hostile_bytes.as_slice()).is_err());
}
