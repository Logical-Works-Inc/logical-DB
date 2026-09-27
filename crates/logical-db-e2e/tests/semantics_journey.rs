//! Episode-to-document semantics journey without daemon or network.
//!
//! Builds one [`Episode`] in account/space [`Uuid`] scope, appends it to an
//! [`OriginLog`], derives the Cairn [`Observation`] from its [`EpisodeId`],
//! persists the CBOR snapshot bytes as a facade [`Database`] document, then
//! reloads and re-verifies identity, digest, and the [`LocalWorldRecord`]
//! evidence-subset guard after reopen. This is a local semantic-port journey,
//! not proof of consumption by canonical LEM ORIGIN storage.
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

/// The episode snapshot roundtrips through a facade document under the guard.
#[test]
fn episode_snapshot_roundtrips_through_facade_document_with_evidence_guard()
-> Result<(), Box<dyn std::error::Error>> {
    let account = AccountId::new();
    let space = SpaceId::new();
    let payload = b"semantics journey payload".to_vec();

    let episode = Episode::new(
        0,
        TokenKind::Event,
        EpisodeTime::default(),
        Vec::new(),
        Vec::new(),
        payload,
        Some((account.as_bytes(), space.as_bytes())),
    )?;
    assert_eq!(
        episode.id.into_bytes(),
        content_id(&episode),
        "episode id must equal its content id"
    );

    let mut log = OriginLog::default();
    let id = log.append(episode.clone())?;
    assert_eq!(id, episode.id, "append must return the episode id");
    log.verify_log()?;

    let reference = EpisodeRef::try_from(id)?;
    let observation = Observation::new(
        reference,
        episode.kind,
        Modality::Text,
        0.1,
        Actionability::Inspect,
        episode.payload.clone(),
        Vec::new(),
    );
    observation.validate()?;
    assert_eq!(
        observation.content_hash(),
        observation.content_hash(),
        "observation content hash must be deterministic"
    );

    let mut snapshot = Vec::new();
    ciborium::into_writer(&episode, &mut snapshot)?;

    let path = std::env::temp_dir().join(format!(
        "logical-db-semantics-{}.redb",
        DocumentId::unique_suffix()
    ));
    let scope = Scope::new(account, space);
    let replica = ReplicaId::new();
    let document = DocumentId::new();
    let database = Database::open(&path, scope, replica)?;
    database.edit(
        document,
        OperationId::new(),
        OperationDigest::new([0x5E; 32]),
        |editor| editor.put(&Object::root(), "snapshot", Scalar::Bytes(snapshot.clone())),
    )?;

    let stored = database.read(document, |reader| reader.get(&Object::root(), "snapshot"))?;
    let Some(Some(Item::Scalar(Scalar::Bytes(bytes)))) = stored else {
        return Err("facade must return the stored snapshot bytes".into());
    };
    let reloaded: Episode = ciborium::from_reader(bytes.as_slice())?;
    assert_eq!(reloaded, episode, "reloaded episode must equal original");
    reloaded.validate()?;

    let via_facade = logical_db::logical_db_cairn::EpisodeRef::try_from(id)?;
    assert_eq!(reference, via_facade, "facade EpisodeRef must match");

    let record = logical_db::logical_db_episode::LocalWorldRecord::new(
        0,
        vec![id],
        vec![id],
        vec![id],
        Vec::new(),
        b"state".to_vec(),
        Some((account.as_bytes(), space.as_bytes())),
    )?;
    assert_eq!(
        record.source_heads,
        vec![id],
        "record source heads must hold the episode"
    );
    let uncovered = LocalWorldRecord::new(
        0,
        vec![id],
        vec![id],
        vec![EpisodeId::new([9u8; 32])?],
        Vec::new(),
        b"state".to_vec(),
        Some((account.as_bytes(), space.as_bytes())),
    );
    assert_eq!(
        uncovered,
        Err(EpisodeError::UnprovenReference),
        "uncovered reference must be refused"
    );

    drop(database);
    let reopened = Database::open(&path, scope, replica)?;
    let stored = reopened.read(document, |reader| reader.get(&Object::root(), "snapshot"))?;
    let Some(Some(Item::Scalar(Scalar::Bytes(bytes)))) = stored else {
        return Err("snapshot must survive reopen".into());
    };
    let reloaded: Episode = ciborium::from_reader(bytes.as_slice())?;
    assert_eq!(reloaded.id, id, "reopened snapshot must keep its id");
    reloaded.validate()?;

    drop(reopened);
    fs::remove_file(path)?;
    Ok(())
}

/// Semantic CBOR rejects zero ids, unknown versions, and forged indexes.
#[test]
fn semantic_cbor_rejects_zero_ids_unknown_versions_and_forged_indexes()
-> Result<(), Box<dyn std::error::Error>> {
    let mut zero = Vec::new();
    ciborium::into_writer(&[0u8; 32], &mut zero)?;
    assert!(
        ciborium::from_reader::<EpisodeId, _>(zero.as_slice()).is_err(),
        "zero id must be rejected as EpisodeId"
    );
    assert!(
        ciborium::from_reader::<EpisodeRef, _>(zero.as_slice()).is_err(),
        "zero id must be rejected as EpisodeRef"
    );

    let episode = Episode::new(
        0,
        TokenKind::Event,
        EpisodeTime::default(),
        Vec::new(),
        Vec::new(),
        b"versioned snapshot".to_vec(),
        None,
    )?;
    let mut encoded = Vec::new();
    ciborium::into_writer(&episode, &mut encoded)?;
    let mut value: Value = ciborium::from_reader(encoded.as_slice())?;
    let Value::Map(ref mut fields) = value else {
        return Err("episode snapshot must be a CBOR map".into());
    };
    let version = fields
        .iter_mut()
        .find(|entry| entry.0 == Value::Text("version".into()))
        .ok_or("episode snapshot must carry a version field")?;
    version.1 = Value::Integer(2.into());
    encoded.clear();
    ciborium::into_writer(&value, &mut encoded)?;
    assert!(
        ciborium::from_reader::<Episode, _>(encoded.as_slice()).is_err(),
        "unknown episode version must be refused"
    );

    let mut log = OriginLog::default();
    log.append(episode.clone())?;
    let mut valid = Vec::new();
    ciborium::into_writer(&log, &mut valid)?;
    let rebuilt: OriginLog = ciborium::from_reader(valid.as_slice())?;
    assert_eq!(
        rebuilt.get(episode.id),
        Ok(&episode),
        "rebuilt log must return the appended episode"
    );
    let mut forged: Value = ciborium::from_reader(valid.as_slice())?;
    let Value::Map(ref mut fields) = forged else {
        return Err("origin-log snapshot must be a CBOR map".into());
    };
    fields.push((Value::Text("index".into()), Value::Map(Vec::new())));
    let mut forged_bytes = Vec::new();
    ciborium::into_writer(&forged, &mut forged_bytes)?;
    assert!(
        ciborium::from_reader::<OriginLog, _>(forged_bytes.as_slice()).is_err(),
        "forged index field must be refused"
    );

    let valid_log: Value = ciborium::from_reader(valid.as_slice())?;
    let Value::Map(log_fields) = valid_log else {
        return Err("origin-log snapshot must be a CBOR map".into());
    };
    let (_, Value::Array(episodes)) = log_fields
        .into_iter()
        .find(|entry| entry.0 == Value::Text("episodes".into()))
        .ok_or("origin-log snapshot must carry episodes")?
    else {
        return Err("episodes field must be a CBOR array".into());
    };
    let first = episodes.first().ok_or("episodes array must not be empty")?;
    let hostile = Value::Map(vec![
        (Value::Text("version".into()), Value::Integer(1.into())),
        (
            Value::Text("episodes".into()),
            Value::Array(vec![first.clone(); 257]),
        ),
    ]);
    let mut hostile_bytes = Vec::new();
    ciborium::into_writer(&hostile, &mut hostile_bytes)?;
    assert!(
        ciborium::from_reader::<OriginLog, _>(hostile_bytes.as_slice()).is_err(),
        "oversized episode array must be refused"
    );
    Ok(())
}
