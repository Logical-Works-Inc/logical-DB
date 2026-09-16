//! Wire fidelity: episode aggregates must survive the estate derive migration.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.
//!
//! Vectors captured 2026-09-16 at the estate hash swap, when content identity
//! moved from the retired SHA-2 derivation (pinned at commit `0309c1a`) to
//! BLAKE3 through `lgwks_std::hash`, matching the LEM SOT's own `blake3`
//! demand. `Episode::id` is content-derived and serialized, so each vector
//! also pins the digest derivation and every CBOR map key the derives emit.

use logical_db_episode::{Episode, EpisodeId};

/// Decodes a lowercase hex literal without reusing the code under test.
fn from_hex(literal: &str) -> Vec<u8> {
    literal
        .as_bytes()
        .chunks(2)
        .map(|pair| {
            let text = std::str::from_utf8(pair).expect("hex ascii");
            u8::from_str_radix(text, 16).expect("hex digit")
        })
        .collect()
}

fn encode(value: &impl lgwks_std::json::Serialize) -> Vec<u8> {
    let mut wire = Vec::new();
    ciborium::into_writer(value, &mut wire).expect("cbor encode");
    wire
}

#[test]
fn a_pre_migration_episode_round_trips_byte_for_byte() {
    let wire = from_hex(concat!(
        "a96776657273696f6e01626964982018831863182b18f218b805188518331820",
        "18f6187118b2131885189d18aa1884184b182e18b40518e5182d1882185d0018",
        "8118d8185918ad183418a86373657107646b696e64656576656e746573636f70",
        "6582500101010101010101010101010101010150020202020202020202020202",
        "020202026474696d65a7686f6273657276656400686f63637572726564006a76",
        "616c69645f66726f6d006876616c69645f746f00687265636f72646564006764",
        "657269766564006873657175656e6365006865766964656e6365819820010101",
        "0101010101010101010101010101010101010101010101010101010101677061",
        "72656e7473819820020202020202020202020202020202020202020202020202",
        "0202020202020202677061796c6f6164881873186e1861187018731868186f18",
        "74",
    ));
    let decoded: Episode = ciborium::from_reader(wire.as_slice()).expect("decode episode");
    assert_eq!(decoded.seq, 7);
    assert_eq!(decoded.payload, b"snapshot".to_vec());
    assert_eq!(decoded.evidence, vec![EpisodeId::new([1; 32]).expect("id")]);
    assert_eq!(decoded.parents, vec![EpisodeId::new([2; 32]).expect("id")]);
    assert_eq!(
        decoded.id.into_bytes().as_slice(),
        from_hex("83632bf2b805853320f671b213859daa844b2eb405e52d825d0081d859ad34a8").as_slice(),
        "content-derived identity drifted"
    );
    assert_eq!(encode(&decoded), wire, "episode wire drifted");
}

#[test]
fn a_pre_migration_empty_episode_round_trips_byte_for_byte() {
    let wire = from_hex(concat!(
        "a96776657273696f6e01626964982018cd1885182518bf182718340004186318",
        "5c18191839184c185918361887189318f41839185a0218961879183e18b81837",
        "18680718b018d718fd18426373657100646b696e6466656e746974796573636f",
        "7065f66474696d65a7686f6273657276656400686f63637572726564006a7661",
        "6c69645f66726f6d006876616c69645f746f00687265636f7264656400676465",
        "7269766564006873657175656e6365006865766964656e63658067706172656e",
        "747380677061796c6f616480",
    ));
    let decoded: Episode = ciborium::from_reader(wire.as_slice()).expect("decode episode");
    assert_eq!(decoded.seq, 0);
    assert_eq!(decoded.kind.as_str(), "entity");
    assert!(decoded.scope.is_none());
    assert!(decoded.evidence.is_empty());
    assert!(decoded.payload.is_empty());
    assert_eq!(encode(&decoded), wire, "empty episode wire drifted");
}
