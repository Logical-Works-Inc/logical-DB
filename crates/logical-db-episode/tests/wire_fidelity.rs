//! Wire fidelity: episode aggregates must survive the estate derive migration.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.
//!
//! Vectors captured from the PRE-migration implementation at commit
//! `0309c1a`. `Episode::id` is content-derived and serialized, so each vector
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
        "a96776657273696f6e01626964982018de18d2186218ce181d18df18ad184018",
        "530e101820181b183118401851187d18da18300f18a4187a182a18e718b51318",
        "b60718aa184e18f518456373657107646b696e64656576656e746573636f7065",
        "8250010101010101010101010101010101015002020202020202020202020202",
        "0202026474696d65a7686f6273657276656400686f63637572726564006a7661",
        "6c69645f66726f6d006876616c69645f746f00687265636f7264656400676465",
        "7269766564006873657175656e6365006865766964656e636581982001010101",
        "0101010101010101010101010101010101010101010101010101010167706172",
        "656e747381982002020202020202020202020202020202020202020202020202",
        "02020202020202677061796c6f6164881873186e1861187018731868186f1874",
    ));
    let decoded: Episode = ciborium::from_reader(wire.as_slice()).expect("decode episode");
    assert_eq!(decoded.seq, 7);
    assert_eq!(decoded.payload, b"snapshot".to_vec());
    assert_eq!(decoded.evidence, vec![EpisodeId::new([1; 32]).expect("id")]);
    assert_eq!(decoded.parents, vec![EpisodeId::new([2; 32]).expect("id")]);
    assert_eq!(
        decoded.id.into_bytes().as_slice(),
        from_hex("ded262ce1ddfad40530e10201b3140517dda300fa47a2ae7b513b607aa4ef545").as_slice(),
        "content-derived identity drifted"
    );
    assert_eq!(encode(&decoded), wire, "episode wire drifted");
}

#[test]
fn a_pre_migration_empty_episode_round_trips_byte_for_byte() {
    let wire = from_hex(concat!(
        "a96776657273696f6e01626964982018e018df187618d11850187018ef181b18",
        "27186018ad1830182d1831187f18ef184f18c018a518d718cc18a8181c18bd18",
        "92186b18b11898186f18c118eb18cd6373657100646b696e6466656e74697479",
        "6573636f7065f66474696d65a7686f6273657276656400686f63637572726564",
        "006a76616c69645f66726f6d006876616c69645f746f00687265636f72646564",
        "006764657269766564006873657175656e6365006865766964656e6365806770",
        "6172656e747380677061796c6f616480",
    ));
    let decoded: Episode = ciborium::from_reader(wire.as_slice()).expect("decode episode");
    assert_eq!(decoded.seq, 0);
    assert_eq!(decoded.kind.as_str(), "entity");
    assert!(decoded.scope.is_none());
    assert!(decoded.evidence.is_empty());
    assert!(decoded.payload.is_empty());
    assert_eq!(encode(&decoded), wire, "empty episode wire drifted");
}
