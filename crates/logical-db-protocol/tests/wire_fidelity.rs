//! Wire fidelity: the estate derive/hex migration must not move a single byte.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.
//!
//! Every vector below was captured from the PRE-migration implementation at
//! commit `0309c1a`, before `lgwks_std::json` derives replaced the direct
//! `serde` edge. Each test decodes the frozen bytes with the current derives
//! and re-encodes them, so a renamed, reordered, or re-typed field fails on
//! the exact byte comparison. CBOR is this estate's wire format; no JSON
//! serialization surface exists at this layer, so the derives' key naming is
//! pinned through the CBOR map keys carried in the same vectors.

use std::error::Error;

use logical_db_model::IndexValue;
use logical_db_protocol::{PROTOCOL_VERSION, PullRequest, PushRequest};

/// Decodes a lowercase hex literal without reusing the code under test.
fn from_hex(literal: &str) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut bytes = Vec::new();
    for pair in literal.as_bytes().chunks(2) {
        let text = std::str::from_utf8(pair)?;
        bytes.push(u8::from_str_radix(text, 16)?);
    }
    Ok(bytes)
}

/// Encodes one aggregate to CBOR without reusing the code under test.
fn encode(value: &impl lgwks_std::json::Serialize) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut wire = Vec::new();
    ciborium::into_writer(value, &mut wire)?;
    Ok(wire)
}

#[test]
fn index_values_keep_their_pre_migration_cbor_vectors() -> Result<(), Box<dyn Error>> {
    let vectors: [(IndexValue, &str); 6] = [
        (
            IndexValue::Boolean(true),
            "a2647479706567626f6f6c65616e6576616c7565f5",
        ),
        (
            IndexValue::Integer(-42),
            "a2647479706567696e74656765726576616c75653829",
        ),
        (
            IndexValue::Unsigned(4_294_967_296),
            "a2647479706568756e7369676e65646576616c75651b0000000100000000",
        ),
        (
            IndexValue::Text("ada\u{00e9}".to_owned()),
            "a2647479706564746578746576616c756565616461c3a9",
        ),
        (
            IndexValue::Bytes(vec![0, 255, 128]),
            "a264747970656562797465736576616c7565830018ff1880",
        ),
        (
            IndexValue::Timestamp(1_700_000_000_000),
            "a264747970656974696d657374616d706576616c75651b0000018bcfe56800",
        ),
    ];
    for (value, literal) in vectors {
        let expected = from_hex(literal)?;
        assert_eq!(encode(&value)?, expected, "re-encoded {value:?}");
        let decoded: IndexValue = ciborium::from_reader(expected.as_slice())?;
        assert_eq!(decoded, value, "the decoded value must match the source");
        assert_eq!(
            encode(&decoded)?,
            expected,
            "re-encoding a decoded value must reproduce the vector"
        );
    }
    Ok(())
}

#[test]
fn a_pre_migration_push_request_round_trips_byte_for_byte() -> Result<(), Box<dyn Error>> {
    let wire = from_hex(concat!(
        "a36870726f746f636f6c01677265706c69636150030303030303030303030303",
        "03030303676368616e67657381a76573636f7065a2676163636f756e74500101",
        "0101010101010101010101010101657370616365500202020202020202020202",
        "020202020268646f63756d656e74500404040404040404040404040404040467",
        "7265706c6963615003030303030303030303030303030303696f706572617469",
        "6f6e500505050505050505050505050505050567726571756573749820030303",
        "0303030303030303030303030303030303030303030303030303030303666469",
        "6765737498200707070707070707070707070707070707070707070707070707",
        "07070707070765627974657343010203",
    ))?;
    let decoded: PushRequest = ciborium::from_reader(wire.as_slice())?;
    assert_eq!(
        decoded.protocol, PROTOCOL_VERSION,
        "the protocol version must decode unchanged"
    );
    assert_eq!(decoded.changes.len(), 1, "one change must decode");
    assert_eq!(
        decoded.changes[0].bytes,
        vec![1, 2, 3],
        "the change bytes must decode unchanged"
    );
    assert_eq!(decoded.validate(), Ok(()), "the decoded push must validate");
    assert_eq!(encode(&decoded)?, wire, "push wire drifted");
    Ok(())
}

#[test]
fn a_pre_migration_pull_request_round_trips_byte_for_byte() -> Result<(), Box<dyn Error>> {
    let wire = from_hex(concat!(
        "a76870726f746f636f6c016573636f7065a2676163636f756e74500101010101",
        "0101010101010101010101657370616365500202020202020202020202020202",
        "0202677265706c69636150030303030303030303030303030303036772657175",
        "6573749820090909090909090909090909090909090909090909090909090909",
        "0909090909656166746572f66b6d61785f6368616e6765731880696d61785f62",
        "797465731a00400000",
    ))?;
    let decoded: PullRequest = ciborium::from_reader(wire.as_slice())?;
    assert_eq!(
        decoded.protocol, PROTOCOL_VERSION,
        "the protocol version must decode unchanged"
    );
    assert_eq!(decoded.max_changes, 128, "the count limit must decode");
    assert_eq!(decoded.max_bytes, 4_194_304, "the byte limit must decode");
    assert!(
        decoded.after.is_none(),
        "the absent cursor must stay absent"
    );
    assert_eq!(encode(&decoded)?, wire, "pull wire drifted");
    Ok(())
}
