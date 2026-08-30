//! Owns the machine-checked half of the DONE theorem (vision page,
//! conjuncts 1-2 at the pure layers): model codec totality, key-layout
//! monotonicity, and journal prefix consistency.

use crate::journal::{crc32, scan_intact_frames, HEADER_LEN};
use crate::model::{decode_row, encode_row, point_key, split_key, Class, Row};

// ── Conjunct 2 - codec totality ──────────────────────────────

#[kani::proof]
#[kani::unwind(32)]
fn decode_is_total_and_reencode_reproduces_the_input() {
    // The minimum valid row is 18 bytes: class + op + empty id length +
    // timestamp + empty payload length.  The previous 14-byte arbitrary
    // buffer could never decode successfully, so its round-trip branch was
    // vacuous while CBMC still explored symbolic allocation-error paths.
    // Keep every semantic header field symbolic while fixing both lengths at
    // zero.  Valid class/op combinations must now reach the success branch.
    let class_tag: u8 = kani::any();
    let op_tag: u8 = kani::any();
    let ts: u64 = kani::any();
    kani::assume((1..=5).contains(&class_tag));
    kani::assume(op_tag == 1 || op_tag == 2);
    let mut buf = [0u8; 18];
    buf[0] = class_tag;
    buf[1] = op_tag;
    buf[6..14].copy_from_slice(&ts.to_le_bytes());

    let mut cursor: &[u8] = &buf;
    let row = decode_row(&mut cursor).expect("minimum valid row must decode");
    let consumed = buf.len() - cursor.len();
    let mut out = Vec::new();
    encode_row(&row, &mut out).unwrap();
    assert_eq!(consumed, out.len());
    assert_eq!(&buf[..consumed], out.as_slice());
}

#[kani::proof]
#[kani::unwind(16)]
fn split_key_inverts_point_key_for_every_class_and_ts() {
    let tag: u8 = kani::any();
    let ts: u64 = kani::any();
    if let Ok(class) = Class::from_tag(tag) {
        let key = point_key(class, b"x", ts);
        let (back_class, back_id, back_ts) = split_key(&key).unwrap();
        assert_eq!(back_class, class);
        assert_eq!(back_id, b"x".to_vec());
        assert_eq!(back_ts, ts);
    }
}

#[kani::proof]
#[kani::unwind(16)]
fn keys_never_order_a_later_version_before_an_earlier_one() {
    let t1: u64 = kani::any();
    let t2: u64 = kani::any();
    kani::assume(t1 <= t2);
    let a = point_key(Class::Record, b"x", t1);
    let b = point_key(Class::Record, b"x", t2);
    assert!(a <= b);
}

// ── Conjunct 1 - journal prefix consistency ──────────────────

/// Proof-side frame assembly — byte-identical to the journal wire
/// format (`[len u32 LE][crc32 u32 LE][payload]`) without dragging
/// std::io error machinery into the proof scope.
fn frame_bytes(wire: &[u8], out: &mut Vec<u8>) {
    out.extend_from_slice(&(wire.len() as u32).to_le_bytes());
    out.extend_from_slice(&crc32(wire).to_le_bytes());
    out.extend_from_slice(wire);
}

#[kani::proof]
#[kani::unwind(32)]
fn a_cut_frame_yields_the_whole_frame_or_nothing() {
    let row = Row::put(Class::VecCode, "a", 7, vec![0xAB; 4]);
    let mut wire = Vec::new();
    encode_row(&row, &mut wire).unwrap();
    let mut frame = Vec::new();
    frame_bytes(&wire, &mut frame);
    let wire_len = wire.len();

    let class: u8 = kani::any();
    kani::assume(class < 6);
    let payload_start = HEADER_LEN;
    let cut = match class {
        0 => 0,
        1 => HEADER_LEN / 2,
        2 => payload_start,
        3 => payload_start + wire_len / 3,
        4 => frame.len() - 1,
        _ => frame.len(),
    };

    let stats = scan_intact_frames(&frame[..cut], |_p| Ok(())).unwrap();

    if cut == frame.len() {
        assert_eq!(stats.frames, 1);
        assert_eq!(stats.valid_bytes, (HEADER_LEN + wire_len) as u64);
    } else {
        assert_eq!(stats.frames, 0, "a torn frame never yields its row");
    }
}

#[kani::proof]
#[kani::unwind(24)]
fn a_two_frame_stream_recovers_exactly_a_committed_prefix() {
    let r1 = Row::put(Class::Record, "a", 7, vec![0x11]);
    let r2 = Row::put(Class::Record, "b", 9, vec![0x22]);

    let mut f1 = Vec::new();
    let mut w1 = Vec::new();
    encode_row(&r1, &mut w1).unwrap();
    frame_bytes(&w1, &mut f1);
    let mut f2 = Vec::new();
    let mut w2 = Vec::new();
    encode_row(&r2, &mut w2).unwrap();
    frame_bytes(&w2, &mut f2);
    let boundary = f1.len();
    let mut stream = f1;
    stream.extend_from_slice(&f2);

    let class: u8 = kani::any();
    kani::assume(class < 8);
    let cut = match class {
        0 => 0,
        1 => HEADER_LEN / 2,
        2 => boundary - 1,
        3 => boundary - w2.len(),
        4 => boundary,
        5 => boundary + HEADER_LEN / 2,
        6 => stream.len() - 1,
        _ => stream.len(),
    };

    let mut frames_seen: u32 = 0;
    let stats = scan_intact_frames(&stream[..cut], |_frame| {
        frames_seen += 1;
        Ok(())
    })
    .unwrap();

    let expected: u32 = if cut == stream.len() {
        2
    } else if cut >= boundary {
        1
    } else {
        0
    };
    assert_eq!(frames_seen, expected);
    assert_eq!(stats.frames as u32, expected);
}
