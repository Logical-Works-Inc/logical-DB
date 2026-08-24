//! Owns the machine-checked half of the DONE theorem (vision page,
//! conjuncts 1-2 at the pure layers): model codec totality, key-layout
//! monotonicity, and journal prefix consistency.

use crate::journal::{append_frame, replay, HEADER_LEN};
use crate::model::{decode_row, encode_row, point_key, split_key, Class, Op, Row};

// ── Conjunct 2 - codec totality ──────────────────────────────

#[kani::proof]
#[kani::unwind(32)]
fn decode_is_total_and_reencode_reproduces_the_input() {
    let buf: [u8; 14] = kani::any();
    let mut cursor: &[u8] = &buf;
    if let Ok(row) = decode_row(&mut cursor) {
        let consumed = buf.len() - cursor.len();
        let mut out = Vec::new();
        encode_row(&row, &mut out).unwrap();
        assert_eq!(consumed, out.len());
        assert_eq!(&buf[..consumed], out.as_slice());
    }
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

#[kani::proof]
#[kani::unwind(32)]
fn a_cut_frame_yields_the_whole_frame_or_nothing() {
    let row = Row::put(Class::VecCode, "a", 7, vec![0xAB; 4]);
    let mut wire = Vec::new();
    encode_row(&row, &mut wire).unwrap();
    let mut frame = Vec::new();
    append_frame(&mut frame, &wire).unwrap();
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

    let stats = replay(&mut &frame[..cut], |_p| Ok(())).unwrap();

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
    append_frame(&mut f1, &w1).unwrap();
    let mut f2 = Vec::new();
    let mut w2 = Vec::new();
    encode_row(&r2, &mut w2).unwrap();
    append_frame(&mut f2, &w2).unwrap();
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
    let stats = replay(&mut &stream[..cut], |_frame| {
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
