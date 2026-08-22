//! Owns the machine-checked half of the DONE theorem (vision page,
//! conjuncts 1–2 at the pure layers): model codec totality, key-layout
//! monotonicity, and journal prefix consistency. Harnesses run only
//! under `cargo kani` — `#[cfg(kani)]` keeps them out of normal builds.
//!
//! INV-HARNESS-BUDGET: framing harnesses keep DATA concrete and make
//! only a tiny case-selector symbolic — never a raw symbolic offset.
//! Two measured blowups (2026-08-21): symbolic `Vec<Row>` accumulation
//! at unwind 96 solved 48+ min; a symbolic slice LENGTH fed through
//! std's `read_exact` kept unwind-32 solving past 15 min even with
//! concrete bytes, because every Read loop unrolls 32× with symbolic
//! pointer arithmetic. Cut coverage here therefore enumerates the
//! SEMANTIC equivalence classes of a byte cut (in-header, post-header,
//! partial payload, one-short, complete); exhaustiveness over every
//! raw offset belongs to the runtime gauntlet
//! (`kernel/tests/property_replay_fold.rs`). Payload generality is the
//! codec harness's job. Unwind is per-harness and sized to the actual
//! loop bound, so each harness solves in seconds-to-minutes and the CI
//! job stays inside its budget.
//!
//! Boundary note: `store.rs` performs real fs syscalls Kani cannot
//! model, so its crash-consistency conjunct is discharged at runtime by
//! `kernel/tests/property_replay_fold.rs` (exhaustive byte-cut gauntlet)
//! until a bounded fs model exists.

use crate::journal::{append_frame, replay, HEADER_LEN};
use crate::model::{decode_row, encode_row, point_key, split_key, Class, Row};

// ── Conjunct 2 · codec totality ──────────────────────────────

/// Decode is total: arbitrary bytes either decode or fail cleanly, and
/// a successful decode re-encodes to exactly the consumed prefix.
#[kani::proof]
#[kani::unwind(32)]
fn decode_is_total_and_reencode_reproduces_the_input() {
    let buf: [u8; 12] = kani::any();
    let mut cursor: &[u8] = &buf;
    if let Ok(row) = decode_row(&mut cursor) {
        let consumed = buf.len() - cursor.len();
        let mut out = Vec::new();
        encode_row(&row, &mut out).unwrap();
        assert_eq!(consumed, out.len());
        assert_eq!(&buf[..consumed], out.as_slice());
    }
}

/// The key layout inverse holds for every valid class tag and every
/// timestamp pair: split(point(x)) == x.
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

/// Big-endian ts in the key makes version order monotone for any two
/// timestamps — INV-TS-MONOTONIC at the layout layer.
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

// ── Conjunct 1 · journal prefix consistency ──────────────────

/// Cutting a one-frame stream replays exactly the intact prefix: the
/// whole frame, or nothing — never a partial frame. The cut ranges
/// over the semantic classes of a byte position (INV-HARNESS-BUDGET).
#[kani::proof]
#[kani::unwind(32)]
fn a_cut_frame_yields_the_whole_frame_or_nothing() {
    let row = Row::new(Class::VecCode, "a", 7, vec![0xAB; 4]);
    let mut wire = Vec::new();
    encode_row(&row, &mut wire).unwrap();
    let mut frame = Vec::new();
    append_frame(&mut frame, &wire).unwrap();
    let wire_len = wire.len();

    // Semantic classes of a cut on one frame of length L = 8 + W:
    // empty · inside header · exactly post-header · early payload ·
    // last-payload-byte-missing · complete.
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

/// Two-frame streams behave compositionally: a byte cut recovers a
/// prefix of committed batches — the replay side of INV-BATCH-IS-
/// ONE-FRAME. The cut ranges over the semantic classes across both
/// frames (INV-HARNESS-BUDGET).
#[kani::proof]
#[kani::unwind(24)]
fn a_two_frame_stream_recovers_exactly_a_committed_prefix() {
    let r1 = Row::new(Class::Record, "a", 7, vec![0x11]);
    let r2 = Row::new(Class::Record, "b", 9, vec![0x22]);

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

    // Semantic classes across two frames: four inside frame one ·
    // exactly at the boundary · frame-two header torn · frame-two
    // payload torn · complete.
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
