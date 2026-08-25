//! Owns the planted-corruption gauntlet: corruption that keeps a frame
//! CRC-intact must fail the open loudly and leave the file untouched;
//! corruption that breaks the CRC truncates exactly to the last intact
//! boundary; absurd length fields fail closed instead of allocating
//! attacker-chosen buffers (issue #6 §8 negative tests).

use logical_db::journal::crc32;
use logical_db::memkv::MemKv;
use logical_db::model::Class;
use logical_db::port::ClockPort;
use logical_db::store::{Ldb, Mutation};
use logical_db::KernelError;
use std::cell::Cell;
use std::path::PathBuf;

#[derive(Default)]
struct CountingClock(Cell<u64>);

impl ClockPort for CountingClock {
    fn now(&self) -> u64 {
        self.0.set(self.0.get() + 1);
        self.0.get()
    }
}

fn tmp_path(tag: &str) -> PathBuf {
    std::env::temp_dir().join(format!("ldb-corrupt-{}-{tag}.journal", std::process::id()))
}

/// Three single-put batches, one journal frame each: frames are
/// addressable by index and their contents distinguishable.
fn plant_three_frames(path: &PathBuf) -> Vec<u8> {
    let mut db = Ldb::open(path, MemKv::new(), CountingClock::default()).unwrap();
    for i in 1..=3 {
        db.insert(&Mutation::put(
            Class::Record,
            format!("m{i}").into_bytes(),
            format!("payload-{i}").into_bytes(),
        ))
        .unwrap();
    }
    std::fs::read(path).unwrap()
}

/// Frame starts in file order: `[len u32 LE][crc u32 LE][payload]`.
fn frame_starts(bytes: &[u8]) -> Vec<usize> {
    let mut starts = Vec::new();
    let mut rest = bytes;
    let mut base = 0usize;
    while rest.len() >= 8 {
        let len = u32::from_le_bytes(rest[..4].try_into().unwrap()) as usize;
        if 8 + len > rest.len() {
            break;
        }
        starts.push(base);
        rest = &rest[8 + len..];
        base += 8 + len;
    }
    starts
}

// ── Intact-frame corruption fails loudly ─────────────────────

#[test]
fn crc_repaired_payload_corruption_fails_open_loudly_and_touches_nothing() {
    let path = tmp_path("intact");
    let original = plant_three_frames(&path);
    let starts = frame_starts(&original);
    assert_eq!(starts.len(), 3);

    // Overwrite the second frame's first row byte (its class tag) and
    // recompute the CRC, so the frame stays intact but undecodable.
    let mut poisoned = original.clone();
    let payload_at = starts[1] + 8;
    poisoned[payload_at] = 0x00;
    let len = u32::from_le_bytes(original[starts[1]..starts[1] + 4].try_into().unwrap()) as usize;
    // Hash the POISONED payload: only a matching CRC makes this frame
    // intact, which is exactly why decode — not the tear path — must
    // reject it.
    let crc = crc32(&poisoned[payload_at..payload_at + len]);
    poisoned[starts[1] + 4..starts[1] + 8].copy_from_slice(&crc.to_le_bytes());

    std::fs::write(&path, &poisoned).unwrap();
    let opened = Ldb::<MemKv, CountingClock>::open(&path, MemKv::new(), CountingClock::default());
    let err = match opened {
        Ok(_) => panic!("the open must fail closed"),
        Err(e) => e,
    };
    assert!(
        matches!(err, KernelError::UnknownClass(0)),
        "intact corruption must abort the open, got {err:?}"
    );
    assert_eq!(
        std::fs::read(&path).unwrap(),
        poisoned,
        "a failed open must not truncate the evidence"
    );

    let _ = std::fs::remove_file(&path);
}

#[test]
fn an_unknown_op_wire_tag_fails_open_loudly() {
    let path = tmp_path("optag");
    // Record, op tag 3 (unassigned), id "x", ts 0, empty payload —
    // framed with a correct CRC so only decode can reject it.
    let mut payload = vec![0x01, 0x03];
    payload.extend_from_slice(&1u32.to_le_bytes());
    payload.extend_from_slice(b"x");
    payload.extend_from_slice(&0u64.to_le_bytes());
    payload.extend_from_slice(&0u32.to_le_bytes());
    std::fs::write(&path, frame_bytes_of(&payload)).unwrap();

    let opened = Ldb::<MemKv, CountingClock>::open(&path, MemKv::new(), CountingClock::default());
    let err = match opened {
        Ok(_) => panic!("the open must fail closed"),
        Err(e) => e,
    };
    assert!(
        matches!(err, KernelError::TruncatedRow),
        "unknown wire ops share the malformed-row error today, got {err:?}"
    );

    let _ = std::fs::remove_file(&path);
}

fn frame_bytes_of(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + payload.len());
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&crc32(payload).to_le_bytes());
    out.extend_from_slice(payload);
    out
}

// ── CRC-breaking corruption truncates to the boundary ────────

#[test]
fn crc_broken_corruption_truncates_exactly_to_the_last_intact_frame() {
    let path = tmp_path("tear");
    let original = plant_three_frames(&path);
    let starts = frame_starts(&original);

    // Flip one payload byte of the second frame WITHOUT repairing the
    // CRC: replay must stop there and cut the tail back to frame one.
    let mut torn = original.clone();
    torn[starts[1] + 8] ^= 0xFF;
    std::fs::write(&path, &torn).unwrap();

    let db = Ldb::open(&path, MemKv::new(), CountingClock::default()).unwrap();
    let rows = db.scan_class(Class::Record).unwrap();
    assert_eq!(rows.len(), 1, "only the first batch survives the tear");
    assert_eq!(rows[0].id, b"m1".to_vec());
    assert_eq!(
        std::fs::read(&path).unwrap().len(),
        starts[1],
        "the torn region is cut back to the last intact boundary"
    );

    let _ = std::fs::remove_file(&path);
}

#[test]
fn an_absurd_frame_length_is_read_as_end_of_data() {
    let path = tmp_path("huge-frame");
    let original = plant_three_frames(&path);

    // A header-only trailer claiming u32::MAX payload bytes: bounds
    // checking ends the scan; nothing is allocated or trusted.
    let mut extended = original.clone();
    extended.extend_from_slice(&u32::MAX.to_le_bytes());
    extended.extend_from_slice(&0u32.to_le_bytes());

    std::fs::write(&path, &extended).unwrap();
    let db = Ldb::open(&path, MemKv::new(), CountingClock::default()).unwrap();
    assert_eq!(db.scan_class(Class::Record).unwrap().len(), 3);
    assert_eq!(
        std::fs::read(&path).unwrap().len(),
        original.len(),
        "the impossible header contributes no bytes"
    );

    let _ = std::fs::remove_file(&path);
}

// ── Attacker length fields fail closed without allocating ────

#[test]
fn absurd_row_length_fields_fail_closed_without_allocating() {
    for (tag, payload) in [
        ("huge-id", {
            let mut p = vec![0x01, 0x01]; // Record, Put
            p.extend_from_slice(&u32::MAX.to_le_bytes()); // id_len
            p
        }),
        ("huge-payload", {
            let mut p = vec![0x01, 0x01];
            p.extend_from_slice(&2u32.to_le_bytes()); // id_len
            p.extend_from_slice(b"id");
            p.extend_from_slice(&0u64.to_le_bytes()); // ts
            p.extend_from_slice(&u32::MAX.to_le_bytes()); // payload_len
            p
        }),
    ] {
        let path = tmp_path(tag);
        std::fs::write(&path, frame_bytes_of(&payload)).unwrap();
        let opened =
            Ldb::<MemKv, CountingClock>::open(&path, MemKv::new(), CountingClock::default());
        let err = match opened {
            Ok(_) => panic!("the open must fail closed"),
            Err(e) => e,
        };
        assert!(
            matches!(err, KernelError::TruncatedRow),
            "{tag}: absurd lengths must be refused, not materialised, got {err:?}"
        );
        let _ = std::fs::remove_file(&path);
    }
}
