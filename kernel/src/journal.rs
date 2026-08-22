//! Owns the framed journal wire format: `[len u32 LE][crc32 u32 LE]
//! [payload]`. INV-BATCH-IS-ONE-FRAME lives here — the store encodes a
//! whole batch into one frame, so replay applies it whole or not at
//! all. Replay stops at the exact byte boundary of the last intact
//! frame; the caller truncates any torn tail. Graft of the fjall
//! journal organ (`src/journal/writer.rs:237,248,326`).

use std::io::{self, Read, Write};

use crate::{KernelError, Result};

// ── CRC32 (IEEE, reflected, 0xEDB88320) ──────────────────────

const fn build_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0usize;
    while i < 256 {
        let mut c = i as u32;
        let mut k = 0;
        while k < 8 {
            c = if c & 1 != 0 {
                0xEDB8_8320 ^ (c >> 1)
            } else {
                c >> 1
            };
            k += 1;
        }
        table[i] = c;
        i += 1;
    }
    table
}

static TABLE: [u32; 256] = build_table();

pub fn crc32(data: &[u8]) -> u32 {
    let mut c = 0xFFFF_FFFFu32;
    for &b in data {
        c = TABLE[((c ^ u32::from(b)) & 0xFF) as usize] ^ (c >> 8);
    }
    c ^ 0xFFFF_FFFF
}

// ── Frame io ─────────────────────────────────────────────────

pub const HEADER_LEN: usize = 8;

/// Appends one frame; the caller owns flushing and fsync (that ordering
/// is INV-DURABLE-BEFORE-ACK and belongs to the store).
pub fn append_frame<W: Write>(w: &mut W, payload: &[u8]) -> io::Result<()> {
    let len = u32::try_from(payload.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "frame exceeds u32"))?;
    w.write_all(&len.to_le_bytes())?;
    w.write_all(&crc32(payload).to_le_bytes())?;
    w.write_all(payload)
}

/// Replay outcome: `valid_bytes` is the prefix of the stream covered by
/// intact frames; anything beyond it is a torn tail to truncate.
#[derive(Debug, PartialEq, Eq)]
pub struct ReplayStats {
    pub frames: u64,
    pub valid_bytes: u64,
}

/// Streams every intact frame through `sink`, stopping cleanly at EOF
/// or at the first torn/corrupt frame. A frame that is complete and
/// crc-valid but fails to decode is a hard error — that is corruption
/// inside a good frame, not a torn tail, and must not be truncated
/// away silently.
pub fn replay<R: Read>(
    r: &mut R,
    mut sink: impl FnMut(&[u8]) -> Result<()>,
) -> Result<ReplayStats> {
    let mut stats = ReplayStats {
        frames: 0,
        valid_bytes: 0,
    };
    let mut header = [0u8; HEADER_LEN];
    loop {
        match r.read_exact(&mut header) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(stats),
            Err(e) => return Err(KernelError::Io(e)),
        }
        let len = u32::from_le_bytes([header[0], header[1], header[2], header[3]]) as usize;
        let crc = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);
        let mut payload = vec![0u8; len];
        match r.read_exact(&mut payload) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(stats),
            Err(e) => return Err(KernelError::Io(e)),
        }
        if crc32(&payload) != crc {
            return Ok(stats);
        }
        sink(&payload)?;
        stats.frames += 1;
        stats.valid_bytes += HEADER_LEN as u64 + len as u64;
    }
}

// ── Tests ────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc_matches_the_ieee_check_value() {
        // External truth, not self-consistency: the standard check
        // value for "123456789" is 0xCBF43926.
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    }

    #[test]
    fn replay_stops_at_a_torn_tail_and_counts_valid_bytes() {
        let mut bytes = Vec::new();
        append_frame(&mut bytes, b"alpha").unwrap();
        append_frame(&mut bytes, b"beta").unwrap();
        let intact = bytes.len();
        bytes.extend_from_slice(&[1, 2, 3]); // torn frame

        let mut seen = Vec::new();
        let stats = replay(&mut bytes.as_slice(), |p| {
            seen.push(p.to_vec());
            Ok(())
        })
        .unwrap();
        assert_eq!(seen, [b"alpha".to_vec(), b"beta".to_vec()]);
        assert_eq!(
            stats,
            ReplayStats {
                frames: 2,
                valid_bytes: intact as u64
            }
        );
    }

    #[test]
    fn a_bad_crc_stops_replay_at_that_frame() {
        let mut bytes = Vec::new();
        append_frame(&mut bytes, b"good").unwrap();
        append_frame(&mut bytes, b"corrupt").unwrap();
        let after_first = HEADER_LEN + 5;
        bytes[after_first + 4] ^= 0xFF; // flip a crc byte of frame two

        let mut seen = Vec::new();
        let stats = replay(&mut bytes.as_slice(), |p| {
            seen.push(p.to_vec());
            Ok(())
        })
        .unwrap();
        assert_eq!(seen, [b"good".to_vec()]);
        assert_eq!(stats.frames, 1);
    }

    #[test]
    fn a_decode_failure_inside_an_intact_frame_is_a_hard_error() {
        let mut bytes = Vec::new();
        // Intact frame whose payload carries invalid wire (tag 0).
        append_frame(&mut bytes, &[0u8]).unwrap();
        // The sink does what the store's sink does: decode the frame.
        let mut calls = 0;
        let err = replay(&mut bytes.as_slice(), |frame| {
            calls += 1;
            let mut cursor: &[u8] = frame;
            crate::model::decode_row(&mut cursor).map(|_| ())
        })
        .unwrap_err();
        assert!(matches!(err, KernelError::UnknownClass(0)));
        assert_eq!(calls, 1);
    }
}
