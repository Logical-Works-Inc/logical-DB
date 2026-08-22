//! Owns the canonical row model: the five storage classes and the Row
//! type with its wire codec. INV-ONE-CORE-FOUR-CLASSES: a row is
//! exactly one of five classes; unknown wire tags are unconstructable.

use crate::{KernelError, Result};

// ── Class ────────────────────────────────────────────────────

/// The five storage classes. Wire tags 1..=5; tag 0 stays unused so a
/// zeroed buffer never decodes to a valid class.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Class {
    Record = 1,
    Term = 2,
    Edge = 3,
    VecCode = 4,
    Claim = 5,
}

impl Class {
    pub fn tag(self) -> u8 {
        self as u8
    }

    /// The only door from wire byte to class: everything else is
    /// rejected, which is what keeps unknown tags unconstructable.
    pub fn from_tag(tag: u8) -> std::result::Result<Class, KernelError> {
        match tag {
            1 => Ok(Class::Record),
            2 => Ok(Class::Term),
            3 => Ok(Class::Edge),
            4 => Ok(Class::VecCode),
            5 => Ok(Class::Claim),
            other => Err(KernelError::UnknownClass(other)),
        }
    }
}

// ── Row ──────────────────────────────────────────────────────

/// One canonical row: `(class, id, ts, payload)`. `ts` is logical time
/// supplied by the caller's clock; ordering is by key, so history for a
/// given (class, id) sorts ascending by construction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Row {
    pub class: Class,
    pub id: Vec<u8>,
    pub ts: u64,
    pub payload: Vec<u8>,
}

impl Row {
    pub fn new(class: Class, id: impl Into<Vec<u8>>, ts: u64, payload: Vec<u8>) -> Self {
        Row {
            class,
            id: id.into(),
            ts,
            payload,
        }
    }

    /// KV primary-index key: `[tag][id][ts BE]`. Big-endian ts makes
    /// same-id versions sort in commit order inside one scan.
    pub fn key(&self) -> Vec<u8> {
        let mut k = Vec::with_capacity(1 + self.id.len() + 8);
        k.push(self.class.tag());
        k.extend_from_slice(&self.id);
        k.extend_from_slice(&self.ts.to_be_bytes());
        k
    }
}

// ── Key layout ───────────────────────────────────────────────

/// `[tag]` — every key of one class shares this one-byte prefix.
pub fn class_prefix(class: Class) -> Vec<u8> {
    vec![class.tag()]
}

/// `[tag][id]` — the full version history of one entity.
pub fn history_prefix(class: Class, id: &[u8]) -> Vec<u8> {
    let mut p = class_prefix(class);
    p.extend_from_slice(id);
    p
}

/// `[tag][id][ts BE]` — one version of one entity.
pub fn point_key(class: Class, id: &[u8], ts: u64) -> Vec<u8> {
    let mut k = history_prefix(class, id);
    k.extend_from_slice(&ts.to_be_bytes());
    k
}

/// The inverse of [`point_key`]; keeps all key-layout knowledge here.
pub fn split_key(key: &[u8]) -> Result<(Class, Vec<u8>, u64)> {
    if key.len() < 1 + 8 {
        return Err(KernelError::TruncatedRow);
    }
    let class = Class::from_tag(key[0])?;
    let id = key[1..key.len() - 8].to_vec();
    let tail = &key[key.len() - 8..];
    let ts = u64::from_be_bytes([
        tail[0], tail[1], tail[2], tail[3], tail[4], tail[5], tail[6], tail[7],
    ]);
    Ok((class, id, ts))
}

// ── Wire codec ───────────────────────────────────────────────

/// Appends the row's journal-frame encoding:
/// `[tag][id_len u32 LE][id][ts u64 LE][payload_len u32 LE][payload]`.
pub fn encode_row(row: &Row, out: &mut Vec<u8>) -> Result<()> {
    if row.id.len() > u32::MAX as usize || row.payload.len() > u32::MAX as usize {
        return Err(KernelError::RowTooLarge);
    }
    out.push(row.class.tag());
    out.extend_from_slice(&(row.id.len() as u32).to_le_bytes());
    out.extend_from_slice(&row.id);
    out.extend_from_slice(&row.ts.to_le_bytes());
    out.extend_from_slice(&(row.payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&row.payload);
    Ok(())
}

fn read_u32(buf: &mut &[u8]) -> Result<u32> {
    if buf.len() < 4 {
        return Err(KernelError::TruncatedRow);
    }
    let (a, rest) = buf.split_at(4);
    *buf = rest;
    Ok(u32::from_le_bytes([a[0], a[1], a[2], a[3]]))
}

fn read_u64(buf: &mut &[u8]) -> Result<u64> {
    if buf.len() < 8 {
        return Err(KernelError::TruncatedRow);
    }
    let mut b = [0u8; 8];
    b.copy_from_slice(&buf[..8]);
    *buf = &buf[8..];
    Ok(u64::from_le_bytes(b))
}

fn take<'a>(buf: &mut &'a [u8], n: usize) -> Result<&'a [u8]> {
    if buf.len() < n {
        return Err(KernelError::TruncatedRow);
    }
    let (a, rest) = buf.split_at(n);
    *buf = rest;
    Ok(a)
}

/// Decodes one row from the front of `buf`, advancing it past the row.
pub fn decode_row(buf: &mut &[u8]) -> Result<Row> {
    let tag = if buf.is_empty() {
        return Err(KernelError::TruncatedRow);
    } else {
        let t = buf[0];
        *buf = &buf[1..];
        t
    };
    let class = Class::from_tag(tag)?;
    let id_len = read_u32(buf)?;
    let id = take(buf, id_len as usize)?.to_vec();
    let ts = read_u64(buf)?;
    let payload_len = read_u32(buf)?;
    let payload = take(buf, payload_len as usize)?.to_vec();
    Ok(Row {
        class,
        id,
        ts,
        payload,
    })
}

// ── Tests ────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_preserves_every_field() {
        let row = Row::new(Class::Claim, b"node/42".to_vec(), 7, vec![9, 8, 7]);
        let mut buf = Vec::new();
        encode_row(&row, &mut buf).unwrap();
        let mut cursor: &[u8] = &buf;
        let back = decode_row(&mut cursor).unwrap();
        assert_eq!(back, row);
        assert!(cursor.is_empty(), "one row consumed the whole buffer");
    }

    #[test]
    fn unknown_class_tag_is_rejected() {
        let mut buf = vec![0u8]; // tag 0 is reserved-invalid by design
        encode_row(&Row::new(Class::Record, "a", 1, vec![]), &mut buf).unwrap();
        buf[0] = 0;
        assert!(matches!(
            decode_row(&mut buf.as_slice()),
            Err(KernelError::UnknownClass(0))
        ));
    }

    #[test]
    fn truncated_buffer_is_rejected() {
        let row = Row::new(Class::Term, "t", 3, vec![1, 2, 3]);
        let mut buf = Vec::new();
        encode_row(&row, &mut buf).unwrap();
        for cut in [1, 5, 9, buf.len() - 1] {
            let short = &buf[..cut];
            assert!(decode_row(&mut (short as &[u8])).is_err(), "cut at {cut}");
        }
    }

    #[test]
    fn keys_sort_history_ascending_within_an_id() {
        let a = Row::new(Class::Record, "x", 1, vec![]);
        let b = Row::new(Class::Record, "x", 2, vec![]);
        assert!(a.key() < b.key());
    }
}
