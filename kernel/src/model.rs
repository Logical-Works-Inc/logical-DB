//! Owns the canonical row model: the five storage classes and the Row
//! type with its wire codec. INV-ONE-CORE-FOUR-CLASSES: a row is
//! exactly one of five classes; unknown wire tags are unconstructable.

use serde::{Deserialize, Serialize};

use crate::{KernelError, Result};

// ── Class ────────────────────────────────────────────────────

/// The five storage classes. Wire tags 1..=5; tag 0 stays unused so a
/// zeroed buffer never decodes to a valid class.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
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

// ── Op ──────────────────────────────────────────────────────

/// Explicit mutation operation. A `Put(Vec::new())` is valid live data;
/// a `Delete` carries no payload. Wire tags: Put=1, Delete=2.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Op {
    Put(Vec<u8>),
    Delete,
}

impl Op {
    pub fn wire_tag(&self) -> u8 {
        match self {
            Op::Put(_) => 1,
            Op::Delete => 2,
        }
    }

    pub fn is_delete(&self) -> bool {
        matches!(self, Op::Delete)
    }

    pub fn payload(&self) -> &[u8] {
        match self {
            Op::Put(p) => p,
            Op::Delete => &[],
        }
    }
}

// ── Row ──────────────────────────────────────────────────────

/// One canonical row: `(class, id, ts, op)`. `ts` is logical time
/// supplied by the caller's clock; ordering is by key, so history for a
/// given (class, id) sorts ascending by construction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Row {
    pub class: Class,
    pub id: Vec<u8>,
    pub ts: u64,
    pub op: Op,
}

impl Row {
    pub fn new(class: Class, id: impl Into<Vec<u8>>, ts: u64, op: Op) -> Self {
        Row {
            class,
            id: id.into(),
            ts,
            op,
        }
    }

    pub fn put(class: Class, id: impl Into<Vec<u8>>, ts: u64, payload: Vec<u8>) -> Self {
        Self::new(class, id, ts, Op::Put(payload))
    }

    pub fn delete(class: Class, id: impl Into<Vec<u8>>, ts: u64) -> Self {
        Self::new(class, id, ts, Op::Delete)
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

/// Appends the row's journal-frame encoding (v1):
/// `[class_tag][op_tag][id_len u32 LE][id][ts u64 LE][payload_len u32 LE][payload]`.
/// Op::Delete encodes with payload_len=0 and no payload bytes.
pub fn encode_row(row: &Row, out: &mut Vec<u8>) -> Result<()> {
    let payload = row.op.payload();
    if row.id.len() > u32::MAX as usize || payload.len() > u32::MAX as usize {
        return Err(KernelError::RowTooLarge);
    }
    out.push(row.class.tag());
    out.push(row.op.wire_tag());
    out.extend_from_slice(&(row.id.len() as u32).to_le_bytes());
    out.extend_from_slice(&row.id);
    out.extend_from_slice(&row.ts.to_le_bytes());
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(payload);
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
    if buf.len() < 2 {
        return Err(KernelError::TruncatedRow);
    }
    let class_tag = buf[0];
    let op_tag = buf[1];
    *buf = &buf[2..];
    let class = Class::from_tag(class_tag)?;
    let id_len = read_u32(buf)?;
    let id = take(buf, id_len as usize)?.to_vec();
    let ts = read_u64(buf)?;
    let payload_len = read_u32(buf)?;
    let payload = take(buf, payload_len as usize)?.to_vec();
    let op = match op_tag {
        1 => Op::Put(payload),
        2 => Op::Delete,
        _ => return Err(KernelError::TruncatedRow),
    };
    Ok(Row { class, id, ts, op })
}

// ── Composite key codec ─────────────────────────────────────

/// Encodes a composite key: `[namespace][len_1 u16 BE][comp_1]...`.
/// No byte is reserved; arbitrary `[u8]` components round-trip.
pub fn encode_composite(namespace: u8, components: &[&[u8]]) -> Result<Vec<u8>> {
    let total: usize = 1 + components.iter().map(|c| 2 + c.len()).sum::<usize>();
    let mut out = Vec::with_capacity(total);
    out.push(namespace);
    for c in components {
        if c.len() > u16::MAX as usize {
            return Err(KernelError::ComponentTooLong);
        }
        out.extend_from_slice(&(c.len() as u16).to_be_bytes());
        out.extend_from_slice(c);
    }
    Ok(out)
}

/// Decodes a composite key back into `(namespace, components)`.
pub fn decode_composite(buf: &[u8]) -> Result<(u8, Vec<Vec<u8>>)> {
    if buf.is_empty() {
        return Err(KernelError::MalformedCompositeKey);
    }
    let namespace = buf[0];
    let mut cursor = &buf[1..];
    let mut components = Vec::new();
    while !cursor.is_empty() {
        if cursor.len() < 2 {
            return Err(KernelError::MalformedCompositeKey);
        }
        let len = u16::from_be_bytes([cursor[0], cursor[1]]) as usize;
        cursor = &cursor[2..];
        if cursor.len() < len {
            return Err(KernelError::MalformedCompositeKey);
        }
        components.push(cursor[..len].to_vec());
        cursor = &cursor[len..];
    }
    Ok((namespace, components))
}

/// Prefix for scanning all entries under a namespace.
pub fn composite_ns_prefix(namespace: u8) -> Vec<u8> {
    vec![namespace]
}

/// Prefix for scanning all entries under a namespace + first component.
pub fn composite_prefix(namespace: u8, first: &[u8]) -> Result<Vec<u8>> {
    if first.len() > u16::MAX as usize {
        return Err(KernelError::ComponentTooLong);
    }
    let mut out = Vec::with_capacity(1 + 2 + first.len());
    out.push(namespace);
    out.extend_from_slice(&(first.len() as u16).to_be_bytes());
    out.extend_from_slice(first);
    Ok(out)
}

// ── Tests ────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_put_preserves_every_field() {
        let row = Row::put(Class::Claim, b"node/42".to_vec(), 7, vec![9, 8, 7]);
        let mut buf = Vec::new();
        encode_row(&row, &mut buf).unwrap();
        let mut cursor: &[u8] = &buf;
        let back = decode_row(&mut cursor).unwrap();
        assert_eq!(back, row);
        assert!(cursor.is_empty(), "one row consumed the whole buffer");
    }

    #[test]
    fn round_trip_delete_preserves_every_field() {
        let row = Row::delete(Class::Record, b"gone".to_vec(), 42);
        let mut buf = Vec::new();
        encode_row(&row, &mut buf).unwrap();
        let mut cursor: &[u8] = &buf;
        let back = decode_row(&mut cursor).unwrap();
        assert_eq!(back, row);
        assert!(cursor.is_empty());
    }

    #[test]
    fn empty_payload_put_is_valid_data() {
        let row = Row::put(Class::Record, b"empty".to_vec(), 1, vec![]);
        let mut buf = Vec::new();
        encode_row(&row, &mut buf).unwrap();
        let mut cursor: &[u8] = &buf;
        let back = decode_row(&mut cursor).unwrap();
        assert_eq!(back.op, Op::Put(vec![]));
        assert!(!back.op.is_delete());
    }

    #[test]
    fn unknown_class_tag_is_rejected() {
        let mut buf = Vec::new();
        encode_row(&Row::put(Class::Record, "a", 1, vec![]), &mut buf).unwrap();
        buf[0] = 0;
        assert!(matches!(
            decode_row(&mut buf.as_slice()),
            Err(KernelError::UnknownClass(0))
        ));
    }

    #[test]
    fn truncated_buffer_is_rejected() {
        let row = Row::put(Class::Term, "t", 3, vec![1, 2, 3]);
        let mut buf = Vec::new();
        encode_row(&row, &mut buf).unwrap();
        for cut in [1, 5, 9, buf.len() - 1] {
            let short = &buf[..cut];
            assert!(decode_row(&mut (short as &[u8])).is_err(), "cut at {cut}");
        }
    }

    #[test]
    fn keys_sort_history_ascending_within_an_id() {
        let a = Row::put(Class::Record, "x", 1, vec![]);
        let b = Row::put(Class::Record, "x", 2, vec![]);
        assert!(a.key() < b.key());
    }

    #[test]
    fn class_survives_json_roundtrip() {
        for class in [
            Class::Record,
            Class::Term,
            Class::Edge,
            Class::VecCode,
            Class::Claim,
        ] {
            let json = lgwks_std::json::to_string(&class).unwrap();
            let back: Class = lgwks_std::json::from_str(&json).unwrap();
            assert_eq!(back, class);
        }
    }

    #[test]
    fn key_hex_is_inspectable() {
        let key = point_key(Class::Record, b"doc/1", 42);
        let hex = lgwks_std::hex::encode(&key);
        assert!(hex.starts_with("01"), "record tag is 0x01");
    }

    #[test]
    fn composite_key_roundtrips_arbitrary_bytes() {
        let cases: Vec<&[&[u8]]> = vec![
            &[b"hello", b"world"],
            &[&[0xFF, 0x00, 0xFF], &[0x00]],
            &[&[], &[0xFF]],
            &[b"single"],
        ];
        for components in &cases {
            let encoded = encode_composite(0x42, components).unwrap();
            let (ns, decoded) = decode_composite(&encoded).unwrap();
            assert_eq!(ns, 0x42);
            let expected: Vec<Vec<u8>> = components.iter().map(|c| c.to_vec()).collect();
            assert_eq!(decoded, expected);
        }
    }

    #[test]
    fn different_composite_tuples_never_alias() {
        let a = encode_composite(0x01, &[b"ab", b""]).unwrap();
        let b = encode_composite(0x01, &[b"a", b"b"]).unwrap();
        assert_ne!(a, b);
    }
}
