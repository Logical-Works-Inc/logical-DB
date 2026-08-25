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

    /// KV primary-index key: one version of one entity under the
    /// module's framed layout; big-endian ts makes same-id versions
    /// sort in commit order inside one scan.
    pub fn key(&self) -> Vec<u8> {
        point_key(self.class, &self.id, self.ts)
    }
}

// ── Key layout ───────────────────────────────────────────────
//
// Point keys are `[tag][escaped(id)][0x00 0x00][ts BE]`. Inside the id,
// a literal 0x00 is escaped as `0x00 0xFF`; the pair `0x00 0x00` can
// therefore only ever be the terminator (INV-ID-PREFIX-DISJOINT). This
// framing makes entity id spaces provably disjoint under prefix scans:
// no id can be a byte-prefix extension of another and leak its rows
// into that entity's history.

/// `[tag]` — every key of one class shares this one-byte prefix.
pub fn class_prefix(class: Class) -> Vec<u8> {
    vec![class.tag()]
}

fn escape_id(id: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(id.len() + 2);
    for &b in id {
        if b == 0x00 {
            out.extend_from_slice(&[0x00, 0xFF]);
        } else {
            out.push(b);
        }
    }
    out
}

/// Scan prefix over every key whose logical id starts with `id_bytes`
/// — a full id or a prefix of one. Escaping is byte-local, so escaped
/// streams diverge exactly where the raw ids diverge; no foreign row
/// can enter the scan.
pub fn class_id_prefix(class: Class, id_bytes: &[u8]) -> Vec<u8> {
    let mut p = class_prefix(class);
    p.extend_from_slice(&escape_id(id_bytes));
    p
}

/// `[tag][escaped(id)][terminator]` — the full version history of one
/// entity, disjoint from every other id's range.
pub fn history_prefix(class: Class, id: &[u8]) -> Vec<u8> {
    let mut p = class_id_prefix(class, id);
    p.extend_from_slice(&[0x00, 0x00]);
    p
}

/// `[tag][escaped(id)][terminator][ts BE]` — one version of one
/// entity. Big-endian ts keeps same-id versions in commit order.
pub fn point_key(class: Class, id: &[u8], ts: u64) -> Vec<u8> {
    let mut k = history_prefix(class, id);
    k.extend_from_slice(&ts.to_be_bytes());
    k
}

/// The inverse of [`point_key`]; keeps all key-layout knowledge here.
/// Rejects any key whose id region breaks the escape framing instead
/// of guessing at field boundaries. Parsing walks slice patterns, not
/// indices: there is no offset arithmetic to drift.
pub fn split_key(key: &[u8]) -> Result<(Class, Vec<u8>, u64)> {
    if key.len() < 1 + 2 + 8 {
        return Err(KernelError::TruncatedRow);
    }
    let (tag, rest) = key.split_first().ok_or(KernelError::TruncatedRow)?;
    let class = Class::from_tag(*tag)?;
    let (escaped, tail) =
        rest.split_at(rest.len().checked_sub(8).ok_or(KernelError::TruncatedRow)?);
    // The id region must close on the two-byte terminator `[0x00 0x00]`;
    // anything else is not a key this crate wrote.
    match escaped.split_last() {
        Some((0x00, body)) if body.last() == Some(&0x00) => {
            let mut id = Vec::with_capacity(body.len());
            let mut cursor = &body[..body.len() - 1];
            while let Some((byte, rest)) = cursor.split_first() {
                match byte {
                    0x00 => match rest.split_first() {
                        Some((0xFF, after_escape)) => {
                            id.push(0x00);
                            cursor = after_escape;
                        }
                        _ => return Err(KernelError::MalformedKey),
                    },
                    plain => {
                        id.push(*plain);
                        cursor = rest;
                    }
                }
            }
            let ts = u64::from_be_bytes(tail.try_into().expect("tail is exactly 8 bytes"));
            Ok((class, id, ts))
        }
        _ => Err(KernelError::MalformedKey),
    }
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

    #[test]
    fn minimal_key_with_empty_id_roundtrips_and_short_keys_are_rejected() {
        // tag + terminator + ts with a zero-length id is the smallest
        // legal key: exactly 11 bytes, and nothing below parses.
        let key = point_key(Class::Claim, b"", 5);
        assert_eq!(key.len(), 11);
        let (class, id, ts) = split_key(&key).unwrap();
        assert_eq!(class, Class::Claim);
        assert!(id.is_empty());
        assert_eq!(ts, 5);
        assert!(split_key(&key[..10]).is_err(), "10 bytes is not a key");
        assert!(split_key(&key[..8]).is_err(), "8 bytes is not a key");
        assert!(split_key(&[]).is_err());
    }

    #[test]
    fn nested_ids_never_alias_through_the_framed_layout() {
        // 0xFF is a raw byte-prefix of 0xFF 0x00 0xFF, and 0xFF 0x00
        // nests past the would-be terminator; under the old unframed
        // layout their scans bled into each other. The framed layout
        // must keep every pair disjoint (INV-ID-PREFIX-DISJOINT).
        let short = history_prefix(Class::Record, &[0xFF]);
        assert!(!point_key(Class::Record, &[0xFF, 0x00, 0xFF], 1).starts_with(&short));
        assert!(!point_key(Class::Record, &[0xFF, 0x00], 1).starts_with(&short));
        let empty = history_prefix(Class::Record, b"");
        assert!(!point_key(Class::Record, &[0x00], 1).starts_with(&empty));
        for id in [
            &b"\xFF"[..],
            &b"\xFF\x00\xFF"[..],
            &b"\x00"[..],
            &b"\x00\xFF"[..],
            &b"\x00\x00"[..],
            b"",
        ] {
            let (class, back, ts) = split_key(&point_key(Class::Record, id, 7)).unwrap();
            assert_eq!(class, Class::Record);
            assert_eq!(back, id);
            assert_eq!(ts, 7);
        }
    }

    #[test]
    fn op_wire_tags_and_predicates_agree() {
        assert_eq!(Op::Put(vec![1]).wire_tag(), 1);
        assert_eq!(Op::Delete.wire_tag(), 2);
        assert!(Op::Delete.is_delete());
        assert!(!Op::Put(vec![]).is_delete());
        assert!(Op::Delete.payload().is_empty());
    }

    #[test]
    fn point_key_bytes_are_pinned_exactly() {
        // Byte-exact layout pins: tag, escape pairs, terminator, and
        // big-endian ts must each sit exactly where the layout says.
        assert_eq!(
            point_key(Class::Record, b"", 0),
            [0x01, 0x00, 0x00, 0, 0, 0, 0, 0, 0, 0, 0]
        );
        assert_eq!(
            point_key(Class::Record, &[0xFF], 1),
            [0x01, 0xFF, 0x00, 0x00, 0, 0, 0, 0, 0, 0, 0, 1]
        );
        assert_eq!(
            point_key(Class::Record, &[0x00, 0xFF], 2),
            [0x01, 0x00, 0xFF, 0xFF, 0x00, 0x00, 0, 0, 0, 0, 0, 0, 0, 2]
        );
        assert_eq!(history_prefix(Class::Claim, b"x"), [0x05, b'x', 0x00, 0x00]);
    }

    #[test]
    fn malformed_framing_is_rejected_strictly() {
        let good = point_key(Class::Record, &[0xFF], 9);
        assert_eq!(good.len(), 12);
        assert!(split_key(&good).is_ok());

        // Terminator's second byte corrupted: the key no longer closes.
        let mut bad_term = good.clone();
        bad_term[2] = 0xFF;
        assert!(matches!(
            split_key(&bad_term),
            Err(KernelError::MalformedKey)
        ));

        // Bare 0x00 inside the id region, not followed by 0xFF: an
        // escaped zero must always come in pairs.
        let mut bare = point_key(Class::Record, &[0xAB, 0xCD], 9);
        bare[2] = 0x00;
        assert!(matches!(split_key(&bare), Err(KernelError::MalformedKey)));

        // Below the minimum framed length (tag + terminator + ts).
        assert!(matches!(
            split_key(&good[..10]),
            Err(KernelError::TruncatedRow)
        ));
    }

    #[test]
    fn u16_boundary_components_roundtrip_and_overflow_is_rejected() {
        let max: Vec<u8> = vec![0xA5; u16::MAX as usize];
        let over: Vec<u8> = vec![0xA5; u16::MAX as usize + 1];

        let enc = encode_composite(0x7F, &[&max, b"x"]).unwrap();
        let (ns, parts) = decode_composite(&enc).unwrap();
        assert_eq!(ns, 0x7F);
        assert_eq!(parts[0].len(), u16::MAX as usize);

        assert!(matches!(
            encode_composite(0x7F, &[&over]),
            Err(KernelError::ComponentTooLong)
        ));
        assert!(matches!(
            composite_prefix(0x7F, &over),
            Err(KernelError::ComponentTooLong)
        ));
        let pref = composite_prefix(0x7F, &max).unwrap();
        assert_eq!(&pref[3..], max.as_slice());

        // A trailing zero-length component is legal: two header bytes
        // close the stream exactly.
        let tail = encode_composite(0x01, &[b"a", b""]).unwrap();
        let (_, parts) = decode_composite(&tail).unwrap();
        assert_eq!(parts, [b"a".to_vec(), vec![]]);
    }
}
