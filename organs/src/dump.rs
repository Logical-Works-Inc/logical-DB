//! One functional smoke per donor crate. INV-ORGANS-COMPILE-AND-RUN:
//! a donor that cannot pass here is named in FRANKENSTEIN.md as
//! blocked, never silently dropped.

// ── fjall · LSM keyspace (journal + rotation donor) ─────────

#[test]
fn fjall_keyspace_roundtrips_a_key() {
    let dir = tempfile::tempdir().unwrap();
    let db = fjall::Database::builder(dir.path()).open().unwrap();
    let ks = db
        .keyspace("items", fjall::KeyspaceCreateOptions::default)
        .unwrap();
    ks.insert(b"k1", b"v1").unwrap();
    assert_eq!(ks.get(b"k1").unwrap().as_deref(), Some(b"v1".as_slice()));
}

// ── redb · embedded ACID B-tree (savepoint donor) ───────────

#[test]
fn redb_table_roundtrips_across_a_reopen() {
    let dir = tempfile::tempdir().unwrap();
    const T: redb::TableDefinition<u64, &[u8]> = redb::TableDefinition::new("organ");
    let path = dir.path().join("r.redb");
    {
        let db = redb::Database::create(&path).unwrap();
        let txn = db.begin_write().unwrap();
        {
            let mut t = txn.open_table(T).unwrap();
            t.insert(&7, b"savepoint-discipline".as_slice()).unwrap();
        }
        txn.commit().unwrap();
    }
    let db = redb::Database::open(&path).unwrap();
    let txn = redb::ReadableDatabase::begin_read(&db).unwrap();
    let t = txn.open_table(T).unwrap();
    assert_eq!(t.get(&7).unwrap().unwrap().value(), b"savepoint-discipline");
}

// ── arroy · HNSW-in-KV over LMDB (vector donor) ──────────────
// heed's EnvOpenOptions is the crate's own unsafe boundary; it stays
// confined to this donor smoke, never in ldb-kernel.

#[test]
fn arroy_finds_the_nearest_item_in_a_tiny_index() {
    use arroy::distances::Euclidean;
    use arroy::{Reader, Writer};
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    let dir = tempfile::tempdir().unwrap();
    let env = unsafe {
        heed::EnvOpenOptions::new()
            .map_size(200 * 1024 * 1024)
            .open(dir.path())
    }
    .unwrap();
    let mut wtxn = env.write_txn().unwrap();
    let db: arroy::Database<Euclidean> = env.create_database(&mut wtxn, None).unwrap();
    let writer = Writer::<Euclidean>::new(db, 0, 3);
    writer.add_item(&mut wtxn, 0, &[0.9, 0.1, 0.1]).unwrap();
    writer.add_item(&mut wtxn, 1, &[0.1, 0.9, 0.1]).unwrap();
    writer.add_item(&mut wtxn, 2, &[0.1, 0.1, 0.9]).unwrap();
    let mut rng = StdRng::seed_from_u64(42);
    writer.builder(&mut rng).build(&mut wtxn).unwrap();
    wtxn.commit().unwrap();

    let rtxn = env.read_txn().unwrap();
    let reader = Reader::<Euclidean>::open(&rtxn, 0, db).unwrap();
    let hit = reader.nns(1).by_item(&rtxn, 0).unwrap().unwrap();
    assert_eq!(hit.first().map(|r| r.0), Some(0));
}

// ── fst · finite-state term dictionary (term index donor) ────

#[test]
fn fst_dictionary_answers_membership_and_prefix() {
    let mut build = fst::MapBuilder::memory();
    for (i, term) in ["edge", "record", "term", "vector"].iter().enumerate() {
        build.insert(*term, i as u64).unwrap();
    }
    let map = fst::Map::new(build.into_inner().unwrap()).unwrap();
    assert_eq!(map.get("term"), Some(2));
    assert_eq!(map.get("absent"), None);
    let keys = map.stream().into_str_keys().unwrap();
    assert_eq!(keys, vec!["edge", "record", "term", "vector"]);
}

// ── petgraph · graph algorithms (GNN-query axis donor) ───────

#[test]
fn petgraph_shortest_path_costs_match_hand_math() {
    use petgraph::algo::dijkstra;
    use petgraph::prelude::*;

    let mut g = DiGraph::new();
    let a = g.add_node("a");
    let b = g.add_node("b");
    let c = g.add_node("c");
    g.add_edge(a, b, 2u32);
    g.add_edge(b, c, 3);
    g.add_edge(a, c, 10);
    let costs = dijkstra(&g, a, None, |e| *e.weight());
    assert_eq!(costs[&c], 5, "a→b→c must beat the direct a→c edge");
}

// ── memmap2 · read-only mapping (INV-MMAP-IS-THE-CACHE) ──────

#[test]
fn mapped_segment_reads_the_bytes_we_wrote() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("seg.bin");
    std::fs::write(&path, b"IMMUTABLE-SEGMENT").unwrap();
    let file = std::fs::File::open(&path).unwrap();
    let map = unsafe { memmap2::Mmap::map(&file).unwrap() };
    assert_eq!(&map[..9], b"IMMUTABLE");
}

// ── half · f16/bf16 codes (quantized payload types) ──────────

#[test]
fn half_preserves_value_through_f16_roundtrip() {
    let v = 0.625f32; // exactly representable in f16
    let q = half::f16::from_f32(v);
    assert_eq!(q.to_f32(), v);
}
