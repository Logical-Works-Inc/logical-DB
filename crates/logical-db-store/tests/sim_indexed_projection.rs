//! Seeded deterministic simulations for the in-memory indexed projection.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.
//!
//! Each test runs its trace twice from one seed and asserts the same trace
//! hash. The seed drives every input: row identities, term choices, query
//! texts, vector seeds, and operation order. No wall clock, no I/O, no
//! unseeded randomness.

use std::collections::BTreeSet;

use logical_db_store::projection_indexed::{IndexedProjection, tokenize};

#[path = "sim_harness.rs"]
mod harness;
use harness::{check, count, opened, seed_vec, test_row, trace_tag};

/// Maximum rows any sim in this file materializes.
const MAX_ROWS: usize = 2_000;
/// Trace domain: this file's traces never collide with another file's.
const TRACE_DOMAIN: &[u8] = b"ldb.sim.indexed.v1";

/// One deterministic term from a word id.
///
/// # Errors
///
/// Returns a dispatch overflow that cannot happen for the fixed modulus.
fn term(word: u64) -> Result<String, Box<dyn std::error::Error>> {
    let lane = word.checked_rem(37).ok_or("sim term dispatch overflow")?;
    Ok(format!("w{lane}"))
}

/// Inserts grow the index and top-k bounds every answer.
#[test]
fn sim_insert_growth_bounds_top_k() -> Result<(), Box<dyn std::error::Error>> {
    check([0x11AA_0001_u64, 0x11AA_0002_u64], insert_growth_trace)
}

/// Trace for [`sim_insert_growth_bounds_top_k`].
fn insert_growth_trace(seed: u64) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    let (mut rng, mut hasher) = opened(seed, TRACE_DOMAIN, b"insert-growth");
    let mut projection = IndexedProjection::new();
    let rows = count(&mut rng, 50, 150)?.min(MAX_ROWS);
    for idx in 0..rows {
        let hash =
            test_row(u64::try_from(idx).map_err(|error| format!("sim row overflow: {error}"))?);
        let terms = vec![
            term(rng.next_u64())?,
            term(rng.next_u64())?,
            String::from("shared"),
        ];
        projection.insert(hash, terms);
        hasher.update(&hash);
    }
    assert_eq!(projection.len(), rows, "every insert lands");
    for top_k in [1_usize, 5, 8, 64, 500] {
        let results = projection.query("shared", &[], top_k)?;
        assert!(
            results.len() <= top_k.min(64),
            "top-k {top_k} must bound the answer"
        );
        for scored in &results {
            hasher.update(&scored.hash);
            hasher.update(&scored.score.to_le_bytes());
        }
    }
    Ok(trace_tag(hasher))
}

/// Reinserting one hash replaces its terms; the row count never moves.
#[test]
fn sim_reinsert_replaces_terms_idempotently() -> Result<(), Box<dyn std::error::Error>> {
    check([0x11BB_0001_u64, 0x11BB_0002_u64], reinsert_trace)
}

/// Trace for [`sim_reinsert_replaces_terms_idempotently`].
fn reinsert_trace(seed: u64) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    let (mut rng, mut hasher) = opened(seed, TRACE_DOMAIN, b"reinsert");
    let mut projection = IndexedProjection::new();
    let hash = test_row(rng.next_u64());
    let rounds = count(&mut rng, 5, 20)?;
    for round in 0..rounds {
        let lane = u64::try_from(round).map_err(|error| format!("sim round overflow: {error}"))?;
        let terms = vec![term(lane)?, term(lane.wrapping_add(1))?];
        projection.insert(hash, terms.clone());
        assert_eq!(projection.len(), 1, "reinsert must not duplicate the row");
        for text in &terms {
            hasher.update(text.as_bytes());
        }
        let results = projection.query(&terms[0], &[], 8)?;
        assert_eq!(results.len(), 1, "the newest term must match");
        assert_eq!(results[0].hash, hash, "the reinserted row ranks first");
    }
    let stale = projection.query("w999_not_indexed", &[], 8)?;
    assert!(stale.is_empty(), "never-indexed terms match nothing");
    Ok(trace_tag(hasher))
}

/// An empty index answers nothing on every query shape.
#[test]
fn sim_empty_index_answers_nothing() -> Result<(), Box<dyn std::error::Error>> {
    check([0x11CC_0001_u64, 0x11CC_0002_u64], empty_trace)
}

/// Trace for [`sim_empty_index_answers_nothing`].
fn empty_trace(seed: u64) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    let (mut rng, mut hasher) = opened(seed, TRACE_DOMAIN, b"empty");
    let projection = IndexedProjection::new();
    assert!(projection.is_empty(), "a fresh index is empty");
    assert_eq!(projection.len(), 0, "a fresh index holds no rows");
    let texts = ["", "shared", "w3", "Hello, World!", "zzz-nope"];
    for text in texts {
        let seed_len = harness::count(&mut rng, 0, 17)?;
        let seed_vec = seed_vec(&mut rng, seed_len)?;
        let results = projection.query(text, &seed_vec, 8)?;
        assert!(
            results.is_empty(),
            "empty index answers nothing for {text:?}"
        );
        hasher.update(text.as_bytes());
    }
    let full = projection.query("", &[1_i32; 16], 8)?;
    assert!(
        full.is_empty(),
        "a live seed still answers nothing when empty"
    );
    Ok(trace_tag(hasher))
}

/// Query answers are stable across repeated calls and reorderings of calls.
#[test]
fn sim_query_ordering_is_stable() -> Result<(), Box<dyn std::error::Error>> {
    check([0x11DD_0001_u64, 0x11DD_0002_u64], ordering_trace)
}

/// Trace for [`sim_query_ordering_is_stable`].
fn ordering_trace(seed: u64) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    let (mut rng, mut hasher) = opened(seed, TRACE_DOMAIN, b"ordering");
    let mut projection = IndexedProjection::new();
    for idx in 0..count(&mut rng, 30, 70)? {
        let row_no = u64::try_from(idx).map_err(|error| format!("sim row overflow: {error}"))?;
        let hash = test_row(row_no);
        let third = row_no
            .checked_div(3)
            .ok_or("sim third-term dispatch overflow")?;
        projection.insert(
            hash,
            vec![term(row_no)?, term(third)?, String::from("shared")],
        );
    }
    // Ask the same queries in two different orders; both orders agree.
    let mut queries = Vec::new();
    for _ in 0..12 {
        let pick = rng
            .below(2)
            .map_err(|error| format!("sim pick overflow: {error}"))?;
        let text = if pick == 0 {
            term(rng.next_u64())?
        } else {
            String::from("shared")
        };
        let seed_len = harness::count(&mut rng, 0, 5)?;
        let seed_vec = seed_vec(&mut rng, seed_len)?;
        queries.push((text, seed_vec));
    }
    let mut forward = Vec::new();
    for pair in &queries {
        forward.push(projection.query(&pair.0, &pair.1, 8)?);
    }
    let mut backward = Vec::new();
    for pair in queries.iter().rev() {
        backward.push(projection.query(&pair.0, &pair.1, 8)?);
    }
    backward.reverse();
    assert_eq!(forward, backward, "query order must not change answers");
    for results in &forward {
        let count_len = u64::try_from(results.len())
            .map_err(|error| format!("sim answer length overflow: {error}"))?;
        hasher.update(&count_len.to_le_bytes());
        for scored in results {
            hasher.update(&scored.hash);
            hasher.update(&scored.score.to_le_bytes());
        }
    }
    Ok(trace_tag(hasher))
}

/// Tokenizer edge inputs never panic and stay deterministic.
#[test]
fn sim_tokenizer_edge_inputs() -> Result<(), Box<dyn std::error::Error>> {
    check([0x11EE_0001_u64, 0x11EE_0002_u64], tokenizer_trace)
}

/// Trace for [`sim_tokenizer_edge_inputs`].
fn tokenizer_trace(seed: u64) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    let (mut rng, mut hasher) = opened(seed, TRACE_DOMAIN, b"tokenizer");
    let alphabet = [
        "hello",
        "WORLD",
        "w3",
        "a-b",
        "x_y",
        "caf\u{e9}",
        "\u{4e2d}\u{6587}",
        "  ",
        "!!!",
        "a1b2c3",
    ];
    for _ in 0..40 {
        let parts = count(&mut rng, 0, 5)?;
        let mut input = String::new();
        for _ in 0..parts {
            let alphabet_len = u64::try_from(alphabet.len())
                .map_err(|error| format!("sim alphabet overflow: {error}"))?;
            let word_draw = rng
                .below(alphabet_len)
                .map_err(|error| format!("sim alphabet overflow: {error}"))?;
            let word = usize::try_from(word_draw)
                .map_err(|error| format!("sim alphabet overflow: {error}"))?;
            input.push_str(alphabet[word]);
            let gap = rng
                .below(2)
                .map_err(|error| format!("sim gap overflow: {error}"))?;
            input.push(if gap == 0 { ' ' } else { ',' });
        }
        let first = tokenize(&input);
        let second = tokenize(&input);
        assert_eq!(
            first, second,
            "tokenizer must be deterministic for {input:?}"
        );
        let mut sorted = first.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(first, sorted, "tokenizer output stays sorted and deduped");
        for token in &first {
            assert!(!token.is_empty(), "no empty tokens");
            hasher.update(token.as_bytes());
        }
    }
    Ok(trace_tag(hasher))
}

/// With an all-zero vector seed, text overlap alone orders results.
#[test]
fn sim_text_overlap_orders_results() -> Result<(), Box<dyn std::error::Error>> {
    check([0x11FF_0001_u64, 0x11FF_0002_u64], overlap_trace)
}

/// Trace for [`sim_text_overlap_orders_results`].
fn overlap_trace(seed: u64) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    let (mut rng, mut hasher) = opened(seed, TRACE_DOMAIN, b"overlap");
    let mut projection = IndexedProjection::new();
    let pool = ["alpha", "beta", "gamma", "delta"];
    let mut rows = Vec::new();
    for idx in 0..count(&mut rng, 20, 30)? {
        let row_no = u64::try_from(idx).map_err(|error| format!("sim row overflow: {error}"))?;
        let hash = test_row(1_000_u64.saturating_add(row_no));
        let take = count(&mut rng, 1, 3)?;
        let skip_draw = rng
            .below(4)
            .map_err(|error| format!("sim skip overflow: {error}"))?;
        let skip =
            usize::try_from(skip_draw).map_err(|error| format!("sim skip overflow: {error}"))?;
        let terms: Vec<String> = pool
            .iter()
            .cycle()
            .skip(skip)
            .take(take)
            .map(|text| String::from(*text))
            .collect();
        projection.insert(hash, terms.clone());
        rows.push((hash, terms));
    }
    // Independent oracle: overlap count descends, row hash breaks ties.
    let query = "alpha beta";
    let query_terms = tokenize(query);
    let results = projection.query(query, &[], 64)?;
    let mut expected: Vec<([u8; 32], usize)> = rows
        .iter()
        .map(|row| {
            let set: BTreeSet<&str> = row.1.iter().map(String::as_str).collect();
            let overlap = query_terms
                .iter()
                .filter(|text| set.contains(text.as_str()))
                .count();
            (row.0, overlap)
        })
        .filter(|row| row.1 > 0)
        .collect();
    expected.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    let got: Vec<[u8; 32]> = results.iter().map(|scored| scored.hash).collect();
    let want: Vec<[u8; 32]> = expected.iter().map(|row| row.0).collect();
    assert_eq!(got, want, "overlap ordering must match the oracle");
    // Scores are nonzero exactly where overlap is nonzero.
    for scored in &results {
        assert!(scored.score > 0, "a matching row carries a live score");
        hasher.update(&scored.hash);
        hasher.update(&scored.score.to_le_bytes());
    }
    Ok(trace_tag(hasher))
}

/// Pure-vector mode returns every row and ranks deterministically.
#[test]
fn sim_pure_vector_mode_covers_all_rows() -> Result<(), Box<dyn std::error::Error>> {
    check([0x12AA_0001_u64, 0x12AA_0002_u64], vector_mode_trace)
}

/// Trace for [`sim_pure_vector_mode_covers_all_rows`].
fn vector_mode_trace(seed: u64) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    let (mut rng, mut hasher) = opened(seed, TRACE_DOMAIN, b"vector-mode");
    let mut projection = IndexedProjection::new();
    let rows = count(&mut rng, 10, 40)?;
    for idx in 0..rows {
        let row_no = u64::try_from(idx).map_err(|error| format!("sim row overflow: {error}"))?;
        projection.insert(
            test_row(2_000_u64.saturating_add(row_no)),
            vec![term(row_no)?],
        );
    }
    let live_seed = seed_vec(&mut rng, 16)?;
    let results = projection.query("", &live_seed, 64)?;
    assert_eq!(results.len(), rows, "pure-vector mode covers every row");
    let again = projection.query("", &live_seed, 64)?;
    assert_eq!(results, again, "the ranking repeats exactly");
    // Scores descend along the answer.
    for window in results.windows(2) {
        assert!(
            window[0].score >= window[1].score,
            "scores must descend in rank order"
        );
    }
    for scored in &results {
        hasher.update(&scored.hash);
        hasher.update(&scored.score.to_le_bytes());
    }
    // An all-zero seed with no terms answers nothing by construction.
    let nothing = projection.query("", &[], 8)?;
    assert!(
        nothing.is_empty(),
        "zero seed plus zero terms ranks nothing"
    );
    Ok(trace_tag(hasher))
}

/// Ten thousand rows still answer within the top-k bound.
#[test]
fn sim_adversarial_ten_thousand_rows() -> Result<(), Box<dyn std::error::Error>> {
    check([0x12BB_0001_u64, 0x12BB_0002_u64], adversarial_trace)
}

/// Trace for [`sim_adversarial_ten_thousand_rows`].
fn adversarial_trace(seed: u64) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    let (mut rng, mut hasher) = opened(seed, TRACE_DOMAIN, b"adversarial-10k");
    let mut projection = IndexedProjection::new();
    for idx in 0..10_000_u64 {
        // Adversarial skew: nine of ten rows share one hot term.
        let hot_lane = idx.checked_rem(10).ok_or("sim skew dispatch overflow")?;
        let terms = if hot_lane == 0 {
            vec![format!("lonely{idx}"), term(rng.next_u64())?]
        } else {
            vec![String::from("hot"), term(rng.next_u64())?]
        };
        projection.insert(test_row(idx), terms);
    }
    assert_eq!(projection.len(), 10_000, "all ten thousand rows land");
    let hot = projection.query("hot", &[], 10)?;
    assert_eq!(hot.len(), 10, "the hot term stays top-k bounded");
    let lonely = 10_000_u64
        .checked_sub(10)
        .ok_or("adversarial lonely probe underflow")?;
    let probe = format!("lonely{lonely}");
    let found = projection.query(&probe, &[], 8)?;
    assert_eq!(found.len(), 1, "the lonely term still resolves");
    let hot_len =
        u64::try_from(hot.len()).map_err(|error| format!("sim hot count overflow: {error}"))?;
    hasher.update(&hot_len.to_le_bytes());
    for scored in hot.iter().chain(found.iter()) {
        hasher.update(&scored.hash);
        hasher.update(&scored.score.to_le_bytes());
    }
    Ok(trace_tag(hasher))
}
