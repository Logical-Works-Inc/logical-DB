//! Seeded simulations of the CI routing contract.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.
//!
//! One seed drives every generated workflow, so a failing seed replays bit
//! for bit and each run is checked by its trace hash.

use crate::ci_routing::verify;
use crate::workspace::ContractError;

/// Seeded generator for the routing simulations (SplitMix64, so one seed
/// replays the same workflows bit for bit).
struct Seeded(u64);

impl Seeded {
    /// The next 64 random bits.
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut mixed = self.0;
        mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        mixed ^ (mixed >> 31)
    }

    /// A value in `0..bound`, or zero when `bound` is zero.
    fn below(&mut self, bound: usize) -> usize {
        let draw = self.next();
        let Ok(wide) = u64::try_from(bound) else {
            return 0;
        };
        let Some(rest) = draw.checked_rem(wide) else {
            return 0;
        };
        let Ok(value) = usize::try_from(rest) else {
            return 0;
        };
        value
    }

    /// A value in `low..=high`.
    fn between(&mut self, low: usize, high: usize) -> usize {
        let span = high.saturating_sub(low).saturating_add(1);
        low.saturating_add(self.below(span))
    }
}

/// Folds one step into an FNV-1a trace hash.
fn fold(trace: u64, step: &str) -> u64 {
    step.bytes().fold(trace, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01B3)
    })
}

/// Runner labels the simulations draw from, with whether each is a literal
/// string label and whether it is the self-hosted marker.
const LABELS: [(&str, bool, bool); 8] = [
    ("self-hosted", true, true),
    ("macOS", true, false),
    ("ARM64", true, false),
    ("ubuntu-latest", true, false),
    ("Linux", true, false),
    ("\"${{ matrix.os }}\"", false, false),
    ("null", false, false),
    ("\"self-hosted\"", true, true),
];

/// One seeded run of random label lists judged against the routing model;
/// returns the trace hash.
fn label_run(seed: u64) -> u64 {
    let mut random = Seeded(seed);
    let mut trace = 0xCBF2_9CE4_8422_2325;
    for _ in 0..500 {
        let count = random.between(1, 5);
        let mut labels = Vec::with_capacity(count);
        let (mut literal, mut hosted) = (true, false);
        for _ in 0..count {
            let pick = LABELS[random.below(LABELS.len())];
            labels.push(pick.0);
            literal &= pick.1;
            hosted |= pick.2;
        }
        let text = format!("jobs:\n  sim:\n    runs-on: [{}]\n", labels.join(", "));
        let passed = verify(&text).is_ok();
        assert_eq!(passed, literal && hosted, "routing model disagrees: {text}");
        trace = fold(trace, &format!("{text}{passed}"));
    }
    trace
}

#[test]
fn sim_seeded_label_lists_match_the_routing_model() {
    for seed in 0..8 {
        assert_eq!(label_run(seed), label_run(seed), "seed {seed} replays");
    }
}

/// One seeded run of multi-job workflows where one job repeats `runs-on`;
/// returns the trace hash.
fn duplicate_run(seed: u64) -> u64 {
    let mut random = Seeded(seed);
    let mut trace = 0xCBF2_9CE4_8422_2325;
    for _ in 0..200 {
        let jobs = random.between(1, 8);
        let twice = random.below(jobs);
        let mut lines = vec!["jobs:\n".to_owned()];
        for job in 0..jobs {
            lines.push(format!("  job{job}:\n    runs-on: [self-hosted]\n"));
            if job == twice {
                let route = LABELS[random.below(LABELS.len())].0;
                lines.push(format!("    runs-on: [{route}]\n"));
            }
        }
        let text = lines.concat();
        assert!(verify(&text).is_err(), "a repeated route passed: {text}");
        trace = fold(trace, &text);
    }
    trace
}

#[test]
fn sim_seeded_duplicate_routes_are_always_refused() {
    for seed in 0..8 {
        assert_eq!(
            duplicate_run(seed),
            duplicate_run(seed),
            "seed {seed} replays"
        );
    }
}

/// One seeded run checking that the job count does not depend on job order;
/// returns the trace hash.
fn order_run(seed: u64) -> Result<u64, ContractError> {
    let mut random = Seeded(seed);
    let mut trace = 0xCBF2_9CE4_8422_2325;
    for _ in 0..200 {
        let mut jobs: Vec<String> = (0..random.between(1, 6))
            .map(|job| format!("  job{job}:\n    runs-on: [self-hosted, macOS]\n"))
            .collect();
        let forward = verify(&format!("jobs:\n{}", jobs.concat()))?;
        for index in (1..jobs.len()).rev() {
            jobs.swap(index, random.between(0, index));
        }
        let shuffled = verify(&format!("jobs:\n{}", jobs.concat()))?;
        assert_eq!(forward, shuffled, "job order changed the count");
        trace = fold(trace, &format!("{forward}{}", jobs.concat()));
    }
    Ok(trace)
}

#[test]
fn sim_seeded_job_order_never_changes_the_verdict() -> Result<(), ContractError> {
    for seed in 0..8 {
        assert_eq!(order_run(seed)?, order_run(seed)?, "seed {seed} replays");
    }
    Ok(())
}
