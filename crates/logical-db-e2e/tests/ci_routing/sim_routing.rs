//! Seeded simulations of the hosted-runner routing gate.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.
//!
//! One seed drives every generated workflow, so a failing seed replays bit
//! for bit and each run is checked by its trace hash.

use super::verify;

/// Seeded generator for the routing simulations (SplitMix64).
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

/// Routes the simulations draw from, with whether the gate must accept each.
const ROUTES: [(&str, bool); 9] = [
    ("ubuntu-24.04", true),
    ("macos-15", true),
    ("windows-2025", true),
    ("\"macos-15\"", true),
    ("ubuntu-24.04 # pinned", true),
    ("ubuntu-latest", false),
    ("self-hosted", false),
    ("\"${{ matrix.os }}\"", false),
    ("[ubuntu-24.04]", false),
];

/// One seeded run of random multi-job workflows judged against the routing
/// model; returns the trace hash.
fn route_run(seed: u64) -> u64 {
    let mut random = Seeded(seed);
    let mut trace = 0xCBF2_9CE4_8422_2325;
    for _ in 0..500 {
        let mut accepted = true;
        let mut lines = vec!["jobs:\n".to_owned()];
        for job in 0..random.between(1, 5) {
            let route = ROUTES[random.below(ROUTES.len())];
            accepted &= route.1;
            lines.push(format!("  job{job}:\n    runs-on: {}\n", route.0));
        }
        let text = lines.concat();
        let passed = verify(&text).is_ok();
        assert_eq!(passed, accepted, "routing model disagrees: {text}");
        trace = fold(trace, &format!("{text}{passed}"));
    }
    trace
}

#[test]
fn sim_seeded_routes_match_the_hosted_model() {
    for seed in 0..8 {
        assert_eq!(route_run(seed), route_run(seed), "seed {seed} replays");
    }
}

/// One seeded run where one job repeats `runs-on`; returns the trace hash.
fn duplicate_run(seed: u64) -> u64 {
    let mut random = Seeded(seed);
    let mut trace = 0xCBF2_9CE4_8422_2325;
    for _ in 0..200 {
        let jobs = random.between(1, 8);
        let twice = random.below(jobs);
        let mut lines = vec!["jobs:\n".to_owned()];
        for job in 0..jobs {
            lines.push(format!("  job{job}:\n    runs-on: macos-15\n"));
            if job == twice {
                let route = ROUTES[random.below(ROUTES.len())].0;
                lines.push(format!("    runs-on: {route}\n"));
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

/// One seeded run checking that the published names do not depend on job
/// order; returns the trace hash.
fn order_run(seed: u64) -> Result<u64, String> {
    let mut random = Seeded(seed);
    let mut trace = 0xCBF2_9CE4_8422_2325;
    for _ in 0..200 {
        let mut jobs: Vec<String> = (0..random.between(1, 6))
            .map(|job| format!("  job{job}:\n    name: check {job}\n    runs-on: ubuntu-24.04\n"))
            .collect();
        let mut forward = verify(&format!("jobs:\n{}", jobs.concat()))?;
        for index in (1..jobs.len()).rev() {
            jobs.swap(index, random.between(0, index));
        }
        let mut shuffled = verify(&format!("jobs:\n{}", jobs.concat()))?;
        forward.sort();
        shuffled.sort();
        assert_eq!(forward, shuffled, "job order changed the published names");
        trace = fold(trace, &format!("{forward:?}{}", jobs.concat()));
    }
    Ok(trace)
}

#[test]
fn sim_seeded_job_order_never_changes_the_names() -> Result<(), String> {
    for seed in 0..8 {
        assert_eq!(order_run(seed)?, order_run(seed)?, "seed {seed} replays");
    }
    Ok(())
}
