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

/// One random route from `ROUTES`.
fn pick(random: &mut Seeded) -> Result<(&'static str, bool), String> {
    ROUTES
        .get(random.below(ROUTES.len()))
        .copied()
        .ok_or_else(|| String::from("the route table is empty"))
}

/// One seeded run of random multi-job workflows judged against the routing
/// model; returns the trace hash.
fn route_run(seed: u64) -> Result<u64, String> {
    let mut random = Seeded(seed);
    let mut trace = 0xCBF2_9CE4_8422_2325;
    for _ in 0..500 {
        let mut accepted = true;
        let mut lines = vec!["jobs:\n".to_owned()];
        for job in 0..random.between(1, 5) {
            let route = pick(&mut random)?;
            accepted &= route.1;
            lines.push(format!("  job{job}:\n    runs-on: {}\n", route.0));
        }
        let text = lines.concat();
        let passed = verify(&text).is_ok();
        assert_eq!(passed, accepted, "routing model disagrees: {text}");
        trace = fold(trace, &format!("{text}{passed}"));
    }
    Ok(trace)
}

/// Trace hashes the eight routing seeds are recorded to produce.
const ROUTE_TRACES: [u64; 8] = [
    15_527_158_201_742_439_996,
    11_610_121_709_553_523_693,
    1_860_396_468_875_387_211,
    9_488_995_942_999_093_036,
    17_054_839_118_388_252_087,
    15_018_096_628_801_540_247,
    13_779_219_139_797_196_038,
    12_165_654_579_172_442_523,
];

#[test]
fn sim_seeded_routes_match_the_hosted_model() -> Result<(), String> {
    let traces = (0..8).map(route_run).collect::<Result<Vec<_>, _>>()?;
    assert_eq!(traces, ROUTE_TRACES, "the seeded routing traces drifted");
    Ok(())
}

/// One seeded run where one job repeats `runs-on`; returns the trace hash.
fn duplicate_run(seed: u64) -> Result<u64, String> {
    let mut random = Seeded(seed);
    let mut trace = 0xCBF2_9CE4_8422_2325;
    for _ in 0..200 {
        let jobs = random.between(1, 8);
        let twice = random.below(jobs);
        let mut lines = vec!["jobs:\n".to_owned()];
        for job in 0..jobs {
            lines.push(format!("  job{job}:\n    runs-on: macos-15\n"));
            if job == twice {
                let route = pick(&mut random)?.0;
                lines.push(format!("    runs-on: {route}\n"));
            }
        }
        let text = lines.concat();
        assert!(verify(&text).is_err(), "a repeated route passed: {text}");
        trace = fold(trace, &text);
    }
    Ok(trace)
}

/// Trace hashes the eight duplicate-route seeds are recorded to produce.
const DUPLICATE_TRACES: [u64; 8] = [
    5_534_339_750_850_967_104,
    4_225_096_689_064_756_014,
    2_534_249_685_225_342_736,
    12_934_025_125_381_992_754,
    10_169_927_609_492_529_383,
    13_155_441_310_882_868_163,
    13_219_231_022_566_044_647,
    6_105_573_747_571_741_408,
];

#[test]
fn sim_seeded_duplicate_routes_are_always_refused() -> Result<(), String> {
    let traces = (0..8).map(duplicate_run).collect::<Result<Vec<_>, _>>()?;
    assert_eq!(
        traces, DUPLICATE_TRACES,
        "the seeded duplicate-route traces drifted"
    );
    Ok(())
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

/// Trace hashes the eight job-order seeds are recorded to produce.
const ORDER_TRACES: [u64; 8] = [
    3_832_367_768_932_028_718,
    13_272_724_353_154_442_240,
    7_342_661_507_178_649_085,
    1_148_926_430_498_069_578,
    16_407_217_790_768_270_686,
    7_283_517_801_777_653_501,
    7_830_018_899_742_664_031,
    164_595_972_867_186_719,
];

#[test]
fn sim_seeded_job_order_never_changes_the_names() -> Result<(), String> {
    let traces = (0..8).map(order_run).collect::<Result<Vec<_>, _>>()?;
    assert_eq!(traces, ORDER_TRACES, "the seeded order traces drifted");
    Ok(())
}
