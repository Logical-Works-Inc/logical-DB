# Local CI receipt — 862cf43

> Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
> S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
> S33 S34 S35 S36 S37 S38 S39 S42 S43. Source URLs are in `evidence/SOURCES.md`.

Host: Darwin arm64  
rustc: rustc 1.98.1 (48a229cea 2026-09-01)  
Mode: full  
Recorded: 2026-10-05T19:56:20Z

| Gate | Result | Started | Ended |
|---|---|---|---|
| lockfile | pass | 2026-10-05T19:56:12Z | 2026-10-05T19:56:12Z |
| compile | pass | 2026-10-05T19:56:12Z | 2026-10-05T19:56:12Z |
| lint | pass | 2026-10-05T19:56:12Z | 2026-10-05T19:56:12Z |
| format | pass | 2026-10-05T19:56:12Z | 2026-10-05T19:56:13Z |
| tests | **FAIL (exit 101)** | 2026-10-05T19:56:13Z | 2026-10-05T19:56:16Z |
| fdb-check-store | pass | 2026-10-05T19:56:16Z | 2026-10-05T19:56:17Z |
| fdb-check-daemon | pass | 2026-10-05T19:56:17Z | 2026-10-05T19:56:17Z |
| fdb-lint-daemon | pass | 2026-10-05T19:56:17Z | 2026-10-05T19:56:17Z |
| deny | **FAIL (exit 4)** | 2026-10-05T19:56:17Z | 2026-10-05T19:56:18Z |
| domain-contract | pass | 2026-10-05T19:56:18Z | 2026-10-05T19:56:18Z |
| access-graph | pass | 2026-10-05T19:56:18Z | 2026-10-05T19:56:18Z |
| evidence | pass | 2026-10-05T19:56:18Z | 2026-10-05T19:56:19Z |
| ci-routing | pass | 2026-10-05T19:56:19Z | 2026-10-05T19:56:19Z |
| suppressions | pass | 2026-10-05T19:56:19Z | 2026-10-05T19:56:19Z |
| artifacts | pass | 2026-10-05T19:56:19Z | 2026-10-05T19:56:19Z |

passed: 13   failed: 2
