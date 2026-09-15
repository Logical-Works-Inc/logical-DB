# Local verification receipt: FDB composition lane and workspace gates

> Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
> S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
> S33 S34 S35 S36 S37 S38 S39 S40 S41 S42 S43. Source URLs are in
> `SOURCES.md`.

This is an author-produced execution receipt for PR #55. It records what ran,
where, at which revision, and with which exact result. It is not independent
reproduction and does not substitute for the hosted or self-hosted lane.
Statements below separate **passed** (executed, exit code observed), **not
run** (not executed in this environment), and **not evidenced** (no machine
could execute them).

## Revision and environment

| Item | Value |
|---|---|
| Tested revision (Rust + lockfile) | `598121251b8e389809d01aa1b2198684a29cde88` |
| Repository state at test time | clean working copy; the only later commit adds this receipt file and the GOVERNANCE corrections, no Rust or lockfile bytes |
| Rust identity | `rustc 1.98.0 (88d9e12ae 2026-08-18)`, host `aarch64-apple-darwin` |
| Lockfile delta vs PR base | `rustls 0.23.43 -> 0.23.45` (RUSTSEC-2026-0285 fix), no other package changed |
| FoundationDB assets | official `FoundationDB-7.3.77_arm64.pkg`, sha256 `aadeb7e4e530fdbefcf10255c45f4022165325ac43c4535a05559ed6d9ea1a37`, checksum-verified before extraction |
| Cluster | `fdbserver 7.3.77`, single-memory mode, `127.0.0.1:4659`, `LOGICAL_DB_FDB_CLUSTER_FILE` exported |
| Link flags | `RUSTFLAGS="-L native=<libdir> -C link-arg=-Wl,-rpath,<libdir>"` against the extracted `libfdb_c.dylib` |

## Lane map against `.github/workflows/ci.yml`

| CI lane | Status | Observed result |
|---|---|---|
| `clean-room evidence` (`scripts/verify-rewrite.py`) | **passed** | exit 0, `PASS clean-room rewrite evidence gates` |
| `quality` / workspace check (`cargo check --workspace --all-targets --locked`) | **passed** | exit 0 |
| `quality` / dependency register (`cargo test -p logical-db-e2e --locked --test dependency_contract`) | **passed** | exit 0, 10 passed / 0 failed |
| `quality` / workspace tests (`cargo test --workspace --locked`) | **passed** | exit 0, 308 passed / 0 failed |
| `quality` / workspace clippy (`cargo clippy --workspace --all-targets --locked -- -D warnings`) | **passed** | exit 0 |
| `format` (`cargo fmt --all -- --check`) | **passed** | exit 0 |
| `supply-chain` (`cargo deny check`) | **passed after repair** | failed on RUSTSEC-2026-0285 at rustls 0.23.43; exit 0 after the lockfile patch to 0.23.45; no `deny.toml` detector changed |
| `package-isolation` (15 crates, `cargo check -p <crate> --all-targets --locked`) | **passed** | 15/15 OK |
| `fdb-composition` check + clippy (`-p logical-db-daemon`, `-p logical-db-e2e`, features `fdb`) | **passed** | exit 0 both |
| `fdb-composition` unit tests (`-p logical-db-daemon`, `-p logical-db-store-fdb`, features `fdb`) | **passed** | 3+8 and 11+0, all ok, exit 0 |
| `fdb-composition` live journey (`fdb-ambiguity-journey`) | **passed** | `PASS fdb-commit-unknown-recovered attempt=2 frontier=5 invocation=consumed page=bounded scope=isolated` |
| `fdb-composition` recovery composition (`--test fdb_recovery_composition`) | **passed** | 1 passed / 0 failed, live cluster |
| `fdb-composition` process kill restore (`--test fdb_process_kill_restore`) | **passed** | 1 passed / 0 failed, live cluster |
| `quality` on `ubuntu-latest` | **not evidenced** | no Linux machine in this environment |
| `quality` on `windows-latest` | **not evidenced** | no Windows machine in this environment |
| all hosted runs of every lane | **not evidenced** | GitHub Actions billing-blocked; every run fails in seconds with zero steps |

## New falsification record (P3 hardening)

The recovery-refusal test was proven to have teeth against the final code:

| mutation | result |
|---|---|
| as committed (`59812125`) | **GREEN**: `recovery_is_refused_before_the_baseline_store_is_contacted` ok, 403 Forbidden |
| `snapshot_baseline` moved above `ProtocolService::recover` authorization (temporary edit, reverted) | **RED**: RefusedStore sentinel panics (`main.rs:338`), test fails, exit 101 |

The allowed `frontier()` read before authorization is preserved and asserted
implicitly: the sentinel for retention methods never fires on the passing run.

## Prior local runs recorded in this PR

The earlier receipts at `11b585df` (attempt 3, frontier 6) and at the
pre-hardening head remain valid for the code they tested; every result above
supersedes them for the current head. All runs executed on the Director's
development machine; the ThinkPad Ryzen 3 acceptance floor remains unmeasured
and no portability claim is made beyond the lanes listed here.

## Follow-up: self-hosted runner execution (2026-09-15)

The `logical-db-local` self-hosted runner executed the two new lanes on the
PR head. Executed runs, all against the exact PR head `4e07c7e9`:

| run | lanes | result |
|---|---|---|
| [34910156966](https://github.com/srinji-kaggss/logical-DB/actions/runs/34910156966) | selfhosted-quality, selfhosted-fdb-composition | **both success** (first execution; hosted lanes billing-blocked) |
| [34910937956](https://github.com/srinji-kaggss/logical-DB/actions/runs/34910937956) | selfhosted-quality, selfhosted-fdb-composition | **both success** (workflow_dispatch) |
| [34912714579](https://github.com/srinji-kaggss/logical-DB/actions/runs/34912714579) | selfhosted-quality, selfhosted-fdb-composition | **both success** (workflow_dispatch) |
| [34914317662](https://github.com/srinji-kaggss/logical-DB/actions/runs/34914317662) | selfhosted-quality | failed once (runner connection drop mid-job, job cancelled), **success on rerun**; selfhosted-fdb-composition **success** |

The one failure was a transient runner connection drop 30 seconds into the
workspace-test step; the identical step passed on rerun and in both prior
runs. Root cause of the drop is unresolved from retained evidence.

Runner lifecycle (machine-side, not repository content): the runner is
ephemeral and on-demand. It is registered fresh for each run, takes one job,
deregisters itself, and exits; a launchd watcher starts it only when a queued
job carries the self-hosted label and kills it after 30 minutes without job
activity. Between runs there is no listener process, no registration, and no
local state. Idle RAM cost of the logical-db runner: zero.
