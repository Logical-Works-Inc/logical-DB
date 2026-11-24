# Codebook

How code is written here. Each rule names what enforces it. Rules with a test
are in `INVARIANTS.md`. The binding estate contract is the operating contract
the Director owns; this file only records how it lands in this repo.

## Lints

- `[workspace.lints]` in the root `Cargo.toml` is the estate lint floor. Every
  entry is `forbid` except `unsafe_code`, `exit`, `print_stderr` and
  `print_stdout`, which are `deny`. Enforced by `cargo clippy --workspace
  --all-targets --locked -- -D warnings` in CI.
- No `#[allow]` or `#[expect]` in source. Fix the code the lint names.
  Enforced by `allow_attributes` (forbid) and the estate guard.
- `unsafe` lives in one file: `crates/logical-db-daemon/src/fdb_boot.rs`, the
  FoundationDB network boot. That crate copies the lint table with only
  `unsafe_code = "allow"`, registered in the estate guard's lint-exception list.
  Every other crate root states `#![forbid(unsafe_code)]`.
- An unknown lint name in `[lints]` is silently ignored. Check a new name
  against a fake control lint in the same run.

## Errors and values

- Typed error enums with `#[source]` or `#[from]`; no `String` errors in a
  library. `?` at module boundaries.
- No `unwrap`, `expect`, `panic!`, `unreachable!`, `todo!` anywhere, tests
  included. Tests return `Result<(), Box<dyn Error>>` and use `?`.
- No substituted default: `.unwrap_or(..)`, `.unwrap_or_else(..)`,
  `.unwrap_or_default()` are refused by the estate guard. Use `?`, `ok_or`,
  `match` or `let .. else`.
- No dropped error: `let _ =`, `.ok();`, `drop(result)`, `Err(_) =>`,
  `map_err(|_| ..)` are refused. Keep the source in the mapped error.
- Casts: `try_from` with a typed error. `as_conversions` is forbid.
- Arithmetic: `checked_*` or `saturating_*`. Slices: `get`, `first_chunk`,
  `split_at_checked`, never an index.
- A poisoned `Mutex` is a typed error, not a recovered guard.

## API shape

- Fields private by default. No `pub` without a caller outside its crate
  (`codegraph callers`).
- `#[non_exhaustive]` on public types, built through `new`.
- `#[must_use]` where the return value is the point.
- Cursor pagination only. Every list, queue, page and payload has a bound.

## Concurrency

- Async is `lgwks_bot::rt`. No `tokio` or `futures` edge in this repo.
- Every spawn is owned by a `JoinSet` and awaited or shut down. Channels are
  bounded. No lock across `.await`.
- No sleep, `park_timeout` or wall-clock race in tests. A test that waits on a
  child waits on a reported seam (stdout line), as
  `tests/fdb_process_kill_restore.rs` does.

## Tests

- Black-box journeys drive public APIs and real processes: kill, restart,
  corrupt input, dropped network. Half the suite is deterministic simulation
  driven by one seed. The sync simulation is the `sim_sync` bin
  (`crates/logical-db-e2e/src/bin/sim_sync.rs`, its wire in
  `sim_sync/sim_net.rs`); `sim_sync <seed>` replays one seed's trace.
- Every assert checks a real value and carries a message.
- Every private item carries a `///` doc (`missing_docs_in_private_items`).

## Dependencies

Estate first: `std`, then `lgwks_std` (JSON, hash, ids), `lgwks_bot` (async,
HTTP, sockets), `lgwks_deps`. JSON is `lgwks_std::json`; do not declare
`serde`. A third-party crate needs a gap issue on the estate crates repo, cited
on its manifest line. `cargo deny check` refuses advisories, unknown licences
and git sources (`deny.toml`). An edge only tests compile is a dev edge; an
edge only a feature compiles is optional behind that feature (ADR-0007). Cargo
warns on any other unused edge, and a warning is a finding.

## Comments and names

Comments state a constraint the code cannot show. No narration or history.
Names use the industry term for the role: "content-addressed baseline",
"retention floor", "idempotency manifest".

## Banned and replacement

| Banned | Use |
|---|---|
| `unbounded_channel`, `std::sync::mpsc::channel` | `lgwks_bot::rt::sync::mpsc::channel` |
| `tokio::spawn`, detached threads | `JoinSet::spawn`, `std::thread::scope` |
| `thread::sleep`, `park_timeout` | an event or seam to wait on |
| any HTTP client crate (`reqwest`, `hyper` client) | `lgwks_std::http`, through `logical_db::http::HttpTransport` |
| `uuid::` in new code | `lgwks_std::id::Uuid` |
| `"127.0.0.1:0"` strings | `SocketAddr::from((Ipv4Addr::LOCALHOST, 0))` |
| `println!` in a library | return the value; bins write through `writeln!` |
| `process::exit` outside a crash fixture | the typed error |

## What green does not prove

A green suite proves what it ran. It does not prove scale, cost, failover or
absence of what no lint can see (aliasing, false sharing, allocation). Say
"not exercised" where nothing ran.
