# Logical-DB v3 experience test report

> Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
> S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
> S33 S34 S35 S36 S37 S38 S39 S40 S41 S42 S43. Primary URLs:
> `../evidence/SOURCES.md`.

## Scope

- Change/surface: complete v3 facade, local redb replica, CRDT boundary,
  synchronization protocol, HTTP client/server, FDB service adapter, Braid
  authority adapter, platform placement, and standalone FDB daemon.
- Actor and job: an app author embeds one-person structured state and later
  enables bounded synchronization; an operator runs the same service boundary.
- Environment: macOS arm64, Rust 1.98, isolated temporary redb files, loopback
  HTTP, and an isolated FoundationDB 7.3.77 single-process cluster.
- Public artifacts tested: `logical_db::Database`, `logical_db::http`, the two
  versioned HTTP routes, and the compiled `logical-db-daemon` binary.
- Explicit exclusions: public TLS termination, multi-node FDB failover,
  disk-full injection, sleep/wake, and sustained load remain outside the
  completed observations.

## Experience charter

- Starting state: empty temporary application directories and an empty
  Directory-Layer-owned FDB subspace; no system FDB installation was assumed.
- Visible success outcome: edits survive restart, exact retries do not duplicate,
  offline replicas converge, bounded indexes/events remain coherent, corrupt or
  partial state fails closed, and standalone HTTP requests reach the correct
  authenticated protocol boundary.
- Critical invariants: one atomic local transition, exact operation identity,
  causal convergence, scope isolation, bounded allocation/work, durable resume,
  no credential disclosure, and no physical storage bytes in the app API.
- Likely misunderstandings: `logical-db-server` is a router rather than a
  deployable process; plaintext service binding is loopback-only; a wakeup is
  not a durable subscription; and a foreign Windows compile is not native proof.

## Code-confidence evidence

| Command/check | Result | Boundary |
|---|---|---|
| `python3 scripts/verify-rewrite.py` | PASS; 100% file/blob churn, 96.47% baseline-line churn, zero citation/DAG failures | Mechanical rewrite contract |
| `cargo check --workspace --all-targets --locked` | PASS | Default workspace compile |
| `cargo test --workspace --locked` | PASS; 84 unit/integration tests plus one compile-fail doctest | Unit, HTTP, restart, kill, migration, and state-machine journeys |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | PASS | Warning-free default Rust boundary |
| `cargo clippy -p logical-db-daemon --features fdb --all-targets --locked -- -D warnings` | PASS with staged official FDB client | Warning-free optional composition root |
| `cargo check -p logical-db ... --target x86_64-pc-windows-gnu --all-targets --locked` | PASS for facade, model, platform, protocol, CRDT, store, and sync | Foreign Windows portability only |
| Package mutation campaigns | Zero viable surviving mutants; final live FDB campaign generated 110, caught 95, found 13 unviable, then killed both initial survivors in a four-mutant focused rerun | Test sensitivity, including real transaction closures |
| FDB feature and daemon build against staged 7.3.77 client | PASS | Native optional composition |

## Experience-confidence evidence

| Experiment | Journey/state | Perturbation | Observation | Verdict | Follow-up |
|---|---|---|---|---|---|
| `embedded-recovery-v3` | Fresh embedded replica, edit, query, restart | Exact retry, forced writer kill, planted state damage | Committed state remained atomic; retries were stable; corruption failed closed | Proven within bounds | Native Windows run and disk-full injection |
| `http-two-replica-v3` | Two offline replicas through real loopback HTTP | Concurrent edits, repeated pages, malformed/oversized bodies | Replicas converged; duplicates created no phantom events; resource edges were exact | Proven within bounds | TLS terminator deployment tape |
| `fdb-live-7-3-77` | Empty isolated official FDB cluster | 150 KiB change, eight writers, duplicate/conflict, restart, missing chunk | `accepted=9 duplicate=2 conflict=1 pages=5 restart=1 corruption=1` | Proven within bounds | Force a genuinely ambiguous commit response |
| `daemon-live-fdb` | Compiled standalone binary against live cluster | Missing/wrong media type, missing bearer, malformed authenticated CBOR, SIGINT | Correct 415/401/400 boundaries and graceful exit 0 | Proven within bounds | Production TLS/issuer host |
| `external-facade-fdb` | External facade-only crate, two redb replicas, daemon, real FDB | Process/package/TCP/auth/CBOR/backend boundary | `pushed=1 pulled=1 value=verified`; no internal crate import | Proven within bounds | Land the equivalent content-addressed LEM tape after publication |
| `fdb-ambiguity-v3` | Exact Braid-authorized writes on real FDB | Official client buggify throws after a full-chunk durable commit | `RecoveredUnknownCommit`; one frontier advance; exact duplicate; operation collision rejected; invocation consumed; bounded page reconstructed; scope isolated | Proven within bounds | Repeat in multi-process failover cluster |
| `mutation-boundaries-v3` | Package and live daemon-composition mutation campaigns | Arithmetic, branch, bound, digest, replay, tuple, transaction, and migration mutations | No viable mutant survived; the live workspace campaign exercised FDB accept/page closures | Proven within bounds | Repeat after any storage-transaction change |
| `windows-portability-v3` | Native GitHub Windows Server 2025 plus Windows GNU foreign-target compile | LOCALAPPDATA placement, Unicode, exclusive lock, restart, read-only edit, forced process termination, and loopback HTTP | All runtime tests passed on native Windows; core also cross-compiled locally | Proven within bounds | Repeat on minimum supported desktop Windows image before packaging |

## Friction ledger

| Point | Actor expectation | Observed behavior | Cost/trust effect | Severity |
|---|---|---|---|---|
| Native FDB client discovery | `cargo build --features fdb` finds FDB | macOS needs the official client library staged in the linker/rpath | Operator setup is explicit but manual | Medium |
| Server vs process | Router crate sounds runnable | A separate daemon composition root is required | Fixed by shipping `logical-db-daemon` | Resolved |
| Media type | Generic `application/cbor` seems plausible | The service correctly requires the versioned vendor media type | Error is safe but bodyless | Low |
| Windows proof | Cross-compile looks sufficient | Native file/HTTP/process behavior required a Windows runner | Closed by the pushed native matrix | Resolved |

## Failures and ambiguity

- The first daemon build failed because FDB Directory Layer errors do not
  implement `std::error::Error`. The composition root now maps allocation and
  prefix failures into contextual I/O errors; feature compilation and the live
  startup are the regression evidence.
- The first daemon launch failed at macOS dynamic loading without an FDB rpath.
  Rebuilding with the staged official library rpath succeeded. Packaging the
  native FDB runtime remains an operator/distribution responsibility.
- An authenticated request with generic `application/cbor` returned 415 before
  authentication. Retesting with `application/vnd.logical-db.v1+cbor` produced
  401 without a credential and 400 after valid authentication plus malformed
  CBOR, proving the intended ordering.
- Initial client-buggify runs appeared to show only ordinary accepted/failed
  operations because the adapter collapsed FDB's `MaybeCommitted` retry state.
  `AcceptResult::RecoveredUnknownCommit` now preserves that state. The retained
  official-client run forced the post-commit path, then proved the stored
  operation, invocation marker, and frontier represented one commit.
- The first live FDB mutation campaign left two viable survivors: an `AND` to
  `OR` change in exact retry classification and a full-chunk size comparison.
  The retained ambiguity test now presents a same-operation/different-bytes
  collision and an encoded change spanning a full 64 KiB chunk. A focused
  rerun killed all four mutants generated at those two expressions.

## Claim boundary

- Proven within bounds: embedded durability/convergence on macOS, loopback HTTP,
  bounded protocol behavior, staged FDB 7.3.77 storage, corruption failure, and
  standalone composition/authentication/graceful shutdown, including an external
  facade-only two-replica push/pull journey through the live FDB daemon.
- Failed: no currently accepted journey failed after its regression was added.
- Ambiguous: none inside the completed single-node FDB scope.
- Blocked: external LEM consumption and final synchronized repository tips.
- Untested: multi-node FDB failover, public TLS/issuer integration, sleep/wake,
  disk exhaustion, mobile sandboxes, and sustained production load.

## Final verdict

The public embedded, loopback thin-client, and single-process FDB service paths
are proven within the named macOS/FDB bounds. This report does not label the
rewrite release-complete until the remaining manifest gates—native Windows,
external LEM consumption, final hostile review, CI, and synchronized tips—close.
