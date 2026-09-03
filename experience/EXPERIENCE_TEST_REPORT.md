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
| `cargo check --workspace --all-targets --all-features --locked` | PASS | Default and optional Rust composition compile |
| `cargo test --workspace --locked` | PASS; 105 unit/integration tests plus one compile-fail doctest | Unit, HTTP, restart, kill, migration, state-machine, topology, conflict, and corruption journeys |
| `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings` | PASS | Warning-free default and optional Rust boundary |
| `cargo deny check` | PASS; advisories, bans, licenses, and sources | Yank/advisory/license/source policy; duplicate versions warn |
| `actionlint -no-color` | PASS | GitHub workflow syntax and expressions |
| Focused `logical-db-sync` mutation campaign | 31 generated; 30 caught and one unviable after closing two initial survivors | Sync progress, exact page budget, backlog, and rejection sensitivity |
| Focused hardening mutation campaign | 26 generated; 20 caught, five unviable, and the one initial confirmation survivor caught on focused rerun | Braid effect/taint/confirmation, topology migration, retry classification, and retry-delay guards |
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
| `external-facade-fdb` | External facade-only crate, two redb replicas, daemon, real FDB | Process/package/TCP/auth/CBOR/backend boundary | Historical pass invalidated: its local authority fabricated Braid's confirmation flag | Retired; not current evidence | Repeat only with independently verifiable confirmation |
| `fdb-ambiguity-v3` | Exact fixture grants against real FDB | Official client buggify throws after a full-chunk durable commit | `RecoveredUnknownCommit`; one frontier advance; exact duplicate; operation collision rejected; invocation consumed; bounded page reconstructed; scope isolated | FDB semantics proven; not Braid confirmation | Repeat in multi-process failover cluster |
| `mutation-boundaries-v3` | Package and live daemon-composition mutation campaigns | Arithmetic, branch, bound, digest, replay, tuple, transaction, and migration mutations | No viable mutant survived; the live workspace campaign exercised FDB accept/page closures | Proven within bounds | Repeat after any storage-transaction change |
| `windows-portability-v3` | Native GitHub Windows Server 2025 plus Windows GNU foreign-target compile | LOCALAPPDATA placement, Unicode, exclusive lock, restart, read-only edit, forced process termination, and loopback HTTP | All runtime tests passed on native Windows; core also cross-compiled locally | Proven within bounds | Repeat on minimum supported desktop Windows image before packaging |
| `hardening-delta-v3` | Existing embedded/thin/service surfaces | Expired checkpoint with one-page budget, upload backlog, second upstream, malformed/rejected changes, concurrent same-field values, orphaned index/event rows, relative native root, invalid bearer, service saturation | Every case produced progress, visible backlog/conflict, or a typed fail-closed result; no silent acceptance | Proven locally; native matrix pending | Run the pushed Windows and live-FDB jobs, then retain links |

## Friction ledger

| Point | Actor expectation | Observed behavior | Cost/trust effect | Severity |
|---|---|---|---|---|
| Native FDB client discovery | `cargo build --features fdb` finds FDB | macOS needs the official client library staged in the linker/rpath | Operator setup is explicit but manual | Medium |
| Server vs process | Router crate sounds runnable | A separate daemon composition root is required | Fixed by shipping `logical-db-daemon` | Resolved |
| Media type | Generic `application/cbor` seems plausible | The service correctly requires the versioned vendor media type | Error is safe but bodyless | Low |
| Windows proof | Cross-compile looks sufficient | Native file/HTTP/process behavior required a Windows runner | Closed by the pushed native matrix | Resolved |
| Foreign Windows TLS check | Installed Rust target looked sufficient | `aws-lc-sys` also requires a MinGW C compiler unavailable on this Mac | Pure embedded target passes; native CI remains authoritative for TLS | Low |
| Facade package dry run | A standalone facade package should resolve its internal crates | `cargo package -p logical-db --no-verify` cannot resolve unpublished `logical-db-*` crates from crates.io | Git/path consumers work; crates.io publication requires an ordered workspace release | High |
| Braid confirmation | Bearer authentication appears sufficient to enable the daemon | Pinned Braid stores `HumanConfirm` as a capsule policy but has no runtime confirmation evidence | Local write/sync authority now fails closed; thin-service consumption is blocked | Critical |
| History growth | Structured state suggests automatic compaction | v1 needs a snapshot baseline and retention floor before old server changes can be deleted safely | Unbounded growth remains a production blocker, now explicit | Critical |

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
  standalone composition/authentication/graceful shutdown. The prior external
  facade-to-daemon sync result is retired because its confirmation was unsound.
- Failed: no currently accepted journey failed after its regression was added.
- Ambiguous: the hardening delta has compiled against the FDB API but has not
  yet rerun the live official-cluster job.
- Blocked: safe history retention/GC, embedded export/restore, tombstones,
  crates.io publication ordering, externally verifiable Braid confirmation,
  external LEM consumption, new independent review, and synchronized protected
  repository tips.
- Untested: multi-node FDB failover, public TLS/issuer integration, sleep/wake,
  disk exhaustion, mobile sandboxes, and sustained production load.

## Final verdict

The public embedded and loopback transport hardening paths are proven within
the named local bounds; the prior single-process FDB storage journey remains
retained, but the daemon's write/sync authority is intentionally disabled.
This report does not label Logical-DB production-complete until the manifest's
retention, recovery, deletion, external-consumer, hostile-review, CI, and remote
repository gates close.
