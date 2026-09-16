# HTTP operation deadline: implementation, not an execution receipt

> Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
> S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
> S33 S34 S35 S36 S37 S38 S39. Source URLs are in `evidence/SOURCES.md`.

## Scope and contract

Refs #54. Reviewed base: `e4e1ead86042cc917e959c3f5fb2206ff61f9319`.
The recovery implementation for #28 is already present according to the current
completion ledger; its remaining acceptance evidence is not replaced or closed
by this transport change. This patch addresses a remaining unbounded external
boundary without changing storage, authority, wire formats, or CI policy.

Push, pull, and baseline recovery now share one 30-second deadline per call,
starting before encoding and credential refresh and continuing through HTTP
headers, streamed body consumption, decoding, and response validation. The
10-second connection limit and 30-second reqwest limit remain defense in depth;
neither gives refresh or a later stage a new operation budget.
`CredentialTimeout` means this call did not dispatch HTTP. `OperationTimeout`
means remote effects may have committed; it does not promise rollback. Both
permit the caller's bounded retry policy for the exact idempotent operation.
Ordinary credential errors retain their distinct non-retryable classification.

Cancellation drops the owned refresh/exchange future. Providers must yield
promptly, release owned work on drop, remain reusable, and not detach refresh
tasks. A blocking provider cannot be preempted by an async timer. Explicit
pre/post deadline checks refuse already-expired work and late-ready results;
this is not a hard real-time scheduling guarantee. See the upstream timer
semantics at https://docs.rs/tokio/latest/tokio/time/fn.timeout.html and the
pinned estate wrapper `lgwks_bot::rt::time` at revision `88d2a75`.

The already-approved `lgwks_bot` dependency moves from dev to normal use;
`contract/APPROVED.toml` already permits both kinds for `logical-db-http`.
No dependency version, runtime provider, or approval rule is changed. Adding
two public error variants requires downstream exhaustive matches to be updated.

## Regression coverage authored, not executed

Seven tests in `crates/logical-db-http/src/tests/deadlines.rs` cover pending
refresh on all three public routes with no TCP dispatch; owned-future drop;
cancellation and reuse of the same transport; shared refresh/header/body
budget; static/refreshed success; distinct errors and retry classification;
expired-stage refusal; and rejection of late-ready credentials before dispatch.
The success fixtures exercise valid empty push/pull responses and baseline's
404/unavailable mapping, not successful installation of a baseline artifact.

Tests shorten only the private per-instance budget, not the production default:
50 ms for pending refresh (1 s watchdog); 500 ms shared by 300 ms refresh and
300 ms HTTP delay (2 s watchdog, accepted elapsed [500 ms, 1 s)). These are
asserted tolerances, NOT measured results. The 30-second constructor default is
asserted separately. Persistent acknowledgement/checkpoint invariance still
needs execution through the existing sync/recovery composition lanes; these
transport tests are not a substitute for that storage evidence.

## Execution record — 2026-09-16 local receipts

Executed on the Director's development machine (`rustc 1.98.0 (88d9e12ae
2026-08-18)`, `aarch64-apple-darwin`), working tree clean at
`3a7aa84caf1c34788cc517e1887272314deec332` plus this docs-only commit; no
Rust or lockfile bytes change after the receipts below.

| Gate | Result |
|---|---|
| `cargo fmt --all -- --check` | passed (exit 0) |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | passed (exit 0) after one hardening commit: the `Late` fixture's `async fn` tripped `clippy::unused_async_trait_impl` under `-D warnings`; converted to `fn -> impl Future` + `std::future::ready` with identical poll-time semantics |
| `cargo test --workspace --locked` | passed (exit 0, 44 suites, 0 failed) |
| `cargo test --locked -p logical-db-http` | passed: 20/0 including all seven deadline tests |
| `python3 scripts/verify-rewrite.py` | `PASS clean-room rewrite evidence gates` (exit 0) |
| Mutation control (refresh deadline removed, reverted) | `pending_credentials_timeout_without_dispatch_on_every_route` and `late_ready_credentials_are_refused_before_dispatch` both FAILED (5/2); restored head: 7/0 |
| Self-hosted CI reproduction on the exact head | executed in CI run for this branch; see PR checks |
| Local FDB composition | not executed this session (no staged FDB binaries at hand); the self-hosted FDB lane covers the recovery journeys |
| Measured timings | the shared-budget test asserts elapsed within its declared [500 ms, 1 s) tolerance and passes; no independent timing instrumentation was added |

Hosted Linux/macOS/Windows lanes remain billing-blocked on this account; that
limitation is unchanged by this patch. The first local clippy rerun was wrongly
reported clean because its output was filtered without capturing the exit
code; the deterministic lint was caught and fixed on the next full run.
