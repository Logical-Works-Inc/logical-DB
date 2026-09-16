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

## Required execution before promotion

No Rust build, rustfmt, clippy, test, mutation campaign, rewrite verifier, or FDB
journey was run for this patch. This environment has no Rust toolchain and its
shell cannot resolve GitHub. GOVERNANCE.md records account-billing-blocked
hosted CI; this patch does not alter or bypass that gate. Keep the PR draft and
#54 open until exact-head reproduction is recorded, including measured timings.

```sh
cargo fmt --all -- --check
cargo clippy --locked -p logical-db-http --all-targets -- -D warnings
cargo test --locked -p logical-db-http tests::deadlines -- --nocapture
cargo test --locked -p logical-db-http
cargo test --locked -p logical-db-e2e
python3 scripts/verify-rewrite.py
```

Also run the existing HTTP/FDB recovery composition on the provisioned FDB lane.
Counterfactual checks should remove the refresh timeout, reset the deadline
after refresh, omit body coverage, and omit the late-ready guard individually;
each corresponding regression must fail. No counterfactual execution is claimed.
