# Clean-room rewrite acceptance manifest

> Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
> S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
> S33 S34 S35 S36 S37 S38 S39 S40 S41 S42 S43. Primary URLs:
> `evidence/SOURCES.md`.

Baseline: `7cffbc07b6f312854986ae306b583ef891623b2d`.

## Mechanically satisfied

- [x] Every baseline blob changed or was deleted; no legacy `kernel` API remains.
- [x] File/blob churn is 100%; baseline-line churn exceeds 85%.
- [x] Every surviving tracked text file cites at least twenty unique ledger IDs.
- [x] The dependency verifier enforces the implemented one-way crate DAG.
- [x] Embedded edits, reads, exact retries, schema indexes, change cursors, and
  restart work without Braid or a network.
- [x] Two offline replicas converge through both in-process and real HTTP paths.
- [x] Push correlation, duplicates, rejection, forged/expired checkpoints, and
  explicit full resync have typed tests.
- [x] The service has bounded HTTP ingress and an async FDB adapter with private
  tuple subspaces, chunked values, and transaction ceilings.
- [x] Braid produces a real pinned `RunnableInvocation` bound to exact scoped
  request facts and the durable frontier; FDB consumes it atomically.
- [x] A committed edit survives subprocess exit without destructor execution.
- [x] Windows-native data placement is represented without POSIX assumptions.
- [x] Schema/index migration is bounded, revision checked, durably resumable,
  write excluding, and atomically finalized against one exact target identity.
- [x] Deterministic state-machine and mutation campaigns cover tuple ordering,
  replay, checkpoint transitions, index replacement, and envelope bounds with
  zero viable surviving mutants in every completed package campaign.
- [x] Kill-during-write, no-destructor exit, restart, and planted corruption
  journeys fail closed without exposing partially committed Logical-DB state.
- [x] A staged official FoundationDB 7.3.77 cluster passed chunking, concurrent
  writers, exact duplicate/conflict behavior, pagination, restart, and planted
  missing-chunk corruption; the deployable daemon also reached its live HTTP
  authentication and decoding boundary against that cluster.
- [x] `logical-db-daemon` is a real loopback-only FDB composition root; embedded
  applications and custom ecosystem hosts remain independent of it.
- [x] A retained public-facade journey crosses HTTP, Braid, and a real FDB
  cluster without importing internal crates in the consumer source.
- [x] Official client buggify forces post-commit `commit_unknown_result`; the
  adapter reports recovered ambiguity, advances once, and rejects invocation reuse.
- [x] HTTP ingress bounds complete in-flight requests before body extraction,
  applies a deadline, and local materialization is bounded per document/page.

## Still blocking release and LEM handoff

- [ ] Run native Windows runtime CI for file locking, concurrent open, Unicode,
  read-only paths, process termination, and HTTP TLS; foreign-target compilation
  alone does not close this gate.
- [ ] Package and consume the facade from LEM in an external-repository tape
  journey without importing internal crates.
- [ ] Run the final independent hostile review after all preceding gates.
- [ ] Merge only that reviewed result, push it, observe all GitHub matrix jobs,
  and prove local/main/origin tips match.

No green subset, churn percentage, or document may override an unchecked gate.
