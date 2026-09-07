# Logical-DB completion and decision ledger

> Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
> S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
> `evidence/SOURCES.md`. Repository and issue evidence is linked below.

## Current delivered state

[PR #36](https://github.com/srinji-kaggss/logical-DB/pull/36) merged as
`2a17d6b90b6f141ea37fc24dfdfa590fe9d9ce10`. Its reviewed head was
`4d0b810eb7d051dcae2da0e48d41d64cd4c0705b`; all seven protected checks passed
in [run 34170132966](https://github.com/srinji-kaggss/logical-DB/actions/runs/34170132966).
The merge commit has its own
[run 34170937860](https://github.com/srinji-kaggss/logical-DB/actions/runs/34170937860);
all seven jobs completed successfully on that merge commit. This post-merge
receipt is separate from the reviewed-head receipt above.

The implemented architecture is described in `ARCHITECTURE.md`. It contains
an embedded replica, an independently hosted FDB synchronization service,
and optional Cairn/episode vocabulary through the facade. Episode v1 snapshots
have bounded decoding and identity over immutable scope and provenance fields.
This is delivered functionality, not a declaration of production completion.

## Decisions requiring the Director

| Decision | Conflicting authority or missing choice | Rule until decided |
|---|---|---|
| Product completion scope | [#6](https://github.com/srinji-kaggss/logical-DB/issues/6) defines an embedded five-class v0.1; [#8](https://github.com/srinji-kaggss/logical-DB/issues/8) supersedes it with a thin FDB layer; merged v3 implements a local-first replica and service | Do not close either issue or declare either historical contract superseded without recording the Director's decision |
| Estate ownership | [#30](https://github.com/srinji-kaggss/logical-DB/issues/30); LEM `SOT.okf.md` reserves plane changes to the Director and still names FjallFactStore | Prepare a reviewable cross-repository decision before changing LEM's source of truth or claiming the repositories are retired |
| Comparative acceptance | [#33](https://github.com/srinji-kaggss/logical-DB/issues/33), [#34](https://github.com/srinji-kaggss/logical-DB/issues/34), [#35](https://github.com/srinji-kaggss/logical-DB/issues/35) require a corpus, baseline, workload, and measurement environment | Record those inputs before claiming measured superiority or closing the benchmark requirements |

A required decision pauses dependent work. Audits, reproducible counterexamples,
and factual documentation may continue. Record the decision, its scope, and its
evidence here and in the affected issue before treating it as authority.

## Open correctness and acceptance work

The order below follows the current implementation's correctness dependencies;
it does not resolve the product-scope decision above.

| Priority | Work | Observed gap and completion evidence |
|---|---|---|
| P0 | [#27: action authorization](https://github.com/srinji-kaggss/logical-DB/issues/27) | `ProtocolService::push` authorizes `change.request` before native CRDT validation; FDB checks the same caller-supplied identity. Separate intent from trusted canonical action identity and exercise field mutation plus exact retry through HTTP, authority, and FDB |
| P0 | [#28: recovery before pruning](https://github.com/srinji-kaggss/logical-DB/issues/28) | `SnapshotBaseline` contains only version and position. Prove a state-bearing baseline restores an unchanged document and tombstones after history has been pruned |
| P1 | [#29: projection semantics](https://github.com/srinji-kaggss/logical-DB/issues/29) | `projection_digest` includes schema version and index IDs, but no projection semantic identity. A changed projection under the same IDs must require migration and rebuild rows atomically |
| P1 | [#25: checked access graph](https://github.com/srinji-kaggss/logical-DB/issues/25) | Audit migration, sync, and service transition coverage against the issue's graph contract after the action and recovery boundaries are settled |
| Dependent | [#32: durable episode service](https://github.com/srinji-kaggss/logical-DB/issues/32) | The embedded episode snapshot journey is not a service-backed episode log. Requires resolved ownership, trusted admission, state-bearing recovery, and a real HTTP episode-to-LocalWorld journey |
| Release evidence | External LEM consumption and deployed-service behavior | The earlier facade/FDB consumer receipt is explicitly retired for an invalid confirmation model. Require current external consumption, verified confirmation, and the deployment journeys listed in `REWRITE_MANIFEST.md` |

Embedded tombstones and export/restore are implemented and have retained tests.
Their presence must not be confused with state-bearing server recovery or with
completion of every process-interruption/deployment scenario.

## Delivery and release gates

A reviewable change requires its PR, all protected checks green on the exact
head, a clean isolated checkout, and direct execution of its affected public
journey. Human review and merge follow. Post-merge checks are separate evidence.

Production completion additionally requires a ratified product scope and every
applicable acceptance row satisfied. A green PR does not close unfinished
roadmap issues. Do not copy historical checkmarks into a current release claim.

The PR #36 Rust scan reported zero blocking findings across 25 files. Numerical
MI remains advisory in that scanner: 21 files were below 76, minimum 49.4.
Three Python verifier findings were documented against argument-list subprocess
calls without a shell. No threshold or detector was changed to hide them.

Unrelated working changes remain outside a delivery patch. The original
checkout's unstaged KV edit was preserved during PR #36; later work must not
silently import or discard it.
