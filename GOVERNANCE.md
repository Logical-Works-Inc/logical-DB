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

PRs [#48](https://github.com/srinji-kaggss/logical-DB/pull/48) through
[#53](https://github.com/srinji-kaggss/logical-DB/pull/53) delivered the
estate-substrate adoption, paged baseline recovery into one atomic client
install, and the process-kill restore journey. The observed `origin/main` tip
on 2026-09-20 is `ae502569` (merge of [#62](https://github.com/srinji-kaggss/logical-DB/pull/62)).
The intervening merged PRs are [#57](https://github.com/srinji-kaggss/logical-DB/pull/57)
(operation deadline), [#58](https://github.com/srinji-kaggss/logical-DB/pull/58)
(ratified PRD), [#59](https://github.com/srinji-kaggss/logical-DB/pull/59)
(SOT/CloudKit documentation), [#60](https://github.com/srinji-kaggss/logical-DB/pull/60)
(local quickstart), [#61](https://github.com/srinji-kaggss/logical-DB/pull/61),
and #62 (estate BLAKE3 migration). These merges carry no hosted-check evidence:
GitHub Actions on this account is billing-blocked and branch protection on
`main` is disabled (the protection API returned HTTP 404 on 2026-09-20). Their
verification receipts are local and do not satisfy the unavailable hosted lane.

Local verification at `0a22f9a7` + the retention-store test-lane repair
(2026-09-14, macOS arm64): the clean-room verifier passes; the workspace test
suite passes 308/0; and the complete FDB composition lane was executed
locally for the first time using the checksummed FoundationDB 7.3.77 arm64
client and server staged from the official release: `cargo test -p
logical-db-daemon --features fdb` 10/10, `cargo test -p logical-db-store-fdb
--features fdb` 11/11, the `fdb-ambiguity-journey` (unknown-commit recovered,
attempt 3, bounded page, isolated scope), `fdb_recovery_composition`, and
`fdb_process_kill_restore` all pass against a live single-memory cluster.
Hosted CI remains the required reproduction environment until a self-hosted
runner exists; these local receipts do not substitute for it.

## 2026-09-16 delivered state and Director decisions

- **PR #57 merged** as `f19867b`: the HTTP transport bounds credential
  refresh and every exchange under one 30-second operation deadline
  (`CredentialTimeout`/`OperationTimeout`). Self-hosted quality and FDB
  lanes passed on the exact code head `c69631a` (run 35124718030),
  including the 300-document `/v1/recovery:baseline` journey and the
  process-kill restore consumer. Issue #54 stays open for independent
  reproduction and measured-tolerance instrumentation.
- **PRD ratified** — PR #58 merged as `b804838`:
  `docs/PRD-UNIFIED-SUBSTRATE.md` is the product definition of record
  (embedded v1, FDB as cloud relay only, Cairn+LEM unification, swarm
  work packages WP0–WP8). The self-hosted FDB lane failed on the PR
  head with no log (runner died before upload); the Rust bytes were
  identical to `c69631a`, whose FDB lane passed — recorded as
  infrastructural, not a source failure.
- **SOT transfer authorized** — the Director authorized properly
  bringing the Cairn and LEM code into this workspace (2026-09-16).
  `docs/adr/0002-cairn-lem-authority-transfer.md` records the transfer;
  the ADR-0001 migration proof is the open obligation (PRD WP2). Until
  it lands, documentation says "migration in progress", never
  "canonical".
- **Issue consolidation** — #6, #8, #30, #32, #33, #34, #35 closed with
  consolidation comments pointing at the PRD; #28, #25, #54 remain open
  as real gates. Open issues are now exactly the acceptance list.
- **CloudKit review recorded** — `docs/review-cloudkit.md`: relay-first
  confirmed; CloudKit is a design reference plus at most a bounded
  optional transport adapter for Apple-only consumer apps.

The implemented architecture is described in `ARCHITECTURE.md`. It contains
an embedded replica, an independently hosted FDB synchronization service,
and optional Cairn/episode vocabulary through the facade. Episode v1 snapshots
have bounded decoding and identity over immutable scope and provenance fields.
This is delivered functionality, not a declaration of production completion.

The current recovery stack layer adds a bounded, content-addressed v2
`SnapshotArtifact` for the server retention path. It preserves canonical
document state and tombstone markers, while retaining readable v1 position-only
records as migration data that cannot authorize pruning. FoundationDB rebuilds,
rechecks, and atomically seals the artifact, and validates the descriptor and
its stored manifests and chunks in the prune transaction. This establishes the
server-side prune gate.

Recovery status, stated once here and referenced everywhere else:
**implementation present** — the v2 artifact, the paged HTTP baseline protocol,
and one atomic client install of recovered state and checkpoint exist on
`main` (PRs #49 through #53). **Local execution reported** — the 2026-09-14
local live-cluster receipts below exercise `fdb_recovery_composition` and the
process-kill restore journey. [PR #65](https://github.com/srinji-kaggss/logical-DB/pull/65)
is open with a main-targeted FDB expired-checkpoint recovery test; its open
state is not delivery evidence. **Not yet established** — issue #28 stays open
until each acceptance item, including the position-1 prune/fresh-replica
counterexample, is independently reproduced on a runnable hosted or self-hosted
lane. No hosted full-recovery evidence is claimed.

## 2026-09-19 domain boundary enforcement (issue #73)

- **Issue #73 is implemented in open [PR #77](https://github.com/srinji-kaggss/logical-DB/pull/77)**
  on branch `feat/domain-boundary-enforcement`, stacked on the open PR #65 that
  defined the four product domains. It is not merged delivery evidence.
  `contract/domains/*.toml` declare owned `crates`,
  `public_contracts`, `may_depend_on`, and `forbidden_domains`;
  `contract/domains/GRAPH.toml` adds the kernel/facade/adapter/e2e layers, the
  composition-layer exemption, and the predecessor twin denylist.
- **Enforcement** — `scripts/domain_contract.py` parses those records, reads
  the live graph from `cargo metadata`, and refuses an undeclared owner, a
  double-owned crate, a disallowed cross-domain edge, a product domain importing
  another domain's implementation crate, and a predecessor twin. The clean-room
  evidence lane runs it and compares the committed `evidence/domain-graph.md`
  receipt (8 owners, 36 edges at this revision).
- **Falsifier** — `scripts/test-domain-contract.py` injects one forbidden edge
  per domain (Cloud→Cairn, Cairn→Cloud implementation, LEM→Cloud
  implementation, Forge→Cloud) plus undeclared, double-owned, and twin crates,
  and asserts the real checker refuses each; 8/8 pass locally.
- **Scope limit** — this is a structure gate over the declared import graph. It
  is not proof that the target Cairn/LEM/Forge crates or services exist, not the
  external-consumer fixture (PRD WP2), and not issue #25's execution-time
  transition graph. Those remain open.

## 2026-09-20 sovereign-program state (issue #66)

Issue [#66](https://github.com/srinji-kaggss/logical-DB/issues/66) is the
current nine-axis acceptance program for the proposed sovereign hyperscale
Cloud Plane. It preserves #25 and #28 as existing correctness gates; it does
not turn the current scope-frontier FDB relay or local receipts into a scale,
availability, cost, or sovereignty claim.

The dependency order recorded in #66 is: #73 structure first; then
[#67](https://github.com/srinji-kaggss/logical-DB/issues/67) partition/routing,
[#68](https://github.com/srinji-kaggss/logical-DB/issues/68) authorized views,
and [#69](https://github.com/srinji-kaggss/logical-DB/issues/69) residency/key/exit
contracts; then #70 delivery, #71 disaster recovery, and #72 matched-scale
evidence. Because PR #77 is open, #67–#69 remain planned contract work rather
than an implemented Cloud Plane. Cairn #74, LEM #75, and Forge #76 are separate
linked migrations; no domain promotion is implied here.

## Recorded product decision

The Director already resolved the backend split in
[issue #8's follow-up](https://github.com/srinji-kaggss/logical-DB/issues/8#issuecomment-5419933958):
redb serves local device state; FoundationDB serves distributed transaction
requirements. Upper key, namespace, and event contracts must be compatible,
but backend guarantees must be explicit through `EmbeddedLocal` and
`DistributedTransactional` capability profiles. Do not fork or strip FDB core
roles. The same 1M-operation trace must compare CPU, memory, bytes written,
latency, startup, recovery, and sleep/wake for common local semantics separately
from distributed guarantees.

[Issue #6's Director ruling](https://github.com/srinji-kaggss/logical-DB/issues/6#issuecomment-5418977305)
rejects the five application-shaped classes as the mandatory core. The
substrate owns ordered transactional KV, versions, range/conflict semantics,
durability, and a distributed-compatible seam. Higher-layer semantics remain
optional consumers/adapters. These recorded rulings supersede the contradictory
parts of the issue bodies; the issues remain open for outstanding acceptance.

The earlier scope question was based on reading issue bodies without their
follow-up rulings. No new human decision is required to resume this work.

## Decisions requiring the Director

| Decision | Conflicting authority or missing choice | Rule until decided |
|---|---|---|
| Comparative acceptance | [#33](https://github.com/srinji-kaggss/logical-DB/issues/33), [#34](https://github.com/srinji-kaggss/logical-DB/issues/34), [#35](https://github.com/srinji-kaggss/logical-DB/issues/35) require a corpus, baseline, workload, and measurement environment | Director ruled 2026-09-14: leave open until a measurement environment exists. No corpus or workload is committed and no measured-superiority claim is made meanwhile |

Resolved 2026-09-14 by Director ruling:

- **Estate ownership ([#30](https://github.com/srinji-kaggss/logical-DB/issues/30)).**
  The estate is one development monorepo per `docs/MONOREPO.md`. Authority
  boundaries are already recorded in `docs/adr/0001-semantic-plane-boundary.md`:
  the LEM SOT remains authoritative for the CAIRN/ORIGIN/FORGE plane table,
  `logical-db-cairn` and `logical-db-episode` are bounded non-authoritative
  ports, and the sibling repositories are frozen read-only references. There is
  no pending cross-repository ownership decision; any future plane-authority
  transfer still requires the ADR's Director-approved SOT diff and an explicit
  migration proof.
- **Hosted CI plan.** GitHub Actions on this account stays billing-blocked.
  The FDB cluster lane moves to a self-hosted runner when one is provisioned.
  `.github/workflows/ci.yml` is unchanged until the runner exists; hosted-lane
  evidence remains absent and must not be claimed.

A required decision pauses dependent work. Audits, reproducible counterexamples,
and factual documentation may continue. Record the decision, its scope, and its
evidence here and in the affected issue before treating it as authority.

## Open correctness and acceptance work

The order below follows the recorded product decision and the current
implementation's correctness dependencies.

| Priority | Work | Observed gap and completion evidence |
|---|---|---|
| Closed | [#27: action authorization](https://github.com/srinji-kaggss/logical-DB/issues/27) | The action-binding stack validates native CRDT bytes before authorization, computes a versioned canonical `ActionDigest`, and makes FDB recompute it. Unit HTTP tests and the recorded FDB journey hold a grant fixed across field mutations and preserve exact retry. Independent runtime Braid confirmation remains a separate release requirement |
| P0 | [#28: recovery before pruning](https://github.com/srinji-kaggss/logical-DB/issues/28) | PRs #49–#53 delivered the state-bearing snapshot artifact, the paged HTTP baseline protocol, and one atomic client install; the local live-cluster lane now proves `fdb_recovery_composition` and the process-kill restore journey pass. The issue stays open until its acceptance checklist is verified item-by-item against that evidence — in particular expired-checkpoint recovery through the public transport and the position-1 prune/fresh-replica counterexample — and the runnable hosted or self-hosted lane reproduces it |
| Delivered in stack | [#29: projection semantics](https://github.com/srinji-kaggss/logical-DB/issues/29) | Every `Projection` now requires an application-supplied `ProjectionSemantics` (nonzero version or opaque digest) hashed with its `IndexId` into the v2 durable schema digest. The same index identity and schema version with changed semantics returns migration-required; `Database::migrate_schema` rebuilds the rows in bounded revision-checked pages. The facade test `changed_projection_semantics_requires_migration_and_rebuilds_rows` proves refusal, duplicate-identity refusal, and atomic rebuild |
| P1 | [#25: checked access graph](https://github.com/srinji-kaggss/logical-DB/issues/25) | Audit migration, sync, and service transition coverage against the issue's graph contract after the action and recovery boundaries are settled |
| In delivery stack | [#73: domain boundaries](https://github.com/srinji-kaggss/logical-DB/issues/73) | PR #77 implements the declared import-graph gate and remains open, stacked on PR #65. It proves neither a target-service migration nor a legal runtime authority path. |
| Proposed Cloud contracts | [#67–#69](https://github.com/srinji-kaggss/logical-DB/issues/66) | Partition routing, authorized views, and region/key/exit are the next Cloud contracts only after #73's structure gate lands. No Cloud implementation or nine-axis performance claim exists. |
| Proposed dependent delivery | #70–#72 | Fan-out, regional disaster recovery, and matched-scale evidence follow the first Cloud contracts; the 1M-operation comparison remains unmeasured. |
| Proposed semantic/action migrations | #74–#76 | Cairn ontology/evidence, LEM/ORIGIN LocalWorld, and Forge capability/receipt boundaries remain separate issues. No domain is promoted merely by documentation. |
| Release evidence | External LEM consumption and deployed-service behavior | The earlier facade/FDB consumer receipt is explicitly retired for an invalid confirmation model. Require current external consumption, verified confirmation, and the deployment journeys listed in `REWRITE_MANIFEST.md` |

Embedded tombstones and export/restore are implemented and have retained tests.
The server retention path now also carries tombstone state in the bounded v2
artifact and gates pruning on that artifact. What remains for #28 is the
acceptance checklist named in the recovery-status paragraph above, not the
implementation of the paged route or the atomic install.

Canonical action encoding is owned by `logical-db-protocol`. PRs #61 and #62
moved repository digests and MACs to `lgwks_std::hash` BLAKE3; this ledger must
not retain its former SHA-256 description. The encoding retains its versioned
domain and length framing, and its golden vector remains independently
reproducible outside Rust. `OperationDigest` remains caller intent; it is
included in, but never substituted for, `ActionDigest`.

## Delivery and release gates

A reviewable change requires its PR, all protected checks green on the exact
head, a clean isolated checkout, and direct execution of its affected public
journey. Human review and merge follow. Post-merge checks are separate evidence.

Merge gate while hosted execution is unavailable, stated once: source review
may proceed against local receipts, but merge eligibility requires executed
reproduction of the affected lanes on hosted or self-hosted CI. A bounded
exception requires explicit Director authorization recorded in this ledger with
its replacement evidence enumerated; a local PASS claim alone is never a
satisfied execution gate. PR #55 carries such an exception: the Director
authorized merge after hardening on 2026-09-14, with replacement evidence being
the local receipts pinned in `evidence/2026-09-14-local-fdb-receipt.md` and the
Director's own review of the exact head. This exception covers PR #55 only.

Production completion additionally requires a ratified product scope and every
applicable acceptance row satisfied. A green PR does not close unfinished
roadmap issues. Do not copy historical checkmarks into a current release claim.

The PR #36 Rust scan reported zero blocking findings across 25 files. Numerical
MI remains advisory in that scanner: 21 files were below 76, minimum 49.4.
Three Python verifier findings were documented against argument-list subprocess
calls without a shell. No threshold or detector was changed to hide them.

Verification notes, 2026-09-14 audit: hosted CI is billing-blocked, so no PR
after #47 carries hosted check evidence and the seven-protected-checks gate
cannot execute until the self-hosted runner exists; hosted checks must never be
described as green while blocked. Branch protection on `main` is disabled, so
merges are currently ungated mechanically; the paragraph above defines the
operative gate in that state. The unstaged KV edit preserved during PR #36 is
no longer present in the working copy and its disposition is untracked; any
future appearance of that edit requires explicit review, not silent import.

Unrelated working changes remain outside a delivery patch. The original
checkout's unstaged KV edit was preserved during PR #36; later work must not
silently import or discard it.
