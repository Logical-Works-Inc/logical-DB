# Governance §5 — ledger, 2026-09-16 and earlier

> Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
> S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
> S33 S34 S35 S36 S37 S38 S39 S42 S43. Source URLs are in `evidence/SOURCES.md`.

Entries of the completion and decision ledger in [GOVERNANCE.md](../../GOVERNANCE.md),
moved verbatim under the 120-line document law. Newest first.

### 2026-09-16 — Delivered state and Director decisions

- **PR #57 merged** (`f19867b`): HTTP transport bounds credential refresh and
  every exchange under one 30-second operation deadline. Self-hosted quality and
  FDB lanes passed on head `c69631a` (run 35124718030). Issue #54 stays open for
  independent reproduction and measured-tolerance instrumentation.
- **PRD ratified** — PR #58 (`b804838`): `docs/PRD-UNIFIED-SUBSTRATE.md` is the
  product definition of record (embedded v1, FDB as cloud relay only,
  Cairn+LEM unification, swarm work packages WP0–WP8).
- **SOT transfer authorized** (2026-09-16): `docs/adr/0002-cairn-lem-authority-transfer.md`
  records the transfer. The ADR-0001 migration proof is the open obligation
  (PRD WP2). Until it lands, documentation says "migration in progress", never
  "canonical".
- **Issue consolidation**: #6, #8, #30, #32, #33, #34, #35 closed with
  consolidation comments pointing at the PRD. #25, #28, #54 remain open as real
  gates. Open issues are exactly the acceptance list.
- **CloudKit review recorded** — `docs/review-cloudkit.md`: relay-first
  confirmed; CloudKit is a design reference plus at most a bounded optional
  transport adapter for Apple-only consumer apps.

### 2026-09-14 — Local verification and the hosted-CI exception

Local verification at `0a22f9a7` plus the retention-store test-lane repair
(macOS arm64): clean-room verifier passes; workspace suite 308/0; the complete
FDB composition lane executed locally for the first time against the checksummed
FoundationDB 7.3.77 arm64 client and server — daemon fdb 10/10, store-fdb fdb
11/11, `fdb-ambiguity-journey`, `fdb_recovery_composition`, and
`fdb_process_kill_restore` all pass against a live single-memory cluster.

**Hosted CI plan, Director ruling.** GitHub Actions on this account stays
billing-blocked. The FDB cluster lane runs on a self-hosted runner. `.github/workflows/ci.yml`
declares literal self-hosted labels; `scripts/verify-ci.py` refuses anything
else. Hosted-lane evidence remains absent and must not be claimed.

**Bounded exception, PR #55 only.** The Director authorized merge after
hardening on 2026-09-14. Replacement evidence is the local receipts pinned in
`evidence/2026-09-14-local-fdb-receipt.md` plus the Director's own review of the
exact head. This exception covers PR #55 and nothing else.

**Estate ownership, issue #30 resolved.** The estate is one development monorepo
per `docs/MONOREPO.md`. Authority boundaries are in
`docs/adr/0001-semantic-plane-boundary.md`: the LEM SOT remains authoritative for
the CAIRN/ORIGIN/FORGE plane table, `logical-db-cairn` and `logical-db-episode`
are bounded non-authoritative ports, and the sibling repositories are frozen
read-only references. Any future plane-authority transfer requires the ADR's
Director-approved SOT diff and an explicit migration proof.

### 2026-09-12 and earlier — Recorded product decisions

**Backend split (issue #8 follow-up).** redb serves local device state;
FoundationDB serves distributed transaction requirements. Upper key, namespace,
and event contracts must be compatible, but backend guarantees are explicit
through `EmbeddedLocal` and `DistributedTransactional` capability profiles. Do
not fork or strip FDB core roles. The same 1M-operation trace must compare CPU,
memory, bytes written, latency, startup, recovery, and sleep/wake for common
local semantics separately from distributed guarantees.

**Core scope (issue #6 ruling).** The five application-shaped classes are
rejected as the mandatory core. The substrate owns ordered transactional KV,
versions, range/conflict semantics, durability, and a distributed-compatible
seam. Higher-layer semantics remain optional consumers and adapters. These
rulings supersede the contradictory parts of the issue bodies; the issues remain
open for outstanding acceptance.

**PR #36 merged** as `2a17d6b9`. Reviewed head `4d0b810e`; all seven protected
checks passed in run 34170132966. Merge commit run 34170937860 also completed
all seven. The PR #36 Rust scan reported zero blocking findings across 25 files.
Numerical MI remains advisory: 21 files below 76, minimum 49.4. Three Python
verifier findings were documented against argument-list subprocess calls without
a shell. No threshold or detector was changed to hide them.

**PRs #48–#53** delivered the estate-substrate adoption, paged baseline recovery
into one atomic client install, and the process-kill restore journey. Recovery
status, stated once and referenced everywhere else: **implementation present** —
the v2 artifact, the paged HTTP baseline protocol, and one atomic client install
of recovered state and checkpoint exist on `main`. **Local execution reported** —
the 2026-09-14 local live-cluster receipts exercise `fdb_recovery_composition`
and the process-kill restore journey. **Not yet established** — issue #28 stays
open until each acceptance item, including the position-1 prune/fresh-replica
counterexample, is independently reproduced on a runnable hosted or self-hosted
lane.

Canonical action encoding is owned by `logical-db-protocol`. PRs #61 and #62
moved repository digests and MACs to `lgwks_std::hash` BLAKE3; this ledger must
not retain its former SHA-256 description. `OperationDigest` remains caller
intent; it is included in, but never substituted for, `ActionDigest`.
