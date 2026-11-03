# Governance §6–§8 — open work, Director decisions, residual state

> Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
> S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
> S33 S34 S35 S36 S37 S38 S39 S42 S43. Source URLs are in `evidence/SOURCES.md`.

Sections 6–8 of [GOVERNANCE.md](../../GOVERNANCE.md), moved under the 120-line
document law. The hosted-execution decision in §7 is resolved (ledger,
2026-11-03) and is recorded there rather than here.

## 6. Open correctness and acceptance work

| Priority | Work | Observed gap and completion evidence |
|---|---|---|
| Closed | [#27: action authorization](https://github.com/srinji-kaggss/logical-DB/issues/27) | Validates native CRDT bytes before authorization, computes a versioned canonical `ActionDigest`, FDB recomputes it. Unit HTTP tests and the FDB journey hold a grant fixed across field mutations and preserve exact retry. Independent runtime host-authority confirmation remains a separate release requirement |
| P0 | [#28: recovery before pruning](https://github.com/srinji-kaggss/logical-DB/issues/28) | PRs #49–#53 delivered the state-bearing snapshot artifact, paged HTTP baseline, one atomic client install. Stays open until the acceptance checklist is verified item-by-item against a runnable lane, in particular expired-checkpoint recovery through the public transport and the position-1 prune/fresh-replica counterexample |
| Delivered | [#29: projection semantics](https://github.com/srinji-kaggss/logical-DB/issues/29) | Every `Projection` requires an application-supplied `ProjectionSemantics` hashed with its `IndexId` into the v2 durable schema digest. Same identity and version with changed semantics returns migration-required; `Database::migrate_schema` rebuilds in bounded revision-checked pages |
| P1 | [#25: checked access graph](https://github.com/srinji-kaggss/logical-DB/issues/25) | Audit migration, sync, and service transition coverage against the issue's graph contract after action and recovery boundaries settle |
| Delivered | [#73: domain boundaries](https://github.com/srinji-kaggss/logical-DB/issues/73) | Declared import-graph gate with an 8/8 falsifier. Structure only |
| Proposed | [#67–#69](https://github.com/srinji-kaggss/logical-DB/issues/66) | Partition routing, authorized views, region/key/exit. Cloud contracts, after #73. No Cloud implementation and no nine-axis performance claim |
| Proposed | #70–#72 | Fan-out, regional disaster recovery, matched-scale evidence. The 1M-operation comparison remains unmeasured |
| Proposed | #74–#76 | Cairn ontology/evidence, LEM/ORIGIN LocalWorld, Forge capability/receipt. No domain promoted by documentation |
| Release | External LEM consumption and deployed-service behaviour | The earlier facade/FDB consumer receipt is retired for an invalid confirmation model. Require current external consumption, verified confirmation, and the deployment journeys in `REWRITE_MANIFEST.md` |
| Frontier | #33, #34, #35 | Director ruled 2026-09-14: leave open until a measurement environment exists. No corpus, workload, or measured-superiority claim meanwhile |

## 7. Decisions requiring the Director

| Decision | Conflicting authority or missing choice | Rule until decided |
|---|---|---|
| Comparative acceptance | #33, #34, #35 require a corpus, baseline, workload, and measurement environment | Leave open. No corpus or workload is committed and no measured-superiority claim is made |

A required decision pauses dependent work. Audits, reproducible counterexamples,
and factual documentation may continue.

## 8. Notes and residual working state

Unrelated working changes remain outside a delivery patch. The original
checkout's unstaged KV edit was preserved during PR #36; its disposition is
untracked; any future appearance of that edit requires explicit review, not
silent import.

Branch protection on `main` is disabled, so merges are currently ungated
mechanically. The merge gate in §3 is the operative gate in that state.
