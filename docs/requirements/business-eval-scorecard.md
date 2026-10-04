> Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
> S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Primary URLs:
> `../../evidence/SOURCES.md`.

# Business evaluation scorecard — what the non-technical side buys

> Audience: accountable executives (risk, ops, finance, amalgamation steering),
> not engineers. Every number is either **measured** (receipt named),
> **published by someone else** (source named), or **PLACEHOLDER** (structure
> only — fill with your ledger, do not quote).
>
> Read with: `semantic-invariant-matrix.md` (what a database guarantees),
> `nine-axis-requirement-graph.yaml` (what we still owe), `evidence/matched-trace/`.

---

## 1. Performance scorecard (as of 2026-09-22)

### 1a. What we measured (receipt: `evidence/matched-trace/embedded-*.json`)

Embedded replica on redb, 32 documents, **32 operations per durable commit**,
1 run each. Latency is **per commit** (the unit the user feels: “my save
returned”). CPU and RSS are **unmeasured** (no portable API without a new
dependency — that is an open pin, not a zero).

| Workload | Cold start | Reopen after close | p50 / commit | p99 / commit | Throughput | Durable bytes | Correctness checks |
|---|---|---|---|---|---|---|---|
| 5,000 ops (156 commits) | 43.3 ms | 7.1 ms | **15.2 ms** | 26.2 ms | 2,084 ops/s | 10.2 MB (2.0 KB/op) | duplicate retry ✓ index ✓ tombstone ✓ |
| 20,000 ops (625 commits) | 29.0 ms | 7.7 ms | **39.6 ms** | 96.9 ms | 781 ops/s | 40.6 MB (2.0 KB/op) | ✓ ✓ ✓ |
| 60,000 ops (1,875 commits) | 35.6 ms | 6.6 ms | **116.2 ms** | 378.8 ms | 246 ops/s | 81.3 MB (1.4 KB/op) | ✓ ✓ ✓ |

**What this does say:** reopen after close is ~7 ms (the Gary problem of “can I
restart the app?” is cheap); integrity checks passed at every size; durable
bytes grow sub-linearly in this shape (1.4–2.0 KB/op including receipts,
changes, indexes, outbox).

**What this does not say:** nothing about multi-replica, nothing about
Postgres, nothing about 1M users, nothing about CPU/RSS, one run only.
Status in the nine-axis ledger: **A09 Unmeasured** against alternatives.

### 1b. What native_db published (source: `vincent-herlemont/native_db` `benches/README.md`, criterion + criterion-table)

Their own caveats: initial benchmark, ignores serialization and `native_model`
overhead, redb has no secondary indexes (N/A columns), delete batch regression
tracked in their #256.

| Operation | redb | native_db | SQLite | Reading for a buyer |
|---|---|---|---|---|
| Insert 1 secondary key, batched txn | **1.09 µs** | 5.36 µs | 1.68 µs | raw key write is **microseconds** |
| Insert 100 SK, one-txn-each | N/A | 916 µs | **98 µs** | index count dominates |
| Get 1 secondary key | **492 ns** | 961 ns | 1.99 µs | point read is **sub-microsecond** |
| Delete 100 SK, batched | N/A | 349 µs | **2.36 µs** | SQLite wins delete by 148× (their numbers) |

### 1c. The honest translation (this is the slide)

| Question | Answer |
|---|---|
| Are we faster than redb / native_db / SQLite on raw get/put? | **No. Not close. Do not say we are.** Their world is microseconds; ours is a durable commit of a whole document’s meaning (snapshot + change + receipt + outbox + indexes + event). |
| Why is our p50 15–116 ms when theirs is µs? | Different unit of work. They insert a row. We commit an explained, retryable, syncable mutation of a document and rewrite its materialized snapshot. |
| Will a client feel it? | Local single-user edit: yes, it is tens of ms per save in the current trace. Interactive local cache (`logical_db::cache`) is the path for hot keys — that has not been traced yet. |
| What is the business claim we **can** make? | **Data efficiency on the wire and in retention**, plus **risk retirement** (see §3). Not raw speed. |

### 1d. Data-efficiency claims (the one performance-ish claim we defend)

| Claim | Evidence | Business meaning |
|---|---|---|
| Durable footprint **1.4–2.0 KB per operation**, receipts included | measured traces | you are not paying for a separate audit table |
| Sync moves **changes**, not tables | design + outbox | remote does not hold every working set; lower retained remote data, transfer, compute (PRODUCT-MAP) |
| Large values **content-addressed**, one digest | `logical_db::cache` tiering | two advisors open the same 3 MB PDF ⇒ shipped once |
| Log can **shrink** (peer-aware retention floor) | R2/GC contract | unlike a forever-growing binlog |
| Export is a **bounded logical dump**, restore refuses corruption | AE3 / `MAX_EXPORT_BYTES` | exit and backup are not “a file and a prayer” |
| On-disk vs Postgres | **do not claim** | we rewrite a full snapshot per edit (write amplification). On-disk we are worse until compaction is measured. |

---

## 2. Business evaluation — the scorecard a steering committee actually uses

Scale: **1** = materially worse than the default (Postgres + people), **3** =
parity, **5** = materially better. **PLACEHOLDER** means the committee fills the
number with its own ledger.

| # | Criterion | Postgres + team | “SQL as found” (Gary) | RedDB | **logical-db** | How it is evidenced |
|---|---|---|---|---|---|---|
| B1 | Time to a safe cutover (amalgamation) | 2 — dual-write you invent | 1 — big bang | 2 — migration story is “git for data”, no multi-master | **4** — dual-write + digest equivalence + export receipts are the product | D3 journeys; 90-day pilot below |
| B2 | Audit / dispute resolution cost | 3 — queryable, archaeology for “why” | 1 — binlog + tribal knowledge | 2 — `ASK` is an LLM summary, not a receipt | **5** — every mutation has identity + receipt + frontier | M1–M3, O2 |
| B3 | Safe to give an **agent** write access | 1 | 1 | 2 (MCP mode; authority undocumented) | **4** (D2 designed; deferred) | O1 / grant tests (EP-X4) |
| B4 | Regulatory / residency / exit | 3 — dump + contracts | 2 | 2 — **BSL-1.1, AGPL in 2030** | **4** — portable logical export + archive key identity | L3–L5 |
| B5 | Vendor / license risk | 5 — boring and universal | 5 | **1** — BSL, commercial for managed | **4** — estate Apache/MIT-class | O8 |
| B6 | Headcount to operate (Tesler) | 2 — DBAs, replicas, vacuum | 2 — one Gary-shaped hero | 3 — new engine, small ecosystem | **3** — no SQL DBA required for the embed path; Cloud Plane is real work | §4 people plan |
| B7 | Query power (BI, PowerBI, joins) | **5** | 3 | 4 (7 models + SQL/Cypher) | **1** — no joins/aggregates by design; needs a sink | R8–R12 |
| B8 | Latency of a simple read/write | 4 | 4 | 4 (claims unvalidated) | **2** — see §1; cache path untraced | §1a |
| B9 | Multi-site / remote teams offline | 2 | 1 | 1 (one writer, `flock`) | **5** — local-first is the product | S1–S3 |
| B10 | Cost of **not** changing (risk retirement) | — | **1** — amalgamation + agent accident + audit finding | — | this is the buy | §3 |
| B11 | Ecosystem / hiring / “can we replace it later” | 5 | 5 | 2 | 3 (logical export; Postgres sink is the plan) | L5 + §5 |
| B12 | Proof maturity (can we show a regulator?) | 5 | 2 | 2 (self-claims; conformance matrix has ⚠️) | **2** — A01/A02/A09 open; this scorecard is why the 90-day proof exists | nine-axis ledger |

**How to read it.** We are the wrong buy if the question is “fastest OLTP” or
“best PowerBI source” (B7/B8). We are the right buy when the binding risks are
**cutover without evidence (B1), explainability (B2), agent safety (B3), and
exit (B4)** — which is exactly the IPC/Quadrus situation.

---

## 3. Risk retirement — the actual ROI frame

Cost of staying (Gary / SQL-as-found) — **PLACEHOLDER rates for your ledger**:

| Failure mode | Frequency | Cost per event | Annualized | Insured by |
|---|---|---|---|---|
| Amalgamation cutover defect (1 Oct 2026) | one-time, high | ____ (book restatement + client trust) | one-time | D3 dual-write digests |
| Dispute: “why is this position X?” | __ / yr | ____ (advisor hours + compliance) | ____ | receipts + frontier (M3) |
| Agent or script made a bad write | rising | ____ (reversal + audit) | ____ | scoped grant + exact-request confirm (D2) |
| Key person (Gary) departure | already happened | ____ | ____ | provenance in the substrate, not in a person |
| Forced migration / regulator export request | rare, severe | ____ | ____ | versioned export + corrupt-restore refusal |
| Audit finding: cannot reconstruct a decision | rare, severe | ____ | ____ | tamper-evident change log (L10) |

Cost of switching (honest):

| Item | 90-day pilot | Year 1 steady state |
|---|---|---|
| Dual-write bridge on one dealer-shaped book | 1 engineer × 90 days | maintained as the cutover runbook |
| Digest equivalence harness + receipts to the steering pack | included above | artifact for the regulator |
| Postgres/SQL sink for PowerBI | 0–small (one export pipeline) | kept — we are not your BI store (B7) |
| Cloud Plane / multi-region | **out of pilot** | staged (A02/#72, #71) |
| Training | 2 days for app devs (Tesler: one dependency) | 0 DBA cult |

**Payback logic (not a promise):** if the cost of one amalgamation defect or one
lost dispute exceeds the 90-day pilot, the pilot is rational insurance. That is
the whole pitch. It is not a throughput pitch.

---

## 4. People and operating model (Tesler’s law, translated)

| Who | Postgres world | logical-db world |
|---|---|---|
| Application developer | SQL + migration scripts + “don’t forget the audit trigger” | one crate: open, edit, sync, export. Receipts are default |
| DBA / platform | 2+ FTE: replicas, vacuum, PITR drills, logical decoding | embed path: none. Cloud Plane: real work, but it is **our** contract, not your schema |
| Compliance | ad-hoc queries into someone else’s audit table | receipts + change log are the evidence pack |
| The AI already in your org | read-only guesses over an opaque schema | reads the same receipts a human does; writes only under a scoped grant |
| Gary (or the next Gary) | the system | the runbook, in git, with digests |

---

## 5. Exit and lock-in (the “can we fire you?” row)

| | Postgres | SQL-as-found | RedDB | **logical-db** |
|---|---|---|---|---|
| Can we leave? | Yes (SQL dump) | Yes, painfully | File + **BSL terms** | Yes (versioned logical export, digest-verified) |
| Leave with **evidence**? | No (dump ≠ history) | No | Partial | **Yes** (export replays operation order and bindings) |
| Leave with **meaning**? | Schema + tribal knowledge | No | Models + prompts | Typed model + projection semantics + receipts |
| Planned Postgres sink | n/a | n/a | n/a | **Yes** — Postgres remains the BI/joins home (B7 honest) |

---

## 6. The 90-day proof (what goes on a board slide)

| Day | Deliverable | Business artifact |
|---|---|---|
| 0–30 | TDD acceptance suite green: replay, kill/reopen, corrupt-restore refuse, two-replica convergence, checkpoint-expiry recovery | “Our semantics hold under failure” one-pager |
| 30–60 | Dual-write one dealer-shaped book (9917 or 7752) into the substrate **and** the incumbent | **Digest equivalence report** |
| 60–90 | Cutover rehearsal on a staging slice + export/restore drill to the archive keys | **Cutover runbook with receipts** + exit drill |
| 90 | Criterion + native_db-class harness on the must-win five; disclose receipt tax | Honest latency slide: we are slower on raw, we retire B1–B4 |

Stop condition: if digest equivalence fails or corrupt-restore does not refuse,
**do not cutover**. That is what fail-closed means in money terms.

---

## 7. One paragraph for the steering pack

> We are not buying a faster database. We are buying a cutover that can be
> proven, an audit trail that answers “why” without archaeology, and an exit
> we can rehearse — during an amalgamation that is already forcing change.
> Postgres stays for BI and joins. The substrate sits under the paths where a
> silent wrong answer is expensive. We are measurably slower on raw reads and
> writes (see scorecard §1); we are not asking anyone to believe we are faster.
> The 90-day pilot retires the four risks on the critical path and leaves a
> receipt pack a regulator can read.
