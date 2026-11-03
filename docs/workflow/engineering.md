# Workflow: engineering practice

> Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
> S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
> S33 S34 S35 S36 S37 S38 S39 S42 S43. Source URLs are in `../../evidence/SOURCES.md`.

Sections 3 to 6 of [`WORKFLOW.md`](../../WORKFLOW.md), split out under the
120-line law. The numbering is kept so existing citations still resolve.

## 3. RAG yourself into expertise

Before implementing anything non-trivial, find two or three production OSS
implementations of the same thing and study their structural decisions: module
organization, trait design, error handling, concurrency primitives, tests. Five
to ten minutes. Record what you are adopting and why, and what you rejected and
why, in your first edit or message. Then implement at that level.

Skip this for trivial changes under twenty lines with no new abstraction, bug
fixes whose root cause is already identified, and config/docs/CI work.

This is not copying code. It is studying architecture and making an informed
decision. The mechanism is RAG for latent implementation expertise: reading real
code activates specifics that prompts alone do not.

## 4. One path, one opinion, one architecture

A capability is either **the** architecture or it does not exist.

- **No default-off parallel paths.** A feature flag that makes an implementation
  a *candidate* is how work fails to carry forward: nothing exercises it, the
  gate never compiles it, and it rots into a second opinion nobody chose. If it
  is how the estate does the thing, it is on by default and the gate runs it.
- **One implementation of a job.** Two builders, two executors, or two
  "equivalent" entry points for the same work is one too many. Pick the right
  one, make it the only one, delete the other including its tests, docs, and
  feature flag.
- **Unification beats a seam.** "Parallel seams, not demolition" governs *how* a
  change lands while the tree stays green. It is not a licence to leave both
  halves standing. The seam closes in the same task.
- A storefront may keep capability features default-off, because selecting is
  its stated purpose. A **consumer** states its opinion: it turns on what it
  uses, in its own defaults, and the workspace gate compiles and tests it.

## 5. Compounding, buildable, surgical

**Compounding.** Every unit, trait, or module added makes future tasks easier.
New capabilities plug in through existing traits without rewriting call sites. If
adding a feature requires rewriting half the repo, the previous architecture was
anti-compounding debt.

**Always buildable.** Every single commit compiles and passes tests. Never
commit a broken intermediate state intending to fix it in the next step. If a
refactor breaks the build for more than twenty minutes, the change radius is too
broad.

**Surgical change radius.**

- Touch only the lines the invariant requires. No gratuitous mass renames, no
  cosmetic formatting rewrites, no sweeping module migrations in the same turn
  as a semantic fix.
- Identify before touching: map callers and downstream consumers first. One
  CodeGraph query replaces five grep chains.
- Parallel seams over in-place demolition when replacing a subsystem: introduce
  the new implementation alongside the old, point callers at it one at a time,
  verify tests, then delete the old code.

Time-box is thirty minutes by default. Not terminal within the box: stop, report
the SHA and the receipt, await direction. Identical-state retry is forbidden.

## 6. Evidence proportionality

| Change | Evidence |
|---|---|
| Product runtime | full nine-axis SLO matrix, measured per axis |
| Config, docs, CI, README, scripts | one line of receipt per file |
| Instruction or config loading | config-loading evidence only |

**The nine axes.** Frontier (design beats credible alternatives), Hyperscale
(>1M concurrent, correct), Idiomatic (ownership, errors, lifetimes sound),
Generalized (works across declared inputs), Decoupled (components independently
replaceable), Ephemeral (survives process loss), Portable (same semantics on all
targets), Multi-tenant (isolated under concurrent use), Performance (fastest
correct implementation).

Every claim carries evidence: command, exit code, result, artifact ID. Before
completion, account for every requirement, every failed check, and every
unknown. No hidden exclusions.

AI and model output is untrusted first-party evidence until independently
verified. Every capability claim separately states which of these it is:
**planned**; **present in code at an exact commit**; **exercised on the exact
claimed path**; **independently evidenced**; **safe to rely on or merge**. Never
collapse these into "done".
