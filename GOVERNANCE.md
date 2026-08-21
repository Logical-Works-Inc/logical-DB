# Governance

How `logical-DB` is governed: who decides, what cannot change, how a change
is made and verified, and the security boundaries that are off-limits to
ordinary edits.

**Authoritative companion documents.** `docs/DB_PATHWAY.md` is the single
source of truth for the *product* (kernel thesis, storage classes, compute
fraction, phases). This file governs the *project* — authority, decision
process, review, proof, security, and licenses. `laws/governance/` holds the
operating process imported from logic-os-kernel (ticketing, DoD, ADRs).
`CODEBOOK.md` binds agents working in the repo; `CLAUDE.md` is the runtime
entry.

## 1. Authority model

- **Director** — final authority on intent, scope, and outward-facing or
  irreversible decisions (license, shipping, vendor relationships,
  constellation registration).
- **Thinking/review lane** — owns problem framing, adversarial questioning,
  architecture, research, plan and issue authoring, and diff review. Decides
  `REJECT`/`REPAIR`/`ACCEPT WITH PROOF`/`UNKNOWN` on every diff.
- **Execution lane** — owns the raw diff: implementing issues, mechanical
  edits, bulk refactors. Hands the diff back for review; does not
  self-certify.

The handoff between lanes is the **issue**
(`laws/governance/issue-template.md`): intent, tasks, exit criterion, and a
proof command with `file:line` for every named defect. An issue that needs
interpretation is an authoring failure.

## 2. Invariants (the contract that outranks convenience)

1. **One model across every repo.** INV-ONE-MODEL-EVERYWHERE: this repo is
   the canonical home of the DB model and kernel. No consumer repo (sentinel,
   logic-os-kernel, …) keeps a parallel storage truth; consumers bind through
   published ports and file issues here for model changes.
2. **No silent fallback.** Unknown class or failed stage → escalate
   (`Capability::Unknown(Escalate)`), never a benign default.
3. **Content-addressed everything.** Rows, segments, weights, and claims are
   content-addressed; `verify()` fails closed.
4. **DiD by construction.** INV-DID-GAUNTLET + INV-NO-STRING-QUERY
   (DB_PATHWAY §11): nothing raw persists; no string-query surface exists.
5. **Out-of-box.** A fresh clone opens and queries with zero external
   services; daemons (FDB, watchers, mounts) are opt-in adapters only.
6. **Permitted languages.** Rust core; C++ only at named boundaries
   (FDB adapter, optional FUSE backend). No other language enters the kernel.
7. **Compute honesty.** Any claim of a compute fraction must name its
   mechanism and its measurement (§3); assertion without a benchmark does not
   change the document.

## 3. Decision process

- **Decision records.** Non-obvious decisions are written down *with their
  evidence and invariant* as ADRs in `laws/governance/adr-*.md`, so an
  executor who was not in the room can apply them. An undocumented decision
  does not transfer.
- **Amendments to DB_PATHWAY** cite new evidence, name the invariant touched,
  and leave §2 intact unless revised explicitly. Assertion without evidence
  does not change the document.
- **Lane discipline.** Grilling a decision is expected. Deciding for the
  Director on anything outward-facing or irreversible is not.

## 4. Review & merge

- Verdicts: `REJECT` · `REPAIR` · `ACCEPT WITH PROOF` · `UNKNOWN`. "Looks
  good" is invalid.
- Proof ladder (lowest → highest): `parse → type/compile → focused
  regression → integration boundary → real journey → negative/malformed →
  fault/recovery → perf → security → clean env → post-merge observation`.
  The author's new test is never sole proof.
- CI is keel-derived (`ci/keel-ci.toml` → `.github/workflows/ci.yml`);
  local runs go through `.wwfd/local-ci.sh`, which writes the spend-guard
  receipt on green. Merge locally, never via GitHub merge commits — eval
  parity requires local evidence at the exact pushed sha.
- Do not merge a diff that weakens a gate, swallows an error, or changes a
  test's expectation to hide a regression.

## 5. Threat model & security boundaries

The security surface is deliberately small and fail-closed:

- **Ingestion is THE boundary.** Untrusted bytes (files, crawls, model
  output) enter only through the gauntlet (§11 of DB_PATHWAY). Security-
  sensitive changes touch `ingest*`, validation schemas, or capability
  checks and require the negative path demonstrated: a poisoned input is
  *refused*, a forged provenance hash *fails verify()*.
- **Query surface.** Typed ASTs only; any PR adding a string-query door is
  rejected on sight (INV-NO-STRING-QUERY).
- **Escalation integrity.** Model-derived rows never acquire source
  authority without a recorded transformation (claim lineage).

## 6. Licenses

- Kernel dependencies must be MIT/Apache-2.0/BSD-class. **NOASSERTION or
  unverified-license dependencies stay off the default path** until terms
  are read and recorded here.
- **Copyleft stays off the default path**: no AGPLv3/SSPL dependency in the
  shipped kernel (the SearXNG precedent from sentinel applies verbatim).
  Copyleft may power an opt-in outboard adapter only if the boundary keeps
  it out of the linked work — Director decision required per case.
- The workspace carries no license field yet — an open Director decision
  (§7), not an accidental omission to be filled by an executor.

## 7. Open decisions (unresolved)

| Decision | Owner | Status |
|---|---|---|
| Workspace license | Director | open |
| Constellation registration of authorities | Director | open — ADR-001 |
| FUSE backend choice | Director | open — post DB-B |
| HelixDB adapter scope | Director | settled 2026-08-21 — survives |
