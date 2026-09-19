# ADR-0004: Forge operates above LEM

> Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
> S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
> S33 S34 S35 S36 S37 S38 S39 S40 S41 S42 S43. Primary URLs:
> `../../evidence/SOURCES.md`.

> Status: proposed. Forge contracts are not implemented by this ADR.
>
> Canonical domain map: `docs/architecture/DOMAIN-BOUNDARIES.md`.

## Decision

Forge is an independent operations domain above LEM / ORIGIN. Cairn owns
observation and candidate evidence; LEM owns canonical semantic memory; Forge
owns capability-bound action invocation and receipt production.

This preserves the closed-loop rule without turning ontology ingestion or memory
into an ambient executor:

```text
Cairn candidate → LEM append → bounded versioned LocalWorld → Forge action
      ▲                                                        │
      └──────────────── Forge receipt candidate ──────────────┘
```

## Forge input and output contract

Every Forge action has:

- a declared action type and version;
- a bounded `LocalWorld` input identity/frontier;
- an explicit capability grant scoped to effect, tenant, view, region, expiry,
  resource budget, and idempotency identity;
- a cancellation/deadline/retry classification;
- an immutable receipt candidate containing request, decision, inputs, action,
  outputs, error, correction, user feedback when supplied, software/schema
  versions, and evidence links.

Forge never obtains direct mutable Cloud, LEM, or Cairn implementation handles.
It invokes an application/host-owned capability port. Its receipt returns to
Cairn as evidence and becomes canonical only through the LEM admission path.

## Prohibitions

- Cairn may not execute an action, turn a candidate into canonical ORIGIN state,
  or assign semantic authority.
- LEM may not run actions or let a projection become an execution authorization.
- Forge may not directly append an ORIGIN episode, mutate a Cloud store, or omit
  a receipt after an attempted effect.
- An LLM response is not a receipt or memory record unless it traverses the
  corresponding candidate and append contracts.

## Required proof

The Forge workstream must prove: valid grant execution; expired/revoked/wrong
scope grant refusal; exact retry; duplicate receipt handling; cancellation;
crash/restart continuation; two-tenant isolation; stale-LocalWorld behavior;
and a black-box closed loop that records every stage through evidence links.

This ADR preserves the predecessor LEM law that every action produces receipts,
while keeping the executor independently developable and releaseable.
