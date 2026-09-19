# ADR-0003: Sovereign hyperscale Cloud Plane

> Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
> S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
> S33 S34 S35 S36 S37 S38 S39 S40 S41 S42 S43. Primary URLs:
> `../../evidence/SOURCES.md`.

> Status: proposed target architecture. Existing FDB relay behavior remains the
> implemented baseline; this ADR must not be read as proof of scale, regional
> availability, or cost.
>
> Related: `docs/PRODUCT-MAP.md`, `docs/architecture/DOMAIN-BOUNDARIES.md`,
> issue #25, issue #28, and closed issue #35.

## Decision

Logical-DB’s Cloud Plane is the sovereign distributed-data domain. It supplies
durable ingress, regional delivery, recovery, and tenant isolation to all
product domains. Local replicas remain first-class execution surfaces because
they lower retained remote data, latency, transfer, and cloud compute; they do
not replace the Cloud Plane as the product’s distributed contract.

The target Cloud Plane has five separately owned layers:

```text
1. capability-bound write ingress
2. partitioned durable causal streams
3. authorized replication-view materialization
4. region-local delivery/fan-out
5. encrypted archive, recovery, export, and audit
```

Current FDB scope-frontier ingestion is Layer-2 baseline behavior. It is not the
target partitioned architecture.

## Required contracts

### Partition routing

A partition contract binds tenant/account, application space, partition ID,
routing epoch, region placement, ordering domain, and recovery authority.

- A client never selects a physical region or bypasses routing authority.
- A stale epoch fails closed and returns a recoverable routing transition.
- Split, merge, rebalance, regional evacuation, and rejoin preserve exact retry,
  causal dependencies, and the declared ordering domain.
- Scope-wide order is not presumed where per-document or per-partition causal
  order is sufficient.

### Authorized replication views

A replication view is a server-authorized, versioned, bounded projection of one
or more partitions. It includes scope, view definition/version, authorization
identity, cursor vector, byte/page limits, revocation state, placement, and
retention/recovery transition.

Client filtering—such as the current `NamespaceDomain` `is_mine` callback—is
not access control and cannot satisfy this contract. A cache/delivery edge may
serve only material already authorized for the exact view and may never become
authoritative for grants, cursor advancement, or revocation.

### Sovereignty and key custody

Every durable Cloud object declares tenant owner, region owner, key owner,
retention/excision policy, backup class, and recovery authority. Tenant/operator
key selection and rotation, auditable placement changes, encrypted backup and
restore, and portable export/import are mandatory contracts.

An optional archive-object-store port may hold large baseline, attachment, and
export objects. RustFS is a candidate self-hosted implementation for that port
only. Adopting RustFS or its crates requires a separate dependency-admission
record and cannot modify Cloud semantic authority.

### Delivery and recovery

Ingest correctness, materialized view generation, and fan-out delivery are
separate resources with explicit ownership and bounds. Delivery must provide
admission, backpressure, per-tenant isolation, regional cache lifecycle, and a
recovery path for revoked/expired/corrupt cursors. Regional fault, process loss,
retry, restore, and rejoin must preserve integrity and disclose unavailability
rather than silently rerouting data across residency boundaries.

## Nine-axis acceptance contract

| Axis | Required invariant | Required measured evidence |
|---|---|---|
| Frontier | The Cloud Plane beats credible current architecture classes on the declared problem without weakening correctness or sovereignty. | Dated matched traces against managed-sync, database-log-sync, CRDT-sync, and object/compute-stack classes; versions/configurations/cost disclosed. |
| Hyperscale | More than 1,000,000 active concurrent clients remain correct under network diversity, tenant skew, faults, and recovery. | Staged ladder ending above 1M, distinguishing delivery clients, active writers, and background replicas; sustained/burst/saturation/recovery telemetry. |
| Idiomatic Rust | Ownership, error, resource, and state boundaries stay explicit across all Cloud contracts. | Compiler/lint, CodeGraph dependency receipt, failure-path and cancellation tests for each boundary crate. |
| Generalized | Partition/view/placement behavior works across declared tenant, region, topology, and dataset families. | Independently selected partition/view/topology cases, property tests, and deployment variation. |
| Decoupled | Ingest, routing, views, delivery, archive, KMS, and stores are replaceable through published contracts. | Contract compatibility/replacement fixtures and version-skew tests. |
| Ephemeral | Loss of processes/regions does not corrupt durable state; recovery is owned and bounded. | Crash, failover, restore, partition/rejoin, retry, and cancellation receipts. |
| Portable | Required semantics reproduce on every declared deployment target. | Reproducible builds and actual E2E results by target; untested targets visible. |
| Multi-tenanted by default | Views, keys, caches, receipts, quotas, placement, and recovery cannot cross tenant/region authority. | Cross-tenant/cross-region/revocation/noisy-neighbor/failure-isolation negatives. |
| Fastest possible correct Rust | Performance comparisons include complete semantic path and do not hide cost in an external plane. | Matched p50/p95/p99, CPU, allocation, memory, I/O, contention, egress, availability, and per-success cost measurements. |

No present-tense marketing statement may claim an unmet row.

## Execution order

1. domain boundary enforcement;
2. partition/routing and authorized-view contracts;
3. residency/key/exit contracts in parallel with the first two;
4. delivery/fan-out and multi-region disaster recovery after those contracts;
5. benchmark harness and staged scale receipts last.

This extends, not replaces, issue #28’s state-bearing client recovery and
issue #25’s checked transition graph.
