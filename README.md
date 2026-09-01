# logical-DB

One processing and storage contract for every app. The crate can run
in-process over a durable redb file or beside a native FoundationDB client.
Both profiles use the same account/container/zone key contract, bounded
request processing, Braid admission, idempotency markers, and durable zone
change feed.

**Status:** hybrid v2 is implemented on the current feature branch, not yet
released. The default build supports in-memory and durable embedded profiles.
`HybridDb::embedded_at` is the app-owned durable entry point. The `fdb`
feature adds `OutboardDb`, which drives the pinned FoundationDB 7.3 client
without giving applications a raw database handle. `HybridDb` exposes the common
`read_version`, `commit_admitted`, `get_admitted`, and `changes_admitted`
surface. Effectful operations consume a real Braid `RunnableInvocation`; its
snapshot and predicate prove the canonical CID of the exact database request.
`BraidExecutionBinding::inspect` also derives the exact Logical-DB capability
set and strongest effect from the capsule and registry; read, delete, and
unrelated write capsules cannot authorize a put request.
Namespace authority remains external and scope-bound. Every commit atomically
writes its data, private markers, zone conflict frontier, and ordered change
row.

The older five-class journal model remains available as a compatibility and
fault-oracle layer. It is not the hybrid app API. Migration begins with the
lossless `LegacyV1Point` copy primitive and follows copy, verify, cutover;
source deletion is deliberately outside that primitive. The live native-FDB
test exercises the hybrid API; a green remote Linux receipt and an LEM
consumer receipt are still required before this branch is production-ready.

## Embedding contract

Applications do not receive a raw KV or provider handle. They first build a
bounded logical request from `Scope`, a public `KeyFamily`, `Tuple`,
`OperationId`, and, for writes, `WriteBatch`. That request yields a
`RequestBinding`. Braid puts its CID in `logical_db.request_cid`, proves exact
equality in the justification, and snapshots `logical_db.read_version`. The
trusted host inspects the capsule and registry into a
`BraidExecutionBinding`, then moves the `AdmittedTxn` and exact request into
`HybridDb`.

`HybridDb::embedded_at` executes the layer in the app process over redb.
`HybridDb::outboard` executes the identical layer beside the official FDB
client. A future CloudKit-shaped HTTP or device-sync adapter belongs above this
boundary and must reconstruct these request bytes rather than add a second
database model. Internal change/outbox families and raw transactions are
sealed; `legacy-raw` is an explicit migration escape hatch.

Both providers enforce the configured timeout before storage work begins.
FoundationDB additionally enforces it inside the native transaction. Embedded
execution will not report a timeout after durable mutation has started; it
returns the real commit outcome. Native uncertainty is a typed
`CommitOutcomeUnknown` and requires retrying the same operation ID and bytes.

## Try it

    cargo test --workspace

The default profile needs no daemon. An app embeds the durable profile with
`HybridDb::embedded_at(path, DatabaseOptions::default())`. Compile the outboard
adapter with
`cargo check --workspace --all-targets --features fdb`; a live run additionally
requires the FoundationDB C client, network guard, and cluster recipe.

On macOS, the hermetic live-FDB journey is:

    export FDB_SCRATCH=/tmp/fdb-f0
    scripts/local-fdb-cluster.sh verify
    scripts/local-fdb-cluster.sh up
    export FDB_CLUSTER_FILE="$FDB_SCRATCH/fdb.cluster"
    export RUSTFLAGS="-L native=$FDB_SCRATCH/macbin \
    -C link-arg=-Wl,-rpath,$FDB_SCRATCH/macbin"
    cargo test --features fdb --test fdb_live -- --ignored --nocapture
    scripts/local-fdb-cluster.sh down

## The five axes

1. **One model across every repo** — sentinel, logic-os-kernel, and
   anything else bind through published ports; nobody forks the storage
   truth.
2. **A fraction of the compute** — quantize by default, rescore rarely;
   mmap-first, immutable segments, and hydration lifecycle are design
   intent pending their phases.
3. **Mouldable / low-code** — schemas and use-case packs are rows; a new
   use-case is data, not a migration or a recompile (design intent).
4. **GNN-native** — message passing over stored edges as a query
   primitive (design intent); AI and non-AI artifacts are rows
   distinguished only by claim lineage.
5. **A filesystem face** — vault-shaped and desktop-search-shaped
   projections over the same store (design intent).

Non-negotiable: **DiD by construction** — everything entering passes the
ingest gauntlet; queries are typed ASTs with bound parameters; there is
no string-query surface, so injection-class bugs are structurally absent.
Rust core; C++ only at named boundaries.

## Read order

`VISION.md` → `docs/DB_PATHWAY.md` → `docs/FORMAT.md` → `GOVERNANCE.md`
→ `laws/governance/`.

## Governance & CI

Governance structure follows logic-os-kernel's (ticketing taxonomy,
definition-of-done, ADR series); the constitution lives in `GOVERNANCE.md`.
CI is keel-derived (`ci/keel-ci.toml` → `.github/workflows/ci.yml`) with
eval parity: local runs go through `.wwfd/local-ci.sh`, which writes the
spend-guard receipt on green; merges are local `--no-ff`, never GitHub
merge commits.
