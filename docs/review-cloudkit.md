# Review: CloudKit as a sync substrate — verdict and contract lessons

> Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
> S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
> S33 S34 S35 S36 S37 S38 S39 S40 S41 S42 S43. Primary URLs:
> `evidence/SOURCES.md`. CloudKit sources below were retrieved 2026-09-16
> from the local Apple developer corpus (`developer.apple.com` pages) and
> crates.io by the review recorded in this document.

- Verdict: **relay-first stands.** CloudKit is a design reference whose
  sync contract this repo already mirrors, plus at most a bounded,
  optional transport adapter for Apple-only consumer apps — never an
  architecture commitment.

## 1. What CloudKit is today

Three databases per container (public/private/shared; private data is
account-gated, public readable without sign-in:
https://developer.apple.com/documentation/cloudkit/ckdatabase.md).
Custom zones exist only in the private database and are the unit of
atomicity — intra-zone transactions, intra-zone references only:
https://developer.apple.com/documentation/cloudkit/ckrecordzone.md.
The modern sync layer is CKSyncEngine (2023+), an app-driven state
machine with delegated batching
(https://developer.apple.com/documentation/cloudkit/cksyncengine-5sie5.md);
Core Data mirrors through NSPersistentCloudKitContainer
(https://developer.apple.com/documentation/coredata/nspersistentcloudkitcontainer.md)
and SwiftData via `ModelConfiguration(cloudKitDatabase: .automatic)`
(https://developer.apple.com/documentation/swiftdata/modelconfiguration/cloudkitdatabase-swift.struct.md).

## 2. The sync contract (what to keep copying)

- **Two-level opaque change tokens** (database + per-zone):
  https://developer.apple.com/documentation/cloudkit/ckserverchangetoken.md
- **Notifications are hints, never data** — coalesced silent pushes; the
  fetch operations are the reliable path:
  https://developer.apple.com/documentation/cloudkit/remote-records.md
  — this is byte-for-byte the Logical-DB contract ("Notifications are
  hints to pull, never state transfer", AGENTS.md).
- **Token and data commit together**; the engine refuses to persist its
  own state — stale token = lost incremental sync:
  https://developer.apple.com/documentation/cloudkit/cksyncengine-5sie5/event/stateupdate.md
- **Seven transient error classes auto-retried** (including
  `requestRateLimited` honoring retry-after); conflict-class errors are
  deliberately app logic:
  https://developer.apple.com/documentation/cloudkit/cksyncengine-5sie5.md
- **Deletions carry reasons** (app-delete / reset / user purge), and the
  reason drives local recovery policy:
  https://developer.apple.com/documentation/cloudkit/cksyncenginezonedeletionreason.md
- **Conflicts are record-level optimistic concurrency** with an
  ancestor record supplied; merge must go into `serverRecord`:
  https://developer.apple.com/documentation/cloudkit/ckerror/code/serverrecordchanged.md

## 3. Hard limits and opacity

Private-DB storage counts against the *user's* iCloud quota; public
against the developer's
(https://developer.apple.com/documentation/cloudkit/ckcontainer.md).
Web-services caps: 200 operations/request, 1 MB records (assets 50 MB):
https://developer.apple.com/library/archive/documentation/DataManagement/Conceptual/CloudKitWebServicesReference/PropertyMetrics.html.
Rate limits are unpublished — only the `requestRateLimited`/`zoneBusy`
error surface exists. The REST reference is frozen archive material
(revision history ends 2016):
https://developer.apple.com/library/archive/documentation/DataManagement/Conceptual/CloudKitWebServicesReference/RevisionHistory.html,
server-to-server auth is public-DB only:
https://developer.apple.com/library/archive/documentation/DataManagement/Conceptual/CloudKitWebServicesReference/SettingUpWebServices.html.
No mature Rust client exists — the crates.io survey found only
macOS-FFI micro-bindings (`cloudkit` 0.3.8, 217 downloads; `objc2-cloud-kit`
bindings; `apple` 0.2.0) — so any Rust transport is hand-rolled REST/JWT.

## 4. Why it cannot host the ontology

Apple-only hosts and Apple-account identity; no self-hosting, no
enterprise governance or audit export; mirroring forbids unique
constraints and requires optional inverted relationships
(https://developer.apple.com/documentation/coredata/creating-a-core-data-model-for-cloudkit.md);
no entity resolution, no semantic layer, no server-side compute beyond
record CRUD + predicates, no agent memory; sharing is zone-shaped
(whole-graph, one share per record hierarchy:
https://developer.apple.com/documentation/coredata/sharing-core-data-objects-between-icloud-users.md).
Conflict resolution is record-level last-writer-with-merge — no CRDTs,
no semantic merge.

## 5. Recommendations (adopted into the PRD's relay work)

1. Keep the FDB relay as the only sync substrate with obligations; the
   change-token/pull-hint/atomic-token-commit contract is already
   implemented here — treat CloudKit as the reference citation, not a
   roadmap item.
2. If an adapter is ever built: Web Services + server-to-server key,
   public-DB scope, behind the existing `Transport` trait as an optional
   feature for Apple-only consumer apps. Native SDK/FFI paths are out of
   scope.
3. Copy the seven-class transient/retry taxonomy into the relay's retry
   classifier; surface conflicts and deletion *reasons* as typed events.
4. Keep deletion-reason semantics on the roadmap for the ontology plane
   (purge reason drives recovery policy — matches our tombstone and
   baseline-recovery design).
5. Subgraph sharing is a differentiator: CloudKit literally cannot
   express it; our CRDT scope/link model can.
