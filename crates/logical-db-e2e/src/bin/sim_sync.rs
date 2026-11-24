//! Deterministic simulation of replica synchronization through the real
//! service under injected network faults.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
//! S33 S34 S35 S36 S37 S38 S39. Source URLs are in `evidence/SOURCES.md`.
//!
//! One seed controls every choice: which replica acts, which document it
//! writes, and whether the network loses an exchange before the service
//! commits it, loses the answer after the service committed it, or delivers it
//! twice, or whether the clock jumps past the checkpoint lifetime so the next
//! pull must resynchronize from scratch. Choices are drawn from a SplitMix64
//! stream over the seed, so a seed replays exactly and its trace hash proves
//! it. The replicas are real embedded databases and the service is the real
//! protocol service over the journey store; only the wire between them is
//! simulated (`sim_sync/net.rs`). After the fault phase the network heals and
//! every replica must hold exactly the model's state: each replica's last
//! write to each document, nothing lost and nothing doubled. The service then
//! seals a baseline at a seeded position and prunes the log below it, and a
//! fresh replica joins under the same faults: it can only recover through the
//! baseline, and it must converge to the same model.

#![forbid(unsafe_code)]

#[path = "sim_sync/sim_net.rs"]
mod net;

use std::{
    any::Any,
    collections::BTreeMap,
    fs,
    io::{self, Write as _},
    num::NonZeroUsize,
    path::PathBuf,
    sync::Arc,
};

use lgwks_bot::rt::runtime::Builder;
use logical_db::{
    AccountId, Database, DocumentId, Item, Object, OperationDigest, OperationId, PeerId, ReplicaId,
    Scalar, Scope, SpaceId, SyncLimits,
};
use logical_db_e2e::support::{Allow, JourneyService, MemoryServiceStore, MemoryStoreError};
use logical_db_service::{
    CheckpointCodec, ConsumedResult, ProtocolService, PruneResult, SealResult, ServiceStore,
};

use net::{Draws, Net, Trace};

/// Replicas sharing one scope through one service.
const REPLICAS: usize = 3;
/// Documents in the scope, each created by one owner replica.
const DOCUMENTS: usize = 4;
/// Seeded actions in the fault phase.
const STEPS: u64 = 48;
/// Healed rounds allowed before convergence is declared failed.
const HEAL_ROUNDS: usize = 8;
/// Lifetime of an issued checkpoint; a clock jump past it expires every
/// checkpoint a replica holds.
const CHECKPOINT_TTL_MS: u64 = 60_000;
/// Index of the clock-jump counter in a run's fault tally.
const CLOCK_JUMPS: usize = 4;
/// Seeds the convergence campaign explores.
const CAMPAIGN_SEEDS: u64 = 256;
/// Parallel lanes. Every write commits with immediate durability, so one run
/// is bound by fsync latency, not CPU; independent seeds run side by side,
/// each lane on its own single-worker runtime, without changing any seed's
/// schedule.
const LANES: usize = 32;

/// The real protocol service over the shared journey store. The admit-all
/// policy grants every justified request, so every refusal comes from the
/// protocol.
type Service = JourneyService<Allow<AdmitAll>>;
/// The policy that admits every scope, effect and action.
type AdmitAll = fn(Scope, logical_db_model::Effect, logical_db_model::ActionDigest) -> bool;
/// A simulation failure: any defect ends the seed with its message.
type Failure = Box<dyn std::error::Error>;
/// Per-class fault tally: clean, lost request, lost response, duplicate, clock jump.
type Faults = [u64; 5];

/// One embedded replica and the file it lives in.
#[derive(Debug)]
struct Replica {
    /// The replica's database.
    database: Database,
    /// Its redb file, removed by [`World::remove`].
    path: PathBuf,
}

/// Everything one seed's run owns.
#[derive(Debug)]
struct World {
    /// Shared by every tenant world built over it with [`World::sharing`].
    service: Arc<Service>,
    /// The service's store, read for the frontier compaction seals through.
    store: Arc<MemoryServiceStore>,
    /// The tenant scope every replica of this world shares.
    scope: Scope,
    /// The seed that drives this world.
    seed: u64,
    /// Distinguishes this world's files from every other world's.
    label: String,
    /// The replicas, in join order.
    replicas: Vec<Replica>,
    /// The scope's documents.
    documents: Vec<DocumentId>,
    /// The service peer every replica syncs with.
    peer: PeerId,
    /// The seeded choice stream.
    draws: Draws,
    /// The run's trace.
    trace: Trace,
    /// The run's fault tally.
    faults: Faults,
    /// Simulated wall time.
    now_unix_ms: u64,
    /// Cycles that found their checkpoint expired and resynchronized fully.
    full_resyncs: u64,
    /// Cycles that installed a state-bearing baseline.
    recoveries: u64,
    /// Last value each replica wrote to each document: the convergence oracle.
    model: BTreeMap<(usize, usize), i64>,
}

/// Result of one seed: its trace hash and the faults it injected.
#[derive(Debug)]
struct Run {
    /// The run's trace digest.
    trace: u64,
    /// The faults the run injected.
    faults: Faults,
    /// Cycles that resynchronized from an expired checkpoint.
    full_resyncs: u64,
    /// Cycles that installed a baseline.
    recoveries: u64,
}

/// The per-replica key a replica writes in every document.
fn key(replica: usize) -> String {
    format!("replica-{replica}")
}

/// Admits every request, so every refusal the simulation sees is the protocol's.
const ADMIT_ALL: AdmitAll = |_scope, _effect, _action| true;

/// Replays the seeds named on the command line (the whole campaign when none
/// is named) and prints one line per seed: its trace hash and fault tally, so
/// a failing seed from CI reproduces with `sim_sync <seed>`.
fn main() -> Result<(), Failure> {
    let named = std::env::args()
        .skip(1)
        .map(|seed| seed.parse::<u64>())
        .collect::<Result<Vec<_>, _>>()?;
    let seeds = if named.is_empty() {
        (0..CAMPAIGN_SEEDS).collect()
    } else {
        named
    };
    let runs = run_seeds(&seeds, "cli", &|seed, label| async move {
        simulate(seed, &label).await
    })?;
    let tenants = run_seeds(&seeds, "cli-tenants", &|seed, label| async move {
        simulate_tenants(seed, &label).await
    })?;
    let mut out = io::stdout().lock();
    for &(seed, ref run) in &runs {
        writeln!(
            out,
            "PASS sim-sync seed={seed} trace={:016x} faults={:?} full_resyncs={} recoveries={}",
            run.trace, run.faults, run.full_resyncs, run.recoveries
        )?;
    }
    writeln!(out, "PASS sim-sync tenants isolated={}", tenants.len())?;
    Ok(())
}

impl World {
    /// A world with its own service and store.
    fn new(seed: u64, label: &str) -> Result<Self, Failure> {
        let store = Arc::new(MemoryServiceStore::default());
        let service = Arc::new(ProtocolService::new(
            Arc::clone(&store),
            Allow::when(ADMIT_ALL),
            CheckpointCodec::new([0x5e; 32], CHECKPOINT_TTL_MS)?,
        ));
        Self::over(service, store, seed, label)
    }

    /// A second tenant: a fresh scope with its own replicas, served by the
    /// same service and store as `other`.
    fn sharing(other: &Self, seed: u64, label: &str) -> Result<Self, Failure> {
        Self::over(
            Arc::clone(&other.service),
            Arc::clone(&other.store),
            seed,
            label,
        )
    }

    /// A fresh scope over `service` with [`REPLICAS`] empty replicas.
    fn over(
        service: Arc<Service>,
        store: Arc<MemoryServiceStore>,
        seed: u64,
        label: &str,
    ) -> Result<Self, Failure> {
        let mut world = Self {
            service,
            store,
            scope: Scope::new(AccountId::new(), SpaceId::new()),
            seed,
            label: label.to_owned(),
            replicas: Vec::with_capacity(REPLICAS.saturating_add(1)),
            documents: (0..DOCUMENTS).map(|_| DocumentId::new()).collect(),
            peer: PeerId::new(),
            draws: Draws::new(seed),
            trace: Trace::default(),
            faults: [0; 5],
            full_resyncs: 0,
            recoveries: 0,
            now_unix_ms: 1_000,
            model: BTreeMap::new(),
        };
        for _ in 0..REPLICAS {
            world.join()?;
        }
        Ok(world)
    }

    /// Opens one more empty replica in the scope.
    fn join(&mut self) -> Result<(), Failure> {
        let path = std::env::temp_dir().join(format!(
            "logical-db-sim-{}-{}-{}-{}.redb",
            self.label,
            self.seed,
            self.replicas.len(),
            std::process::id()
        ));
        let database = Database::open(&path, self.scope, ReplicaId::new())?;
        self.replicas.push(Replica { database, path });
        Ok(())
    }

    /// Closes every replica, then removes its file.
    fn remove(self) -> Result<(), Failure> {
        for replica in self.replicas {
            drop(replica.database);
            fs::remove_file(&replica.path)?;
        }
        Ok(())
    }

    /// Seals a baseline through a seeded position, marks it consumed and
    /// prunes the log below it, so a replica that joins later must recover
    /// from the baseline rather than from the log.
    async fn compact(&mut self) -> Result<(), Failure> {
        let frontier: Result<u64, MemoryStoreError> = self.store.frontier(self.scope).await;
        let frontier = frontier?;
        let span = usize::try_from(frontier)?;
        if span == 0 {
            return Err("compaction needs a non-empty log".into());
        }
        let through = u64::try_from(self.draws.below(span)?)?.saturating_add(1);
        let now = self.now_unix_ms;
        let request = OperationDigest::new(self.draws.bytes());
        let sealed = self
            .service
            .seal_snapshot(self.scope, through, request, now)
            .await?;
        if !matches!(sealed, SealResult::Sealed { .. }) {
            return Err(format!("seal through {through} of {frontier}: {sealed:?}").into());
        }
        let request = OperationDigest::new(self.draws.bytes());
        let consumed = self
            .service
            .mark_consumed(self.scope, through, request, now)
            .await?;
        if !matches!(consumed, ConsumedResult::Advanced { .. }) {
            return Err(format!("consume through {through}: {consumed:?}").into());
        }
        let request = OperationDigest::new(self.draws.bytes());
        let pruned = self
            .service
            .prune(self.scope, through, None, request, now)
            .await?;
        if !matches!(pruned, PruneResult::Pruned(_)) {
            return Err(format!("prune through {through}: {pruned:?}").into());
        }
        self.trace.record(b"compact").record(&through.to_le_bytes());
        Ok(())
    }

    /// Replica `replica` writes `value` under its own key in `document`.
    fn write(&mut self, replica: usize, document: usize, value: i64) -> Result<(), Failure> {
        let target = *self
            .documents
            .get(document)
            .ok_or("document out of range")?;
        let request = OperationDigest::new(self.draws.bytes());
        let database = &self
            .replicas
            .get(replica)
            .ok_or("replica out of range")?
            .database;
        database.edit(target, OperationId::new(), request, |editor| {
            editor.put(&Object::root(), &key(replica), Scalar::Integer(value))
        })?;
        self.model.insert((document, replica), value);
        self.trace
            .record(b"write")
            .record(&u64::try_from(replica)?.to_le_bytes())
            .record(&u64::try_from(document)?.to_le_bytes())
            .record(&value.to_le_bytes());
        Ok(())
    }

    /// One bounded sync cycle for `replica`. A simulated network loss is an
    /// expected outcome; any other failure is a defect.
    async fn sync(
        &mut self,
        replica: usize,
        faulty: bool,
    ) -> Result<(usize, usize, bool), Failure> {
        let database = &self
            .replicas
            .get(replica)
            .ok_or("replica out of range")?
            .database;
        if faulty && self.draws.next().is_multiple_of(8) {
            self.now_unix_ms = self
                .now_unix_ms
                .saturating_add(CHECKPOINT_TTL_MS.saturating_add(1));
            if let Some(jumps) = self.faults.get_mut(CLOCK_JUMPS) {
                *jumps = jumps.saturating_add(1);
            }
            self.trace.record(b"clock-jump");
        }
        let mut net = Net {
            service: self.service.as_ref(),
            draws: &mut self.draws,
            trace: &mut self.trace,
            faults: &mut self.faults,
            now_unix_ms: &mut self.now_unix_ms,
            faulty,
            lost: false,
        };
        let outcome = database
            .synchronize(self.peer, &mut net, SyncLimits::default(), || false)
            .await;
        // A cycle stops at its first transport failure, so a failed cycle whose
        // last exchange the network lost failed because of that loss.
        let lost = net.lost;
        match outcome {
            Ok(report) => {
                if report.full_resync {
                    self.full_resyncs = self.full_resyncs.saturating_add(1);
                }
                if report.recovered {
                    self.recoveries = self.recoveries.saturating_add(1);
                }
                self.trace
                    .record(b"synced")
                    .record(&[u8::from(report.full_resync)])
                    .record(&u64::try_from(report.pushed)?.to_le_bytes())
                    .record(&u64::try_from(report.pulled)?.to_le_bytes());
                Ok((report.pushed, report.pulled, report.more))
            }
            Err(loss) if faulty && lost => {
                self.trace
                    .record(b"lost")
                    .record(loss.to_string().as_bytes());
                Ok((0, 0, true))
            }
            Err(error) => Err(format!("replica {replica} sync failed: {error}").into()),
        }
    }

    /// Syncs every replica until a full round moves nothing.
    async fn heal(&mut self) -> Result<(), Failure> {
        for _round in 0..HEAL_ROUNDS {
            let mut moved = false;
            for replica in 0..self.replicas.len() {
                loop {
                    let (pushed, pulled, more) = self.sync(replica, false).await?;
                    moved |= pushed > 0 || pulled > 0;
                    if !more {
                        break;
                    }
                }
            }
            if !moved {
                return Ok(());
            }
        }
        Err(format!("replicas did not quiesce within {HEAL_ROUNDS} healed rounds").into())
    }

    /// Every replica holds exactly the model's value for every key.
    fn assert_converged(&self) -> Result<(), Failure> {
        for (index, replica) in self.replicas.iter().enumerate() {
            for (document, &id) in self.documents.iter().enumerate() {
                for writer in 0..REPLICAS {
                    let observed = replica
                        .database
                        .read(id, |reader| reader.get(&Object::root(), &key(writer)))?
                        .flatten();
                    let expected = self
                        .model
                        .get(&(document, writer))
                        .map(|&value| Item::Scalar(Scalar::Integer(value)));
                    if observed != expected {
                        return Err(format!(
                            "replica {index} document {document} key {} holds {observed:?}, model says {expected:?}",
                            key(writer)
                        )
                        .into());
                    }
                }
            }
        }
        Ok(())
    }

    /// Each document is created by its owner and distributed over a clean
    /// network, so the fault phase only ever edits documents every replica has.
    async fn setup(&mut self) -> Result<(), Failure> {
        for document in 0..DOCUMENTS {
            let owner = document
                .checked_rem(REPLICAS)
                .ok_or("the simulation needs at least one replica")?;
            self.write(owner, document, 0)?;
        }
        self.heal().await
    }

    /// One seeded fault-phase action: a faulty sync or a write.
    async fn step(&mut self, step: u64) -> Result<(), Failure> {
        let replica = self.draws.below(REPLICAS)?;
        if self.draws.below(3)? == 0 {
            self.sync(replica, true).await?;
        } else {
            let document = self.draws.below(DOCUMENTS)?;
            self.write(replica, document, i64::try_from(step)?.saturating_add(1))?;
        }
        Ok(())
    }

    /// No replica of this tenant holds any document of `other`.
    fn assert_isolated_from(&self, other: &Self) -> Result<(), Failure> {
        for (index, replica) in self.replicas.iter().enumerate() {
            for &foreign in &other.documents {
                let leaked = replica
                    .database
                    .read(foreign, |reader| reader.get(&Object::root(), "replica-0"))?;
                if leaked.is_some() {
                    return Err(format!("replica {index} holds another tenant's document").into());
                }
            }
        }
        Ok(())
    }
}

/// Runs one seed end to end and returns its trace.
async fn simulate(seed: u64, label: &str) -> Result<Run, Failure> {
    let mut world = World::new(seed, label)?;
    world.setup().await?;
    for step in 0..STEPS {
        world.step(step).await?;
    }
    world.heal().await?;
    world.assert_converged()?;
    world.compact().await?;
    world.join()?;
    for _attempt in 0..8 {
        world.sync(REPLICAS, true).await?;
    }
    world.heal().await?;
    world.assert_converged()?;
    for (&(document, writer), value) in &world.model {
        world
            .trace
            .record(&u64::try_from(document)?.to_le_bytes())
            .record(&u64::try_from(writer)?.to_le_bytes())
            .record(&value.to_le_bytes());
    }
    let run = Run {
        trace: world.trace.digest(),
        faults: world.faults,
        full_resyncs: world.full_resyncs,
        recoveries: world.recoveries,
    };
    world.remove()?;
    Ok(run)
}

/// Two tenants share one service and store; their faulty schedules
/// interleave. Each must converge to its own model and hold nothing of the
/// other's.
async fn simulate_tenants(seed: u64, label: &str) -> Result<(), Failure> {
    let mut first = World::new(seed, &format!("{label}-a"))?;
    let mut second = World::sharing(&first, seed ^ 0x7e7a_9e37, &format!("{label}-b"))?;
    first.setup().await?;
    second.setup().await?;
    for step in 0..STEPS {
        if first.draws.next().is_multiple_of(2) {
            first.step(step).await?;
        } else {
            second.step(step).await?;
        }
    }
    first.heal().await?;
    second.heal().await?;
    first.assert_converged()?;
    second.assert_converged()?;
    first.assert_isolated_from(&second)?;
    second.assert_isolated_from(&first)?;
    first.remove()?;
    second.remove()
}

/// The message a panicked lane carried, so the failure names its cause.
fn panic_message(payload: &(dyn Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        return format!("a simulation lane panicked: {message}");
    }
    if let Some(message) = payload.downcast_ref::<String>() {
        return format!("a simulation lane panicked: {message}");
    }
    "a simulation lane panicked with a non-text payload".to_owned()
}

/// Runs every seed under `label` and returns the runs in seed order.
fn run_seeds<T, Job, Fut>(seeds: &[u64], label: &str, job: &Job) -> Result<Vec<(u64, T)>, Failure>
where
    T: Send,
    Job: Fn(u64, String) -> Fut + Sync,
    Fut: Future<Output = Result<T, Failure>>,
{
    let lanes = std::thread::scope(|scope| {
        // Every lane is spawned before any is joined, so the lanes overlap.
        let mut handles = Vec::with_capacity(LANES);
        for lane in 0..LANES {
            handles.push(scope.spawn(move || -> Result<Vec<(u64, T)>, String> {
                let runtime = Builder::new()
                    .worker_threads(NonZeroUsize::new(1))
                    .build()
                    .map_err(|error| error.to_string())?;
                seeds
                    .iter()
                    .skip(lane)
                    .step_by(LANES)
                    .map(|&seed| {
                        runtime
                            .block_on(job(seed, label.to_owned()))
                            .map(|run| (seed, run))
                            .map_err(|error| format!("seed {seed}: {error}"))
                    })
                    .collect()
            }));
        }
        handles
            .into_iter()
            .map(|handle| match handle.join() {
                Ok(lane) => lane,
                Err(payload) => Err(panic_message(payload.as_ref())),
            })
            .collect::<Result<Vec<_>, String>>()
    })?;
    let mut runs = lanes.into_iter().flatten().collect::<Vec<_>>();
    runs.sort_by_key(|&(seed, _)| seed);
    Ok(runs)
}

/// Every fault class fires across the campaign and every seed converges.
#[test]
fn sim_sync_converges_under_seeded_network_faults() -> Result<(), Failure> {
    let seeds = (0..CAMPAIGN_SEEDS).collect::<Vec<_>>();
    let runs = run_seeds(&seeds, "campaign", &|seed, label| async move {
        simulate(seed, &label).await
    })?;
    assert_eq!(runs.len(), seeds.len(), "every seed must report");
    let mut faults: Faults = [0; 5];
    let mut full_resyncs = 0_u64;
    let mut recoveries = 0_u64;
    for entry in &runs {
        for (total, count) in faults.iter_mut().zip(entry.1.faults) {
            *total = total.saturating_add(count);
        }
        full_resyncs = full_resyncs.saturating_add(entry.1.full_resyncs);
        recoveries = recoveries.saturating_add(entry.1.recoveries);
    }
    let [
        _clean,
        lost_requests,
        lost_responses,
        duplicates,
        clock_jumps,
    ] = faults;
    assert!(
        lost_requests > 0 && lost_responses > 0 && duplicates > 0 && clock_jumps > 0,
        "the campaign must inject every fault class: {faults:?}"
    );
    assert!(
        full_resyncs > 0,
        "clock jumps must drive the expired-checkpoint full resync path"
    );
    assert!(
        recoveries > 0,
        "replicas joining after a prune must recover through the baseline"
    );
    Ok(())
}

/// The same seed replays the same trace; distinct seeds drive distinct ones.
#[test]
fn sim_sync_same_seed_replays_the_same_trace() -> Result<(), Failure> {
    let seeds = (0..16).collect::<Vec<_>>();
    let replay = |seed, label: String| async move { simulate(seed, &label).await };
    let first = run_seeds(&seeds, "replay-a", &replay)?;
    let second = run_seeds(&seeds, "replay-b", &replay)?;
    let mut traces = std::collections::BTreeSet::new();
    for (left, right) in first.iter().zip(&second) {
        let seed = left.0;
        assert_eq!(seed, right.0, "both replays must cover the same seeds");
        assert_eq!(
            left.1.trace, right.1.trace,
            "seed {seed} did not replay exactly"
        );
        assert_eq!(
            left.1.faults, right.1.faults,
            "seed {seed} injected different faults"
        );
        traces.insert(left.1.trace);
    }
    assert_eq!(
        traces.len(),
        seeds.len(),
        "distinct seeds must drive distinct schedules"
    );
    Ok(())
}

/// Two tenants over one service converge separately and never leak.
#[test]
fn sim_sync_tenants_sharing_a_service_stay_isolated() -> Result<(), Failure> {
    let seeds = (0..64).collect::<Vec<_>>();
    let runs = run_seeds(&seeds, "tenants", &|seed, label| async move {
        simulate_tenants(seed, &label).await
    })?;
    assert_eq!(runs.len(), seeds.len(), "every seed must report");
    Ok(())
}
