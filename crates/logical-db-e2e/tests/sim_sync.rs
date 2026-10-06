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
//! pull must resynchronize from scratch. Choices are drawn from a SplitMix64 stream over the seed, so a seed
//! replays exactly and its trace hash proves it. The replicas are real embedded
//! databases and the service is the real protocol service over the journey
//! store; only the wire between them is simulated. After the fault phase the
//! network heals and every replica must hold exactly the model's state: each
//! replica's last write to each document, nothing lost and nothing doubled.
//! The service then seals a baseline at a seeded position and prunes the log
//! below it, and a fresh replica joins under the same faults: it can only
//! recover through the baseline, and it must converge to the same model.

#![forbid(unsafe_code)]

use std::{
    collections::BTreeMap,
    fmt,
    hash::{DefaultHasher, Hasher},
    path::PathBuf,
    sync::Arc,
};

use logical_db::{
    AccountId, Database, DocumentId, Item, Object, OperationDigest, OperationId, PeerId, ReplicaId,
    Scalar, Scope, SpaceId, SyncLimits,
    transport::{
        BaselineRequest, BaselineResult, PullRequest, PullResult, PushRequest, PushResponse,
        Transport,
    },
};
use logical_db_e2e::support::MemoryServiceStore;
use logical_db_model::{ActionDigest, Effect};
use logical_db_service::{
    Authority, CheckpointCodec, ConsumedResult, LocalGrant, ProtocolService, PruneResult,
    SealResult, ServiceBaseline, ServicePull, ServiceStore,
};

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

type Service = ProtocolService<Arc<MemoryServiceStore>, Open>;

/// The simulation's authority: every justified request in the scope is
/// granted, so every refusal the run sees comes from the protocol itself.
struct Open;

impl Authority for Open {
    type Grant = LocalGrant;

    fn authorize(
        &self,
        scope: logical_db::Scope,
        effect: Effect,
        action: ActionDigest,
        frontier: u64,
        _now: u64,
    ) -> Option<LocalGrant> {
        Some(LocalGrant::new(scope, effect, action, frontier, u64::MAX))
    }
}

/// Order-sensitive digest of everything one run observed.
#[derive(Default)]
struct Trace(DefaultHasher);

impl Trace {
    fn record(&mut self, bytes: &[u8]) -> &mut Self {
        self.0.write(bytes);
        self
    }

    fn digest(&self) -> u64 {
        self.0.finish()
    }
}
type Failure = Box<dyn std::error::Error>;

/// Seeded choice stream (SplitMix64): std has no seeded generator and the
/// simulation must not draw from the OS.
struct Draws {
    state: u64,
}

impl Draws {
    const fn new(seed: u64) -> Self {
        Self {
            state: seed ^ 0x6c62_272e_07bb_0142,
        }
    }

    const fn next(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut mixed = self.state;
        mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        mixed ^ (mixed >> 31)
    }

    /// Thirty-two seeded bytes.
    fn bytes(&mut self) -> [u8; 32] {
        let mut bytes = [0; 32];
        for chunk in bytes.as_chunks_mut::<8>().0 {
            *chunk = self.next().to_le_bytes();
        }
        bytes
    }

    /// A choice in `0..bound`, `bound` being one of this file's small constants.
    fn below(&mut self, bound: usize) -> Result<usize, Failure> {
        let bound = u64::try_from(bound)?;
        let choice = self
            .next()
            .checked_rem(bound)
            .ok_or("a simulated choice needs a nonzero range")?;
        Ok(usize::try_from(choice)?)
    }
}

/// What the network does to one exchange.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Fault {
    /// Delivered once.
    Clean,
    /// Lost before the service saw it.
    LoseRequest,
    /// Committed by the service, answer lost on the way back.
    LoseResponse,
    /// Delivered twice; the client sees the second answer.
    Duplicate,
}

impl Fault {
    const fn tag(self) -> u8 {
        match self {
            Self::Clean => 0,
            Self::LoseRequest => 1,
            Self::LoseResponse => 2,
            Self::Duplicate => 3,
        }
    }
}

/// A transport failure the client must survive without acknowledging anything.
#[derive(Debug)]
enum NetError {
    /// The request never reached the service.
    LostRequest,
    /// The service answered; the answer was lost.
    LostResponse,
    /// The service refused the exchange.
    Service(String),
    /// The service returned a variant this simulation does not model.
    Unmodelled,
}

impl fmt::Display for NetError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::LostRequest => formatter.write_str("simulated request loss"),
            Self::LostResponse => formatter.write_str("simulated response loss"),
            Self::Service(ref error) => write!(formatter, "service refused: {error}"),
            Self::Unmodelled => formatter.write_str("unmodelled service answer"),
        }
    }
}

impl std::error::Error for NetError {}

/// The simulated wire between one replica and the shared service.
struct Net<'world> {
    service: &'world Service,
    draws: &'world mut Draws,
    trace: &'world mut Trace,
    faults: &'world mut [u64; 5],
    now_unix_ms: &'world mut u64,
    faulty: bool,
    /// Whether the last exchange was lost by the simulated network.
    lost: bool,
}

impl Net<'_> {
    fn exchange(&mut self, kind: &[u8]) -> Fault {
        let fault = if self.faulty {
            match self.draws.next() % 10 {
                0 => Fault::LoseRequest,
                1 => Fault::LoseResponse,
                2 => Fault::Duplicate,
                _ => Fault::Clean,
            }
        } else {
            Fault::Clean
        };
        if let Some(count) = self.faults.get_mut(usize::from(fault.tag())) {
            *count = count.saturating_add(1);
        }
        self.trace.record(kind).record(&[fault.tag()]);
        fault
    }

    /// Advances simulated time well inside the checkpoint lifetime.
    fn tick(&mut self) -> u64 {
        *self.now_unix_ms = self.now_unix_ms.saturating_add(7);
        *self.now_unix_ms
    }
}

/// Runs `call` against the service as `fault` dictates.
macro_rules! deliver {
    ($net:ident, $fault:ident, $call:expr) => {{
        $net.lost = $fault == Fault::LoseRequest || $fault == Fault::LoseResponse;
        if $fault == Fault::LoseRequest {
            return Err(NetError::LostRequest);
        }
        let mut answer = $call;
        if $fault == Fault::Duplicate {
            answer = $call;
        }
        if $fault == Fault::LoseResponse {
            return Err(NetError::LostResponse);
        }
        answer
    }};
}

impl Transport for Net<'_> {
    type Error = NetError;

    async fn push(&mut self, request: PushRequest) -> Result<PushResponse, NetError> {
        let fault = self.exchange(b"push");
        let response = deliver!(self, fault, {
            let now = self.tick();
            self.service
                .push(&request, now)
                .await
                .map_err(|error| NetError::Service(error.to_string()))?
        });
        Ok(response)
    }

    async fn pull(&mut self, request: PullRequest) -> Result<PullResult, NetError> {
        let fault = self.exchange(b"pull");
        let answer = deliver!(self, fault, {
            let now = self.tick();
            self.service
                .pull(&request, now)
                .await
                .map_err(|error| NetError::Service(error.to_string()))?
        });
        match answer {
            ServicePull::Page(page) => Ok(PullResult::Page(page)),
            ServicePull::CheckpointExpired => Ok(PullResult::CheckpointExpired),
            ServicePull::BaselineRequired { .. } => Ok(PullResult::BaselineRequired),
            _ => Err(NetError::Unmodelled),
        }
    }

    async fn baseline(&mut self, request: BaselineRequest) -> Result<BaselineResult, NetError> {
        let fault = self.exchange(b"baseline");
        let answer = deliver!(self, fault, {
            let now = self.tick();
            self.service
                .recover(&request, now)
                .await
                .map_err(|error| NetError::Service(error.to_string()))?
        });
        match answer {
            ServiceBaseline::Artifact(page) => Ok(BaselineResult::Artifact(*page)),
            ServiceBaseline::Unavailable => Ok(BaselineResult::Unavailable),
            _ => Err(NetError::Unmodelled),
        }
    }
}

/// A replica's database file, removed when the replica is dropped.
struct ScratchFile(PathBuf);

impl Drop for ScratchFile {
    fn drop(&mut self) {
        drop(std::fs::remove_file(&self.0));
    }
}

/// One embedded replica. The database drops before its file is removed.
struct Replica {
    database: Database,
    _file: ScratchFile,
}

/// Everything one seed's run owns.
struct World {
    /// Shared by every tenant world built over it with [`World::sharing`].
    service: Arc<Service>,
    /// The service's store, read for the frontier compaction seals through.
    store: Arc<MemoryServiceStore>,
    scope: Scope,
    seed: u64,
    label: String,
    replicas: Vec<Replica>,
    documents: Vec<DocumentId>,
    peer: PeerId,
    draws: Draws,
    trace: Trace,
    faults: [u64; 5],
    now_unix_ms: u64,
    /// Cycles that found their checkpoint expired and resynchronized fully.
    full_resyncs: u64,
    /// Cycles that installed a state-bearing baseline.
    recoveries: u64,
    /// Last value each replica wrote to each document: the convergence oracle.
    model: BTreeMap<(usize, usize), i64>,
}

/// Result of one seed: its trace hash and the faults it injected.
struct Run {
    trace: u64,
    faults: [u64; 5],
    full_resyncs: u64,
    recoveries: u64,
}

impl World {
    fn new(seed: u64, label: &str) -> Result<Self, Failure> {
        let store = Arc::new(MemoryServiceStore::default());
        let service = Arc::new(ProtocolService::new(
            Arc::clone(&store),
            Open,
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
        let file = ScratchFile(path.clone());
        self.replicas.push(Replica {
            database: Database::open(&path, self.scope, ReplicaId::new())?,
            _file: file,
        });
        Ok(())
    }

    /// Seals a baseline through a seeded position, marks it consumed and
    /// prunes the log below it, so a replica that joins later must recover
    /// from the baseline rather than from the log.
    async fn compact(&mut self) -> Result<(), Failure> {
        let frontier = self.store.frontier(self.scope).await?;
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
            Err(_) if faulty && lost => {
                self.trace.record(b"lost");
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
}

fn key(replica: usize) -> String {
    format!("replica-{replica}")
}

/// Runs one seed end to end and returns its trace.
impl World {
    /// Each document is created by its owner and distributed over a clean
    /// network, so the fault phase only ever edits documents every replica has.
    async fn setup(&mut self) -> Result<(), Failure> {
        for document in 0..DOCUMENTS {
            self.write(document % REPLICAS, document, 0)?;
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
    Ok(Run {
        trace: world.trace.digest(),
        faults: world.faults,
        full_resyncs: world.full_resyncs,
        recoveries: world.recoveries,
    })
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
    second.assert_isolated_from(&first)
}

/// Parallel lanes. Every write commits with immediate durability, so one run
/// is bound by fsync latency, not CPU; independent seeds run side by side,
/// each lane on its own single-worker runtime, without changing any seed's
/// schedule.
const LANES: usize = 32;

/// Runs every seed under `label` and returns the runs in seed order.
fn run_seeds<T, Job, Fut>(seeds: &[u64], label: &str, job: Job) -> Result<Vec<(u64, T)>, Failure>
where
    T: Send,
    Job: Fn(u64, String) -> Fut + Sync,
    Fut: Future<Output = Result<T, Failure>>,
{
    let job = &job;
    let lanes = std::thread::scope(|scope| {
        // Every lane is spawned before any is joined, so the lanes overlap.
        let mut handles = Vec::with_capacity(LANES);
        for lane in 0..LANES {
            handles.push(scope.spawn(move || -> Result<Vec<(u64, T)>, String> {
                let runtime = lgwks_bot::Builder::new()
                    .worker_threads(std::num::NonZeroUsize::new(1))
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
            .map(|handle| {
                handle
                    .join()
                    .map_err(|_| "a simulation lane panicked".to_owned())
                    .and_then(|lane| lane)
            })
            .collect::<Result<Vec<_>, String>>()
    })?;
    let mut runs = lanes.into_iter().flatten().collect::<Vec<_>>();
    runs.sort_by_key(|&(seed, _)| seed);
    Ok(runs)
}

#[test]
fn sim_sync_converges_under_seeded_network_faults() -> Result<(), Failure> {
    let seeds = (0..CAMPAIGN_SEEDS).collect::<Vec<_>>();
    let runs = run_seeds(&seeds, "campaign", |seed, label| async move {
        simulate(seed, &label).await
    })?;
    assert_eq!(runs.len(), seeds.len(), "every seed must report");
    let mut faults = [0_u64; 5];
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

#[test]
fn sim_sync_same_seed_replays_the_same_trace() -> Result<(), Failure> {
    let seeds = (0..16).collect::<Vec<_>>();
    let replay = |seed, label: String| async move { simulate(seed, &label).await };
    let first = run_seeds(&seeds, "replay-a", replay)?;
    let second = run_seeds(&seeds, "replay-b", replay)?;
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

#[test]
fn sim_sync_tenants_sharing_a_service_stay_isolated() -> Result<(), Failure> {
    let seeds = (0..64).collect::<Vec<_>>();
    let runs = run_seeds(&seeds, "tenants", |seed, label| async move {
        simulate_tenants(seed, &label).await
    })?;
    assert_eq!(runs.len(), seeds.len(), "every seed must report");
    Ok(())
}
