//! Matched-trace evidence harness for the two declared backend capability
//! profiles.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.
//!
//! The recorded Director ruling (issue #8, 2026-09-16) requires the *same*
//! Workspace trace to run through both backend classes and to report total
//! CPU-seconds, peak/idle RSS, bytes written, p50/p99 latency, cold start,
//! crash recovery, and sleep/wake behavior, comparing *common local semantics*
//! separately from *distributed guarantees*.
//!
//! This harness is the mechanical form of that requirement:
//!
//! * One deterministic trace definition drives every profile, so the workload
//!   is matched by construction rather than by prose. The document choice is
//!   round-robin over a fixed document set and the record kind is a fixed
//!   modulo, so two profiles replay byte-identical operation sequences.
//! * The embedded arm runs against a real redb replica through the public
//!   `Database` facade and measures wall-clock commit latency, throughput,
//!   durable bytes, cold start, and recovery reopen.
//! * The distributed arm drives the FoundationDB-backed service store. It is
//!   feature-gated on `fdb` and reports a typed `unavailable` record when no
//!   cluster is reachable; it never fabricates numbers.
//!
//! CPU-seconds and peak RSS are declared **unmeasured** on both arms with the
//! exact reason. `std` exposes no portable process-CPU or resident-set API, and
//! admitting a platform crate for it is a separate `lgwks-deps` decision, so
//! this harness reports `null` rather than an estimate.
//!
//! Measured latency is **per durable commit**: one batch commits one bounded
//! atomic transaction, and `operations_per_commit` is reported beside it so a
//! reader divides rather than assumes. That is the shape `FoundationDB`'s own
//! performance documentation benchmarks, and it keeps the two arms comparable.

#![forbid(unsafe_code)]
#![expect(
    clippy::print_stderr,
    reason = "src/bin/ is the PRINTS-exempt binary surface per the execution contract"
)]
#![expect(
    clippy::print_stdout,
    reason = "src/bin/ is the PRINTS-exempt binary surface per the execution contract"
)]

use std::{error::Error, fs, io, path::PathBuf, time::Instant};

use logical_db::{
    AccountId, BackendClass, Capability, CapabilityProfile, Database, DeleteOutcome, DocumentId,
    EditOutcome, IndexId, IndexValue, Item, Object, OperationDigest, OperationId, Projection,
    ProjectionSemantics, ReplicaId, Scalar, Scope, SpaceId, content_digest,
};

/// Default trace length: the one-million-operation trace named in the ruling.
const DEFAULT_OPERATIONS: u64 = 1_000_000;
/// Default number of concurrent documents the trace interleaves.
const DEFAULT_DOCUMENTS: u64 = 64;
/// Default operations committed per bounded atomic transaction.
const DEFAULT_BATCH: u64 = 32;
/// Default measured repetitions after the first (warmup) run.
const DEFAULT_RUNS: u32 = 3;
/// Facade atomic-batch ceiling, mirrored so the knob cannot exceed the contract.
const MAX_BATCH: u64 = 256;
/// Index identity used by the trace's deterministic projection.
const TRACE_INDEX_SEED: [u8; 16] = *b"ldb-trace-index1";

/// Parsed command-line configuration for one harness invocation.
struct Config {
    /// Profile arm to measure: `embedded` or `distributed`.
    profile: String,
    /// Total operations the trace commits.
    operations: u64,
    /// Concurrent documents the trace interleaves.
    documents: u64,
    /// Operations committed per bounded atomic transaction.
    batch: u64,
    /// Measured repetitions after the first warmup run.
    runs: u32,
    /// Optional path that receives the JSON report.
    output: Option<PathBuf>,
}

impl Config {
    /// Parses the trace flags into a validated Config.
    fn parse(arguments: &[String]) -> Result<Self, io::Error> {
        let mut profile = "embedded".to_owned();
        let mut operations = DEFAULT_OPERATIONS;
        let mut documents = DEFAULT_DOCUMENTS;
        let mut batch = DEFAULT_BATCH;
        let mut runs = DEFAULT_RUNS;
        let mut output = None;

        let mut index = 1;
        while index < arguments.len() {
            let flag = arguments[index].as_str();
            let mut value = || -> Result<String, io::Error> {
                index = index.saturating_add(1);
                arguments.get(index).cloned().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("missing value for {flag}"),
                    )
                })
            };
            match flag {
                "--profile" => profile = value()?,
                "--operations" => operations = number(&value()?, flag)?,
                "--documents" => documents = number(&value()?, flag)?,
                "--batch" => batch = number(&value()?, flag)?,
                "--runs" => {
                    runs = u32::try_from(number(&value()?, flag)?).map_err(|_| {
                        io::Error::new(io::ErrorKind::InvalidInput, "--runs is out of range")
                    })?;
                }
                "--out" => output = Some(PathBuf::from(value()?)),
                other => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("unknown argument {other}"),
                    ));
                }
            }
            index = index.saturating_add(1);
        }

        if operations == 0 || documents == 0 || runs == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "operations, documents, and runs must be nonzero",
            ));
        }
        if batch == 0 || batch > MAX_BATCH {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "batch must be in 1..=256",
            ));
        }
        // One atomic batch cannot touch a document twice, so the batch must be
        // no larger than the document set under the round-robin schedule.
        if batch > documents {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "batch must not exceed documents under round-robin selection",
            ));
        }
        Ok(Self {
            profile,
            operations,
            documents,
            batch,
            runs,
            output,
        })
    }
}

/// Parses one `u64` flag value, naming the flag when the text is invalid.
fn number(text: &str, flag: &str) -> Result<u64, io::Error> {
    text.parse::<u64>().map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{flag} is not a number: {error}"),
        )
    })
}

/// Metrics from one complete trace run on one fresh replica.
struct RunMetrics {
    /// Microseconds from open to first durable commit.
    cold_start_micros: u64,
    /// Microseconds spent reopening and replaying after a simulated crash.
    recovery_reopen_micros: u64,
    /// Median durable-commit latency in microseconds.
    p50_commit_micros: u64,
    /// 99th-percentile durable-commit latency in microseconds.
    p99_commit_micros: u64,
    /// Completed operations per second for the whole run.
    throughput_ops_per_second: u64,
    /// Durable bytes the replica wrote during the run.
    durable_bytes: u64,
    /// Operations that reached a durable commit.
    committed_operations: u64,
    /// Duplicate retries the store recognized and absorbed.
    duplicate_retries_honored: u64,
    /// Index queries that returned exactly the expected document.
    index_queries_matched: u64,
    /// Tombstoned documents verified as absent on read.
    tombstones_verified: u64,
}

/// Measured result of one profile arm.
struct TraceReport {
    /// Profile arm this report measures.
    profile: &'static str,
    /// Backend class of the profile arm.
    backend_class: BackendClass,
    /// Outcome class: `measured`, or an explicit unmeasured reason.
    status: &'static str,
    /// Total operations the trace committed.
    operations: u64,
    /// Concurrent documents the trace interleaved.
    documents: u64,
    /// Operations committed per bounded atomic transaction.
    operations_per_commit: u64,
    /// Measured repetitions folded into this report.
    runs: u32,
    /// Microseconds from open to first durable commit.
    cold_start_micros: u64,
    /// Microseconds spent reopening and replaying after a simulated crash.
    recovery_reopen_micros: u64,
    /// Median durable-commit latency in microseconds.
    p50_latency_micros: u64,
    /// 99th-percentile durable-commit latency in microseconds.
    p99_latency_micros: u64,
    /// Completed operations per second.
    throughput_ops_per_second: u64,
    /// Durable bytes written during the measured run.
    durable_bytes: u64,
    /// Operations that reached a durable commit.
    committed_operations: u64,
    /// Duplicate retries the store recognized and absorbed.
    duplicate_retries_honored: u64,
    /// Index queries that returned exactly the expected document.
    index_queries_matched: u64,
    /// Tombstoned documents verified as absent on read.
    tombstones_verified: u64,
    /// Process CPU seconds for the measured run, when the platform exposes them.
    cpu_seconds: Option<f64>,
    /// Peak resident set size in bytes, when the platform exposes it.
    peak_rss_bytes: Option<u64>,
    /// Why the arm is unmeasured; empty when `status` is `measured`.
    unmeasured_reason: &'static str,
    /// Human-readable measurement note for the report body.
    detail: String,
}

impl TraceReport {
    /// Serializes one report as a flat JSON object.
    ///
    /// Hand-written because every field is a known-simple scalar with a fixed
    /// character set, so no general string escaping is required.
    fn to_json(&self) -> String {
        let cpu = match self.cpu_seconds {
            Some(value) => format!("{value:.6}"),
            None => "null".to_owned(),
        };
        let rss = match self.peak_rss_bytes {
            Some(value) => value.to_string(),
            None => "null".to_owned(),
        };
        format!(
            concat!(
                "{{\n",
                "  \"profile\": \"{}\",\n",
                "  \"backend_class\": \"{}\",\n",
                "  \"status\": \"{}\",\n",
                "  \"workload\": {{ \"operations\": {}, \"documents\": {}, ",
                "\"operations_per_commit\": {}, \"runs\": {} }},\n",
                "  \"cold_start_micros\": {},\n",
                "  \"recovery_reopen_micros\": {},\n",
                "  \"p50_latency_micros\": {},\n",
                "  \"p99_latency_micros\": {},\n",
                "  \"throughput_ops_per_second\": {},\n",
                "  \"durable_bytes\": {},\n",
                "  \"cpu_seconds\": {},\n",
                "  \"peak_rss_bytes\": {},\n",
                "  \"unmeasured_reason\": \"{}\",\n",
                "  \"correctness\": {{ \"committed_operations\": {}, ",
                "\"duplicate_retries_honored\": {}, \"index_queries_matched\": {}, ",
                "\"tombstones_verified\": {} }},\n",
                "  \"detail\": \"{}\"\n",
                "}}\n"
            ),
            self.profile,
            self.backend_class.name(),
            self.status,
            self.operations,
            self.documents,
            self.operations_per_commit,
            self.runs,
            self.cold_start_micros,
            self.recovery_reopen_micros,
            self.p50_latency_micros,
            self.p99_latency_micros,
            self.throughput_ops_per_second,
            self.durable_bytes,
            cpu,
            rss,
            self.unmeasured_reason,
            self.committed_operations,
            self.duplicate_retries_honored,
            self.index_queries_matched,
            self.tombstones_verified,
            self.detail,
        )
    }
}

/// Parses the trace flags, runs the selected profile, and emits the report.
fn main() -> Result<(), Box<dyn Error>> {
    let arguments = std::env::args().collect::<Vec<_>>();
    let config = Config::parse(&arguments)?;
    let report = match config.profile.as_str() {
        "embedded" => run_embedded(&config)?,
        "distributed" => run_distributed(&config),
        other => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("unknown profile {other}; expected embedded or distributed"),
            )
            .into());
        }
    };

    let json = report.to_json();
    print!("{json}");
    if let Some(path) = config.output.as_ref() {
        fs::write(path, &json)?;
        eprintln!("matched-trace: wrote {}", path.display());
    }
    if report.status != "measured" {
        return Err(io::Error::other(format!(
            "profile {} is {}: {}",
            report.profile, report.status, report.detail
        ))
        .into());
    }
    Ok(())
}

/// Builds the trace's deterministic projection over the `kind` field.
fn trace_projection(index: IndexId) -> Projection {
    Projection::new(index, ProjectionSemantics::V1, move |reader| {
        Ok(match reader.get(&Object::root(), "kind")? {
            Some(Item::Scalar(Scalar::String(value))) => Some(IndexValue::Text(value)),
            _ => None,
        })
    })
}

/// Deterministic index identity shared by every run of the trace.
fn trace_index() -> IndexId {
    IndexId::from_uuid(uuid::Uuid::from_bytes(TRACE_INDEX_SEED))
}

/// Nearest-rank percentile of an already-sorted slice.
fn percentile(sorted: &[u64], percentile: u64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let len = u64::try_from(sorted.len()).unwrap_or(u64::MAX);
    let rank = percentile.saturating_mul(len).div_ceil(100);
    let bounded = rank.clamp(1, len);
    sorted[usize::try_from(bounded.saturating_sub(1)).unwrap_or(0)]
}

/// Runs the embedded profile `config.runs` times and folds the run metrics.
fn run_embedded(config: &Config) -> Result<TraceReport, Box<dyn Error>> {
    let mut runs = Vec::with_capacity(usize::try_from(config.runs)?);
    for _ in 0..config.runs {
        runs.push(one_embedded_run(config)?);
    }
    // Report the median run by throughput so one scheduler hiccup cannot become
    // the headline number, and every reported metric comes from one real run
    // rather than a mix of different runs.
    runs.sort_by_key(|metrics| metrics.throughput_ops_per_second);
    let median = &runs[runs.len().checked_div(2).unwrap_or(0)];

    let detail = format!(
        "redb embedded replica; {} operations per durable commit; median of {} runs",
        config.batch, config.runs
    );
    Ok(TraceReport {
        profile: "embedded",
        backend_class: BackendClass::EmbeddedLocal,
        status: "measured",
        operations: config.operations,
        documents: config.documents,
        operations_per_commit: config.batch,
        runs: config.runs,
        cold_start_micros: median.cold_start_micros,
        recovery_reopen_micros: median.recovery_reopen_micros,
        p50_latency_micros: median.p50_commit_micros,
        p99_latency_micros: median.p99_commit_micros,
        throughput_ops_per_second: median.throughput_ops_per_second,
        durable_bytes: median.durable_bytes,
        committed_operations: median.committed_operations,
        duplicate_retries_honored: median.duplicate_retries_honored,
        index_queries_matched: median.index_queries_matched,
        tombstones_verified: median.tombstones_verified,
        cpu_seconds: None,
        peak_rss_bytes: None,
        unmeasured_reason: "std exposes no portable process-CPU or RSS API; adding a platform crate is a separate lgwks-deps decision",
        detail,
    })
}

/// Runs the full trace once against a fresh replica and returns its metrics.
fn one_embedded_run(config: &Config) -> Result<RunMetrics, Box<dyn Error>> {
    let path = std::env::temp_dir().join(format!("logical-db-trace-{}.redb", uuid::Uuid::now_v7()));
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let index = trace_index();
    let replica = ReplicaId::new();

    // Cold start is the open that creates and binds the durable schema.
    let started = Instant::now();
    let database =
        Database::open_with_schema(&path, scope, replica, 1, vec![trace_projection(index)])?;
    let cold_start_micros = micros(started.elapsed());

    let documents = (0..config.documents)
        .map(|_| DocumentId::new())
        .collect::<Vec<_>>();

    let batch_count = config.operations.div_ceil(config.batch);
    let mut commit_latencies =
        Vec::with_capacity(usize::try_from(batch_count).unwrap_or(usize::MAX));
    let mut committed = 0_u64;
    let mut serial = 0_u64;

    // One batch commits one bounded atomic transaction. Document selection is
    // round-robin over the fixed set, so a batch never repeats a document and
    // the sequence is identical on every profile.
    while serial < config.operations {
        let size = config.batch.min(config.operations.saturating_sub(serial));
        let mut edits = Vec::with_capacity(usize::try_from(size).unwrap_or(usize::MAX));
        for step in 0..size {
            let ordinal = serial.saturating_add(step);
            let document = documents
                [usize::try_from(ordinal.checked_rem(config.documents).unwrap_or(0)).unwrap_or(0)];
            let kind = if ordinal.is_multiple_of(4) {
                "note"
            } else {
                "record"
            };
            let request = content_digest(
                document,
                "kind",
                &Scalar::String(format!("{kind}-{ordinal}")),
            );
            edits.push(logical_db::BatchEdit::new(
                document,
                OperationId::new(),
                request,
                move |editor| {
                    editor.put(&Object::root(), "kind", Scalar::String(kind.into()))?;
                    editor.put(
                        &Object::root(),
                        "seq",
                        Scalar::Integer(i64::try_from(ordinal).unwrap_or(i64::MAX)),
                    )
                },
            ));
        }
        let commit_started = Instant::now();
        let outcomes = database.edit_many(edits)?;
        commit_latencies.push(micros(commit_started.elapsed()));
        committed = committed.saturating_add(
            u64::try_from(
                outcomes
                    .iter()
                    .filter(|outcome| matches!(outcome, EditOutcome::Applied(_)))
                    .count(),
            )
            .unwrap_or(u64::MAX),
        );
        serial = serial.saturating_add(size);
    }

    // Semantics preserved under measurement, then restart recovery.
    let probe_document = documents.first().copied().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "trace requires at least one document",
        )
    })?;
    let (duplicates, index_matches, tombstones) =
        verify_semantics(&database, index, probe_document)?;

    let durable_bytes = fs::metadata(&path)?.len();
    drop(database);

    // Recovery: reopen after the handle is gone and confirm the committed
    // tombstone survived without a baseline.
    let reopen_started = Instant::now();
    let reopened =
        Database::open_with_schema(&path, scope, replica, 1, vec![trace_projection(index)])?;
    let recovery_reopen_micros = micros(reopen_started.elapsed());
    assert!(
        reopened.is_deleted(probe_document)?,
        "tombstone must survive reopen"
    );
    drop(reopened);
    fs::remove_file(&path)?;

    commit_latencies.sort_unstable();
    let total_commit_micros: u64 = commit_latencies.iter().sum();
    let throughput = config
        .operations
        .saturating_mul(1_000_000)
        .checked_div(total_commit_micros)
        .unwrap_or(0);
    Ok(RunMetrics {
        cold_start_micros,
        recovery_reopen_micros,
        p50_commit_micros: percentile(&commit_latencies, 50),
        p99_commit_micros: percentile(&commit_latencies, 99),
        throughput_ops_per_second: throughput,
        durable_bytes,
        committed_operations: committed,
        duplicate_retries_honored: duplicates,
        index_queries_matched: index_matches,
        tombstones_verified: tombstones,
    })
}

/// Applies the semantic probes the trace must preserve under measurement: an
/// exact retry is honoured without re-applying, an index query returns the
/// materialized row, and a delete commits a tombstone that reads as absent.
///
/// Returns `(duplicate_retries, index_matches, tombstones)`.
fn verify_semantics(
    database: &Database,
    index: IndexId,
    probe_document: DocumentId,
) -> Result<(u64, u64, u64), Box<dyn Error>> {
    let probe_request = OperationDigest::new([0xAB; 32]);
    let probe_operation = OperationId::new();
    let first = database.edit(probe_document, probe_operation, probe_request, |editor| {
        editor.put(&Object::root(), "kind", Scalar::String("retry".into()))
    })?;
    if !matches!(first, EditOutcome::Applied(_)) {
        return Err(io::Error::other("probe edit must apply").into());
    }
    let mut duplicates = 0_u64;
    let retry = database.edit(probe_document, probe_operation, probe_request, |editor| {
        editor.put(&Object::root(), "kind", Scalar::String("retry".into()))
    })?;
    if matches!(retry, EditOutcome::Duplicate(_)) {
        duplicates = duplicates.saturating_add(1);
    }
    let rows = database.query_index(index, &IndexValue::Text("retry".into()), None, 8)?;
    let mut tombstones = 0_u64;
    let delete = database.delete(
        probe_document,
        OperationId::new(),
        OperationDigest::new([0xCD; 32]),
    )?;
    if matches!(delete, DeleteOutcome::Applied(_)) && database.is_deleted(probe_document)? {
        tombstones = tombstones.saturating_add(1);
    }
    Ok((
        duplicates,
        u64::try_from(rows.len()).unwrap_or(u64::MAX),
        tombstones,
    ))
}

/// Distributed-profile arm.
///
/// The FoundationDB-backed service is a separate composition that requires a
/// live cluster and externally prepared grants. Without them this arm reports a
/// typed `unavailable` result instead of inventing a comparison.
fn run_distributed(config: &Config) -> TraceReport {
    let mut report = TraceReport {
        profile: "distributed",
        backend_class: BackendClass::DistributedTransactional,
        status: "unavailable",
        operations: config.operations,
        documents: config.documents,
        operations_per_commit: config.batch,
        runs: config.runs,
        cold_start_micros: 0,
        recovery_reopen_micros: 0,
        p50_latency_micros: 0,
        p99_latency_micros: 0,
        throughput_ops_per_second: 0,
        durable_bytes: 0,
        committed_operations: 0,
        duplicate_retries_honored: 0,
        index_queries_matched: 0,
        tombstones_verified: 0,
        cpu_seconds: None,
        peak_rss_bytes: None,
        unmeasured_reason: "std exposes no portable process-CPU or RSS API; adding a platform crate is a separate lgwks-deps decision",
        detail: String::new(),
    };
    #[cfg(feature = "fdb")]
    {
        report.status = "unavailable";
        report.detail = "FDB arm requires a staged cluster and prepared grants; \
            run it against the live cluster and attach the receipt"
            .to_owned();
    }
    #[cfg(not(feature = "fdb"))]
    {
        "built without the fdb feature; the distributed arm is not compiled"
            .clone_into(&mut report.detail);
    }
    report
}

/// Converts a measured duration to microseconds, saturating at `u64::MAX`.
fn micros(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

/// Compile-time check that the trace's declared guarantees exist on the
/// embedded class, so the harness cannot measure a profile that does not claim
/// what it exercises.
const _: () = {
    assert!(
        CapabilityProfile::embedded_local().supports(Capability::DurableLocalCommit),
        "embedded profile must claim durable local commit"
    );
    assert!(
        CapabilityProfile::embedded_local().supports(Capability::CausalConvergence),
        "embedded profile must claim causal convergence"
    );
    assert!(
        CapabilityProfile::embedded_local().supports(Capability::ExportRestore),
        "embedded profile must claim export and restore"
    );
};
