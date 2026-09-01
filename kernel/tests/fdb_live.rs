//! Live FDB proof — host cluster at /tmp/fdb-f0 or docker foundationdb.
//!
//! macOS: stage/start with `scripts/local-fdb-cluster.sh up`, then add its
//! `macbin` to the native link path and executable rpath through `RUSTFLAGS`.
//! The exact command is documented in the repository README.
//! Docker alt: docker run --network host -v /tmp/fdb-f0/fdb.cluster:/etc/foundationdb/fdb.cluster
//!           -v /tmp/fdb-f0/macbin/libfdb_c.so:/usr/lib/libfdb_c.so -v $PWD:/work -w /work
//!           rust:bookworm cargo test --features fdb --test fdb_live -- --ignored --nocapture

#![cfg(feature = "fdb")]
use braid_capability::Capability;
use braid_flow_plan::{CompletionMap, FlowSnapshot};
use braid_ir::{
    Braid, Capsule, ConfirmPolicy, EffectClass, Exposure, Strand, TermRegistry, TermSpec, TypeTag,
    Value, IR_VERSION,
};
use braid_run::{JustificationProof, RunnableInvocation};
use foundationdb::{KeySelector, RangeOption};
use logical_db::admitted::{
    AdmittedOutcome, AdmittedTxn, AuthorityProofRef, BraidExecutionBinding, Effect, HybridDb,
    OperationId, OutboardDb, RequestBinding, StateBinding, WriteBatch, DATABASE_VERSION_FACT,
};
use logical_db::tuple::{AccountId, ContainerId, Element, KeyFamily, Scope, Tuple, ZoneId};
use logical_db::{DatabaseOptions, KernelError};
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::sync::Arc;

fn admitted(
    version: u64,
    scope: Scope,
    effect: Effect,
    operation: OperationId,
    binding: RequestBinding,
) -> AdmittedTxn {
    let capability = Capability::new(match effect {
        Effect::Put => "logical-db.record.write",
        Effect::Delete => "logical-db.record.delete",
        Effect::Get => "logical-db.record.read",
        Effect::Scan => "logical-db.record.scan",
    });
    let mut registry = TermRegistry::new(1);
    registry
        .insert(TermSpec {
            id: capability.as_str().into(),
            inputs: vec![],
            output: TypeTag::Bool,
            capability: Some(capability.clone()),
            effect: match effect {
                Effect::Put | Effect::Delete => EffectClass::ReversibleWrite,
                Effect::Get | Effect::Scan => EffectClass::Read,
            },
            source_exposure: Exposure::Public,
            egress_ceiling: None,
            cost: 1,
        })
        .unwrap();
    let capsule = Capsule {
        ir_version: IR_VERSION,
        vocab_version: registry.vocab_version,
        registry_cid: registry.cid(),
        intent: "exercise the native logical-db boundary".into(),
        grants: vec![capability.clone()],
        braid: Braid {
            strands: vec![Strand {
                term: capability.as_str().into(),
                inputs: vec![],
            }],
            outputs: vec![0],
        },
        budget: 1,
        confirm: ConfirmPolicy::None,
        evidence: vec![],
    };
    let execution = BraidExecutionBinding::inspect(&capsule, &registry).unwrap();
    let (request_name, request_value) = binding.snapshot_fact();
    let snapshot = FlowSnapshot::new(BTreeMap::from([
        (
            DATABASE_VERSION_FACT.into(),
            Value::Int(i64::try_from(version).unwrap()),
        ),
        (request_name.into(), request_value),
    ]));
    let predicate = binding.predicate().unwrap();
    let justification =
        JustificationProof::prove(&capsule, &predicate, &snapshot, &CompletionMap::new()).unwrap();
    let ambient = vec![capability];
    let admission = braid_verify::admit(&capsule.to_bytes(), &registry, &ambient).unwrap();
    let runnable = RunnableInvocation::prepare(
        capsule,
        &registry,
        admission,
        justification,
        &ambient,
        &snapshot,
    )
    .unwrap();
    AdmittedTxn::new(
        runnable,
        StateBinding::new(version, &snapshot).unwrap(),
        execution,
        AuthorityProofRef::new("live-fdb-authority", vec![scope.clone()], vec![effect]).unwrap(),
        scope,
        vec![effect],
        operation,
    )
    .unwrap()
}

fn prefix_end(mut prefix: Vec<u8>) -> Vec<u8> {
    let position = prefix
        .iter()
        .rposition(|byte| *byte != u8::MAX)
        .expect("logical scope prefix is never all 0xff");
    prefix[position] += 1;
    prefix.truncate(position + 1);
    prefix
}

#[tokio::test]
#[ignore]
async fn live_fdb_cluster_roundtrip() {
    let cluster =
        std::env::var("FDB_CLUSTER_FILE").unwrap_or_else(|_| "/tmp/fdb-f0/fdb.cluster".into());
    assert!(
        std::path::Path::new(&cluster).exists(),
        "live FDB proof requires a cluster at {cluster}; run scripts/local-fdb-cluster.sh up"
    );
    // SAFETY: boot spawns network thread; fdb_select_api_version once per process.
    let _net = unsafe { foundationdb::boot() };
    let db = foundationdb::Database::new(Some(cluster.as_str())).expect("open database");

    let key = b"logical-db:live:probe:contains-many";
    let val = b"ok-7.3.79-arm64-host-and-docker";

    // single-key roundtrip — proves ordered transactional KV
    db.run(|tr, _| {
        let k = key.to_vec();
        let v = val.to_vec();
        async move {
            tr.set_option(foundationdb::options::TransactionOption::Timeout(5_000))?;
            tr.set(&k, &v);
            Ok(())
        }
    })
    .await
    .expect("set");

    let got = db
        .run(|tr, _| async move {
            tr.set_option(foundationdb::options::TransactionOption::Timeout(5_000))?;
            Ok(tr.get(key, false).await?)
        })
        .await
        .expect("get");
    assert_eq!(got.as_deref(), Some(val.as_slice()));

    // range — proves ordered scan is the substrate (used by contains_many)
    let range = db
        .run(|tr, _| async move {
            tr.set_option(foundationdb::options::TransactionOption::Timeout(5_000))?;
            let begin =
                KeySelector::first_greater_or_equal(Cow::Borrowed(b"logical-db:live:" as &[u8]));
            let end = KeySelector::first_greater_or_equal(Cow::Borrowed(
                b"logical-db:live:\xff" as &[u8],
            ));
            let opt = RangeOption::from((begin, end));
            let r = tr.get_range(&opt, 10, false).await?;
            Ok(r.iter()
                .map(|kv| kv.key().to_vec())
                .collect::<Vec<Vec<u8>>>())
        })
        .await
        .expect("range");
    assert!(range.iter().any(|k| k == key));

    // clean up
    db.run(|tr, _| async move {
        tr.set_option(foundationdb::options::TransactionOption::Timeout(5_000))?;
        tr.clear(key);
        Ok(())
    })
    .await
    .expect("clear");

    // The product journey: the same Braid-bound, scoped API used by the
    // embedded profile executes against the official FoundationDB client.
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        .to_be_bytes();
    let scope = Scope::new(
        AccountId::new([b"live-account-".as_slice(), &unique].concat()).unwrap(),
        ContainerId::new(b"notes".to_vec()).unwrap(),
        ZoneId::new(b"private".to_vec()).unwrap(),
    );
    let scope_prefix = scope.zone_prefix();
    let options = DatabaseOptions {
        cluster_file: Some(cluster.clone()),
        ..DatabaseOptions::default()
    };
    let outboard = OutboardDb::from_database(db, options).expect("attach logical layer");
    let db = Arc::new(HybridDb::outboard(outboard));
    let object = Tuple::new(vec![Element::Bytes(b"object-1".to_vec())]);

    let write_version = db.read_version().await.expect("read version");
    let write_operation = OperationId::new(b"live-write-1".to_vec()).unwrap();
    let mut batch = WriteBatch::new(scope.clone(), vec![Effect::Put]).unwrap();
    batch
        .set(KeyFamily::Record, &object, b"hybrid-value".to_vec())
        .unwrap();
    let write = admitted(
        write_version,
        scope.clone(),
        Effect::Put,
        write_operation.clone(),
        batch.request_binding(&write_operation),
    );
    assert!(matches!(
        db.commit_admitted(write, batch).await.unwrap(),
        AdmittedOutcome::Applied { .. }
    ));

    let read_version = db.read_version().await.expect("post-write version");
    let read_operation = OperationId::new(b"live-read-1".to_vec()).unwrap();
    let read_binding = RequestBinding::for_get(
        &scope,
        &[Effect::Get],
        &read_operation,
        KeyFamily::Record,
        &object,
    )
    .unwrap();
    let read = admitted(
        read_version,
        scope.clone(),
        Effect::Get,
        read_operation,
        read_binding,
    );
    assert_eq!(
        db.get_admitted(read, KeyFamily::Record, &object)
            .await
            .unwrap(),
        Some(b"hybrid-value".to_vec())
    );

    let scan_version = db.read_version().await.expect("change version");
    let scan_operation = OperationId::new(b"live-scan-1".to_vec()).unwrap();
    let scan_binding =
        RequestBinding::for_changes(&scope, &[Effect::Scan], &scan_operation, None, 10).unwrap();
    let scan = admitted(
        scan_version,
        scope.clone(),
        Effect::Scan,
        scan_operation,
        scan_binding,
    );
    let changes = db.changes_admitted(scan, None, 10).await.unwrap();
    assert_eq!(changes.events.len(), 1);
    assert_eq!(changes.events[0].operation_id.as_bytes(), b"live-write-1");

    // Two independently admitted writes prepared against the same zone state
    // race at the real FDB boundary. The zone frontier permits exactly one;
    // the loser must re-plan against fresh state.
    let concurrent_version = db.read_version().await.expect("concurrent version");
    let gate = Arc::new(tokio::sync::Barrier::new(2));
    let mut tasks = Vec::new();
    for index in 0_u8..2 {
        let concurrent_object = Tuple::new(vec![Element::Bytes(vec![b'c', index])]);
        let operation = OperationId::new(vec![b'c', b'o', index]).unwrap();
        let mut concurrent_batch = WriteBatch::new(scope.clone(), vec![Effect::Put]).unwrap();
        concurrent_batch
            .set(KeyFamily::Record, &concurrent_object, vec![b'v', index])
            .unwrap();
        let envelope = admitted(
            concurrent_version,
            scope.clone(),
            Effect::Put,
            operation.clone(),
            concurrent_batch.request_binding(&operation),
        );
        let task_db = Arc::clone(&db);
        let task_gate = Arc::clone(&gate);
        tasks.push(tokio::spawn(async move {
            task_gate.wait().await;
            task_db.commit_admitted(envelope, concurrent_batch).await
        }));
    }
    let mut applied = 0;
    let mut stale = 0;
    for task in tasks {
        match task.await.expect("concurrent writer task") {
            Ok(AdmittedOutcome::Applied { .. }) => applied += 1,
            Err(KernelError::StaleJustification { .. }) => stale += 1,
            outcome => panic!("unexpected concurrent writer outcome: {outcome:?}"),
        }
    }
    assert_eq!((applied, stale), (1, 1));
    drop(db);

    let cleanup = foundationdb::Database::new(Some(cluster.as_str())).expect("reopen for cleanup");
    let scope_end = prefix_end(scope_prefix.clone());
    cleanup
        .run(move |tr, _| {
            let begin = scope_prefix.clone();
            let end = scope_end.clone();
            async move {
                tr.set_option(foundationdb::options::TransactionOption::Timeout(5_000))?;
                tr.clear_range(&begin, &end);
                Ok(())
            }
        })
        .await
        .expect("clear logical scope");

    eprintln!(
        "LIVE FDB + HYBRID API OK cluster={cluster} key={:?}",
        String::from_utf8_lossy(key)
    );
}

#[test]
fn fdb_api_version_is_pinned_730() {
    // proves INV-FDB-PIN: binding compiled against 7.3, max api >= 730
    let max = foundationdb::api::get_max_api_version();
    assert!(max >= 730, "max api {max} must be >= 730 for fdb-7_3");
}
