//! Live FDB proof — host cluster at /tmp/fdb-f0 or docker foundationdb.
//!
//! Run: DYLD_FALLBACK_LIBRARY_PATH=/tmp/fdb-f0/macbin FDB_CLUSTER_FILE=/tmp/fdb-f0/fdb.cluster
//!      cargo test --features fdb --test fdb_live -- --ignored --nocapture
//! Docker alt: docker run --network host -v /tmp/fdb-f0/fdb.cluster:/etc/foundationdb/fdb.cluster
//!           -v /tmp/fdb-f0/macbin/libfdb_c.so:/usr/lib/libfdb_c.so -v $PWD:/work -w /work
//!           rust:bookworm cargo test --features fdb --test fdb_live -- --ignored --nocapture

#![cfg(feature = "fdb")]
use foundationdb::{KeySelector, RangeOption};
use std::borrow::Cow;

#[tokio::test]
#[ignore]
async fn live_fdb_cluster_roundtrip() {
    let cluster =
        std::env::var("FDB_CLUSTER_FILE").unwrap_or_else(|_| "/tmp/fdb-f0/fdb.cluster".into());
    if !std::path::Path::new(&cluster).exists() {
        eprintln!("SKIP live_fdb: no cluster at {cluster}");
        return;
    }
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
            tr.set(&k, &v);
            Ok(())
        }
    })
    .await
    .expect("set");

    let got = db
        .run(|tr, _| async move { Ok(tr.get(key, false).await?) })
        .await
        .expect("get");
    assert_eq!(got.as_deref(), Some(val.as_slice()));

    // range — proves ordered scan is the substrate (used by contains_many)
    let range = db
        .run(|tr, _| async move {
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
        tr.clear(key);
        Ok(())
    })
    .await
    .expect("clear");

    eprintln!(
        "LIVE FDB OK cluster={cluster} key={:?}",
        String::from_utf8_lossy(key)
    );
}

#[test]
fn fdb_api_version_is_pinned_730() {
    // proves INV-FDB-PIN: binding compiled against 7.3, max api >= 730
    let max = foundationdb::api::get_max_api_version();
    assert!(max >= 730, "max api {max} must be >= 730 for fdb-7_3");
}
