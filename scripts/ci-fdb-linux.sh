#!/usr/bin/env bash
# Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
# S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. See evidence/SOURCES.md.
set -euo pipefail
fdb_root=/opt/logical-db-fdb-7.3.77
mkdir -p "${fdb_root}"
for asset in libfdb_c.x86_64.so fdbserver.x86_64 fdbcli.x86_64; do
  if [[ ! -f "${fdb_root}/${asset}" ]]; then
    curl --fail --location --retry 5 \
      "https://github.com/apple/foundationdb/releases/download/7.3.77/${asset}" \
      --output "${fdb_root}/${asset}"
  fi
  if [[ ! -f "${fdb_root}/${asset}.sha256" ]]; then
    curl --fail --location --retry 5 \
      "https://github.com/apple/foundationdb/releases/download/7.3.77/${asset}.sha256" \
      --output "${fdb_root}/${asset}.sha256"
  fi
  (cd "${fdb_root}" && sha256sum --check "${asset}.sha256")
done
cd "${fdb_root}"
if [[ ! -e libfdb_c.so ]]; then ln --symbolic libfdb_c.x86_64.so libfdb_c.so; fi
chmod +x fdbserver.x86_64 fdbcli.x86_64
export FDB_LIBRARY_ROOT="${fdb_root}"
cd /workspace

export RUSTFLAGS="-L native=${FDB_LIBRARY_ROOT} -C link-arg=-Wl,-rpath,${FDB_LIBRARY_ROOT}"
cargo check -p logical-db-store-fdb --features fdb --locked
cargo check -p logical-db-e2e --features fdb --all-targets --locked
cargo clippy -p logical-db-daemon --features fdb --all-targets --locked -- -D warnings
cargo clippy -p logical-db-e2e --features fdb --all-targets --locked -- -D warnings
cargo test -p logical-db-store-fdb --features fdb --locked
cargo test -p logical-db-daemon --features fdb --locked

set -euo pipefail
export RUSTFLAGS="-L native=${FDB_LIBRARY_ROOT} -C link-arg=-Wl,-rpath,${FDB_LIBRARY_ROOT}"
live_root=$(mktemp -d /tmp/logical-db-fdb-live.XXXXXX)
mkdir -p "${live_root}/data" "${live_root}/logs"
cluster_file="${live_root}/fdb.cluster"
echo "logicaldb:ci@127.0.0.1:4650" > "${cluster_file}"
"${FDB_LIBRARY_ROOT}/fdbserver.x86_64" \
  -p 127.0.0.1:4650 -C "${cluster_file}" \
  -d "${live_root}/data" -L "${live_root}/logs" \
  --memory 1GiB --cache-memory 256MiB --storage-memory 256MiB &
fdb_pid=$!
cleanup() {
  kill "${fdb_pid}" 2>/dev/null || true
  wait "${fdb_pid}" 2>/dev/null || true
}
trap cleanup EXIT
for _attempt in {1..40}; do
  if "${FDB_LIBRARY_ROOT}/fdbcli.x86_64" -C "${cluster_file}" \
    --exec "configure new single memory"; then break; fi
  sleep 1
done
for _attempt in {1..40}; do
  status=$("${FDB_LIBRARY_ROOT}/fdbcli.x86_64" -C "${cluster_file}" \
    --exec "status minimal" 2>&1 || true)
  if grep --fixed-strings --line-regexp --quiet \
    "The database is available." <<< "${status}"; then
    echo "${status}"
    break
  fi
  if [[ "${_attempt}" == 40 ]]; then
    echo "${status}"
    exit 1
  fi
  sleep 1
done
export LOGICAL_DB_FDB_CLUSTER_FILE="${cluster_file}"
cargo run -p logical-db-daemon --bin fdb-ambiguity-journey \
  --features fdb --locked
cargo test -p logical-db-e2e --features fdb --locked \
  --test fdb_recovery_composition -- --nocapture
cargo test -p logical-db-e2e --features fdb --locked \
  --test fdb_process_kill_restore -- --nocapture
