#!/usr/bin/env bash
# Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
# S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. See evidence/SOURCES.md.
# Input is a frozen Git-tree snapshot. A read-only live working-tree bind
# would still permit host edits to alter this script during a cargo command.
set -euo pipefail
lane=${1:?expected linux or windows}
case "$lane" in
  linux) uname -a ;;
  windows)
    wine --version
    /usr/local/bin/logical-db-wine /tmp/ci-windows-smoke.exe
    ;;
  *) echo "unsupported emulator lane: $lane" >&2; exit 2 ;;
esac
# Backpressure on retained caches, never erase state to manufacture cleanliness.
check_cache_budget() {
cache_paths=()
for path in /workspace/target /root/.cargo /root/.wine /tmp; do
  if [[ -d "$path" ]]; then cache_paths+=("$path"); fi
done
used_kib=$(du -sk "${cache_paths[@]}" | awk '{sum += $1} END {print sum+0}')
# The retained Linux cache reached 12,844,708 KiB after the Rust 1.98.1
# rebuild. Keep a finite 16 GiB worker budget for subsequent test artifacts;
# Windows also retains its checksummed runtime export in this bound.
cache_limit_kib=16777216
echo "CACHE lane=$lane used_kib=$used_kib limit_kib=$cache_limit_kib"
[[ "$used_kib" -le "$cache_limit_kib" ]] || { echo "worker cache budget reached; maintenance required" >&2; exit 1; }
}
check_cache_budget
rustc --version --verbose
cargo check --workspace --all-targets --locked
if [[ "$lane" == windows ]]; then
  # Cargo discovers every test executable; the system VM runs this inventory.
  cargo test --workspace --locked --no-run --message-format=json > /tmp/ci-test-artifacts.jsonl
  cargo metadata --locked --no-deps --format-version 1 > /tmp/ci-cargo-metadata.json
  cargo test --workspace --doc --locked
  python3 scripts/ci-windows-runtime.py export
else
  cargo test -p logical-db-e2e --locked --test dependency_contract
  cargo test --workspace --locked
fi
cargo clippy --workspace --all-targets --locked -- -D warnings
if [[ "$lane" == linux ]]; then
  # Preserve the original Linux package-isolation lane and report every
  # failing package, without silently stopping after the first failure.
  failed=0
  for crate in logical-db logical-db-cairn logical-db-crdt logical-db-daemon logical-db-episode logical-db-http logical-db-kv logical-db-model logical-db-platform logical-db-protocol logical-db-server logical-db-service logical-db-store logical-db-sync; do
    if ! cargo check -p "$crate" --all-targets --locked; then failed=1; fi
  done
  if [[ "$failed" != 0 ]]; then exit "$failed"; fi
  bash scripts/ci-fdb-linux.sh
fi
check_cache_budget
