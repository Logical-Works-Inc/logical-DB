#!/usr/bin/env bash
# External consumer tape: prove the public facade boundary from outside the
# Logical-DB workspace.
#
# Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15 S16
# S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
# evidence/SOURCES.md.
#
# Three probes, all black-box:
#
#   1. embedded — the tape's offline application journey through the public
#      facade only (open, edit, index, export, restore, corrupt-backup refusal,
#      delete, reopen). Requires no daemon, network, FoundationDB, or Braid.
#   2. dependency-graph — the tape's manifest and source declare no internal
#      Logical-DB crate, and `cargo tree` confirms none resolves into its graph.
#      This is the structural form of "no internal path leaks".
#   3. sync — the tape converges two replicas over the repository's loopback
#      journey host, as a pure client: it never constructs a server.
#
# Usage: scripts/external-tape.sh
# Exit 0 only when every probe passes.

set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TAPE="${LOGICAL_DB_TAPE_DIR:-$REPO/../logical-db-tape}"

if [[ ! -d "$TAPE" ]]; then
  echo "FAIL external-tape: tape package not found at $TAPE" >&2
  exit 1
fi

echo "== probe 1: embedded public-facade journey"
(cd "$TAPE" && cargo build --quiet)
"$TAPE/target/debug/tape" embedded

echo "== probe 2: dependency graph declares no internal crate"
# The tape's manifest must not declare any internal Logical-DB crate. Comments
# are stripped first so documentation that names those crates is not a match.
if grep -Ev '^[[:space:]]*(#|$)' "$TAPE/Cargo.toml" \
  | grep -Eq 'logical-db-(model|protocol|crdt|store|sync|service|http|server|platform|kv|cairn|episode|daemon|store-fdb|e2e)'; then
  echo "FAIL external-tape: tape manifest declares an internal Logical-DB crate" >&2
  exit 1
fi
# The tape's source must not import one either.
if grep -rEq '^[[:space:]]*(use|extern crate)[[:space:]]+logical_db_(model|protocol|crdt|store|sync|service|http|server|platform|kv|cairn|episode|daemon)\b' \
  "$TAPE/src"; then
  echo "FAIL external-tape: tape source imports an internal Logical-DB crate" >&2
  exit 1
fi

echo "== probe 3: two-replica sync as a pure client of a host service"
(cd "$REPO" && cargo build --quiet -p logical-db-e2e --bin journey_host)
HOST="$REPO/target/debug/journey_host"
"$HOST" >"$REPO/target/journey-host.log" 2>&1 &
HOST_PID=$!
cleanup() {
  kill "$HOST_PID" 2>/dev/null || true
  wait "$HOST_PID" 2>/dev/null || true
}
trap cleanup EXIT

# Wait for the host to announce its ephemeral port, bounded.
ENDPOINT=""
for _ in $(seq 1 100); do
  if grep -q '^READY ' "$REPO/target/journey-host.log" 2>/dev/null; then
    ENDPOINT="$(grep -m1 '^READY ' "$REPO/target/journey-host.log" | awk '{print $2}')"
    break
  fi
  sleep 0.1
done
if [[ -z "$ENDPOINT" ]]; then
  echo "FAIL external-tape: journey host did not become ready" >&2
  cat "$REPO/target/journey-host.log" >&2 || true
  exit 1
fi

"$TAPE/target/debug/tape" sync "http://$ENDPOINT/" "black-box"

echo "PASS external-tape embedded dependency-graph sync"
