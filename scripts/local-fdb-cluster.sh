#!/usr/bin/env bash
# scripts/local-fdb-cluster.sh — hermetic one-process FDB dev cluster.
#
# Owns pinned-artifact verification and cluster lifecycle for the
# issue #9 falsifier. INV-FDB-PIN — an artifact failing its upstream
# .sha256 aborts before anything executes (corrupt a byte to see verify
# exit 1). INV-FDB-SCRATCH — nothing installs system-wide; clean
# removes the whole scratch root exactly.
#
# Usage: local-fdb-cluster.sh verify|up|status|down|clean
# Env: FDB_VERSION=7.3.79 · FDB_SCRATCH=/tmp/fdb-f0

set -euo pipefail

FDB_VERSION="${FDB_VERSION:-7.3.79}"
SCRATCH="${FDB_SCRATCH:-/tmp/fdb-f0}"
PORT="${FDB_PORT:-4500}"
BIN="$SCRATCH/macbin"
BASE="https://github.com/apple/foundationdb/releases/download/$FDB_VERSION"
CLUSTER="$SCRATCH/fdb.cluster"
PIDFILE="$SCRATCH/server.pid"

die() { echo "REFUSED: $*" >&2; exit 1; }

fetch() { curl -sfL -o "$1" "$BASE/$2"; }

cli() { "$BIN/fdbcli" -C "$CLUSTER" --exec "$1"; }

wait_available() {
    for _ in $(seq 1 60); do
        cli 'status minimal' 2>/dev/null | grep -q available && return 0
        sleep 1
    done
    die "server not available within 60s"
}

# ── verify ───────────────────────────────────────────────────────

cmd_verify() {
    mkdir -p "$SCRATCH"
    local name="FoundationDB-${FDB_VERSION}_arm64.pkg"
    # FDB_REFETCH=1 forces a redownload; otherwise an existing artifact
    # is verified in place so byte-tampering is detectable.
    if [ ! -f "$SCRATCH/$name" ] || [ "${FDB_REFETCH:-0}" = 1 ]; then
        fetch "$SCRATCH/$name" "$name"
    fi
    if [ ! -f "$SCRATCH/$name.sha256" ] || [ "${FDB_REFETCH:-0}" = 1 ]; then
        fetch "$SCRATCH/$name.sha256" "$name.sha256"
    fi
    (
        cd "$SCRATCH"
        shasum -a 256 -c "$name.sha256" || die "INV-FDB-PIN violated"
    )
}

stage() {
    [ -x "$BIN/fdbserver" ] && return 0
    mkdir -p "$BIN"
    local pkg="FoundationDB-${FDB_VERSION}_arm64.pkg"
    pkgutil --expand-full "$SCRATCH/$pkg" "$SCRATCH/expanded" || die "pkg expand failed (INV-FDB-PIN)"
    cp "$SCRATCH/expanded/FoundationDB-server.pkg/Payload/usr/local/libexec/fdbserver" "$BIN/"
    cp "$SCRATCH/expanded/FoundationDB-clients.pkg/Payload/usr/local/bin/fdbcli" "$BIN/"
    cp "$SCRATCH/expanded/FoundationDB-clients.pkg/Payload/usr/local/lib/libfdb_c.dylib" "$BIN/"
    chmod +x "$BIN/fdbserver" "$BIN/fdbcli"
}

# ── cluster lifecycle ────────────────────────────────────────────

cmd_up() {
    stage
    mkdir -p "$SCRATCH/data" "$SCRATCH/logs"
    if [ ! -f "$CLUSTER" ]; then
        printf 'local_fdb:f0e1d2c3b4a5968778695a4b3c2d1e0f@127.0.0.1:%s\n' \
            "$PORT" > "$CLUSTER"
    fi
    nohup "$BIN/fdbserver" -C "$CLUSTER" -p "127.0.0.1:$PORT" \
        -d "$SCRATCH/data" -L "$SCRATCH/logs" \
        >"$SCRATCH/server.out" 2>&1 &
    echo $! > "$PIDFILE"
    wait_available
    if [ ! -f "$SCRATCH/.configured" ]; then
        if [ -n "$(find "$SCRATCH/data" -maxdepth 1 -type f 2>/dev/null | head -n1)" ]; then
            touch "$SCRATCH/.configured"
        else
            cli 'configure new single memory'
            touch "$SCRATCH/.configured"
        fi
    fi
    echo "up: pid $(cat "$PIDFILE") cluster $CLUSTER bin $BIN"
}

cmd_status() {
    cli 'status minimal' | head -1
    ps -o rss=,etime= -p "$(cat "$PIDFILE")"
}

cmd_down() {
    kill "$(cat "$PIDFILE")"
}

cmd_clean() {
    cmd_down >/dev/null 2>&1 || true
    rm -rf "$SCRATCH"
}

usage() {
    grep -A6 '^# Usage:' "$0" | sed 's/^# \{0,3\}//' >&2
    exit 1
}

main() {
    case "${1:-}" in
        verify) cmd_verify ;;
        up) cmd_up ;;
        status) cmd_status ;;
        down) cmd_down ;;
        clean) cmd_clean ;;
        *) usage ;;
    esac
}
main "$@"
