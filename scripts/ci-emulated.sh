#!/usr/bin/env bash
# Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
# S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. See evidence/SOURCES.md.
# Parse the complete host driver before starting long-running child commands.
# The guest then reads only the frozen source snapshot, never this live file.
main() {
set -euo pipefail
lane=${1:?expected linux or windows}
case "$lane" in linux|windows) ;; *) echo "unsupported emulator lane: $lane" >&2; exit 2 ;; esac
cd "$(git rev-parse --show-toplevel)"
checkout_id=$(printf '%s' "$PWD" | shasum -a 256 | cut -c 1-12)
worker="logical-db-ci-${lane}-${checkout_id}"
if docker container inspect "$worker" >/dev/null 2>&1; then
  [[ $(docker container inspect "$worker" --format '{{.State.Running}}') == false ]] || {
    echo "Worker $worker is already owned by an active run." >&2; exit 1;
  }
fi
# A read-only bind still sees host edits. Freeze the index, not the live
# working tree, so edits during compilation cannot change a running script.
git diff --quiet || { echo "Stage the intended source before an emulator run." >&2; exit 1; }
source_tree=$(git write-tree)
source_parent="$HOME/.cache/logical-db-ci/$checkout_id/$lane"
source_root="$source_parent/source"
mkdir -p "$source_parent" "$HOME/.Trash"
if [[ -e "$source_root" ]]; then
  archived="$HOME/.Trash/logical-db-ci-source-$lane-$checkout_id-$(date -u +%Y%m%dT%H%M%SZ)-$$"
  mv "$source_root" "$archived"
  [[ ! -e "$source_root" && -d "$archived" ]]
  echo "Previous source snapshot retained in $archived"
fi
mkdir -p "$source_root"
git archive "$source_tree" | tar -xf - -C "$source_root"
mkdir -p "$source_root/target"
cd "$source_root"
echo "SOURCE tree=$source_tree snapshot=$source_root"
# BuildKit resolves a locally imported image by tag. Verify the immutable ID
# first; do not silently download a different base or use a hosted runner.
expected=sha256:a66180f4e264b4a8038399ed4d1aa7e77fc23f242d3d9a43830fa0388d6e0c90
actual=$(docker image inspect logical-db-act:local --format '{{.Id}}')
[[ "$actual" == "$expected" ]] || { echo "runner image identity mismatch" >&2; exit 1; }
image_tag="logical-db-ci-${lane}:${source_tree}"
docker build --pull=false --platform linux/amd64 -f "scripts/ci-${lane}.Dockerfile" -t "$image_tag" .
image_id=$(docker image inspect "$image_tag" --format '{{.Id}}')

# A stopped worker owns only disposable build caches. Preserve its complete
# filesystem before replacing it whenever its verified image or mount contract
# no longer matches the newly built emulator image.
replace_retained_worker() {
  local reason=$1
  local archived_worker
  archived_worker="$HOME/.Trash/logical-db-ci-worker-${lane}-${checkout_id}-$(date -u +%Y%m%dT%H%M%SZ)-$$.tar"
  docker export "$worker" -o "$archived_worker"
  [[ -s "$archived_worker" ]] || { echo "worker export was empty: $archived_worker" >&2; exit 1; }
  docker rm "$worker" >/dev/null
  echo "Retained worker $reason; exported worker to $archived_worker before replacement"
}

# Docker may collect an image while retaining its stopped container. Preserve
# that worker's complete filesystem before replacement; otherwise the later
# image inspection fails with an opaque "No such image" and the CI lane cannot
# recover on its own.
if docker container inspect "$worker" >/dev/null 2>&1; then
  worker_image=$(docker container inspect "$worker" --format '{{.Image}}')
  if ! docker image inspect "$worker_image" >/dev/null 2>&1; then
    replace_retained_worker "image was unavailable"
  fi
fi

# One reusable, normally stopped worker per checkout/lane. The frozen source
# mount excludes .git (checkout credentials), host caches and sockets.
# Build/Wine caches belong to this worker; logs rotate at 2 x 10 MiB.
[[ "$PWD" != *,* ]] || { echo "unsupported comma in source path" >&2; exit 2; }
mounts=(--mount "type=bind,source=$PWD,target=/workspace,readonly")
# Stable per-target caches also serve the Actions checkout on this machine.
# Cargo serializes access with its own registry/build locks. Never share a
# target directory between Linux and Windows or mount the host Cargo home.
mounts+=(
  --mount "type=volume,source=logical-db-ci-${lane}-target,target=/workspace/target"
  --mount "type=volume,source=logical-db-ci-${lane}-registry,target=/root/.cargo/registry"
  --mount "type=volume,source=logical-db-ci-${lane}-git,target=/root/.cargo/git"
)
configuration=$(printf '%s\0' "$PWD" "$image_id" "${mounts[@]}" | shasum -a 256 | cut -d ' ' -f 1)
if docker container inspect "$worker" >/dev/null 2>&1; then
  worker_image=$(docker container inspect "$worker" --format '{{.Image}}')
  # Attestations can change an index digest on a cached rebuild. Compare the
  # actual runtime configuration and filesystem, while retaining both receipts.
  image_content=$(docker image inspect "$image_id" --format '{{json .Config}} {{json .RootFS}}')
  worker_content=$(docker image inspect "$worker_image" --format '{{json .Config}} {{json .RootFS}}')
  [[ "$image_content" == "$worker_content" ]] || replace_retained_worker "had different runtime image bytes"
  if docker container inspect "$worker" >/dev/null 2>&1; then
    configuration=$(printf '%s\0' "$PWD" "$worker_image" "${mounts[@]}" | shasum -a 256 | cut -d ' ' -f 1)
    recorded=$(docker container inspect "$worker" --format '{{index .Config.Labels "logical-db.configuration"}}')
    [[ "$recorded" == "$configuration" ]] || replace_retained_worker "had a different image or mount configuration"
  fi
else
  docker create --name "$worker" --platform linux/amd64 --cpus 4 --memory 6g --pids-limit 512 \
    --network bridge --log-opt max-size=10m --log-opt max-file=2 \
    --label logical-db.owner=ci --label "logical-db.configuration=$configuration" \
    "${mounts[@]}" --entrypoint /bin/bash "$image_id" \
    -c 'timeout --signal=TERM --kill-after=2s 3000 bash scripts/ci-guest.sh "$1"' bash "$lane"
fi
trap 'docker stop --timeout 2 "$worker" >/dev/null' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
echo "EMULATOR worker=$worker image=$image_id lane=$lane"
docker start --attach "$worker"
result=$(docker container inspect "$worker" --format '{{.State.ExitCode}}')
[[ $(docker container inspect "$worker" --format '{{.State.Running}}') == false ]]
if [[ "$result" == 0 && "$lane" == windows ]]; then
  bash scripts/ci-windows-runtime.sh "$worker" "$source_tree" "$source_root"
fi
exit "$result"
}
main "$@"
