#!/usr/bin/env bash
# Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
# S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. See evidence/SOURCES.md.
main() {
set -euo pipefail
worker=${1:?expected stopped compiler worker}
tree=${2:?expected source tree}
source_root=${3:?expected frozen source}
[[ "$worker" == logical-db-ci-windows-* && "$tree" =~ ^[[:xdigit:]]{40}$ ]]
[[ $(docker inspect "$worker" --format '{{.State.Running}}') == false ]]
[[ $(docker inspect "$worker" --format '{{index .Config.Labels "logical-db.owner"}}') == ci ]]
vm=logicaldb-windows-emulator
source_kib=$(du -sk "$source_root" | cut -f1)
record=$(limactl list --json | python3 -c 'import json,sys
for line in sys.stdin:
    value=json.loads(line)
    if value["name"] == "logicaldb-windows-emulator": print(json.dumps(value))')
if [[ -n "$record" ]]; then
  python3 -c 'import json,sys
v=json.load(sys.stdin); c=v["config"]
assert v["status"] == "Stopped", "VM is already owned by an active run"
assert (v["vmType"],v["arch"],v["cpus"],v["memory"],v["disk"]) == ("qemu","x86_64",4,4294967296,21474836480)
assert c["plain"] and not c.get("mounts")
assert not c["ssh"]["forwardAgent"] and not c["ssh"]["loadDotSSHPubKeys"]
assert c["images"][0]["digest"] == "sha256:117816726abbdefc5ef3e38902e81a76f1c76c3610e709999d0885f9d5d9b477"
' <<< "$record"
fi
cleanup() {
  result=$?
  trap - EXIT
  if ! limactl stop "$vm"; then
    echo "Owned VM stop failed; inspect $vm before retrying." >&2
    if [[ "$result" == 0 ]]; then result=1; fi
  fi
  exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
if [[ -n "$record" ]]; then
  limactl start --tty=false --timeout=10m "$vm"
else
  limactl start --tty=false --timeout=10m --name "$vm" "$source_root/scripts/ci-windows-vm.yaml"
fi
# Expansion belongs to the guest shell, not this host shell.
# shellcheck disable=SC2016
limactl shell "$vm" bash -c 'set -eu
if ! command -v wine >/dev/null; then
  sudo timeout 600 apt-get update
  sudo timeout 600 apt-get install -y --no-install-recommends wine=10.0~repack-12ubuntu1
fi
[[ $(uname -m) == x86_64 && $(wine --version) == wine-10.* ]]
/usr/lib/x86_64-linux-gnu/wine/wineserver --version
sudo mkdir -p /var/lib/logical-db-ci
'
guest_root="/var/lib/logical-db-ci/$tree"
limactl copy --backend=scp "$source_root/scripts/ci-windows-runtime.py" "$vm:/tmp/logical-db-ci-runtime.py"
docker cp "$worker:/tmp/ci-windows-runtime/.ci-windows/tests.json" - | tar -xOf - | \
  limactl shell "$vm" python3 /tmp/logical-db-ci-runtime.py plan "$guest_root" "$source_kib"
limactl shell "$vm" sudo mkdir -p "$guest_root"
tar -C "$source_root" --exclude=./target -cf - . | limactl shell "$vm" sudo tar -xf - -C "$guest_root"
docker cp "$worker:/tmp/ci-windows-runtime/." - | limactl shell "$vm" sudo tar -xf - -C "$guest_root"
# Only the workspace pointer changes. Every revision's source/artifacts remain
# recoverable; the VM has no host mounts, agent forwarding or host credentials.
# shellcheck disable=SC2016
limactl shell "$vm" sudo bash -c 'set -eu
[[ ! -e /workspace || -L /workspace ]]
chown -hR root:root "$1"
chmod -R a+rX,a-w "$1"
ln -sfn "$1" /workspace' bash "$guest_root"
echo "SYSTEM-EMULATOR vm=$vm source=$tree"
limactl shell "$vm" timeout --signal=TERM --kill-after=2s 1800 python3 /workspace/scripts/ci-windows-runtime.py run
}
main "$@"
