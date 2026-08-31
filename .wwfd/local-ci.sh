#!/usr/bin/env bash
# .wwfd/local-ci.sh — exec boundary to the one canonical CI orchestrator.
#
# INV-ONE-ORCHESTRATOR: the job graph lives only in ci/keel-ci.toml, rendered
# by keel-ci-plan; this script delegates to it and, on GO, writes the
# spend-guard receipt (~/wwfd/state/local-ci-receipt.json) that unlocks
# git push / gh pr create / gh run rerun for 30 minutes. It also produces the
# local eval evidence ($HOME/.keel-ci/eval/<head_sha>.jsonl) that the
# eval-log job byte-compares on GitHub — a head sha that never ran locally
# cannot go green.
#
# Usage: .wwfd/local-ci.sh [head_rev] [base_rev]   (default: @ origin/main)

set -euo pipefail
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PLAN="${KEEL_CI_PLAN_BIN:-$HOME/keel/keel-core/target/release/keel-ci-plan}"
[ -x "$PLAN" ] || {
  echo "FAIL compatible keel-ci-plan binary missing at $PLAN." >&2
  echo "     Set KEEL_CI_PLAN_BIN to the renderer locked in ci/keel-ci.toml." >&2
  exit 1
}

cd "$REPO_ROOT"
"$PLAN" check
if "$PLAN" run "${1:-@}" "${2:-origin/main}"; then
  printf '{"repo_root":"%s","branch":"%s","head":"%s","ts":"%s"}\n' \
    "$REPO_ROOT" \
    "$(git rev-parse --abbrev-ref HEAD)" \
    "$(git rev-parse HEAD)" \
    "$(date -u +%FT%TZ)" \
    > "$HOME/wwfd/state/local-ci-receipt.json"
  echo "OK   local CI green; spend receipt written."
else
  echo "FAIL local CI red; no receipt." >&2
  exit 1
fi
