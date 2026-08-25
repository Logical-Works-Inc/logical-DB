# Decision 2026-08-25 — hermetic CI verification and nonvacuous teeth

## Decision

Every CI job that reads tree bytes carries its own `actions/checkout`
pinned to its own run's head sha (INV-HERMETIC-CHECKOUT). The
cargo-mutants tier refuses a zero-mutant run instead of exiting green
(INV-MUTANTS-NONVACUOUS). Jobs that read no tree bytes
(stack-position, mcdc-coverage) and cleanup carry no checkout by
design.

## Evidence

Run 32824022656 went red on good sha `bed11ff`: the runner exposes one
shared `_work` worktree, and at 10:22:45Z an orphaned job from a
backlogged run flipped it to pre-rename sha `d824345` mid-run (runner
reflog). Six jobs then certified the wrong tree — Teeth passed in 3s
with "Found 0 mutants", exit 0 — while Property (`-p logical-db`)
failed in 2s on the same bytes. Local merge-sha gate GO 11/11 proved
the sha itself good.

## Invariants

- INV-HERMETIC-CHECKOUT — header comment and checkout steps,
  `ci/keel-ci.toml`.
- INV-MUTANTS-NONVACUOUS — `mutation-test-verify` job script.

## Exit criterion

First post-fix run 32888949763 green with real work: 13/13 jobs,
Teeth 651s of actual mutation testing where the race produced a 3s
vacuous pass.

## Scope and owner

Applies to this repository's generated workflows only
(`ci/keel-ci.toml` → `.github/workflows/ci.yml`). No keel changes were
required or made; the plan schema already supported per-job action
steps. Director-approved 2026-08-25.
