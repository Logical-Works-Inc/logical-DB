# Workflow

How work moves. Rules: `CODEBOOK.md`, `INVARIANTS.md`. Authority:
`GOVERNANCE.md`.

## Local gate

```sh
cargo run -p logical-db-e2e --bin ci_local            # full gate
cargo run -p logical-db-e2e --bin ci_local -- --fast  # compile, lint, fmt
```

`crates/logical-db-e2e/src/bin/ci_local.rs` is the one definition of the gate.
`.github/workflows/ci.yml` runs the same commands. When they disagree, fix the
workflow.

## Delivery

1. One issue, one branch, one PR.
2. Write the failing test first, then the change.
3. Small commits, each green.
4. The PR body carries the commands and their output.
5. Merge when every lane is green on the head.

## CI lanes

| Lane | Runner | Runs |
|---|---|---|
| `linux / test-clippy` | `ubuntu-24.04` | check, tests (not e2e), clippy, per-crate isolation |
| `linux / end-to-end journeys` | `ubuntu-24.04` | dependency register, then every e2e test |
| `macos / test-clippy` | `macos-15` | check, tests, clippy |
| `windows / test-clippy` | `windows-2025` | check, tests, clippy |
| `rustfmt` | `ubuntu-24.04` | `cargo fmt --check` |
| `advisory, license, and source policy` | `ubuntu-24.04` | `cargo deny check` |
| `official FDB client / *` | `macos-15`, `ubuntu-24.04` | FDB 7.3 feature build and live-cluster journeys |
| `clean-room evidence` | `ubuntu-24.04` | `actionlint` on the workflows |

The repo is public, so jobs run only on pinned GitHub-hosted images
(`tests/ci_routing.rs`). Every `rust-cache` step saves from `main` only, so pull
requests never evict main's 10 GiB cache.

A test gated to `target_os = "linux"` compiles to nothing elsewhere. Run Linux
for those; a green macOS run there proves nothing.
