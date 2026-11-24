# Governance

Who decides, and what a merge needs. Code rules: `CODEBOOK.md`. Process:
`WORKFLOW.md`. Rules with tests: `INVARIANTS.md`.

## Authority

| Holder | Decides | Does not decide |
|---|---|---|
| Director | product scope, release claims, gate exceptions | day-to-day implementation |
| Maintainer or agent | implementation inside the scope, refactors that keep CI green, docs that state fact | new scope, scale or cost claims, closing unproven issues |
| `logical_db_service::Authority` | one `ExecutionGrant` per action, scope and frontier | anything outside the bound scope |
| Storage adapters | durability, key layout, chunking | authorization, transport |

A capability is in the run path or it does not exist. No default-off second
path.

## Decisions

- A Director ruling wins over issue bodies, plans and older rulings.
- A decision that narrows a rule or moves ownership is an ADR in `docs/adr/`,
  with the constraint, evidence, owner, issue and review date.
- Open work and decisions live in GitHub issues, not in files here.

## Merge

- Every lane in `.github/workflows/ci.yml` is green on the PR head.
- `cargo run -p logical-db-e2e --bin ci_local` reproduces CI locally.
- Never widen a lint, a licence or advisory exception, or a dependency register
  to pass a check. That is a suppressed gate.
- A dependency PR is landed, never closed in place of landing.
- Never commit `target/`, `node_modules/`, `.codegraph/` or secrets.
