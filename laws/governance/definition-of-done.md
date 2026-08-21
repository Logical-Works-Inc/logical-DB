# Definition Of Done

Adapted from logic-os-kernel (2026-08-21): frontend-hardcoding clauses
replaced by this repo's injection-surface equivalents.

## Ticket Done

A ticket is done only when:

- scope is implemented or documented as specified
- acceptance criteria are satisfied
- security considerations are addressed (ingest gauntlet untouched unless
  the issue names it; negative path demonstrated for security-sensitive
  changes)
- no string-query surface was added (INV-NO-STRING-QUERY) or the issue
  explicitly justifies and hardens one
- tests cover happy paths, edge/corner cases, unclean inputs, vulnerability
  scenarios, and regression risks (once code exists; spec-only tickets name
  their verification command instead)
- verification steps are recorded with output
- PR references `Closes #N`
- review has no blocking Hacker or QA-Refiner findings
- `.wwfd/local-ci.sh` green at the exact merged sha
- PR is merged (locally `--no-ff`, never a GitHub merge commit)

## Milestone Done

A milestone is done only when:

- every milestone issue is closed through a PR
- repo-map node statuses reflect reality (`node scripts/repo-map.mjs check`)
- governance files reflect material decisions
- Director sign-off is recorded
