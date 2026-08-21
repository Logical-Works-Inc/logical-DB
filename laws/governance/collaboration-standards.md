# Collaboration Standards

Adapted from logic-os-kernel (2026-08-21): scopes renamed to this repo's
lanes.

## 1. Issue Authoring Principle

All contributors (Humans and agents) MUST author issues using the declarative
schema in `laws/governance/issue-template.md`.

### 1.1 The Rejection Rule

If an agent encounters a ticket that does not match the schema (missing
prefix, missing template sections, or missing agent-interop payload), it
MUST:

1. Halt execution of the requested work.
2. Rewrite the ticket to match the schema.
3. Add a system comment: `System: Issue reformatted to comply with the DiD
   Governance Schema.`

## 2. Commit Message Enforcement

Commits must be atomic, descriptive, and follow the `type(scope): description`
format.

- **Types:** `feat`, `fix`, `arch`, `security`, `docs`, `chore`.
- **Scopes:** `kernel`, `spec`, `governance`, `infra`.
- **Format:** `feat(kernel): add segment journal group commit`
- **Rule:** Commits MUST map to specific checklist items in the local workpad
  (`.project/{issue}/plan.md`).

## 3. Pull Request Standards

See `definition-of-done.md` (canonical): verification recorded, negative path
demonstrated for security-sensitive changes, local CI green at the merged
sha, review verdict attached.

## 4. Sub-Issue & Epic Triggers

- **Epic:** work requires >3 days or touches >3 disparate modules.
- **Sub-Issue:** an unexpected bug is found during feature work (do not fix
  inline).
- **Flag:** work requires changing an agreed ADR → add
  `[FLAG: ARCH DIVERGENCE]` and pause.
