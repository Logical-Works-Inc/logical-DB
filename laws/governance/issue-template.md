# `[type]([lane]): [action] [target]`

**ALL TICKETS MUST FOLLOW THIS DECLARATIVE SCHEMA. NO FREE-FORM PROSE EXCEPT
WHERE NOTED.** Adapted from logic-os-kernel (2026-08-21): lanes are this
repo's; trust-boundary section now targets the ingest gauntlet and query
surface.

## 1. Contract

- **Epic:** [Epic ID / Name — DB-A…DB-E or governance]
- **Lane:** [Kernel / Spec / Governance / Infra]
- **Local Workpad:** `.project/{issue_number}/` *(implementation strategy)*

## 2. Intent & Scope

- **Problem:** [1–2 sentences]
- **In Scope:** [bullet points]
- **Out of Scope:** [explicit exclusions]

## 3. Security Constraints (DiD)

- **Boundary Affected:** [Ingest gauntlet / Query AST / Storage classes /
  Outboard adapter / None]
- **Attacker/Abuse Scenario:** [how could poisoned input, forged provenance,
  or a query door be exploited?]
- **Required Control:** [enforced mitigation; negative path to demonstrate]

## 4. Acceptance & Verification

- [ ] [observable outcome 1]
- [ ] Happy path verification command: `[command]`
- [ ] Exploit path verification (MUST fail): `[command]`

## 5. Agent Interoperability Map (JSON Payload)

Upon completion, update your agent card if capabilities changed.

```json
{
  "k": "gh_issue",
  "band": "gh_issue",
  "layer": "impl",
  "addresses": "[TARGET e.g., kernel-spine, vec-class, pack-format]",
  "pathway_rel": "How does this advance DB_PATHWAY phases §5 / axes §8–§11?",
  "agent_interop": {
    "protocol": "A2A/1.0",
    "required_skills": [],
    "affected_agent_cards": []
  },
  "learnings": []
}
```
