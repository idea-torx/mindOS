# MindOS release — Autopilot v3 operations and managed botmail

## Highlights

This release brings the verified Autopilot v3 self-improvement slice and the first managed intra-bot communication layer into the MindOS source repository.

### Autopilot v3 self-improvement

- provider-neutral `runner/v1` execution receipts for Hermes, DSH, OpenCode, Codex, Claude Code, and future harnesses;
- durable activity reports with last action, next intent, progress state, evidence, and stall deadlines;
- `activity_stalled` findings for work that stops making meaningful progress;
- explicit `transient`, `infra`, and `defect` failure causes;
- bounded correction-child planning with lineage, grant windows, family caps, and audited refusals;
- audit append locking to prevent concurrent nanny ticks from forking the hash chain;
- grounded `all_clear`, `working`, `hit_snag`, `decision_needed`, and `carried_over` impulse behavior.

### Managed intra-bot communication

- provider-neutral botmail envelope v1;
- bot, harness, peer, profile, direction, capability epoch, and provenance fields;
- peer allowlists with capability and expiry validation;
- idempotent accepted/rejected/duplicate/expired/failed receipts;
- replay, correlation-chain, and self-loop budgets;
- secret guarding before storage or context inclusion;
- separation between bot chat, user relay, handoff, and task receipt content;
- bounded profile-scoped bot-chat context with provenance;
- cross-harness parsing, including Hermes DM text.

## Verification

- Autopilot verification: 17/17;
- botmail tests: 11/11 scenarios passed;
- bridge, SQLite, gateway, and context suites passed;
- secret/PII scans clean;
- diff checks clean.

## Rollout note

The runtime deployment and repository source are separate controls. Live Autopilot v3 and the verified botmail runtime have rollback backups. Actual Hermes peer-DM delivery, gateway roster reconciliation, Hindsight bot-chat synchronization, and the live cross-profile sentinel remain explicit follow-up gates.
