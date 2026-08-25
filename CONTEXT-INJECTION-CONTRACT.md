# MindOS context injection contract

Implement bounded, profile-safe relevant context injection from MindOS into new Hermes sessions. This is an isolated implementation worktree; do not touch live ~/.hermes/mindos, rollback, gateway, GitHub, or other worktrees.

## Required design

- Start with session-start context packs, not synthetic per-turn user messages or system-prompt rewriting.
- Use existing MindOS session search/recall, temporal facts, handoffs, receipts, and Hindsight shared context where available.
- Preserve prompt caching, role alternation, provenance, redaction, deterministic digests, and idempotence.
- Make the pack bounded by bytes/items and profile-safe; never cross profiles or leak secrets/PII.
- Include source/provenance, generated-at, freshness, digest, and stale/recompute behavior.
- Fail closed or degrade honestly when sources are unavailable; do not fabricate context.
- Provide an explicit opt-out/disable path and tests for empty, stale, unavailable, redacted, cross-profile, and duplicate cases.
- Add a live sentinel/verification command that proves a new Hermes session receives the pack without mutating live runtime during tests.

## Workflow

First inspect the current bridge, native SQLite adapter, gateway hook, session lifecycle, existing recall/context-pack functions, and R1-R7 protocol. Write CONTEXT-INJECTION-DESIGN.md with the smallest architecture and data flow before broad edits. Then implement, add focused tests, run py_compile, bridge/SQLite/gateway/full verify gates plus context-injection tests, diff check, secret/PII scan, and commit with Leo identity. Do not install live or deploy.
