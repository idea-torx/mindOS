# OX orphan-receipt migration repair

Fix the failed MindOS onboarding import in this isolated worktree.

Observed failure: migrate-import fails foreign-key integrity because the source contains receipt rows referencing task IDs absent from the source tasks table (example is a disposable verification receipt). Do not weaken foreign keys or silently lose evidence.

Design a principled migration behavior for orphan receipts. It may quarantine/detach them, skip them with an explicit audited report, or introduce another clean representation. Preserve provenance, hashes, rollback accounting, and source immutability. The behavior must be fail-closed by default where ambiguity is dangerous, but allow the verified onboarding path to complete without creating broken execution truth. Never invent a task or attach a receipt to the wrong task.

Required:
- inspect the existing migration/import/rollback schema and contracts
- implement the smallest coherent architecture
- add regression tests for orphan receipts, normal receipts, re-run idempotency, rollback, and reporting
- preserve secret guard and receipt integrity semantics
- update README and STABILITY.md
- run py_compile, verify.py, diff check, secret/PII scan
- commit the fix on this branch and leave the tree clean

Never access or modify live ~/.hermes state, Hindsight, Keychain, credentials, external APIs, or production. No merge, push, deploy, or install. Return exact commit, behavior, tests, and remaining risks.