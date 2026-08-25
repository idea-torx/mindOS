# MindOS live conversation bridge — progress log

Isolated worktree branch: feat/mindos-live-conversation-bridge
Contract: MINDOS-LIVE-CONVERSATION-BRIDGE.md (read-only vs live ~/.hermes; disposable fixtures only)

## Log

- [t0] Read contract. Inventoried repo: autopilot.py (session cache schema, FTS5, secret guard,
  session-scan/ingest/search), ops.py (provider-neutral Hindsight GET-only binding probes),
  verify.py (disposable-home harness pattern via HERMES_AUTOPILOT_HOME).
- [t0] Design decision — Hindsight write path: repo exposes only GET endpoints
  (/health, /v1/default/banks, /banks/{bank}/stats). Contract forbids inventing endpoint shapes,
  so the bridge implements GET-only health/bank verification, a provenance-complete
  export manifest (JSONL) for provider-specific import, and an honest `pending`
  sync state. No fabricated success is possible by construction.
- [t1] Implementing mindos_bridge.py:
    - sync: read-only adapter over Hermes-style JSONL session stores -> sessions/session_messages
      keyed by source/profile/session/message identity + content hash; idempotent; atomic re-index;
      secret guard refuses by default (--redact / audited --allow-secret).
    - bridge_hindsight ledger table: message-key -> sync state (pending/exported), full provenance.
    - hindsight-check: GET-only health + shared-bank binding probe (unavailable/degraded honest).
    - export: bounded, deterministic JSONL manifest carrying source/profile/session/seq/role/at/content-hash.
    - promote: explicit-only hook to notes/facts/tasks with provenance citation.
    - watch: near-real-time loop (interval) around sync; batch/cron remains fallback.
- [ ] Tests: tests_bridge.py (fixtures, sentinel, idempotency, change re-index, secret paths,
      degraded/unavailable Hindsight, roles, search, budgeted recall).
- [ ] Gates: py_compile x3, full verify.py, tests_bridge.py, git diff check, secret/PII scan.
- [ ] Commit + merge-ready receipt.

- [t2] Digest determinism fix: root cause was the TEST comparing digests of two
  recalls with different flags (--related-scope global vs default project). The
  fixture's ingested session carries project='' while task promo-1 is in
  'Verify', so project scope correctly filtered the session out — live
  filtering semantics were working as designed; the digest itself already
  excludes only recalled_at (runtime query time) and seals durable context
  provenance. Fix: second recall now uses identical flags and asserts
  bundle['digest'] equality directly (no phantom recall_digest fallback key).
- [t2] Gates: tests_bridge.py all PASS; verify.py full PASS; py_compile x4 OK;
  git tracked diff clean; secret/PII scan clean on all four new files.
