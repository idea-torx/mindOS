# MindOS live Hermes conversation bridge

Build the missing end-to-end bridge so important live Hermes conversations become available to the MindOS shared brain without requiring manual promotion every time.

Work only in this isolated worktree. Do not modify live `~/.hermes`, the active MindOS home, rollback, the Hermes installation, gateways, credentials, GitHub, or other worktrees. Do not send messages or publish anything.

## Problem

Hermes-native conversations are immediately available to Hermes session search, but the current MindOS ingestion jobs mainly sync Claude project memory and the Autopilot temporal sidecar. A live Telegram/Hermes conversation can therefore be absent from MindOS SQLite session cache and Hindsight even when it contains important project direction.

## Target architecture

Implement a provider-neutral, profile-safe bridge with these layers:

1. **Read-only source adapter** for Hermes session/transcript records, using existing session-ingestion conventions. Never mutate Hermes source sessions.
2. **Incremental durable cache** in MindOS sessions/session_messages keyed by source/profile/session/message identity and content hash. Re-running unchanged input must be idempotent.
3. **Hindsight semantic sync** for eligible conversation content with provenance: source, Hermes profile, session id, message id/sequence, role, timestamp, and content hash. Use the existing Hindsight provider/bank binding and APIs discovered from the repo. Do not invent endpoint shapes.
4. **Secret/PII guard** before any Hindsight write. Default behavior must refuse credential-shaped content or redact only under an explicit flag/policy. Never leak values in receipts, audit output, tests, or reports.
5. **Promotion hooks** so explicit decisions can become MindOS facts/handoffs/tasks, but do not turn every message into execution truth. Preserve the distinction between raw session evidence, semantic memory, and structured control-plane state.
6. **Near-real-time operation**: provide a command or safe event hook that can ingest the current/changed Hermes session on demand, plus keep the existing scheduled batch as fallback. Do not assume the 5-minute batch is sufficient for live operator direction.

## Acceptance tests

- Disposable Hermes-style JSONL fixtures ingest successfully and are searchable through MindOS session search/context.
- A live-conversation sentinel fixture can be ingested and its provenance is visible in the resulting MindOS/Hindsight record without exposing source secrets.
- Changed messages re-index atomically; unchanged messages are skipped; re-runs produce no duplicates.
- Secret-shaped fixture refuses by default and redacts only with explicit option; raw secret value is absent from all stored/output artifacts.
- Hindsight unavailable/degraded path is honest: local cache remains correct, receipt reports semantic sync pending, no invented success.
- Current/future validity and role distinctions remain intact; user/assistant messages are distinguishable from tool output.
- Context/recall packs can include related sessions with bounded budget and deterministic digest behavior.
- Run py_compile, full verify.py, targeted bridge tests, diff check, secret/PII scan, and any local Hindsight fixture/integration tests available.
- Produce a concise integration receipt documenting exact command, source path, Hindsight bank, provenance fields, latency/trigger behavior, and limitations.

## Do not claim

Do not claim every Telegram message is automatically ingested until a real end-to-end sentinel proves it. Do not change the live selector or deploy. Commit the implementation and tests only in this worktree; return a merge-ready receipt for review.
