# Managed intra-bot communication — design (facts / gaps / smallest safe slice)

Worktree: `/Users/leofelix/Documents/mindos-intra-bot-communication` (branch
`feat/managed-intra-bot-communication`). Live `~/.hermes`, rollback, gateway,
GitHub and other worktrees were not modified. The only live interaction was a
read-only protocol probe (`hermes peer --help`, `hermes peer list`).

## Facts (verified)

### Hermes managed bot-to-bot protocol (read-only probe)

- `hermes peer` supports `{add,set,list,remove,dm}`. Peers are registered
  gateways (`--url http://host:port`) whose `API_SERVER_KEY` is stored locally
  as a **credential in `~/.hermes/.env`** — the key value must never enter
  MindOS storage.
- `hermes peer dm <peer>[/<agent>] "..."` delivers into the remote agent's
  **canonical Bot Chat** and prints the reply; `<peer>/<agent>` addresses a
  named profile on a multiplexed peer. Exit codes: 0 ok, 1 delivery/peer
  error, 2 usage error.
- Live probe result: **no peers registered** on this machine today, so there
  is no live roster to migrate; the MindOS layer can define its own allowlist
  bootstrap without touching live state.

### MindOS control plane (code audit)

- Home resolution: `HERMES_AUTOPILOT_HOME` env > selector > default
  (`autopilot._resolve_home`). All disposable tests use temp homes.
- Schema (`autopilot.SCHEMA`): `tasks`, `heartbeats`, `receipts` (task-scoped),
  `audit_events` (hash-chained via `ap.audit`), `task_deps`, `notes`,
  `handoffs`, `sessions`/`session_messages` (ingestion cache),
  `facts` (temporal graph) + FTS5 external-content indexes.
- Secret guard ladder: `ap._secret_findings` (kind-only reporting),
  `ap._redact_secrets` (`[REDACTED:<kind>]`), refuse-by-default everywhere;
  values never appear in errors, audit payloads, exports or receipts.
- Bridge (`mindos_bridge.py`): incremental idempotent session cache keyed by
  content hash; `bridge_hindsight_ledger` honest pending/exported/failed sync
  states; GET-only Hindsight probe; explicit `promote` hook.
- SQLite adapter (`mindos_sqlite_adapter.py`): strictly read-only
  (`mode=ro`, `PRAGMA query_only=ON`); only user/assistant conversational rows
  cached; tool output skipped.
- Context pack (`mindos_context_pack.py`): bounded byte/item budgets,
  deterministic digest, exact-match profile scoping (cross-profile sessions
  can never be packed), per-section honest statuses.
- Autonomy/model declarations exist for tasks: `AUTONOMY_LEVELS = L0/L1/L2`,
  `_valid_model_binding` regex, grants with expiry (`declare`/grant flow).

## Gaps

1. **No bot-message envelope.** A bot DM ingested through the session bridge
   would be cached as an ordinary user/assistant turn — indistinguishable
   from human chat, and eligible for promotion/context like user evidence.
2. **No peer allowlist or capability epochs in MindOS.** Nothing records
   which bots may talk, with which capabilities, until when, at which epoch.
3. **No idempotent delivery receipts for bot messages.** The `receipts`
   table is task-scoped (`REFERENCES tasks(id)`), so it cannot carry
   accepted/rejected/duplicate/expired/failed lifecycle for envelopes.
4. **No loop/replay budgets.** Two misconfigured peers could ping-pong
   forever; nothing caps correlation-chain length or refuses replays.
5. **No cross-harness neutrality.** Everything upstream assumes Hermes
   conventions; DSH/OpenCode/Codex/Claude participation has no defined shape.
6. **No autonomy/model/provider provenance for bot-originated actions.**
7. **Context pack has no bot-chat section**, so even correctly-ingested bot
   coordination would not reach session-start context under a budget.
8. Peer credentials live in `~/.hermes/.env`; any ingestion path must keep
   key material out of the brain (existing secret ladder covers shapes, but
   bot envelopes add a new write path that must route through it).

## Smallest safe vertical slice (implemented)

New module `mindos_botmail.py` (+ `tests_botmail.py`), reusing the shared
home, audit chain, and secret guard:

- **Envelope v1** (`mindos-bot-envelope-v1`): `message_id`, `correlation_id`,
  `in_reply_to`, sender (`bot`, `harness`, `profile`), recipient (`bot`,
  `profile`), `direction`, `capability_epoch`, `timestamp`, `content_class`
  (`bot_chat|user_relay|handoff|task_receipt`), `content`, optional
  `autonomy_level` (L0–L2), `model_binding`, `provider`, `provenance`.
  Parser accepts the canonical nested form, a flat single-object variant, and
  Hermes-style dm text (`Message from 🤖 name (@handle): body`) so DSH /
  OpenCode / Codex / Claude / future agents participate through the envelope,
  not Hermes-specific assumptions.
- **Peer registry** (`bot_peers`): allowlist keyed by `harness:bot`, with
  capability list, monotone `capability_epoch`, `allowed_profiles`,
  `expires_at`, revocation. Ingress requires: peer registered & not revoked,
  epoch match, capability covers the content class, source profile allowed,
  expiry honored (`expired` status, not silent drop).
- **Idempotent ingest + receipts** (`bot_messages`, `bot_receipts`):
  `message_id` primary key makes replays `duplicate` (never re-stored);
  one receipt row per (message,status) with attempt counting; statuses
  `accepted|rejected|duplicate|expired|failed` with kind-only reasons.
- **Loop/replay budgets**: self-addressed envelopes refused; correlation
  chain capped (`--max-chain`, default 16); distinct-message replay of the
  same (peer, recipient, correlation, content hash) inside the chain window
  refused (`replay_budget`).
- **Redaction/secret guard before any write**: refuse default, `--redact`
  stores `[REDACTED:*]`, `--allow-secret` audited override; raw values never
  in receipts, audit payloads, errors or test output.
- **Separation of channels**: `content_class` distinguishes bot chat, user
  relay, handoff and task receipt; bot mail lives outside
  `sessions/session_messages`, so promotion semantics stay explicit.
- **Autonomy/model/provider provenance** validated on ingress
  (`AUTONOMY_LEVELS`, `_valid_model_binding`) and stored on the envelope row.
- **Bounded context inclusion**: `context` command plus a new `bot_chat`
  section in `mindos_context_pack.build_pack` — profile-exact scoping
  (`target_profile` match), secret-guard ladder, byte/item budget, included
  in the deterministic digest; no cross-profile leakage.
- **Fail-open reply path**: unexpected internal errors during ingest produce
  a durable `failed` receipt + hash-chained audit event and a structured
  `ok:false` result instead of an unhandled crash; guard refusals still exit
  non-zero honestly.

### Explicitly deferred (risks / remaining for live installation)

- No network delivery in this slice: `hermes peer dm` integration, retries,
  and remote reply capture are a follow-on (this slice is ingest/coordinate
  only — nothing sends messages from the worker).
- No Hindsight semantic sync for bot chat yet (ledger pattern exists to copy).
- Capability epochs are local to this MindOS home; reconciling them with
  gateway-side rosters needs the live installation step.
- Live end-to-end sentinel against real peers (currently zero registered)
  remains open; all proofs here are disposable-fixture based.

## Gates run

py_compile; `tests_bridge.py`; `tests_sqlite_adapter.py`;
`tests_gateway_bridge.py`; `tests_context_injection.py`;
`tests_context_integration.py`; `tests_botmail.py`; full `verify.py`;
secret/PII scan over new/changed files; `git diff --check`.
