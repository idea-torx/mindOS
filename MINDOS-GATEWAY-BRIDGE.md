# Wire MindOS conversation bridge into Hermes gateway

Integrate the already-tested `mindos_bridge.py` into the Hermes gateway in this isolated worktree. Do not touch the live gateway, `~/.hermes`, rollback, GitHub, or other worktrees until the implementation and tests pass.

## Goal

New and changed Hermes conversation/session records must be eligible for near-real-time MindOS ingestion without requiring manual commands. Preserve the architecture:

- raw conversation → MindOS sessions/session_messages cache
- semantic sync → honest Hindsight pending/export state because current provider adapter is GET-only
- explicit promotion → MindOS notes/facts/tasks
- execution truth remains SQLite tasks/receipts/handoffs, never raw chat

## Integration requirements

1. Inspect Hermes gateway lifecycle and existing hooks/events. Use the narrowest provider-neutral integration point. Do not inject synthetic messages, mutate prompt history, or break prompt caching/message alternation.
2. Add a profile-safe configurable bridge setting in `config.yaml` or existing gateway config conventions. Do not add a non-secret HERMES_* environment variable. Default must be safe/off or a clearly documented opt-in mode until the live installation explicitly enables it.
3. Provide an async/non-blocking or bounded background trigger so a slow session ingest/Hindsight probe cannot delay the user-facing reply. The existing `watch`/batch fallback must remain available.
4. Respect the active profile's `HERMES_HOME`; never hardcode ~/.hermes. Use disposable test homes.
5. Redact/refuse credential-shaped content before any cache/export write according to the bridge contract. Never log message contents.
6. Add integration tests proving: gateway message/session event triggers bridge scheduling, reply path is not delayed by bridge failure, duplicate events are idempotent, profile paths are isolated, disabled configuration does not ingest, and enabled configuration ingests a disposable sentinel.
7. Document setup/enable/disable, latency expectations, failure behavior, and how to verify a sentinel. Do not claim Hindsight semantic write success when only pending export is available.

## Gates

Run the bridge suite, Hermes targeted gateway tests, full MindOS verify.py, py_compile, diff check, secret/PII scan, and a disposable end-to-end gateway sentinel. Commit with Leo Felix <leo@matteblack.io>. Return exact files/config, enable command, tests, and install steps. Do not modify live state in this worktree.
