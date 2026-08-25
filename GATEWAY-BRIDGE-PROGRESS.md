# MindOS gateway bridge — progress log

Worktree: ~/Documents/mindos-gateway-bridge (branch feat/mindos-gateway-bridge)
Contract: MINDOS-GATEWAY-BRIDGE.md. Live ~/.hermes, live gateway, rollback, GitHub,
other worktrees: untouched.

## Log

- [g0] Read contract. Inventoried existing bridge: mindos_bridge.py already provides
  sync/watch/export/hindsight-check/promote with secret guard, idempotent re-index,
  honest pending/export ledger; tests_bridge.py passes on disposable homes.
- [g0] Inspected Hermes gateway lifecycle + hooks in ~/.hermes/hermes-agent source:
  - agent/turn_finalizer.py fires plugin hook `on_session_end` at the end of EVERY
    run_conversation turn (gateway included), kwargs: session_id, task_id, turn_id,
    completed, interrupted, model, platform.
  - agent/shell_hooks.py bridges config `hooks:` block onto the same invoke_hook
    dispatcher — zero core changes needed. Wire protocol: JSON on stdin
    {hook_event_name, session_id, extra{...}}, stdout ignored for observers.
  - Hooks fail OPEN on non-blocking events: a slow/broken hook logs a warning and the
    reply path is unaffected. Registration is idempotent CLI+gateway via
    register_from_config(load_config()).
  - SHELL_UNSUPPORTED_HOOKS only excludes transform_api_error_classification;
    `on_session_end` IS shell-hookable (VALID_HOOKS includes it).
- [g1] Integration point chosen (narrowest provider-neutral): a config-gated
  `hooks.on_session_end` shell entry that spawns `mindos_gateway_hook.py`
  fire-and-forget. The hook script:
  - reads ONLY session metadata from stdin (session_id/platform/completed) — never
    message contents; never logs content;
  - resolves MindOS home strictly from HERMES_AUTOPILOT_HOME / selector / default via
    autopilot's own _resolve_home (profile-safe, no hardcoded ~/.hermes writes);
  - runs one bounded incremental `sync --apply` pass over the configured store root +
    a bounded `export` manifest pass (GET-only Hindsight honesty preserved);
  - disabled by default: absent `mindos_bridge.enabled: true` in config.yaml the
    hook exits 0 instantly without touching anything.
  Non-blocking guarantee: the hook itself daemonizes (double-fork style detach) so
  even a hung ingest cannot delay the user-facing reply beyond hook timeout; the
  batch `watch` fallback remains available unchanged.
- [g2] Implemented mindos_gateway_hook.py + tests_gateway_bridge.py.
  Fixes found by running gates:
  - bridge export requires concrete --out; default now resolved in the parent via
    autopilot.ROOT (worker executes prebuilt commands only).
  - _as_bool(None, True) bug: missing config key ignored its default, silently
    disabling export_on_sync; fixed to honor default on None.
- [g2] Gates ALL PASS:
  - py_compile x7 (autopilot, ops, verify, mindos_bridge, mindos_gateway_hook,
    both test suites)
  - tests_bridge.py full PASS (existing bridge behavior unregressed)
  - tests_gateway_bridge.py PASS: disabled=no-op, enabled+root ingests sentinel,
    export provenance manifest under resolved home, duplicate events idempotent,
    HERMES_HOME/HERMES_AUTOPILOT_HOME isolation, broken root returns instantly,
    secret guard refuse/redact end-to-end (raw value absent from cache+manifest)
  - verify.py full PASS ("autopilot verification: PASS")
  - git tracked diff clean (only new files); secret/PII scan clean on new files
    (only documented AKIA…EXAMPLE fixture inside test assertions, matching the
    existing tests_bridge.py convention).
- [g3] Committed on feat/mindos-gateway-bridge as Leo Felix <leo@matteblack.io>.
  Live install NOT performed from this worktree per contract.
- [g4] Session-start context integration (mindos-context-injection worktree):
  inspected the Hermes session lifecycle — `on_session_start`
  (agent/conversation_loop.py) is observer-only (return value discarded, no
  context channel); the host's documented injection path is `pre_llm_call`
  results shaped {"context": ...} which agent/turn_context.py injects
  ephemerally into the first turn's user message. Added an opt-in
  `context_pack: true` branch to mindos_gateway_hook.py: on first turn only,
  run mindos_context_pack.py session-pack under a wall-clock cap and print
  {"context": <deterministic markdown>} for the host to inject once at
  session start. No writes, no synthetic messages after turn 1, no system-
  prompt rewriting; HERMES_MINDOS_CONTEXT off switch and profile isolation
  preserved end-to-end. tests_context_integration.py 9/9 PASS alongside all
  existing gates.

