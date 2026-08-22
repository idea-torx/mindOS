# MindOS session-start context injection — design

Contract: CONTEXT-INJECTION-CONTRACT.md (isolated worktree; live `~/.hermes/mindos`,
rollback home, gateway config, GitHub, and other worktrees are never touched).

## Smallest architecture

One new tool, `mindos_context_pack.py`, in the same shape as the existing
bridge tools (`mindos_bridge.py`, `mindos_sqlite_adapter.py`): a standalone
CLI that reuses `autopilot.py` for home resolution (`HERMES_AUTOPILOT_HOME`
env > reversible selector > default), schema/conn, secret guard
(`_secret_findings` / `_redact_secrets`) and the Hindsight bank adapter. No
runtime logic moves out of `autopilot.py`/`ops.py`; nothing is installed or
deployed.

A session-start context pack is a **bounded, sealed JSON document** generated
once when a new Hermes session starts — never synthetic per-turn user
messages, never system-prompt rewriting, and prompt caching is preserved by
construction (the pack enters as ordinary session-start context the host
already handles; this tool only produces the file).

```
Hermes session start
        │  (profile, project, optional focus query)
        ▼
mindos_context_pack.py session-pack
        │  reads-only from MindOS home state.db + Hindsight bank file
        ▼
sealed pack JSON (provenance, generated_at, freshness, digest)
        → stdout and/or --out file; verify-pack recomputes freshness later
```

## Data flow — sections (each bounded, ordered, flag-gated)

Sources reuse what already exists; nothing new is invented:

1. `temporal_facts` — currently-valid rows from the `facts` table
   (valid_until empty or in the future), newest first.
2. `handoffs` — latest non-superseded `handoffs` rows with task provenance.
3. `receipts` — latest receipts across tasks (id, kind, task_id, created_at).
4. `session_context` — ingested session-message snippets via the same
   retrieval discipline as `_related_session_candidates` (FTS5 with LIKE
   fallback), restricted to the requested Hermes **profile** when one is
   given; cross-profile content can never be packed.
5. `semantic` — Hindsight bank recall (read-only, deterministic ordering,
   graceful no-op when no bank exists) via `autopilot._hindsight_candidates`-style
   matching over the pack query text.

Every item carries its own provenance block (source table/engine, ids,
timestamps, profile where applicable). Every section reports
`status: ok | empty | unavailable | refused-secret` so degradation is honest:
a missing DB or absent bank yields an explicitly `unavailable` empty section —
never fabricated context.

## Boundedness, safety, determinism

- Budget: `--max-bytes` (default 4096) and per-section/max-total
  `--max-items`. Items are packed in fixed order at ≤ budget; overflow sets
  `truncated: true` plus per-section counts (`requested/matched/packed`).
- Profile safety: session items require `sessions.profile == --profile`
  whenever a profile is supplied; facts/handoffs/receipts are fleet-level
  but carry full provenance. No cross-profile session content can appear.
- Secret/PII guard ladder before packing any text: default = drop the item
  and count it under `refused_secret_items` (fail closed); `--redact` packs
  `[REDACTED:<kind>]` copies; audited `--allow-secret` passes verbatim.
  Secret values never reach stdout, files, digests, or errors.
- Digest: `digest = sha256(json(pack minus {generated_at, digest}))`,
  sort_keys, tight separators — identical state ⇒ identical digest
  (idempotent regeneration). `generated_at` sits outside the digest exactly
  like the house protocol-seal format.
- Freshness/staleness: the envelope records `max_age_hours`;
  `verify-pack` re-runs generation against current state and compares
  digests: `fresh`, `stale` (state moved; `current_digest` returned for
  recompute), or `aged` (past max age). Exit code stays 0 either way.

## Opt-out / disable

`HERMES_MINDOS_CONTEXT=off|0|false|no` (checked first) disables emission:
the command prints `{"enabled": false}` and exits 0 without reading sources.
Absent the env kill-switch the tool is opt-in by invocation — nothing calls
it unless a session-start hook or operator does.

## Live sentinel (proof without touching live runtime)

`mindos_context_pack.py sentinel` builds an entirely disposable fixture
world under a temp dir (MindOS home via `HERMES_AUTOPILOT_HOME`, synthetic
JSONL store, two profiles, Hindsight bank via `HERMES_HINDSIGHT_HOME`),
ingests through the real bridge, then proves:

1. a new-session pack contains the injected context with provenance;
2. regeneration on unchanged state is byte-stable (same digest);
3. profile B's pack excludes profile A's session content;
4. credential-shaped content is excluded by default and redacted under
   `--redact`, with no raw value anywhere;
5. the disable env yields `{"enabled": false}`;
6. unavailable DB/bank degrade to honest `unavailable` statuses.

Zero writes outside the temp dir; live homes stay read-only by construction.
