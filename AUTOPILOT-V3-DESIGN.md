# Autopilot v3 continuation — design (audit-grounded)

Contract: AUTOPILOT-V3-CONTRACT.md (isolated worktree `feat/autopilot-v3-continuation`;
live `~/.hermes`, rollback home, gateway, GitHub, and other worktrees are never
touched; nothing is deployed).

## Ground truth from the audit

The STABILITY.md audit round established the properties this design must not
break, and the existing substrate already provides every primitive the v3
continuation loop needs:

- **Guarded sweeps** (`ops.py recover`, `escalate`): snapshot SELECT → guarded
  UPDATE with `skipped` reporting; deterministic order. A nanny tick can reuse
  them verbatim and is double-run safe by construction.
- **Typed findings** (`ops.py sense`): doctor / verify-chain / recall-stale /
  unverified-completions mapped onto content-hashed findings carrying
  `suggested_repair`.
- **Repairs as normal queue tasks** (`ops.py repair`): create → claim (lease)
  → run → sealed receipt of the required kind → complete citing it; breaker
  facts disable looping playbooks; blast-radius tiers gate on human approval.
- **Temporal fact graph**: validity-windowed triples are the natural home for
  *autonomy grants* — a grant that expires is exactly what
  `_fact_live_sql()` already models.
- **Hash-chained audit ledger**: every state change leaves provenance;
  refusals are audited on their own connection so rolled-back transactions
  still leave a trail.

## P0 — autonomy/model declarations, grant facts, stamping, enforcement

Additive only; single-file shape preserved (runtime logic stays in
`autopilot.py`; tests in `verify.py`).

1. **Task metadata columns** (SCHEMA + `_migrate`, all defaulted, additive):
   - `autonomy_level TEXT DEFAULT ''` — vocabulary `L0` (observe/report),
     `L1` (bounded execute under human seams), `L2` (unattended continuation).
     Empty = legacy task, no autonomy semantics.
   - `model_binding TEXT DEFAULT ''` — the exact provider/model allowed to
     execute this task (e.g. `opencode/x-preview-f-free`). Validated to a
     safe token charset (no quotes/whitespace/newlines), ≤128 chars.
   - `recap TEXT DEFAULT ''` — latest completion recap (bounded prose stamped
     into the audited `completed` event).
2. **`autopilot.py declare <task-id>`** — one command stamps all grant facts:
   - `--model <binding>` required; `--autonomy-level L0|L1|L2` (default L1);
     `--granted-by <name>` required (the human who explicitly granted —
     refusal without it is fail-closed); `--grant-hours N` (default 24,
     >0) bounds the grant's validity window.
   - Writes: task row fields; a temporal fact
     (`subject='autonomy:<task-id>'`, `predicate='level-granted'`,
     `object=<level>`, `source=<granted-by>`, windowed); an audited
     `autonomy_declared` event carrying level, model binding, granter,
     grant fact id, and `valid_until`. Re-declaring supersedes by inserting
     a fresh windowed row — history is never rewritten.
3. **Transition enforcement at claim time** (`claim` and the
   `next --claim` filter stage): a task with a declared autonomy level may
   only be claimed while a **live** grant fact exists whose level is
   `>=` the declared level. Missing/expired → refuse
   `claim_refused_autonomy {reason: no_live_grant}`; lower-level grant →
   `{reason: grant_below_declared_level}`. Refusals are audited on their own
   connection (same pattern as seam/policy refusals). `--force` remains the
   deliberate override and records `autonomy_override` in the claimed event;
   dispatch (`next --claim`) skips ungranted candidates with explain reason
   `autonomy_grant_missing` instead of failing after the pick.
4. **Receipt and audit stamping**: `complete --recap "<text>"` stores the
   recap on the task and stamps it into the hash-chained `completed` event
   next to the evidence receipts it already cites — the recap is metadata
   about verified execution, never a substitute for receipts.

## P1 — bounded `ops.py nanny` tick (no daemon)

One invocation = one bounded tick over existing primitives, in order:

```
nanny tick ──► recover      (guarded stale-lease sweep, backoff respected)
        ├────► sense        (typed findings, content-hashed)
        ├────► repair       (≤ --max-repairs tier-0 playbooks whose
        │                    suggested_repair matches a finding; breakers,
        │                    leases, receipts, learning all inherited)
        ├────► escalate     (overdue SLA sweep, guarded bumps)
        └────► audit+report (single JSON doc; audited nanny_tick event)
```

Boundedness:

- `--max-repairs N` (default 2) caps mutation work per tick; there is no
  loop, no sleep, no daemon. A cron/operator re-invocation starts a fresh
  tick.
- Double-run safe: recover/escalate are guarded sweeps; repairs run as
  deterministically-idempotent leased tasks, so a concurrent second tick
  gets a clean refusal recorded as `skipped`, not duplicate work.
- `--dry-run` previews recover/escalate plans and lists would-repair
  findings without mutating anything.

## HUMAN-IMPULSE-SUGGESTIONS

The contract asks: what safe additions make continuation feel more human
rather than mechanical? Each suggestion below was reviewed against the
deterministic / auditable / privacy-safe / bounded / explicit-human-seam
bar. Split into **implemented now** vs **deferred**, with tradeoffs.

### Implemented in this slice

1. **Bounded impulse states** — every nanny tick reports exactly one
   `state` from a closed vocabulary derived deterministically from tick
   results:
   - `all_clear` — nothing recovered, no findings open;
   - `working` — this tick took action (recoveries/repairs executed);
   - `hit_snag` — findings remain after the bounded repair budget;
   - `decision_needed` — a circuit breaker tripped or a requires-user
     playbook is suggested: a concrete human decision is being asked for.
   Tradeoff: four states cannot express nuance, but they cannot spam,
   drift, or fabricate either — the vocabulary is closed and testable.
2. **Momentum memory (anti-spam)** — each real tick audits a `nanny_tick`
   event carrying its open finding hashes; the next tick diffs against it
   and reports previously-seen findings only compactly (`carried_over`
   hashes), spending detail (`new_findings`) on what changed. A finding
   that persists does not re-narrate itself every tick. Tradeoff: the
   previous-tick lookup adds one indexed audit read; events recorded
   before this feature simply start a new memory (no carried_over).
3. **Durable next-intent in recaps** — `complete --recap` makes the
   "where this leaves off" sentence durable, provenance-stamped data on
   the task rather than chat text, so the next session resumes from intent,
   not archaeology. Tradeoff: bounded free text could go stale — but it is
   stamped beside receipts, which stay execution truth.
4. **Context-aware progress language** — the tick report separates
   `actions_taken` from `still_open` and names the exact decision needed
   (`decision`: breaker fact id / investigate task id), so "I'm still
   working" and "here is the decision I need" are grounded in evidence ids
   instead of adjectives.

### Deferred (recorded, not implemented)

5. **Adaptive check-in cadence** — vary tick frequency by fleet momentum
   (quiet fleet → longer intervals; active repairs → shorter). Deferred:
   needs a scheduler surface, and the contract forbids a daemon; a cron
   policy file could carry it later as data.
6. **Tone/style rendering layer** — render the same JSON tick as
   situation-appropriate prose ("picked up where we left off", "hit a snag,
   here's the fork"). Deferred: rendering is presentation; keeping it out
   of the runtime preserves byte-determinism and keeps the JSON the single
   source of truth. A pure function over the tick doc can be added later
   without touching the runtime path.
7. **Recap supersession chains** — reuse note-style temporal supersession
   for multi-recap tasks. Deferred: P2 owns sealed recaps; adding chains
   now would pre-empt that design.
8. **Model-binding observability in dashboards** — surface declared model
   bindings per running task in `dashboard`/`metrics`. Deferred: trivially
   additive later; not needed for the enforcement slice and every extra
   default-output field risks breaking byte-compat expectations elsewhere.

## Gates

py_compile over all Python files; bridge/SQLite/gateway/context-injection
test suites; full `verify.py` (including two new focused cases: autonomy
declaration/grant expiry/enforcement, and nanny boundedness/double-run/
impulse states); context-pack sentinel; secret/PII scan; git diff review.
Commit as Leo Felix only after everything is green. No deploy.
