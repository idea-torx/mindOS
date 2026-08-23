# Autopilot v3 self-improvement — design (audit-grounded)

Contract: AUTOPILOT-V3-SELF-IMPROVEMENT-CONTRACT.md (isolated worktree
`feat/autopilot-v3-self-improvement`; live `~/.hermes`, rollback home,
gateway, GitHub, and other worktrees are never touched; nothing is deployed).
Agent of choice: OpenCode Zen OX (`opencode/x-preview-f-free`).

## Facts (audit ground truth)

Everything below was verified against the code at HEAD (`153e05f`):

- **P0/P1 continuation already merged** (`825811c`): `declare` stamps
  `autonomy_level`/`model_binding` plus windowed grant facts
  (`subject=autonomy:<task-id>`, `predicate=level-granted`,
  `source=<human granter>`); claim time refuses `claim_refused_autonomy`
  with `no_live_grant` / `grant_below_declared_level` (audited on its own
  connection); `complete --recap` stamps the recap beside evidence receipts;
  `ops.py nanny` is one bounded recover→sense→tier-0-repair→escalate tick
  reporting the closed four-state impulse vocabulary
  (`all_clear/working/hit_snag/decision_needed`) with momentum memory
  (`carried_over`) diffed against the previous audited `nanny_tick` event.
- **Heartbeat exists but is lease plumbing only** (autopilot.py:4287): renews
  the lease +15 min, upserts `heartbeats(task_id,owner,state,at,note)`, audits
  `heartbeat {owner,lease_expires_at}`. It carries **no** last meaningful
  action, next intent, progress state, evidence links, or stall deadline.
- **Failure handling has budget but no taxonomy** (`fail`, autopilot.py:3358):
  retry budget/backoff (`retry_count`, `recover_after`), terminal
  `task_failed_terminal` with `dependents_stranded`. The single free-text
  `reason` cannot distinguish transient vs infra vs defect, and nothing ever
  proposes or executes a *next correction* from durable evidence.
- **Receipts are sealed but execution-blind** (autopilot.py:4307): atomic
  0600 file + sha256 in `receipts.file_hash`; kinds are free-form
  (`approval`, `repair`, `verification` observed); **no** payload field today
  records harness, provider/model, session, workspace, timeout, or outcome.
- **No runner concept anywhere**: zero occurrences of runner/harness/
  workspace/provider abstractions; "provider-neutral" exists only as prose.
- **Correction children do not exist**: the only spawned task in the system is
  the breaker's P0 `investigate-*` task (ops.py:3527). No max-attempts bound
  beyond the breaker window (`_repair_count`, break-after 3).
- **Nanny sense has four finding sources** (ops.py:3443): doctor, verify-chain,
  recall-stale, unverified-completions — all content-hashed via
  `_make_finding`. Stalled activity is invisible to it.
- **No soak machinery exists**; every test uses disposable
  `HERMES_AUTOPILOT_HOME` temp homes (16 registered cases in `verify.py`).

## Gaps

1. A run performed by *any* harness leaves no provider-neutral, sealed,
   cross-checkable record tied to the task.
2. "Work silently disappeared" remains possible: a leased task whose holder
   dies mid-work looks identical to one making progress until the lease
   expires; there is no declared stall deadline to detect stalls earlier.
3. Terminal failures carry no cause, so continuation policy (retry vs repair
   vs rethink) cannot be selected deterministically, and no bounded
   correction plan exists.
4. Autonomy grants die with their task: a correction child would face
   claim-time enforcement with no live grant of its own.

## Proposals (implemented in this slice)

All additive, single-file shape preserved (runtime logic in `autopilot.py`;
fleet sweeps in `ops.py`; tests in `verify.py`). No daemon, no queue service.

### 1. Provider-neutral runner receipt — `autopilot.py run-receipt`

One common `runner/v1` receipt schema for every harness (Hermes, DSH,
OpenCode, Codex, Claude Code, future):

```
{schema: "runner/v1", harness, model, session, workspace, outcome,
 timeout_seconds?, capabilities[], started_at?, finished_at?}
```

- Sealed exactly like every receipt: atomic 0600 file, sha256 in
  `file_hash`, row linked via `tasks.last_receipt`; kind fixed `run`.
- Validated fail-closed: `harness` = lowercase tag-charset token (open set —
  the protocol is harness-neutral by construction); `model` = the existing
  model-binding validator (same regex as `declare`); `outcome` ∈
  `ran|passed|failed`; `capabilities` = validated tags; strings ≤256 chars;
  `timeout_seconds` > 0.
- Audited `run_receipt {receipt_id, kind, harness, model, outcome}`.
- Evidence receipts stay the truth source; the run receipt describes *who ran
  what, where, and how it ended* — metadata about verified execution.

### 2. Activity report contract — extended `heartbeat`

Additive optional flags turn the existing lease renewal into an activity
report: `--action` (last meaningful action), `--intent` (next intent),
`--progress-state working|blocked|waiting_human|done`, `--evidence <rid>`
(repeatable; each id must be an existing receipt **on this task** — evidence
is never invented), `--stall-deadline` (ISO or `+Nm`/`+Nh` relative).
Stored in five new defaulted `heartbeats` columns via `_migrate`; the audit
event gains keys only when provided (legacy behavior byte-compatible).

### 3. Stalled-activity detection — `activity_stalled` finding

`ops.py sense` gains one read-only sweep: `running` tasks whose heartbeat
declared a `stall_deadline` that has passed become typed, content-hashed
`activity_stalled` findings (severity P2, no `suggested_repair` — recovery of
a *stalled* holder is the existing guarded `recover` sweep's job once the
lease truly expires; the finding exists so humans and ticks see the stall
*before* expiry). Findings flow into the existing nanny impulse pipeline:
they surface as `hit_snag` with momentum-memory `carried_over` semantics.

### 4. Failure-state continuation — `fail --cause` + bounded correction child

- New defaulted task column `failure_cause`; `fail --cause
  transient|infra|defect` (default empty = legacy). Cause lands on the task
  row and both audit payloads.
- Deterministic strategy selection: `transient` → existing backoff retry;
  `infra` → same retry with the backoff base doubled (environment, not code,
  is broken — back off harder); `defect` → retry budget still applies first,
  and on the *terminal* failure of an autonomy-declared task (L1+) the loop
  plans the next safe correction.
- Bounded correction planning (durable evidence, no hidden reasoning): a
  terminal `defect` failure creates one correction child task carrying the
  parent's project, tags, model binding, declared level, the failure reason,
  attempt count, prior recap, and the parent's `run` receipt ids as
  provenance. Lineage is data: facts `correction-of` (immediate parent) and
  `correction-root` (family root) let any harness reconstruct the chain.
- Grant inheritance is copy-not-mint: the child's `autonomy:<child>`
  subject receives verbatim copies of the parent's *currently-live* grant
  facts (same human `source`, same validity window), audited as
  `autonomy_grant_inherited` citing the parent fact id. No live parent grant
  → child is created **without** a grant and simply waits at the existing
  fail-closed seam (`autonomy_grant_missing` at dispatch). Authority is
  never expanded — only inherited while the human's window is still open.
- Hard bound `MAX_CORRECTION_ATTEMPTS = 3` per family root (the original
  attempt counts as 1): the third terminal failure refuses further children
  with an audited `correction_refused_max_attempts` — the loop cannot
  recurse forever, and refusal is the explicit escalation to a human.

### 5. Bounded self-improvement tick

No new tick: the existing nanny already owns recover/sense/repair/escalate
with caps, breakers, leases, and double-run safety. This slice feeds it two
new signals (stall findings; correction children dispatching through the
existing grant-enforced claim path) and changes nothing about its
boundedness.

### Fixtures

New `verify.py` case `runner_activity_and_correction_continuation` on a
disposable home: cross-harness run receipts (identical schema, different
harness tokens) + invalid-payload refusals; activity heartbeat with evidence
validation + stall detection via `nanny --dry-run` (fresh → hit_snag,
second tick → carried_over, healthy deadline → no finding); full correction
chain parent→child→grandchild→refusal-at-cap under a live grant; no-child
path without a declaration; grant-less child path with an expired grant
(child waits at the dispatch seam); `verify-chain` green throughout.

## Rejected ideas

- **Runner daemon / queue service** — contract forbids it; cron + bounded
  commands cover the need.
- **Hidden chain-of-thought critique** — planning must cite durable evidence
  ids (receipts, recaps, failure reasons); free-form hidden deliberation is
  unauditable by design here.
- **Auto-minting fresh grants for correction children** — would expand
  authority beyond Leo's explicit window; mirroring live windows keeps the
  human seam exact.
- **Auto-executing repairs for `activity_stalled`** — a stalled *holder*
  might still be alive (slow tool call); killing work on a finding would be
  wrong. Recovery stays with the guarded expired-lease sweep; the finding
  only surfaces the stall.
- **Closed harness enum** — an open validated token set stays
  harness-neutral; a closed enum would need a release per new harness.
- **Per-task adaptive cadence** — deferred below; needs a scheduler surface
  the contract forbids building now.

## HUMAN-IMPULSE-AND-AUTONOMY-SUGGESTIONS

### Implemented now

1. **Declared stall deadlines** — a holder says how long silence is healthy
   (`--stall-deadline +30m`); the fleet reports `hit_snag` when it lies
   silent past its own word, instead of everyone discovering the stall after
   lease expiry. Tradeoff: requires holders to opt in; legacy heartbeats are
   simply never flagged (fail-open to today's behavior, never false alarms).
2. **Evidence-linked activity language** — "still working" is grounded:
   heartbeat carries the last meaningful action, the concrete next intent,
   and receipt ids proving the claim. Tradeoff: slightly heavier heartbeat
   calls; all fields optional so mechanical loops stay cheap.
3. **Corrections feel like handing off, not retrying blind** — a correction
   child literally inherits the failure story (reason, attempts, recap, run
   receipts) in its description, so the next session resumes from intent and
   evidence rather than archaeology. Tradeoff: one more task in the fleet;
   capped at 3 attempts per family so it cannot pile up.
4. **Refusal as communication** — hitting the correction cap or missing a
   grant produces named, audited refusals (`correction_refused_max_attempts`,
   `autonomy_grant_missing`) rather than silent stops: the decision request
   a human sees is always a concrete fact id away.

### Deferred

5. **Adaptive nanny cadence** (vary tick interval by momentum) — needs a
   scheduler surface; could land later as a cron policy file (data, not
   daemon).
6. **Tone/rendering layer over tick JSON** — presentation belongs outside
   the runtime; a pure function over the tick doc can be added without
   touching the audited path.
7. **Model escalation on repeated defects** (retry the family's correction on
   a stronger bound model) — needs a policy mapping cause→model ladder and
   interacts with blast-radius gating; deferred until a second real workload
   justifies the matrix.
8. **Soak harness** (long-running replayable fixture loops over many
   simulated days) — the new fixture is replayable and disposable, but a
   true soak runner is infrastructure the contract defers.
9. **Cross-harness capability negotiation** (dispatch filtered by runner
   capabilities) — capabilities are recorded on run receipts now; acting on
   them at dispatch is a larger scheduling change.

## Gates

py_compile over all Python files; bridge/SQLite/gateway/context-injection/
context-integration suites; full `verify.py` including the new focused case;
context-pack sentinel; secret/PII scan over the diff; git diff review.
Commit as Leo Felix <leo@matteblack.io> only after everything is green.
No deploy.
