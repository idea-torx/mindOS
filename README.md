# IdeatorX Autopilot Control Plane v1

Durable local coordination layer for Hermes, Client Manager, Momentum Manager, cron jobs, and explicitly requested Claude Code work.

## Safety boundary

This v1 is a registry and evidence layer. It does **not** deploy, merge, send external messages, submit applications, or run arbitrary agent commands.

## Commands

```bash
A=~/.hermes/autopilot/autopilot.py
python3 "$A" init
python3 "$A" create --project Trove --title "Example task" --priority P1 --next-action "Inspect evidence"
python3 "$A" list
python3 "$A" claim <task-id> --owner hermes --minutes 30
python3 "$A" heartbeat <task-id> --owner hermes --note "Running verification"
python3 "$A" receipt <task-id> --kind verification --payload '{"tests":"pass","health":200}'
python3 "$A" update <task-id> --status waiting_for_user --next-action "Leo review"
python3 "$A" complete <task-id> --owner hermes --note "tests pass"  # requires live lease
python3 "$A" release <task-id> --owner hermes       # live-lease holder only
python3 "$A" renew <task-id> --owner hermes --minutes 45   # extend a live lease
python3 "$A" leases [--all] [--owner hermes]        # fleet-wide lease view
python3 "$A" cancel <task-id> --owner leo --reason "obsolete"       # rejected on foreign leases
python3 "$A" show <task-id>          # task detail + receipts + audit trail + dependencies
python3 "$A" search "deploy"         # substring search over task text fields
python3 "$A" search "audit" --status queued --project Trove --priority P1
python3 "$A" metrics                 # JSON observability snapshot
python3 "$A" verify-chain            # recompute the audit hash chain; report tampering
python3 "$A" events --action claimed --limit 20      # query the global audit stream
python3 "$A" events --entity-id <task-id> --since 2026-08-21T00:00:00Z --verify
python3 "$A" dashboard
```

## Lease fencing epochs

Every lease acquisition bumps a monotonic `lease_epoch` on the task and
surfaces it in the `claim` / `next --claim` output. A holder can pass its epoch
back on mutations so a stale process is rejected even when the owner name still
matches (e.g. its lease expired, was recovered, and reacquired by the same
owner):

```bash
python3 "$A" claim <task-id> --owner hermes --minutes 30          # → lease_epoch: 1
python3 "$A" heartbeat <task-id> --owner hermes --epoch 1         # ok while epoch matches
python3 "$A" complete <task-id> --owner hermes --epoch 1          # fenced completion
python3 "$A" release <task-id> --owner hermes --epoch 1           # fenced release
```

A mismatch fails with `lease superseded (held epoch N, current M); reclaim …`.
Passing no `--epoch` preserves the previous behavior, so existing callers keep
working; new agents should always fence with the epoch they were issued.

## Voluntary lease release

A lease holder can hand a task back to the queue without consuming retry
budget (unlike stale-lease recovery):

```bash
python3 "$A" release <task-id> --owner hermes   # live-lease holder only
```

Only the current holder of a live lease may release; foreign, expired, and
terminal states are rejected. The transition is audited as `lease_released`.

## Lease renewal & fleet view

A holder can extend its lease without changing task status (unlike heartbeat,
which forces `running` and a fixed 15-minute window) — useful for long-running
work that outlives the original claim window:

```bash
python3 "$A" renew <task-id> --owner hermes --minutes 45   # extend from now
python3 "$A" renew <task-id> --owner hermes --epoch 1      # fenced renewal
```

`renew` keeps the task's current status and preserves the fencing epoch, so a
renewal never invalidates a holder's token; a superseded holder passing a stale
`--epoch` is still rejected. Only the holder of a *live* lease may renew;
foreign or expired leases are rejected. The transition is audited as
`lease_renewed`.

Operators can see every held lease at a glance:

```bash
python3 "$A" leases                 # live leases, soonest expiry first
python3 "$A" leases --all           # include expired-but-still-held (recovery candidates)
python3 "$A" leases --owner hermes  # filter by holder
```

Each entry carries `task_id`, `project`, `title`, `status`, `priority`,
`owner`, `lease_expires_at`, `lease_epoch`, a `live` flag, and
`seconds_remaining`. Default output hides expired-held leases so it answers
"what is active right now"; `--all` answers "what will `recover` sweep".

## Shared memory (task notes)

Tasks carry a structured memory of provenance-tagged notes. This is the
retrieval substrate agents share across Hindsight/SQLite boundaries:

```bash
python3 "$A" note <task-id> --kind fact --content "API rate limit is 60/min" --source hermes
python3 "$A" note <task-id> --kind constraint --content "MUST NOT deploy on Friday" --pinned
python3 "$A" notes <task-id>              # live notes, oldest first
python3 "$A" notes <task-id> --all        # include superseded history
python3 "$A" supersede-note <note-id> --content "rate limit raised to 120/min"
python3 "$A" context <task-id> --budget 4000   # prompt-ready pack within a char budget
python3 "$A" search-notes "rate limit" --project Trove --kind fact
```

Design properties:

- **Kinds**: `fact`, `decision`, `observation`, `evidence`, `constraint`.
- **Provenance**: every note records its `source` (agent/operator) and timestamp.
- **Deduplication**: exact duplicate content on the same task returns the
  existing note (`deduplicated: true`) instead of growing the store; a
  duplicate add of pinned content promotes the existing note to pinned.
- **Pinning**: `--pinned` marks a note as critical. Pinned notes pack first in
  `context` (and survive tight budgets that drop unpinned notes), and the pin
  survives supersession so temporal fact chains stay protected.
- **Temporal facts**: `supersede-note` atomically retires an old note and links
  it to its replacement (`superseded_by`); superseded notes are hidden from
  default views but retained for audit.
- **Context budgets**: `context` packs a task summary header, unsatisfied
  dependencies, then live notes pinned-first (oldest→newest within each group)
  within a character budget. It reports `used_chars`, `truncated`, pack counts,
  and `notes_pinned_packed` so callers can assemble prompts deterministically.
- **Retrieval**: `search-notes` does keyword search over live note content with
  `--kind` plus task-level `--project` / `--status` filters via a join.
- **Lineage**: `note-history <note-id>` walks a temporal fact chain end to end
  (oldest predecessor → newest live successor), so agents can reconstruct how
  a fact evolved without manual `--all` archaeology.

`metrics` reports `notes_total`, `notes_superseded`, and `notes_pinned_live`;
`ops.py doctor` checks for orphaned notes and dangling supersession links.

## Agent handoff protocol (provider-neutral)

Any agent — Hermes, Claude Code, Codex, OpenCode — can publish a durable,
structured handoff on a task. The latest live handoff is the authoritative
resume point: a killed or fresh session reconstructs its working context from
`handoff-current` (or the `context` pack) in moments, with no vendor-specific
format:

```bash
python3 "$A" handoff <task-id> --from-agent codex --to-agent claude-code \
  --status running --objective "implement retry path" \
  --evidence "tests pass locally" --constraint "no new dependencies" \
  --decision "use exponential backoff" --file src/retry.py \
  --commit abc1234 --next-action "open PR" --risk "flaky integration test"
python3 "$A" handoff-current <task-id>   # recovery point for a new session
python3 "$A" handoffs <task-id>          # live handoff
python3 "$A" handoffs <task-id> --all    # full temporal chain
```

Design properties:

- **Complete carrier**: every handoff records source agent, target agent, work
  status, objective, verified evidence, constraints, decisions, files/commit,
  next actions, risks, and a timestamp — the fields an incoming agent needs
  before acting.
- **Temporal**: recording a new handoff atomically supersedes the previous
  live one (`superseded_by` link); superseded handoffs stay queryable via
  `--all`, so context evolution is auditable. A self-report is still not
  execution truth — pair handoffs with receipts for verified evidence.
- **Deduplicated**: an identical live payload returns the existing handoff
  (`deduplicated: true`) instead of growing the store; every write is
  provenance-tagged in the hash-chained audit ledger (`handoff_recorded`,
  `handoff_deduplicated`).
- **Context-budget aware**: `context` packs the live handoff immediately after
  the task header (before notes), so the resume point survives tight budgets;
  output reports `handoff` / `handoff_packed`. `show` surfaces it too.
- **Privacy boundary**: handoff payloads are plain operator/agent text — never
  copy credentials, tokens, raw personal data, or unrelated client context
  into them.
- **Consistency-checked**: `ops.py doctor` detects orphaned handoffs, dangling
  supersession links, and invariant violations (more than one live handoff per
  task); snapshots and archives carry the `handoffs` table.

## Retrieval-augmented context packs

`context --related N` turns the pack into cross-task RAG: up to N live notes
from *other* tasks whose content matches this task's title/description/
next_action are appended after the task's own notes, within the same character
budget. Each related note carries its source task (`task_id`,
`via_task_title`) and an FTS relevance `score`, so agents see prior knowledge
from sibling work without manual searching:

```bash
python3 "$A" context <task-id> --budget 4000 --related 5
python3 "$A" context <task-id> --budget 4000 --related 5 --related-scope global
python3 "$A" recall <task-id> --agent opencode   # session bootstrap: pack + lease + receipts + digest
```

Design properties:

- **Recall-oriented ranking**: candidate tokens are OR-combined through the
  FTS5 index and ordered by BM25, so the strongest matches pack first even
  when only one token overlaps.
- **Provenance**: every related note is tagged with the task it came from;
  the task's own notes are never duplicated into the related section.
- **Budget-honest**: related notes consume the same budget as own notes and
  report `related_requested` / `related_matched` / `related_packed`;
  `truncated` flips when anything was dropped.
- **Scope**: default `--related-scope project` restricts candidates to the
  task's project; `global` searches all projects.
- **Graceful fallback**: on non-FTS builds it degrades to any-token substring
  matching (same shape, minus `score`); tokenless task text yields zero
  related matches instead of an error.

## Session bootstrap (recall)

`recall` is the one-call pre-action ritual the handoff protocol requires: every
agent recalls the relevant context pack *before* acting. It bundles everything
in `context --related` (task header, unsatisfied deps, live handoff,
pinned-first notes, cross-task related notes) plus:

- **Lease awareness**: current lease owner/expiry/fencing epoch, whether it is
  live, and `held_by_caller` when `--agent` matches — so an agent knows if it
  must claim before editing.
- **Latest receipts**: the 3 most recent receipts with parsed payloads.
- **Sealed digest**: a deterministic SHA-256 over the durable context (the
  recall timestamp is excluded), so identical state yields an identical,
  referenceable digest. Any state change moves it.

Each recall is audited as a `context_recalled` event carrying the agent and
digest, so a receipt can later prove exactly which context was recalled before
acting. A self-report is never execution truth without a receipt; a digest ties
the two together.

```bash
python3 "$A" recall <task-id> --agent codex --budget 6000 --related 5
python3 "$A" events --action context_recalled --entity-id <task-id>
```

## Ranked retrieval (FTS5)

Search can run through SQLite FTS5 (stdlib — no external dependency) instead of
substring matching. Pass `--rank` to `search-notes` or `search` for BM25-ranked
results; each hit carries a `score` (more negative = more relevant):

```bash
python3 "$A" search-notes "postgres pool" --rank --kind fact --project Trove
python3 "$A" search "rate limit" --rank --status queued
```

Design properties:

- **Always in sync**: the `notes_fts` / `tasks_fts` indexes are external-content
  tables maintained by triggers on every insert/update/delete, including
  snapshot restores — no separate reindex step.
- **Graceful fallback**: on SQLite builds without FTS5 the indexes are skipped
  entirely and `--rank` degrades silently to the substring path (same output
  shape, minus `score`). Tokenless queries return `[]` rather than erroring.
- **Conjunctive semantics**: multi-token queries match documents containing all
  tokens; superseded notes are excluded from ranked note search.
- **Drift detection**: `ops.py doctor` compares indexed vs source row counts
  and reports `fts_index_drift` if they ever diverge.

## Dispatch fairness (per-owner lease caps)

By default an owner may hold unlimited live leases. Set a cap to stop one
agent from hogging dispatch:

```bash
python3 "$A" claim <task-id> --owner hermes --minutes 30 --max-active 4
python3 "$A" next --claim --owner hermes --max-active 4
export AUTOPILOT_MAX_ACTIVE_PER_OWNER=4   # default for every claim/next
```

The cap is enforced atomically inside the lease acquire statement, so
concurrent dispatchers cannot race past it. When an owner is at capacity the
claim fails with `owner '<name>' at lease capacity (n/max)`; completing or
releasing a lease frees capacity immediately. `metrics` reports
`active_leases_by_owner` for observability.

## Deadlines (due_at)

Tasks can carry an optional UTC deadline. Dispatch honors it: within a
priority class, the earliest deadline is picked first and undated tasks sort
last. Overdue non-terminal tasks surface in `metrics` (`overdue_tasks`,
`due_within_24h`), can be listed with `list --overdue`, and are flagged
`[OVERDUE …]` on the dashboard:

```bash
python3 "$A" create --project Trove --title "Renew cert" --due-at "2026-09-01T17:00:00Z"
python3 "$A" update <task-id> --due-at "2026-09-02T09:00:00+02:00"   # reschedule (normalized to UTC)
python3 "$A" update <task-id> --due-at ""                            # clear the deadline
python3 "$A" list --overdue
```

Timestamps accept any ISO 8601 form (naive values are assumed UTC) and are
stored normalized; invalid timestamps are rejected. `context` includes
`due_at` in the task summary so agents see deadlines in their prompt pack.

## Blocking & unblocking

Operators and agents can park work with a reason without cancelling it:

```bash
python3 "$A" block <task-id> --owner leo --reason "waiting on credentials"
python3 "$A" unblock <task-id> --owner leo          # requeues, clears the reason
python3 "$A" blocked-by <task-id>                   # transitive blockers, depth-tagged
```

Design properties:

- **Lease-safe**: `block` never overrides a foreign or expired lease; blocking
  a task you hold a live lease on releases that lease so blocked tasks cannot
  look active to recovery or dispatch.
- **Audited**: transitions are recorded as `blocked` (with `previous_status`)
  and `unblocked` events in the hash chain.
- **Claim guard**: claiming a blocked task is rejected with its reason —
  deliberate migration from earlier behavior where blocked tasks were directly
  claimable; call `unblock` first.
- **DAG visibility**: `blocked-by` walks all transitive prerequisites via a
  recursive CTE, reporting each blocker's `depth` (direct deps at 1), live
  status, title, and `satisfied` flag, plus a top-level `blocked` boolean.
- **Reverse edges**: `show` now includes `dependents` — the tasks waiting on
  this one — so operators can see what completing a task unblocks.

## Dispatch diagnostics

`next --explain` reports how many queued candidates were considered and why
each skipped candidate was not picked (`unsatisfied_dependencies` with the
blocking ids). Without the flag the output shape is unchanged:

```bash
python3 "$A" next --project Trove --explain
# → { "task": null, "considered": 3, "skipped": [{"task_id": "t2",
#      "reason": "unsatisfied_dependencies", "blocked_by": ["t1"]}] }
```

## Operator search

`search` does a substring match across `id`, `project`, `title`,
`description`, `next_action`, and `blocked_reason`, with optional
`--status`, `--project`, and `--priority` filters. Results use the same
priority-then-recency ordering as `list`.

## Dependency-aware dispatch

Tasks can declare dependencies on other tasks. A task with an incomplete
dependency cannot be claimed, and `next` skips it when dispatching:

```bash
python3 "$A" create --project Trove --title "Dependent task" --depends-on <prereq-id>
python3 "$A" dep <task-id> <prereq-id>   # add a dependency edge (cycles rejected)
python3 "$A" dep-remove <task-id> <prereq-id>   # remove a mistaken edge (audited)
python3 "$A" next                        # highest-priority queued task whose deps are completed
python3 "$A" next --project Trove --claim --owner hermes --minutes 30
```

`dep-remove` corrects a mistaken `dep` / `create --depends-on` call: the edge is
deleted, the removal is audited as `dependency_removed`, and the dependent task
becomes claimable immediately. Removing a non-existent edge (or naming a task
that does not exist) is rejected.

## Correcting tasks after creation

Tasks are not immutable: `update` can also edit identity fields, so a typo or a
re-prioritization does not force a create/cancel round trip:

```bash
python3 "$A" update <task-id> --title "Corrected title"
python3 "$A" update <task-id> --description "Fuller description" --priority P1
python3 "$A" update <task-id> --project RenamedProject
```

Invalid priorities are rejected by the CLI, and every change is recorded in the
task's audit trail with the new values.

`next` orders by priority (`P0` first), then oldest-created. With `--claim`, the
picked task's lease is acquired atomically in the same step, so concurrent
dispatchers can never double-claim. `metrics` reports
`queued_blocked_by_deps` for tasks waiting on prerequisites.

## Safe operations (ops.py)

```bash
O=~/.hermes/autopilot/ops.py
python3 "$O" recover --max-retries 3   # requeue stale leases; fail tasks past retry budget
python3 "$O" recover --dry-run         # preview what the next pass would do, mutating nothing
python3 "$O" approval approve <task-id> --by leo
python3 "$O" policy <project> <action> # check user-approval policy for an action
python3 "$O" processes                 # list active agent processes (read-only)
python3 "$O" morning                   # morning brief
python3 "$O" snapshot                  # consistent JSON export of all tables, sealed with a SHA-256
python3 "$O" snapshot-check <file>     # verify a snapshot's integrity hash (exit 1 on tampering)
python3 "$O" snapshot-restore <file> --force   # rebuild the database from a verified snapshot
python3 "$O" archive --before "2026-09-01T00:00:00Z"           # seal + remove terminal tasks
python3 "$O" archive --before "..." --dry-run                  # preview without mutating
python3 "$O" archive-check <file>      # verify an archive's integrity hash
python3 "$O" archive-restore <file> [--force]  # re-import archived tasks
```

`snapshot` writes an atomic, `autopilot-snapshot-v1` JSON document (default
under `~/.hermes/autopilot/backups/`) containing every table plus a self-hash,
giving a point-in-time backup for disaster recovery without touching the live
database. `snapshot-check` recomputes the hash and exits non-zero if the file
was modified.

`snapshot-restore` closes the recovery loop: it verifies the snapshot's
integrity hash *before* touching anything, refuses to overwrite a non-empty
database unless `--force` is passed, reloads every table in one transaction,
then re-checks foreign-key consistency and the audit hash chain (exiting
non-zero if either fails). Restores preserve audit-event ordering and lease
state exactly as snapshotted.

`recover` consumes one unit of each task's retry budget per pass. Tasks whose
retry budget is exhausted (`retry_count > max-retries`, default 3) transition to
`failed` with reason `max lease retries exceeded` instead of looping forever.
`--dry-run` reports `would_recover` / `would_fail` without touching state,
making it safe to run from monitoring cron before committing to a real pass.

## Recovery backoff (dispatch cooldown)

A task whose lease just went stale is requeued by `recover`, but redispatching
it instantly lets a repeatedly failing task hot-loop through its retry budget.
Recovered tasks therefore enter a deterministic exponential cooldown:
`recover_after = now + backoff_base * 2^(retry_count-1)` seconds (default base
60s, capped at 3600s via `--backoff-cap`; `--backoff-base 0` disables the
cooldown entirely for the old instant-redispatch behavior):

```bash
python3 "$O" recover --backoff-base 60 --backoff-cap 3600
python3 "$O" recover --dry-run     # previews the cooldown per task in "backoff"
```

Design properties:

- **Dispatch-level enforcement**: `next` never picks a queued task whose
  `recover_after` is in the future; `next --explain` reports those candidates
  with reason `recovery_backoff` and their deadline.
- **Explicit override preserved**: a direct `claim` of a cooling-down task is
  still allowed as a deliberate operator action, and any successful lease
  acquisition clears the cooldown.
- **Audited**: the `lease_recovered` audit event records the applied
  `recover_after`.
- **Observable**: `metrics` reports `tasks_in_backoff`.

## Archival & retention

Terminal tasks accumulate forever without a retention path. `ops.py archive`
consolidates them: every `completed` / `failed` / `cancelled` task with
`updated_at <= --before` is sealed into an atomic, self-hash-verified
`autopilot-archive-v1` JSON document (default under
`~/.hermes/autopilot/backups/`), then removed from the live database together
with its dependencies, heartbeats, receipts, notes, and receipt files:

```bash
python3 "$O" archive --before "2026-09-01T00:00:00Z" --dry-run   # preview ids + counts
python3 "$O" archive --before "2026-09-01T00:00:00Z" --out /path/archive.json
python3 "$O" archive-check <archive.json>       # exit 1 if the file was tampered with
python3 "$O" archive-restore <archive.json>     # re-import; --force replaces collisions
```

Design properties:

- **Seal-then-destroy**: the archive file is written and fsynced before any
  row is deleted; deletion happens in one transaction in child-first order.
- **Dependency guard**: archiving refuses while any *live* task still depends
  on a terminal candidate, so dispatch prerequisites can never be archived out
  from under queued work.
- **Append-only audit**: audit events are retained in the live database (the
  hash chain must stay verifiable) but are counted and copied into the archive
  for reference. `verify-chain` remains `ok` after an archive pass.
- **FTS-consistent**: deletions and restores fire the external-content triggers,
  so ranked search never surfaces (or misses) archived notes.
- **Restorable**: `archive-restore` verifies integrity first, refuses task-id
  collisions unless `--force`, reinserts rows in FK order, recreates receipt
  files atomically with `0600` permissions, and re-checks foreign keys.

## Lifecycle guardrails

- `complete` enforces claim-before-complete: only the current holder of a live
  lease may complete a task. Unleased, foreign-held, or expired leases are
  rejected.
- `cancel` is an operator transition that tolerates an unleased task but never
  overrides a foreign or expired lease.
- `block`/`unblock` park and requeue work with audited reasons; blocked tasks
  cannot be claimed until unblocked (see "Blocking & unblocking").
- Any `update --status` to a terminal state (`completed`, `failed`,
  `cancelled`) releases the held lease so terminal tasks cannot look active.

## Audit event stream

`events` queries the global hash-chained audit ledger — not just the per-task
trail from `show` — with entity/action filters, an ISO 8601 time window, a
limit, and optional inline chain verification:

```bash
python3 "$A" events --action claimed --limit 20
python3 "$A" events --entity-id <task-id> --since "2026-08-21T00:00:00Z" --until "2026-08-22T00:00:00Z"
python3 "$A" events --limit 100 --verify   # also recompute the chain in the same call
```

Results are newest-first and carry parsed `payload` objects. `total_matching`
and `truncated` report how the limit clipped the result set; invalid
timestamps are rejected with the offending flag named in the error.

## Audit integrity

Every audit event is linked into a SHA-256 hash chain (`prev_hash`, `hash`).
Existing databases are migrated and backfilled automatically on first use.
`autopilot.py verify-chain` recomputes the chain and reports any
`hash_mismatch` or `broken_link`, giving tamper-evident history. `ops.py
doctor` runs the same check as part of a broader consistency sweep.

```bash
O=~/.hermes/autopilot/ops.py
python3 "$O" doctor   # orphan deps, receipt index/file drift, audit chain, stale leases, note integrity
```

## Task lifecycle

```text
queued → claimed → running → waiting_for_user → completed
                         ↘ waiting_for_review
                         ↘ blocked
```

Terminal states are `completed`, `failed`, and `cancelled`.

## Receipts

Receipts are stored in `receipts/` and indexed in SQLite. A completed engineering task should carry evidence such as:

- test/typecheck result
- commit SHA
- PR URL
- CI result
- deployment URL
- health-check result
- approval record

## Next integration

The hourly control tower should read this registry and report task changes. Existing project-specific policies should be added under `policies/` before allowing automatic side effects.
