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
python3 "$A" fail <task-id> --owner codex --reason "tests red"      # failure + retry budget/backoff
python3 "$A" release <task-id> --owner hermes       # live-lease holder only
python3 "$A" renew <task-id> --owner hermes --minutes 45   # extend a live lease
python3 "$A" leases [--all] [--owner hermes]        # fleet-wide lease view
python3 "$A" transfer <task-id> --from-owner hermes --to-agent codex   # reassign a live lease
python3 "$A" resume <task-id> --agent codex         # idempotent killed-session recovery
python3 "$A" cancel <task-id> --owner leo --reason "obsolete"       # rejected on foreign leases
python3 "$A" defer <task-id> --owner hermes --until "2026-08-22T09:00:00Z"  # park out of dispatch
python3 "$A" tag <task-id> --tag autopilot-safe      # attach capability/scope tags (repeatable)
python3 "$A" untag <task-id> --tag client:trove      # remove one tag
python3 "$A" next --claim --owner codex --tag autopilot-safe  # tag-scoped dispatch
python3 "$A" plan [--project P] [--tag T]   # parallel dispatch-wave schedule
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

## Seam conflicts (worktree/branch exclusivity)

Leases prevent two agents from taking the same *task*, but not two agents
editing the same *checkout*. A seam is the shared filesystem/VCS resource where
concurrent agents physically collide: an identical non-empty `worktree` path,
or the same `branch` name within the same `project` (the same branch across
different projects is a different repository, so it is not a conflict).

`claim` refuses a task whose seam is held by another live lease:

```bash
python3 "$A" claim wt-2 --owner codex
# seam conflict: wt-1 holds worktree '/srv/wt' (owner hermes); complete/release the holder first or pass --force
python3 "$A" claim wt-2 --owner codex --force   # deliberate override (e.g. holder is you, read-only pass)
```

The refusal is audited as `claim_refused_seam` (kind-level conflict list, no
lease left behind), and dispatch participates too: `next --claim` skips
seam-conflicted candidates and picks the best unconflicted task instead of
failing after picking — `--explain` reports them as `seam_conflict` skips with
the holding tasks. Set `worktree`/`branch` via `update`; empty values are
never seams.

## Downstream impact & unblock-aware scheduling

`blocked-by` answers "what holds this task?"; `impact` answers the mirror
question: "what is waiting on this task?" It walks the dependency DAG downward
and reports every transitive dependent with its depth, live status, and a
settled flag (completed/cancelled work no longer cares), plus a summary that
answers "what happens if I block, defer, or cancel this?":

```bash
python3 "$A" impact <task-id>
# { "impacted": 5, "open": 4, "by_status": {"queued": 3, "running": 1, ...},
#   "dependents": [{"id": ..., "depth": 1, "status": ..., "settled": 0}, ...] }
```

Completion feeds back downstream: `complete` now reports `newly_unblocked` —
the queued direct dependents whose dependencies it just satisfied — in both
its output and the audited `completed` event, so an agent finishing a hub
knows exactly which work it freed (and the audit trail records it).

Dispatch can also schedule by graph shape. `next --prefer-unblocking` adds a
critical-path tie-break: within one effective priority tier and deadline
class, candidates are ordered by descending count of queued direct dependents
(surfaced as `unblocks` on every pick and under `--explain` with
`unblock_scheduling: true`). Finishing a hub frees more of the graph than
finishing a leaf — but priority, deadlines, and aging fairness are never
overridden; without the flag, ordering is byte-for-byte unchanged.

```bash
python3 "$A" next --claim --owner codex --prefer-unblocking
```

## Critical path (longest unfinished chain)

`blocked-by` looks up from one task and `impact` looks down from one task;
`critical-path` answers the fleet-level question: what is the longest chain of
still-unfinished prerequisite work? Its `length` is the minimum number of
sequential dispatch waves needed to drain the open graph — no schedule can
finish faster — and its members are the bottleneck chain: slipping on any of
them slips everything behind it.

```bash
python3 "$A" critical-path                 # whole fleet
python3 "$A" critical-path --project Trove # one project's graph
# { "length": 3, "path": [{"id": ..., "title": ..., "status": ..., "priority": ...}, ...],
#   "open_tasks": 12, "by_status": {"queued": 9, "running": 2, "blocked": 1} }
```

Design properties:

- **Open graph only**: completed/cancelled tasks leave the graph entirely; a
  missing prerequisite referenced by a live edge appears as a `missing` node,
  because it blocks dispatch exactly like a real task.
- **Deterministic**: ties at every step break to the lexicographically smallest
  id, so identical state yields an identical path.
- **Composable**: pair it with `next --prefer-unblocking` to actually work the
  chain — the path names what matters, unblocking tie-breaks help drain it.
- **Observable**: `metrics` reports `critical_path_length` fleet-wide.

## Dispatch wave plan (parallel schedule)

`critical-path` says how many serial waves the open graph needs; `plan` says
which tasks go in each wave. Wave 1 is every ready task (in-flight work
included, shown with its live status), each later wave is what the previous
waves unblock, and anything that can never start inside the requested scope is
reported under `unschedulable` with its blockers instead of being silently
dropped:

```bash
python3 "$A" plan                 # whole fleet
python3 "$A" plan --project Trove # one project's graph
python3 "$A" plan --tag autopilot-safe   # one capability scope
# { "waves": [{"wave": 1, "tasks": [{"id": ..., "title": ..., "status": ..., "priority": ...}, ...]},
#             {"wave": 2, "tasks": [...]}],
#   "unschedulable": [{"id": ..., "blocked_by": [{"id": ..., "status": "missing"}]}],
#   "waves_total": 2, "scheduled_tasks": 5, "open_tasks": 6 }
```

Design properties:

- **Deterministic**: within a wave, tasks order by dispatch preference —
  priority rank, then earliest deadline (undated last), then oldest-created,
  then id — so identical state yields an identical schedule and re-running
  `plan` diffs to nothing.
- **Honest about scope**: a prerequisite outside the plan (a missing id, or a
  live task in another project under `--project`) can never be scheduled here,
  so it and everything downstream of it land in `unschedulable` with blocker
  ids and statuses.
- **Read-only simulation**: runtime guards that depend on live state (seam
  conflicts, recovery backoff, deferral windows) still apply at claim time and
  are deliberately not folded into the waves.
- **Composable**: `waves_total` equals `critical-path`'s `length` for the same
  scope — use `plan` to size parallel capacity per wave and work the waves
  with `next --claim`.

## Lease transfer (cross-agent ownership)

When work moves from one agent to another, ownership moves with it. `transfer`
atomically reassigns a live lease — only the current holder of a *live* lease
may transfer, the fencing epoch bumps so the old holder's token is invalidated
immediately, and status is preserved:

```bash
python3 "$A" transfer <task-id> --from-owner codex --to-owner claude-code --minutes 45
python3 "$A" transfer <task-id> --from-owner codex --to-owner claude-code --epoch 3   # fenced
```

The transition is audited as `lease_transferred` with both owners and both
epochs. Terminal tasks, foreign holders, expired leases, and same-owner
transfers are rejected.

## Session recovery (resume)

`resume` is the one-call recovery half of the handoff protocol: a killed or
fresh session recreates itself from durable state in moments, instead of hand-
orchestrating claim + recall. It applies the same guards as `claim` (terminal,
blocked, and dep-blocked tasks are rejected with their reasons), then:

- **live lease held by the caller** → no mutation; returns the sealed recall
  bundle with `action: already_held` (calling `resume` twice is safe);
- **expired or absent lease** → claimed atomically for the caller, honoring
  per-owner caps (`action: claimed`);
- **live lease held by someone else** → rejected; ask them to `transfer`.

```bash
python3 "$A" resume <task-id> --agent codex [--budget 6000] [--related 5]
```

The response embeds the full recall bundle (task header, deps, live handoff,
notes, lease state, latest receipts) sealed with the deterministic digest, and
every resume is audited as `session_resumed` carrying the agent, action, and
digest — so recovery is idempotent, observable, and provably context-fresh.

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
- **Near-duplicate guard**: rephrased restatements (high token-Jaccard overlap,
  default ≥ 0.8, tunable via `AUTOPILOT_NEAR_DUP_THRESHOLD`) are still stored
  but flagged: the response carries `similar_to` with note ids and similarity
  scores, and the audited `note_added` event records `similar_notes`, so shared
  memory does not silently accumulate near-identical facts.
- **Pinning**: `--pinned` marks a note as critical. Pinned notes pack first in
  `context` (and survive tight budgets that drop unpinned notes), and the pin
  survives supersession so temporal fact chains stay protected.
- **Temporal facts**: `supersede-note` atomically retires an old note and links
  it to its replacement (`superseded_by`); superseded notes are hidden from
  default views but retained for audit.
- **TTL (note lifetimes)**: `--ttl-hours N` gives a fact a lifetime. Past its
  `expires_at`, an *unpinned* note retires: it is excluded from context packs,
  recall bundles, related-note candidates, and `search-notes` (counted in the
  pack as `notes_expired_excluded` rather than dropped silently; search can
  surface retirees with `--include-expired`). *Pinned* notes are immortal by
  design — an expired pin still packs but carries `expired: true` so the agent
  knows a fresh supersede is due; silently dropping a critical constraint is
  exactly the failure mode TTL must not introduce. Re-adding a retired note's
  exact content revives it (`revived: true`, audited as `note_ttl_refreshed`)
  and restates its lifetime — omitting `--ttl-hours` makes it immortal again.
  A superseding note is fresh: it inherits no expiry unless `--ttl-hours` is
  passed explicitly. `metrics` reports `notes_expired_live` and
  `ops.py notes-expired` lists every expired live note fleet-wide with the
  action it needs (`revive` for retirees, `supersede` for expired pins).
- **Context budgets**: `context` packs a task summary header, unsatisfied
  dependencies, then live notes pinned-first (oldest→newest within each group)
  within a character budget. It reports `used_chars`, `truncated`, pack counts,
  and `notes_pinned_packed` so callers can assemble prompts deterministically.
- **Retrieval**: `search-notes` does keyword search over live note content with
  `--kind` plus task-level `--project` / `--status` filters via a join.
- **Lineage**: `note-history <note-id>` walks a temporal fact chain end to end
  (oldest predecessor → newest live successor), so agents can reconstruct how
  a fact evolved without manual `--all` archaeology.
- **Handoff lineage**: `handoff-history <handoff-id>` is the same walk for the
  resume point: given any link in a task's supersession chain, it reconstructs
  the full sequence oldest → newest, showing how the handoff (owner, objective,
  evidence) evolved across agents.

`metrics` reports `notes_total`, `notes_superseded`, `notes_pinned_live`,
`notes_expired_live`, and `notes_consolidated_total`; `ops.py doctor` checks
for orphaned notes and dangling supersession links.

## Memory consolidation (ops.py consolidate)

The `note` command's near-duplicate guard only *flags* rephrased restatements;
over weeks of agent traffic, shared memory still accumulates near-identical
facts that all consume context budget and all surface in retrieval.
`ops.py consolidate` finishes the job: live notes on each task are clustered by
token-Jaccard similarity (the same measure as the guard), and every
non-canonical member is superseded into its cluster's canonical note — the
pinned note when a cluster has one, else the newest:

```bash
python3 ops.py consolidate --dry-run        # preview clusters without mutating
python3 ops.py consolidate                  # merge fleet-wide
python3 ops.py consolidate --task <task-id> # scope to one task
python3 ops.py consolidate --threshold 0.7  # looser matching (default: 0.8 / env)
```

Design properties:

- **Deterministic clustering**: notes are processed oldest→newest; each joins
  the first cluster whose canonical note it matches at or above the threshold,
  else founds its own cluster. Same rows always yield the same plan.
- **Pin-aware canonical choice**: a pinned note beats an unpinned one for
  survival regardless of age, so critical constraints never dissolve into a
  newer paraphrase; among equals the newest wins.
- **History-preserving**: losers are superseded (never deleted) — they point at
  their canonical note via `superseded_by`, so `note-history` still reconstructs
  how a fact was restated, and audit retains every step.
- **Audited + observable**: each merge records a `note_consolidated` event
  carrying the kept note id and similarity; `metrics` reports
  `notes_consolidated_total`.
- **Idempotent**: consolidated notes leave the live set, so repeated passes
  find nothing new; concurrent supersedes lose safely against the
  `WHERE superseded_by=''` guard.
- **Retired notes excluded**: expired unpinned notes are already invisible to
  packs and retrieval, so consolidation never resurrects them.

## Task deduplication (similar / ops.py dup-tasks)

The same dedup discipline that keeps shared memory clean applies to the work
queue itself: two open tasks describing the same work split agent effort across
two seams, both surface in dispatch, and neither inherits the other's context.
Task creation now flags this at the source — when a new task's
title+description text overlaps an open (non-terminal) same-project task at or
above the near-duplicate threshold, the response carries `similar_open_tasks`
and the audited `created` event records `similar_open_tasks`, so provenance
shows the collision was visible from birth:

```bash
python3 autopilot.py similar <task-id>              # triage: what restates this task?
python3 autopilot.py similar <task-id> --threshold 0.7   # looser matching
python3 ops.py dup-tasks                            # fleet-wide cluster sweep
python3 ops.py dup-tasks --threshold 0.7 --dry-run  # (read-only either way)
```

Design properties:

- **Open tasks only**: settled work is history, not a collision — completed,
  failed, and cancelled tasks never count as duplicates.
- **Same-project only**: by the seam rule, the same title under a different
  project is a different checkout and never conflicts.
- **Informational, never blocking**: creation is never refused; agents and
  operators decide whether to cancel a duplicate or fold it into the canonical
  task via `dep`.
- **Read-only fleet sweep**: unlike notes, tasks cannot be auto-superseded —
  merging them is a lifecycle decision — so `dup-tasks` clusters with the same
  greedy token-Jaccard algorithm as `consolidate` (canonical = oldest) and
  reports each cluster with a suggested action instead of mutating.
- **Deterministic**: similarity descending, then id; same rows always yield
  the same clusters.

## Secret guard (privacy boundary for shared memory)

The contract is explicit: credentials, private tokens, and raw personal data
never enter shared memory. The write path now enforces it instead of trusting
agent discipline. `note`, `supersede-note`, and `handoff` scan their content
(objective + list fields for handoffs) with shape detectors — AWS access keys,
GitHub / OpenAI-style / Slack / Google tokens, private-key blocks, bearer
headers, and generic `password:`/`api_key=` assignments — and refuse to store
credential-shaped content:

```bash
python3 autopilot.py note t1 --content 'key is AKIA...'            # blocked (audited secret_blocked)
python3 autopilot.py note t1 --content 'key is AKIA...' --redact   # stored as [REDACTED:aws_access_key]
python3 autopilot.py handoff t1 ... --allow-secret                 # verbatim override, audited
```

Design properties:

- **Kind-only reporting**: findings are named by pattern kind, never by value —
  errors, output, and audit payloads never echo the secret itself.
- **Low false positives**: the generic assignment pattern requires a digit in
  the value, so prose like `fencing token: lease_epoch chain` passes while real
  secrets (which almost always mix digits in) still trip it.
- **Three audited outcomes**: `secret_blocked` (default), `secret_redacted`
  (`--redact` stores a `[REDACTED:<kind>]` copy), `secret_allowed`
  (`--allow-secret` override) — every escape hatch leaves a trail.
- **Fleet sweep**: `ops.py secret-scan` finds credential-shaped content already
  sitting in live notes/handoffs (legacy rows or overrides) with the same
  detector; read-only, remediation is a history-preserving supersede. `--all`
  includes superseded rows.
- **Observable**: `metrics` reports `secrets_blocked_total`,
  `secrets_redacted_total`, and `secrets_allowed_total`.

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
python3 "$A" ack <task-id> --agent claude-code   # accept an inbound handoff
python3 "$A" handoff-inbox --agent claude-code [--unacked-only]  # fleet-wide inbound view
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
- **Recall provenance**: pass `--recall-digest <sha256>` (the digest from a
  prior `recall`) to attach proof of the exact context the handoff was written
  against; `complete --recall-digest` does the same for completions. The
  digest is stored on the record and in its audit event, and `metrics`
  reports `handoffs_with_recall_proof`. Digests are validated as 64-char hex.
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

## Handoff inbox (fleet-wide inbound view)

Per-task commands answer "what is the state of this task"; `handoff-inbox`
answers the question an incoming agent actually starts from — "what work was
handed to me across the whole fleet?":

```bash
python3 "$A" handoff-inbox --agent claude-code
python3 "$A" handoff-inbox --agent claude-code --project Trove --limit 10
```

Only *live* (non-superseded) handoffs whose `to_agent` matches are listed, so
when a handoff is superseded by one addressed to someone else, the task leaves
the previous recipient's inbox automatically. Each item joins the task's live
state — title, status, priority, lease owner/liveness — plus the handoff's
`from_agent`, objective, commit, recall digest, and timestamp, so an agent can
triage its inbound work without a follow-up `show` per task. The natural loop
is: `handoff-inbox` → `resume <task-id> --agent me` → act → publish a receipt.

## Handoff acknowledgment (ack)

The inbox surfaces inbound work; `ack` records that the recipient has
*accepted* it, closing the loop between "handed to" and "picked up by":

```bash
python3 "$A" ack <task-id> --agent claude-code
python3 "$A" ack <task-id> --agent claude-code --recall-digest <sha256>   # tie acceptance to recalled context
```

Design properties:

- **Addressed-only**: only the agent the live handoff is addressed to may ack
  it; a foreign agent is rejected.
- **Idempotent**: re-acking returns the existing acknowledgment
  (`already_acked: true`) instead of duplicating state.
- **Reset by supersession**: recording a new handoff clears acceptance — a
  reassigned or updated handoff must be picked up again by its new recipient.
- **Provenance**: an optional `--recall-digest` ties the acceptance to proof of
  the context pack the recipient recalled before accepting.
- **Audited**: every first ack records a `handoff_acknowledged` event in the
  hash chain; `handoff-current`, `show`, and `handoffs --all` surface
  `acked_by` / `acked_at`.
- **Triage-aware**: inbox items carry `acked` / `acked_at`, and
  `handoff-inbox --unacked-only` restricts the view to work not yet picked up.
- **Observable**: `metrics` reports `handoffs_acked_total`, and
  `ops.py handoff-check` flags an addressed live handoff older than the ack SLA
  (`--ack-sla-hours`, default 24) that was never acknowledged as
  `stale_unacknowledged`.

## Handoff protocol lint (handoff-check)

The handoff protocol makes promises — an objective, a recipient, evidence or
next actions, and recall provenance when `--recall-digest` is cited.
`ops.py handoff-check` is the read-only enforcement sweep that turns violations
into observable problems instead of silent drift:

```bash
python3 ops.py handoff-check                 # lint every live handoff fleet-wide
python3 ops.py handoff-check --task <task-id>   # scope to one task
```

Reported reasons:

- `unaddressed` — no `to_agent`, so no inbox will ever surface it
- `missing_objective` — the resume point has no stated goal
- `sparse_no_evidence_or_next_actions` — a self-report without proof
- `unproven_recall_digest` — the cited digest never appears in the audited
  recall stream (`context_recalled` or `session_resumed`; a resume digest is
  first-class provenance) — fabricated or mistyped citation
- `older_than_latest_recall` — genuinely recalled, but a newer audited recall
  for the task exists since, so the handoff may rest on stale context
- `terminal_task_handoff` — live handoff on a completed/failed/cancelled task
- `stale_unacknowledged` — addressed live handoff older than the ack SLA
  (`--ack-sla-hours`, default 24) that its recipient never acknowledged

Provenance is checked against the audit ledger, not recomputed digests, so
routine lease renewals and the handoff's own recording never false-positive;
fine-grained freshness against *current* state remains `recall-verify`'s job.

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

### Related handoffs: neighboring resume points

`--related-handoffs N` extends the same idea to the handoff protocol itself:
up to N live handoffs on *other* tasks whose objective/status/agents match the
task's text are packed after related notes, within the same budget. Where
related notes carry prior *knowledge*, related handoffs carry neighboring
*resume points* — the decisions and commit refs of sibling work an agent would
otherwise rediscover:

```bash
python3 "$A" context <task-id> --budget 6000 --related 5 --related-handoffs 3
```

Each entry carries `task_id`, `via_task_title`, `from_agent`/`to_agent`,
`status`, `objective`, and `commit_ref`; the pack reports
`related_handoffs_requested` / `_matched` / `_packed`. The flag is opt-in and
digest-gated: packs built without it stay byte-identical to the legacy shape,
and a recall made with it only verifies when recomputed with the same value.
Candidates are matched through the FTS index but ordered deterministically
(created_at DESC, rowid) with no relevance score emitted — BM25 scores drift
whenever any handoff joins the index, which would falsely stale every sealed
digest. Superseded handoffs are never candidates; the task's own handoffs are
excluded (its own live handoff already appears in the bundle).

## Handoff search

`search-handoffs` is fleet-wide keyword retrieval over the protocol: "what
decided/did work like this before?" Live handoffs are searched by default;
`--all` includes superseded ones (tagged with `superseded_by`). Filters:
`--task`, `--from-agent`, `--to-agent`, `--project`. With `--rank` (and an
FTS5-capable SQLite) results are BM25-ranked over
objective/status/from_agent/to_agent via a dedicated `handoffs_fts` index and
carry a `score`; otherwise substring LIKE matching with identical output shape
minus the score. Every row joins its task's project/title so hits are
triageable without a follow-up `show`:

```bash
python3 "$A" search-handoffs "postgres pool" --rank --project Trove
python3 "$A" search-handoffs "migration" --from-agent codex --all
```


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
  referenceable digest. Any state change moves it. A second `core_digest`
  excludes the live-handoff section: an agent recalls first and records its
  handoff afterwards, so its own handoff must not count as drift against its
  own citation — note/receipt/lease/dep drift still moves both digests.

Each recall is audited as a `context_recalled` event carrying the agent, the
digest, the core digest, and the bundle parameters (budget, related count,
scope) — so downstream sweeps can recompute the digest exactly as it was
recalled. `resume` audits its bundle the same way (`session_resumed`), making a
resume digest first-class recall provenance for handoffs and completions. A
self-report is never execution truth without a receipt; a digest ties the two
together.

`recall-verify` closes the loop: pass a previously recalled digest and it
recomputes the current bundle (same algorithm, no audit write) and reports
`fresh: true` when nothing durable has changed since that recall — notes,
handoffs, lease state, receipts, deps all match. A stale result carries the
new `current_digest` so the agent can re-`recall` before acting. Handoffs and
completions cite the digest they acted on via `--recall-digest`, making stale
context detectable after the fact.

```bash
python3 "$A" recall <task-id> --agent codex --budget 6000 --related 5
python3 "$A" recall-verify <task-id> --digest <sha256> --agent codex --budget 6000 --related 5
python3 "$A" recall-diff <task-id> --digest <sha256>
python3 "$A" events --action context_recalled --entity-id <task-id>
python3 ops.py recall-stale   # fleet sweep: which live handoffs cite drifted context?
```

### Recall diff (what exactly moved)

`recall-verify` answers "is my context still fresh?" with a boolean;
`recall-diff` answers the follow-up an agent actually acts on — "what
changed?". Every `recall`, `resume`, and `next --claim --recall` now records a
compact per-section manifest of the bundle alongside its digest in the audited
event (never hashed into the digest itself, so digests stay byte-compatible
with pre-manifest recalls). Given a cited digest, `recall-diff` looks up that
event, recomputes the current bundle *exactly as it was originally recalled*
(recorded budget/related/scope/rerank parameters), and diffs section by
section:

- `task` — status/priority/due_at/next_action/blocked_reason field moves
- `dependencies` — satisfied vs newly-added prerequisite ids
- `handoff` — the live resume point was recorded or superseded (`from`/`to`)
- `notes` — added/removed note ids plus pinned/expired flag flips
- `related_notes` — cross-task retrieval candidates that appeared or left
- `lease` — owner/epoch/expiry/liveness changes (`from`/`to`)
- `receipts` — evidence receipts posted or rotated out of the top 3

The result carries `fresh`, `unchanged`, `changes`, and `sections_changed`.
A digest with no audited provenance reports `unproven_recall_digest`; events
recorded before manifests existed degrade to the plain fresh/stale verdict
(`legacy_event: true`) instead of guessing. Exit code stays 0 either way.

## Dispatch-and-recall (next --claim --recall)

`recall` after `next --claim` is two round trips for what is one decision. With
`--recall`, dispatch embeds the full sealed recall bundle in the claim response
and audits it as `context_recalled` — one call takes work AND proves which
context it was taken against:

```bash
python3 "$A" next --claim --owner codex --recall --budget 8000 --related 5
```

The agent defaults to the claiming `--owner`; `--budget`, `--related`, and
`--related-scope` tune the bundle exactly like `recall`. The response carries
`recall` (the bundle) plus `recall_digest`, which is first-class provenance:
it passes `handoff-check`, can be cited by `handoff --recall-digest` /
`complete --recall-digest`, and is fresh per `recall-verify`. Without
`--recall` the output shape is unchanged; `--recall` without `--claim` is
rejected.

## Fleet freshness sweep (recall-stale)

`recall-verify` answers freshness for one task and one digest the caller
already holds; `ops.py recall-stale` answers the operator question across the
whole fleet. For every *live* handoff citing a `--recall-digest`, it recomputes
the task's current recall bundle exactly as it was originally recalled (the
audited event stores the bundle parameters) and compares digests:

- `fresh` — the cited recall's core context still matches current durable state
- `stale` — notes, receipts, lease state, or deps moved since; the item carries
  the recomputed `current_digest` so the next agent can re-recall before acting
- `unproven_recall_digest` — no audited recall/resume ever produced the digest
- `unknown_recall_params` — proven by a legacy pre-parameter-capture event;
  freshness cannot be recomputed exactly

Read-only: reports problems, never mutates.

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

## Temporal hybrid rerank

Pure BM25 is blind to time: a perfectly-matched note from months ago outranks
a fresh one, and stale facts are exactly what agents must not pack first.
Retrieval commands accept an opt-in hybrid re-scoring pass — lexical match ×
recency decay + pinned bonus:

```bash
python3 "$A" search-notes "postgres pool" --rank --rerank --recency-half-life-hours 24
python3 "$A" context <task-id> --related 5 --rerank
python3 "$A" recall <task-id> --agent codex --related 5 --rerank --pinned-boost 0.5
python3 "$A" next --claim --recall --owner codex --rerank
```

Design properties:

- **Hybrid score**: the best BM25 match in the candidate set normalizes to
  1.0 (LIKE-fallback rows count as 1.0 — that path is already newest-first),
  multiplied by an exponential recency decay (`--recency-half-life-hours`,
  default 168 = one week), plus a flat `--pinned-boost` (default 0.5) for
  pinned notes. Each row carries its `rank_score`; results sort by it,
  ties newest-first.
- **Deterministic digests**: note ages are floored to whole hours before the
  decay is applied, so a recall bundle's scores — and therefore its sealed
  digest — are stable within the hour instead of drifting on every
  recomputation. Identical state still yields an identical digest.
- **Provenance-preserving**: when a recall/resume uses `--rerank`, the
  half-life and boost are recorded in the audited `context_recalled` /
  `session_resumed` event, so `ops.py recall-stale` recomputes cited digests
  exactly as originally recalled; events recorded before this feature
  recompute unchanged (rerank off).
- **Opt-in and shape-stable**: without `--rerank`, every command's output
  (and digest behavior) is byte-identical to the pre-rerank semantics.

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

## Deferral (not_before)

A queued task can be parked out of dispatch until a future instant — useful
for "retry after the deploy window", rate-limited external calls, or scheduled
follow-ups. Unlike `block`, no reason or lifecycle change is involved; the
task stays queued and simply is not dispatched until its time arrives:

```bash
python3 "$A" defer <task-id> --owner hermes --until "2026-08-22T09:00:00Z"
python3 "$A" defer <task-id> --owner hermes --until ""      # clear the deferral
python3 "$A" create --project Trove --title "Follow up" --not-before "2026-08-25T00:00:00Z"
python3 "$A" update <task-id> --not-before "2026-08-23T12:00:00+02:00"
```

`next` skips deferred tasks (reason `deferred_until` with the `not_before`
timestamp under `--explain`) exactly like recovery backoff, while an explicit
`claim` remains allowed as a deliberate operator override. `metrics` reports
the count as `queued_deferred`. Every defer/clear is audited with the owner
and previous status.

## Dispatch aging (starvation guard)

Static priority ordering can starve old low-priority work when fresh P0/P1
tasks keep arriving. `next` therefore applies a virtual priority boost at
dispatch time: a queued task that has waited `--aging-minutes` (default 360)
per level since creation is promoted one effective level, up to
`--aging-boost` levels (default 2). A P3 that has waited 12+ hours dispatches
like a P1 without its stored priority ever being mutated:

```bash
python3 "$A" next --explain                       # defaults: 360 min/level, max boost 2
python3 "$A" next --aging-minutes 120 --aging-boost 3   # more aggressive fairness
python3 "$A" next --aging-minutes 0               # strict static ordering (old behavior)
```

With `--explain`, a boosted pick reports `effective_priority` and
`priority_boost`. Within one effective tier the longest-waiting task wins
(oldest `created_at` first), so equal-priority work drains FIFO instead of
last-touched-first.

## Task tags (capability/scope dispatch policy)

Tags are a lightweight vocabulary on tasks — `autopilot-safe`, `client:trove`,
`infra` — that turn dispatch policy into data instead of per-agent prompts.
An operator marks what each task is allowed for, and every agent constrains
itself with the same flag shape across Hermes, Claude Code, Codex, and
OpenCode:

```bash
python3 "$A" create --project Infra --title "rotate logs" --tag autopilot-safe
python3 "$A" tag t-123 --tag autopilot-safe --tag client:trove   # idempotent, audited
python3 "$A" untag t-123 --tag client:trove                      # audited; absent tag fails
python3 "$A" next --claim --owner codex --tag autopilot-safe     # scoped dispatch
python3 "$A" list --tag autopilot-safe                           # triage filter
python3 "$A" search "logs" --tag autopilot-safe                  # search filter
```

Tag-scoped dispatch is a hard filter: an agent constrained to
`--tag autopilot-safe` never receives untagged or differently-tagged work,
even when that work outranks it — an empty scope dispatches nothing rather
than leaking other work. Tags are validated to lowercase
`[a-z0-9:_./-]` (max 64 chars), which keeps them safe inside the JSON-array
LIKE filter and stable as CLI flags. Every task output exposes `tags` as a
JSON array, and tagging is audited (`task_tagged`/`task_untagged`) like all
state changes.

## Project dispatch policy (required tags & WIP caps)

Task tags put dispatch policy on the tasks; project policies put it on the
projects. The same `policies/<project>.yaml` files that gate merge/deploy
readiness can now also gate dispatch itself, so a project's rules hold no
matter which agent claims the work:

```yaml
# policies/client-trove.yaml
dispatch_requires_tag: client:trove   # only tagged work is dispatchable here
max_wip_per_owner: 2                  # an owner holds at most 2 live leases here
```

- `dispatch_requires_tag` — `next` skips the project's untagged tasks
  (`policy_missing_tag`, with the required tag, under `--explain`) and a
  direct `claim` refuses until the task is tagged. `--force` is the
  deliberate override; the override is recorded in the `claimed` audit event
  as `policy_overrides`, so forced past-the-gate claims leave provenance.
- `max_wip_per_owner` — counts an owner's live leases *within that project*
  (the global `--max-active` cap stays independent). At cap, `next --claim`
  skips the project's candidates (`policy_wip_cap`, with the held ids) and
  picks the best task elsewhere instead of failing after the pick — a
  multi-project dispatcher is steered toward work it may actually take. A
  direct `claim` at cap refuses with the held ids; `--force` overrides.

Both gates run before the lease is acquired, refusals are audited as
`claim_refused_policy` on their own connection (gate kind + held ids, never
resurrected by the rolled-back transaction), and `metrics` reports
`claims_refused_by_policy` fleet-wide. Policy-less projects behave exactly as
before; deleting a policy file reopens dispatch immediately. Like the seam
guard, `plan` remains a read-only simulation — policy enforcement happens at
dispatch/claim time against live state.

## Dependency priority inheritance (urgency flows upstream)

A P0 task is useless if its P3 prerequisite never gets dispatched. `next`
therefore walks the dependency DAG in reverse to a fixpoint: every queued
prerequisite inherits the urgency of its dependents, so if a P0 task depends on
a P2 which depends on a P3, all three dispatch at P0 urgency. Stored priorities
are never mutated — inheritance is a dispatch-time view, composable with aging
(the better of the two effective levels wins). Terminal dependents confer
nothing (their chain is already satisfied), and cycle-checked edges guarantee
the fixpoint terminates. With `--explain`, an inherited pick reports
`effective_priority` and `inherited_via` (the nearest dependent that conferred
the urgency):

```bash
python3 "$A" next --explain    # inherited_via shows which dependent made this urgent
```

## Dependency evidence inheritance

Priority flows upstream through the dep DAG; evidence should flow downstream.
When a prerequisite completes, its live handoff and latest sealed receipt are
the verified proof of what upstream produced — yet only *unsatisfied*
dependencies surface in context packs, so an agent picking up downstream work
starts blind to what it is building on. `--dep-context N` (on `context`,
`recall`, `recall-verify`, `resume`, and `next --claim --recall`) packs up to N
completed direct prerequisites into the bundle, each with its id, title, live
handoff (the resume point) and latest receipt (sealed evidence, payload
included), within the same character budget as everything else:

```bash
python3 "$A" recall <task-id> --agent codex --dep-context 3
python3 "$A" next --claim --owner codex --recall --dep-context 3
```

Design properties:

- **Opt-in and digest-sealed**: every output key is present only when the flag
  is used, so packs built without it stay byte-identical (and
  digest-compatible) to the legacy shape. Using the flag moves the sealed
  digest; `recall-verify` is fresh only under identical parameters.
- **Provenance-complete**: the flag value is recorded in the audited
  `context_recalled` / `session_resumed` payload, so `ops.py recall-stale`
  recomputes cited digests exactly and `recall-diff` reports a `dep_context`
  section (prerequisite evidence added/removed) like any other section.
- **Budget-honest**: each entry costs its real serialized size; under a tight
  budget entries drop out and `truncated` flags it rather than lying.
- **Deterministic**: prerequisites appear in dependency-edge creation order,
  so identical state yields identical bundles.

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
python3 "$O" notes-expired             # list live notes past their TTL (read-only)
python3 "$O" consolidate [--task ID] [--dry-run]   # merge near-duplicate notes into canonical facts
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

## Failure recording & retry budget (`fail`)

`complete` records success, but an agent that attempted the work and could not
finish had no first-class path — generic `update --status failed` silently lost
the attempt. `fail` is its counterpart:

```bash
python3 "$A" fail <task-id> --owner codex --reason "tests red after rebase"
python3 "$A" fail <task-id> --owner codex --reason "unrecoverable" --no-retry
python3 "$A" fail <task-id> --owner codex --max-retries 5 --backoff-base 120 --backoff-cap 7200
```

Design properties:

- **Lease-gated**: only the current holder of a live lease may record failure,
  fenced by `--epoch` exactly like `complete`; terminal tasks are final.
- **Shared retry budget**: each failure bumps `retry_count`, the same counter
  stale-lease recovery consumes, so an agent's failures and recoveries draw
  from one budget. While `retry_count <= --max-retries` (default 3) the task
  returns to `queued`.
- **Backoff, not hot-looping**: the task re-enters dispatch under the same
  deterministic exponential cooldown as recovery — `recover_after = now +
  backoff_base * 2^(retry_count-1)` seconds (default base 60s, capped at
  3600s; base 0 disables). `next` skips cooling-down tasks (reason
  `recovery_backoff` under `--explain`); a direct claim stays allowed as a
  deliberate override and any lease acquisition clears the cooldown.
- **Terminal escalation**: with the budget exhausted or `--no-retry`, the task
  goes terminally `failed` with the reason preserved in `blocked_reason`, and
  the response names `dependents_stranded` — direct non-terminal dependents the
  permanent failure froze — so an operator can cancel or re-plan them.
- **Audited & observable**: `task_failed` (retry scheduled) or
  `task_failed_terminal` in the hash chain; `metrics` reports
  `failures_retried_total` / `failures_terminal_total`.

## Deadline escalation (SLA sweep)

Dispatch orders by priority then earliest deadline, but a stale P3 task that
misses its deadline keeps losing dispatch races to fresh P2 work forever.
`ops.py escalate` is the SLA sweep: every non-terminal task whose `due_at` has
passed climbs exactly one priority level per pass (P3→P2→P1→P0), so repeated
passes converge an ignored overdue task toward the front of the queue:

```bash
python3 "$O" escalate            # bump all overdue non-terminal tasks one level
python3 "$O" escalate --dry-run  # preview the bumps without mutating
```

Design properties:

- **Convergent, not jumpy**: one level per pass keeps operator intent visible
  in the audit trail instead of slamming everything to P0 at once; tasks
  already at P0 are reported as `already_p0` rather than being silently stuck.
- **Terminal-safe**: completed/failed/cancelled tasks are never escalated even
  when overdue.
- **Audited**: each bump records a `priority_escalated` event with the old and
  new priority, the deadline, and `reason: overdue` in the hash chain.

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

## Portable work orders (cross-home task transfer)

Archives move *retired* fleet history; work orders move *live* work. A work
order is the provider-neutral unit of cross-boundary recovery: one task's full
execution state — the task row, dependency edges in both directions, complete
note and handoff history, receipts with their sealed files, and the heartbeat —
sealed under a single sha256 so any Autopilot home can verify integrity before
importing:

```bash
python3 "$O" export-task t1 --out /tmp/t1.json     # sealed autopilot-workorder-v1
python3 "$O" import-task /tmp/t1.json --dry-run    # seal check + merge preview
python3 "$O" import-task /tmp/t1.json              # merge into this home
```

Design properties:

- **Tamper-evident**: the sha256 seal covers every exported row and receipt
  file; `import-task` refuses a mutated file before touching the database, and
  `--force` does not bypass the seal.
- **Lease sanitization**: an imported task can never arrive still leased —
  `claimed`/`running`/`waiting_for_agent` reset to `queued` with lease fields
  cleared, because the previous owner does not exist in this home.
- **Idempotent recovery**: an identical re-import deduplicates (audited) instead
  of duplicating; a changed export refuses without `--force`, and `--force`
  merges rather than clobbers (local child rows are preserved, only new rows
  are inserted).
- **Dependency-aware**: dependency edges are inserted only when both endpoints
  exist locally; dangling ones are reported in `skipped_deps`, never silently
  dropped. Import prerequisite tasks first to carry the full graph.
- **Privacy boundary on both ends**: the same secret guard that protects
  shared-memory writes scans the whole document at export *and* again at
  import (so an `--allow-secret` override at the source cannot leak credentials
  into this home unnoticed). Default is refuse; `--redact` transfers
  `[REDACTED:<kind>]` copies; every decision is audited kind-only.
- **Atomic writes**: files are written via fsync + atomic rename with `0600`
  permissions; receipt files are restored only when absent locally.

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

## Audit checkpoints (truncation detection)

A hash chain is tamper-evident for *modification* but blind to tail
truncation: deleting the newest events leaves every remaining link perfectly
valid. Checkpoints close that gap by pinning the chain head — last event id,
head hash, and total count — into a self-hash-sealed
`autopilot-checkpoint-v1` file:

```bash
O=~/.hermes/autopilot/ops.py
python3 "$O" checkpoint                 # seal the current head (default under backups/)
python3 "$O" checkpoint-check <file>    # verify the seal + containment in the live chain
python3 "$A" verify-chain --checkpoint <file>   # chain recompute + checkpoint pin in one call
```

Design properties:

- **Divergence is proof**: a missing pinned event (`chain_truncated`), a
  changed head hash (`checkpoint_head_mismatch`), or a shrunken event count
  (`events_removed_since_checkpoint`) each prove history was deleted or
  rewritten after the checkpoint was sealed. Growth past the checkpoint is
  normal operation and never flagged.
- **Seal-then-compare**: the checkpoint file carries its own integrity hash;
  a modified file is refused outright rather than trusted.
- **Doctor-integrated**: `ops.py doctor` validates every
  `checkpoint-*.json` under `backups/` against the live ledger on every sweep,
  so a stale operator checkpoint turns truncation into a routine finding.

```bash
python3 "$O" doctor   # orphan deps, receipt index/file drift, audit chain, stale leases, note integrity, checkpoint pins
```

## Task lifecycle

```text
queued → claimed → running → waiting_for_user → completed
                         ↘ waiting_for_review
                         ↘ blocked
```

Terminal states are `completed`, `failed`, and `cancelled`.

## Receipts

Receipts are stored in `receipts/` and indexed in SQLite. Every new receipt is
**integrity-sealed**: the row carries a `file_hash` (sha256 of the exact file
bytes, also printed by the `receipt` command), and `ops.py doctor` re-verifies
each sealed file so silent corruption or tampering surfaces as a
`receipt_file_hash_mismatch` problem. Rows created before sealing
(`file_hash=''`) are skipped by the check. A completed engineering task should carry evidence such as:

- test/typecheck result
- commit SHA
- PR URL
- CI result
- deployment URL
- health-check result
- approval record

## Evidence-linked completions & policy-gated readiness

A self-report is never execution truth without a receipt. `complete` accepts
repeatable `--receipt <id>` flags citing integrity-sealed receipts **on the
same task**; unknown ids and other tasks' receipts are refused, and the cited
ids are recorded in the audited `completed` event so provenance survives in
the chain. Omitting the flag keeps the legacy shape byte-compatible.

Two observability paths make unverified completions visible instead of
aspirational:

- `metrics` reports `completions_without_receipt` — completed tasks with zero
  receipts (bare agent claims);
- `ops.py unverified-completions` sweeps the fleet read-only for both bare
  claims (`no_receipts`) and completions whose cited evidence later vanished
  (`evidence_receipt_missing`, e.g. deleted rows or partial restore).

Project policies can gate side-effectful readiness promises: with
`policies/<project>.yaml` containing `merge_requires_user: true` (or
`deploy_requires_user`), `update --status ready_to_merge|ready_to_deploy`
refuses until `--approved-by <name>` names who accepted it. The approver and
gate kind are recorded in the audited `updated` event. Re-stating the current
status is not a transition and stays ungated; projects without a policy file
behave exactly as before.

## Next integration

The hourly control tower should read this registry and report task changes. Existing project-specific policies should be added under `policies/` before allowing automatic side effects.
