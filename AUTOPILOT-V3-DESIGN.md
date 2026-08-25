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

## Hindsight retirement — local memory engine

The external Hindsight dependency is gone. It spanned three surfaces and all
three were removed:

1. **`autopilot.py`** — the `bank.jsonl` adapter (`_hindsight_candidates`,
   `hindsight-retain`) is replaced by a `memories` table plus a `memories_fts`
   FTS5 index in the control-plane database. Retrieval is deterministic token
   matching: no model, no embeddings, no network.
2. **`mindos_bridge.py` / `mindos_sqlite_adapter.py`** — the GET-only service
   probe and the `hindsight-check` command are deleted;
   `bridge_hindsight_ledger` is migrated to `bridge_export_ledger` (rows carry
   over, `bank` becomes `channel`) and `export` is now a plain provenance
   manifest with an `export-status` command in place of the probe.
3. **`ops.py`** — the `_brain_hindsight` HTTP probe, the
   `bindings/hindsight-shared-bank.json` binding, and the `--hindsight-url` /
   `--bank` flags are gone. `brain-inventory` now makes no outbound call at
   all, and semantic memory is inventoried as a local `memories` source.

The legacy on-disk classifier (`hindsight_bank`) is deliberately kept: it is
how `migrate-inventory` discovers a legacy bank so `autopilot.py memory-import`
can bring it across.

### Why in-database rather than a better file

Being in-DB is the structural fix, not an implementation detail. It removes,
by construction, every failure mode the file/service design invited: retains
now commit with their audit event in one transaction (no orphan lines), the
store is sealed and backed up with everything else, a torn write is impossible,
an out-of-band edit can no longer silently invalidate a sealed pack digest, and
there is no shared external bank to bleed one context into another.

### Defects fixed in the same pass

- **Cross-project leak** — the retired recall fell back to unscoped results
  whenever its project filter matched nothing. Scope is now enforced.
- **Retrieval noise** — four near-identical tokenizers had drifted apart;
  matching was raw substring OR, so a task containing "the" recalled every
  memory containing "the". One shared `_search_tokens` now serves all
  collectors (legacy defaults preserved so sealed digests stay valid), and
  the memory engine opts into stopword/min-length filtering.
- **Unbounded read** — the bank was read fully into memory with no size cap.
- **Id collisions** — memory ids hashed timestamp+text, so same-second retains
  collided while the same fact retained a day apart duplicated. Ids are now
  content-addressed per project, making re-retain idempotent.
- **`bridge promote --kind task` was dead** — its INSERT had 14 columns, 7
  placeholders and 6 parameters, so every task promotion raised
  `ProgrammingError`; the literals were also shifted a column left, which would
  have written owner into status and status into priority.
- **Cross-channel export drain** — an export with no `--bank` treated the empty
  bank as a wildcard and marked every other bank's pending rows exported.
- **Permanently-stuck ledger rows** — a re-indexed transcript that got shorter
  left ledger rows for vanished message seqs; `export` inner-joins
  `session_messages`, so they could never be emitted and never left `pending`.
- **Export manifest permissions** — the manifest was written then chmod-ed to
  0600, leaving a world-readable window; it is now opened 0600 and fsynced.
- **Docstring/behavior contradiction** — `bridge_export` claimed unavailability
  left rows pending while marking them exported unconditionally.

### Structural guards against recurrence

`memories_fts` joins the doctor FTS drift sweep and `ops.py fts-rebuild`, so
the memory index cannot silently drift the way an unwatched file could;
`memory-list` gives the store an inspection surface it never had; `doctor`
reports a `memory_store` note with live/retracted counts and FTS readiness;
and retraction (`memory-forget`) is recorded rather than deleting, so the audit
chain still explains why a pack changed.

## Audit chain: concurrent-writer fork (found while auditing the engine)

`audit()` is a read-modify-write: it reads the current chain tail and appends
an event linked to it. Python's `sqlite3` opens a deferred transaction only at
the INSERT, so the tail read ran *outside* any write lock. Two processes
appending at the same moment both observed the same tail and both wrote an
event carrying that `prev_hash` — a permanent fork, since every later event
chains off one branch and `verify-chain` reports `broken_link` plus
`hash_mismatch` from that point on. Nothing heals it; the ledger is
retroactively unprovable.

This was live, not theoretical. It is what made
`nanny_bounded_double_run_and_impulse_states` fail intermittently at unmodified
HEAD (FAIL, PASS, PASS, FAIL, FAIL over five runs in a clean worktree): that
case is the only one deliberately running two `ops.py nanny` processes
concurrently, so it was the only one racing the ledger. The forked chain
surfaced as persistent `audit_chain_break` findings, which no playbook repairs,
so the tick reported `hit_snag` with four carried-over findings instead of the
expected `working`. The symptom looked like nanny nondeterminism; the cause was
one line below it in the stack.

Fix: `audit()` takes the write lock before reading the tail
(`BEGIN IMMEDIATE` when the connection is not already in a transaction —
callers that already wrote hold it, and `busy_timeout` makes a competing writer
wait rather than fail). Pinned by `audit_chain_survives_concurrent_writers`:
four processes × 25 appends each, asserting all 100 events land *and* the chain
verifies. Reverting the one guard reproduces `broken_link` immediately, so the
regression test is proven to bite.

## Semantic layer and monitor-gated consolidation (optional, additive)

Retiring Hindsight removed a real capability along with a broken one. FTS5
answers "which memories share these words"; it cannot answer "which memories
are about the same thing". The evidence for the gap is direct: the query
`rollback failure in production` returns nothing from keyword search, and
ranks `the deploy broke on the gateway lease path after the rollout` at
**+0.694** semantically, with no content word in common.

What the retired engine got wrong was not embeddings — it was where it put
them. It ran an always-on private worker calling a remote model on ingest.
Over five days its logs show 30 fact-extractions, 23 consolidations, 16
reflections, 1,638 service restarts (roughly one every four minutes), 212 hard
billing failures against the `opencode.ai` endpoint, and **three** retrieval
events. It paid continuously to curate a store that almost nothing read.

So the layer here is restructured around two constraints, both measured rather
than assumed.

**The model cannot be on the retrieval path.** Loading `BAAI/bge-small-en-v1.5`
takes ~6 s against a `recall` that completes end-to-end in ~95 ms. Therefore
`related_semantic` in a context pack stays FTS5, byte-identical to before, and
embeddings are exposed only through separate commands used by callers already
inside a long-running session. Verified: embedding a store leaves a pack's
`core_digest` unchanged, and `recall` still runs in 110 ms with vectors present.

**Curation must be an event, not a background process.** Memories sit inside a
pack's sealed core, so continuous rewriting would put every citing pack into
perpetual staleness. Consolidation is therefore a discrete session whose merges
land in the audit chain as a retain plus a retraction naming its successor.

### Shape

| | |
|---|---|
| `memory_vectors` | separate table, keyed `(memory_id, model)`; never columns on `memories`, whose row shape is sealed into pack digests |
| `embed_worker.py` | out-of-process, runs on the venv that already has torch; **reads only** — every write stays in `autopilot.py` and its chain |
| `memory-embed` | dry-run first; bounded by `--limit`; derived data, so safe to interrupt and re-run |
| `memory-search` | optional semantic retrieval; scope enforced, never relaxed to unscoped on an empty match |
| `memory-consolidate-brief` | near-duplicate clusters via blocked cosine + union-find; the deterministic half of consolidation |
| `memory-status --digest` | the cron's monitor line |

Nothing is installed. The worker reuses the virtualenv and Hugging Face cache
the retired service left behind, and forces `HF_HUB_OFFLINE=1`, so a missing
model surfaces as `model_unavailable` rather than a silent download. On a
machine with neither, every surface exits 0 with `available: false` and a
`note` — the graceful-unavailable discipline used everywhere else here.

### Why monitor-gated beats a seven-day interval

Hermes suppresses the agent run entirely when a monitor script's stdout bytes
are unchanged. So the schedule can be frequent — memory that lags a week is not
memory — while an idle store costs zero model calls. That only holds if the
gate is honest, which drives two decisions that are easy to get wrong:

- The digest carries **content state only** (live count, retracted count,
  newest timestamp). Vector counts and embedding backlog are excluded even
  though `memory-status` prints them, because the session itself changes them:
  a gate that re-opens on its own output is not a gate, it is a daemon with
  extra steps.
- It carries no timestamp, duration, or probe result of its own — any of those
  differ every tick and pin the gate permanently open.

Retraction is included, so the cycle settles: a merge moves the line once, the
next tick opens, finds nothing left to merge, and the gate closes. Verified
end-to-end — five memories with three phrasings of one fact consolidate to
three, the brief empties, the monitor line goes byte-stable, and `verify-chain`
stays `ok`.

### Git ingest — closing the writer-agent gap

The stated purpose (a writer agent producing factually accurate build logs)
needed commits and PRs to *be* in the store. `memory-ingest-git` puts them
there, and it is small for one reason: memories are content-addressed under a
uniqueness constraint, so **idempotence is free**. The commit SHA is embedded
in the memory text, which makes the same commit produce the same memory id
forever. No cursor, no state file, no lock, no "where did I get to last time".
A second `--apply` ingests zero rows.

It is a pull, not a post-commit hook. A hook would put an audit-chain write on
the critical path of every `git commit` — a locked database would start failing
commits — and would only ever see commits made after installation on this
machine. Reading history that already exists has neither problem, and it
backfills.

Two retrieval modes, deliberately distinct:

- **Enumeration** — `memory-list --kind commit --since DATE --project P`
  returns *every* matching commit in order. A build log must be complete;
  ranking it by relevance would silently drop the boring commits, which is
  precisely how a build log ends up factually wrong.
- **Semantic** — `memory-search` for "what do we know about X".

Bounding is enforced on both sources. `gh pr list` has no date filter of its
own, so `--since` is resolved via `git rev-parse --since=` (git's own parser,
not a second one that could disagree with the commit query) and applied to
`mergedAt`. Without it a single repository contributed **213** merged PRs
against 285 commits from five repos, and that volume alone would have biased
every later recall toward whichever project used PRs most. Skipped PRs are
reported, never silently dropped.

### Events are not restatements

Consolidation merges a cluster on the premise that it is one fact said several
ways, so collapsing it loses nothing. Git-derived memories break that premise.
Two pull requests merged the same afternoon with near-identical titles are two
releases, not one fact, and merging them deletes an event the build log exists
to report.

This was not hypothetical. On the live store at threshold 0.92, **63 of 122
clusters — 127 of 246 clustered memories — were pure commit/PR groups**. PRs
#220, #221 and #224 (three distinct releases on 2026-08-17) clustered as one.
Over half the consolidation workload was destructive, and it pointed at exactly
the records the writer agent depends on.

`memory-consolidate-brief` therefore excludes `commit` and `pull_request` by
default and reports the exclusion in `excluded_kinds`; `--include-git` opts back
in for a deliberate cleanup. The filter lives in the worker's candidate query,
so excluded kinds are never scored — this is not a post-filter the model could
be talked past.

### Installed

Two cron jobs, deliberately split so that cadence and cost are separate knobs:

| job | schedule | mode | cost |
|---|---|---|---|
| `git memory ingest` | `0 0,12 * * *` | `--no-agent` | ~4 s, no model |
| `memory consolidation` | `0 6 * * *` | monitor-gated | ≤1 agent run/day |

Ingest frequency buys freshness; only the monitor job's schedule buys tokens.
Midnight is the authoritative boundary for a writer agent that reports on the
previous day; noon exists only to keep interactive sessions current.

Ingest is silent on success — Hermes delivers nothing for a `--no-agent` job
with empty stdout — so it speaks only when commits actually landed or a
repository failed. It still writes one audit event per run either way, which is
deliberate: with a silent success path, the chain is the only evidence the job
is alive.

### Landed on the live system

The store was empty; it now holds **654 memories**, all embedded, chain intact
at 1,606 events (`verify-chain` ok). Backup taken first via the SQLite backup
API at `~/.hermes/mindos/state.db.bak-preimport-20260824-155556`.

| source | memories |
|---|---|
| legacy Hindsight bank (312 lines → 295 unique) | 295 |
| byfelix-app (74 commits + 74 PRs) | 148 |
| mindos-site-opencode | 88 |
| mindos-autopilot-v3 | 75 |
| OurPower-Website | 26 |
| invoicer-pro | 22 |

The legacy bank's 312 lines deduped to 295 on import — 17 were already
restatements of each other. `bank.jsonl` was opened read-only and its md5 is
unchanged.

Consolidation now has real work: at threshold 0.92, **122 clusters covering 246
of 654 memories**, still computed in 0.2 s. Some of that is genuine
duplication in the sources rather than drift — `mindos-site-opencode` carries
runs of commits with identical messages ("site: infinite loop cycle 2"), which
look like output from an automated loop and will cluster hard.

One caveat found while scanning for repos: `find -name .git -type d` misses
worktrees, where `.git` is a *file*. `mindos-autopilot-v3` is itself a worktree
and was invisible to the first scan.

### Measured against the real store

The live database currently holds **0 memories** — the rip-out landed but the
312-line legacy bank was never imported, so there is presently nothing for any
writer agent to read. Run against a byte-for-byte copy of the live database
(the original untouched, md5 unchanged):

- migration is clean: 1,599 existing audit events intact, `verify-chain` ok
- `memory-import` brings in 295 memories, 0 malformed lines, 0 secrets found
- embedding all 295 takes **16.7 s wall**, once, including model load
- `memory-consolidate-brief` over the whole store takes **0.18 s** — cluster
  mode never loads the model, it only reads stored vectors, so an open gate is
  nearly free
- at threshold 0.90 it finds **62 clusters covering 105 of 295 memories**:
  roughly a third of the store is near-duplicate, including several exact
  restatements that content-hash dedupe missed because they differ by project
  or by a few characters

One caveat on the monitor contract: Hermes requires monitor output to be
stable, and the digest does print `newest=<timestamp>`. That is a *content*
timestamp — the creation time of the newest memory — not a clock reading. It
changes only when a memory is added, which is exactly when the gate should
open.

## Gates

py_compile over all Python files; bridge/SQLite/gateway/context-injection
test suites; full `verify.py` 20/20 (including `memory_semantic_recall`, which
pins each retired-engine defect as a named regression,
`audit_chain_survives_concurrent_writers`,
`memory_embedding_layer_is_optional_and_off_the_pack_path`, and
`git_ingest_is_bounded_and_idempotent`); context-pack
sentinel;
secret/PII scan; git diff review.
Commit as Leo Felix only after everything is green. No deploy.
