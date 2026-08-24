# MindOS release — Autopilot local memory engine

## Highlights

This release retires the external Hindsight dependency and replaces it with a
memory engine that lives inside the control-plane database, so cross-session
recall is deterministic, offline, and inside the audit chain.

### Hindsight retirement

The dependency spanned three surfaces and all three are gone:

- `autopilot.py` — the `bank.jsonl` adapter is replaced by a `memories` table
  and a `memories_fts` FTS5 index, with a LIKE fallback where FTS5 is absent
  (engine tag `memory-fts-v1`);
- `mindos_bridge.py` / `mindos_sqlite_adapter.py` — the GET-only service probe
  and `hindsight-check` are removed, `bridge_hindsight_ledger` migrates to
  `bridge_export_ledger`, and `export` is a plain provenance manifest with an
  `export-status` command;
- `ops.py` — the `_brain_hindsight` HTTP probe, the shared-bank binding, and
  the `--hindsight-url` / `--bank` flags are removed, so `brain-inventory`
  makes no outbound call at all.

Being in-database is the structural change rather than an implementation
detail: a retain and its audit event now commit in one transaction, so an
orphaned memory line is impossible by construction.

### Memory commands

- `memory-retain`, `memory-forget`, `memory-list`, `memory-status`;
- `memory-import` brings a legacy bank across transactionally — dry-run by
  default, and the source file is never mutated.

### Optional embedding layer

- `embed_worker.py` runs `sentence-transformers` with `BAAI/bge-small-en-v1.5`
  out of process, because the system interpreter has no numpy; it speaks JSON
  over stdin/stdout and opens the database read-only so the audit chain keeps a
  single author;
- vectors are L2-normalised, so cosine similarity reduces to a dot product;
  clustering is a blocked similarity matrix plus union-find;
- the layer is entirely optional and off the context-pack path. Absent or
  unbuilt, every command still exits 0 and reports the gap under `notes`,
  never `problems`.

### Consolidation does not eat history

`commit` and `pull_request` memories are excluded from clustering by default,
filtered at the worker's candidate query so excluded kinds are never scored.
Measured on the live store, 63 of 122 clusters were pure git groups — PRs
\#220, \#221 and \#224 are three distinct releases that clustered as though
they were one, and merging them would delete events a build log is meant to
report. `--include-git` opts back in for a deliberate cleanup.

### Git ingest

`memory-ingest-git` records commits and, with `--prs`, merged pull requests, so a
writer agent can see across sessions rather than only within one. Ingest is
idempotent by content address: a `(project, content_hash)` unique index makes
re-ingest a free no-op, so there is no cursor or state file to corrupt.
`--since` delegates date parsing to git itself.

### Cadence scripts

- `memory_ingest.sh` — five repositories, per-repo failure isolation, silent on
  a no-op;
- `memory_monitor.sh` — content digest only, so an unchanged store suppresses
  the agent run entirely;
- `memory_session.sh` — embed backlog, status, consolidation brief.

## Verification

- Autopilot verification: 20/20, including `git_ingest_is_bounded_and_idempotent`
  and `memory_embedding_layer_is_optional_and_off_the_pack_path`;
- botmail, bridge, SQLite, gateway, and context suites passed;
- context-pack sentinel passed;
- secret scan clean.

## Rollout note

The runtime deployment and the repository source remain separate controls. The
ingest and consolidation cron jobs are installed on the live Hermes host at
`0 0,12 * * *` and `0 6 * * *` respectively; the consolidation job is
monitor-gated, and its first tick always runs the agent because no digest is
stored yet.
