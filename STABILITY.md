# Stability report — audit & hardening phase

Produced by the audit round following the verified migration/install checkpoint
(inventory → import → rollback). Scope: full read of `autopilot.py`, `ops.py`,
`verify.py`; failure-injection and race-injection exercises added to `verify.py`.
This report separates **fixed issues**, **accepted risks**, and **merge blockers**.

## Fixed issues (this round)

1. **`ops.py policy` ignored `HERMES_AUTOPILOT_HOME`** — it hardcoded
   `Path.home()/'~/.hermes/autopilot/policies'` (a path that cannot exist, since
   `~` was nested inside an absolute path), so every policy query reported
   "no project policy" regardless of real gates. Now resolves through
   `autopilot.POLICIES` like every other command.
2. **Lost-update races in `complete` / `fail` / `release`** — the lease guard ran
   on a row read, then the UPDATE landed unconditionally by id. A lease that
   expired and was re-acquired (or transferred) between read and write could be
   clobbered by the stale holder. All three mutations are now guarded
   (`WHERE ... AND lease_owner=? AND lease_expires_at>?`) and refuse with
   "lease changed since check" when the seam moved underneath them.
3. **`ops.py recover` could wipe fresh leases** — the sweep planned from a
   snapshot SELECT, then UPDATEd unconditionally; a worker claiming a task
   mid-sweep lost its lease. Updates are now guarded on the snapshot's owner and
   expiry; tasks that moved mid-sweep are reported in a new additive `skipped`
   key. The sweep is also deterministically ordered (`ORDER BY id`).
4. **`ops.py escalate` bumped tasks that settled mid-sweep** — same pattern;
   now guarded on observed priority + open status, with `skipped` reporting.
5. **Duplicate `create --id` crashed with a raw IntegrityError traceback** —
   now a clean `task id already exists` refusal.
6. **`ops.py approval` inserted receipt rows with no file or hash**, poisoning
   every later `doctor` run with permanent `receipt_file_missing` findings.
   Approval receipts are now sealed exactly like `receipt`: atomic 0600 file,
   sha256 recorded in `file_hash`, row linked via `last_receipt`.
7. **`tag`/`untag` were unguarded read-modify-write** — concurrent tag writes
   silently dropped one side. Both are compare-and-swap on the observed tag set
   now, refusing with "tags changed concurrently; retry".
8. **`_load_result_doc` tracebacks** — malformed JSON or a missing
   migration-result file now fail with clean SystemExit messages.
9. **`doctor` FTS drift checks were ineffective for external-content tables** —
   `COUNT(*)` over an external-content FTS5 table reads through to the content
   table, so index drift was invisible (proven experimentally). All three tables
   (`notes_fts`, `tasks_fts`, and previously-unchecked `handoffs_fts`) are now
   compared against the true inverted-index contents via `fts5vocab(...,'instance')`
   (temp-table only — the swept database is never written), naming
   `missing_from_index` / `stale_in_index` rowids, with a count fallback when
   fts5vocab is unavailable.
10. **`ops.py` was not importable** — module-level `parse_args()` consumed the
    importer's argv. Now guarded by `__main__`, enabling in-process testing.
11. **Flag-gated recall sections were dropped on recompute (false staleness)**
    — `recall-diff` and `ops.py recall-stale` both rebuild the cited recall
    bundle from the audited parameters but never passed
    `--related-sessions` through, so any recall made with that flag recomputed
    a *different* bundle: `recall-diff` reported fresh recalls as stale, and
    `recall-stale` flagged live handoffs as drifted with zero real context
    change. Both paths now pass every flag-gated section parameter
    (`related_sessions`, and the new temporal-fact `related_facts`) exactly
    as recorded; regression coverage proves a `--related-sessions` +
    `--related-facts` recall diffs `fresh` immediately after being taken and
    only turns stale when genuinely new matching context arrives.
12. **Migration-inventory seal covered `created_at`, breaking its own resume
    contract** (found by `verify.py` failing at HEAD) — the sha256 seal was
    computed over a body that included the run timestamp, so two runs over
    identical trees produced different seals and could never "reproduce an
    identical manifest modulo created_at" as documented; every re-run looked
    tampered to strict byte comparison. Both sealing (`migrate_inventory`) and
    verification (`_load_inventory`) now exclude `created_at` from the digest;
    all content fields remain covered and the tamper-refusal test still passes.

## Accepted risks (documented, not fixed)

- **Receipt-file write happens after commit** (`receipt`, `approval`): a crash
  in between leaves a row without its file. `doctor` detects this as
  `receipt_file_missing`; remediation is manual re-seal. Moving the write inside
  the transaction would trade a detectable gap for an un-detectable one.
- **`update --status` remains a permissive operator escape hatch**: it can move
  terminal tasks back to queued and does not manage leases beyond clearing them
  on terminal transitions. This is long-standing operator surface; agents are
  expected to use `claim`/`complete`/`fail`.
- **`ack_handoff` is last-writer-wins** under a same-instant double-ack race;
  both outcomes are semantically "acked", so no fencing was added.
- **`_critical_path` recursion depth** tracks chain length; a >1000-deep
  dependency chain would hit Python's recursion limit before producing wrong
  output (it fails loudly, not falsely).
- **`ops.py sentry`/`github`/`processes` touch live systems by design**
  (Keychain, Sentry API, `gh`) and remain operator-invoked only; the contract's
  live-system boundary holds because rounds never invoke them.
- **FTS5 absence degrades to LIKE search** everywhere (unchanged); BM25 scores
  and digests are only comparable within the same engine mode.
- **SQLite single-writer serialization** is assumed; `busy_timeout=10000`  absorbs contention, but very long transactions (migration import) can still
  starve concurrent writers for their duration.
- **The session cache grows without TTL** — ingested transcripts stay until a
  future consolidation/retention pass. This is bounded by explicit `--root`
  ingestion choices and the rows are disposable by design (`DELETE FROM
  sessions` cascades messages and the FTS triggers keep the index in sync);
  no automatic retention was added because deleting conversation history
  silently is exactly the kind of false-success behavior this runtime avoids.

## Fixed in the fact-graph transfer round (previously an accepted risk)

1. **Fact-graph rows were not carried by migration import or work orders**
   (accepted risk from the audit round) — both boundaries now move the
   temporal fact graph: work orders carry facts provenance-linked to the
   exported task with validity windows byte-intact and deduplicate by fact
   id on re-import; `migrate-import` carries the whole graph idempotently
   (fleet-level facts included), treats a source predating the facts table
   as legacy shape rather than corruption, scans the free-form `source`
   field through the same secret guard as notes, and journals imported
   facts for rollback; `migrate-rollback` blocks on local facts whose soft
   provenance would dangle and deletes them explicitly under `--force`
   (facts have no FK cascade by design). Regression coverage spans both
   boundaries: window survival, dedup re-import, guard refusal via a
   credential-shaped `source`, journal coverage, local-fact blocker +
   forced cascade accounting, exact post-rollback zero, and clean import of
   a dropped-facts-table legacy source.

## Merge blockers

None found. The audit hash chain, receipt sealing, work-order/migration seals,
and secret guard all held under tamper injection (chain rewrite, tail truncation,
checkpoint divergence, tampered manifests/result docs, redaction bypass attempts).

## Verification added

- Black-box: policy env resolution, duplicate-id refusal, sealed approval
  receipts + clean doctor, malformed/missing result-doc handling,
  handoffs_fts drift detection + rebuild repair.
- In-process race injection (fresh isolated home): stolen-lease refusals for
  `complete`/`fail`/`release` proving the thief's claim survives; recover
  skipping a mid-sweep fresh claim; escalate skipping a task that settled
  mid-sweep; tag compare-and-swap refusal.
- Session-ingestion adapter (black-box, isolated fixture store): redacted
  scan inventory with kind-only findings and value-never-leaves assertion;
  dry-run writes nothing; credential refusal fail-closed; `--redact` stores
  placeholders with the raw value provably absent from the cache; tool-result
  skipping / duplicate collapse / malformed counting; idempotent re-run;
  atomic re-index of a changed source; role-filtered FTS search; flag-gated
  context-pack integration with digest staleness on newly ingested sessions;
  `--since` plan filtering; byte-level proof that sources are never mutated.
- Inventory seal fix: determinism assertion (identical trees → identical
  manifest modulo created_at) now passes; tampered manifests still refused.
