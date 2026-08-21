# Combinatorial Architecture — why MindOS is more than separate features

MindOS is often described by its feature list: tasks, notes, handoffs,
receipts, facts, migration. That list undersells the system. The architecture
is **combinatorial**: eight orthogonal durable dimensions whose pairwise and
higher-order combinations produce capabilities that no single dimension (and
no plain Git + Markdown store) provides.

## The dimensions

```text
execution truth        × semantic memory
× temporal facts       × session/context history
× agents & handoffs    × evidence/receipts
× recovery/rollback    × policy/provenance
```

Each dimension alone is just data:

- **Execution truth** is rows in `state.db` (`autopilot.py` schema) — Git can
  store JSON, but it cannot atomically arbitrate two agents claiming one task.
- **Semantic memory** lives in Hindsight, bound provider-neutrally and never
  copied (`bindings/hindsight-shared-bank.json` from `ops.py brain-import`).
- **Temporal facts** are triples with validity windows in `temporal.db`
  (`fact-assert` / `fact-retract`).
- **Session/context history** is a rebuildable redacted cache over raw vendor
  transcripts (`session-ingest`) plus recall packs.
- **Agents & handoffs** are addressed, acked, superseded resume points.
- **Evidence/receipts** are hash-sealed 0600 files indexed in SQLite.
- **Recovery & rollback** are sealed manifests, journals, snapshots, archives,
  checkpoints.
- **Policy & provenance** are `policies/<project>.yaml`, tags, approval gates,
  and the hash-chained audit ledger.

## The combinations

| Combination | Capability | Where it is operational |
| --- | --- | --- |
| agent + task + lease | **Safe ownership** — exactly one holder, fenced epochs reject stale writers even under the same owner name | `autopilot.py claim/renew/transfer/resume`; guarded mutations; `verify.py` fencing tests |
| agent + task + receipt | **Evidence-backed completion** — completion cites same-task sealed receipts; unverified completions surface in `metrics.completions_without_receipt` and `ops.py unverified-completions` | `complete --receipt`; doctor re-verifies file hashes |
| memory + temporal fact + provenance | **Current-vs-historical truth** — retrieval packs only currently-valid triples while closed windows stay queryable as history | `facts --all`, validity windows, soft task provenance, `note-history` chains |
| session + context pack + handoff | **Resumable multi-agent work** — a killed session reconstructs objective, evidence, constraints, and lease state in one call | `recall`, `resume`, `handoff-current`, `context --related*` |
| migration + manifest + rollback | **Portable brain with reversion** — every applied import is precisely undoable via its journal; sources stay immutable | `migrate-inventory/import/rollback`; rollback-to-zero tests |
| policy + receipt + audit chain | **Enforceable definition of done** — required receipt kinds block completion; readiness gates require named approvers; every refusal is audited | `--requires-receipt`, `policies/*.yaml`, `completion_blocked_evidence` |

Higher-order combinations compound further:

- `lease epoch + audit chain + recover + backoff` = crash-safe dispatch that
  cannot hot-loop or double-claim (`ops.py recover`, deterministic cooldown).
- `context pack + digest + audit event` = *provable context*: a handoff or
  completion citing `--recall-digest` is checkable later by
  `ops.py recall-stale`, which recomputes the digest exactly as it was
  recalled from the audited bundle parameters.
- `secret guard + quarantine + kind-only reporting` = privacy boundary that
  holds across shared-memory writes, migration imports, work orders, and
  session ingestion — structural (restricted fact charset), not advisory.
- `FTS5 index + hybrid rerank + deterministic digests` = ranked retrieval
  whose scores are floored to stable units so sealed digests do not drift on
  recomputation.

## Why not "just use Git"?

Git/Markdown genuinely stores much of this data — MindOS's own docs live there
too. What plain storage lacks is precisely the combinatorial layer:

- **Concurrency**: atomic lease acquisition inside one SQL statement, seam
  conflict refusal for shared worktrees/branches, per-owner WIP caps.
- **Atomic transitions**: supersede-note, lease transfer, import merge — each
  is one transaction with guards against mid-flight change
  (`AND lease_owner=? AND lease_expires_at>?`).
- **Retrieval**: FTS5 BM25 ranking, cross-task related notes/handoffs/sessions/
  facts packed into budget-bounded prompt-ready bundles.
- **Policy gates**: dispatch-time tag/WIP/approval enforcement with audited
  overrides, before any side effect.
- **Receipts**: tamper-evident evidence files verified on every doctor sweep.
- **Temporal state**: validity windows make "what was true then" a query, not
  an archaeology project.
- **Recovery**: dry-run-first, sealed, journal-precise undo.

Remove any dimension and its combinations collapse: drop receipts and
completion becomes self-report; drop leases and dispatch double-claims; drop
validity windows and stale facts outrank fresh ones; drop the journal and
migration becomes one-way. The features are not the product — the
combinations are. Every combination above is exercised end to end by
`python3 verify.py` against disposable fixture homes; see the fact table in
[ARCHITECTURE.md](../ARCHITECTURE.md#fact-table) for claim → source → test
mapping.
