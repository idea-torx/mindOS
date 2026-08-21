# Architecture Overview

MindOS is a durable local coordination plane ("control tower") for autonomous
agent fleets. It answers one question continuously: *what is true right now,
and what evidence proves it?*

## Layers

```
┌──────────────────────────────────────────────────────────┐
│ CLI surface: autopilot.py (tasks) · ops.py (fleet ops)   │
├──────────────────────────────────────────────────────────┤
│ Guarantees: leases w/ fencing epochs · audit hash chain  │
│ receipt sealing · secret guard · FTS search              │
├──────────────────────────────────────────────────────────┤
│ SQLite home ($MINDOS_HOME): state.db + optional          │
│ temporal.db fact-graph sidecar                           │
├──────────────────────────────────────────────────────────┤
│ Migration/inventory layer: dry-run-first manifests,      │
│ rollback journal, orphan quarantine                      │
└──────────────────────────────────────────────────────────┘
```

### autopilot.py — execution truth

Tasks, claims, live leases with monotonic fencing epochs, heartbeats,
retry budgets/backoff, receipts (hash-sealed 0600 evidence files), notes with
supersession, a provenance-linked fact graph (temporal triples with validity
windows), handoffs between agents with recall digests, dependency edges,
impact analysis, dispatch planning, metrics, and a tamper-evident global audit
stream (`verify-chain` recomputes the hash chain).

### ops.py — fleet operations

Recovery of stale leases (guarded so mid-sweep claim losses are skipped and
reported), escalation, doctor health checks (including FTS5 inverted-index
drift detection via fts5vocab), project policies under `policies/`,
migration inventory/import/rollback, brain inventory across nine source kinds
(Autopilot, Hindsight binding, temporal sidecar, Claude memory sync, memory
archives, sessions, profiles/skills, cron definitions).

### verify.py — end-to-end verification

Self-contained suite that builds disposable fixture homes and exercises every
command, including failure injection, race injection, interrupted re-runs,
rollback to zero, source immutability, secret-guard refusal/redaction, FTS
drift, Hindsight-unavailable degradation, and cross-agent probes.

## Core invariants

1. **Single authority** — SQLite is the execution authority; Hindsight remains
   a shared semantic bank and is never copied into SQLite as a second one.
2. **Fail closed** — ambiguity/corruption refuses with exact blockers; absent
   optional sources are recorded honestly without blocking.
3. **Evidence over claims** — completions require sealed receipts; audit
   events are digest-only where values could be sensitive.
4. **Dry-run first** — destructive operations plan first, apply only on an
   explicit flag, journal for rollback, and re-run idempotently.
5. **Source immutability** — migration reads never mutate their sources;
   full-fixture hash comparisons prove it.

See `README.md` for command-level detail and `STABILITY.md` for the audited
risk ledger.
