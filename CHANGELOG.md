# Changelog

All notable changes to MindOS. Format loosely follows Keep a Changelog;
dates are release dates of the source-available publication.

## [0.5.0] — 2026-08-24

Publication catches up to the active development line, which had moved four
releases ahead. This entry covers v0.3.0 through v0.5.0.

### Added
- **Local memory engine** — an in-database `memories` table with a
  `memories_fts` FTS5 index (LIKE fallback where FTS5 is absent), engine tag
  `memory-fts-v1`. Commands: `memory-retain`, `memory-forget`, `memory-list`,
  `memory-status`, `memory-import`.
- **Git ingest** — `memory-ingest-git` records commits and merged pull
  requests so cross-session work is visible to later sessions. Idempotent by
  content address, so re-ingest is a no-op and no cursor file exists to
  corrupt.
- **Optional embedding layer** — `embed_worker.py` runs sentence-transformers
  out of process and read-only, adding `memory-embed`, `memory-search`, and
  `memory-consolidate-brief`. Entirely optional and off the context-pack path.
- **Autopilot v3 continuation** — bounded nanny tick, autonomy declarations
  with model binding and human grant windows, four-state impulse reporting,
  `runner/v1` execution receipts, activity/stall reports, and bounded
  correction children.
- **Managed intra-bot communication** — provider-neutral botmail envelopes,
  peer allowlists, capability epochs, and idempotent receipts.
- **Hermes session-start context packs** — one bounded, digest-sealed pack on
  the first turn of a session, with provenance and an end-to-end opt-out.
- CI now byte-compiles every module, runs the six component suites and the
  context-pack sentinel, and then the full verification suite.

### Changed
- **Hindsight is retired.** The external semantic-memory dependency is removed
  from all three surfaces it spanned: the `bank.jsonl` adapter in
  `autopilot.py`, the service probe and `hindsight-check` in the bridge, and
  the `_brain_hindsight` probe plus `--hindsight-url` / `--bank` flags in
  `ops.py`. `brain-inventory` now makes no outbound call at all. Retrieval is
  deterministic, offline, and inside the audit chain; a retain and its audit
  event commit in one transaction, so an orphaned memory line is impossible by
  construction. Legacy banks import through `memory-import` — dry-run by
  default, and the source file is never mutated.
- `bridge_hindsight_ledger` migrates to `bridge_export_ledger` (`bank` becomes
  `channel`); `export` is a plain provenance manifest with `export-status`.
- Consolidation excludes `commit` and `pull_request` memories by default:
  measured on a live store, 63 of 122 clusters were pure git groups, and
  merging them would delete the very events a build log reports.
  `--include-git` opts back in.
- Audit append takes `BEGIN IMMEDIATE` before the tail read, fixing a race
  where concurrent ticks forked the hash chain.

### Security
- `proof/` (live inventories and scratch reports carrying machine-local paths)
  is removed from the published tree and ignored going forward, alongside
  `installation-reports/`.
- The `memory_*.sh` cron scripts are not published: they carry machine-specific
  repository paths. The commands they wrap are documented instead.

## [Unreleased]

### Added
- Publication layer: official FSL-1.1-MIT LICENSE (source-available now,
  MIT after two years per release), CONTRIBUTING.md, SECURITY.md,
  ARCHITECTURE.md, INSTALL.md, ROADMAP.md, CHANGELOG.md, hardened .gitignore
  excluding live state, receipts, sessions, Hindsight exports, backups,
  Keychain values, agent memory contents, and .env files.
- `seed_current_state.py` now resolves its home via `MINDOS_HOME` /
  `HERMES_AUTOPILOT_HOME` instead of a hardcoded machine path.

### Security
- Removed tracked installation-report manifests (machine-local inventory
  artifacts) from the published tree; they remain ignored going forward.

## [Initial publication baseline]

Pre-publication history (see git log for the full audited trail):
- Autopilot control plane v1: tasks, leases with fencing epochs, receipts,
  audit hash chain, notes, fact graph, handoffs, dispatch planning, metrics.
- Fleet ops: recover/escalate/doctor/policies with guarded sweeps and FTS
  drift detection.
- Migration layer: brain inventory (nine source kinds), dry-run-first
  import/export, orphan-receipt quarantine, rollback journal, idempotent
  re-runs.
- Session retention (`sessions-prune`) with mandatory age bounds.
- Full end-to-end verification suite (`verify.py`) incl. failure/race
  injection; stability audit documented in STABILITY.md.
