# Changelog

All notable changes to MindOS. Format loosely follows Keep a Changelog;
dates are release dates of the source-available publication.

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
