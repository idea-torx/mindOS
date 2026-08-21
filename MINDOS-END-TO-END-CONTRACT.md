# MindOS end-to-end brain migration and audit contract

Implement and verify the next stage of the MindOS installation without touching live state.

## Goal
Extend the installer from Autopilot execution-state migration to a safe end-to-end brain migration plan covering semantic memory, temporal facts, session cache, profiles, skills, cron definitions, and Claude memory sync metadata.

## Live sources (read-only only)
- Current Autopilot: /Users/leofelix/.hermes/autopilot
- Hindsight service: http://127.0.0.1:8888, bank autopilot-shared-context
- Claude memory: /Users/leofelix/.claude/projects/*/memory/
- Claude sync state: /Users/leofelix/.hermes/hindsight/claude-memory-sync.json
- Temporal sidecar: current autopilot temporal.db if present
- Hermes profiles and cron definitions under /Users/leofelix/.hermes

Do not mutate these sources, access Keychain values, print secrets, call external APIs for writes, or alter Hermes defaults.

## Required behavior
Add a dry-run-first, manifest-backed end-to-end migration layer that:
- inventories each source with redacted counts, checksums, health, and provenance
- distinguishes execution truth, semantic memory, temporal facts, raw session cache, human archive, profile/cron definitions, and sync metadata
- imports only into an explicit new MindOS home or disposable target
- preserves source provenance, supersession, dates, scope, and hashes
- is idempotent and resumable
- refuses ambiguity/corruption and reports exact blockers
- never deletes source data
- supports rollback or explicit non-destructive quarantine for unsupported records
- verifies Hindsight health and shared-bank accessibility without duplicating or mutating the live bank
- supports a post-install cross-agent recall/handoff probe

Where Hindsight is a shared service rather than a file store, implement a safe verification/export manifest or provider-neutral adapter rather than pretending a local copy exists. No semantic memory should be copied into SQLite as a second authority.

## Installation authorization

After the isolated implementation passes its audit and disposable migration tests, proceed with the full parallel installation into `/Users/leofelix/.hermes/mindos`. Preserve `/Users/leofelix/.hermes/autopilot` as the immutable rollback source and do not replace, delete, or mutate its logical data. The new MindOS home must receive the verified execution state, Hindsight connectivity/brain manifest or safe shared-bank binding, temporal sidecar, Claude memory sync metadata, session cache where supported, profiles/skills/cron definitions, and provider-neutral handoff protocol. Use explicit manifests, dry-run first, apply only after dry-run passes, run doctor and cross-agent probes, and write a sealed install report. Switch Hermes defaults only after the new home passes health checks; retain a one-command rollback to the old home. If a source cannot be safely copied, bind, or migrated, preserve it in place with a clear report rather than inventing a duplicate authority.

## Audit gates
Run full compile/tests, migration dry-run and apply against disposable targets, interrupted re-run, rollback, source immutability, secret scan, FTS drift, Hindsight unavailable behavior, cross-agent probe, and post-install doctor. Produce a concise stability and migration report. Commit only verified implementation changes on this branch; do not push or deploy application code.