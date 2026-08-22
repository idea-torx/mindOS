# OX Overnight R7 Hardening — Sealed Report

Date: 2026-08-21 (PDT, Vancouver). Branch `overnight/r7-hardening`.
Worktree: `~/Documents/autopilot-overnight-r7-hardening`.
Live home `~/.hermes/autopilot`: READ-ONLY throughout — zero writes; evidence below.

## Part A — wave integration

Merged the verified wave tip `b6f4e31` (case registry 4696864 → pack/dispatch
decomposition 7d010af → protocol 87150ff → Hindsight f1017a5 →
sense/repair/breaker/learning 5fa50ad → defect hardening b6f4e31) into this
overnight line via merge commit. Full verification immediately after the
merge: **10/10 cases PASS**.

## Part B — D1/D2/D3 in ops.py (all verified by new fixture case)

### D1 — WAL-safe read-only discovery (`_classify_sqlite`)
`mode=ro` now falls back to `mode=ro&immutable=1` when it cannot attach
(live WAL-sidecar homes), trusted only after `integrity_check` passes on that
same connection. Because `sqlite3.connect` is lazy, each candidate URI is
fully exercised before fallback. The sealed inventory reports honestly:
"discovery used immutable=1 fallback after mode=ro could not attach;
integrity_check passed on the immutable snapshot." Corrupted databases still
fail closed (fixture-proven).

### D2 — foreign-key orphan refusal (`migrate_import`)
Both dry-run and apply run `PRAGMA foreign_key_check` on the source and fail
closed naming every dangling row/table before any import is offered. The tool
detects and refuses; it never repairs live data — remediation stays a
separate approved operation. Fixture plants a dangling receipt + heartbeat
and proves refusal at both gates.

### D3 — receipt-file inventory scope + drift/import gates
Stage-one inventory now checksums + secret-scans `<source>/receipts/` inside
the autopilot source scope. Import additionally refuses pre-flight when a
receipt ROW whose task exists lacks its sealed file (rows for dead tasks
still follow documented orphan quarantine). Fixtures prove missing-file
refusal, drift visibility via manifest sha256, and a healthy control import.
Import-time source opens moved to immutable mode so apply cannot hit the D1
attach failure either.

## Gates

- `python3 -m py_compile autopilot.py ops.py verify.py` — OK
- full `python3 verify.py` — **11/11 cases PASS** (10 original + new
  `r7_live_fleet_hardening`)
- focused fixtures: `r7_live_fleet_hardening` — PASS
- `git diff --check` — clean

## Part C — real-home rerun (live read-only)

1. `ops.py migrate-inventory --root ~/.hermes/autopilot` against the LIVE
   home directly now SUCCEEDS (D1 fixed): healthy=1,
   counts {tasks 22, receipts 8, heartbeats 2, audit_events 55}, sealed at
   `proof/inventory-live.json` (sha256 7f6ee9d5…).
2. Live dry-run import refuses exactly as designed (D2): three FK violations
   — receipts rowid 1,2 + heartbeats rowid 1 pointing at deleted test tasks.
   Artifact: `proof/live-dryrun-refusal.txt`.
3. D3 confirmed live: receipt row `approval-test-bd5324c5-1787008811` has no
   file on disk (8 rows / 7 files) — would previously have imported then
   failed doctor forever.
4. Scratch end-to-end (byte-copy of live via immutable backup connection,
   approved orphan cleanup applied to the COPY only):
   onboard `--apply --probe` all 7 stages ok, verify-chain `{ok: true,
   events: 66, problems: []}`. Artifacts: `proof/inventory-scratch.json`,
   `proof/onboard-report-scratch.json`.
5. Live-home immutability evidence: tree digest of state.db + all receipt
   files after the entire run: deaa2f46c65e9fa9…; state.db mtime unchanged
   (Aug 21 20:36 PDT); live counts unchanged (22/8/2/55).

## Migration-ready verdict

**NO — blocked until Leo approves remediation.** Exact blockers:

1. Two orphaned receipt rows + one orphaned heartbeat referencing deleted
   test tasks (`test-f3259ac8`, `test-bd5324c5`) fail `foreign_key_check`;
   import refuses by design.
2. One receipt row (`approval-test-bd5324c5-1787008811`) has no sealed file
   on disk; import refuses rather than create unresolvable
   receipt_file_missing findings.

After an approved cleanup of those four rows in the live home (or explicit
approval to run the same copy-and-clean flow used for proof), the fleet
imports cleanly end-to-end — proven on the scratch copy.
