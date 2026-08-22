# OX Contract — Fable Full Wave (R2, R1, R3, R4, R7, §3 minimum slice)

You are the OX self-improvement worker for the IdeatorX Autopilot Control Plane
(MindOS execution core). You are an autonomous agent; your only user-facing output
is this contract plus your commits. Work sequentially through the rounds below.
Commit after every round that passes its gates. Never push to a remote.

## Environment facts

- Repo: isolated git worktree at your current working directory.
- Branch: `ox/fable-full-wave`, based on `2c1607a` (head of `overnight/autopilot-self-improvement`).
- Runtime: `python3 autopilot.py`, `python3 ops.py`, `python3 verify.py`.
- `verify.py` currently passes fully on this base. Confirm with a baseline run before round 1.
- The live homes `~/.hermes/autopilot` and `~/.hermes/mindos` are READ-ONLY for you.
  Round 5 (R7) may read them and may create scratch copies under `/tmp`, but must never
  write to, migrate, or mutate any live home. All destructive testing happens in temp dirs.

## Hard rules

1. Single-file shape stays: all runtime logic remains in `autopilot.py` and `ops.py`;
   tests stay in `verify.py`. No new modules, no packaging, no framework deps in the
   runtime path. Data files under `policies/repairs/*.yaml` are allowed (round 6).
2. Every commit message follows the existing house style: `feat:`/`fix:` prefix, one
   long semicolon-linked sentence covering what, why, and verification coverage.
3. After each round: run `python3 verify.py` — it must exit 0 before you commit.
4. If a round turns out to be already satisfied by the current code, verify that,
   note it in the commit message (`chore:`), and move on. Do not invent work.
5. If you cannot finish everything, finish rounds IN ORDER and leave the tree clean
   (all work committed) when you stop. A partial wave committed is better than an
   uncommitted sprawl.

## Round order and acceptance gates

### Round 1 — R2: verify.py reports every failure per run
- Add a minimal case registry inside `verify.py`: a `@case` decorator registering each
  test function; runner gives each case a fresh temp home; ALL cases run; failures are
  collected and reported at the end; non-zero exit if any failed.
- Add `--only <name>` to run a single case (for fast iteration).
- Preserve the existing black-box + in-process style of every existing assertion;
  convert existing tests to registered cases without weakening any assertion.
- Gate: full run green, failure-reporting demonstrated (temporarily break one case in
  a scratch check, confirm it reports alongside others passing, then restore).

### Round 2 — R1: split the two growth magnets
- Decompose `_build_pack` into a pack-spec-driven design: a small spec dataclass plus
  pure per-section builder functions, so adding a flag-gated section is one
  registration rather than threading kwargs through five signatures.
- Decompose `next_task` into stages: candidates → filter → rank → claim, each a named
  function with its own seam, callable/testable independently.
- Constraint: byte-identical observable behavior. Existing context packs, digests, and
  provenance records must not change shape; legacy packs must still diff fresh.
- Gate: full verify green including the false-staleness regression cases.

### Round 3 — R3: runtime self-description
- Add `autopilot.py protocol`: emits JSON describing the handoff field contract,
  recall/ack/receipt loop, status machine, refusal vocabulary, and flag-gated pack
  sections — generated from the code where possible so it cannot silently drift.
- Seal the protocol document with the house digest format (created_at outside digest).
- Gate: verify coverage that protocol output is stable across calls, digest verifies,
  and a tampered protocol file fails verification.

### Round 4 — R4: wire Hindsight as optional semantic recall
- Add an adapter behind the same shape as sessions: read-only semantic recall surfaced
  via `--related-semantic` on context/recall/recall-verify/resume/next --claim packs.
- Write-path: guarded retain of facts/decisions behind the same secret guard as notes.
- Graceful-unavailable path: if no Hindsight bank/config exists, the flag is a no-op
  and doctor reports hindsight as `unavailable` (healthy-with-note), never failing.
- Digest participation: semantic sections participate in pack digests with their own
  engine tag so staleness detection covers them.
- You do NOT have real Hindsight credentials in this environment; build against the
  documented bank format and prove the integration with a fixture bank in temp dirs.
- Gate: verify covers fixture-bank recall, secret-guard refusal, unavailable path,
  digest staleness on changed bank content.

### Round 5 — R7: live-fleet proof (READ-ONLY against live homes)
- Run `migrate-inventory` against the real `~/.hermes/autopilot` home producing an
  inventory artifact written INSIDE this worktree (not into the live home).
- Dry-run import into a scratch home under /tmp; then `onboard --probe` there.
- Report exact findings: schema version, counts, chain integrity result, anything the
  dry-run refuses. If the dry-run surfaces real defects, record them in STABILITY.md
  as findings; do NOT attempt fixes against live state.
- Gate: artifacts exist in-tree under `proof/` (inventory json, dry-run plan, probe
  report), committed; zero writes to any live home (double-check with `ls -la`
  timestamps if unsure).

### Round 6 — §3 minimum viable slice: sense → playbook repair → breaker → learning
- `ops.py sense`: one sweep reusing existing doctor/verify-chain/recall-stale/
  unverified-completions checks, emitting typed findings (id, severity, evidence,
  suggested repair kind). Findings are content-hashed for recurrence detection.
- Repair playbooks as data: `policies/repairs/*.yaml` with trigger finding kind,
  preconditions, command, required receipt kind, rollback command, blast-radius tier.
  Ship exactly two: Tier-0 FTS drift → rebuild FTS; Tier-1 stale lease → recover.
- Repairs execute as tasks in the normal queue (claim/lease/receipt lifecycle), gated
  by blast-radius tier policy in the same policies file shape as merge/deploy gates.
- Circuit breaker: a finding repaired N times (default 3) within a window disables the
  playbook, creates a P0 investigate task, and records the breaker in the fact graph
  with a validity window.
- Learning: successful repairs assert `finding:<hash> repaired-by <playbook>` triples
  into the fact graph.
- Verify coverage: sense detects injected faults; playbook executes end-to-end as a
  task with receipt; breaker trips and records; fact-graph learning entry lands;
  rollback command present and runnable in dry-run form.

## Finish report

When done (or stopping), print a compact summary: per-round status, commits created
(`git log --oneline`), final verify result, and any findings from round 5.
