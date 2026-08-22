# OX Contract — Overnight integration + live-fleet migration hardening

Worktree: `~/Documents/autopilot-overnight-r7-hardening`
Branch: `overnight/r7-hardening`
Base: `overnight/autopilot-self-improvement` at `2c1607a`
Model/provider: Hermes/OpenRouter `stealth/ox-alpha`

This worker owns the integration requested by Leo. Do not edit live homes until
all code gates and dry-run gates pass. Never push remotely.

## Part A — integrate the verified wave

Bring the completed wave history from `~/.hermes/autopilot-ox-wave` onto this
overnight line. Preserve the existing overnight commits and history; use a
normal merge or cherry-picks with explicit receipts. The wave commits to bring
over are the verified R1/R2/R3/R4/§3 plus hardening line, ending at:

- `4696864` R2 case registry
- `7d010af` R1 decomposition
- `87150ff` R3 protocol
- `f1017a5` R4 Hindsight
- `5fa50ad` §3 sense/playbooks/breaker/learning
- `b6f4e31` defect hardening (D1/D2/D3/D4)

Do not duplicate commits already reachable; resolve conflicts by preserving
newer overnight functionality and the verified wave behavior. Run the full
verification after integration.

## Part B — R7 live-fleet findings in ops.py

Implement and test these findings from `~/.hermes/autopilot-ox-r7/FINDINGS.md`:

1. **D1:** `_classify_sqlite` read-only discovery must handle WAL sidecars. Use
   a safe immutable/read-only fallback after integrity checks, without writing
   to the source home, and report the mode/result honestly in the sealed
   inventory.
2. **D2:** migrate-import dry-run must run `PRAGMA foreign_key_check` on the
   source and fail closed with explicit dangling-row findings before apply is
   offered. Confirm the real live shape has the three dangling references and
   preserve the source read-only boundary. Add a temp fixture regression.
3. **D3:** inventory must include receipt files in the checksummed source scope,
   or fail closed/warn before an import can insert receipt rows without files.
   Prefer including the receipt directory in the manifest and test missing-file
   drift. Add a temp fixture regression.

Do not silently repair or delete live orphan rows. The tool must detect and
refuse/describe them; live remediation remains a separate approved operation.

## Part C — real-home R7 rerun

After Part A+B pass in temp homes:

- Run the inventory script against the real `~/.hermes/autopilot` with output
  written under this worktree `proof/`, never into the live home.
- Run dry-run import against a scratch home under `/tmp`.
- If and only if the dry-run is clean and explicitly safe, run the documented
  apply/onboard/probe path against a new scratch destination, not the live home.
- Do NOT mutate or migrate the live home automatically. Produce a sealed report
  stating whether the live fleet is migration-ready and exactly what remains
  (D2 orphan rows are expected to block until Leo approves remediation).

## Gates

- `python3 -m py_compile autopilot.py ops.py verify.py`
- full `python3 verify.py` after integration and after D1-D3
- focused temp fixtures for WAL fallback, FK-orphan refusal, and receipt-file
  inventory/drift
- `git diff --check`
- clean worktree after commit, with proof artifacts committed
- final report: commits, tests, live-home read-only evidence, migration-ready
  yes/no, and exact blockers
