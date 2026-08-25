# Contributing to MindOS

Thank you for looking at MindOS. This repository is source-available under the
[FSL-1.1-MIT](LICENSE) license: you may use, modify, and redistribute it for
any non-competing purpose today, and it becomes fully MIT-licensed two years
after each release. It is **not** OSI open source during the restricted period.

## Ground rules

- Read `README.md` first — it documents every command and design rule.
- Preserve the safety boundary: MindOS v1 is a registry and evidence layer.
  It never deploys, merges, sends external messages, submits applications, or
  runs arbitrary agent commands.
- Fail closed. Where ambiguity is dangerous (migration imports, secret scans,
  rollback), refuse with an exact report rather than guessing.
- Every behavior change ships with end-to-end coverage in `verify.py`.

## Development workflow

1. Fork/branch from the default branch.
2. Make your change in `autopilot.py`, `ops.py`, or a new module.
3. Update `README.md` (command docs + design-rule sections) and `STABILITY.md`
   if the change closes a documented risk.
4. Add regression tests to `verify.py` on disposable fixtures only.
5. Run the full gate:

   ```bash
   python3 -m py_compile autopilot.py ops.py verify.py seed_current_state.py
   python3 verify.py          # prints: autopilot verification: PASS
   ```

6. Scan your diff for secrets/PII before committing. Never commit state.db,
   temporal.db, receipts, session caches, semantic-memory exports, Keychain values,
   `.env` files, or anything under `installation-reports/`.
7. Open a pull request describing the behavior, tests added, and risks.

## Commit style

One coherent change per commit. The message states the problem, the smallest
principled architecture that closes it, the guarantees preserved, and the test
coverage added — see existing history for the house format.
