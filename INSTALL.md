# Installation & Migration Guide

## Prerequisites

- Python 3.9+ (standard library only; no third-party packages required)
- macOS or Linux

## Install

MindOS is a local control plane — there is no server to deploy. Clone and run
against a dedicated home:

```bash
git clone <repo-url> mindos
cd mindos
export MINDOS_HOME="$HOME/.hermes/mindos"   # any writable path works
python3 autopilot.py init
python3 ops.py doctor                        # health-check the new home
```

`init` creates the SQLite schema (state.db, plus temporal.db when the fact
graph is used). All commands accept the home via `MINDOS_HOME` / 
`HERMES_AUTOPILOT_HOME`.

### Seeding an initial queue (optional)

`seed_current_state.py` demonstrates upserting a task queue without executing
any work. Point it at your own home and edit the item list first.

## Migrating from an existing Autopilot home

Migration is dry-run-first and never mutates its source:

```bash
# 1. Inventory every brain source (read-only, redacted, sealed manifest)
python3 ops.py brain-inventory --out inventory.json
python3 ops.py brain-inventory-check inventory.json   # verify the seal

# 2. Plan the execution-truth import against a disposable target
python3 ops.py migrate-import --dry-run ...

# 3. Apply only after the dry-run passes; journal enables rollback
python3 ops.py migrate-import ...
python3 ops.py migrate-rollback ...   # one-command recovery

# 4. Post-install verification
python3 ops.py doctor
```

Guarantees: sources stay byte-immutable (proven by hash comparison in
verify), orphan receipts are quarantined with full provenance instead of
breaking foreign-key integrity or losing evidence, re-runs are idempotent,
and rollback returns the target to its pre-import state including receipt
accounting.

## Upgrading an existing installation

Pull, run `python3 -m py_compile *.py && python3 verify.py`, then run
`ops.py doctor` on the live home before switching traffic. Schema changes ship
with migration coverage in the end-to-end suite.
