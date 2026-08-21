# IdeatorX Autopilot Control Plane v1

Durable local coordination layer for Hermes, Client Manager, Momentum Manager, cron jobs, and explicitly requested Claude Code work.

## Safety boundary

This v1 is a registry and evidence layer. It does **not** deploy, merge, send external messages, submit applications, or run arbitrary agent commands.

## Commands

```bash
A=~/.hermes/autopilot/autopilot.py
python3 "$A" init
python3 "$A" create --project Trove --title "Example task" --priority P1 --next-action "Inspect evidence"
python3 "$A" list
python3 "$A" claim <task-id> --owner hermes --minutes 30
python3 "$A" heartbeat <task-id> --owner hermes --note "Running verification"
python3 "$A" receipt <task-id> --kind verification --payload '{"tests":"pass","health":200}'
python3 "$A" update <task-id> --status waiting_for_user --next-action "Leo review"
python3 "$A" complete <task-id> --owner hermes --note "tests pass"  # requires live lease
python3 "$A" cancel <task-id> --owner leo --reason "obsolete"       # rejected on foreign leases
python3 "$A" show <task-id>          # task detail + receipts + audit trail + dependencies
python3 "$A" metrics                 # JSON observability snapshot
python3 "$A" verify-chain            # recompute the audit hash chain; report tampering
python3 "$A" dashboard
```

## Dependency-aware dispatch

Tasks can declare dependencies on other tasks. A task with an incomplete
dependency cannot be claimed, and `next` skips it when dispatching:

```bash
python3 "$A" create --project Trove --title "Dependent task" --depends-on <prereq-id>
python3 "$A" dep <task-id> <prereq-id>   # add a dependency edge (cycles rejected)
python3 "$A" next                        # highest-priority queued task whose deps are completed
python3 "$A" next --project Trove --claim --owner hermes --minutes 30
```

`next` orders by priority (`P0` first), then oldest-created. With `--claim`, the
picked task's lease is acquired atomically in the same step, so concurrent
dispatchers can never double-claim. `metrics` reports
`queued_blocked_by_deps` for tasks waiting on prerequisites.

## Safe operations (ops.py)

```bash
O=~/.hermes/autopilot/ops.py
python3 "$O" recover --max-retries 3   # requeue stale leases; fail tasks past retry budget
python3 "$O" approval approve <task-id> --by leo
python3 "$O" policy <project> <action> # check user-approval policy for an action
python3 "$O" processes                 # list active agent processes (read-only)
python3 "$O" morning                   # morning brief
```

`recover` consumes one unit of each task's retry budget per pass. Tasks whose
retry budget is exhausted (`retry_count > max-retries`, default 3) transition to
`failed` with reason `max lease retries exceeded` instead of looping forever.

## Lifecycle guardrails

- `complete` enforces claim-before-complete: only the current holder of a live
  lease may complete a task. Unleased, foreign-held, or expired leases are
  rejected.
- `cancel` is an operator transition that tolerates an unleased task but never
  overrides a foreign or expired lease.
- Any `update --status` to a terminal state (`completed`, `failed`,
  `cancelled`) releases the held lease so terminal tasks cannot look active.

## Audit integrity

Every audit event is linked into a SHA-256 hash chain (`prev_hash`, `hash`).
Existing databases are migrated and backfilled automatically on first use.
`autopilot.py verify-chain` recomputes the chain and reports any
`hash_mismatch` or `broken_link`, giving tamper-evident history. `ops.py
doctor` runs the same check as part of a broader consistency sweep.

```bash
O=~/.hermes/autopilot/ops.py
python3 "$O" doctor   # orphan deps, receipt index/file drift, audit chain, stale leases
```

## Task lifecycle

```text
queued → claimed → running → waiting_for_user → completed
                         ↘ waiting_for_review
                         ↘ blocked
```

Terminal states are `completed`, `failed`, and `cancelled`.

## Receipts

Receipts are stored in `receipts/` and indexed in SQLite. A completed engineering task should carry evidence such as:

- test/typecheck result
- commit SHA
- PR URL
- CI result
- deployment URL
- health-check result
- approval record

## Next integration

The hourly control tower should read this registry and report task changes. Existing project-specific policies should be added under `policies/` before allowing automatic side effects.
