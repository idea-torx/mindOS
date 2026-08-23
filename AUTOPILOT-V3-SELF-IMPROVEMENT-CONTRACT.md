# Autopilot v3 self-improvement contract

Use OpenCode Zen OX as the agent of choice. Work only in this isolated MindOS worktree. Do not touch live ~/.hermes, rollback, gateway, GitHub, or other worktrees; do not deploy.

## Goal

Design and implement a bounded self-improvement loop for Autopilot v3 that increases autonomy in failure states without creating an unbounded self-modifying agent.

The loop should:

- detect unresolved failures and stalled activity quickly;
- maintain durable activity reports so work cannot silently disappear;
- use context, prior attempts, receipts, and failure causes to propose and execute the next safe correction;
- continue through plan → act → verify → correct → rerun → recap when the task's explicit autonomy grant permits it;
- stop at real human seams with a clear decision request;
- express progress with compact human-like impulse states without repetitive spam;
- operate across Hermes, DSH, OpenCode, Codex, Claude Code, and future harnesses through a provider-neutral runner/receipt protocol.

## Required audit first

Audit current v3 task state, nanny, autonomy grants, model binding, receipts, recaps, handoffs, context injection, bridge, cron, and protocol surfaces. Write AUTOPILOT-V3-SELF-IMPROVEMENT-DESIGN.md separating facts, gaps, proposals, and rejected ideas.

## Structural areas to consider

- provider-neutral runner contract: model/provider, harness, session, workspace, task, autonomy grant, capabilities, timeout, receipt;
- activity heartbeat/report contract with last meaningful action, next intent, progress state, evidence, and stall deadline;
- failure taxonomy and correction strategy selection;
- bounded self-critique and next-action planning using durable evidence, not hidden chain-of-thought;
- failure-state autonomy levels and human approval seams;
- adaptive nanny cadence with quiet all-clear and escalating activity reports;
- correction child tasks, breaker behavior, max attempts, and model escalation;
- cross-harness adapters with one common receipt/provenance schema;
- self-improvement proposals that require tests, a bounded diff, and explicit promotion gates;
- replayable disposable fixtures and soak tests.

Do not invent a daemon or queue service. Reuse existing MindOS primitives and Hermes cron where possible, but keep the core runner protocol harness-neutral.

## Implementation policy

After the audit, implement the smallest useful vertical slice, preferably: provider-neutral activity/runner contract, failure-state continuation plan, bounded self-improvement tick, and activity report/receipt fixtures. Add a HUMAN-IMPULSE-AND-AUTONOMY-SUGGESTIONS section with implemented vs deferred ideas.

Run focused tests, full verify.py, bridge/context suites, compile, diff check, secret/PII scan, and any soak fixture. Commit with Leo Felix only when all gates pass. Return exact files, tests, commit, human-impulse improvements, deferred suggestions, and blockers.
