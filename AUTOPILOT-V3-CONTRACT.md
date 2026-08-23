# Autopilot v3 continuation contract

Build Autopilot v3/MindOS continuation in this isolated worktree only. Do not touch live ~/.hermes, rollback, gateway, GitHub, or other worktrees. Do not deploy.

## Product goal

Complex work should continue without Leo repeatedly reigniting the conversation:
plan → implement → run → detect failure → correct → rerun → recap → continue → complete.

At the beginning of every autonomous run, state:
- exact model and provider
- that Leo explicitly granted autonomy
- task scope and permission boundary
- stop conditions and human seams

Return only after verified completion or a concrete human decision/blocker.

## Build from the OX audit

Implement the smallest clean phases that fit the existing contract substrate:

P0: additive autonomy_level/model_binding/recap metadata, grant facts, receipt and audit stamping, transition enforcement.
P1: bounded `ops.py nanny` tick using existing recover, sense, repair, leases, backoff, and dispatch primitives; double-run safe; no daemon.
P2: sealed recaps, explicit transient/infra/defect failure causes, correction-child tasks, continuation/handoff-before-stop lint.
P3: bridge staleness finding and optional notification seam only if small and justified.

Do not add a queue server or framework. Preserve fail-closed policy gates, dry-run-first behavior, prompt caching, role alternation, profile isolation, redaction, audit-chain integrity, and rollback.

## Human-like impulse review

Before implementation, ask: what safe additions make continuation feel more human rather than mechanical? Consider durable next-intent, context-aware progress language, bounded “I’m still working / I hit a snag / here is the decision I need” states, adaptive check-in cadence, momentum memory, and avoiding repetitive status spam. Only include additions that are deterministic, auditable, privacy-safe, bounded, and consistent with explicit human seams. Record suggestions separately from implemented scope and explain tradeoffs.

## Gates

Read AGENTS.md, OX_CONTRACT.md, STABILITY.md, and the existing context-injection contracts. Add focused tests for nanny races, autonomy/model declaration, grant expiry, handoff-before-stop, failure causes, correction tasks, recaps, breaker behavior, and human-like impulse output. Run py_compile, bridge/SQLite/gateway/context tests, full verify.py, sentinel, secret/PII scan, and git diff check. Commit with Leo Felix <leo@matteblack.io> only after all gates pass. Return exact files, tests, commit, human-impulse suggestions, and blockers.
