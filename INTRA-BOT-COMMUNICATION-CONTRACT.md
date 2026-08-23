# Managed intra-bot communication contract

Implement the next MindOS capability for Hermes managed bot-to-bot communication in this isolated worktree only. Route the worker through Hermes → Nous Portal → stealth/ox-alpha. Do not touch live ~/.hermes, rollback, gateway, GitHub, or other worktrees; do not deploy from the worker.

## Context

Hermes now supports managed bot-to-bot communication with peer registration, peer rosters, capability epochs, bot mentions, and `hermes peer dm`. MindOS currently handles profile-scoped session ingestion, context packs, handoffs, leases, receipts, autonomy grants, model binding, and audit chains, but does not yet provide a dedicated bot-message envelope.

## Goal

Create a provider-neutral, profile-safe managed intra-bot message layer that can ingest and coordinate bot chat without confusing it with ordinary user chat or silently creating loops.

## Required design

Audit current Hermes bot-mode/peer protocol and MindOS bridge/handoff architecture first. Write INTRA-BOT-COMMUNICATION-DESIGN.md separating facts, gaps, and proposals.

Design/implement the smallest safe slice with:

- stable envelope: message_id, correlation_id, sender_bot, recipient_bot, peer identity, source profile, target profile, direction, capability epoch, timestamp, content class, provenance;
- explicit peer allowlist and capability/epoch validation;
- idempotent ingest and delivery receipts: accepted, rejected, duplicate, expired, failed;
- loop/replay budgets and correlation-chain limits;
- redaction/secret guard before storage or context injection;
- separation between bot chat, user chat, handoffs, and task receipts;
- autonomy/model/provider declarations for bot-originated actions;
- bounded context-pack inclusion with provenance and no cross-profile leakage;
- fail-open reply path with durable failure/audit state;
- cross-harness/provider-neutral protocol so DSH, OpenCode, Codex, Claude, and future agents can participate through the envelope, not Hermes-specific assumptions.

## Tests/gates

Add disposable fixtures for peer authorization, capability expiry, duplicate delivery, replay/loop refusal, profile isolation, redaction, receipt lifecycle, cross-harness envelope parsing, and failure recovery. Run py_compile, bridge/SQLite/gateway/context/full verify gates, secret/PII scan, git diff check, and a live-Hermes API read-only protocol probe if available. Commit with Leo Felix only after all gates pass. Return exact receipt, implementation phases, deferred risks, and what remains for live installation.
