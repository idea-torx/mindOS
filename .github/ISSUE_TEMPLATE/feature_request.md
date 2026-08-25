---
name: Feature request
about: Propose a capability for MindOS Autopilot v3
title: ''
labels: enhancement
assignees: ''
---

**Problem to solve**
What gap in the durable operating system does this close? Which contract rule or accepted risk (see STABILITY.md) does it touch?

**Proposed behavior**
The command surface or semantics you'd expect. MindOS conventions to respect:
- dry-run first, apply behind an explicit flag
- fail closed, naming exact blockers
- every side effect audited; receipts as evidence
- secrets never enter shared memory (refuse / redact / audited allow)

**Alternatives considered**
Other approaches and why they're worse.

**Verification plan**
How the end-to-end verify suite would prove it.
