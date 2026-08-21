# Roadmap

Honest status as of August 2026. Nothing below is claimed as done unless it
ships in this repository with verify coverage.

## Shipped (v1)

- Task registry with live leases, fencing epochs, retry budgets, backoff,
  dependency edges, impact analysis, dispatch planning, and metrics.
- Hash-chained audit stream with tamper detection (`verify-chain`).
- Sealed receipts and approval receipts (0600 files + sha256).
- Notes with supersession; provenance-linked temporal fact graph.
- Cross-agent handoffs with recall digests; session retention (`sessions-prune`).
- Secret guard (block/redact/audited override) over all free-text fields.
- Doctor health checks incl. FTS5 drift detection via fts5vocab.
- Dry-run-first migration layer: inventory, import, orphan-receipt
  quarantine, rollback journal, idempotent re-runs, source immutability proofs.
- End-to-end brain inventory across nine source kinds with sealed manifests.

## Next

1. **Hourly control tower** — read this registry and report task changes on a
   schedule without executing work.
2. **Cross-agent probe hardening** — broader post-install recall/handoff
   probes for additional agent runtimes.
3. **Policy expansion** — more project policies under `policies/` gating
   automatic side effects per client.
4. **Parallel MindOS home** — verified parallel installation into a fresh
   home with one-command rollback, before any default switch.
5. **Packaging** — single-command bootstrap and versioned release notes.

## Non-goals (v1)

Deploying, merging, sending external messages, submitting applications, or
running arbitrary agent commands from within MindOS itself.
