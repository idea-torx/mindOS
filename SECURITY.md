# Security & Privacy Boundary

## What MindOS will never do (v1 contract)

MindOS is a registry and evidence layer. It does **not**:

- deploy, merge, or push code
- send external messages or call external APIs for writes
- submit applications or take side effects on any external system
- run arbitrary agent commands
- access Keychain values or credential stores

## Data boundary

All state lives in a local SQLite home (`$MINDOS_HOME`, default
`~/.hermes/autopilot`). The repository itself must never contain:

| Never commit | Why |
| --- | --- |
| `state.db`, `temporal.db` | live execution truth / temporal facts |
| `receipts/`, `installation-reports/` | sealed evidence and inventory manifests |
| `sessions/`, semantic-memory exports | session caches and semantic memory |
| `backups/`, `runtime/` | machine-local recovery artifacts |
| `.env`, key files | credentials |
| Claude memory contents (`memory/`, sync JSON) | personal agent memory |

## Built-in secret guard

Every user-supplied text field passes `_secret_findings()` in `autopilot.py`
before persistence. It detects GitHub/Slack/Google-style tokens and generic
credential-shaped assignments. Findings either **block** the write (audited as
`secret_blocked`), **redact** it (`secret_redacted`), or pass verbatim only
under an explicit, itself-audited `--allow-secret` override (`secret_allowed`)
so fleet sweeps can still locate the value. Migration imports run the same
guard over free-form provenance fields.

## Reporting

Report suspected vulnerabilities privately to the maintainer rather than
opening a public issue. Include reproduction steps and affected commands.
