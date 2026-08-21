# MindOS GitHub publication contract

Prepare and publish the MindOS source as a private GitHub repository for continued development and review.

## Scope

- Read the existing runtime, README, STABILITY.md, policies, and migration docs.
- Add complete repository context without copying live state, receipts, personal memory, credentials, or private client data.
- Add the correct FSL-1.1-MIT license text from the official Functional Source License source. Describe the project honestly as source-available now and MIT after two years; do not call it OSI open source during the restricted period.
- Add or improve README, architecture overview, installation/migration guide, development guide, security/privacy boundary, contribution guide, roadmap, and changelog/release notes.
- Preserve the existing code and tests; do not invent features or claim live installation that has not been verified.
- Ensure .gitignore excludes state.db, temporal.db, receipts, sessions, Hindsight exports, backups, Keychain values, Claude memory contents, .env files, and runtime artifacts.

## GitHub publication

Create or use the private repository `idea-torx/mindOS` through authenticated `gh`. Set the remote and push the completed branch only after checks pass. Do not make the repository public. Do not print credentials or tokens.

## Verification

Run Python compile checks, the full verify suite, git diff check, and a secret/PII scan. Check tracked files for live state or credential-shaped content. Report exact repo URL, visibility, commit, files added, test results, and remaining limitations. No deploy or external side effects beyond creating/updating this private GitHub repository.
