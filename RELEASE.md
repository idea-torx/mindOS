# Release policy — MindOS Autopilot v3

**There is no public release yet.** This repository is private and source-available.

## Licensing

The code is offered under **FSL-1.1-MIT** (Functional Source License, version 1.1, with MIT
grant — see LICENSE). During the FSL restriction period the software is **not OSI open source**:
you may use, copy, modify, and distribute it subject to the license's competing-use restriction.
On and after each file's applicable two-year change date, that code becomes available under the
**MIT License** automatically.

## When releases begin

Any future public release will:

1. Tag a version on this repository (e.g. `v2.0.0`).
2. Carry a `CHANGELOG.md` entry describing everything in the tag.
3. State its exact FSL-1.1-MIT terms and the MIT conversion date per the LICENSE.
4. Pass `python3 verify.py` end-to-end on the tagged commit.

Nothing in this file grants distribution rights beyond those in LICENSE.
