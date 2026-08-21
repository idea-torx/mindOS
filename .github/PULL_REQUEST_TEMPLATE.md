<!--
Thank you for contributing to MindOS. Before opening this PR:
- Run the full verification suite: python3 verify.py (must print "autopilot verification: PASS").
- Byte-compile all sources: python3 -m py_compile autopilot.py ops.py verify.py seed_current_state.py.
- Update README.md sections, STABILITY.md (fixed issues / accepted risks), and end-to-end verify coverage for any behavior change.
- Never commit secrets, live state databases, receipts, or absolute home paths. The secret guard's rules apply to contributions too.
-->

## Summary

What does this PR change and why?

## Contract compliance

- [ ] Dry-run-first preserved (new side effects gated behind explicit apply flags)
- [ ] Fail-closed semantics with exact blocker names
- [ ] New/changed side effects are audited
- [ ] Secret guard coverage unchanged or extended

## Verification

- [ ] `python3 verify.py` passes end-to-end
- [ ] `python3 -m py_compile` clean on all sources
- [ ] README + STABILITY.md updated
- [ ] No secrets, live state, receipts, or machine-local paths added
