#!/bin/sh
# Session script for the memory-consolidation cron: gathers the work item and
# prints it for the partner model. It performs exactly one write -- computing
# embeddings for memories that lack them, which is derived data -- and makes no
# judgement of its own.
#
# Everything the model is asked to decide arrives as JSON on stdout. It does
# not get database access; its merges go back through `memory-retain` and
# `memory-forget`, so each one lands in the hash-chained audit ledger with a
# successor named. That is the difference from the retired engine, which
# rewrote a bank file out of band and left sealed context packs citing content
# that had silently changed underneath them.
set -eu
# Hermes requires cron scripts to live under ~/.hermes/scripts/, so this file
# is installed away from the repo and cannot locate autopilot.py relative to
# itself. The path is therefore explicit, overridable, and checked -- a silent
# "command not found" here would print nothing, and for the monitor script
# empty output is a *stable* hash, which would wedge the gate shut forever
# rather than fail visibly.
AUTOPILOT_REPO="${AUTOPILOT_REPO:-/Users/leofelix/Documents/mindos-autopilot-v3}"
AP="$AUTOPILOT_REPO/autopilot.py"
if [ ! -f "$AP" ]; then
  echo "memory cron: autopilot.py not found at $AP (set AUTOPILOT_REPO)" >&2
  exit 1
fi

AP="python3 $AP"
PROJECT="${AUTOPILOT_CONSOLIDATE_PROJECT:-}"
THRESHOLD="${AUTOPILOT_CONSOLIDATE_THRESHOLD:-0.85}"

# Bounded: a large backlog is worked off across ticks rather than stalling one.
$AP memory-embed --apply --limit 500 >/dev/null

echo '### memory store'
$AP memory-status
echo '### consolidation candidates'
if [ -n "$PROJECT" ]; then
  $AP memory-consolidate-brief --project "$PROJECT" --threshold "$THRESHOLD"
else
  $AP memory-consolidate-brief --threshold "$THRESHOLD"
fi
