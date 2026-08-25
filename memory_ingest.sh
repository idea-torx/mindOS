#!/bin/sh
# Ingest script for the git/PR memory cron. Runs with --no-agent: there is no
# model in this job at all, so it is free to run often. Cadence here buys
# freshness, not tokens -- the separate consolidation job owns the LLM spend.
#
# Silence is the success case. Hermes treats empty stdout from a --no-agent
# job as "nothing to say" and delivers nothing, so a run that finds no new
# commits notifies you not at all. Anything this script prints WILL reach you,
# which is why it prints only genuinely new work and genuine failures.
#
# Every repository is attempted even when an earlier one fails. A `gh` outage
# on one project must not silently cost you the other four's commits, and
# `set -e` would do exactly that, so each call is guarded individually and the
# failures are collected and reported at the end.
#
# --since is deliberately left at its 30-day default rather than being narrowed
# to the cron interval. Re-reading a month of history every run is what makes
# this self-healing: a closed laptop, a failed network call, or a machine that
# was off all weekend costs nothing, because the next run backfills whatever
# was missed. The redundant ~99% is free -- memories are content-addressed on
# (project, content_hash) with the commit SHA inside the text, so re-ingesting
# an already-known commit is a no-op, not a duplicate. Steady state across all
# five repositories measures ~4s wall including the network PR fetch.
set -eu
# Hermes requires cron scripts to live under ~/.hermes/scripts/, so this file
# is installed away from the repo and cannot locate autopilot.py relative to
# itself. The path is therefore explicit, overridable, and checked -- a silent
# "command not found" here would look exactly like the (silent) success case
# and the store would quietly stop being fed.
AUTOPILOT_REPO="${AUTOPILOT_REPO:-/Users/leofelix/Documents/mindos-autopilot-v3}"
AP="$AUTOPILOT_REPO/autopilot.py"
if [ ! -f "$AP" ]; then
  echo "memory ingest: autopilot.py not found at $AP (set AUTOPILOT_REPO)"
  exit 1
fi

# Repository list, newline-separated absolute paths. The --project name is not
# passed: autopilot defaults it to the directory name, so a repo that moves on
# disk keeps its project identity and does not fork into a second project.
REPOS="${AUTOPILOT_INGEST_REPOS:-\
/Users/leofelix/Documents/mindos-autopilot-v3
/Users/leofelix/Documents/mindos-site-opencode
/Users/leofelix/Documents/byfelix-app
/Users/leofelix/Documents/OurPower-Website
/Users/leofelix/Documents/invoicer-pro}"

SINCE="${AUTOPILOT_INGEST_SINCE:-30 days ago}"

report=""
failures=""
total=0

# The list is newline-separated by construction, so split on newlines only:
# the default IFS would also split on spaces and silently truncate any
# repository path that contains one.
OLDIFS=$IFS
IFS='
'
for repo in $REPOS; do
  IFS=$OLDIFS
  [ -n "$repo" ] || continue
  name=$(basename "$repo")
  if [ ! -d "$repo" ]; then
    failures="${failures}  ${name}: not found at ${repo}
"
    continue
  fi
  if out=$(python3 "$AP" memory-ingest-git --repo "$repo" --since "$SINCE" --prs --apply 2>&1); then
    line=$(printf '%s' "$out" | python3 -c '
import json, sys
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(2)
n = d.get("ingested", 0)
if not n:
    sys.exit(0)
k = d.get("by_kind", {})
bits = ", ".join(f"{v} {name}" for name, v in sorted(k.items()) if v)
proj = d.get("project") or "?"
print(f"  {proj}: +{n} ({bits})")
') || {
      failures="${failures}  ${name}: unreadable ingest output
"
      continue
    }
    if [ -n "$line" ]; then
      report="${report}${line}
"
      total=$((total + 1))
    fi
  else
    # Keep the first line only: a gh traceback should not become the notification.
    why=$(printf '%s' "$out" | head -1)
    failures="${failures}  ${name}: ${why}
"
  fi
done
IFS=$OLDIFS

# Empty stdout = silent run. Only speak when there is new work or a failure.
if [ -n "$report" ]; then
  echo "git memory ingest -- new in ${total} repo(s):"
  printf '%s' "$report"
fi
if [ -n "$failures" ]; then
  echo "git memory ingest -- FAILED:"
  printf '%s' "$failures"
fi
