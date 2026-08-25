#!/bin/sh
# Monitor script for the memory-consolidation cron.
#
# Hermes hashes this script's exact stdout bytes and suppresses the agent run
# entirely when they are unchanged since the previous tick. That is the whole
# cost-control mechanism: an idle store makes zero model calls, no matter how
# often the cron fires. It is why the schedule can be hourly instead of the
# seven days a fixed-interval job would need to stay affordable -- frequency is
# free when nothing changed.
#
# This prints content state only (live count, retracted count, newest
# timestamp). It must never print a timestamp of its own, a duration, or any
# derived value the session itself mutates: any of those would change on every
# tick and the gate would be permanently open, which is exactly the always-on
# behaviour the retired engine had.
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

exec python3 "$AP" memory-status --digest
