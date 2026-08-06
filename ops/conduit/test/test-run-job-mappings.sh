#!/bin/sh
set -eu

runner=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)/bin/polyedge-run-job
sh -n "$runner"
for job in prospective chart-backfill backfill; do
  grep -F "  $job)" "$runner" >/dev/null
done
grep -F 'if [ "$job" = origin-check ]; then' "$runner" >/dev/null
grep -F 'POLYEDGE_ORIGIN_EXPECTED_COUNTRY must equal CO' "$runner" >/dev/null
grep -F 'getent ahostsv4 polymarket.com' "$runner" >/dev/null
grep -F -- '--network polyedge' "$runner" >/dev/null
grep -F 'origin.country !== "CO" || origin.ip !== expectedIp' "$runner" >/dev/null
grep -F 'POLYEDGE_RAW_EVENT_PREFIX%/}/' "$runner" >/dev/null
grep -F 'set POLYEDGE_RAW_EVENT_PREFIX for Azure upload freshness' "$runner" >/dev/null
grep -F 'POLYEDGE_LOCAL_RAW_ROOT%/}/$DAY/$HOUR/' "$runner" >/dev/null
grep -F 'POLYEDGE_LOCAL_RAW_ROOT must equal /input/events' "$runner" >/dev/null
grep -F 'set -- --volume "$ring/segments:/input/events:ro,Z"' "$runner" >/dev/null
test "$(grep -c 'cpus=1.5 memory=' "$runner")" -eq 3
grep -F '*) work=$ring/jobs/research credential=research ;;' "$runner" >/dev/null
grep -F 'shadow-qset) work=$ring/jobs/shadow-qset credential=shadow-qset ;;' "$runner" >/dev/null
