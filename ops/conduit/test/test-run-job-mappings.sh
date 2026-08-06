#!/bin/sh
set -eu

runner=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)/bin/polyedge-run-job
sh -n "$runner"
for job in prospective chart-backfill backfill; do
  grep -F "  $job)" "$runner" >/dev/null
done
grep -F '*) work=$ring/jobs/research credential=research ;;' "$runner" >/dev/null
grep -F 'shadow-qset) work=$ring/jobs/shadow-qset credential=shadow-qset ;;' "$runner" >/dev/null
