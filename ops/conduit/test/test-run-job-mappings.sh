#!/bin/sh
set -eu

bundle=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
runner=$bundle/bin/polyedge-run-job
sh -n "$runner"
for job in prospective chart-backfill backfill; do
  grep -F "  $job)" "$runner" >/dev/null
done
grep -F 'if [ "$job" = origin-check ]; then' "$runner" >/dev/null
grep -F 'freshness:0|hourly:0)' "$runner" >/dev/null
grep -F 'exec /usr/bin/flock -w 3600 /run/polyedge/utility.lock' "$runner" >/dev/null
grep -F 'POLYEDGE_AUDIT_TARGET=${POLYEDGE_AUDIT_TARGET:-$(date -u -d' "$runner" >/dev/null
grep -F 'audit_target=$POLYEDGE_AUDIT_TARGET' "$runner" >/dev/null
target_line=$(grep -n 'POLYEDGE_AUDIT_TARGET=${POLYEDGE_AUDIT_TARGET:-$(date -u -d' "$runner" | cut -d: -f1)
lock_line=$(grep -n 'exec /usr/bin/flock -w 3600 /run/polyedge/utility.lock' "$runner" | cut -d: -f1)
use_line=$(grep -n 'audit_target=$POLYEDGE_AUDIT_TARGET' "$runner" | cut -d: -f1)
[ "$target_line" -lt "$lock_line" ] && [ "$lock_line" -lt "$use_line" ]
grep -F 'POLYEDGE_ORIGIN_EXPECTED_COUNTRY must equal CO' "$runner" >/dev/null
grep -F 'getent ahostsv4 polymarket.com' "$runner" >/dev/null
grep -F -- '--network polyedge' "$runner" >/dev/null
grep -F 'origin.country !== "CO" || origin.ip !== expectedIp' "$runner" >/dev/null
grep -F 'POLYEDGE_RAW_EVENT_PREFIX%/}/' "$runner" >/dev/null
grep -F 'set POLYEDGE_RAW_EVENT_PREFIX for Azure upload freshness' "$runner" >/dev/null
grep -F -- '--max-age-seconds 900 --expected-interval-seconds 600' "$runner" >/dev/null
grep -F 'POLYEDGE_LOCAL_RAW_ROOT%/}/$POLYEDGE_AUDIT_DAY/$POLYEDGE_AUDIT_HOUR/' "$runner" >/dev/null
grep -F 'POLYEDGE_LOCAL_RAW_ROOT must equal /input/events' "$runner" >/dev/null
grep -F 'POLYEDGE_DISABLE_RESEARCH_ARTIFACT_PUBLISH must equal true for primary OCI jobs' "$runner" >/dev/null
grep -F 'set -- --volume "$ring/segments:/input/events:ro,Z"' "$runner" >/dev/null
grep -F 'POLYEDGE_RESEARCH_DATE=${POLYEDGE_RESEARCH_DATE:-$(date -u -d yesterday +%Y-%m-%d)}' "$runner" >/dev/null
grep -F "grep -Eq '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'" "$runner" >/dev/null
grep -F 'date -u -d "$POLYEDGE_RESEARCH_DATE 00:00:00Z" +%s' "$runner" >/dev/null
grep -F 'date -u -d "@$research_date_epoch" +%Y-%m-%d' "$runner" >/dev/null
grep -F 'set -- --volume "$ring/segments:/input/events:ro,Z" --env POLYEDGE_RESEARCH_DATE' "$runner" >/dev/null
grep -F 'exec /usr/local/libexec/polyedge-parity-record-daily "$POLYEDGE_RESEARCH_DATE"' "$runner" >/dev/null
test "$(grep -c 'cpus=1.5 memory=' "$runner")" -eq 3
grep -F '*) work=$ring/jobs/research credential=research ;;' "$runner" >/dev/null
grep -F 'shadow-qset) work=$ring/jobs/shadow-qset credential=shadow-qset ;;' "$runner" >/dev/null
grep -F 'credential_dir=/run/polyedge-federated-$credential' "$runner" >/dev/null
grep -F 'credential_dir=/etc/polyedge/credentials/$credential' "$runner" >/dev/null
grep -F -- '-v "$credential_dir:/run/credentials:ro,Z"' "$runner" >/dev/null
grep -F 'daily|replay|prospective|chart-backfill|backfill|shadow-qset)' "$runner" >/dev/null
grep -F 'set -- /usr/bin/flock -w 129600 /run/polyedge/research.lock "$@"' "$runner" >/dev/null
test "$(grep -c '/usr/bin/flock -w 129600 /run/polyedge/research.lock' "$runner")" -eq 1
test "$(grep -c -- '--pull=never --log-driver=journald' "$runner")" -eq 2
grep -F 'OnCalendar=*-*-* 03:10:00 UTC' "$bundle/systemd/polyedge-daily.timer" >/dev/null
grep -F 'OnCalendar=*-*-* 03:15:00 UTC' "$bundle/systemd/polyedge-replay.timer" >/dev/null
