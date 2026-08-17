#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname "$0")/../../.." && pwd)
timer=$root/ops/conduit/systemd/polyedge-ring-sync.timer
sync_service=$root/ops/conduit/systemd/polyedge-ring-sync.service
health=$root/ops/conduit/systemd/polyedge-ring-health.service
parity=$root/ops/conduit/systemd/polyedge-parity-hourly.service
collector=$root/ops/conduit/bin/polyedge-parity-hourly

grep -Fx 'OnCalendar=*-*-* *:01/5:00 UTC' "$timer"
grep -Fx 'Persistent=true' "$timer"
grep -Fx 'AccuracySec=1s' "$timer"
grep -Fx 'RandomizedDelaySec=0' "$timer"
grep -Fx 'MemoryMax=10G' "$sync_service"
grep -Fx 'TimeoutStartSec=3h' "$sync_service"
grep -Fx 'ExecStart=/usr/bin/flock -n /run/polyedge-ring-sync.lock /usr/bin/flock -w 3600 /run/polyedge/utility.lock /usr/local/libexec/polyedge-ring-sync' "$sync_service"
grep -Fx 'After=local-fs.target polyedge-ring-sync.service' "$health"
grep -Fx 'Requires=polyedge-ring-health.service' "$parity"
grep -Fx 'After=network-online.target polyedge-job@hourly.service polyedge-ring-sync.service polyedge-ring-health.service' "$parity"
grep -Fx 'ExecStart=/usr/local/libexec/polyedge-parity-hourly' "$parity"
grep -Fx 'TimeoutStartSec=2h' "$parity"
grep -Fx 'CPUQuota=50%' "$parity"
! grep -Eq '^Requires=.*polyedge-ring-sync\.service' "$parity"
! grep -Eq '^Requires=.*polyedge-funded-signer\.service' "$parity"
target_line=$(grep -n 'POLYEDGE_PARITY_TARGET_HOUR_UTC=${POLYEDGE_PARITY_TARGET_HOUR_UTC:-$(date -u -d' "$collector" | cut -d: -f1)
lock_line=$(grep -n 'exec /usr/bin/flock -w 3600 /run/polyedge/utility.lock' "$collector" | cut -d: -f1)
[ "$target_line" -lt "$lock_line" ]
