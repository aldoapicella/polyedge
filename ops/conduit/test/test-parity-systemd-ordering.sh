#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname "$0")/../../.." && pwd)
timer=$root/ops/conduit/systemd/polyedge-ring-sync.timer
sync_service=$root/ops/conduit/systemd/polyedge-ring-sync.service
health=$root/ops/conduit/systemd/polyedge-ring-health.service
parity=$root/ops/conduit/systemd/polyedge-parity-hourly.service

grep -Fx 'OnBootSec=2min' "$timer"
grep -Fx 'OnCalendar=*-*-* *:01/5:00 UTC' "$timer"
grep -Fx 'Persistent=true' "$timer"
! grep -Eq '^(OnCalendar|Persistent)=' "$timer"
grep -Fx 'AccuracySec=1s' "$timer"
grep -Fx 'RandomizedDelaySec=0' "$timer"
grep -Fx 'MemoryMax=10G' "$sync_service"
grep -Fx 'TimeoutStartSec=2h' "$sync_service"
grep -Fx 'After=local-fs.target polyedge-ring-sync.service' "$health"
grep -Fx 'Requires=polyedge-ring-health.service' "$parity"
grep -Fx 'After=network-online.target polyedge-job@hourly.service polyedge-ring-sync.service polyedge-ring-health.service' "$parity"
! grep -Eq '^Requires=.*polyedge-ring-sync\.service' "$parity"
