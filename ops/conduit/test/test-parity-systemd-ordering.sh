#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname "$0")/../../.." && pwd)
timer=$root/ops/conduit/systemd/polyedge-ring-sync.timer
health=$root/ops/conduit/systemd/polyedge-ring-health.service
parity=$root/ops/conduit/systemd/polyedge-parity-hourly.service

grep -Fx 'OnBootSec=2min' "$timer"
grep -Fx 'OnUnitInactiveSec=1min' "$timer"
! grep -Eq '^(OnCalendar|Persistent)=' "$timer"
grep -Fx 'AccuracySec=1s' "$timer"
grep -Fx 'RandomizedDelaySec=0' "$timer"
grep -Fx 'After=local-fs.target polyedge-ring-sync.service' "$health"
grep -Fx 'Requires=polyedge-ring-health.service' "$parity"
grep -Fx 'After=network-online.target polyedge-job@hourly.service polyedge-ring-sync.service polyedge-ring-health.service' "$parity"
! grep -Eq '^Requires=.*polyedge-ring-sync\.service' "$parity"
