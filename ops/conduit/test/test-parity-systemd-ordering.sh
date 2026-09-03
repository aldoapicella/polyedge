#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname "$0")/../../.." && pwd)
timer=$root/ops/conduit/systemd/polyedge-ring-sync.timer
sync_service=$root/ops/conduit/systemd/polyedge-ring-sync.service
domain_timer=$root/ops/conduit/systemd/polyedge-ring-sync@.timer
domain_service=$root/ops/conduit/systemd/polyedge-ring-sync@.service
health=$root/ops/conduit/systemd/polyedge-ring-health.service
parity=$root/ops/conduit/systemd/polyedge-parity-hourly.service
collector=$root/ops/conduit/bin/polyedge-parity-hourly
reboot=$root/ops/conduit/systemd/polyedge-reboot-attestation.service

grep -Fx 'OnCalendar=*-*-* *:01/5:00 UTC' "$timer"
grep -Fx 'Persistent=true' "$timer"
grep -Fx 'AccuracySec=1s' "$timer"
grep -Fx 'RandomizedDelaySec=0' "$timer"
grep -Fx 'MemoryMax=10G' "$sync_service"
grep -Fx 'TimeoutStartSec=3h' "$sync_service"
grep -Fx 'ExecStart=/usr/bin/flock -n /run/polyedge-ring-sync.lock /usr/bin/flock -w 3600 /run/polyedge/utility.lock /usr/local/libexec/polyedge-ring-sync' "$sync_service"
grep -Fx 'Unit=polyedge-ring-sync@%i.service' "$domain_timer"
grep -Fx 'ExecStart=/usr/bin/flock -n /run/polyedge-ring-sync-%i.lock /usr/bin/flock -w 3600 /run/polyedge/utility.lock /usr/bin/env POLYEDGE_RING_ENV_FILE=/etc/polyedge/ring-%i.env /usr/local/libexec/polyedge-ring-sync' "$domain_service"
for mapping in \
  'funded-intent-producer polyedge-shadow-events' \
  'shadow-qset-v5 polyedge-shadow-qset-v5-events' \
  'shadow-qset-v7 polyedge-shadow-qset-v7-events'
do
  set -- $mapping
  domain_env=$root/ops/conduit/env/ring-$1.env.example
  grep -Fx "POLYEDGE_RING_ROOT=/srv/polyedge-ring/domains/$1" "$domain_env"
  grep -Fx "OCI_OBJECT_STORAGE_BUCKET=$2" "$domain_env"
  grep -Fx 'POLYEDGE_AZURE_RING_UPLOAD_ENABLED=0' "$domain_env"
  grep -Fx "Volume=/srv/polyedge-ring/domains/$1:/srv/polyedge-ring:Z" "$root/ops/conduit/quadlets/polyedge-$1.container"
done
grep -Fx 'After=local-fs.target polyedge-ring-sync.service' "$health"
grep -Fx 'Requires=polyedge-ring-health.service' "$parity"
grep -Fx 'After=network-online.target polyedge-job@hourly.service polyedge-ring-sync.service polyedge-ring-health.service' "$parity"
grep -Fx 'ExecStart=/usr/local/libexec/polyedge-parity-hourly' "$parity"
grep -Fx 'TimeoutStartSec=2h' "$parity"
grep -Fx 'CPUQuota=50%' "$parity"
! grep -Eq '^Requires=.*polyedge-ring-sync\.service' "$parity"
grep -Fx 'ExecStart=/usr/local/libexec/polyedge-reboot-attestation complete' "$reboot"
grep -Fx 'ConditionPathExists=/srv/polyedge-ring/parity/reboot/pending.json' "$reboot"
grep -Fx 'WantedBy=multi-user.target' "$reboot"
grep -Fx 'Wants=network-online.target polyedge-funded-intent-producer.service polyedge-federated-token@funded-intent-producer.timer' "$reboot"
grep -Fx 'Requires=polyedge-ring-health.service' "$reboot"
grep -Fx 'Restart=on-failure' "$reboot"
grep -Fx 'RestartSec=60s' "$reboot"
grep -Fx 'StartLimitBurst=10' "$reboot"
! grep -Eq '^Wants=.*polyedge-funded-signer\.service' "$reboot"
! grep -Fx 'After=multi-user.target' "$reboot"
after=$(grep '^After=' "$reboot")
for dependency in \
  network-online.target spire-server.service spire-agent.service spire-oidc-discovery-provider.service \
  caddy.service polyedge-api.service polyedge-frontend.service polyedge-ring-sync.timer \
  polyedge-ring-health.service polyedge-ring-health.timer polyedge-boot-disk-guard.timer \
  polyedge-freshness.timer polyedge-hourly.timer polyedge-daily.timer polyedge-replay.timer \
  polyedge-parity-hourly.timer polyedge-federated-token@api.timer \
  polyedge-federated-token@research.timer polyedge-funded-signer.service \
  polyedge-federated-token@funded-signer.timer polyedge-funded-intent-producer.service \
  polyedge-federated-token@funded-intent-producer.timer
do
  printf '%s\n' "$after" | grep -F "$dependency" >/dev/null
done
! grep -Eq '^Requires=.*polyedge-funded-signer\.service' "$parity"
target_line=$(grep -n 'POLYEDGE_PARITY_TARGET_HOUR_UTC=${POLYEDGE_PARITY_TARGET_HOUR_UTC:-$(date -u -d' "$collector" | cut -d: -f1)
lock_line=$(grep -n 'exec /usr/bin/flock -w 3600 /run/polyedge/utility.lock' "$collector" | cut -d: -f1)
[ "$target_line" -lt "$lock_line" ]
