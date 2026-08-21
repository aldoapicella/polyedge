#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
mkdir -p "$tmp/quadlets" "$tmp/rollback" "$tmp/bin"

old=ghcr.io/example/polyedge-rust-backend@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
new=ghcr.io/example/polyedge-rust-backend@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
next=ghcr.io/example/polyedge-rust-backend@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc
signer=ghcr.io/example/polyedge-venue-probe@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
printf '[Container]\nImage=%s\nContainerName=polyedge-api\n' "$old" >"$tmp/quadlets/polyedge-api.container"
printf '[Container]\nImage=%s\nContainerName=polyedge-shadow-qset\n' "$old" >"$tmp/quadlets/polyedge-shadow-qset.container"
printf '[Container]\nImage=%s\nContainerName=polyedge-funded-intent-producer\n' "$old" >"$tmp/quadlets/polyedge-funded-intent-producer.container"
printf '[Container]\nImage=%s\nContainerName=polyedge-funded-signer\n' "$old" >"$tmp/quadlets/polyedge-funded-signer.container"

printf '%s\n' '#!/bin/sh' 'set -eu' 'printf "%s\\n" "$*" >>"$TEST_LOG"' \
  'case "$1 $2" in' "  'image inspect') printf 'linux/arm64\\n' ;;" \
  "  'inspect --type') printf 'true %s %s\\n' \"\$TEST_IMAGE_ID\" \"\$TEST_IMAGE\" ;;" 'esac' >"$tmp/bin/podman"
printf '%s\n' '#!/bin/sh' 'set -eu' 'printf "%s\\n" "$*" >>"$TEST_LOG"' >"$tmp/bin/systemctl"
printf '%s\n' '#!/bin/sh' 'set -eu' 'printf "disk %s\\n" "$*" >>"$TEST_LOG"' >"$tmp/bin/disk-guard"
chmod +x "$tmp/bin/podman" "$tmp/bin/systemctl" "$tmp/bin/disk-guard"

run() {
  image=$1
  running_image=$2
  timestamp=$3
  unit=${4:-polyedge-api}
  TEST_LOG="$tmp/log" TEST_IMAGE_ID=linux/arm64 TEST_IMAGE="$running_image" \
    POLYEDGE_TEST_ALLOW_UNPRIVILEGED=1 \
    POLYEDGE_QUADLET_DIR="$tmp/quadlets" \
    POLYEDGE_ROLLBACK_DIR="$tmp/rollback" \
    POLYEDGE_PODMAN="$tmp/bin/podman" \
    POLYEDGE_SYSTEMCTL="$tmp/bin/systemctl" \
    POLYEDGE_DISK_GUARD="$tmp/bin/disk-guard" \
    POLYEDGE_DEPLOY_TIMESTAMP="$timestamp" \
    "$root/bin/polyedge-quadlet-deploy" "$unit" "$image"
}

run "$new" "$new" 20260805T000000Z
grep -Fx "Image=$new" "$tmp/quadlets/polyedge-api.container" >/dev/null
grep -Fx "Image=$old" "$tmp/rollback/20260805T000000Z-polyedge-api.container" >/dev/null
grep -Fx "pull $new" "$tmp/log" >/dev/null
grep -Fx 'disk --pull-gate' "$tmp/log" >/dev/null
grep -Fx 'disk --assert-headroom' "$tmp/log" >/dev/null
grep -Fx 'restart polyedge-api.service' "$tmp/log" >/dev/null

run "$signer" "$signer" 20260805T000002Z polyedge-funded-signer
grep -Fx "Image=$signer" "$tmp/quadlets/polyedge-funded-signer.container" >/dev/null

run "$next" "$next" 20260805T000004Z polyedge-shadow-qset
grep -Fx "Image=$next" "$tmp/quadlets/polyedge-shadow-qset.container" >/dev/null

run "$next" "$next" 20260805T000005Z polyedge-funded-intent-producer
grep -Fx "Image=$next" "$tmp/quadlets/polyedge-funded-intent-producer.container" >/dev/null

if run "$new" "$new" 20260805T000003Z polyedge-funded-signer; then
  echo 'wrong repository was accepted for funded signer' >&2
  exit 1
fi

# A wrong running digest must leave the previously working Quadlet in place.
if run "$next" "$new" 20260805T000001Z; then
  echo 'digest mismatch was accepted' >&2
  exit 1
fi
grep -Fx "Image=$new" "$tmp/quadlets/polyedge-api.container" >/dev/null
test "$(grep -c '^restart polyedge-api.service$' "$tmp/log")" -eq 3

if POLYEDGE_TEST_ALLOW_UNPRIVILEGED=1 "$root/bin/polyedge-quadlet-deploy" nope "$new"; then
  echo 'unknown service was accepted' >&2
  exit 1
fi

grep -F 'Authorization: Bearer %%s' "$root/quadlets/polyedge-api.container" >/dev/null
grep -F '"$$API_BEARER_TOKEN"' "$root/quadlets/polyedge-api.container" >/dev/null
grep -Fx 'HealthOnFailure=kill' "$root/quadlets/polyedge-api.container" >/dev/null
grep -F -- '--pull=never --log-driver=journald' "$root/quadlets/polyedge-api.container" >/dev/null
grep -Fx 'User=986:982' "$root/quadlets/polyedge-funded-signer.container" >/dev/null
grep -Fx 'IP=10.89.0.250' "$root/quadlets/polyedge-funded-signer.container" >/dev/null
grep -Fx 'WantedBy=multi-user.target' "$root/quadlets/polyedge-funded-signer.container" >/dev/null
grep -Fx 'Wants=network-online.target polyedge-federated-token@funded-signer.service' "$root/quadlets/polyedge-funded-signer.container" >/dev/null
grep -Fx 'After=network-online.target polyedge-network.service polyedge-funded-egress.service polyedge-federated-token@funded-signer.service' "$root/quadlets/polyedge-funded-signer.container" >/dev/null
grep -Fx 'Requires=polyedge-network.service polyedge-funded-egress.service' "$root/quadlets/polyedge-funded-signer.container" >/dev/null
grep -Fq '10.89.0.250/32 ! -d 10.89.0.0/24' "$root/systemd/polyedge-funded-egress.service"
grep -Fq -- '--to-source 10.0.0.81' "$root/systemd/polyedge-funded-egress.service"
grep -Fq 'ExecStartPre=/usr/bin/bash -ec' "$root/quadlets/polyedge-funded-signer.container"
grep -Fq '986:982:600:1' "$root/quadlets/polyedge-funded-signer.container"
grep -Fq -- 'PodmanArgs=--cpus=0.5 ' "$root/quadlets/polyedge-funded-signer.container"
if grep -Fq -- '--security-opt=no-new-privileges' "$root/quadlets/polyedge-funded-signer.container"; then
  echo 'funded signer cannot use the OCI Podman 4.9 no-new-privileges network path' >&2
  exit 1
fi
grep -Fx 'User=987:983' "$root/quadlets/polyedge-shadow-qset.container" >/dev/null
grep -Fx 'EnvironmentFile=/etc/polyedge/shadow-qset.env' "$root/quadlets/polyedge-shadow-qset.container" >/dev/null
grep -Fx 'LOCAL_JSONL_RECORDER_ENABLED=false' "$root/env/shadow-qset.env.example" >/dev/null
grep -Fx 'Volume=/run/polyedge-federated-shadow-qset:/run/credentials:ro,Z' "$root/quadlets/polyedge-shadow-qset.container" >/dev/null
grep -Fq -- '--read-only --tmpfs=/tmp:rw,noexec,nosuid,size=64m --cap-drop=all --pull=never' "$root/quadlets/polyedge-shadow-qset.container"
grep -Fx 'WantedBy=multi-user.target' "$root/quadlets/polyedge-shadow-qset.container" >/dev/null
if grep -q '^PublishPort=' "$root/quadlets/polyedge-shadow-qset.container"; then
  echo 'qset writer must not publish a host port' >&2
  exit 1
fi
grep -Fx 'User=984:980' "$root/quadlets/polyedge-funded-intent-producer.container" >/dev/null
grep -Fx 'Network=polyedge.network' "$root/quadlets/polyedge-funded-intent-producer.container" >/dev/null
grep -Fx 'IP=10.89.0.248' "$root/quadlets/polyedge-funded-intent-producer.container" >/dev/null
grep -Fx 'EnvironmentFile=/etc/polyedge/funded-intent-producer.env' "$root/quadlets/polyedge-funded-intent-producer.container" >/dev/null
grep -Fx 'Volume=/run/polyedge-federated-funded-intent-producer:/run/credentials:ro,Z' "$root/quadlets/polyedge-funded-intent-producer.container" >/dev/null
grep -Fx 'Wants=network-online.target polyedge-federated-token@funded-intent-producer.service' "$root/quadlets/polyedge-funded-intent-producer.container" >/dev/null
grep -Fx 'After=network-online.target polyedge-network.service polyedge-funded-intent-producer-egress.service polyedge-federated-token@funded-intent-producer.service' "$root/quadlets/polyedge-funded-intent-producer.container" >/dev/null
grep -Fx 'Requires=polyedge-network.service polyedge-funded-intent-producer-egress.service' "$root/quadlets/polyedge-funded-intent-producer.container" >/dev/null
grep -Fq -- '--read-only --tmpfs=/tmp:rw,noexec,nosuid,size=64m --cap-drop=all --pull=never' "$root/quadlets/polyedge-funded-intent-producer.container"
if grep -Eq '^PublishPort=|funded-signer' "$root/quadlets/polyedge-funded-intent-producer.container"; then
  echo 'funded intent producer must not expose a port or link the signer' >&2
  exit 1
fi
test "$(grep -Fc '10.89.0.248/32 ! -d 10.89.0.0/24' "$root/systemd/polyedge-funded-intent-producer-egress.service")" -eq 3
test "$(grep -Fc -- '--to-source 10.0.0.81' "$root/systemd/polyedge-funded-intent-producer-egress.service")" -eq 3
grep -Fx 'Before=polyedge-funded-intent-producer.service' "$root/systemd/polyedge-funded-intent-producer-egress.service" >/dev/null
grep -Fx 'User=polyedge-funded-producer' "$root/systemd/polyedge-federated-token@funded-intent-producer.service.d/override.conf" >/dev/null
grep -Fx 'Group=polyedge-funded-producer' "$root/systemd/polyedge-federated-token@funded-intent-producer.service.d/override.conf" >/dev/null
grep -Fx 'APP_NAME=polyedge-funded-intent-producer' "$root/env/funded-intent-producer.env.example" >/dev/null
grep -Fx 'EXECUTION_MODE=paper' "$root/env/funded-intent-producer.env.example" >/dev/null
grep -Fx 'ALLOW_LIVE=false' "$root/env/funded-intent-producer.env.example" >/dev/null
grep -Fx 'RUN_BOT_ON_STARTUP=false' "$root/env/funded-intent-producer.env.example" >/dev/null
grep -Fx 'STRATEGY_INTENT_OPERATOR_DIRECT=true' "$root/env/funded-intent-producer.env.example" >/dev/null
grep -Fx 'AZURE_TENANT_ID=9767f0dc-e83f-4cc1-94e1-0d5f9d287d32' "$root/env/funded-intent-producer.env.example" >/dev/null
grep -Fx 'AZURE_CLIENT_ID=54f0136b-5e72-4ad1-b23e-cb1269d356c1' "$root/env/funded-intent-producer.env.example" >/dev/null
grep -Fx 'AZURE_FEDERATED_TOKEN_FILE=/run/credentials/azure-federated-token' "$root/env/funded-intent-producer.env.example" >/dev/null
grep -Fx 'AZURE_TOKEN_CREDENTIALS=WorkloadIdentityCredential' "$root/env/funded-intent-producer.env.example" >/dev/null
grep -Fx 'AZURE_STORAGE_CONTAINER_NAME=polyedge-shadow-events' "$root/env/funded-intent-producer.env.example" >/dev/null
grep -Fx 'AZURE_FUNDED_STORAGE_CONTAINER_NAME=polyedge-funded-evidence' "$root/env/funded-intent-producer.env.example" >/dev/null
grep -Fx 'AZURE_MODEL_STORAGE_CONTAINER_NAME=polyedge-models' "$root/env/funded-intent-producer.env.example" >/dev/null
grep -Fx 'FUNDED_DIRECT_SERVICE_BUS_ENABLED=true' "$root/env/funded-intent-producer.env.example" >/dev/null
grep -Fx 'FUNDED_DIRECT_SERVICE_BUS_NAMESPACE=sb-polyedge-funded-cl-6urdjr5nmwx7w' "$root/env/funded-intent-producer.env.example" >/dev/null
grep -Fx 'FUNDED_DIRECT_SERVICE_BUS_QUEUE=funded-dynamic-quote-intents' "$root/env/funded-intent-producer.env.example" >/dev/null
if grep -Eiq 'qset|bot-events|key.?vault|wallet|receiver|secret|private.?key|api.?key|passphrase' "$root/env/funded-intent-producer.env.example"; then
  echo 'funded intent producer environment includes a forbidden lane or credential surface' >&2
  exit 1
fi
if grep -Eq 'funded-signer|polyedge-identity-funded-intent-producer' "$root/quadlets/polyedge-funded-intent-producer.container" "$root/systemd/polyedge-funded-intent-producer-egress.service" "$root/systemd/polyedge-federated-token@funded-intent-producer.service.d/override.conf"; then
  echo 'funded intent producer operational files link the signer or overlong Unix account' >&2
  exit 1
fi
