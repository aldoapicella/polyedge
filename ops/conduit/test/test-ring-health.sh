#!/bin/sh
set -eu

env_file=$(mktemp)
cleanup() {
  [ ! -e "$env_file" ] || unlink "$env_file"
  POLYEDGE_RING_ENV_FILE=ops/conduit/env/ring.env.example \
    POLYEDGE_RING_HEALTH_TEST=1 \
    ops/conduit/bin/polyedge-ring-health >/dev/null 2>&1 || true
}
trap cleanup EXIT HUP INT TERM

printf '%s\n' \
  'POLYEDGE_RING_SEAL_ONLY=1' \
  'POLYEDGE_RING_ROOT=/srv/polyedge-ring' \
  'POLYEDGE_RING_RETENTION_HOURS=48' \
  'POLYEDGE_RING_SEAL_GRACE_SECONDS=60' \
  'POLYEDGE_RING_MIN_FREE_BYTES=999999999999' \
  'POLYEDGE_RING_MAX_UNUPLOADED_SECONDS=1200' \
  'RECORDER_SEGMENT_SECONDS=600' > "$env_file"

if POLYEDGE_RING_ENV_FILE=$env_file POLYEDGE_RING_HEALTH_TEST=1 \
  ops/conduit/bin/polyedge-ring-health; then
  echo 'low-space health check unexpectedly passed' >&2
  exit 1
fi
jq -e '.free_ok == false and .seal_only == true and .retention_hours == 48' \
  /srv/polyedge-ring/status.json >/dev/null

echo 'ring health self-test passed'
