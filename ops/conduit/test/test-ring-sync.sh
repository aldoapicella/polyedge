#!/bin/sh
set -eu

fixture_dir=/srv/polyedge-ring/segments/ring-sync-selftest-$$
fixture=$fixture_dir/1785960000.jsonl
manifest=$fixture.manifest.json
env_file=$(mktemp)
cleanup() {
  [ ! -e "$fixture" ] || unlink "$fixture"
  [ ! -e "$manifest" ] || unlink "$manifest"
  [ ! -e "$env_file" ] || unlink "$env_file"
  rmdir "$fixture_dir" 2>/dev/null || true
}
trap cleanup EXIT HUP INT TERM

install -d -m 0750 "$fixture_dir"
printf '{"test":1}\n' > "$fixture"
printf '%s\n' \
  'POLYEDGE_RING_IMAGE=ghcr.io/test/polyedge-rust-backend@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' \
  'POLYEDGE_RING_ROOT=/srv/polyedge-ring' \
  'POLYEDGE_RING_BLOB_PREFIX=events-oci-test' \
  'RECORDER_SEGMENT_SECONDS=600' \
  'AZURE_STORAGE_ACCOUNT_NAME=test' \
  'AZURE_STORAGE_CONTAINER_NAME=bot-events' \
  'AZURE_TENANT_ID=11111111-2222-3333-4444-555555555555' \
  'AZURE_CLIENT_ID=aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee' > "$env_file"

POLYEDGE_RING_ENV_FILE=$env_file POLYEDGE_RING_SEAL_ONLY=1 \
  ops/conduit/bin/polyedge-ring-sync
jq -e '
  .schema_version == 1 and
  .bytes == 11 and
  .lines == 1 and
  (.sha256 | startswith("sha256:")) and
  .blob_name == "events-oci-test/2026/08/05/20/1785960000.jsonl"
' "$manifest" >/dev/null
expected=$(jq -r '.sha256' "$manifest" | cut -d: -f2)
actual=$(sha256sum "$fixture" | awk '{print $1}')
[ "$actual" = "$expected" ]
grep -F -- '--cap-drop=all --cap-add=DAC_OVERRIDE' ops/conduit/bin/polyedge-ring-sync >/dev/null
grep -F -- '-v "$segments:/srv/polyedge-ring/segments:Z"' ops/conduit/bin/polyedge-ring-sync >/dev/null

echo 'ring sealer self-test passed'
