#!/bin/sh
set -eu

root=$(mktemp -d)
fake=$root/fake-bin
fixture_dir=$root/segments/ring-sync-selftest-$$
legacy=$fixture_dir/1785960000.jsonl
fixture=$fixture_dir/1785960600.jsonl
post_upload=$fixture_dir/1785961800.jsonl
bad=$fixture_dir/1785961200.jsonl
archive_dir=$root/archive/ring-sync-selftest-$$
legacy_archive=$archive_dir/1785960000.jsonl.gz
archive_fixture=$archive_dir/1785960600.jsonl.gz
legacy_manifest=$legacy_archive.manifest.json
manifest=$archive_fixture.manifest.json
post_upload_manifest=$archive_dir/1785961800.jsonl.gz.manifest.json
env_file=$root/ring.env
cleanup() { rm -rf "$root"; }
trap cleanup EXIT HUP INT TERM

install -d -m 0750 "$fixture_dir" "$fake"
printf '%s\n' '#!/bin/sh' 'exit 0' >"$fake/mountpoint"
chmod 0755 "$fake/mountpoint"
printf '{"test":"legacy"}
' > "$legacy"
printf '%s\n' \
  '#!/bin/sh' \
  'echo "$POLYEDGE_TEST_UPLOAD_MODE" >> "$POLYEDGE_TEST_UPLOAD_LOG"' \
  'printf "%s\n" "{\"test\":3,\"recorder_instance_id\":\"323e4567-e89b-42d3-a456-426614174000\",\"recorder_sequence\":1}" > "$POLYEDGE_TEST_POST_UPLOAD_SEGMENT"' \
  '[ "$POLYEDGE_TEST_UPLOAD_MODE" = fail ] && exit 1' \
  'exit 0' >"$fake/podman"
chmod 0755 "$fake/podman"
sync=$root/polyedge-ring-sync
sed "s|/usr/bin/podman|$fake/podman|" "$(pwd)/ops/conduit/bin/polyedge-ring-sync" > "$sync"
chmod 0755 "$sync"
printf '%s\n' \
  '{"test":1,"recorder_instance_id":"123e4567-e89b-42d3-a456-426614174000","recorder_sequence":1}' \
  '{"test":2,"recorder_instance_id":"223e4567-e89b-42d3-a456-426614174000","recorder_sequence":1}' > "$fixture"
printf 'POLYEDGE_RING_ROOT=%s\n' "$root" > "$env_file"
printf '%s\n' \
  'POLYEDGE_RING_IMAGE=ghcr.io/test/polyedge-rust-backend@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' \
  'POLYEDGE_RING_BLOB_PREFIX=events-oci-test' \
  'RECORDER_SEGMENT_SECONDS=600' \
  'POLYEDGE_RING_SEAL_ONLY=0' \
  'POLYEDGE_RING_LEGACY_CUTOFF_EPOCH=1785960600' \
  'AZURE_STORAGE_ACCOUNT_NAME=test' \
  'AZURE_STORAGE_CONTAINER_NAME=bot-events' \
  'AZURE_TENANT_ID=11111111-2222-3333-4444-555555555555' \
  'AZURE_CLIENT_ID=aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee' >> "$env_file"

PATH="$fake:$PATH" POLYEDGE_RING_ENV_FILE=$env_file POLYEDGE_RING_POST_UPLOAD_SEAL=1 \
  "$sync"
jq -e '
  .schema_version == 4 and
  .recorder_runs == [
    {recorder_instance_id:"123e4567-e89b-42d3-a456-426614174000",recorder_first_sequence:1,recorder_last_sequence:1,recorder_event_count:1},
    {recorder_instance_id:"223e4567-e89b-42d3-a456-426614174000",recorder_first_sequence:1,recorder_last_sequence:1,recorder_event_count:1}
  ] and
  .compression == "gzip" and
  .source_bytes > 11 and
  .lines == 2 and
  (.sha256 | startswith("sha256:")) and
  (.source_sha256 | startswith("sha256:")) and
  .segment_path == "segments/ring-sync-selftest-'"$$"'/1785960600.jsonl" and
  .archive_path == "archive/ring-sync-selftest-'"$$"'/1785960600.jsonl.gz" and
  .blob_name == "events-oci-test/2026/08/05/20/1785960600.jsonl.gz"
' "$manifest" >/dev/null
expected=$(jq -r '.sha256' "$manifest" | cut -d: -f2)
jq -e '.schema_version == 2 and has("recorder_instance_id") | not' "$legacy_manifest" >/dev/null
actual=$(sha256sum "$archive_fixture" | awk '{print $1}')
[ "$actual" = "$expected" ]
expected_source=$(jq -r '.source_sha256' "$manifest" | cut -d: -f2)
actual_source=$(sha256sum "$fixture" | awk '{print $1}')
[ "$actual_source" = "$expected_source" ]
upload_log=$root/upload.log
if PATH="$fake:$PATH" POLYEDGE_RING_ENV_FILE=$env_file \
  POLYEDGE_TEST_UPLOAD_MODE=fail POLYEDGE_TEST_UPLOAD_LOG=$upload_log \
  POLYEDGE_TEST_POST_UPLOAD_SEGMENT=$post_upload "$sync" >/dev/null 2>&1; then
  echo "failed upload unexpectedly passed" >&2
  exit 1
fi
[ ! -e "$post_upload_manifest" ]
rm -f "$post_upload"
PATH="$fake:$PATH" POLYEDGE_RING_ENV_FILE=$env_file \
  POLYEDGE_TEST_UPLOAD_MODE=success POLYEDGE_TEST_UPLOAD_LOG=$upload_log \
  POLYEDGE_TEST_POST_UPLOAD_SEGMENT=$post_upload "$sync"
[ -e "$post_upload_manifest" ]
[ "$(wc -l < "$upload_log")" = 2 ]
[ "$(gzip -dc "$archive_fixture" | jq -s 'length')" = 2 ]
printf '{"test":"bad"}
' > "$bad"
if PATH="$fake:$PATH" POLYEDGE_RING_ENV_FILE=$env_file POLYEDGE_RING_POST_UPLOAD_SEAL=1 ops/conduit/bin/polyedge-ring-sync >/dev/null 2>&1; then echo 'unsequenced post-cutoff segment unexpectedly passed' >&2; exit 1; fi
[ ! -e "$bad.sequence."* ]
grep -F -- '--cap-drop=all --cap-add=DAC_OVERRIDE' ops/conduit/bin/polyedge-ring-sync >/dev/null
grep -F -- '-v "$segments:/srv/polyedge-ring/segments:Z"' ops/conduit/bin/polyedge-ring-sync >/dev/null
grep -F -- '-v "$archive:/srv/polyedge-ring/archive:Z"' ops/conduit/bin/polyedge-ring-sync >/dev/null
grep -F 'prefix=${POLYEDGE_RING_BLOB_PREFIX:-events-oci-hot7-v1}' ops/conduit/bin/polyedge-ring-sync >/dev/null

echo 'ring sealer self-test passed'
