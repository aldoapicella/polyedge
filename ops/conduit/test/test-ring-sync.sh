#!/bin/sh
set -eu

root=$(mktemp -d)
fake=$root/fake-bin
fixture_dir=$root/segments/ring-sync-selftest-$$
legacy=$fixture_dir/1785960000.jsonl
fixture=$fixture_dir/1785960600.jsonl
post_upload=$fixture_dir/1785962400.jsonl
bad=$root/segments/2026/08/05/20/1785961200.jsonl
later=$fixture_dir/1785961800.jsonl
quarantined_post_upload=$fixture_dir/1785963600.jsonl
near_miss=$fixture_dir/1785963000.jsonl
archive_dir=$root/archive/ring-sync-selftest-$$
legacy_archive=$archive_dir/1785960000.jsonl.gz
archive_fixture=$archive_dir/1785960600.jsonl.gz
legacy_manifest=$legacy_archive.manifest.json
manifest=$archive_fixture.manifest.json
post_upload_manifest=$archive_dir/1785962400.jsonl.gz.manifest.json
bad_archive=$root/archive/2026/08/05/20/1785961200.jsonl.gz
later_archive=$archive_dir/1785961800.jsonl.gz
later_manifest=$later_archive.manifest.json
quarantined_post_archive=$archive_dir/1785963600.jsonl.gz
quarantined_post_manifest=$quarantined_post_archive.manifest.json
env_file=$root/ring.env
quarantine=$root/quarantine/recorder-sequence-proof-v1
cleanup() { rm -rf "$root"; }
trap cleanup EXIT HUP INT TERM

install -d -m 0750 "$fixture_dir" "${bad%/*}" "$fake"
printf '%s\n' '#!/bin/sh' 'exit 0' >"$fake/mountpoint"
chmod 0755 "$fake/mountpoint"
real_sha256sum=$(command -v sha256sum)
printf '%s\n' \
  '#!/bin/sh' \
  '[ -n "${POLYEDGE_TEST_FORBIDDEN_HASH:-}" ] && [ "${1:-}" = "$POLYEDGE_TEST_FORBIDDEN_HASH" ] && { echo "sealed source was re-read" >&2; exit 99; }' \
  'exec "$POLYEDGE_TEST_REAL_SHA256SUM" "$@"' >"$fake/sha256sum"
chmod 0755 "$fake/sha256sum"
export POLYEDGE_TEST_REAL_SHA256SUM=$real_sha256sum
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
PATH="$fake:$PATH" POLYEDGE_RING_ENV_FILE=$env_file POLYEDGE_RING_POST_UPLOAD_SEAL=1 \
  POLYEDGE_TEST_FORBIDDEN_HASH=$fixture "$sync"
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
printf '%s\n' \
  '{"test":"bad-1","recorder_instance_id":"523e4567-e89b-42d3-a456-826614174000","recorder_sequence":1}' \
  '{"test":"bad-3","recorder_instance_id":"523e4567-e89b-42d3-a456-826614174000","recorder_sequence":3}' > "$bad"
printf '%s\n' \
  '{"test":4,"recorder_instance_id":"523e4567-e89b-42d3-a456-826614174000","recorder_sequence":4}' \
  '{"test":5,"recorder_instance_id":"523e4567-e89b-42d3-a456-826614174000","recorder_sequence":5}' > "$later"
bad_hash=$(sha256sum "$bad" | awk '{print $1}')
bad_relative=segments/2026/08/05/20/1785961200.jsonl
receipt_id=$(printf '%s:%s:%s' "${#bad_relative}" "$bad_relative" "sha256:$bad_hash" | sha256sum | awk '{print $1}')
receipt=$quarantine/$receipt_id.json
receipt_expected=$root/receipt-expected.json
if PATH="$fake:$PATH" POLYEDGE_RING_ENV_FILE=$env_file POLYEDGE_RING_POST_UPLOAD_SEAL=1 "$sync" >/dev/null 2>&1; then
  echo 'quarantined post-cutoff segment unexpectedly passed' >&2
  exit 1
fi
[ "$(sha256sum "$bad" | awk '{print $1}')" = "$bad_hash" ]
[ ! -e "$bad.manifest.json" ]
[ ! -e "$bad_archive" ]
[ ! -e "$bad_archive.manifest.json" ]
jq -cn \
  --arg source_segment_path "$bad_relative" \
  --arg source_sha256 "sha256:$bad_hash" \
  --argjson source_bytes "$(wc -c < "$bad")" \
  --argjson source_lines "$(wc -l < "$bad")" \
  '{schema:"polyedge_ring_quarantine.v1",type:"quarantine_receipt",source_segment_path:$source_segment_path,source_sha256:$source_sha256,source_bytes:$source_bytes,source_lines:$source_lines,reason_code:"invalid_recorder_sequence_proof"}' > "$receipt_expected"
cmp -s "$receipt_expected" "$receipt"
jq -e 'keys == ["reason_code","schema","source_bytes","source_lines","source_segment_path","source_sha256","type"] and (. | tostring | contains("\\\"test\\\"")) | not' "$receipt" >/dev/null
[ -e "$later_archive" ]
[ -e "$later_manifest" ]
later_sha=$(sha256sum "$later_archive" | awk '{print $1}')
receipt_hash=$(sha256sum "$receipt" | awk '{print $1}')
upload_count=$(wc -l < "$upload_log")
if PATH="$fake:$PATH" POLYEDGE_RING_ENV_FILE=$env_file \
  POLYEDGE_TEST_UPLOAD_MODE=success POLYEDGE_TEST_UPLOAD_LOG=$upload_log \
  POLYEDGE_TEST_POST_UPLOAD_SEGMENT=$quarantined_post_upload "$sync" >/dev/null 2>&1; then
  echo 'quarantined upload unexpectedly passed' >&2
  exit 1
fi
[ "$(wc -l < "$upload_log")" = "$((upload_count + 1))" ]
[ -e "$quarantined_post_archive" ]
[ -e "$quarantined_post_manifest" ]
[ "$(sha256sum "$bad" | awk '{print $1}')" = "$bad_hash" ]
if PATH="$fake:$PATH" POLYEDGE_RING_ENV_FILE=$env_file POLYEDGE_RING_POST_UPLOAD_SEAL=1 "$sync" >/dev/null 2>&1; then
  echo 'quarantined rerun unexpectedly passed' >&2
  exit 1
fi
[ "$(sha256sum "$later_archive" | awk '{print $1}')" = "$later_sha" ]
[ "$(sha256sum "$receipt" | awk '{print $1}')" = "$receipt_hash" ]
upload_count=$(wc -l < "$upload_log")
printf '{}' > "$quarantine/orphan.json"
if PATH="$fake:$PATH" POLYEDGE_RING_ENV_FILE=$env_file \
  POLYEDGE_TEST_UPLOAD_MODE=success POLYEDGE_TEST_UPLOAD_LOG=$upload_log \
  POLYEDGE_TEST_POST_UPLOAD_SEGMENT=$quarantined_post_upload "$sync" >"$root/orphan.log" 2>&1; then
  echo 'malformed orphan receipt unexpectedly passed' >&2
  exit 1
fi
grep -F 'invalid quarantine receipt' "$root/orphan.log" >/dev/null
[ "$(wc -l < "$upload_log")" = "$upload_count" ]
rm -f "$quarantine/orphan.json"
ln -s /dev/null "$quarantine/link.json"
if PATH="$fake:$PATH" POLYEDGE_RING_ENV_FILE=$env_file \
  POLYEDGE_TEST_UPLOAD_MODE=success POLYEDGE_TEST_UPLOAD_LOG=$upload_log \
  POLYEDGE_TEST_POST_UPLOAD_SEGMENT=$quarantined_post_upload "$sync" >"$root/link.log" 2>&1; then
  echo 'symlink receipt unexpectedly passed' >&2
  exit 1
fi
grep -F 'non-regular quarantine entry' "$root/link.log" >/dev/null
[ "$(wc -l < "$upload_log")" = "$upload_count" ]
rm -f "$quarantine/link.json"
mkfifo "$quarantine/fifo.json"
if PATH="$fake:$PATH" POLYEDGE_RING_ENV_FILE=$env_file \
  POLYEDGE_TEST_UPLOAD_MODE=success POLYEDGE_TEST_UPLOAD_LOG=$upload_log \
  POLYEDGE_TEST_POST_UPLOAD_SEGMENT=$quarantined_post_upload "$sync" >"$root/fifo.log" 2>&1; then
  echo 'FIFO receipt unexpectedly passed' >&2
  exit 1
fi
grep -F 'non-regular quarantine entry' "$root/fifo.log" >/dev/null
[ "$(wc -l < "$upload_log")" = "$upload_count" ]
rm -f "$quarantine/fifo.json"
missing_relative=segments/ring-sync-selftest-$$/missing.jsonl
missing_sha=sha256:$(printf '2%.0s' $(seq 1 64))
missing_receipt=$quarantine/$(printf '%s:%s:%s' "${#missing_relative}" "$missing_relative" "$missing_sha" | sha256sum | awk '{print $1}').json
jq -cn \
  --arg source_segment_path "$missing_relative" \
  --arg source_sha256 "$missing_sha" \
  '{schema:"polyedge_ring_quarantine.v1",type:"quarantine_receipt",source_segment_path:$source_segment_path,source_sha256:$source_sha256,source_bytes:0,source_lines:0,reason_code:"invalid_recorder_sequence_proof"}' > "$missing_receipt"
if PATH="$fake:$PATH" POLYEDGE_RING_ENV_FILE=$env_file \
  POLYEDGE_TEST_UPLOAD_MODE=success POLYEDGE_TEST_UPLOAD_LOG=$upload_log \
  POLYEDGE_TEST_POST_UPLOAD_SEGMENT=$quarantined_post_upload "$sync" >"$root/missing.log" 2>&1; then
  echo 'missing quarantine source unexpectedly passed' >&2
  exit 1
fi
grep -F 'quarantine receipt source is missing' "$root/missing.log" >/dev/null
[ "$(wc -l < "$upload_log")" = "$upload_count" ]
rm -f "$missing_receipt"
printf '{"test":"near-miss"}
' > "$near_miss"
near_relative=segments/ring-sync-selftest-$$/1785963000.jsonl
zeros=$(printf '0%.0s' $(seq 1 64))
near_receipt=$quarantine/$(printf '%s:%s:%s' "${#near_relative}" "$near_relative" "sha256:$zeros" | sha256sum | awk '{print $1}').json
jq -cn \
  --arg source_segment_path "$near_relative" \
  --arg source_sha256 "sha256:$zeros" \
  '{schema:"polyedge_ring_quarantine.v1",type:"quarantine_receipt",source_segment_path:$source_segment_path,source_sha256:$source_sha256,source_bytes:0,source_lines:0,reason_code:"invalid_recorder_sequence_proof"}' > "$near_receipt"
if PATH="$fake:$PATH" POLYEDGE_RING_ENV_FILE=$env_file POLYEDGE_RING_POST_UPLOAD_SEAL=1 "$sync" >"$root/near-miss.log" 2>&1; then
  echo 'near-miss quarantine receipt unexpectedly passed' >&2
  exit 1
fi
grep -F 'quarantined source changed' "$root/near-miss.log" >/dev/null
rm -f "$near_receipt"
traversal_relative=segments/../escape.jsonl
ones=$(printf '1%.0s' $(seq 1 64))
traversal_receipt=$quarantine/$(printf '%s:%s:%s' "${#traversal_relative}" "$traversal_relative" "sha256:$ones" | sha256sum | awk '{print $1}').json
jq -cn \
  --arg source_segment_path "$traversal_relative" \
  --arg source_sha256 "sha256:$ones" \
  '{schema:"polyedge_ring_quarantine.v1",type:"quarantine_receipt",source_segment_path:$source_segment_path,source_sha256:$source_sha256,source_bytes:0,source_lines:0,reason_code:"invalid_recorder_sequence_proof"}' > "$traversal_receipt"
if PATH="$fake:$PATH" POLYEDGE_RING_ENV_FILE=$env_file POLYEDGE_RING_POST_UPLOAD_SEAL=1 "$sync" >"$root/traversal.log" 2>&1; then
  echo 'traversal quarantine receipt unexpectedly passed' >&2
  exit 1
fi
grep -F 'non-canonical quarantine receipt path' "$root/traversal.log" >/dev/null
rm -f "$traversal_receipt" "$near_miss"

resolved=$root/quarantine/resolved-recorder-sequence-proof-v1/$receipt_id
install -d -m 0750 "$resolved"
cp "$bad" "$resolved/source.jsonl"
cp "$receipt" "$resolved/quarantine-receipt.json"
remote_prefix=events-oci-quarantine-v1/invalid-recorder-sequence-proof/$receipt_id
receipt_sha=sha256:$(sha256sum "$receipt" | awk '{print $1}')
jq -n \
  --arg receipt_id "$receipt_id" \
  --arg source_path "$bad_relative" \
  --arg source_sha "sha256:$bad_hash" \
  --argjson source_bytes "$(wc -c < "$bad")" \
  --argjson source_lines "$(wc -l < "$bad")" \
  --arg receipt_sha "$receipt_sha" \
  --arg remote_prefix "$remote_prefix" \
  '{schema:"polyedge_ring_quarantine_resolution.v1",type:"invalid_recorder_sequence_proof_resolution",disposition:"preserved_historical_pre_boundary_non_parity",active_ring:false,parity_eligible:false,retention_policy:"indefinite_outside_lifecycle",receipt_id:$receipt_id,approval_reference:"approved-test-change",formal_boundary_epoch:1785962400,segment_start_epoch:1785961200,segment_end_epoch:1785961800,source_segment_path:$source_path,source_sha256:$source_sha,source_bytes:$source_bytes,source_lines:$source_lines,quarantine_receipt_sha256:$receipt_sha,remote_prefix:$remote_prefix,source_blob_name:($remote_prefix+"/source.jsonl"),quarantine_receipt_blob_name:($remote_prefix+"/quarantine-receipt.json"),resolution_blob_name:($remote_prefix+"/resolution.json")}' \
  > "$resolved/resolution.json"
resolution_sha=sha256:$(sha256sum "$resolved/resolution.json" | awk '{print $1}')
jq -n \
  --arg receipt_id "$receipt_id" \
  --arg source_sha "sha256:$bad_hash" \
  --arg receipt_sha "$receipt_sha" \
  --arg resolution_sha "$resolution_sha" \
  --arg remote_prefix "$remote_prefix" \
  '{schema:"polyedge_ring_quarantine_resolution_upload.v1",receipt_id:$receipt_id,source_blob_name:($remote_prefix+"/source.jsonl"),quarantine_receipt_blob_name:($remote_prefix+"/quarantine-receipt.json"),resolution_blob_name:($remote_prefix+"/resolution.json"),source_sha256:$source_sha,quarantine_receipt_sha256:$receipt_sha,resolution_sha256:$resolution_sha,verified_ts:"2026-08-17T00:00:00Z"}' \
  > "$resolved/resolution.uploaded.json"
chmod 0640 "$resolved/source.jsonl" "$resolved/quarantine-receipt.json" "$resolved/resolution.json" "$resolved/resolution.uploaded.json"
PATH="$fake:$PATH" POLYEDGE_RING_ENV_FILE=$env_file POLYEDGE_RING_POST_UPLOAD_SEAL=1 "$sync"
[ ! -e "$bad.manifest.json" ]
[ ! -e "$bad_archive" ]
[ ! -e "$root/escape.jsonl" ]
[ ! -e "$bad.sequence."* ]
mv "$root/quarantine" "$root/quarantine-real"
ln -s "$root/quarantine-real" "$root/quarantine"
if PATH="$fake:$PATH" POLYEDGE_RING_ENV_FILE=$env_file POLYEDGE_RING_POST_UPLOAD_SEAL=1 \
  "$sync" >"$root/symlink.log" 2>&1; then
  echo 'symlinked quarantine ancestor unexpectedly passed' >&2
  exit 1
fi
grep -F 'unsafe ring directory' "$root/symlink.log" >/dev/null
printf '%s\n' '#!/bin/sh' 'echo 0' > "$fake/id"
chmod 0755 "$fake/id"
if PATH="$fake:$PATH" POLYEDGE_RING_ENV_FILE=$env_file \
  ops/conduit/bin/polyedge-ring-quarantine "$receipt_id" 1785962400 approved-test-change \
  >"$root/quarantine-helper-symlink.log" 2>&1; then
  echo 'quarantine helper accepted a symlinked ancestor' >&2
  exit 1
fi
grep -F 'unsafe or missing ring directory' "$root/quarantine-helper-symlink.log" >/dev/null
grep -F -- '--cap-drop=all --cap-add=DAC_OVERRIDE' ops/conduit/bin/polyedge-ring-sync >/dev/null
grep -F -- '-v "$segments:/srv/polyedge-ring/segments:Z"' ops/conduit/bin/polyedge-ring-sync >/dev/null
grep -F -- '-v "$archive:/srv/polyedge-ring/archive:Z"' ops/conduit/bin/polyedge-ring-sync >/dev/null
grep -F 'prefix=${POLYEDGE_RING_BLOB_PREFIX:-events-oci-hot7-v1}' ops/conduit/bin/polyedge-ring-sync >/dev/null
grep -F '[ "$(id -u)" -eq 0 ]' ops/conduit/bin/polyedge-ring-quarantine >/dev/null
grep -F '/usr/bin/flock -n 9' ops/conduit/bin/polyedge-ring-quarantine >/dev/null
grep -F '[ "${POLYEDGE_AZURE_ARC_IDENTITY:-}" = 1 ]' ops/conduit/bin/polyedge-ring-quarantine >/dev/null
grep -F '/usr/bin/timeout --signal=TERM --kill-after=60s 3h' ops/conduit/bin/polyedge-ring-quarantine >/dev/null
! grep -F 'AZURE_CLIENT_SECRET_FILE' ops/conduit/bin/polyedge-ring-quarantine >/dev/null
grep -F -- '-v "$segments:/srv/polyedge-ring/segments:ro,Z"' ops/conduit/bin/polyedge-ring-quarantine >/dev/null
grep -F 'ring-quarantine-resolve' ops/conduit/bin/polyedge-ring-quarantine >/dev/null
grep -F 'events-oci-quarantine-v1/invalid-recorder-sequence-proof' crates/polyedge-cli/src/main.rs >/dev/null

echo 'ring sealer self-test passed'
