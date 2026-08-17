#!/bin/sh
set -eu

root=$(mktemp -d)
fake=$root/fake-bin
env_file=$root/ring.env
status_file=$root/status.json
source_relative=segments/2026/08/16/17/1786900200.jsonl
source_file=$root/$source_relative
quarantine=$root/quarantine/recorder-sequence-proof-v1
resolved_root=$root/quarantine/resolved-recorder-sequence-proof-v1
cleanup() { rm -rf "$root"; }
trap cleanup EXIT HUP INT TERM

install -d -m 0750 "${source_file%/*}" "$root/archive/2026/08/16/17" "$quarantine" "$resolved_root" "$fake"
printf '%s\n' '#!/bin/sh' 'exit 0' > "$fake/mountpoint"
chmod 0755 "$fake/mountpoint"
printf '%s\n' \
  'POLYEDGE_RING_SEAL_ONLY=1' \
  "POLYEDGE_RING_ROOT=$root" \
  "POLYEDGE_RING_STATUS=$status_file" \
  'POLYEDGE_RING_RETENTION_HOURS=48' \
  'POLYEDGE_RING_SEAL_GRACE_SECONDS=60' \
  'POLYEDGE_RING_MIN_FREE_BYTES=0' \
  'POLYEDGE_RING_MAX_UNUPLOADED_SECONDS=1200' \
  'RECORDER_SEGMENT_SECONDS=600' > "$env_file"

PATH="$fake:$PATH" POLYEDGE_RING_ENV_FILE=$env_file POLYEDGE_RING_HEALTH_TEST=1 \
  ops/conduit/bin/polyedge-ring-health
jq -e '.unresolved_quarantine_count == 0 and .resolved_quarantine_count == 0 and .resolved_quarantine_bytes == 0 and .malformed_quarantine_count == 0' "$status_file" >/dev/null
low_space_env=$root/ring-low-space.env
sed 's/POLYEDGE_RING_MIN_FREE_BYTES=0/POLYEDGE_RING_MIN_FREE_BYTES=999999999999/' "$env_file" > "$low_space_env"
if PATH="$fake:$PATH" POLYEDGE_RING_ENV_FILE=$low_space_env POLYEDGE_RING_HEALTH_TEST=1 \
  ops/conduit/bin/polyedge-ring-health; then
  echo 'low-space health check unexpectedly passed' >&2
  exit 1
fi
jq -e '.free_ok == false and .seal_only == true and .retention_hours == 48' "$status_file" >/dev/null

printf '%s\n' \
  '{"recorder_instance_id":"7c66d77b-a911-4f9b-95f2-98ca9395255e","recorder_sequence":1}' \
  '{"recorder_instance_id":"7c66d77b-a911-4f9b-95f2-98ca9395255e","recorder_sequence":3}' > "$source_file"
source_sha=sha256:$(sha256sum "$source_file" | awk '{print $1}')
source_bytes=$(wc -c < "$source_file")
source_lines=$(wc -l < "$source_file")
receipt_id=$(printf '%s:%s:%s' "${#source_relative}" "$source_relative" "$source_sha" | sha256sum | awk '{print $1}')
receipt=$quarantine/$receipt_id.json
jq -cn \
  --arg source_path "$source_relative" --arg source_sha "$source_sha" \
  --argjson source_bytes "$source_bytes" --argjson source_lines "$source_lines" \
  '{schema:"polyedge_ring_quarantine.v1",type:"quarantine_receipt",source_segment_path:$source_path,source_sha256:$source_sha,source_bytes:$source_bytes,source_lines:$source_lines,reason_code:"invalid_recorder_sequence_proof"}' \
  > "$receipt"

if PATH="$fake:$PATH" POLYEDGE_RING_ENV_FILE=$env_file POLYEDGE_RING_HEALTH_TEST=1 \
  ops/conduit/bin/polyedge-ring-health; then
  echo 'unresolved quarantine health check unexpectedly passed' >&2
  exit 1
fi
jq -e '.unresolved_quarantine_count == 1 and .resolved_quarantine_count == 0 and .unsealed_closed_count == 0' "$status_file" >/dev/null

resolved=$resolved_root/$receipt_id
remote_prefix=events-oci-quarantine-v1/invalid-recorder-sequence-proof/$receipt_id
receipt_sha=sha256:$(sha256sum "$receipt" | awk '{print $1}')
write_resolution() {
  boundary=$1
  rm -rf "$resolved"
  install -d -m 0750 "$resolved"
  cp "$source_file" "$resolved/source.jsonl"
  cp "$receipt" "$resolved/quarantine-receipt.json"
  jq -n \
    --arg receipt_id "$receipt_id" --arg source_path "$source_relative" \
    --arg source_sha "$source_sha" --argjson source_bytes "$source_bytes" \
    --argjson source_lines "$source_lines" --arg receipt_sha "$receipt_sha" \
    --arg remote_prefix "$remote_prefix" --argjson boundary "$boundary" \
    '{schema:"polyedge_ring_quarantine_resolution.v1",type:"invalid_recorder_sequence_proof_resolution",disposition:"preserved_historical_pre_boundary_non_parity",active_ring:false,parity_eligible:false,retention_policy:"indefinite_outside_lifecycle",receipt_id:$receipt_id,approval_reference:"approved-test-change",formal_boundary_epoch:$boundary,segment_start_epoch:1786900200,segment_end_epoch:1786900800,source_segment_path:$source_path,source_sha256:$source_sha,source_bytes:$source_bytes,source_lines:$source_lines,quarantine_receipt_sha256:$receipt_sha,remote_prefix:$remote_prefix,source_blob_name:($remote_prefix+"/source.jsonl"),quarantine_receipt_blob_name:($remote_prefix+"/quarantine-receipt.json"),resolution_blob_name:($remote_prefix+"/resolution.json")}' \
    > "$resolved/resolution.json"
  resolution_sha=sha256:$(sha256sum "$resolved/resolution.json" | awk '{print $1}')
  jq -n \
    --arg receipt_id "$receipt_id" --arg source_sha "$source_sha" \
    --arg receipt_sha "$receipt_sha" --arg resolution_sha "$resolution_sha" \
    --arg remote_prefix "$remote_prefix" \
    '{schema:"polyedge_ring_quarantine_resolution_upload.v1",receipt_id:$receipt_id,source_blob_name:($remote_prefix+"/source.jsonl"),quarantine_receipt_blob_name:($remote_prefix+"/quarantine-receipt.json"),resolution_blob_name:($remote_prefix+"/resolution.json"),source_sha256:$source_sha,quarantine_receipt_sha256:$receipt_sha,resolution_sha256:$resolution_sha,verified_ts:"2026-08-17T00:00:00Z"}' \
    > "$resolved/resolution.uploaded.json"
  chmod 0640 "$resolved/source.jsonl" "$resolved/quarantine-receipt.json" "$resolved/resolution.json" "$resolved/resolution.uploaded.json"
}

write_resolution 1786924800
PATH="$fake:$PATH" POLYEDGE_RING_ENV_FILE=$env_file POLYEDGE_RING_HEALTH_TEST=1 \
  ops/conduit/bin/polyedge-ring-health
jq -e --argjson bytes "$source_bytes" '.unresolved_quarantine_count == 0 and .resolved_quarantine_count == 1 and .resolved_quarantine_bytes == $bytes and .malformed_quarantine_count == 0 and .unsealed_closed_count == 0' "$status_file" >/dev/null
chmod 0644 "$resolved/source.jsonl"
if PATH="$fake:$PATH" POLYEDGE_RING_ENV_FILE=$env_file POLYEDGE_RING_HEALTH_TEST=1 \
  ops/conduit/bin/polyedge-ring-health; then
  echo 'open-mode resolution health check unexpectedly passed' >&2
  exit 1
fi
chmod 0640 "$resolved/source.jsonl"

write_resolution 1786900200
if PATH="$fake:$PATH" POLYEDGE_RING_ENV_FILE=$env_file POLYEDGE_RING_HEALTH_TEST=1 \
  ops/conduit/bin/polyedge-ring-health; then
  echo 'post-boundary resolution health check unexpectedly passed' >&2
  exit 1
fi
jq -e '.unresolved_quarantine_count == 1 and .malformed_quarantine_count == 1' "$status_file" >/dev/null

write_resolution 1786924800
rm "$resolved/resolution.uploaded.json"
if PATH="$fake:$PATH" POLYEDGE_RING_ENV_FILE=$env_file POLYEDGE_RING_HEALTH_TEST=1 \
  ops/conduit/bin/polyedge-ring-health; then
  echo 'partial resolution health check unexpectedly passed' >&2
  exit 1
fi

write_resolution 1786924800
printf 'tampered\n' > "$resolved/source.jsonl"
if PATH="$fake:$PATH" POLYEDGE_RING_ENV_FILE=$env_file POLYEDGE_RING_HEALTH_TEST=1 \
  ops/conduit/bin/polyedge-ring-health; then
  echo 'tampered resolution health check unexpectedly passed' >&2
  exit 1
fi

write_resolution 1786924800
rm "$source_file"
if PATH="$fake:$PATH" POLYEDGE_RING_ENV_FILE=$env_file POLYEDGE_RING_HEALTH_TEST=1 \
  ops/conduit/bin/polyedge-ring-health; then
  echo 'missing quarantine source health check unexpectedly passed' >&2
  exit 1
fi
jq -e '.malformed_quarantine_count > 0' "$status_file" >/dev/null

mv "$root/quarantine" "$root/quarantine-real"
ln -s "$root/quarantine-real" "$root/quarantine"
if PATH="$fake:$PATH" POLYEDGE_RING_ENV_FILE=$env_file POLYEDGE_RING_HEALTH_TEST=1 \
  ops/conduit/bin/polyedge-ring-health >"$root/symlink.log" 2>&1; then
  echo 'symlinked quarantine ancestor health check unexpectedly passed' >&2
  exit 1
fi
grep -F 'unsafe or missing ring directory' "$root/symlink.log" >/dev/null

echo 'ring health self-test passed'
