#!/bin/sh
set -eu

root=$(mktemp -d)
trap 'rm -rf "$root"' EXIT HUP INT TERM
collector=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)/bin/polyedge-parity-hourly
uid=$(id -u)
gid=$(id -g)
fake=$root/fake-bin
mkdir -p "$fake"

cat >"$fake/curl" <<'EOF'
#!/bin/sh
printf '%s\n' "$*" >>"$FAKE_CALLS/curl"
out=
previous=
for arg do
  [ "$previous" != output ] || out=$arg
  previous=
  [ "$arg" != --output ] || previous=output
done
[ -n "$out" ] || exit 2
case "$*" in
  *oauth2/v2.0/token*) printf '%s\n' '{"access_token":"fixture-access-token"}' >"$out" ;;
  *) cp "$FAKE_AZURE_REPORT" "$out" ;;
esac
EOF

cat >"$fake/podman" <<'EOF'
#!/bin/sh
case "$1" in
  inspect) printf '%s\n' healthy ;;
  run)
    printf '%s\n' "$*" >>"$FAKE_CALLS/podman"
    host=
    for arg do case "$arg" in *:/evidence:Z) host=${arg%:/evidence:Z} ;; esac; done
    [ -n "$host" ] || exit 2
    cp "$FAKE_SAME_REPORT" "$host/.same-input-audit.tmp"
    ;;
  *) exit 2 ;;
esac
EOF

cat >"$fake/systemctl" <<'EOF'
#!/bin/sh
case "$1" in
  is-active)
    case "$3" in polyedge-job@shadow-qset.service|polyedge-funded-signer.service) exit 3 ;; *) exit 0 ;; esac
    ;;
  is-enabled)
    case "$2" in polyedge-shadow-qset.timer) printf '%s\n' not-found; exit 1 ;; polyedge-funded-signer.service) printf '%s\n' masked ;; *) exit 2 ;; esac
    ;;
  *) exit 2 ;;
esac
EOF

cat >"$fake/mountpoint" <<'EOF'
#!/bin/sh
[ "$FAKE_MOUNTPOINT_OK" = 1 ]
EOF

cat >"$fake/df" <<'EOF'
#!/bin/sh
printf '%s\n' 'Filesystem 1-blocks Used Available Capacity Mounted on'
printf 'fixture 100000000000 1 %s 1%% /\n' "$FAKE_DF_AVAILABLE"
EOF

cat >"$fake/flock" <<'EOF'
#!/bin/sh
exit 0
EOF

cat >"$fake/timeout" <<'EOF'
#!/bin/sh
[ "$1" != --preserve-status ] || shift
shift
exec "$@"
EOF
chmod 0755 "$fake"/*

report() {
  marker=$1 output=$2
  jq -n --arg marker "$marker" '{
    git_sha:"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    generated_at:"2026-08-09T16:13:00Z",
    input_path:"azure://stpolyedge6urdjr5nmwx7w/bot-events/events/2026/08/09/15/?prefetch_blobs=8",
    result:{total_events:123,fatal_data_quality_issues:[],runtime_provenance:{observations:60,valid_observations:60,invalid_observations:0,first_timestamp:"2026-08-09T15:00:01Z",last_timestamp:"2026-08-09T15:59:59Z"},fixture_marker:$marker},
    warnings:[]
  }' >"$output"
}

fixture() {
  case_root=$1
  target=${2:-2026-08-09T15:00:00Z}
  window=${3:-2026-08-09T14:10:00Z}
  mkdir -p "$case_root/calls" "$case_root/run" "$case_root/token" "$case_root/ring/parity" \
    "$case_root/ring/segments/2026/08/09/15" "$case_root/ring/archive/2026/08/09/15" \
    "$case_root/reports/2026/08/09/15"
  chmod 0700 "$case_root/run" "$case_root/token"
  chmod 0750 "$case_root/ring/parity"
  : >"$case_root/token/azure-federated-token"
  chmod 0600 "$case_root/token/azure-federated-token"
  jq -n --arg start "$window" '{
    schemaVersion:1,status:"in_progress",azureAuthoritative:true,azureDeletionAllowed:false,
    windowStartUtc:$start,acceptedCleanLiveHours:0,rebootRecoveryPassed:false,
    shadowQsetEnabled:false,fundedSignerEnabled:false
  }' >"$case_root/ring/parity/ledger.json"
  chmod 0640 "$case_root/ring/parity/ledger.json"
  jq -n '{capacity_ok:true,free_ok:true,upload_fresh:true,unsealed_closed_count:0,unuploaded_count:0}' >"$case_root/ring/status.json"
  chmod 0640 "$case_root/ring/status.json"
  fixture_now=$(date -u -d '2026-08-09T16:20:00Z' +%s)
  touch -d "@$fixture_now" "$case_root/ring/status.json"
  cat >"$case_root/hourly.env" <<'EOF'
POLYEDGE_RESEARCH_IMAGE=ghcr.io/aldoapicella/polyedge-rust-backend@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
POLYEDGE_DISABLE_RESEARCH_ARTIFACT_PUBLISH=true
AZURE_STORAGE_ACCOUNT_NAME=stpolyedge6urdjr5nmwx7w
AZURE_STORAGE_CONTAINER_NAME=bot-events
AZURE_TENANT_ID=tenant
AZURE_CLIENT_ID=client
AZURE_FEDERATED_TOKEN_FILE=/run/credentials/azure-federated-token
EOF
  chmod 0600 "$case_root/hourly.env"
  cat >"$case_root/parity.env" <<EOF
POLYEDGE_PARITY_WINDOW_START_UTC=$window
POLYEDGE_PARITY_LEDGER=$case_root/ring/parity/ledger.json
POLYEDGE_PARITY_HOURLY_ENV=$case_root/hourly.env
POLYEDGE_PARITY_RING_ROOT=$case_root/ring
POLYEDGE_PARITY_ROOT=$case_root/ring/parity
POLYEDGE_PARITY_SEGMENTS_ROOT=$case_root/ring/segments
POLYEDGE_PARITY_ARCHIVE_ROOT=$case_root/ring/archive
POLYEDGE_PARITY_REPORT_ROOT=$case_root/reports
POLYEDGE_PARITY_RING_STATUS=$case_root/ring/status.json
POLYEDGE_PARITY_BOOT_ROOT=$case_root
POLYEDGE_PARITY_PAUSE_FILE=$case_root/run/image-pulls-paused
POLYEDGE_PARITY_TOKEN_FILE=$case_root/token/azure-federated-token
POLYEDGE_PARITY_RUNTIME_DIR=$case_root/run
POLYEDGE_PARITY_LOCK_FILE=$case_root/run/research.lock
POLYEDGE_PARITY_TARGET_HOUR_UTC=$target
POLYEDGE_PARITY_NOW_EPOCH=$fixture_now
POLYEDGE_PARITY_TOKEN_UID=$uid
POLYEDGE_PARITY_TOKEN_GID=$gid
EOF
  chmod 0640 "$case_root/parity.env"
  report azure "$case_root/azure.json"
  report azure "$case_root/reports/2026/08/09/15/audit.json"
  jq '.input_path="/input/events/2026/08/09/15/"' "$case_root/reports/2026/08/09/15/audit.json" >"$case_root/oci.tmp"
  mv "$case_root/oci.tmp" "$case_root/reports/2026/08/09/15/audit.json"
  report azure "$case_root/same.json"

  i=0
  start=$(date -u -d '2026-08-09T15:00:00Z' +%s)
  while [ "$i" -lt 6 ]; do
    epoch=$((start + i * 600))
    source=$case_root/ring/segments/2026/08/09/15/$epoch.jsonl
    gzip_file=$case_root/ring/archive/2026/08/09/15/$epoch.jsonl.gz
    manifest=$gzip_file.manifest.json
    receipt=$manifest.uploaded.json
    printf '{"fixture":%s}\n' "$i" >"$source"
    gzip -1 -n -c "$source" >"$gzip_file"
    source_sha=sha256:$(sha256sum "$source" | awk '{print $1}')
    gzip_sha=sha256:$(sha256sum "$gzip_file" | awk '{print $1}')
    jq -n --arg segment "segments/2026/08/09/15/$epoch.jsonl" \
      --arg archive "archive/2026/08/09/15/$epoch.jsonl.gz" --arg blob "events-oci-hot7-v1/2026/08/09/15/$epoch.jsonl.gz" \
      --arg source_sha "$source_sha" --arg gzip_sha "$gzip_sha" --argjson start "$epoch" \
      '{schema_version:2,segment_path:$segment,archive_path:$archive,blob_name:$blob,compression:"gzip",sha256:$gzip_sha,source_sha256:$source_sha,segment_start_epoch:$start,segment_end_epoch:($start+600)}' >"$manifest"
    manifest_sha=sha256:$(sha256sum "$manifest" | awk '{print $1}')
    jq -n --arg blob "events-oci-hot7-v1/2026/08/09/15/$epoch.jsonl.gz" --arg sha "$manifest_sha" \
      '{schema_version:1,blob_name:$blob,manifest_blob_name:($blob+".manifest.json"),manifest_sha256:$sha,verified_ts:"2026-08-09T16:12:00Z"}' >"$receipt"
    i=$((i + 1))
  done
}

run_collector() {
  case_root=$1
  env PATH="$fake:$PATH" \
    POLYEDGE_PARITY_EXPECTED_UID="$uid" POLYEDGE_PARITY_EXPECTED_GID="$gid" \
    POLYEDGE_PARITY_ENV_FILE="$case_root/parity.env" \
    FAKE_CALLS="$case_root/calls" FAKE_DF_AVAILABLE="${FAKE_DF_AVAILABLE:-20000000000}" FAKE_MOUNTPOINT_OK="${FAKE_MOUNTPOINT_OK:-1}" \
    FAKE_AZURE_REPORT="$case_root/azure.json" FAKE_SAME_REPORT="$case_root/same.json" \
    "$collector"
}

protected() {
  jq -cS '{status,azureAuthoritative,azureDeletionAllowed,rebootRecoveryPassed,shadowQsetEnabled,fundedSignerEnabled}' "$1"
}
seed_artifacts() {
  case_root=$1
  which=$2
  artifact_dir=$case_root/ring/parity/hourly/20260809T15
  mkdir -p "$artifact_dir"
  chmod 0750 "$case_root/ring/parity/hourly" "$artifact_dir"
  case "$which" in
    azure|both) cp "$case_root/azure.json" "$artifact_dir/azure-scheduled-audit.json"; chmod 0640 "$artifact_dir/azure-scheduled-audit.json" ;;
  esac
  case "$which" in
    same|both) cp "$case_root/same.json" "$artifact_dir/same-input-audit.json"; chmod 0640 "$artifact_dir/same-input-audit.json" ;;
  esac
}

success=$root/success
fixture "$success"
jq '.rebootRecoveryPassed = true' "$success/ring/parity/ledger.json" >"$success/reboot.tmp"
chmod 0640 "$success/reboot.tmp"
mv "$success/reboot.tmp" "$success/ring/parity/ledger.json"
before=$(protected "$success/ring/parity/ledger.json")
run_collector "$success" >/dev/null
[ "$(jq -r '.acceptedCleanLiveHours' "$success/ring/parity/ledger.json")" = 1 ]
jq -e '.acceptedForParityWindow == true and .sameInput.deterministicResultExactMatch == true and (.segments | length) == 6' \
  "$success/ring/parity/hourly/20260809T15/evidence.json" >/dev/null
[ "$(protected "$success/ring/parity/ledger.json")" = "$before" ]
runs=$(wc -l <"$success/calls/podman")
run_collector "$success" >/dev/null
[ "$(jq -r '.acceptedCleanLiveHours' "$success/ring/parity/ledger.json")" = 1 ]
[ "$(wc -l <"$success/calls/podman")" = "$runs" ]
recovery=$root/recovery
fixture "$recovery"
seed_artifacts "$recovery" both
run_collector "$recovery" >/dev/null
[ "$(jq -r '.acceptedCleanLiveHours' "$recovery/ring/parity/ledger.json")" = 1 ]
[ ! -e "$recovery/calls/curl" ] && [ ! -e "$recovery/calls/podman" ]

azure_only=$root/azure-only
fixture "$azure_only"
seed_artifacts "$azure_only" azure
run_collector "$azure_only" >/dev/null
[ "$(jq -r '.acceptedCleanLiveHours' "$azure_only/ring/parity/ledger.json")" = 1 ]
[ ! -e "$azure_only/calls/curl" ] && [ "$(wc -l <"$azure_only/calls/podman")" = 1 ]

same_only=$root/same-only
fixture "$same_only"
seed_artifacts "$same_only" same
run_collector "$same_only" >/dev/null
[ "$(jq -r '.acceptedCleanLiveHours' "$same_only/ring/parity/ledger.json")" = 1 ]
[ "$(wc -l <"$same_only/calls/curl")" = 2 ] && [ ! -e "$same_only/calls/podman" ]

mismatch=$root/mismatch
fixture "$mismatch"
report mismatch "$mismatch/same.json"
if run_collector "$mismatch" >/dev/null 2>&1; then
  echo 'comparison mismatch unexpectedly passed' >&2
  exit 1
fi
[ "$(jq -r '.acceptedCleanLiveHours' "$mismatch/ring/parity/ledger.json")" = 0 ]
[ ! -e "$mismatch/ring/parity/hourly/20260809T15/evidence.json" ]

disk=$root/disk
fixture "$disk"
if FAKE_DF_AVAILABLE=16106127359 run_collector "$disk" >/dev/null 2>&1; then
  echo 'disk-floor failure unexpectedly passed' >&2
  exit 1
fi
[ "$(jq -r '.acceptedCleanLiveHours' "$disk/ring/parity/ledger.json")" = 0 ]

excluded=$root/excluded
mount_failure=$root/mount-failure
fixture "$mount_failure"
if FAKE_MOUNTPOINT_OK=0 run_collector "$mount_failure" >/dev/null 2>&1; then
  echo 'non-mount ring root unexpectedly passed' >&2
  exit 1
fi

redirect=$root/redirect
fixture "$redirect"
cp "$redirect/ring/parity/ledger.json" "$redirect/redirected.json"
chmod 0640 "$redirect/redirected.json"
sed -i "s#POLYEDGE_PARITY_LEDGER=.*#POLYEDGE_PARITY_LEDGER=$redirect/redirected.json#" "$redirect/parity.env"
if run_collector "$redirect" >/dev/null 2>&1; then
  echo 'redirected ledger unexpectedly passed' >&2
  exit 1
fi
fixture "$excluded" 2026-08-09T14:00:00Z 2026-08-09T14:10:00Z
before=$(protected "$excluded/ring/parity/ledger.json")
run_collector "$excluded" >/dev/null
[ "$(jq -r '.acceptedCleanLiveHours' "$excluded/ring/parity/ledger.json")" = 0 ]
jq -e '.status == "excluded_pre_window" and .acceptedForParityWindow == false' \
  "$excluded/ring/parity/hourly/20260809T14/evidence.json" >/dev/null
[ "$(protected "$excluded/ring/parity/ledger.json")" = "$before" ]

if grep -R -F 'fixture-access-token' "$root"/*/calls >/dev/null || grep -R -F 'fixture-jwt' "$root"/*/calls >/dev/null; then
  echo 'a token leaked into command arguments' >&2
  exit 1
fi

echo 'parity hourly collector self-test passed'
