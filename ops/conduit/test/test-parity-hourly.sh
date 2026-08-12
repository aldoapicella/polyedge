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
  exec) printf 'HTTP/1.0 200 OK\r\n\r\n'; cat "$FAKE_API_STATUS" ;;
  run)
    printf '%s\n' "$*" >>"$FAKE_CALLS/podman"
    host=
    for arg do case "$arg" in *:/evidence:rw,Z) host=${arg%:/evidence:rw,Z} ;; esac; done
    [ -n "$host" ] || exit 2
    cp "$FAKE_SAME_REPORT" "$host/audit.json"
    : >"$host/audit.md"
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
    result:{
      total_events:123,fatal_data_quality_issues:[],fixture_marker:$marker,
      runtime_provenance:{
        observations:60,valid_observations:60,invalid_observations:0,distinct_identity_count:1,
        first_timestamp:"2026-08-09T15:00:01Z",last_timestamp:"2026-08-09T15:59:59Z",max_gap_ms:60000,
        identities:[{
          app_name:"polyedge",runtime_role:"primary",execution_mode:"paper",allow_live:false,enable_taker_orders:false,
          allow_emergency_account_cancel:false,research_only:true,shadow_only:false,backend_impl:"rust",
          storage_container:"bot-events",event_blob_prefix:"events",adaptive_regime_enabled:false,
          adaptive_regime_mode:"paper_only",paper_maker_fill_policy:"touch_after_quote_was_live",
          publish_strategy_canary_intents:false,candidate:null,
          git_sha:"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
          runtime_config_hash:"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
          decision_config_schema:"polyedge.decision_config.v1",
          decision_config_sha256:"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
          decision_pipeline_schema:"polyedge.strategy_decision_batch.v4",
          decision_pipeline_parity_scope:"full_decision_pipeline_recomputation"
        }]
      },
      strategy_batches:1,strategy_batch_replayed:1,strategy_batch_matches:1,
      strategy_batch_invalid:0,strategy_batch_contract_invalid:0,
      strategy_batch_contract_invalid_reasons:{},strategy_batch_missing_independent_start:0,
      strategy_batch_ineligible:0,strategy_batch_conflicts:0,strategy_binding_ineligible:0,
      strategy_binding_conflicts:0,unbound_strategy_decisions:0,decision_application_invalid:0,
      decision_application_conflicts:0,orphan_decision_applications:0,
      decision_pipeline_replay_rate:1,decision_output_binding_rate:1,decision_parity_rate:1
    },
    warnings:[]
  }' >"$output"
}

fixture() {
  case_root=$1
  target=${2:-2026-08-09T15:00:00Z}
  window=${3:-2026-08-09T14:10:00Z}
  manifest_schema=${4:-3}
  mkdir -p "$case_root/calls" "$case_root/run" "$case_root/token" "$case_root/ring/parity" \
    "$case_root/ring/segments/2026/08/09/15" "$case_root/ring/archive/2026/08/09/15" \
    "$case_root/reports/2026/08/09/15"
  chmod 0700 "$case_root/run" "$case_root/token"
  chmod 0750 "$case_root/ring/parity"
  : >"$case_root/token/azure-federated-token"
  chmod 0600 "$case_root/token/azure-federated-token"
  jq -n --arg start "$window" '{
    schemaVersion:1,status:"in_progress",azureAuthoritative:true,azureDeletionAllowed:false,
    windowStartUtc:$start,acceptedCleanLiveHours:0,acceptedHourlyEvidence:[],
    completedDailyCycles:0,acceptedDailyEvidence:[],rebootRecoveryPassed:false,
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
POLYEDGE_PARITY_LOCK_FILE=$case_root/run/ledger.lock
POLYEDGE_PARITY_TARGET_HOUR_UTC=$target
POLYEDGE_PARITY_NOW_EPOCH=$fixture_now
POLYEDGE_PARITY_STATUS_NOW_EPOCH=$fixture_now
POLYEDGE_PARITY_TOKEN_UID=$uid
POLYEDGE_PARITY_TOKEN_GID=$gid
EOF
  chmod 0640 "$case_root/parity.env"
  report azure "$case_root/azure.json"
  report azure "$case_root/reports/2026/08/09/15/audit.json"
  jq '.input_path="/input/events/2026/08/09/15/"' "$case_root/reports/2026/08/09/15/audit.json" >"$case_root/oci.tmp"
  mv "$case_root/oci.tmp" "$case_root/reports/2026/08/09/15/audit.json"
  report azure "$case_root/same.json"
  jq -n '{
    task_health:{api:"ok",runtime_loop:"running",feeds:"running"},
    feed_status:{
      Discovery:{status:"ok",updated_at:"2026-08-09T16:19:59Z"},
      PolymarketClobMarket:{status:"ok",updated_at:"2026-08-09T16:19:59.123456789Z"},
      PolymarketRtdsChainlink:{status:"ok",updated_at:"2026-08-09T16:19:59Z"},
      PolymarketRtdsBinance:{status:"ok",updated_at:"2026-08-09T16:19:59Z"}
    },
    drop_counts:{},recorder_status:{error_count:0,dropped_count:0},
    recorder_metrics:{recorder_instance_id:"123e4567-e89b-42d3-a456-426614174000",last_assigned_sequence:60,queued:0,enqueued_total:60,persisted_total:60,failed_total:0,unrecovered_durable_events:0,flush_unrecovered:false}
  }' >"$case_root/api-status.json"

  i=0
  start=$(date -u -d '2026-08-09T15:00:00Z' +%s)
  while [ "$i" -lt 6 ]; do
    epoch=$((start + i * 600))
    first=$((i * 10 + 1))
    last=$((first + 9))
    source=$case_root/ring/segments/2026/08/09/15/$epoch.jsonl
    gzip_file=$case_root/ring/archive/2026/08/09/15/$epoch.jsonl.gz
    manifest=$gzip_file.manifest.json
    receipt=$manifest.uploaded.json
    minute=0
    while [ "$minute" -lt 10 ]; do
      seq=$((first + minute))
      observed=$(date -u -d "@$((epoch + minute * 60 + 1))" +%Y-%m-%dT%H:%M:%SZ)
      jq -nc --arg observed "$observed" --argjson seq "$seq" '{
        event_type:"runtime_provenance",recorded_ts:$observed,
        payload:{essential_feed_health:{summary:"running",feed_status:{
          Discovery:{status:"ok",updated_at:$observed},
          PolymarketClobMarket:{status:"ok",updated_at:$observed},
          PolymarketRtdsChainlink:{status:"ok",updated_at:$observed},
          PolymarketRtdsBinance:{status:"ok",updated_at:$observed}
        }}},recorder_instance_id:"123e4567-e89b-42d3-a456-426614174000",recorder_sequence:$seq
      }' >>"$source"
      minute=$((minute + 1))
    done
    gzip -1 -n -c "$source" >"$gzip_file"
    source_sha=sha256:$(sha256sum "$source" | awk '{print $1}')
    gzip_sha=sha256:$(sha256sum "$gzip_file" | awk '{print $1}')
    jq -n --arg segment "segments/2026/08/09/15/$epoch.jsonl" \
      --arg archive "archive/2026/08/09/15/$epoch.jsonl.gz" --arg blob "events-oci-hot7-v1/2026/08/09/15/$epoch.jsonl.gz" \
      --arg source_sha "$source_sha" --arg gzip_sha "$gzip_sha" --argjson start "$epoch" --argjson first "$first" --argjson last "$last" \
      --argjson schema "$manifest_schema" \
      '{schema_version:$schema,lines:10,segment_path:$segment,archive_path:$archive,blob_name:$blob,compression:"gzip",sha256:$gzip_sha,source_sha256:$source_sha,segment_start_epoch:$start,segment_end_epoch:($start+600)} +
       (if $schema == 4 then {recorder_runs:[{recorder_instance_id:"123e4567-e89b-42d3-a456-426614174000",recorder_first_sequence:$first,recorder_last_sequence:$last,recorder_event_count:10}]}
        else {recorder_instance_id:"123e4567-e89b-42d3-a456-426614174000",recorder_first_sequence:$first,recorder_last_sequence:$last,recorder_event_count:10} end)' >"$manifest"
    manifest_sha=sha256:$(sha256sum "$manifest" | awk '{print $1}')
    jq -n --arg blob "events-oci-hot7-v1/2026/08/09/15/$epoch.jsonl.gz" --arg sha "$manifest_sha" \
      '{schema_version:1,blob_name:$blob,manifest_blob_name:($blob+".manifest.json"),manifest_sha256:$sha,verified_ts:"2026-08-09T16:12:00Z"}' >"$receipt"
    i=$((i + 1))
  done
}

refresh_segment() {
  source=$1
  gzip_file=$(printf '%s\n' "$source" | sed 's#/segments/#/archive/#').gz
  manifest=$gzip_file.manifest.json
  receipt=$manifest.uploaded.json
  gzip -1 -n -c "$source" >"$gzip_file"
  source_sha=sha256:$(sha256sum "$source" | awk '{print $1}')
  gzip_sha=sha256:$(sha256sum "$gzip_file" | awk '{print $1}')
  lines=$(wc -l <"$source")
  first=$(jq -r -s '.[0].recorder_sequence' "$source")
  last=$(jq -r -s '.[-1].recorder_sequence' "$source")
  jq --arg source_sha "$source_sha" --arg gzip_sha "$gzip_sha" --argjson lines "$lines" \
    --argjson first "$first" --argjson last "$last" \
    '.source_sha256=$source_sha | .sha256=$gzip_sha | .lines=$lines |
     .recorder_first_sequence=$first | .recorder_last_sequence=$last | .recorder_event_count=$lines' \
    "$manifest" >"$manifest.tmp"
  mv "$manifest.tmp" "$manifest"
  manifest_sha=sha256:$(sha256sum "$manifest" | awk '{print $1}')
  jq --arg sha "$manifest_sha" '.manifest_sha256=$sha' "$receipt" >"$receipt.tmp"
  mv "$receipt.tmp" "$receipt"
}

run_collector() {
  case_root=$1
  env PATH="$fake:$PATH" \
    POLYEDGE_PARITY_EXPECTED_UID="$uid" POLYEDGE_PARITY_EXPECTED_GID="$gid" \
    POLYEDGE_PARITY_ENV_FILE="$case_root/parity.env" \
    FAKE_CALLS="$case_root/calls" FAKE_DF_AVAILABLE="${FAKE_DF_AVAILABLE:-20000000000}" FAKE_MOUNTPOINT_OK="${FAKE_MOUNTPOINT_OK:-1}" \
    FAKE_AZURE_REPORT="$case_root/azure.json" FAKE_SAME_REPORT="$case_root/same.json" FAKE_API_STATUS="$case_root/api-status.json" \
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

v2_manifest=$root/v2-manifest
fixture "$v2_manifest"
manifest=$(find "$v2_manifest/ring/archive" -name '*.manifest.json' | head -n 1)
jq '.schema_version=2' "$manifest" >"$manifest.tmp" && mv "$manifest.tmp" "$manifest"
if run_collector "$v2_manifest" >/dev/null 2>&1; then echo 'v2 manifest unexpectedly passed' >&2; exit 1; fi

gap=$root/sequence-gap
fixture "$gap"
manifest=$(find "$gap/ring/archive" -name '*.manifest.json' | sort | sed -n '2p')
jq '.recorder_first_sequence=8 | .recorder_last_sequence=8' "$manifest" >"$manifest.tmp" && mv "$manifest.tmp" "$manifest"
if run_collector "$gap" >/dev/null 2>&1; then echo 'sequence gap unexpectedly passed' >&2; exit 1; fi

duplicate=$root/sequence-duplicate
fixture "$duplicate"
manifest=$(find "$duplicate/ring/archive" -name '*.manifest.json' | sort | sed -n '2p')
jq '.recorder_first_sequence=1 | .recorder_last_sequence=1' "$manifest" >"$manifest.tmp" && mv "$manifest.tmp" "$manifest"
if run_collector "$duplicate" >/dev/null 2>&1; then echo 'duplicate sequence unexpectedly passed' >&2; exit 1; fi

instance_change=$root/instance-change
fixture "$instance_change"
manifest=$(find "$instance_change/ring/archive" -name '*.manifest.json' | sort | sed -n '2p')
jq '.recorder_instance_id="223e4567-e89b-42d3-a456-426614174000"' "$manifest" >"$manifest.tmp" && mv "$manifest.tmp" "$manifest"
if run_collector "$instance_change" >/dev/null 2>&1; then echo 'instance change unexpectedly passed' >&2; exit 1; fi

metric_failure=$root/metric-failure
fixture "$metric_failure"
jq '.recorder_metrics.queued=1' "$metric_failure/api-status.json" >"$metric_failure/status.tmp" && mv "$metric_failure/status.tmp" "$metric_failure/api-status.json"
if run_collector "$metric_failure" >/dev/null 2>&1; then echo 'recorder metric failure unexpectedly passed' >&2; exit 1; fi

feed_failure=$root/feed-failure
fixture "$feed_failure"
jq '.feed_status.PolymarketClobMarket.status="error"' "$feed_failure/api-status.json" >"$feed_failure/status.tmp" && mv "$feed_failure/status.tmp" "$feed_failure/api-status.json"
if run_collector "$feed_failure" >/dev/null 2>&1; then echo 'essential feed failure unexpectedly passed' >&2; exit 1; fi

stale_feed=$root/stale-feed
fixture "$stale_feed"
jq '.feed_status.PolymarketRtdsChainlink.updated_at="2026-08-09T16:14:59Z"' "$stale_feed/api-status.json" >"$stale_feed/status.tmp" && mv "$stale_feed/status.tmp" "$stale_feed/api-status.json"
if run_collector "$stale_feed" >/dev/null 2>&1; then echo 'stale essential feed unexpectedly passed' >&2; exit 1; fi

hour_feed_failure=$root/hour-feed-failure
fixture "$hour_feed_failure"
source=$(find "$hour_feed_failure/ring/segments" -name '*.jsonl' | sort | tail -n 1)
jq -nc '{event_type:"feed_error",recorded_ts:"2026-08-09T15:59:30Z",
  payload:{feed:"PolymarketClobMarket",error:"fixture disconnect"},
  recorder_instance_id:"123e4567-e89b-42d3-a456-426614174000",recorder_sequence:61}' >>"$source"
refresh_segment "$source"
jq '.recorder_metrics.last_assigned_sequence=61 | .recorder_metrics.enqueued_total=61 | .recorder_metrics.persisted_total=61' \
  "$hour_feed_failure/api-status.json" >"$hour_feed_failure/status.tmp" && mv "$hour_feed_failure/status.tmp" "$hour_feed_failure/api-status.json"
if run_collector "$hour_feed_failure" >/dev/null 2>&1; then echo 'target-hour feed error unexpectedly passed' >&2; exit 1; fi

hour_health_gap=$root/hour-health-gap
fixture "$hour_health_gap"
source=$(find "$hour_health_gap/ring/segments" -name '*.jsonl' | sort | sed -n '3p')
jq -c 'if .recorder_sequence == 21 then .payload.essential_feed_health.summary="degraded" else . end' \
  "$source" >"$source.tmp" && mv "$source.tmp" "$source"
refresh_segment "$source"
if run_collector "$hour_health_gap" >/dev/null 2>&1; then echo 'unhealthy feed observation unexpectedly passed' >&2; exit 1; fi

late_status_sample=$root/late-status-sample
fixture "$late_status_sample"
early=$(date -u -d '2026-08-09T16:15:00Z' +%s)
sed -i "s/POLYEDGE_PARITY_NOW_EPOCH=.*/POLYEDGE_PARITY_NOW_EPOCH=$early/" "$late_status_sample/parity.env"
touch -d "@$early" "$late_status_sample/ring/status.json"
run_collector "$late_status_sample" >/dev/null
[ "$(jq -r '.acceptedCleanLiveHours' "$late_status_sample/ring/parity/ledger.json")" = 1 ]

success=$root/success
fixture "$success"
jq '.rebootRecoveryPassed = true' "$success/ring/parity/ledger.json" >"$success/reboot.tmp"
chmod 0640 "$success/reboot.tmp"
mv "$success/reboot.tmp" "$success/ring/parity/ledger.json"
before=$(protected "$success/ring/parity/ledger.json")
run_collector "$success" >/dev/null
[ "$(jq -r '.acceptedCleanLiveHours' "$success/ring/parity/ledger.json")" = 1 ]
[ "$(find "$success/ring/segments" -name '*.sequence.*' | wc -l)" -eq 0 ]
jq -e '.acceptedForParityWindow == true and .sameInput.deterministicResultExactMatch == true and (.segments | length) == 6' \
  "$success/ring/parity/hourly/20260809T15/evidence.json" >/dev/null
grep -q -- "--user $uid:$gid" "$success/calls/podman"
grep -q -- "$success/token/azure-federated-token:/run/credentials/azure-federated-token:ro,Z" "$success/calls/podman"
! grep -q -- '--security-opt=no-new-privileges' "$success/calls/podman"
[ "$(protected "$success/ring/parity/ledger.json")" = "$before" ]
runs=$(wc -l <"$success/calls/podman")
run_collector "$success" >/dev/null
[ "$(jq -r '.acceptedCleanLiveHours' "$success/ring/parity/ledger.json")" = 1 ]
[ "$(find "$success/ring/segments" -name '*.sequence.*' | wc -l)" -eq 0 ]
[ "$(wc -l <"$success/calls/podman")" = "$runs" ]

v4_success=$root/v4-success
fixture "$v4_success" "2026-08-09T15:00:00Z" "2026-08-09T14:10:00Z" 4
run_collector "$v4_success" >/dev/null
jq -e '.acceptedCleanLiveHours == 1 and (.acceptedHourlyEvidence | length) == 1' \
  "$v4_success/ring/parity/ledger.json" >/dev/null

legacy_feed_evidence=$root/legacy-feed-evidence
fixture "$legacy_feed_evidence"
run_collector "$legacy_feed_evidence" >/dev/null
evidence=$legacy_feed_evidence/ring/parity/hourly/20260809T15/evidence.json
jq 'del(.continuousFeedHealth)' "$evidence" >"$evidence.tmp" && mv "$evidence.tmp" "$evidence"
chmod 0640 "$evidence"
jq '.acceptedCleanLiveHours=0 | .acceptedHourlyEvidence=[]' "$legacy_feed_evidence/ring/parity/ledger.json" >"$legacy_feed_evidence/ledger.tmp"
chmod 0640 "$legacy_feed_evidence/ledger.tmp"
mv "$legacy_feed_evidence/ledger.tmp" "$legacy_feed_evidence/ring/parity/ledger.json"
if run_collector "$legacy_feed_evidence" >/dev/null 2>&1; then echo 'legacy feed evidence unexpectedly passed' >&2; exit 1; fi

legacy=$root/legacy
fixture "$legacy"
jq 'del(.completedDailyCycles,.acceptedDailyEvidence)' "$legacy/ring/parity/ledger.json" >"$legacy/ledger.tmp"
chmod 0640 "$legacy/ledger.tmp"
mv "$legacy/ledger.tmp" "$legacy/ring/parity/ledger.json"
run_collector "$legacy" >/dev/null
[ "$(jq -r '.acceptedCleanLiveHours' "$legacy/ring/parity/ledger.json")" = 1 ]
jq -e 'has("completedDailyCycles") == false and has("acceptedDailyEvidence") == false' "$legacy/ring/parity/ledger.json" >/dev/null

invalid_parity=$root/invalid-parity
fixture "$invalid_parity"
for file in "$invalid_parity/azure.json" "$invalid_parity/same.json" \
  "$invalid_parity/reports/2026/08/09/15/audit.json"; do
  jq '.result.strategy_batch_replayed=0 | .result.strategy_batch_matches=0 |
    .result.strategy_batch_invalid=1 | .result.decision_pipeline_replay_rate=0 |
    .result.decision_output_binding_rate=null | .result.decision_parity_rate=0' "$file" >"$file.tmp"
  mv "$file.tmp" "$file"
done
if run_collector "$invalid_parity" >/dev/null 2>&1; then
  echo 'invalid decision parity unexpectedly passed' >&2
  exit 1
fi
[ "$(jq -r '.acceptedCleanLiveHours' "$invalid_parity/ring/parity/ledger.json")" = 0 ]

decision_config_mismatch=$root/decision-config-mismatch
fixture "$decision_config_mismatch"
jq '.result.runtime_provenance.identities[0].decision_config_sha256="sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"' \
  "$decision_config_mismatch/reports/2026/08/09/15/audit.json" >"$decision_config_mismatch/oci.tmp"
mv "$decision_config_mismatch/oci.tmp" "$decision_config_mismatch/reports/2026/08/09/15/audit.json"
if run_collector "$decision_config_mismatch" >/dev/null 2>&1; then
  echo 'cross-report decision config mismatch unexpectedly passed' >&2
  exit 1
fi

gapped=$root/gapped
fixture "$gapped"
for file in "$gapped/azure.json" "$gapped/same.json" "$gapped/reports/2026/08/09/15/audit.json"; do
  jq '.result.runtime_provenance.max_gap_ms=600000' "$file" >"$file.tmp"
  mv "$file.tmp" "$file"
done
if run_collector "$gapped" >/dev/null 2>&1; then
  echo 'gapped runtime provenance unexpectedly passed' >&2
  exit 1
fi
[ "$(jq -r '.acceptedCleanLiveHours' "$gapped/ring/parity/ledger.json")" = 0 ]

second=$success/ring/parity/hourly/20260809T16
mkdir -m 0750 "$second"
jq '.generatedAtUtc="2026-08-09T17:19:45Z" | .hourStartUtc="2026-08-09T16:00:00Z" | .hourEndUtc="2026-08-09T17:00:00Z" |
  .recorderSequence.recorder_first_sequence=61 | .recorderSequence.recorder_last_sequence=120 |
  .recorderStatus.sampledAtUtc="2026-08-09T17:20:00Z" |
  (.recorderStatus.feedStatus[]).updated_at="2026-08-09T17:19:59Z" |
  .continuousFeedHealth.firstObservationUtc="2026-08-09T16:00:01Z" |
  .continuousFeedHealth.lastObservationUtc="2026-08-09T16:59:01Z"' \
  "$success/ring/parity/hourly/20260809T15/evidence.json" >"$second/evidence.json"
chmod 0640 "$second/evidence.json"
sed -i 's/POLYEDGE_PARITY_TARGET_HOUR_UTC=.*/POLYEDGE_PARITY_TARGET_HOUR_UTC=2026-08-09T16:00:00Z/' "$success/parity.env"
sed -i "s/POLYEDGE_PARITY_NOW_EPOCH=.*/POLYEDGE_PARITY_NOW_EPOCH=$(date -u -d '2026-08-09T17:20:00Z' +%s)/" "$success/parity.env"
run_collector "$success" >/dev/null
[ "$(jq -r '.acceptedCleanLiveHours' "$success/ring/parity/ledger.json")" = 2 ]
jq -e '(.acceptedHourlyEvidence | length) == 2 and .acceptedHourlyEvidence[1].hourStartUtc == "2026-08-09T16:00:00Z"' \
  "$success/ring/parity/ledger.json" >/dev/null
[ "$(wc -l <"$success/calls/podman")" = "$runs" ]

cross_hour_gap=$root/cross-hour-gap
fixture "$cross_hour_gap"
run_collector "$cross_hour_gap" >/dev/null
mkdir -m 0750 "$cross_hour_gap/ring/parity/hourly/20260809T16"
jq '.generatedAtUtc="2026-08-09T17:19:45Z" | .hourStartUtc="2026-08-09T16:00:00Z" | .hourEndUtc="2026-08-09T17:00:00Z" |
  .recorderSequence.recorder_first_sequence=62 | .recorderSequence.recorder_last_sequence=121 |
  .recorderStatus.sampledAtUtc="2026-08-09T17:20:00Z" |
  (.recorderStatus.feedStatus[]).updated_at="2026-08-09T17:19:59Z" |
  .continuousFeedHealth.firstObservationUtc="2026-08-09T16:00:01Z" |
  .continuousFeedHealth.lastObservationUtc="2026-08-09T16:59:01Z"' \
  "$cross_hour_gap/ring/parity/hourly/20260809T15/evidence.json" >"$cross_hour_gap/ring/parity/hourly/20260809T16/evidence.json"
chmod 0640 "$cross_hour_gap/ring/parity/hourly/20260809T16/evidence.json"
sed -i 's/POLYEDGE_PARITY_TARGET_HOUR_UTC=.*/POLYEDGE_PARITY_TARGET_HOUR_UTC=2026-08-09T16:00:00Z/' "$cross_hour_gap/parity.env"
sed -i "s/POLYEDGE_PARITY_NOW_EPOCH=.*/POLYEDGE_PARITY_NOW_EPOCH=$(date -u -d '2026-08-09T17:20:00Z' +%s)/" "$cross_hour_gap/parity.env"
if run_collector "$cross_hour_gap" >/dev/null 2>&1; then echo 'cross-hour sequence gap unexpectedly passed' >&2; exit 1; fi

superseded=$root/superseded
fixture "$superseded"
historical=$superseded/ring/parity/hourly/20260809T14
mkdir -p -m 0750 "$historical"
jq -n --arg ledger "$superseded/ring/parity/old-ledger.json" '{
  schemaVersion:1,status:"validated",acceptedForParityWindow:true,
  windowStartUtc:"2026-08-09T14:00:00Z",hourStartUtc:"2026-08-09T14:00:00Z",hourEndUtc:"2026-08-09T15:00:00Z",
  ledgerPath:$ledger,azureAuthoritative:true,azureDeletionAllowed:false,
  sameInput:{deterministicResultExactMatch:true}
}' >"$historical/evidence.json"
chmod 0640 "$historical/evidence.json"
run_collector "$superseded" >/dev/null
[ "$(jq -r '.acceptedCleanLiveHours' "$superseded/ring/parity/ledger.json")" = 1 ]

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
grep -F '/usr/bin/flock -w 300 9' "$collector" >/dev/null

echo 'parity hourly collector self-test passed'
