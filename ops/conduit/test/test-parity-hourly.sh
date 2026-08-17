#!/bin/sh
set -eu

root=$(mktemp -d)
trap 'rm -rf "$root"' EXIT HUP INT TERM
collector=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)/bin/polyedge-parity-hourly
uid=$(id -u)
gid=$(id -g)
fake=$root/fake-bin
mkdir -p "$fake"

grep -Fq '(.[0] + .[1]) as $runs |' "$collector"
grep -Fq 'validate_evidence_artifacts() (' "$collector"
printf '%s\n%s\n' \
  '[{"recorder_instance_id":"a","recorder_first_sequence":1,"recorder_last_sequence":1}]' \
  '[{"recorder_instance_id":"a","recorder_first_sequence":2,"recorder_last_sequence":2}]' | jq -se '
  (.[0] + .[1]) as $runs |
  all(range(1; ($runs | length)); . as $index |
    $runs[$index].recorder_first_sequence == ($runs[$index - 1].recorder_last_sequence + 1))
' >/dev/null
if printf '%s\n%s\n' \
    '[{"recorder_instance_id":"a","recorder_first_sequence":1,"recorder_last_sequence":1}]' \
    '[{"recorder_instance_id":"a","recorder_first_sequence":3,"recorder_last_sequence":3}]' | jq -se '
    (.[0] + .[1]) as $runs |
    all(range(1; ($runs | length)); . as $index |
      $runs[$index].recorder_first_sequence == ($runs[$index - 1].recorder_last_sequence + 1))
  ' >/dev/null; then
  echo 'gapped cross-hour recorder sequence unexpectedly passed' >&2
  exit 1
fi

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
  *management.azure.com*) cp "$FAKE_AZURE_EXECUTION" "$out" ;;
  *audit.json.attestation.json*) cp "$FAKE_AZURE_ATTESTATION" "$out" ;;
  *) cp "$FAKE_AZURE_REPORT" "$out" ;;
esac
EOF

cat >"$fake/podman" <<'EOF'
#!/bin/sh
case "$1" in
  inspect)
    case "$*" in
      *State.Status*polyedge-funded-signer*) [ "${FAKE_FUNDED_ACTIVE:-0}" = 1 ] && printf '%s\n' running || exit 2 ;;
      *Config.Image*polyedge-funded-signer*) printf '%s\n' "$FAKE_FUNDED_IMAGE" ;;
      *Config.User*polyedge-funded-signer*) printf '%s\n' "$FAKE_FUNDED_UID:$FAKE_FUNDED_GID" ;;
      *Config.Env*polyedge-funded-signer*) jq -nc \
        --arg session "${FAKE_FUNDED_SESSION:-dynamic-quote-funded-2026-08-13-v10}" \
        --arg manifest "${FAKE_FUNDED_SESSION_SHA:-sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff}" \
        --arg config "${FAKE_FUNDED_CONFIG_SHA:-sha256:9999999999999999999999999999999999999999999999999999999999999999}" \
        '["VENUE_PROBE_FUNDED_CAMPAIGN_ID="+$session,"FUNDED_DIRECT_SESSION_MANIFEST_SHA256="+$manifest,
          "STRATEGY_CANARY_CANDIDATE_CONFIG_HASH="+$config]' ;;
      *State.Health.Status*) printf '%s\n' healthy ;;
      *Config.Image*) printf '%s\n' "$FAKE_OCI_IMAGE" ;;
      *ImageDigest*) printf '%s\n' "$FAKE_OCI_IMAGE_DIGEST" ;;
      *Id*) printf '%s\n' eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee ;;
      *) exit 2 ;;
    esac
    ;;
  exec)
    printf 'HTTP/1.0 200 OK\r\n\r\n'
    attempt_file=$FAKE_CALLS/api-status-attempts
    attempt=0
    [ ! -f "$attempt_file" ] || attempt=$(cat "$attempt_file")
    attempt=$((attempt + 1))
    printf '%s\n' "$attempt" >"$attempt_file"
    if [ "$attempt" -le "${FAKE_API_BUSY_ATTEMPTS:-0}" ]; then
      jq '.recorder_status.error_count=null | .recorder_status.dropped_count=null | .recorder_metrics.queued=1' \
        "$FAKE_API_STATUS"
    else
      cat "$FAKE_API_STATUS"
    fi
    ;;
  run)
    printf '%s\n' "$*" >>"$FAKE_CALLS/podman"
    execution=${POLYEDGE_GENERATOR_EXECUTION_ID:-}
    [ -n "$execution" ] || exit 2
    evidence_dir=
    previous=
    for arg do
      if [ "$previous" = volume ]; then
        case "$arg" in *:/evidence:rw,Z) evidence_dir=${arg%:/evidence:rw,Z} ;; esac
      fi
      previous=
      [ "$arg" != -v ] || previous=volume
    done
    [ -n "$evidence_dir" ] || exit 2
    jq --arg execution "$execution" '.generator_provenance.execution_id=$execution' \
      "$FAKE_SAME_REPORT" >"$FAKE_CALLS/container-audit.json"
    : >"$FAKE_CALLS/container-audit.md"
    cp "$FAKE_CALLS/container-audit.json" "$evidence_dir/audit.json"
    cp "$FAKE_CALLS/container-audit.md" "$evidence_dir/audit.md"
    ;;
  cp)
    shift
    [ "${1:-}" != --archive=false ] || shift
    source=$1 destination=$2
    case "$source" in
      *:/evidence/audit.json) cp "$FAKE_CALLS/container-audit.json" "$destination" ;;
      *:/evidence/audit.md) cp "$FAKE_CALLS/container-audit.md" "$destination" ;;
      *) exit 2 ;;
    esac
    ;;
  rm) printf '%s\n' "$*" >>"$FAKE_CALLS/podman-rm" ;;
  *) exit 2 ;;
esac
EOF

cat >"$fake/systemctl" <<'EOF'
#!/bin/sh
case "$1" in
  is-active)
    unit=${3:-${2:-}}
    case "$unit" in
      polyedge-job@shadow-qset.service) [ "${FAKE_QSET_ACTIVE:-0}" = 1 ] && exit 0 || exit 3 ;;
      polyedge-funded-signer.service|polyedge-federated-token@funded-signer.timer)
        [ "${FAKE_FUNDED_ACTIVE:-0}" = 1 ] && exit 0 || exit 3
        ;;
      *) exit 0 ;;
    esac
    ;;
  is-enabled)
    case "$2" in
      polyedge-shadow-qset.timer) printf '%s\n' not-found; exit 1 ;;
      polyedge-funded-signer.service)
        if [ "${FAKE_FUNDED_ACTIVE:-0}" = 1 ]; then printf '%s\n' "${FAKE_FUNDED_ENABLEMENT:-enabled}"; else printf '%s\n' masked; fi
        ;;
      polyedge-federated-token@funded-signer.timer)
        if [ "${FAKE_FUNDED_ACTIVE:-0}" = 1 ]; then printf '%s\n' enabled; else printf '%s\n' masked; fi
        ;;
      *) exit 2 ;;
    esac
    ;;
  show)
    case "${2:-}" in
      multi-user.target) printf '%s\n' "${FAKE_FUNDED_TARGET_WANTS:-polyedge-funded-signer.service}" ;;
      *) printf '%s\n' aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa ;;
    esac
    ;;
  *) exit 2 ;;
esac
EOF

cat >"$fake/journalctl" <<'EOF'
#!/bin/sh
[ "${FAKE_FUNDED_ACTIVE:-0}" = 1 ] || exit 0
start=$(date -u -d '2026-08-09T15:00:00Z' +%s)
case "$*" in
  *polyedge-federated-token@funded-signer.service*)
    i=0
    while [ "$i" -lt 30 ]; do
      if [ "${FAKE_FUNDED_TOKEN_GAP:-0}" = 1 ] && [ "$i" -eq 15 ]; then i=$((i + 1)); continue; fi
      timestamp=$(( (start + 60 + i * 120) * 1000000 ))
      message='Finished polyedge-federated-token@funded-signer.service - Rotate the funded-signer PolyEdge JWT-SVID token file.'
      jq -nc --arg timestamp "$timestamp" --arg message "$message" \
        '{__REALTIME_TIMESTAMP:$timestamp,MESSAGE:$message}'
      i=$((i + 1))
    done
    [ "${FAKE_FUNDED_TOKEN_FAILURE:-0}" != 1 ] || jq -nc --arg timestamp "$(( (start + 1800) * 1000000 ))" \
      '{__REALTIME_TIMESTAMP:$timestamp,MESSAGE:"Failed to start funded token refresh"}'
    ;;
  *)
    i=0
    while [ "$i" -lt 60 ]; do
      if [ "${FAKE_FUNDED_GAP:-0}" = 1 ] && [ "$i" -eq 30 ]; then i=$((i + 1)); continue; fi
      if [ "${FAKE_FUNDED_BURST:-0}" = 1 ]; then second=$((start + 30 + i)); else second=$((start + 30 + i * 60)); fi
      timestamp=$((second * 1000000))
      invocation=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
      [ "${FAKE_FUNDED_RESTART:-0}" != 1 ] || [ "$i" -ne 30 ] || invocation=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
      message=$(jq -nc '{schema:"polyedge.funded_direct_service.v2",status:"persistent_service_heartbeat",
        processed_messages:1,failed_messages:0,consecutive_latency_breaches:0,redemption_failures:0,
        executor:{user_channel_ready:true,market_channel_ready:true,user_channel_gaps:0,market_channel_gaps:0,
          user_channel_unparsed:0,market_channel_unparsed:0,reconnect_reconciliation_required:false,
          safety_snapshot_cache_ready:true,safety_snapshot_cache_error:null,risk_reservation_index_ready:true}}')
      jq -nc --arg timestamp "$timestamp" --arg invocation "$invocation" \
        --arg container eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee --arg message "$message" \
        '{__REALTIME_TIMESTAMP:$timestamp,_SYSTEMD_INVOCATION_ID:$invocation,CONTAINER_ID_FULL:$container,MESSAGE:$message}'
      i=$((i + 1))
    done
    if [ "${FAKE_FUNDED_ALERT:-0}" = 1 ]; then
      message=$(jq -nc '{schema:"polyedge.funded_direct_alert.v1",status:"websocket_gap_or_reconciliation_required"}')
      jq -nc --arg timestamp "$(( (start + 1800) * 1000000 ))" --arg invocation aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
        --arg container eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee --arg message "$message" \
        '{__REALTIME_TIMESTAMP:$timestamp,_SYSTEMD_INVOCATION_ID:$invocation,CONTAINER_ID_FULL:$container,MESSAGE:$message}'
    fi
    if [ "${FAKE_FUNDED_FAILED_CLOSED:-0}" = 1 ]; then
      message=$(jq -nc '{schema:"polyedge.funded_direct_service.v1",status:"failed_closed"}')
      jq -nc --arg timestamp "$(( (start + 1800) * 1000000 ))" --arg invocation aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
        --arg container eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee --arg message "$message" \
        '{__REALTIME_TIMESTAMP:$timestamp,_SYSTEMD_INVOCATION_ID:$invocation,CONTAINER_ID_FULL:$container,MESSAGE:$message}'
    fi
    ;;
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

azure_image=crpolyedge6urdjr5nmwx7w.azurecr.io/polyedge-rust-research@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
oci_image=ghcr.io/aldoapicella/polyedge-rust-backend@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
oci_image_digest=sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc
azure_execution=polyedge-hourly-quality-job-fixture
azure_args='TARGET=$(date -u -d "1 hour ago" +%Y/%m/%d/%H); DAY=${TARGET%/*}; HOUR=${TARGET##*/}; OUT="reports/research/hourly/$DAY/$HOUR/audit.json"; /bin/sh /app/research/run_compact_report_job.sh polyedge_hourly_quality "$OUT" polyedge-rs research audit --input "azure://$AZURE_STORAGE_ACCOUNT_NAME/$AZURE_STORAGE_CONTAINER_NAME/events/$DAY/$HOUR/?prefetch_blobs=8" --out "$OUT" --markdown "reports/research/hourly/$DAY/$HOUR/audit.md" --exclude-file "data_quality/exclusion_windows.yaml"'

report() {
  kind=$1 output=$2
  marker=azure
  platform=oci_podman
  image=$oci_image
  execution=$kind-fixture
  job=null
  if [ "$kind" = azure ]; then
    platform=azure_container_apps_job
    image=$azure_image
    execution=$azure_execution
    job=polyedge-hourly-quality-job
  elif [ "$kind" = mismatch ]; then
    marker=mismatch
  fi
  jq -n --arg marker "$marker" --arg platform "$platform" --arg image "$image" \
    --arg execution "$execution" --argjson job "$(printf '%s' "$job" | jq -R 'if . == "null" then null else . end')" '{
    git_sha:"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    generated_at:"2026-08-09T16:13:00Z",
    input_path:"azure://stpolyedge6urdjr5nmwx7w/bot-events/events/2026/08/09/15/?prefetch_blobs=8",
    generator_provenance:{schema_version:1,platform:$platform,image:$image,execution_id:$execution,job_name:$job},
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

attest() {
  report_file=$1 output=$2
  jq --arg report_sha256 "sha256:$(sha256sum "$report_file" | awk '{print $1}')" \
    --arg image_digest "$oci_image_digest" \
    '.generator_provenance + {report_sha256:$report_sha256} +
      (if .generator_provenance.platform == "oci_podman" then
        {image_digest:$image_digest,container_id:"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"}
       else {} end)' "$report_file" >"$output"
  chmod 0640 "$output"
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
  : >"$case_root/token/funded-azure-federated-token"
  chmod 0600 "$case_root/token/azure-federated-token"
  chmod 0600 "$case_root/token/funded-azure-federated-token"
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
  touch -d "@$fixture_now" "$case_root/token/funded-azure-federated-token"
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
POLYEDGE_PARITY_EXPECTED_GIT_SHA=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
POLYEDGE_PARITY_EXPECTED_AZURE_RESEARCH_IMAGE=$azure_image
POLYEDGE_PARITY_EXPECTED_OCI_RESEARCH_IMAGE=$oci_image
AZURE_SUBSCRIPTION_ID=11111111-1111-1111-1111-111111111111
POLYEDGE_PARITY_AZURE_RESOURCE_GROUP=rg-polyedge-dev
POLYEDGE_PARITY_AZURE_JOB_NAME=polyedge-hourly-quality-job
EOF
  chmod 0640 "$case_root/parity.env"
  report azure "$case_root/azure.json"
  attest "$case_root/azure.json" "$case_root/azure.json.attestation.json"
  report oci "$case_root/reports/2026/08/09/15/audit.json"
  jq '.input_path="/input/events/2026/08/09/15/"' "$case_root/reports/2026/08/09/15/audit.json" >"$case_root/oci.tmp"
  mv "$case_root/oci.tmp" "$case_root/reports/2026/08/09/15/audit.json"
  attest "$case_root/reports/2026/08/09/15/audit.json" "$case_root/reports/2026/08/09/15/audit.json.attestation.json"
  report same "$case_root/same.json"
  jq -n --arg execution "$azure_execution" --arg image "$azure_image" --arg args "$azure_args" '{
    name:$execution,properties:{status:"Succeeded",startTime:"2026-08-09T16:10:00+00:00",endTime:"2026-08-09T16:14:00+00:00",
      template:{containers:[{name:"research-job",image:$image,command:["/bin/sh","-lc"],args:[$args],env:[
        {name:"ALLOW_LIVE",value:"false"},{name:"EXECUTION_MODE",value:"paper"},
        {name:"RUN_BOT_ON_STARTUP",value:"false"},{name:"ENABLE_TAKER_ORDERS",value:"false"},
        {name:"ALLOW_EMERGENCY_ACCOUNT_CANCEL",value:"false"},{name:"REQUIRE_API_AUTH",value:"true"},
        {name:"API_BEARER_TOKEN",secretRef:"cappjob-polyedge-hourly-quality-job"},
        {name:"POLYEDGE_GENERATOR_PLATFORM",value:"azure_container_apps_job"},
        {name:"POLYEDGE_GENERATOR_IMAGE",value:$image}
      ]}]}}
  }' >"$case_root/azure-execution.json"
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

activate_funded_fixture() {
  case_root=$1
  active_ledger=$case_root/ring/parity/20260809T141000Z-funded-active.json
  jq --arg user "$uid:$gid" '.fundedSignerEnabled=true | .fundedSignerMode="active" |
    .fundedSignerImage="ghcr.io/aldoapicella/polyedge-venue-probe@sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd" |
    .fundedSignerUser=$user | .fundedSessionId="dynamic-quote-funded-2026-08-13-v10" |
    .fundedSessionManifestSha256="sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff" |
    .fundedConfigSha256="sha256:9999999999999999999999999999999999999999999999999999999999999999"' \
    "$case_root/ring/parity/ledger.json" >"$active_ledger"
  rm "$case_root/ring/parity/ledger.json"
  chmod 0640 "$active_ledger"
  sed -i "s#POLYEDGE_PARITY_LEDGER=.*#POLYEDGE_PARITY_LEDGER=$active_ledger#" "$case_root/parity.env"
  cat >>"$case_root/parity.env" <<EOF
POLYEDGE_PARITY_FUNDED_MODE=active
POLYEDGE_PARITY_EXPECTED_FUNDED_IMAGE=ghcr.io/aldoapicella/polyedge-venue-probe@sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd
POLYEDGE_PARITY_FUNDED_UID=$uid
POLYEDGE_PARITY_FUNDED_GID=$gid
POLYEDGE_PARITY_EXPECTED_FUNDED_SESSION_ID=dynamic-quote-funded-2026-08-13-v10
POLYEDGE_PARITY_EXPECTED_FUNDED_SESSION_SHA256=sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
POLYEDGE_PARITY_EXPECTED_FUNDED_CONFIG_SHA256=sha256:9999999999999999999999999999999999999999999999999999999999999999
POLYEDGE_PARITY_FUNDED_TOKEN_FILE=$case_root/token/funded-azure-federated-token
EOF
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
    POLYEDGE_PARITY_UTILITY_LOCKED=1 \
    POLYEDGE_PARITY_EXPECTED_UID="$uid" POLYEDGE_PARITY_EXPECTED_GID="$gid" \
    POLYEDGE_PARITY_ENV_FILE="$case_root/parity.env" \
    FAKE_CALLS="$case_root/calls" FAKE_DF_AVAILABLE="${FAKE_DF_AVAILABLE:-20000000000}" FAKE_MOUNTPOINT_OK="${FAKE_MOUNTPOINT_OK:-1}" \
    FAKE_AZURE_REPORT="$case_root/azure.json" FAKE_AZURE_ATTESTATION="$case_root/azure.json.attestation.json" \
    FAKE_AZURE_EXECUTION="$case_root/azure-execution.json" FAKE_SAME_REPORT="$case_root/same.json" \
    FAKE_OCI_IMAGE="$oci_image" FAKE_OCI_IMAGE_DIGEST="$oci_image_digest" FAKE_API_STATUS="$case_root/api-status.json" \
    FAKE_FUNDED_ACTIVE="${FAKE_FUNDED_ACTIVE:-0}" FAKE_QSET_ACTIVE="${FAKE_QSET_ACTIVE:-0}" \
    FAKE_FUNDED_ENABLEMENT="${FAKE_FUNDED_ENABLEMENT:-enabled}" \
    FAKE_FUNDED_TARGET_WANTS="${FAKE_FUNDED_TARGET_WANTS:-polyedge-funded-signer.service}" \
    FAKE_FUNDED_ALERT="${FAKE_FUNDED_ALERT:-0}" FAKE_FUNDED_FAILED_CLOSED="${FAKE_FUNDED_FAILED_CLOSED:-0}" \
    FAKE_FUNDED_BURST="${FAKE_FUNDED_BURST:-0}" FAKE_FUNDED_GAP="${FAKE_FUNDED_GAP:-0}" \
    FAKE_FUNDED_RESTART="${FAKE_FUNDED_RESTART:-0}" FAKE_FUNDED_TOKEN_GAP="${FAKE_FUNDED_TOKEN_GAP:-0}" \
    FAKE_FUNDED_TOKEN_FAILURE="${FAKE_FUNDED_TOKEN_FAILURE:-0}" \
    FAKE_FUNDED_IMAGE="${FAKE_FUNDED_IMAGE:-ghcr.io/aldoapicella/polyedge-venue-probe@sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd}" \
    FAKE_FUNDED_UID="${FAKE_FUNDED_UID:-$uid}" FAKE_FUNDED_GID="${FAKE_FUNDED_GID:-$gid}" \
    FAKE_FUNDED_SESSION="${FAKE_FUNDED_SESSION:-dynamic-quote-funded-2026-08-13-v10}" \
    FAKE_FUNDED_SESSION_SHA="${FAKE_FUNDED_SESSION_SHA:-sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff}" \
    FAKE_FUNDED_CONFIG_SHA="${FAKE_FUNDED_CONFIG_SHA:-sha256:9999999999999999999999999999999999999999999999999999999999999999}" \
    FAKE_API_BUSY_ATTEMPTS="${FAKE_API_BUSY_ATTEMPTS:-0}" \
    "$collector"
}

protected() {
  jq -cS '{status,azureAuthoritative,azureDeletionAllowed,rebootRecoveryPassed,shadowQsetEnabled,fundedSignerEnabled,
    fundedSignerMode,fundedSignerImage,fundedSignerUser,fundedSessionId,fundedSessionManifestSha256,fundedConfigSha256}' "$1"
}
seed_artifacts() {
  case_root=$1
  which=$2
  artifact_dir=$case_root/ring/parity/hourly/20260809T15
  mkdir -p "$artifact_dir"
  chmod 0750 "$case_root/ring/parity/hourly" "$artifact_dir"
  case "$which" in
    azure|both)
      cp "$case_root/azure.json" "$artifact_dir/azure-scheduled-audit.json"
      cp "$case_root/azure.json.attestation.json" "$artifact_dir/azure-scheduled-audit.json.attestation.json"
      chmod 0640 "$artifact_dir/azure-scheduled-audit.json" "$artifact_dir/azure-scheduled-audit.json.attestation.json"
      ;;
  esac
  case "$which" in
    same|both)
      cp "$case_root/same.json" "$artifact_dir/same-input-audit.json"
      attest "$artifact_dir/same-input-audit.json" "$artifact_dir/same-input-audit.json.attestation.json"
      chmod 0640 "$artifact_dir/same-input-audit.json" "$artifact_dir/same-input-audit.json.attestation.json"
      ;;
  esac
}

image_mismatch=$root/image-mismatch
fixture "$image_mismatch"
jq --arg image "$oci_image" '.properties.template.containers[0].image=$image' \
  "$image_mismatch/azure-execution.json" >"$image_mismatch/execution.tmp"
mv "$image_mismatch/execution.tmp" "$image_mismatch/azure-execution.json"
if run_collector "$image_mismatch" >/dev/null 2>&1; then
  echo 'image mismatch unexpectedly passed' >&2
  exit 1
fi

wrong_execution_time=$root/wrong-execution-time
fixture "$wrong_execution_time"
seed_artifacts "$wrong_execution_time" both
jq '.properties.startTime="2026-08-09T15:59:59Z"' "$wrong_execution_time/azure-execution.json" >"$wrong_execution_time/execution.tmp"
mv "$wrong_execution_time/execution.tmp" "$wrong_execution_time/azure-execution.json"
if run_collector "$wrong_execution_time" >/dev/null 2>&1; then
  echo 'wrong Azure execution time unexpectedly passed' >&2
  exit 1
fi

wrong_execution_command=$root/wrong-execution-command
fixture "$wrong_execution_command"
seed_artifacts "$wrong_execution_command" both
jq '.properties.template.containers[0].args=["polyedge-rs research audit --wrong-input"]' \
  "$wrong_execution_command/azure-execution.json" >"$wrong_execution_command/execution.tmp"
mv "$wrong_execution_command/execution.tmp" "$wrong_execution_command/azure-execution.json"
if run_collector "$wrong_execution_command" >/dev/null 2>&1; then
  echo 'wrong Azure execution command unexpectedly passed' >&2
  exit 1
fi

wrong_execution_env=$root/wrong-execution-env
fixture "$wrong_execution_env"
seed_artifacts "$wrong_execution_env" both
jq '(.properties.template.containers[0].env[] | select(.name=="ALLOW_LIVE").value)="true"' \
  "$wrong_execution_env/azure-execution.json" >"$wrong_execution_env/execution.tmp"
mv "$wrong_execution_env/execution.tmp" "$wrong_execution_env/azure-execution.json"
if run_collector "$wrong_execution_env" >/dev/null 2>&1; then
  echo 'wrong Azure execution safety environment unexpectedly passed' >&2
  exit 1
fi

unknown_secret_ref=$root/unknown-secret-ref
fixture "$unknown_secret_ref"
seed_artifacts "$unknown_secret_ref" both
jq '(.properties.template.containers[0].env[] | select(.name=="API_BEARER_TOKEN").secretRef)="untrusted-secret"' \
  "$unknown_secret_ref/azure-execution.json" >"$unknown_secret_ref/execution.tmp"
mv "$unknown_secret_ref/execution.tmp" "$unknown_secret_ref/azure-execution.json"
if run_collector "$unknown_secret_ref" >/dev/null 2>&1; then
  echo 'unknown Azure execution bearer secret reference unexpectedly passed' >&2
  exit 1
fi

builtin_execution_override=$root/builtin-execution-override
fixture "$builtin_execution_override"
seed_artifacts "$builtin_execution_override" both
jq '.properties.template.containers[0].env += [{name:"CONTAINER_APP_JOB_EXECUTION_NAME",value:"forged"}]' \
  "$builtin_execution_override/azure-execution.json" >"$builtin_execution_override/execution.tmp"
mv "$builtin_execution_override/execution.tmp" "$builtin_execution_override/azure-execution.json"
if run_collector "$builtin_execution_override" >/dev/null 2>&1; then
  echo 'explicit Container Apps execution override unexpectedly passed' >&2
  exit 1
fi

wrong_execution=$root/wrong-execution
fixture "$wrong_execution"
jq '.generator_provenance.execution_id="polyedge-hourly-quality-job-wrong"' \
  "$wrong_execution/azure.json" >"$wrong_execution/azure.tmp"
mv "$wrong_execution/azure.tmp" "$wrong_execution/azure.json"
attest "$wrong_execution/azure.json" "$wrong_execution/azure.json.attestation.json"
if run_collector "$wrong_execution" >/dev/null 2>&1; then
  echo 'wrong Azure execution unexpectedly passed' >&2
  exit 1
fi

report_hash_mismatch=$root/report-hash-mismatch
fixture "$report_hash_mismatch"
jq '.report_sha256="sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"' \
  "$report_hash_mismatch/reports/2026/08/09/15/audit.json.attestation.json" >"$report_hash_mismatch/attestation.tmp"
mv "$report_hash_mismatch/attestation.tmp" "$report_hash_mismatch/reports/2026/08/09/15/audit.json.attestation.json"
chmod 0640 "$report_hash_mismatch/reports/2026/08/09/15/audit.json.attestation.json"
if run_collector "$report_hash_mismatch" >/dev/null 2>&1; then
  echo 'report hash mismatch unexpectedly passed' >&2
  exit 1
fi

missing_attestation=$root/missing-attestation
fixture "$missing_attestation"
rm "$missing_attestation/reports/2026/08/09/15/audit.json.attestation.json"
if run_collector "$missing_attestation" >/dev/null 2>&1; then
  echo 'missing attestation unexpectedly passed' >&2
  exit 1
fi

stale_attestation=$root/stale-attestation
fixture "$stale_attestation"
seed_artifacts "$stale_attestation" same
cp "$stale_attestation/reports/2026/08/09/15/audit.json.attestation.json" \
  "$stale_attestation/ring/parity/hourly/20260809T15/same-input-audit.json.attestation.json"
if run_collector "$stale_attestation" >/dev/null 2>&1; then
  echo 'stale copied attestation unexpectedly passed' >&2
  exit 1
fi

git_mismatch=$root/git-mismatch
fixture "$git_mismatch"
sed -i 's/^POLYEDGE_PARITY_EXPECTED_GIT_SHA=.*/POLYEDGE_PARITY_EXPECTED_GIT_SHA=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb/' "$git_mismatch/parity.env"
if run_collector "$git_mismatch" >/dev/null 2>&1; then
  echo 'git mismatch unexpectedly passed' >&2
  exit 1
fi

early_runtime=$root/early-runtime
fixture "$early_runtime"
for file in "$early_runtime/azure.json" "$early_runtime/same.json" \
  "$early_runtime/reports/2026/08/09/15/audit.json"; do
  jq '.result.runtime_provenance.first_timestamp="2026-08-09T14:59:59Z"' "$file" >"$file.tmp"
  mv "$file.tmp" "$file"
done
if run_collector "$early_runtime" >/dev/null 2>&1; then
  echo 'early runtime provenance unexpectedly passed' >&2
  exit 1
fi

late_runtime=$root/late-runtime
fixture "$late_runtime"
for file in "$late_runtime/azure.json" "$late_runtime/same.json" \
  "$late_runtime/reports/2026/08/09/15/audit.json"; do
  jq '.result.runtime_provenance.last_timestamp="2026-08-09T16:00:00Z"' "$file" >"$file.tmp"
  mv "$file.tmp" "$file"
done
if run_collector "$late_runtime" >/dev/null 2>&1; then
  echo 'late runtime provenance unexpectedly passed' >&2
  exit 1
fi

copied_source=$root/copied-source
fixture "$copied_source"
run_collector "$copied_source" >/dev/null
copied_evidence=$root/copied-evidence
fixture "$copied_evidence"
mkdir -p -m 0750 "$copied_evidence/ring/parity/hourly/20260809T15"
cp "$copied_source/ring/parity/hourly/20260809T15/evidence.json" "$copied_evidence/ring/parity/hourly/20260809T15/evidence.json"
jq --arg ledger "$copied_evidence/ring/parity/ledger.json" '.ledgerPath=$ledger' \
  "$copied_evidence/ring/parity/hourly/20260809T15/evidence.json" >"$copied_evidence/evidence.tmp"
mv "$copied_evidence/evidence.tmp" "$copied_evidence/ring/parity/hourly/20260809T15/evidence.json"
chmod 0640 "$copied_evidence/ring/parity/hourly/20260809T15/evidence.json"
if run_collector "$copied_evidence" >/dev/null 2>&1; then
  echo 'copied evidence unexpectedly credited' >&2
  exit 1
fi

hourly_pin_override=$root/hourly-pin-override
fixture "$hourly_pin_override"
printf '%s\n' 'POLYEDGE_PARITY_EXPECTED_GIT_SHA=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb' >>"$hourly_pin_override/hourly.env"
if run_collector "$hourly_pin_override" >/dev/null 2>&1; then
  echo 'hourly pin override unexpectedly passed' >&2
  exit 1
fi

formal_identity_override=$root/formal-identity-override
fixture "$formal_identity_override"
printf '%s\n' 'AZURE_SUBSCRIPTION_ID=22222222-2222-2222-2222-222222222222' >>"$formal_identity_override/hourly.env"
if run_collector "$formal_identity_override" >/dev/null 2>&1; then
  echo 'hourly formal identity override unexpectedly passed' >&2
  exit 1
fi

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

transient_recorder_busy=$root/transient-recorder-busy
fixture "$transient_recorder_busy"
FAKE_API_BUSY_ATTEMPTS=3 run_collector "$transient_recorder_busy" >/dev/null
[ "$(cat "$transient_recorder_busy/calls/api-status-attempts")" -eq 4 ]

feed_failure=$root/feed-failure
fixture "$feed_failure"
jq '.feed_status.PolymarketClobMarket.status="error"' "$feed_failure/api-status.json" >"$feed_failure/status.tmp" && mv "$feed_failure/status.tmp" "$feed_failure/api-status.json"
if run_collector "$feed_failure" >/dev/null 2>&1; then
  echo 'essential feed failure unexpectedly passed' >&2
  exit 1
else
  [ "$?" -eq 1 ]
fi

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
if run_collector "$hour_feed_failure" >/dev/null 2>&1; then
  echo 'target-hour feed error unexpectedly passed' >&2
  exit 1
else
  [ "$?" -eq 78 ]
fi

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

unexpected_active_ledger=$root/unexpected-active-ledger
fixture "$unexpected_active_ledger"
jq '.fundedSignerEnabled=true' "$unexpected_active_ledger/ring/parity/ledger.json" >"$unexpected_active_ledger/ledger.tmp"
mv "$unexpected_active_ledger/ledger.tmp" "$unexpected_active_ledger/ring/parity/ledger.json"
chmod 0640 "$unexpected_active_ledger/ring/parity/ledger.json"
if run_collector "$unexpected_active_ledger" >/dev/null 2>&1; then
  echo 'active funded ledger unexpectedly passed in the default masked mode' >&2
  exit 1
fi

active_funded=$root/active-funded
fixture "$active_funded"
activate_funded_fixture "$active_funded"
FAKE_FUNDED_ACTIVE=1 run_collector "$active_funded" >/dev/null
jq -e '.acceptedCleanLiveHours == 1 and .fundedSignerEnabled == true and
  (.acceptedHourlyEvidence | length) == 1' "$active_funded/ring/parity/20260809T141000Z-funded-active.json" >/dev/null
jq -e '.services.fundedSignerMode == "active" and .services.fundedSignerEnabled == true and
  .services.fundedSignerActive == true and .services.fundedSignerMasked == false and
  .services.fundedSignerUser == "'"$uid:$gid"'" and .services.fundedSessionId == "dynamic-quote-funded-2026-08-13-v10" and
  .services.fundedRuntime.heartbeatCount == 60 and .services.fundedRuntime.alertCount == 0 and
  .services.fundedRuntime.maxHeartbeatGapSeconds == 60 and .services.fundedRuntime.tokenRefreshCount == 30 and
  .services.fundedRuntime.maxTokenRefreshGapSeconds == 120' \
  "$active_funded/ring/parity/hourly/20260809T15/evidence.json" >/dev/null

generated_funded=$root/generated-funded
fixture "$generated_funded"
activate_funded_fixture "$generated_funded"
FAKE_FUNDED_ACTIVE=1 FAKE_FUNDED_ENABLEMENT=generated run_collector "$generated_funded" >/dev/null

generated_not_wanted=$root/generated-not-wanted
fixture "$generated_not_wanted"
activate_funded_fixture "$generated_not_wanted"
if FAKE_FUNDED_ACTIVE=1 FAKE_FUNDED_ENABLEMENT=generated FAKE_FUNDED_TARGET_WANTS=polyedge-api.service \
  run_collector "$generated_not_wanted" >/dev/null 2>&1; then
  echo 'generated funded signer without a reboot-target dependency unexpectedly passed' >&2
  exit 1
fi

reused_masked_ledger=$root/reused-masked-ledger
fixture "$reused_masked_ledger"
activate_funded_fixture "$reused_masked_ledger"
mv "$reused_masked_ledger/ring/parity/20260809T141000Z-funded-active.json" "$reused_masked_ledger/ring/parity/ledger.json"
sed -i "s#POLYEDGE_PARITY_LEDGER=.*#POLYEDGE_PARITY_LEDGER=$reused_masked_ledger/ring/parity/ledger.json#" "$reused_masked_ledger/parity.env"
if FAKE_FUNDED_ACTIVE=1 run_collector "$reused_masked_ledger" >/dev/null 2>&1; then
  echo 'reused masked ledger path unexpectedly passed active funded parity' >&2
  exit 1
fi

inactive_funded=$root/inactive-funded
fixture "$inactive_funded"
activate_funded_fixture "$inactive_funded"
if run_collector "$inactive_funded" >/dev/null 2>&1; then
  echo 'inactive funded signer unexpectedly passed an active parity window' >&2
  exit 1
fi

active_qset=$root/active-qset
fixture "$active_qset"
activate_funded_fixture "$active_qset"
if FAKE_FUNDED_ACTIVE=1 FAKE_QSET_ACTIVE=1 run_collector "$active_qset" >/dev/null 2>&1; then
  echo 'active qset unexpectedly passed the funded parity window' >&2
  exit 1
fi

wrong_funded_image=$root/wrong-funded-image
fixture "$wrong_funded_image"
activate_funded_fixture "$wrong_funded_image"
if FAKE_FUNDED_ACTIVE=1 FAKE_FUNDED_IMAGE=ghcr.io/aldoapicella/polyedge-venue-probe@sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee \
    run_collector "$wrong_funded_image" >/dev/null 2>&1; then
  echo 'wrong funded image unexpectedly passed the funded parity window' >&2
  exit 1
fi

stale_funded_token=$root/stale-funded-token
fixture "$stale_funded_token"
activate_funded_fixture "$stale_funded_token"
touch -d "@$((fixture_now - 241))" "$stale_funded_token/token/funded-azure-federated-token"
if FAKE_FUNDED_ACTIVE=1 run_collector "$stale_funded_token" >/dev/null 2>&1; then
  echo 'stale funded token unexpectedly passed the funded parity window' >&2
  exit 1
fi

funded_alert=$root/funded-alert
fixture "$funded_alert"
activate_funded_fixture "$funded_alert"
if FAKE_FUNDED_ACTIVE=1 FAKE_FUNDED_ALERT=1 run_collector "$funded_alert" >/dev/null 2>&1; then
  echo 'funded alert unexpectedly passed the funded parity window' >&2
  exit 1
fi

for funded_case in burst gap restart failed-closed token-gap token-failure wrong-user wrong-config; do
  case_root=$root/funded-$funded_case
  fixture "$case_root"
  activate_funded_fixture "$case_root"
  case "$funded_case" in
    burst) if FAKE_FUNDED_ACTIVE=1 FAKE_FUNDED_BURST=1 run_collector "$case_root" >/dev/null 2>&1; then passed=1; else passed=0; fi ;;
    gap) if FAKE_FUNDED_ACTIVE=1 FAKE_FUNDED_GAP=1 run_collector "$case_root" >/dev/null 2>&1; then passed=1; else passed=0; fi ;;
    restart) if FAKE_FUNDED_ACTIVE=1 FAKE_FUNDED_RESTART=1 run_collector "$case_root" >/dev/null 2>&1; then passed=1; else passed=0; fi ;;
    failed-closed) if FAKE_FUNDED_ACTIVE=1 FAKE_FUNDED_FAILED_CLOSED=1 run_collector "$case_root" >/dev/null 2>&1; then passed=1; else passed=0; fi ;;
    token-gap) if FAKE_FUNDED_ACTIVE=1 FAKE_FUNDED_TOKEN_GAP=1 run_collector "$case_root" >/dev/null 2>&1; then passed=1; else passed=0; fi ;;
    token-failure) if FAKE_FUNDED_ACTIVE=1 FAKE_FUNDED_TOKEN_FAILURE=1 run_collector "$case_root" >/dev/null 2>&1; then passed=1; else passed=0; fi ;;
    wrong-user) if FAKE_FUNDED_ACTIVE=1 FAKE_FUNDED_UID=999 run_collector "$case_root" >/dev/null 2>&1; then passed=1; else passed=0; fi ;;
    wrong-config) if FAKE_FUNDED_ACTIVE=1 FAKE_FUNDED_CONFIG_SHA=sha256:8888888888888888888888888888888888888888888888888888888888888888 run_collector "$case_root" >/dev/null 2>&1; then passed=1; else passed=0; fi ;;
  esac
  if [ "$passed" -eq 1 ]; then
    echo "invalid funded runtime unexpectedly passed: $funded_case" >&2
    exit 1
  fi
done

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
grep -q -- ':/evidence:rw,Z' "$success/calls/podman"
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
mkdir -p -m 0750 "$second" "$success/ring/segments/2026/08/09/16" "$success/ring/archive/2026/08/09/16" "$success/reports/2026/08/09/16"
for file in "$success"/ring/segments/2026/08/09/15/*.jsonl; do cp "$file" "$success/ring/segments/2026/08/09/16/"; done
for file in "$success"/ring/archive/2026/08/09/15/*; do cp "$file" "$success/ring/archive/2026/08/09/16/"; done
cp "$success/reports/2026/08/09/15/audit.json" "$success/reports/2026/08/09/16/audit.json"
cp "$success/reports/2026/08/09/15/audit.json.attestation.json" "$success/reports/2026/08/09/16/audit.json.attestation.json"
cp "$success/ring/parity/hourly/20260809T15/azure-scheduled-audit.json" "$second/azure-scheduled-audit.json"
cp "$success/ring/parity/hourly/20260809T15/azure-scheduled-audit.json.attestation.json" "$second/azure-scheduled-audit.json.attestation.json"
cp "$success/ring/parity/hourly/20260809T15/same-input-audit.json" "$second/same-input-audit.json"
cp "$success/ring/parity/hourly/20260809T15/same-input-audit.json.attestation.json" "$second/same-input-audit.json.attestation.json"
chmod 0640 "$success/reports/2026/08/09/16/audit.json" "$success/reports/2026/08/09/16/audit.json.attestation.json" \
  "$second/azure-scheduled-audit.json" "$second/azure-scheduled-audit.json.attestation.json" \
  "$second/same-input-audit.json" "$second/same-input-audit.json.attestation.json"
jq '.generatedAtUtc="2026-08-09T17:19:45Z" | .hourStartUtc="2026-08-09T16:00:00Z" | .hourEndUtc="2026-08-09T17:00:00Z" |
  (.scheduledAudits.azure.path |= sub("20260809T15"; "20260809T16")) |
  (.scheduledAudits.azure.attestation.path |= sub("20260809T15"; "20260809T16")) |
  (.scheduledAudits.oci.path |= sub("/15/"; "/16/")) |
  (.scheduledAudits.oci.attestation.path |= sub("/15/"; "/16/")) |
  (.sameInput.path |= sub("20260809T15"; "20260809T16")) |
  (.sameInput.attestation.path |= sub("20260809T15"; "20260809T16")) |
  (.segments[].source.path |= sub("/15/"; "/16/")) |
  (.segments[].gzip.path |= sub("/15/"; "/16/")) |
  (.segments[].manifest.path |= sub("/15/"; "/16/")) |
  (.segments[].receipt.path |= sub("/15/"; "/16/")) |
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
[ "$(wc -l <"$recovery/calls/curl")" = 2 ] && [ ! -e "$recovery/calls/podman" ]

azure_only=$root/azure-only
fixture "$azure_only"
seed_artifacts "$azure_only" azure
run_collector "$azure_only" >/dev/null
[ "$(jq -r '.acceptedCleanLiveHours' "$azure_only/ring/parity/ledger.json")" = 1 ]
[ "$(wc -l <"$azure_only/calls/curl")" = 2 ] && [ "$(wc -l <"$azure_only/calls/podman")" = 1 ]

same_only=$root/same-only
fixture "$same_only"
seed_artifacts "$same_only" same
run_collector "$same_only" >/dev/null
[ "$(jq -r '.acceptedCleanLiveHours' "$same_only/ring/parity/ledger.json")" = 1 ]
[ "$(wc -l <"$same_only/calls/curl")" = 5 ] && [ ! -e "$same_only/calls/podman" ]

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

launcher_root=$root/run-job
mkdir -p "$launcher_root/bin" "$launcher_root/etc/jobs" "$launcher_root/run/polyedge-federated-research" "$launcher_root/ring"
: >"$launcher_root/etc/jobs/hourly.env"
: >"$launcher_root/run/polyedge-federated-research/azure-federated-token"
sed -e "s#/etc/polyedge/jobs/#$launcher_root/etc/jobs/#g" \
  -e "s#/srv/polyedge-ring#$launcher_root/ring#g" \
  -e "s#/run/polyedge-federated-#$launcher_root/run/polyedge-federated-#g" \
  -e "s#/run/polyedge/utility.lock#$launcher_root/run/utility.lock#g" \
  -e "s#/etc/polyedge/credentials/#$launcher_root/etc/credentials/#g" \
  -e "s#/usr/bin/timeout#$launcher_root/bin/timeout#g" \
  -e "s#/usr/bin/podman#$launcher_root/bin/podman#g" \
  -e "s/chown 0:0/chown $uid:$gid/" \
  -e "s/'0:0:640:1'/'$uid:$gid:640:1'/" \
  "$(dirname "$collector")/polyedge-run-job" >"$launcher_root/run-job"
chmod 0755 "$launcher_root/run-job"
cat >"$launcher_root/bin/timeout" <<'EOF'
#!/bin/sh
[ "$1" != --preserve-status ] || shift
shift
exec "$@"
EOF
cat >"$launcher_root/bin/podman" <<'EOF'
#!/bin/sh
case "$1" in
  run)
    printf '%s\n' "$*" >>"$LAUNCH_CALLS/run"
    report="$LAUNCH_RING/jobs/research/reports/research/hourly/$POLYEDGE_AUDIT_DAY/$POLYEDGE_AUDIT_HOUR/audit.json"
    mkdir -p "${report%/*}"
    execution=$POLYEDGE_GENERATOR_EXECUTION_ID
    [ "${LAUNCH_MODE:-}" != provenance ] || execution=forged-execution
    jq -n --arg image "$POLYEDGE_GENERATOR_IMAGE" --arg execution "$execution" '{
      generator_provenance:{schema_version:1,platform:"oci_podman",image:$image,execution_id:$execution,job_name:null}
    }' >"$report"
    ;;
  inspect)
    case "$*" in
      *Config.Image*) printf '%s\n' "$POLYEDGE_RESEARCH_IMAGE" ;;
      *ImageDigest*)
        if [ "${LAUNCH_MODE:-}" = inspect ]; then printf '%s\n' invalid; else printf '%s\n' "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"; fi
        ;;
      *Id*) printf '%s\n' eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee ;;
      *) exit 2 ;;
    esac
    ;;
  rm) printf '%s\n' "$*" >>"$LAUNCH_CALLS/rm" ;;
  *) exit 2 ;;
esac
EOF
cat >"$launcher_root/bin/sha256sum" <<'EOF'
#!/bin/sh
if [ "${LAUNCH_MODE:-}" = hash ] && [ "${1##*/}" = audit.json ]; then
  count=0
  [ ! -f "$LAUNCH_CALLS/hash-count" ] || count=$(cat "$LAUNCH_CALLS/hash-count")
  count=$((count + 1))
  printf '%s\n' "$count" >"$LAUNCH_CALLS/hash-count"
  if [ "$count" = 1 ]; then
    printf '%064d  %s\n' 0 "$1"
    exit 0
  fi
fi
exec /usr/bin/sha256sum "$@"
EOF
chmod 0755 "$launcher_root/bin"/*

run_launcher() {
  mode=$1
  rm -rf "$launcher_root/ring/jobs" "$launcher_root/calls"
  mkdir -p "$launcher_root/calls"
  env PATH="$launcher_root/bin:$fake:$PATH" FAKE_MOUNTPOINT_OK=1 FAKE_DF_AVAILABLE=100000000000 \
    LAUNCH_MODE="$mode" LAUNCH_CALLS="$launcher_root/calls" LAUNCH_RING="$launcher_root/ring" \
    POLYEDGE_RESEARCH_IMAGE="$oci_image" POLYEDGE_LOCAL_RAW_ROOT=/input/events \
    POLYEDGE_DISABLE_RESEARCH_ARTIFACT_PUBLISH=true POLYEDGE_JOB_MIN_FREE_BYTES=1 \
    "$launcher_root/run-job" hourly
}

run_launcher success
launcher_report=$(find "$launcher_root/ring/jobs/research/reports/research/hourly" -name audit.json -print)
launcher_attestation=$launcher_report.attestation.json
jq -e --arg sha "sha256:$(/usr/bin/sha256sum "$launcher_report" | awk '{print $1}')" --arg image "$oci_image" '
  .schema_version == 1 and .report_sha256 == $sha and .platform == "oci_podman" and .image == $image and
  (.image_digest | test("^sha256:[0-9a-f]{64}$")) and (.container_id | test("^[0-9a-f]{64}$"))
' "$launcher_attestation" >/dev/null
! grep -q -- '--rm' "$launcher_root/calls/run"
grep -q '^rm polyedge-job-hourly-' "$launcher_root/calls/rm"

for launcher_failure in provenance hash inspect; do
  if run_launcher "$launcher_failure" >/dev/null 2>&1; then
    echo "launcher $launcher_failure failure unexpectedly passed" >&2
    exit 1
  fi
  [ -s "$launcher_root/calls/rm" ] || { echo "launcher $launcher_failure failure skipped container cleanup" >&2; exit 1; }
  ! find "$launcher_root/ring/jobs/research/reports" -name '*.attestation.json' -print -quit | grep -q .
done

if grep -R -F 'fixture-access-token' "$root"/*/calls >/dev/null || grep -R -F 'fixture-jwt' "$root"/*/calls >/dev/null; then
  echo 'a token leaked into command arguments' >&2
  exit 1
fi
grep -F '/usr/bin/flock -w 300 9' "$collector" >/dev/null
grep -Fx 'RestartPreventExitStatus=78' "$(dirname "$collector")/../systemd/polyedge-parity-hourly.service" >/dev/null

echo 'parity hourly collector self-test passed'
