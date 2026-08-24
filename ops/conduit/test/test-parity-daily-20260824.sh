#!/bin/sh
set -eu

root=$(mktemp -d)
trap 'rm -rf "$root"' EXIT HUP INT TERM
recorder=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)/bin/polyedge-parity-record-daily-20260824
collector=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)/bin/polyedge-parity-hourly-20260824
reboot_attestor=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)/bin/polyedge-reboot-attestation-20260824
collector_sha=sha256:$(sha256sum "$collector" | awk '{print $1}')
validator_sha=sha256:$(sha256sum "$reboot_attestor" | awk '{print $1}')
uid=$(id -u)
gid=$(id -g)
fake=$root/fake-bin
mkdir -p "$fake"
cat >"$fake/mountpoint" <<'EOF'
#!/bin/sh
exit 0
EOF
cat >"$fake/df" <<'EOF'
#!/bin/sh
printf '%s\n' 'Filesystem 1-blocks Used Available Capacity Mounted on'
printf '%s\n' 'fixture 100000000000 1 50000000000 1% /'
EOF
cat >"$fake/runuser" <<'EOF'
#!/bin/sh
while [ "$1" != -- ]; do shift; done
shift
exec "$@"
EOF
cat >"$fake/az" <<'EOF'
#!/bin/sh
jq -nc --arg status "${FAKE_SERVICE_BUS_STATUS:-Active}" \
  --argjson active "${FAKE_SERVICE_BUS_ACTIVE:-0}" --argjson scheduled "${FAKE_SERVICE_BUS_SCHEDULED:-0}" \
  --argjson dlq "${FAKE_SERVICE_BUS_DLQ:-1311}" \
  '{status:$status,countDetails:{activeMessageCount:$active,scheduledMessageCount:$scheduled,deadLetterMessageCount:$dlq}}'
EOF
chmod 0755 "$fake"/*

make_bundle() {
  case_root=$1 date=$2 completed=$3 role=${4:-primary} quality=${5:-pass} git=${6:-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb}
  run="daily-$date-$(printf '%s' "$completed" | tr -d ':-')"
  date_dir=$case_root/ring/jobs/research/reports/research/daily/$date
  bundle=$date_dir/runs/$run
  marker=$case_root/ring/jobs/research/data/research/daily/$date/normalized/.polyedge-daily-complete.json
  mkdir -p "$bundle" "${marker%/*}"
  artifacts='{}'
  for name in baseline calibration data_audit execution_quality final_report markets_summary raw_data_audit regimes sample_size; do
    case "$quality:$name" in
      primary-na*:data_audit)
        jq -n --arg git "$git" --arg quality "$quality" --arg date "$date" '{result:{decision_grade_applicable:false,strategy_evaluations:(if $quality == "primary-na-nonzero" then 1 else 0 end),decision_grade_evaluations:0,decision_grade_coverage:null,final_decision_grade_coverage:null,runtime_provenance:{observations:1440,valid_observations:1440,invalid_observations:0,first_timestamp:(if $quality == "primary-na-malformed-window" then "garbage" elif $quality == "primary-na-partial-window" then ($date + "T12:00:00Z") else ($date + "T00:00:01Z") end),last_timestamp:(if $quality == "primary-na-partial-window" then ($date + "T12:01:00Z") else ($date + "T23:59:59Z") end),max_gap_ms:60000,distinct_identity_count:1,invalid_reasons:[],identities:[{schema_version:1,backend_impl:"rust",git_sha:$git,runtime_config_hash:("sha256:" + ("a" * 64)),app_name:(if $quality == "primary-na-shadow" then "polyedge-shadow-neu" else "polyedge" end),runtime_role:(if $quality == "primary-na-shadow" then "profitability_shadow" else "primary" end),shadow_only:($quality == "primary-na-shadow"),execution_mode:"paper",allow_live:false,enable_taker_orders:false,allow_emergency_account_cancel:false,paper_maker_fill_policy:(if $quality == "primary-na-shadow" then "none" else "touch_after_quote_was_live" end),adaptive_regime_enabled:($quality == "primary-na-shadow"),adaptive_regime_mode:(if $quality == "primary-na-shadow" then "dynamic_quote_style" else "paper_only" end),decision_pipeline_schema:"polyedge.strategy_decision_batch.v4",decision_pipeline_parity_scope:"full_decision_pipeline_recomputation",decision_config_schema:"polyedge.decision_config.v1",decision_config_sha256:("sha256:" + ("b" * 64)),candidate:null,publish_strategy_canary_intents:false,research_only:true,authoritative_recorder_backend:"local_jsonl",storage_account:null,storage_container:"bot-events",event_blob_prefix:"events"}]}}}' >"$bundle/$name.json"
        ;;
      *) printf '{"result":{"fixture":"%s"}}\n' "$name" >"$bundle/$name.json" ;;
    esac
    hash=$(sha256sum "$bundle/$name.json" | awk '{print $1}')
    bytes=$(stat -c %s "$bundle/$name.json")
    artifacts=$(printf '%s' "$artifacts" | jq --arg key "${name}_json" --arg path "$name.json" --arg hash "$hash" --argjson bytes "$bytes" \
      '. + {($key):{name:$key,relative_path:$path,sha256:$hash,bytes:$bytes}}')
  done
  input=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
  jq -n --arg date "$date" --arg run "$run" --arg completed "$completed" --arg input "$input" --arg git "$git" --arg role "$role" \
    --arg quality "$quality" --argjson artifacts "$artifacts" '($quality | startswith("primary-na")) as $na | {schema_version:2,git_sha:$git,runtime_role:$role,date:$date,run_id:$run,created_at:$completed,completed_at:$completed,input_sha256:$input,status:"COMPLETE",artifacts:$artifacts,data_quality:{registry_version:"research-data-quality-v5",total_events:1,decision_grade_coverage:(if $na then "0" else "1" end),fatal_issues:(if $quality == "fail" then ["fixture"] else [] end),warnings:[],out_of_order_events:0,event_time_ordering_restored:true,coverage_breakdown:{start_price_capture_rate:"1",settlement_rate:"1",exact_reference_hour_coverage:"1",decision_metadata_coverage:"1",decision_grade_coverage:(if $na then null else "1" end),decision_grade_applicable:($na | not),final_decision_grade_coverage:(if $na then null else "1" end),execution_field_coverage:"1",decision_parity_rate:"1",queue_position_coverage:null,queue_position_applicable:false,markout_1s_completion:null,markout_1s_applicable:false,markout_5s_completion:null,markout_5s_applicable:false,markout_30s_completion:null,markout_30s_applicable:false}}}' \
    >"$bundle/run_manifest.json"
  manifest_sha=$(sha256sum "$bundle/run_manifest.json" | awk '{print $1}')
  jq -n --arg date "$date" --arg run "$run" --arg path "runs/$run/run_manifest.json" --arg sha "$manifest_sha" --arg completed "$completed" \
    '{schema_version:1,date:$date,run_id:$run,manifest_path:$path,manifest_sha256:$sha,promoted_at:$completed}' >"$date_dir/latest.json"
  jq -n --arg date "$date" --arg git "$git" --arg input "$input" \
    '{schema_version:1,date:$date,git_sha:$git,events_manifest_sha256:$input}' >"$marker"
  chmod 0600 "$marker"
}

fixture() {
  case_root=$1 window_start=${2:-2026-08-11T00:00:00Z}
  mkdir -p "$case_root/run" "$case_root/ring/parity" "$case_root/ring/jobs/research/reports/research/daily" \
    "$case_root/ring/jobs/research/data/research/daily"
  chmod 0750 "$case_root/ring/parity"
  jq -n --arg start "$window_start" '{schemaVersion:1,status:"in_progress",windowStartUtc:$start,azureAuthoritative:true,azureDeletionAllowed:false,acceptedCleanLiveHours:1,acceptedHourlyEvidence:[{fixture:true}],completedDailyCycles:0,acceptedDailyEvidence:[],rebootRecoveryPassed:false,shadowQsetEnabled:false,fundedSignerEnabled:false}' \
    >"$case_root/ring/parity/ledger.json"
  chmod 0640 "$case_root/ring/parity/ledger.json"
  jq -n '{capacity_ok:true,free_ok:true,upload_fresh:true,unsealed_closed_count:0,unuploaded_count:0}' >"$case_root/ring/status.json"
  chmod 0640 "$case_root/ring/status.json"
  cat >"$case_root/parity.env" <<EOF
POLYEDGE_PARITY_WINDOW_START_UTC=$window_start
POLYEDGE_PARITY_LEDGER=$case_root/ring/parity/ledger.json
POLYEDGE_PARITY_RING_ROOT=$case_root/ring
POLYEDGE_PARITY_DAILY_REPORT_ROOT=$case_root/ring/jobs/research/reports/research/daily
POLYEDGE_PARITY_DAILY_DATA_ROOT=$case_root/ring/jobs/research/data/research/daily
POLYEDGE_PARITY_RING_STATUS=$case_root/ring/status.json
POLYEDGE_PARITY_BOOT_ROOT=$case_root
POLYEDGE_PARITY_PAUSE_FILE=$case_root/run/image-pulls-paused
POLYEDGE_PARITY_LOCK_FILE=$case_root/run/ledger.lock
POLYEDGE_PARITY_EXPECTED_GIT_SHA=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
POLYEDGE_PARITY_EXPECTED_AZURE_RESEARCH_IMAGE=crpolyedgefixture.azurecr.io/polyedge-rust-research@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc
POLYEDGE_PARITY_EXPECTED_OCI_RESEARCH_IMAGE=ghcr.io/fixture/polyedge-rust-backend@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
POLYEDGE_PARITY_EXPECTED_RESEARCH_IMAGE=ghcr.io/fixture/polyedge-rust-backend@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
EOF
  chmod 0640 "$case_root/parity.env"
}

activate_funded_fixture() {
  case_root=$1
  funded_image=ghcr.io/fixture/polyedge-venue-probe@sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd
  funded_revision=7777777777777777777777777777777777777777
  funded_dlq=1311
  producer_image=ghcr.io/aldoapicella/polyedge-rust-backend@sha256:6398418916a60793d5c8d28cbf10592edcfd5203f4f2b700014c1b27a5e815fc
  producer_revision=6666666666666666666666666666666666666666
  activation=$case_root/ring/parity/activation
  rollout=$activation/post-redemption-venue-redemption-20260824182234412-7ef7b79f-attestation.json
  install -d -m 0750 "$activation"
  jq -n --arg image "$funded_image" --arg revision "$funded_revision" --arg producer_image "$producer_image" \
    --arg producer_revision "$producer_revision" --argjson dlq "$funded_dlq" \
    '{schema:"polyedge.funded_signer_post_redemption_attestation.v1",status:"attested",createdAtUtc:"2026-08-24T18:30:00Z",
      helperSha256:("sha256:" + ("1" * 64)),authorizedDeadLetterBaseline:$dlq,
      redemption:{transactionHash:("0x" + ("2" * 64)),settlementBlob:"fixture-settlement"},
      evidence:{
        liveSummary:{path:"fixture-live",sha256:("sha256:" + ("3" * 64))},
        followUpDryRun:{path:"fixture-dry",sha256:("sha256:" + ("4" * 64))},
        internalSettlement:{path:"fixture-settlement",sha256:("sha256:" + ("5" * 64))}},
      runtime:{
        signer:{invocationId:("a" * 32),containerId:("e" * 64),restartCount:0,image:$image,revision:$revision,user:"986:982"},
        producer:{invocationId:("b" * 32),containerId:("f" * 64),restartCount:0,image:$producer_image,
          revision:$producer_revision,user:"984:980",status:"running",health:"healthy"}},
      heartbeat:{capturedAtEpoch:1787596150,processedMessages:1,failedMessages:0,failedAttempts:0,
        executor:{
          busy:false,user_channel_ready:true,market_channel_ready:true,user_channel_gaps:0,market_channel_gaps:0,
          user_channel_unparsed:0,market_channel_unparsed:0,reconnect_reconciliation_required:false,
          safety_snapshot_cache_ready:true,safety_snapshot_cache_age_ms:100,safety_snapshot_open_order_count:0,
          safety_snapshot_unresolved_position_count:0,safety_snapshot_unresolved_risk_reservation_count:0,
          safety_snapshot_cache_error:null,risk_reservation_index_ready:true}},
      queue:{before:{status:"Active",activeMessageCount:0,scheduledMessageCount:0,deadLetterMessageCount:$dlq},
        after:{status:"Active",activeMessageCount:0,scheduledMessageCount:0,deadLetterMessageCount:$dlq},deadLetterNonGrowth:true},
      servicesMutated:false,staleRecoveryReceiptsAccepted:false,parityTimerRemainsPaused:true,azureDeletionAllowed:false}' >"$rollout"
  chmod 0640 "$rollout"
  rollout_sha=sha256:$(sha256sum "$rollout" | awk '{print $1}')
  active_ledger=$case_root/ring/parity/20260811T000000Z-funded-active.json
  jq --arg user "$uid:$gid" --arg rollout "$rollout" --arg rollout_sha "$rollout_sha" \
    --arg collector_sha "$collector_sha" --arg validator_sha "$validator_sha" '.fundedSignerEnabled=true | .fundedSignerMode="active" |
    .fundedSignerImage="ghcr.io/fixture/polyedge-venue-probe@sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd" |
    .fundedSignerRevision="7777777777777777777777777777777777777777" |
    .fundedRolloutReceiptPath=$rollout | .fundedRolloutReceiptSha256=$rollout_sha |
    .fundedServiceBusDlqBaseline=1311 |
    .parityCollectorSha256=$collector_sha | .rebootValidatorSha256=$validator_sha |
    .fundedSignerUser=$user | .fundedSessionId="dynamic-quote-funded-2026-08-13-v10" |
    .fundedSessionManifestSha256="sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff" |
    .fundedConfigSha256="sha256:9999999999999999999999999999999999999999999999999999999999999999" |
    .fundedIntentProducerEnabled=true | .fundedIntentProducerImage="ghcr.io/aldoapicella/polyedge-rust-backend@sha256:6398418916a60793d5c8d28cbf10592edcfd5203f4f2b700014c1b27a5e815fc" |
    .fundedIntentProducerUser="984:980" | .fundedIntentProducerConfigSha256="sha256:56d8d0573ffbc2f50354100921355244ceedb71e1b28bbf32dea9f0a18b0c87b"' \
    "$case_root/ring/parity/ledger.json" >"$active_ledger"
  rm "$case_root/ring/parity/ledger.json"
  chmod 0640 "$active_ledger"
  sed -i "s#POLYEDGE_PARITY_LEDGER=.*#POLYEDGE_PARITY_LEDGER=$active_ledger#" "$case_root/parity.env"
  cat >>"$case_root/parity.env" <<EOF
POLYEDGE_PARITY_FUNDED_MODE=active
POLYEDGE_PARITY_EXPECTED_FUNDED_IMAGE=ghcr.io/fixture/polyedge-venue-probe@sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd
POLYEDGE_PARITY_FUNDED_UID=$uid
POLYEDGE_PARITY_EXPECTED_FUNDED_REVISION=$funded_revision
POLYEDGE_PARITY_FUNDED_ROLLOUT_RECEIPT=$rollout
POLYEDGE_PARITY_EXPECTED_FUNDED_ROLLOUT_RECEIPT_SHA256=$rollout_sha
POLYEDGE_PARITY_EXPECTED_FUNDED_SERVICE_BUS_DLQ=$funded_dlq
POLYEDGE_PARITY_EXPECTED_COLLECTOR_SHA256=$collector_sha
POLYEDGE_PARITY_EXPECTED_REBOOT_VALIDATOR_SHA256=$validator_sha
POLYEDGE_PARITY_FUNDED_GID=$gid
POLYEDGE_PARITY_EXPECTED_FUNDED_SESSION_ID=dynamic-quote-funded-2026-08-13-v10
POLYEDGE_PARITY_EXPECTED_FUNDED_SESSION_SHA256=sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
POLYEDGE_PARITY_EXPECTED_FUNDED_CONFIG_SHA256=sha256:9999999999999999999999999999999999999999999999999999999999999999
POLYEDGE_PARITY_EXPECTED_FUNDED_PRODUCER_IMAGE=ghcr.io/aldoapicella/polyedge-rust-backend@sha256:6398418916a60793d5c8d28cbf10592edcfd5203f4f2b700014c1b27a5e815fc
POLYEDGE_PARITY_EXPECTED_FUNDED_PRODUCER_CONFIG_SHA256=sha256:56d8d0573ffbc2f50354100921355244ceedb71e1b28bbf32dea9f0a18b0c87b
EOF
}

run_recorder() {
  case_root=$1 date=$2
  env PATH="$fake:$PATH" POLYEDGE_PARITY_EXPECTED_UID="$uid" POLYEDGE_PARITY_EXPECTED_GID="$gid" \
    POLYEDGE_PARITY_COLLECTOR_BIN="${TEST_COLLECTOR_BIN:-$collector}" POLYEDGE_REBOOT_ATTESTATION_BIN="${TEST_VALIDATOR_BIN:-$reboot_attestor}" POLYEDGE_REBOOT_EXPECTED_UID="$uid" POLYEDGE_REBOOT_EXPECTED_GID="$gid" \
    POLYEDGE_RUNUSER_BIN="$fake/runuser" POLYEDGE_AZ_BIN="$fake/az" \
    FAKE_SERVICE_BUS_DLQ="${FAKE_SERVICE_BUS_DLQ:-1311}" FAKE_SERVICE_BUS_STATUS="${FAKE_SERVICE_BUS_STATUS:-Active}" \
    FAKE_SERVICE_BUS_ACTIVE="${FAKE_SERVICE_BUS_ACTIVE:-0}" FAKE_SERVICE_BUS_SCHEDULED="${FAKE_SERVICE_BUS_SCHEDULED:-0}" \
    POLYEDGE_RESEARCH_IMAGE=ghcr.io/fixture/polyedge-rust-backend@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
    POLYEDGE_PARITY_ENV_FILE="$case_root/parity.env" "$recorder" "$date"
}

frozen=$root/frozen
fixture "$frozen"
make_bundle "$frozen" 2026-08-11 2026-08-12T08:00:00Z
set -a
. "$frozen/parity.env"
set +a
printf '%s\n' 'POLYEDGE_PARITY_EXPECTED_RESEARCH_IMAGE=ghcr.io/fixture/polyedge-rust-backend@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc' \
  >"$frozen/parity.env"
chmod 0640 "$frozen/parity.env"
env PATH="$fake:$PATH" POLYEDGE_PARITY_EXPECTED_UID="$uid" POLYEDGE_PARITY_EXPECTED_GID="$gid" \
  POLYEDGE_REBOOT_ATTESTATION_BIN="$reboot_attestor" POLYEDGE_REBOOT_EXPECTED_UID="$uid" POLYEDGE_REBOOT_EXPECTED_GID="$gid" \
  POLYEDGE_RESEARCH_IMAGE=ghcr.io/fixture/polyedge-rust-backend@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  POLYEDGE_PARITY_BINDINGS_FROZEN=1 POLYEDGE_PARITY_ENV_FILE="$frozen/parity.env" \
  "$recorder" 2026-08-11 >/dev/null
[ "$(jq -r '.completedDailyCycles' "$frozen/ring/parity/ledger.json")" = 1 ]
unset POLYEDGE_PARITY_WINDOW_START_UTC POLYEDGE_PARITY_LEDGER POLYEDGE_PARITY_RING_ROOT \
  POLYEDGE_PARITY_DAILY_REPORT_ROOT POLYEDGE_PARITY_DAILY_DATA_ROOT POLYEDGE_PARITY_RING_STATUS \
  POLYEDGE_PARITY_BOOT_ROOT POLYEDGE_PARITY_PAUSE_FILE POLYEDGE_PARITY_LOCK_FILE \
  POLYEDGE_PARITY_EXPECTED_GIT_SHA POLYEDGE_PARITY_EXPECTED_RESEARCH_IMAGE

prewindow=$root/prewindow
fixture "$prewindow"
prewindow_ledger_sha=$(sha256sum "$prewindow/ring/parity/ledger.json")
run_recorder "$prewindow" 2026-08-10 >/dev/null
[ "$(sha256sum "$prewindow/ring/parity/ledger.json")" = "$prewindow_ledger_sha" ]
[ ! -e "$prewindow/ring/parity/daily" ]

noon=$root/noon
fixture "$noon" 2026-08-17T12:00:00Z
noon_ledger_sha=$(sha256sum "$noon/ring/parity/ledger.json")
run_recorder "$noon" 2026-08-17 >/dev/null
[ "$(sha256sum "$noon/ring/parity/ledger.json")" = "$noon_ledger_sha" ]
[ ! -e "$noon/ring/parity/daily" ]
make_bundle "$noon" 2026-08-18 2026-08-19T08:00:00Z
run_recorder "$noon" 2026-08-18 >/dev/null
jq -e '.completedDailyCycles == 1 and (.acceptedDailyEvidence | length) == 1 and .acceptedDailyEvidence[0].cycleDate == "2026-08-18"' \
  "$noon/ring/parity/ledger.json" >/dev/null
make_bundle "$noon" 2026-08-19 2026-08-20T08:00:00Z
run_recorder "$noon" 2026-08-19 >/dev/null
[ "$(jq -r '.completedDailyCycles' "$noon/ring/parity/ledger.json")" = 2 ]

success=$root/success
fixture "$success"
make_bundle "$success" 2026-08-11 2026-08-12T08:00:00Z
before=$(jq -cS '{status,azureAuthoritative,azureDeletionAllowed,rebootRecoveryPassed,rebootRecovery,shadowQsetEnabled,fundedSignerEnabled,fundedSignerMode,fundedSignerImage,fundedSignerUser,fundedSessionId,fundedSessionManifestSha256,fundedConfigSha256,fundedIntentProducerEnabled,fundedIntentProducerImage,fundedIntentProducerUser,fundedIntentProducerConfigSha256,acceptedCleanLiveHours,acceptedHourlyEvidence}' "$success/ring/parity/ledger.json")
run_recorder "$success" 2026-08-11 >/dev/null
[ "$(jq -r '.completedDailyCycles' "$success/ring/parity/ledger.json")" = 1 ]
[ "$(jq -r '.acceptedDailyEvidence | length' "$success/ring/parity/ledger.json")" = 1 ]
[ "$(jq -cS '{status,azureAuthoritative,azureDeletionAllowed,rebootRecoveryPassed,rebootRecovery,shadowQsetEnabled,fundedSignerEnabled,fundedSignerMode,fundedSignerImage,fundedSignerUser,fundedSessionId,fundedSessionManifestSha256,fundedConfigSha256,fundedIntentProducerEnabled,fundedIntentProducerImage,fundedIntentProducerUser,fundedIntentProducerConfigSha256,acceptedCleanLiveHours,acceptedHourlyEvidence}' "$success/ring/parity/ledger.json")" = "$before" ]
evidence_sha=$(sha256sum "$success/ring/parity/daily/2026-08-11/evidence.json")
run_recorder "$success" 2026-08-11 >/dev/null
[ "$(sha256sum "$success/ring/parity/daily/2026-08-11/evidence.json")" = "$evidence_sha" ]
jq -e '.completedDailyCycles == 1 and (.acceptedDailyEvidence | length) == 1' "$success/ring/parity/ledger.json" >/dev/null
make_bundle "$success" 2026-08-12 2026-08-13T08:00:00Z
run_recorder "$success" 2026-08-12 >/dev/null
[ "$(jq -r '.completedDailyCycles' "$success/ring/parity/ledger.json")" = 2 ]

unexpected_active_ledger=$root/unexpected-active-ledger
fixture "$unexpected_active_ledger"
jq '.fundedSignerEnabled=true' "$unexpected_active_ledger/ring/parity/ledger.json" >"$unexpected_active_ledger/ledger.tmp"
mv "$unexpected_active_ledger/ledger.tmp" "$unexpected_active_ledger/ring/parity/ledger.json"
chmod 0640 "$unexpected_active_ledger/ring/parity/ledger.json"
make_bundle "$unexpected_active_ledger" 2026-08-11 2026-08-12T08:00:00Z
if run_recorder "$unexpected_active_ledger" 2026-08-11 >/dev/null 2>&1; then
  echo 'active funded ledger unexpectedly passed in the default masked mode' >&2
  exit 1
fi

active_funded=$root/active-funded
fixture "$active_funded"
activate_funded_fixture "$active_funded"
make_bundle "$active_funded" 2026-08-11 2026-08-12T08:00:00Z
run_recorder "$active_funded" 2026-08-11 >/dev/null
jq -e '.fundedSignerEnabled == true and .completedDailyCycles == 1 and
  (.acceptedDailyEvidence | length) == 1' "$active_funded/ring/parity/20260811T000000Z-funded-active.json" >/dev/null
jq -e '.fundedSignerMode == "active" and .fundedSignerEnabled == true and
  .fundedSignerRevision == "7777777777777777777777777777777777777777" and
  (.fundedRolloutReceipt.path | endswith("/activation/post-redemption-venue-redemption-20260824182234412-7ef7b79f-attestation.json")) and
  (.fundedRolloutReceipt.sha256 | test("^sha256:[0-9a-f]{64}$")) and
  .fundedServiceBusDlqBaseline == 1311 and
  .fundedSessionId == "dynamic-quote-funded-2026-08-13-v10" and .fundedSignerUser == "'"$uid:$gid"'" and
  .fundedIntentProducerEnabled == true and .fundedIntentProducerUser == "984:980" and
  .fundedIntentProducerConfigSha256 == "sha256:56d8d0573ffbc2f50354100921355244ceedb71e1b28bbf32dea9f0a18b0c87b"' \
  "$active_funded/ring/parity/daily/2026-08-11/evidence.json" >/dev/null

queue_drift=$root/queue-drift
fixture "$queue_drift"
activate_funded_fixture "$queue_drift"
make_bundle "$queue_drift" 2026-08-11 2026-08-12T08:00:00Z
if FAKE_SERVICE_BUS_DLQ=1312 run_recorder "$queue_drift" 2026-08-11 >/dev/null 2>&1; then
  echo 'drifted Service Bus DLQ unexpectedly passed daily parity' >&2
  exit 1
fi

validator_drift_bin=$root/validator-drift-bin
cp "$reboot_attestor" "$validator_drift_bin"
printf '\n' >>"$validator_drift_bin"
chmod 0755 "$validator_drift_bin"
validator_drift=$root/validator-drift
fixture "$validator_drift"
activate_funded_fixture "$validator_drift"
make_bundle "$validator_drift" 2026-08-11 2026-08-12T08:00:00Z
if TEST_VALIDATOR_BIN="$validator_drift_bin" run_recorder "$validator_drift" 2026-08-11 >/dev/null 2>&1; then
  echo 'drifted installed reboot validator unexpectedly passed daily parity' >&2
  exit 1
fi


producer_drift=$root/producer-drift
fixture "$producer_drift"
activate_funded_fixture "$producer_drift"
producer_ledger=$producer_drift/ring/parity/20260811T000000Z-funded-active.json
jq '.fundedIntentProducerConfigSha256="sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"' \
  "$producer_ledger" >"$producer_drift/ledger.tmp"
chmod 0640 "$producer_drift/ledger.tmp"
mv "$producer_drift/ledger.tmp" "$producer_ledger"
make_bundle "$producer_drift" 2026-08-11 2026-08-12T08:00:00Z
if run_recorder "$producer_drift" 2026-08-11 >/dev/null 2>&1; then
  echo 'drifted producer config unexpectedly passed daily parity' >&2
  exit 1
fi

primary_na=$root/primary-na
fixture "$primary_na"
make_bundle "$primary_na" 2026-08-11 2026-08-12T08:00:00Z primary primary-na
run_recorder "$primary_na" 2026-08-11 >/dev/null
[ "$(jq -r '.completedDailyCycles' "$primary_na/ring/parity/ledger.json")" = 1 ]

primary_na_nonzero=$root/primary-na-nonzero
fixture "$primary_na_nonzero"
make_bundle "$primary_na_nonzero" 2026-08-11 2026-08-12T08:00:00Z primary primary-na-nonzero
if run_recorder "$primary_na_nonzero" 2026-08-11 >/dev/null 2>&1; then
  echo 'decision-grade N/A with nonzero evaluations unexpectedly passed' >&2
  exit 1
fi

primary_na_shadow=$root/primary-na-shadow
fixture "$primary_na_shadow"
make_bundle "$primary_na_shadow" 2026-08-11 2026-08-12T08:00:00Z primary primary-na-shadow
if run_recorder "$primary_na_shadow" 2026-08-11 >/dev/null 2>&1; then
  echo 'decision-grade N/A with shadow provenance unexpectedly passed' >&2
  exit 1
fi

for quality in primary-na-malformed-window primary-na-partial-window; do
  invalid_window=$root/$quality
  fixture "$invalid_window"
  make_bundle "$invalid_window" 2026-08-11 2026-08-12T08:00:00Z primary "$quality"
  if run_recorder "$invalid_window" 2026-08-11 >/dev/null 2>&1; then
    echo "decision-grade N/A with $quality unexpectedly passed" >&2
    exit 1
  fi
done

legacy=$root/legacy
fixture "$legacy"
jq 'del(.completedDailyCycles,.acceptedDailyEvidence)' "$legacy/ring/parity/ledger.json" >"$legacy/ledger.tmp"
chmod 0640 "$legacy/ledger.tmp"
mv "$legacy/ledger.tmp" "$legacy/ring/parity/ledger.json"
make_bundle "$legacy" 2026-08-11 2026-08-12T08:00:00Z
run_recorder "$legacy" 2026-08-11 >/dev/null
jq -e '.completedDailyCycles == 1 and (.acceptedDailyEvidence | length) == 1' "$legacy/ring/parity/ledger.json" >/dev/null

gap=$root/gap
fixture "$gap"
make_bundle "$gap" 2026-08-12 2026-08-13T08:00:00Z
if run_recorder "$gap" 2026-08-12 >/dev/null 2>&1; then
  echo 'gapped daily evidence unexpectedly passed' >&2
  exit 1
fi
make_bundle "$gap" 2026-08-11 2026-08-12T08:00:00Z
run_recorder "$gap" 2026-08-11 >/dev/null
[ "$(jq -r '.completedDailyCycles' "$gap/ring/parity/ledger.json")" = 1 ]
run_recorder "$gap" 2026-08-12 >/dev/null
[ "$(jq -r '.completedDailyCycles' "$gap/ring/parity/ledger.json")" = 2 ]

tamper=$root/tamper
fixture "$tamper"
make_bundle "$tamper" 2026-08-11 2026-08-12T08:00:00Z
printf 'tampered\n' >>"$(find "$tamper/ring/jobs/research/reports/research/daily/2026-08-11/runs" -name baseline.json)"
if run_recorder "$tamper" 2026-08-11 >/dev/null 2>&1; then
  echo 'tampered daily artifact unexpectedly passed' >&2
  exit 1
fi

role=$root/role
fixture "$role"
make_bundle "$role" 2026-08-11 2026-08-12T08:00:00Z profitability_shadow
if run_recorder "$role" 2026-08-11 >/dev/null 2>&1; then
  echo 'shadow daily bundle unexpectedly passed as primary' >&2
  exit 1
fi

quality=$root/quality
fixture "$quality"
make_bundle "$quality" 2026-08-11 2026-08-12T08:00:00Z primary fail
if run_recorder "$quality" 2026-08-11 >/dev/null 2>&1; then
  echo 'fatal daily data quality unexpectedly passed' >&2
  exit 1
fi

source=$root/source
fixture "$source"
make_bundle "$source" 2026-08-11 2026-08-12T08:00:00Z primary pass cccccccccccccccccccccccccccccccccccccccc
if run_recorder "$source" 2026-08-11 >/dev/null 2>&1; then
  echo 'unapproved daily source unexpectedly passed' >&2
  exit 1
fi

sh -n "$recorder"
grep -F 'POLYEDGE_PARITY_LOCK_FILE' "$recorder" >/dev/null
echo 'parity daily recorder self-test passed'
