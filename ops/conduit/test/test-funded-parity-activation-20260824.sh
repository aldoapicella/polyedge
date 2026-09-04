#!/usr/bin/env bash
set -euo pipefail

repo=$(cd "$(dirname "$0")/../../.." && pwd)
stage=$repo/ops/conduit/bin/polyedge-parity-stage-funded-active-20260824
first=$repo/ops/conduit/bin/polyedge-parity-collect-first-hour-20260824
root=$(mktemp -d)
grep -Fq 'readonly window=2026-08-26T14:00:00Z' "$stage"
grep -Fq 'readonly rollout=/srv/polyedge-ring/parity/activation/post-redemption-venue-redemption-20260826124254663-9c84ed65-attestation.json' "$stage"
grep -Fq 'readonly rollout_receipt_sha=sha256:512444359038a3d22c2b0cd1b3c76250688761a24ae340c6683e979157337e4b' "$stage"
grep -Fq 'readonly rollout_helper_sha=sha256:260cd403f022235c26efe96483ef0809f73cb51d669707080337e135b738f3de' "$stage"
grep -Fq 'readonly authorized_dlq=1343' "$stage"
grep -Fq 'readonly rollout_receipt_sha=sha256:512444359038a3d22c2b0cd1b3c76250688761a24ae340c6683e979157337e4b' "$first"
grep -Fq 'persistent_message_failed_closed' "$stage"
grep -Fq '.failed_attempts == 0' "$stage"
grep -Fq '$warmed | length) >= 1' "$stage"
grep -Fq 'processed_messages > $heartbeats[0].event.processed_messages' "$stage"
grep -Fq '{{ index .Labels "org.opencontainers.image.revision" }}' "$stage"
! grep -Fq '{{ index .Labels \"org.opencontainers.image.revision\" }}' "$stage"
grep -Fq '{{ index .Labels "org.opencontainers.image.revision" }}' "$first"
! grep -Fq '{{ index .Labels \"org.opencontainers.image.revision\" }}' "$first"
grep -Fq 'as $all_heartbeats' "$stage"
grep -Fq 'as $ready_start' "$stage"
grep -Fq '$ready_start != null' "$stage"
stage_jq=$(sed -n '524,553p' "$stage")
printf '%s' '' | jq -Rs --arg invocation aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa --arg container eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee --argjson now 0 "$stage_jq" >/dev/null
! grep -Fq 'recovery_dlq=' "$stage"
trap 'rm -rf "$root"' EXIT

fixture_recovery=$root/recovery.json
fixture_settlement=$root/settlement.json
fixture_rollout=$root/post-redemption-fixture-attestation.json
jq -n '{
  schema:"polyedge.acknowledged_no_fill_reconciliation.v1",status:"finalized_no_fill",
  decision_id:"65b559290100796ef9137179176bf053c8ab5421a1ef3c80bd8fd81676611de7",
  run_id:"funded-direct-20260822235002541-68c5aaa9",
  order_id:"0xd9b1491affbcc71b3876763af5583411b05e08ef8b46a74a88d983dbea1a9319",
  order_submission_attempted:true,recovery_order_submission_attempted:false,
  recovery_grant_consumed:false,recovery_risk_reservation_created:false,
  reconciliation_reason:"acknowledged_evicted_order_no_fill",evidence:{observation_ms:10000},
  recoveryImage:"ghcr.io/aldoapicella/polyedge-venue-probe@sha256:212a34d97075ff74b57681aff65e49913431e6caf2f7c015104102c62837e6f3",
  signerImageUnchanged:"ghcr.io/aldoapicella/polyedge-venue-probe@sha256:212a34d97075ff74b57681aff65e49913431e6caf2f7c015104102c62837e6f3",
  producerImageUnchanged:"ghcr.io/aldoapicella/polyedge-rust-backend@sha256:9eb1b04b01b131bd440bb956c8784e8e493a6e03fe4f03aeb27142284c6fcba8",
  reservationEvidence:{blob:"reports/research/venue-probe/risk-reservations/2026-08-22/funded-direct-65b559290100796ef9137179176bf053c8ab5421a1ef3c80bd8fd81676611de7.json",sha256:"sha256:e32a1ec82254c5490f0c25a25d2fed99fe3ab129002610234c704ef688a24a9d"},
  completionEvidence:{blob:"reports/funded/dynamic-quote/sessions/dynamic-quote-funded-2026-08-13-v10/completed/65b559290100796ef9137179176bf053c8ab5421a1ef3c80bd8fd81676611de7.json",sha256:"sha256:fcc2f5861c5364202bd171b4b229e883472980f1db752ab7588bdba1d2f3bed9"},
  summaryEvidence:{blob:"reports/research/venue-probe/runs/2026-08-22/funded-direct-20260822235002541-68c5aaa9/summary.json",sha256:"sha256:a5ea76b54d8b3b9b3fa4ae5d549de6466d0eae1c22d4ca4074b362ec0c725d57"},
  unresolvedReservationsAfter:0,queueActiveMessages:0,queueScheduledMessages:0,queueDeadLetterMessages:1308,
  parityTimerRemainsPaused:true,azureDeletionAllowed:false,cutoverCompletedAtUtc:"2026-08-24T05:23:21Z"
}' >"$fixture_recovery"
fixture_recovery_sha=sha256:$(sha256sum "$fixture_recovery" | cut -d' ' -f1)
jq -n '{schema:"polyedge.funded_settlement_loss_reconciliation.v1",status:"finalized",authorizedDeadLetterBaseline:1311}' >"$fixture_settlement"
fixture_settlement_sha=sha256:$(sha256sum "$fixture_settlement" | cut -d' ' -f1)
new_image=ghcr.io/aldoapicella/polyedge-venue-probe@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc
revision=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
producer_image=ghcr.io/aldoapicella/polyedge-rust-backend@sha256:6398418916a60793d5c8d28cbf10592edcfd5203f4f2b700014c1b27a5e815fc
jq -n --arg image "$new_image" --arg revision "$revision" --arg producer_image "$producer_image" '{
  schema:"polyedge.funded_signer_post_redemption_attestation.v1",status:"attested",createdAtUtc:"2026-08-24T18:30:00Z",
  helperSha256:"sha256:1111111111111111111111111111111111111111111111111111111111111111",
  authorizedDeadLetterBaseline:1311,
  redemption:{transactionHash:("0x" + ("2" * 64)),settlementBlob:"fixture-settlement"},
  evidence:{
    liveSummary:{path:"fixture-live",sha256:("sha256:" + ("3" * 64))},
    followUpDryRun:{path:"fixture-dry",sha256:("sha256:" + ("4" * 64))},
    internalSettlement:{path:"fixture-settlement",sha256:("sha256:" + ("5" * 64))}},
  runtime:{
    signer:{invocationId:"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",containerId:("e" * 64),restartCount:0,image:$image,revision:$revision,user:"986:982"},
    producer:{invocationId:"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",containerId:("f" * 64),restartCount:0,image:$producer_image,
      revision:"6666666666666666666666666666666666666666",user:"984:980",status:"running",health:"healthy"}},
  heartbeat:{capturedAtEpoch:1787596150,processedMessages:1,failedMessages:0,failedAttempts:0,executor:{
    busy:false,user_channel_ready:true,market_channel_ready:true,user_channel_gaps:0,market_channel_gaps:0,
    user_channel_unparsed:0,market_channel_unparsed:0,reconnect_reconciliation_required:false,
    safety_snapshot_cache_ready:true,safety_snapshot_cache_age_ms:100,safety_snapshot_open_order_count:0,
    safety_snapshot_unresolved_position_count:0,safety_snapshot_unresolved_risk_reservation_count:0,
    safety_snapshot_cache_error:null,risk_reservation_index_ready:true}},
  queue:{before:{status:"Active",activeMessageCount:0,scheduledMessageCount:0,deadLetterMessageCount:1311},
    after:{status:"Active",activeMessageCount:0,scheduledMessageCount:0,deadLetterMessageCount:1311},deadLetterNonGrowth:true},
  servicesMutated:false,staleRecoveryReceiptsAccepted:false,parityTimerRemainsPaused:true,azureDeletionAllowed:false
}' >"$fixture_rollout"

(
  POLYEDGE_TEST_UID=$(id -u) POLYEDGE_TEST_GID=$(id -g) POLYEDGE_TEST_SOURCE_ONLY=1 . "$stage"
  validate_recovery_receipt "$fixture_recovery"
  validate_rollout_receipt "$fixture_rollout"
  test "$signer_image" = "$new_image"
  test "$signer_revision" = "$revision"
  test "$service_bus_dlq" = 1311

  source_env=$root/source.env
  rendered=$root/rendered.env
  cat >"$source_env" <<'ENV'
POLYEDGE_PARITY_WINDOW_START_UTC=2026-08-21T20:00:00Z
POLYEDGE_PARITY_LEDGER=/srv/polyedge-ring/parity/old.json
POLYEDGE_PARITY_FUNDED_MODE=active
POLYEDGE_PARITY_EXPECTED_FUNDED_IMAGE=old
POLYEDGE_PARITY_FUNDED_UID=986
POLYEDGE_PARITY_FUNDED_GID=982
POLYEDGE_PARITY_EXPECTED_FUNDED_SESSION_ID=fixture-session
POLYEDGE_PARITY_EXPECTED_FUNDED_SESSION_SHA256=sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
POLYEDGE_PARITY_EXPECTED_FUNDED_CONFIG_SHA256=sha256:9999999999999999999999999999999999999999999999999999999999999999
POLYEDGE_PARITY_EXPECTED_FUNDED_PRODUCER_IMAGE=ghcr.io/aldoapicella/polyedge-rust-backend@sha256:9eb1b04b01b131bd440bb956c8784e8e493a6e03fe4f03aeb27142284c6fcba8
POLYEDGE_PARITY_EXPECTED_FUNDED_PRODUCER_CONFIG_SHA256=sha256:8888888888888888888888888888888888888888888888888888888888888888
ENV
  funded_uid=986 funded_gid=982 funded_session=fixture-session
  funded_session_sha=sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
  funded_config_sha=sha256:9999999999999999999999999999999999999999999999999999999999999999
  funded_producer_image=ghcr.io/aldoapicella/polyedge-rust-backend@sha256:9eb1b04b01b131bd440bb956c8784e8e493a6e03fe4f03aeb27142284c6fcba8
  funded_producer_config_sha=sha256:8888888888888888888888888888888888888888888888888888888888888888
  keys=(POLYEDGE_PARITY_WINDOW_START_UTC POLYEDGE_PARITY_LEDGER POLYEDGE_PARITY_FUNDED_MODE
    POLYEDGE_PARITY_EXPECTED_FUNDED_IMAGE POLYEDGE_PARITY_EXPECTED_FUNDED_REVISION
    POLYEDGE_PARITY_FUNDED_ROLLOUT_RECEIPT POLYEDGE_PARITY_EXPECTED_FUNDED_ROLLOUT_RECEIPT_SHA256
    POLYEDGE_PARITY_FUNDED_UID POLYEDGE_PARITY_FUNDED_GID POLYEDGE_PARITY_EXPECTED_FUNDED_SESSION_ID
    POLYEDGE_PARITY_EXPECTED_FUNDED_SESSION_SHA256 POLYEDGE_PARITY_EXPECTED_FUNDED_CONFIG_SHA256
    POLYEDGE_PARITY_EXPECTED_FUNDED_PRODUCER_IMAGE POLYEDGE_PARITY_EXPECTED_FUNDED_PRODUCER_CONFIG_SHA256
    POLYEDGE_PARITY_EXPECTED_FUNDED_SERVICE_BUS_DLQ POLYEDGE_PARITY_EXPECTED_COLLECTOR_SHA256
    POLYEDGE_PARITY_EXPECTED_REBOOT_VALIDATOR_SHA256)
  render_env "$source_env" "$rendered" "${keys[@]}"
  for key in "${keys[@]}"; do test "$(grep -c "^${key}=" "$rendered")" = 1; done
  grep -qx "POLYEDGE_PARITY_EXPECTED_FUNDED_REVISION=$revision" "$rendered"
  grep -qx 'POLYEDGE_PARITY_EXPECTED_FUNDED_SERVICE_BUS_DLQ=1311' "$rendered"
  printf '%s\n' 'POLYEDGE_PARITY_FUNDED_MODE=active' >>"$source_env"
  if render_env "$source_env" "$root/duplicate.env" "${keys[@]}"; then exit 1; fi

  jq '.staleRecoveryReceiptsAccepted=true' "$fixture_rollout" >"$root/bad-rollout.json"
  if validate_rollout_receipt "$root/bad-rollout.json"; then exit 1; fi

  tx_root=$root/transaction
  mkdir -p "$tx_root"
  printf '%s\n' before-formal >"$tx_root/before-formal"
  printf '%s\n' before-hourly >"$tx_root/before-hourly"
  printf '%s\n' candidate-formal >"$tx_root/candidate-formal"
  printf '%s\n' candidate-hourly >"$tx_root/candidate-hourly"
  cp "$tx_root/before-formal" "$tx_root/formal"
  cp "$tx_root/before-hourly" "$tx_root/hourly"
  cp "$tx_root/candidate-formal" "$tx_root/formal.tmp"
  cp "$tx_root/candidate-hourly" "$tx_root/hourly.tmp"
  chmod 0640 "$tx_root/before-formal" "$tx_root/candidate-formal" "$tx_root/formal" "$tx_root/formal.tmp"
  chmod 0600 "$tx_root/before-hourly" "$tx_root/candidate-hourly" "$tx_root/hourly" "$tx_root/hourly.tmp"
  set +e
  (
    POLYEDGE_TEST_KILL_BETWEEN_ENV_MOVES=1 commit_candidate_pair "$tx_root/marker.json" \
      "$tx_root/formal" "$tx_root/hourly" "$tx_root/before-formal" "$tx_root/before-hourly" \
      "$tx_root/candidate-formal" "$tx_root/candidate-hourly" "$tx_root/formal.tmp" "$tx_root/hourly.tmp"
  ) >/dev/null 2>&1
  killed_rc=$?
  set -e
  test "$killed_rc" = 137
  cmp "$tx_root/formal" "$tx_root/candidate-formal"
  cmp "$tx_root/hourly" "$tx_root/before-hourly"
  jq -e '.phase == "formal_moved"' "$tx_root/marker.json" >/dev/null
  recover_env_transaction "$tx_root/marker.json" "$tx_root/formal" "$tx_root/hourly" \
    "$tx_root/before-formal" "$tx_root/before-hourly" "$tx_root/candidate-formal" "$tx_root/candidate-hourly" before
  cmp "$tx_root/formal" "$tx_root/before-formal"
  cmp "$tx_root/hourly" "$tx_root/before-hourly"
  jq -e '.schema == "polyedge.parity_env_transaction.v1" and .phase == "recovered_before"' "$tx_root/marker.json" >/dev/null
  printf '%s\n' ambiguous >"$tx_root/formal"
  if recover_env_transaction "$tx_root/marker.json" "$tx_root/formal" "$tx_root/hourly" \
    "$tx_root/before-formal" "$tx_root/before-hourly" "$tx_root/candidate-formal" "$tx_root/candidate-hourly" before; then exit 1; fi
)

(
  POLYEDGE_TEST_UID=$(id -u) POLYEDGE_TEST_GID=$(id -g) POLYEDGE_TEST_SOURCE_ONLY=1 . "$first"
  staged_fixture=$root/staged.json
  jq -n --arg window "$window" --arg ledger "$ledger" --arg recovery "$fixture_recovery" --arg recovery_sha "$fixture_recovery_sha" \
    --arg rollout "$fixture_rollout" --arg rollout_sha "sha256:$(sha256sum "$fixture_rollout" | cut -d' ' -f1)" \
    --arg settlement "$fixture_settlement" --arg settlement_sha "$fixture_settlement_sha" \
    --arg collector "$collector_sha" --arg validator "$validator_sha" '{
      schemaVersion:1,status:"staged",windowStartUtc:$window,ledger:{path:$ledger},
      recovery:{path:$recovery,sha256:$recovery_sha},settlement:{path:$settlement,sha256:$settlement_sha},
      authorizedDeadLetterBaseline:1311,rollout:{path:$rollout,sha256:$rollout_sha},
      bindings:{collectorSha256:$collector,validatorSha256:$validator},acceptedCleanLiveHours:0,
      completedDailyCycles:0,rebootRecoveryPassed:false,recurrenceEnabled:false,
      azureAuthoritative:true,azureDeletionAllowed:false
    }' >"$staged_fixture"
  validate_staged_receipt "$staged_fixture" "$fixture_recovery" "$fixture_rollout" "$fixture_settlement"
  jq '.bindings.validatorSha256="sha256:0000000000000000000000000000000000000000000000000000000000000000"' \
    "$staged_fixture" >"$root/bad-staged.json"
  if validate_staged_receipt "$root/bad-staged.json" "$fixture_recovery" "$fixture_rollout" "$fixture_settlement"; then exit 1; fi

  export POLYEDGE_PARITY_EXPECTED_FUNDED_REVISION=$revision
  export POLYEDGE_PARITY_FUNDED_ROLLOUT_RECEIPT=$fixture_rollout
  export POLYEDGE_PARITY_EXPECTED_FUNDED_ROLLOUT_RECEIPT_SHA256=sha256:$(sha256sum "$fixture_rollout" | cut -d' ' -f1)
  export POLYEDGE_PARITY_EXPECTED_FUNDED_SERVICE_BUS_DLQ=1311
  export POLYEDGE_PARITY_EXPECTED_FUNDED_IMAGE=$new_image
  export POLYEDGE_PARITY_FUNDED_UID=986 POLYEDGE_PARITY_FUNDED_GID=982
  export POLYEDGE_PARITY_EXPECTED_FUNDED_SESSION_ID=fixture-session
  export POLYEDGE_PARITY_EXPECTED_FUNDED_SESSION_SHA256=sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
  export POLYEDGE_PARITY_EXPECTED_FUNDED_CONFIG_SHA256=sha256:9999999999999999999999999999999999999999999999999999999999999999
  export POLYEDGE_PARITY_EXPECTED_FUNDED_PRODUCER_IMAGE=$producer_image
  export POLYEDGE_PARITY_EXPECTED_FUNDED_PRODUCER_CONFIG_SHA256=sha256:56d8d0573ffbc2f50354100921355244ceedb71e1b28bbf32dea9f0a18b0c87b
  evidence_fixture=$root/evidence.json
  jq -n --arg window "$window" --arg ledger "$ledger" --arg revision "$revision" \
    --arg rollout "$fixture_rollout" --arg rollout_sha "$POLYEDGE_PARITY_EXPECTED_FUNDED_ROLLOUT_RECEIPT_SHA256" \
    --arg namespace "$namespace" --arg queue "$queue" --arg collector "$collector_sha" --arg validator "$validator_sha" \
    --arg funded_image "$POLYEDGE_PARITY_EXPECTED_FUNDED_IMAGE" --arg funded_user "986:982" \
    --arg funded_session "$POLYEDGE_PARITY_EXPECTED_FUNDED_SESSION_ID" \
    --arg funded_session_sha "$POLYEDGE_PARITY_EXPECTED_FUNDED_SESSION_SHA256" \
    --arg funded_config_sha "$POLYEDGE_PARITY_EXPECTED_FUNDED_CONFIG_SHA256" \
    --arg producer_image "$POLYEDGE_PARITY_EXPECTED_FUNDED_PRODUCER_IMAGE" \
    --arg producer_config_sha "$POLYEDGE_PARITY_EXPECTED_FUNDED_PRODUCER_CONFIG_SHA256" \
    --arg producer_env_sha "$producer_env_binding_sha" --arg producer_mount_sha "$producer_mount_binding_sha" '{
      schemaVersion:1,status:"validated",acceptedForParityWindow:true,hourStartUtc:$window,ledgerPath:$ledger,
      services:{fundedSignerEnabled:true,fundedSignerMode:"active",fundedSignerRevision:$revision,
        fundedSignerImage:$funded_image,fundedSignerUser:$funded_user,fundedSessionId:$funded_session,
        fundedSessionManifestSha256:$funded_session_sha,fundedConfigSha256:$funded_config_sha,
        fundedRolloutReceipt:{path:$rollout,sha256:$rollout_sha},fundedServiceBusDlqBaseline:1311,
        parityCollectorSha256:$collector,rebootValidatorSha256:$validator,
        fundedServiceBusRuntime:{namespace:$namespace,queue:$queue,status:"Active",activeMessageCount:0,
          scheduledMessageCount:0,deadLetterMessageCount:1311,expectedDeadLetterMessageCount:1311},
        fundedRuntime:{invocationId:"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",containerId:"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",heartbeatCount:60,alertCount:0,failedClosedCount:0,failedAttempts:0,marketWarmedCount:1,processedMessagesDelta:1,restartCount:0},
        fundedIntentProducerEnabled:true,fundedIntentProducerImage:$producer_image,
        fundedIntentProducerUser:"984:980",fundedIntentProducerConfigSha256:$producer_config_sha,
        fundedIntentProducerRuntime:{invocationId:"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",containerId:("f" * 64),configEnvBindingSha256:$producer_env_sha,
          tokenMountBindingSha256:$producer_mount_sha,continuity:{restartCount:0}}},
      sameInput:{deterministicResultExactMatch:true},azureAuthoritative:true,azureDeletionAllowed:false
    }' >"$evidence_fixture"
  validate_first_hour_evidence "$evidence_fixture"
  for mutation in revision rollout-sha dlq collector validator signer-image signer-user signer-session \
    signer-session-sha signer-config producer-image producer-user producer-config producer-env producer-mount signer-invocation signer-container producer-invocation producer-container failed-attempts market-warmed processed-delta; do
    case "$mutation" in
      revision) filter='.services.fundedSignerRevision = "0000000000000000000000000000000000000000"' ;;
      rollout-sha) filter='.services.fundedRolloutReceipt.sha256 = "sha256:" + ("0" * 64)' ;;
      dlq) filter='.services.fundedServiceBusRuntime.deadLetterMessageCount = 1312' ;;
      collector) filter='.services.parityCollectorSha256 = "sha256:" + ("0" * 64)' ;;
      validator) filter='.services.rebootValidatorSha256 = "sha256:" + ("0" * 64)' ;;
      signer-image) filter='.services.fundedSignerImage = "ghcr.io/fixture/polyedge-venue-probe@sha256:" + ("0" * 64)' ;;
      signer-user) filter='.services.fundedSignerUser = "0:0"' ;;
      signer-session) filter='.services.fundedSessionId = "wrong-session"' ;;
      signer-session-sha) filter='.services.fundedSessionManifestSha256 = "sha256:" + ("0" * 64)' ;;
      signer-config) filter='.services.fundedConfigSha256 = "sha256:" + ("0" * 64)' ;;
      producer-image) filter='.services.fundedIntentProducerImage = "ghcr.io/fixture/polyedge-rust-backend@sha256:" + ("0" * 64)' ;;
      producer-user) filter='.services.fundedIntentProducerUser = "0:0"' ;;
      producer-config) filter='.services.fundedIntentProducerConfigSha256 = "sha256:" + ("0" * 64)' ;;
      producer-env) filter='.services.fundedIntentProducerRuntime.configEnvBindingSha256 = "sha256:" + ("0" * 64)' ;;
      producer-mount) filter='.services.fundedIntentProducerRuntime.tokenMountBindingSha256 = "sha256:" + ("0" * 64)' ;;
      signer-invocation) filter='.services.fundedRuntime.invocationId = "cccccccccccccccccccccccccccccccc"' ;;
      signer-container) filter='.services.fundedRuntime.containerId = ("d" * 64)' ;;
      producer-invocation) filter='.services.fundedIntentProducerRuntime.invocationId = "cccccccccccccccccccccccccccccccc"' ;;
      producer-container) filter='.services.fundedIntentProducerRuntime.containerId = ("c" * 64)' ;;
      failed-attempts) filter='.services.fundedRuntime.failedAttempts = 1' ;;
      market-warmed) filter='.services.fundedRuntime.marketWarmedCount = 0' ;;
      processed-delta) filter='.services.fundedRuntime.processedMessagesDelta = 0' ;;
    esac
    jq "$filter" "$evidence_fixture" >"$root/evidence-$mutation.json"
    if validate_first_hour_evidence "$root/evidence-$mutation.json"; then
      echo "first-hour $mutation drift unexpectedly passed" >&2
      exit 1
    fi
  done
)

printf 'funded parity activation fixture tests passed\n'
