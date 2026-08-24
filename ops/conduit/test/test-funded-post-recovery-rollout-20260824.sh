#!/usr/bin/env bash
set -euo pipefail

repo=$(cd "$(dirname "$0")/../../.." && pwd)
helper=$repo/ops/conduit/bin/polyedge-funded-signer-post-recovery-rollout-20260824
old_image=ghcr.io/aldoapicella/polyedge-venue-probe@sha256:212a34d97075ff74b57681aff65e49913431e6caf2f7c015104102c62837e6f3
new_image=ghcr.io/aldoapicella/polyedge-venue-probe@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc
revision=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
producer_image=ghcr.io/aldoapicella/polyedge-rust-backend@sha256:9eb1b04b01b131bd440bb956c8784e8e493a6e03fe4f03aeb27142284c6fcba8
root=$(mktemp -d)
trap 'rm -rf "$root"' EXIT

make_fixture() {
  case_root=$1
  mkdir -p "$case_root/bin" "$case_root/state" "$case_root/ring/funded-recovery" "$case_root/ring/activation"
  printf '%s\n' "$old_image" >"$case_root/state/signer-image"
  printf '%032d\n' 1 >"$case_root/state/signer-invocation"
  printf '%064d\n' 2 >"$case_root/state/signer-container"
  printf '%032d\n' 3 >"$case_root/state/producer-invocation"
  printf '0\n' >"$case_root/state/producer-active"
  printf '0|||\n' >"$case_root/state/binding"
  printf '0\n' >"$case_root/state/deploy-count"
  printf '0\n' >"$case_root/state/start-count"
  printf '%s\n' '{"status":"Active","activeMessageCount":0,"scheduledMessageCount":0,"deadLetterMessageCount":1311}' >"$case_root/state/queue.json"

  /usr/bin/jq -n --arg old "$old_image" --arg producer "$producer_image" '{
    schema:"polyedge.acknowledged_no_fill_reconciliation.v1",status:"finalized_no_fill",
    cutoverCompletedAtUtc:"2026-08-24T05:30:01Z",
    decision_id:"65b559290100796ef9137179176bf053c8ab5421a1ef3c80bd8fd81676611de7",
    run_id:"funded-direct-20260822235002541-68c5aaa9",
    order_id:"0xd9b1491affbcc71b3876763af5583411b05e08ef8b46a74a88d983dbea1a9319",
    order_submission_attempted:true,recovery_order_submission_attempted:false,
    recovery_grant_consumed:false,recovery_risk_reservation_created:false,
    reconciliation_reason:"acknowledged_evicted_order_no_fill",evidence:{observation_ms:10000},
    recoveryImage:$old,signerImageUnchanged:$old,producerImageUnchanged:$producer,
    reservationEvidence:{blob:"reports/research/venue-probe/risk-reservations/2026-08-22/funded-direct-65b559290100796ef9137179176bf053c8ab5421a1ef3c80bd8fd81676611de7.json",sha256:"sha256:e32a1ec82254c5490f0c25a25d2fed99fe3ab129002610234c704ef688a24a9d"},
    completionEvidence:{blob:"reports/funded/dynamic-quote/sessions/dynamic-quote-funded-2026-08-13-v10/completed/65b559290100796ef9137179176bf053c8ab5421a1ef3c80bd8fd81676611de7.json",sha256:"sha256:fcc2f5861c5364202bd171b4b229e883472980f1db752ab7588bdba1d2f3bed9"},
    summaryEvidence:{blob:"reports/research/venue-probe/runs/2026-08-22/funded-direct-20260822235002541-68c5aaa9/summary.json",sha256:"sha256:a5ea76b54d8b3b9b3fa4ae5d549de6466d0eae1c22d4ca4074b362ec0c725d57"},
    unresolvedReservationsAfter:0,queueActiveMessages:0,queueScheduledMessages:0,queueDeadLetterMessages:1308,
    parityTimerRemainsPaused:true,azureDeletionAllowed:false
  }' >"$case_root/ring/funded-recovery/recovery.json"

  /usr/bin/jq -n \
    --argjson decisions '["5d778fdd0a99ca1cc499f426c55979ac3e4df24b74bce4c4a2b813239893661c","d9f6065521ea9156fd8bbff4273796113c6344f9b8da082355a5a99f0cf2378f","6b8be1369f157f12e2f97bc0cbe28197f64e9437fe723bf76b134d8242618759"]' \
    --argjson orders '["0xbcdf3a4bd0e6c1d8cf61aca501c86ca0ed8d557547929bdb87326c6133ad99d9","0xc655d44197be86d5013939e9366c223076915c80b5df10b3982be77632ecef68","0x8a022015f95b2ec94f09865feaa05d7e95c34176e76299c8208f216d8c0ba5da"]' '{
      schema:"polyedge.funded_dlq_settlement_reconciliation.v1",
      status:"attested_durable_completion_no_duplicate_execution",authorizedDeadLetterBaseline:1311,
      queueBefore:{status:"Active",activeMessageCount:0,scheduledMessageCount:0,deadLetterMessageCount:1311},
      queueAfter:{status:"Active",activeMessageCount:0,scheduledMessageCount:0,deadLetterMessageCount:1311},
      brokerOperations:["peekMessages"],mutationOperations:[],remoteStateMutationPerformed:false,
      safety:{openOrders:0,unresolvedPositions:0,unresolvedRiskReservations:0,durableRiskIndexUnresolvedReservations:0},
      duplicateExecutionEvidence:{matchingDlqMessages:3,immutableAuthorizations:3,immutableCompletions:3,terminalReservations:3,orderLifecycles:3,distinctOrderIds:3},
      parityTimerRemainsPaused:true,azureDeletionAllowed:false,
      durableEvidence:[range(0;3) as $i | {
        message:{messageId:$decisions[$i],decisionId:$decisions[$i]},
        authorization:{decisionId:$decisions[$i],childRunId:("run-"+($i|tostring))},
        completion:{decisionId:$decisions[$i],childRunId:("run-"+($i|tostring))},
        reservation:{runId:("run-"+($i|tostring)),orderId:$orders[$i],state:"finalized_no_fill",reconciliationComplete:true,zeroOpenOrdersConfirmed:true,matchedNotional:0},
        summary:{runId:("run-"+($i|tostring)),decisionId:$decisions[$i],orderId:$orders[$i],reconciliationComplete:true,zeroOpenOrdersConfirmed:true}
      }]
    }' >"$case_root/ring/funded-recovery/settlement.json"

  printf '#!/usr/bin/env bash\nexit 0\n' >"$case_root/bin/recovery"
  printf '#!/usr/bin/env bash\nexit 0\n' >"$case_root/bin/disk-guard"
  printf '[Container]\nImage=%s\n' "$old_image" >"$case_root/signer.container"
  sha256sum "$case_root/signer.container" | cut -d' ' -f1 >"$case_root/state/old-quadlet-sha"

  node -e "require('fs').writeFileSync(process.argv[1], require('fs').readFileSync(0, 'utf8'))" "$case_root/bin/systemctl" <<'SCRIPT'
#!/usr/bin/env bash
set -euo pipefail
case "$1" in
  is-active)
    case "$3" in
      polyedge-funded-signer.service) exit 0 ;;
      polyedge-funded-intent-producer.service) test "$(cat "$FAKE_STATE/producer-active")" = 1 ;;
      polyedge-parity-hourly.timer) exit 3 ;;
    esac ;;
  stop)
    test "$2" = polyedge-funded-intent-producer.service
    printf '0\n' >"$FAKE_STATE/producer-active" ;;
  start)
    test "$2" = polyedge-funded-intent-producer.service
    test "$(cat "$FAKE_STATE/signer-image")" = "$FAKE_NEW_IMAGE"
    test "$(cat "$FAKE_STATE/binding")" = '0|||'
    test -e "$FAKE_STATE/new-signer-proof"
    count=$(cat "$FAKE_STATE/start-count")
    printf '%s\n' "$((count + 1))" >"$FAKE_STATE/start-count"
    printf '1\n' >"$FAKE_STATE/producer-active" ;;
  show)
    case "$4" in
      InvocationID)
        case "$2" in
          polyedge-funded-signer.service) cat "$FAKE_STATE/signer-invocation" ;;
          polyedge-funded-intent-producer.service) cat "$FAKE_STATE/producer-invocation" ;;
        esac ;;
      NRestarts) printf '0\n' ;;
    esac ;;
esac
SCRIPT

  node -e "require('fs').writeFileSync(process.argv[1], require('fs').readFileSync(0, 'utf8'))" "$case_root/bin/podman" <<'SCRIPT'
#!/usr/bin/env bash
set -euo pipefail
if [ "$1" = inspect ]; then
  if [ "$4" = polyedge-funded-signer ]; then
    case "$3" in
      '{{.Config.Image}}|{{.Config.User}}|{{.State.Status}}') printf '%s|986:982|running\n' "$(cat "$FAKE_STATE/signer-image")" ;;
      '{{.Id}}') cat "$FAKE_STATE/signer-container" ;;
    esac
  else
    test "$(cat "$FAKE_STATE/producer-active")" = 1
    printf '%s|984:980|running|healthy\n' "$FAKE_PRODUCER_IMAGE"
  fi
elif [ "$1" = image ]; then
  test "$5" = "$FAKE_NEW_IMAGE"
  printf 'linux/arm64|%s\n' "$FAKE_REVISION"
elif [ "$1" = exec ]; then
  cat "$FAKE_STATE/binding"
elif [ "$1" = container ]; then
  test "$(cat "$FAKE_STATE/producer-active")" = 1
else
  exit 2
fi
SCRIPT

  node -e "require('fs').writeFileSync(process.argv[1], require('fs').readFileSync(0, 'utf8'))" "$case_root/bin/journalctl" <<'SCRIPT'
#!/usr/bin/env bash
set -euo pipefail
if [ "$(cat "$FAKE_STATE/signer-image")" = "$FAKE_NEW_IMAGE" ]; then
  : >"$FAKE_STATE/new-signer-proof"
fi
message=$(/usr/bin/jq -nc '{schema:"polyedge.funded_direct_service.v2",status:"persistent_service_heartbeat",failed_messages:0,executor:{busy:false,user_channel_ready:true,market_channel_ready:true,user_channel_gaps:0,market_channel_gaps:0,user_channel_unparsed:0,market_channel_unparsed:0,reconnect_reconciliation_required:false,safety_snapshot_cache_ready:true,safety_snapshot_cache_age_ms:1,safety_snapshot_open_order_count:0,safety_snapshot_unresolved_position_count:0,safety_snapshot_unresolved_risk_reservation_count:0,safety_snapshot_cache_error:null,risk_reservation_index_ready:true}}')
/usr/bin/jq -nc --arg ts "$(/usr/bin/date -u +%s)000000" --arg inv "$(cat "$FAKE_STATE/signer-invocation")" --arg container "$(cat "$FAKE_STATE/signer-container")" --arg message "$message" '{__REALTIME_TIMESTAMP:$ts,_SYSTEMD_INVOCATION_ID:$inv,CONTAINER_ID_FULL:$container,MESSAGE:$message}'
SCRIPT

  node -e "require('fs').writeFileSync(process.argv[1], require('fs').readFileSync(0, 'utf8'))" "$case_root/bin/runuser" <<'SCRIPT'
#!/usr/bin/env bash
shift 3
exec "$@"
SCRIPT
  node -e "require('fs').writeFileSync(process.argv[1], require('fs').readFileSync(0, 'utf8'))" "$case_root/bin/az" <<'SCRIPT'
#!/usr/bin/env bash
/usr/bin/jq -c '{status:.status,countDetails:{activeMessageCount:.activeMessageCount,scheduledMessageCount:.scheduledMessageCount,deadLetterMessageCount:.deadLetterMessageCount}}' "$FAKE_STATE/queue.json"
SCRIPT
  node -e "require('fs').writeFileSync(process.argv[1], require('fs').readFileSync(0, 'utf8'))" "$case_root/bin/deploy" <<'SCRIPT'
#!/usr/bin/env bash
set -euo pipefail
test "$1" = polyedge-funded-signer
test "$2" = "$FAKE_NEW_IMAGE"
count=$(cat "$FAKE_STATE/deploy-count")
printf '%s\n' "$((count + 1))" >"$FAKE_STATE/deploy-count"
if [ "${FAKE_DEPLOY_UNSAFE:-0}" = 1 ]; then
  printf '1|unresolved.json|unsafe-run|unsafe-order\n' >"$FAKE_STATE/binding"
fi
if [ "${FAKE_DEPLOY_FAIL:-0}" = 1 ]; then exit 1; fi
/usr/bin/sed -i "s|^Image=.*|Image=$FAKE_NEW_IMAGE|" "$FAKE_QUADLET"
printf '%s\n' "$FAKE_NEW_IMAGE" >"$FAKE_STATE/signer-image"
printf '%032d\n' 4 >"$FAKE_STATE/signer-invocation"
printf '%064d\n' 5 >"$FAKE_STATE/signer-container"
if [ "${FAKE_DEPLOY_INTERRUPT:-0}" = 1 ]; then kill -KILL "$PPID"; fi
SCRIPT

  chmod 0700 "$case_root/bin/recovery"
  chmod 0755 "$case_root/bin/disk-guard" "$case_root/bin/systemctl" "$case_root/bin/podman" \
    "$case_root/bin/journalctl" "$case_root/bin/runuser" "$case_root/bin/az" "$case_root/bin/deploy"
  chmod 0600 "$case_root/signer.container"
  chmod 0640 "$case_root/ring/funded-recovery/recovery.json" "$case_root/ring/funded-recovery/settlement.json"
}

run_helper() {
  case_root=$1
  shift
  env FAKE_STATE="$case_root/state" FAKE_NEW_IMAGE="$new_image" FAKE_REVISION="$revision" \
    FAKE_PRODUCER_IMAGE="$producer_image" FAKE_QUADLET="$case_root/signer.container" \
    POLYEDGE_TEST_ALLOW_UNPRIVILEGED=1 \
    POLYEDGE_TEST_RECOVERY="$case_root/ring/funded-recovery/recovery.json" \
    POLYEDGE_TEST_SETTLEMENT="$case_root/ring/funded-recovery/settlement.json" \
    POLYEDGE_TEST_ROLLOUT="$case_root/ring/activation/rollout.json" \
    POLYEDGE_TEST_RECOVERY_SCRIPT="$case_root/bin/recovery" POLYEDGE_TEST_QUADLET="$case_root/signer.container" \
    POLYEDGE_TEST_DEPLOY="$case_root/bin/deploy" POLYEDGE_TEST_DISK_GUARD="$case_root/bin/disk-guard" \
    POLYEDGE_TEST_SYSTEMCTL="$case_root/bin/systemctl" POLYEDGE_TEST_PODMAN="$case_root/bin/podman" \
    POLYEDGE_TEST_JOURNALCTL="$case_root/bin/journalctl" POLYEDGE_TEST_RUNUSER="$case_root/bin/runuser" \
    POLYEDGE_TEST_AZ="$case_root/bin/az" POLYEDGE_TEST_UID="$(id -u)" POLYEDGE_TEST_GID="$(id -g)" \
    POLYEDGE_TEST_RECOVERY_RECEIPT_SHA="$(sha256sum "$case_root/ring/funded-recovery/recovery.json" | cut -d' ' -f1)" \
    POLYEDGE_TEST_SETTLEMENT_RECEIPT_SHA="$(sha256sum "$case_root/ring/funded-recovery/settlement.json" | cut -d' ' -f1)" \
    POLYEDGE_TEST_RECOVERY_SCRIPT_SHA="$(sha256sum "$case_root/bin/recovery" | cut -d' ' -f1)" \
    POLYEDGE_TEST_QUADLET_SHA="$(cat "$case_root/state/old-quadlet-sha")" \
    POLYEDGE_TEST_DEPLOY_SHA="$(sha256sum "$case_root/bin/deploy" | cut -d' ' -f1)" \
    POLYEDGE_TEST_LOCK_FILE="$case_root/utility.lock" POLYEDGE_TEST_WAIT_ATTEMPTS=2 POLYEDGE_TEST_WAIT_SECONDS=0 \
    "$@" "$helper" "$new_image" "$revision"
}

assert_fail_closed() {
  case_root=$1
  test "$(cat "$case_root/state/producer-active")" = 0
  test "$(cat "$case_root/state/start-count")" = 0
  test ! -e "$case_root/ring/activation/rollout.json"
}

chmod 0755 "$helper"

success=$root/success
make_fixture "$success"
run_helper "$success"
pending=$success/ring/activation/20260824T060435Z-funded-signer-rollout.pending.json
test "$(cat "$success/state/producer-active")" = 1
test "$(cat "$success/state/deploy-count")" = 1
test "$(cat "$success/state/start-count")" = 1
test "$(stat -c %a "$pending")" = 640
test "$(stat -c %a "$success/ring/activation/rollout.json")" = 640
/usr/bin/jq -e --arg pending "$pending" --arg pending_sha "sha256:$(sha256sum "$pending" | cut -d' ' -f1)" \
  --arg recovery "$success/ring/funded-recovery/recovery.json" \
  --arg recovery_sha "sha256:$(sha256sum "$success/ring/funded-recovery/recovery.json" | cut -d' ' -f1)" \
  --arg settlement "$success/ring/funded-recovery/settlement.json" \
  --arg settlement_sha "sha256:$(sha256sum "$success/ring/funded-recovery/settlement.json" | cut -d' ' -f1)" '
  .producerWasActive == false and .producerStoppedForDeployment == false and
  .producerStartedAfterSignerProof == true and .producerRestored == true and
  .authorizedDeadLetterBaseline == 1311 and
  .recoveryReceipt == {path:$recovery,sha256:$recovery_sha} and
  .settlementReceipt == {path:$settlement,sha256:$settlement_sha} and
  .pendingRollout == {path:$pending,sha256:$pending_sha} and
  .queue == {status:"Active",activeMessageCount:0,scheduledMessageCount:0,deadLetterMessageCount:1311}' \
  "$success/ring/activation/rollout.json" >/dev/null
run_helper "$success"
test "$(cat "$success/state/deploy-count")" = 1
test "$(cat "$success/state/start-count")" = 1

safe=$root/safe-failure
make_fixture "$safe"
if run_helper "$safe" FAKE_DEPLOY_FAIL=1; then exit 1; fi
assert_fail_closed "$safe"

active=$root/active-failure
make_fixture "$active"
printf '1\n' >"$active/state/producer-active"
if run_helper "$active" FAKE_DEPLOY_FAIL=1; then exit 1; fi
assert_fail_closed "$active"
/usr/bin/jq -e '.producerWasActive == true' "$active/ring/activation/20260824T060435Z-funded-signer-rollout.pending.json" >/dev/null

unsafe=$root/unsafe-failure
make_fixture "$unsafe"
if run_helper "$unsafe" FAKE_DEPLOY_FAIL=1 FAKE_DEPLOY_UNSAFE=1; then exit 1; fi
assert_fail_closed "$unsafe"

interrupted=$root/interrupted-resume
make_fixture "$interrupted"
if run_helper "$interrupted" FAKE_DEPLOY_INTERRUPT=1; then exit 1; fi
interrupted_pending=$interrupted/ring/activation/20260824T060435Z-funded-signer-rollout.pending.json
assert_fail_closed "$interrupted"
test "$(cat "$interrupted/state/deploy-count")" = 1
test -e "$interrupted_pending"
run_helper "$interrupted"
test "$(cat "$interrupted/state/producer-active")" = 1
test "$(cat "$interrupted/state/deploy-count")" = 1
test "$(cat "$interrupted/state/start-count")" = 1

drift=$root/interrupted-drift
make_fixture "$drift"
if run_helper "$drift" FAKE_DEPLOY_INTERRUPT=1; then exit 1; fi
printf '# drift\n' >>"$drift/signer.container"
if run_helper "$drift"; then exit 1; fi
assert_fail_closed "$drift"

tamper_case() {
  name=$1
  filter=$2
  case_root=$root/$name
  make_fixture "$case_root"
  /usr/bin/jq "$filter" "$case_root/ring/funded-recovery/settlement.json" >"$case_root/ring/funded-recovery/settlement.tmp"
  mv "$case_root/ring/funded-recovery/settlement.tmp" "$case_root/ring/funded-recovery/settlement.json"
  chmod 0640 "$case_root/ring/funded-recovery/settlement.json"
  if run_helper "$case_root"; then exit 1; fi
  assert_fail_closed "$case_root"
}

tamper_case wrong-schema '.schema="bad"'
tamper_case wrong-status '.status="bad"'
tamper_case wrong-broker '.brokerOperations += ["completeMessage"]'
tamper_case mutation-list '.mutationOperations=["deadLetterMessage"]'
tamper_case remote-mutation '.remoteStateMutationPerformed=true'
tamper_case exposed '.safety.openOrders=1'
tamper_case wrong-baseline '.authorizedDeadLetterBaseline=1312'
tamper_case receipt-queue-drift '.queueBefore.deadLetterMessageCount=1312'
tamper_case duplicate-decision '.durableEvidence[1].message.decisionId=.durableEvidence[0].message.decisionId'
tamper_case duplicate-order '.durableEvidence[1].reservation.orderId=.durableEvidence[0].reservation.orderId'

wrong_digest=$root/wrong-digest
make_fixture "$wrong_digest"
if run_helper "$wrong_digest" POLYEDGE_TEST_SETTLEMENT_RECEIPT_SHA=0000000000000000000000000000000000000000000000000000000000000000; then exit 1; fi
assert_fail_closed "$wrong_digest"

live_drift=$root/live-queue-drift
make_fixture "$live_drift"
printf '%s\n' '{"status":"Active","activeMessageCount":0,"scheduledMessageCount":0,"deadLetterMessageCount":1312}' >"$live_drift/state/queue.json"
if run_helper "$live_drift"; then exit 1; fi
assert_fail_closed "$live_drift"

printf 'funded post-recovery rollout tests passed\n'
