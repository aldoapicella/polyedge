#!/usr/bin/env bash
set -euo pipefail

repo=$(cd "$(dirname "$0")/../../.." 2>/dev/null && pwd || true)
helper=${POLYEDGE_TEST_HELPER:-$repo/ops/conduit/bin/polyedge-funded-dlq-settlement-reconcile-20260824}
root=$(/usr/bin/mktemp -d)
trap '/usr/bin/rm -rf "$root"' EXIT HUP INT TERM
uid=$(/usr/bin/id -u); gid=$(/usr/bin/id -g)
invocation=11111111111111111111111111111111
container=2222222222222222222222222222222222222222222222222222222222222222
image=ghcr.io/aldoapicella/polyedge-venue-probe@sha256:212a34d97075ff74b57681aff65e49913431e6caf2f7c015104102c62837e6f3
messages='["5d778fdd0a99ca1cc499f426c55979ac3e4df24b74bce4c4a2b813239893661c","d9f6065521ea9156fd8bbff4273796113c6344f9b8da082355a5a99f0cf2378f","6b8be1369f157f12e2f97bc0cbe28197f64e9437fe723bf76b134d8242618759"]'

expected_evidence() {
  /usr/bin/awk '
    /^readonly expected_evidence='"'"'\[/ {copy=1; sub(/^readonly expected_evidence='"'"'/, "")}
    copy {if ($0 == "]'"'"'") {print "]"; exit} print}
  ' "$helper"
}

make_fixture() {
  local case_root=$1 expected
  expected=$(expected_evidence)
  /usr/bin/jq -e --argjson ids "$messages" '
    length == 3 and [.[]|.message.messageId] == $ids and
    all(.[]; .message.deliveryCount == 1 and .authorization.singleUse == true and
      .completion.status == "child_completed" and .reservation.state == "finalized_no_fill" and
      .reservation.matchedNotional == 0 and .summary.orderSubmitted == true and
      .summary.reconciliationComplete == true and .summary.zeroOpenOrdersConfirmed == true)' <<<"$expected" >/dev/null
  /usr/bin/mkdir -p "$case_root/bin" "$case_root/state" "$case_root/runtime" "$case_root/receipts"
  /usr/bin/chmod 0750 "$case_root/receipts"
  printf '0\n' >"$case_root/state/az-calls"
  /usr/bin/jq -n --argjson evidence "$expected" '{brokerOperations:["peekMessages"],distinctOrderIds:3,
    dlq:{scanned:1311,matchingMessages:3},evidence:$evidence,executionEvidenceCount:3,
    mutationOperations:[],orderLifecycleMatches:3,riskIndexUnresolvedCount:0}' >"$case_root/state/probe.json"

  /usr/bin/tee "$case_root/bin/systemctl" >/dev/null <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
case "$1:$2" in
  is-active:--quiet)
    case "$3" in polyedge-funded-signer.service) exit 0 ;; polyedge-parity-hourly.timer) exit 3 ;; *) exit 1 ;; esac ;;
  show:polyedge-funded-signer.service) printf '%s\n' "$FAKE_INVOCATION" ;;
  *) exit 1 ;;
esac
EOF
  /usr/bin/tee "$case_root/bin/podman" >/dev/null <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
case "$1" in
  inspect) printf '%s|986:982|running|%s\n' "$FAKE_IMAGE" "$FAKE_CONTAINER" ;;
  exec) [ "${FAKE_MISSING_DURABLE:-0}" = 0 ] || exit 44; /usr/bin/cat "$FAKE_PROBE" ;;
  *) exit 1 ;;
esac
EOF
  /usr/bin/tee "$case_root/bin/journalctl" >/dev/null <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
if printf '%s\n' "$*" | /usr/bin/grep -Fq -- '-n 500'; then
  open=${FAKE_OPEN_ORDERS:-0}
  message=$(/usr/bin/jq -nc --argjson open "$open" '{schema:"polyedge.funded_direct_service.v2",status:"persistent_service_heartbeat",executor:{busy:false,safety_snapshot_cache_ready:true,safety_snapshot_cache_age_ms:1,safety_snapshot_open_order_count:$open,safety_snapshot_unresolved_position_count:0,safety_snapshot_unresolved_risk_reservation_count:0,safety_snapshot_cache_error:null,risk_reservation_index_ready:true}}')
  /usr/bin/jq -nc --arg ts "$(/usr/bin/date -u +%s)000000" --arg invocation "$FAKE_INVOCATION" --arg container "$FAKE_CONTAINER" --arg message "$message" '{__REALTIME_TIMESTAMP:$ts,_SYSTEMD_INVOCATION_ID:$invocation,CONTAINER_ID_FULL:$container,MESSAGE:$message}'
else
  /usr/bin/jq -r '.[]' <<<"$FAKE_MESSAGES" | while read -r id; do
    lost=$(/usr/bin/jq -nc --arg id "$id" '{schema:"polyedge.funded_direct_service.v2",status:"persistent_message_settlement_lost_after_durable_completion",message_id:$id,delivery_count:0,worker_status:"persistent_intent_completed",order_submission_attempted:true,broker_redelivery_expected:true,error:"AggregateError"}')
    dead=$(/usr/bin/jq -nc --arg id "$id" '{schema:"polyedge.funded_direct_service.v2",status:"persistent_message_failed_closed",message_id:$id,delivery_count:1,error:"fail closed: funded intent handoff has insufficient remaining TTL",terminal_failure:true,settlement_action:"dead_lettered",settlement_succeeded:true,broker_redelivery_expected:false}')
    /usr/bin/jq -nc --arg invocation "$FAKE_INVOCATION" --arg container "$FAKE_CONTAINER" --arg message "$lost" '{_SYSTEMD_INVOCATION_ID:$invocation,CONTAINER_ID_FULL:$container,MESSAGE:$message}'
    /usr/bin/jq -nc --arg invocation "$FAKE_INVOCATION" --arg container "$FAKE_CONTAINER" --arg message "$dead" '{_SYSTEMD_INVOCATION_ID:$invocation,CONTAINER_ID_FULL:$container,MESSAGE:$message}'
  done
fi
EOF
  /usr/bin/tee "$case_root/bin/runuser" >/dev/null <<'EOF'
#!/usr/bin/env bash
while [ "$1" != -- ]; do shift; done
shift
exec "$@"
EOF
  /usr/bin/tee "$case_root/bin/az" >/dev/null <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
calls=$(/usr/bin/cat "$FAKE_STATE/az-calls"); printf '%s\n' "$((calls + 1))" >"$FAKE_STATE/az-calls"
dlq=1311
if [ "${FAKE_QUEUE_DRIFT:-0}" = 1 ] && [ "$calls" -ge 1 ]; then dlq=1312; fi
/usr/bin/jq -nc --argjson active "${FAKE_ACTIVE:-0}" --argjson dlq "$dlq" '{status:"Active",countDetails:{activeMessageCount:$active,scheduledMessageCount:0,deadLetterMessageCount:$dlq}}'
EOF
  /usr/bin/tee "$case_root/bin/disk-guard" >/dev/null <<'EOF'
#!/usr/bin/env bash
[ "$1" = --assert-headroom ]
EOF
  /usr/bin/chmod 0755 "$case_root/bin/"*
}

run_helper() {
  local case_root=$1
  shift
  env FAKE_STATE="$case_root/state" FAKE_PROBE="$case_root/state/probe.json" FAKE_MESSAGES="$messages" \
    FAKE_INVOCATION="$invocation" FAKE_CONTAINER="$container" FAKE_IMAGE="$image" "$@" \
    POLYEDGE_TEST_ALLOW_UNPRIVILEGED=1 POLYEDGE_TEST_RECEIPT_ROOT="$case_root/receipts" \
    POLYEDGE_TEST_RECEIPT="$case_root/receipts/receipt.json" POLYEDGE_TEST_LOCK_FILE="$case_root/lock" \
    POLYEDGE_TEST_RUNTIME_DIR="$case_root/runtime" POLYEDGE_TEST_PODMAN="$case_root/bin/podman" \
    POLYEDGE_TEST_SYSTEMCTL="$case_root/bin/systemctl" POLYEDGE_TEST_JOURNALCTL="$case_root/bin/journalctl" \
    POLYEDGE_TEST_RUNUSER="$case_root/bin/runuser" POLYEDGE_TEST_AZ="$case_root/bin/az" \
    POLYEDGE_TEST_DISK_GUARD="$case_root/bin/disk-guard" POLYEDGE_TEST_UID="$uid" POLYEDGE_TEST_GID="$gid" "$helper"
}

/usr/bin/chmod 0755 "$helper"
! /usr/bin/grep -Eq '\.(receiveMessages|completeMessage|abandonMessage|deadLetterMessage|sendMessages|scheduleMessages|deleteBlob|uploadData)\(' "$helper"

success=$root/success; make_fixture "$success"; run_helper "$success"
[ "$(/usr/bin/stat -c %a "$success/receipts/receipt.json")" = 640 ]
/usr/bin/jq -e --argjson ids "$messages" '.status=="attested_durable_completion_no_duplicate_execution" and
  .messageIds==$ids and .authorizedDeadLetterBaseline==1311 and (.durableEvidence|length)==3 and
  .duplicateExecutionEvidence.distinctOrderIds==3 and .remoteStateMutationPerformed==false and
  .azureDeletionAllowed==false' "$success/receipts/receipt.json" >/dev/null
run_helper "$success"

tamper=$root/tamper; make_fixture "$tamper"; run_helper "$tamper"
/usr/bin/jq '.durableEvidence[1].reservation.sha256="sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"' "$tamper/receipts/receipt.json" >"$tamper/tmp"
/usr/bin/mv "$tamper/tmp" "$tamper/receipts/receipt.json"; /usr/bin/chmod 0640 "$tamper/receipts/receipt.json"
if run_helper "$tamper"; then echo 'tampered receipt unexpectedly passed' >&2; exit 1; fi

missing=$root/missing; make_fixture "$missing"
if run_helper "$missing" FAKE_MISSING_DURABLE=1; then echo 'missing durable evidence unexpectedly passed' >&2; exit 1; fi

duplicate=$root/duplicate; make_fixture "$duplicate"
/usr/bin/jq '.executionEvidenceCount=4' "$duplicate/state/probe.json" >"$duplicate/tmp"; /usr/bin/mv "$duplicate/tmp" "$duplicate/state/probe.json"
if run_helper "$duplicate"; then echo 'duplicate execution evidence unexpectedly passed' >&2; exit 1; fi

artifact_tamper=$root/artifact-tamper; make_fixture "$artifact_tamper"
/usr/bin/jq '.evidence[2].completion.sha256="sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"' "$artifact_tamper/state/probe.json" >"$artifact_tamper/tmp"; /usr/bin/mv "$artifact_tamper/tmp" "$artifact_tamper/state/probe.json"
if run_helper "$artifact_tamper"; then echo 'tampered durable artifact unexpectedly passed' >&2; exit 1; fi

exposure=$root/exposure; make_fixture "$exposure"
if run_helper "$exposure" FAKE_OPEN_ORDERS=1; then echo 'open exposure unexpectedly passed' >&2; exit 1; fi

drift=$root/drift; make_fixture "$drift"
if run_helper "$drift" FAKE_QUEUE_DRIFT=1; then echo 'queue drift unexpectedly passed' >&2; exit 1; fi

mutation=$root/mutation; make_fixture "$mutation"
/usr/bin/jq '.brokerOperations=["receiveMessages"] | .mutationOperations=["deadLetterMessage"]' "$mutation/state/probe.json" >"$mutation/tmp"; /usr/bin/mv "$mutation/tmp" "$mutation/state/probe.json"
if run_helper "$mutation"; then echo 'non-peek broker operation unexpectedly passed' >&2; exit 1; fi

printf 'funded DLQ settlement reconciliation tests passed\n'
