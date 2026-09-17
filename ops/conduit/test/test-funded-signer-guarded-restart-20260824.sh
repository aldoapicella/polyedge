#!/usr/bin/env bash
set -euo pipefail

repo=$(cd "$(dirname "$0")/../../.." && pwd)
helper=$repo/ops/conduit/bin/polyedge-funded-signer-guarded-restart-20260824
signer_image=ghcr.io/aldoapicella/polyedge-venue-probe@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc
prior_signer_image=ghcr.io/aldoapicella/polyedge-venue-probe@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
signer_revision=eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
producer_image=ghcr.io/aldoapicella/polyedge-rust-backend@sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd
approved_condition=0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
root=$(mktemp -d)
trap 'rm -rf "$root"' EXIT
grep -F '"_SYSTEMD_INVOCATION_ID=$invocation_id" "CONTAINER_ID_FULL=$container" -o json --all --no-pager' "$helper" >/dev/null

fixture() {
  local d=$1
  mkdir -p "$d/bin" "$d/state" "$d/ring/activation" "$d/tokens" "$d/rollback"
  printf '1\n' >"$d/state/producer-active"; printf 'before\n' >"$d/state/phase"; printf '0|||\n' >"$d/state/binding"
  printf '1\n' >"$d/state/signer-active"
  printf '%032d\n' 1 >"$d/state/invocation"; printf '%064d\n' 2 >"$d/state/container"
  printf '%s\n' '{"status":"Active","active":0,"scheduled":0,"dlq":1311}' >"$d/state/queue"
  printf "Image=%s\n" "$signer_image" >"$d/quadlet"; chmod 600 "$d/quadlet"
  printf token >"$d/tokens/token"; chmod 600 "$d/tokens/token"
  printf '%s\n' '{"status":"validated","azureDeletionAuthorized":false}' >"$d/prior.json"
  printf '%s\n' '{"status":"succeeded","azureDeletionAuthorized":false}' >"$d/lifecycle.json"
  /usr/bin/jq -n --arg finished "$(date -u +%Y-%m-%dT%H:%M:%S).399Z" '{schema_version:1,status:"nothing_to_redeem",dry_run:true,redemption_submitted:false,zero_open_orders_confirmed:true,finished_ts:$finished,portfolio:{redeemable_winner_count:0},selection:{selected_gross_payout:0,selected:[]}}' >"$d/preflight.json"
  /usr/bin/jq -n --arg created "$(date -u +%Y-%m-%dT%H:%M:%S).399Z" '{schema:"polyedge.funded_stopped_binding.v1",status:"verified_zero",createdAtUtc:$created,readOnly:true,unresolvedRiskReservationCount:0,records:[]}' >"$d/binding-proof.json"
  chmod 640 "$d/prior.json" "$d/lifecycle.json" "$d/preflight.json" "$d/binding-proof.json"
  cat >"$d/bin/systemctl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
case "$1" in
 is-active) case "$3" in polyedge-funded-signer.service) test "$(cat "$FAKE/state/signer-active")" = 1;; polyedge-funded-intent-producer.service) test "$(cat "$FAKE/state/producer-active")" = 1;; polyedge-parity-hourly.timer) exit 3;; esac ;;
 stop) case "$2" in polyedge-funded-signer.service) printf '0\n' >"$FAKE/state/signer-active";; polyedge-funded-intent-producer.service) printf '0\n' >"$FAKE/state/producer-active"; if [ "${FAKE_RUNTIME_CHANGE:-0}" = 1 ]; then printf '%032d\n' 9 >"$FAKE/state/invocation"; printf '%064d\n' 9 >"$FAKE/state/container"; fi;; esac ;;
 start) case "$2" in polyedge-funded-signer.service) printf '1\n' >"$FAKE/state/signer-active"; printf 'after\n' >"$FAKE/state/phase"; printf '%032d\n' 3 >"$FAKE/state/invocation"; printf '%064d\n' 4 >"$FAKE/state/container";; polyedge-funded-intent-producer.service) printf '1\n' >"$FAKE/state/producer-active";; esac ;;
 restart) test "$2" = polyedge-funded-signer.service; printf 'after\n' >"$FAKE/state/phase"; printf '%032d\n' 3 >"$FAKE/state/invocation"; printf '%064d\n' 4 >"$FAKE/state/container" ;;
 daemon-reload) : ;;
 show) case "$4" in InvocationID) cat "$FAKE/state/invocation";; NRestarts) printf '0\n';; esac ;;
esac
EOF
  cat >"$d/bin/podman" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
case "$1" in
 pull) if [ "${FAKE_EVIDENCE_CHANGE:-0}" = 1 ]; then printf '\n' >>"$FAKE/recovery.json"; fi ;;
 image) printf "%s\n" "linux/arm64|$FAKE_SIGNER_REVISION" ;;
 inspect)
  image=$FAKE_SIGNER_IMAGE; [ "$(cat "$FAKE/state/phase")" = after ] || image=${FAKE_PRIOR_SIGNER_IMAGE:-$FAKE_SIGNER_IMAGE}
  if [ "$4" = polyedge-funded-signer ]; then case "$3" in '{{.Config.Image}}|{{.Config.User}}|{{.State.Status}}') printf '%s|%s|running\n' "$image" "$FAKE_USER";; '{{.Id}}') cat "$FAKE/state/container";; esac
  else test "$(cat "$FAKE/state/producer-active")" = 1; health=${FAKE_PRODUCER_HEALTH:-healthy}
    if [ "${FAKE_PRODUCER_HEALTH_FIRST:-}" = starting ] && [ ! -e "$FAKE/state/producer-health-seen" ]; then touch "$FAKE/state/producer-health-seen"; health=starting; fi
    printf '%s|%s|running|%s\n' "$FAKE_PRODUCER_IMAGE" "$FAKE_USER" "$health"
  fi ;;
 exec)
  if [ "$2" = -i ]; then
    test "$3:$4:$5:$6:$7:$8:$9" = '--workdir:/app:polyedge-funded-signer:node:--input-type=module:-:--collect'
    test "${10}:${11}:${12}" = "$FAKE_APPROVED_CONDITION:$FAKE_SIGNER_IMAGE:$FAKE_SIGNER_REVISION"
    cat >/dev/null
    mutation='.'
    if [ "${FAKE_PREFLIGHT_MUTATE_AFTER_STOP:-0}" != 1 ] || [ "$(cat "$FAKE/state/producer-active")" = 0 ]; then mutation=${FAKE_PREFLIGHT_MUTATION:-.}; fi
    jq "$mutation" "$FAKE/approved-preflight.json"
  else [[ "${6:-}" == *loadCampaignUnresolvedRiskReservationRecords* ]]; cat "$FAKE/state/binding"; fi ;;
esac
EOF
  cat >"$d/bin/journalctl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
attempts=0; failed_messages=0
if [ "$(cat "$FAKE/state/phase")" = before ]; then attempts=${FAKE_FAILED_ATTEMPTS:-0}; failed_messages=${FAKE_FAILED_MESSAGES:-0}; fi
[ "${FAKE_BAD_POST:-0}" = 1 ] && [ "$(cat "$FAKE/state/phase")" = after ] && attempts=$((attempts + 1))
redemption_failures=0; [ "${FAKE_REPAIR_PRE:-0}" != 1 ] || [ "$(cat "$FAKE/state/phase")" = after ] || redemption_failures=2
partial=false; [ "${FAKE_PRESTART_PARTIAL:-0}" != 1 ] || [ "$(cat "$FAKE/state/producer-active")" = 1 ] || partial=true
if [ "${FAKE_COLD_AFTER_REPAIR:-0}" = 1 ] && [ "$(cat "$FAKE/state/phase")" = after ] && [ "$(cat "$FAKE/state/producer-active")" = 0 ]; then partial=true; fi
message=$(/usr/bin/jq -nc --argjson attempts "$attempts" --argjson failed_messages "$failed_messages" --argjson redemption_failures "$redemption_failures" --argjson partial "$partial" '{schema:"polyedge.funded_direct_service.v2",status:"persistent_service_heartbeat",failed_attempts:$attempts,failed_messages:$failed_messages,redemption_failures:$redemption_failures,processed_messages:0,executor:{busy:false,user_channel_ready:true,market_channel_ready:($partial|not),user_channel_gaps:0,market_channel_gaps:0,user_channel_unparsed:0,market_channel_unparsed:0,reconnect_reconciliation_required:false,safety_snapshot_cache_ready:($partial|not),safety_snapshot_cache_age_ms:(if $partial then null else 1 end),safety_snapshot_open_order_count:(if $partial then null else 0 end),safety_snapshot_unresolved_position_count:(if $partial then null else 0 end),safety_snapshot_unresolved_risk_reservation_count:(if $partial then null else 0 end),safety_snapshot_cache_error:null,risk_reservation_index_ready:true}}')
started=$(/usr/bin/jq -nc --argjson enabled "${FAKE_AUTO_REDEMPTION_ENABLED:-true}" '{schema:"polyedge.funded_direct_service.v2",status:"persistent_service_started",automatic_redemption_enabled:$enabled}')
ts="$(( $(date -u +%s) - 1 ))000000"
if [ "${FAKE_REPAIR_PRE:-0}" = 1 ] && [ "$(cat "$FAKE/state/phase")" = before ]; then
  alert=$(/usr/bin/jq -nc '{schema:"polyedge.funded_direct_alert.v1",status:"known_repair_trigger"}')
  /usr/bin/jq -nc --arg ts "$ts" --arg inv "$(cat "$FAKE/state/invocation")" --arg container "$(cat "$FAKE/state/container")" --arg message "$alert" '{__REALTIME_TIMESTAMP:$ts,_SYSTEMD_INVOCATION_ID:$inv,CONTAINER_ID_FULL:$container,MESSAGE:$message}'
fi
if [ "${FAKE_LATE_ALERT:-0}" = 1 ] && [ "$(cat "$FAKE/state/phase")" = before ]; then
  /usr/bin/jq -nc --arg ts "$ts" --arg inv "$(cat "$FAKE/state/invocation")" --arg container "$(cat "$FAKE/state/container")" '{__REALTIME_TIMESTAMP:$ts,_SYSTEMD_INVOCATION_ID:$inv,CONTAINER_ID_FULL:$container,MESSAGE:"{\"schema\":\"polyedge.funded_direct_alert.v1\",\"status\":\"later_failure\"}"}'
fi
/usr/bin/jq -nc --arg ts "$ts" --arg inv "$(cat "$FAKE/state/invocation")" --arg container "$(cat "$FAKE/state/container")" --arg message "$started" '{__REALTIME_TIMESTAMP:$ts,_SYSTEMD_INVOCATION_ID:$inv,CONTAINER_ID_FULL:$container,MESSAGE:$message}'
/usr/bin/jq -nc --arg ts "$ts" --arg inv "$(cat "$FAKE/state/invocation")" --arg container "$(cat "$FAKE/state/container")" --arg message "$message" '{__REALTIME_TIMESTAMP:$ts,_SYSTEMD_INVOCATION_ID:$inv,CONTAINER_ID_FULL:$container,MESSAGE:$message}'
if [ "${FAKE_AUTOMATIC_REDEMPTION:-0}" = 1 ] && [ "$(cat "$FAKE/state/phase")" = after ]; then
  summary=$(/usr/bin/jq -nc --arg condition "$FAKE_APPROVED_CONDITION" --argjson payout "${FAKE_AUTOMATIC_PAYOUT:-5}" --arg finished "$(date -u +%Y-%m-%dT%H:%M:%S).123Z" '{schema:"polyedge.funded_redemption_service.v1",status:"redemption_worker_summary",redemption:{schema_version:1,run_id:"venue-redemption-20260826190000000-abcdef12",status:"redeemed_and_verified",finished_ts:$finished,redemption_submitted:true,transaction_id:"automatic-test",transaction_hash:("0x" + ("f" * 64)),zero_open_orders_confirmed:true,selection:{selected_gross_payout:$payout,selected:[{condition_id:$condition,gross_payout:$payout}]},internal_settlement_blobs:["reports/funded/internal-settlement.json"]}}')
  cycle=$(/usr/bin/jq -nc '{schema:"polyedge.funded_redemption_service.v1",status:"automatic_redemption_cycle_completed",redemption_status:"redeemed_and_verified",redemption_submitted:true}')
  first=${summary:0:127}; second=${summary:127}
  /usr/bin/jq -nc --arg ts "$ts" --arg inv "$(cat "$FAKE/state/invocation")" --arg container "$(cat "$FAKE/state/container")" --arg message "$first" '{__REALTIME_TIMESTAMP:$ts,_SYSTEMD_INVOCATION_ID:$inv,CONTAINER_ID_FULL:$container,MESSAGE:$message}'
  /usr/bin/jq -nc --arg ts "$ts" --arg inv "$(cat "$FAKE/state/invocation")" --arg container "$(cat "$FAKE/state/container")" --arg message "$second" '{__REALTIME_TIMESTAMP:$ts,_SYSTEMD_INVOCATION_ID:$inv,CONTAINER_ID_FULL:$container,MESSAGE:$message}'
  /usr/bin/jq -nc --arg ts "$ts" --arg inv "$(cat "$FAKE/state/invocation")" --arg container "$(cat "$FAKE/state/container")" --arg message "$cycle" '{__REALTIME_TIMESTAMP:$ts,_SYSTEMD_INVOCATION_ID:$inv,CONTAINER_ID_FULL:$container,MESSAGE:$message}'
fi
EOF
  cat >"$d/bin/runuser" <<'EOF'
#!/usr/bin/env bash
shift 3; exec "$@"
EOF
  cat >"$d/bin/az" <<'EOF'
#!/usr/bin/env bash
/usr/bin/jq -c '{status:.status,countDetails:{activeMessageCount:.active,scheduledMessageCount:.scheduled,deadLetterMessageCount:.dlq}}' "$FAKE/state/queue"
EOF
  cat >"$d/bin/deploy" <<"EOF"
#!/usr/bin/env bash
set -euo pipefail
test "$1" = polyedge-funded-signer
[ "$(cat "$FAKE/state/signer-active")" = 1 ] || [ "${POLYEDGE_DEPLOY_LEAVE_STOPPED:-}" = true ]
backup="$POLYEDGE_ROLLBACK_DIR/fixture-polyedge-funded-signer.container"
cp -p "$FAKE/quadlet" "$backup"
sed -i "s|^Image=.*|Image=$2|" "$FAKE/quadlet"
printf "after\n" >"$FAKE/state/phase"; printf "%032d\n" 3 >"$FAKE/state/invocation"; printf "%064d\n" 4 >"$FAKE/state/container"
printf "deployed polyedge-funded-signer; rollback copy: %s\n" "$backup"
EOF
  printf '#!/usr/bin/env bash\nexit 0\n' >"$d/bin/disk"; chmod 755 "$d/bin/"*
}

bind_repair_evidence() {
  local d=$1 invocation container
  invocation=$(cat "$d/state/invocation"); container=$(cat "$d/state/container")
  /usr/bin/jq -n --arg invocation "$invocation" --arg container "$container" --arg image "$prior_signer_image" "{status:\"validated\",azureDeletionAuthorized:false,signer:{newInvocationId:\$invocation,newContainerId:\$container,image:\$image}}" >"$d/prior.json"
  /usr/bin/jq -n --arg invocation "$invocation" --arg container "$container" --arg image "$prior_signer_image" "{schema:\"polyedge.funded_signer_post_redemption_attestation.v1\",status:\"attested\",azureDeletionAllowed:false,runtime:{signer:{invocationId:\$invocation,containerId:\$container,image:\$image}}}" >"$d/lifecycle.json"
  chmod 640 "$d/prior.json" "$d/lifecycle.json"
  printf "Image=%s\n" "$prior_signer_image" >"$d/quadlet"; chmod 600 "$d/quadlet"
}


automatic_preflight() {
  local d=$1 condition=$2
  /usr/bin/jq -n --arg finished "$(date -u +%Y-%m-%dT%H:%M:%S).399Z" --arg condition "$condition" '{schema_version:1,status:"redemption_ready_no_transaction",dry_run:true,redemption_submitted:false,zero_open_orders_confirmed:true,finished_ts:$finished,portfolio:{redeemable_winner_count:1},selection:{available_winner_conditions:1,skipped_winner_conditions:0,selected_gross_payout:5,selected:[{condition_id:$condition,gross_payout:5}]}}' >"$d/preflight.json"
  chmod 640 "$d/preflight.json"
}
run() {
  local d=$1; shift
  env FAKE="$d" FAKE_SIGNER_IMAGE="$signer_image" FAKE_SIGNER_REVISION="$signer_revision" FAKE_USER="$(id -u):$(id -g)" FAKE_PRODUCER_IMAGE="$producer_image" \
    POLYEDGE_GUARDED_RESTART_PRIOR_RECEIPT="$d/prior.json" POLYEDGE_GUARDED_RESTART_PRIOR_RECEIPT_SHA256="$(sha256sum "$d/prior.json" | cut -d' ' -f1)" \
    POLYEDGE_GUARDED_RESTART_LIFECYCLE_EVIDENCE="$d/lifecycle.json" POLYEDGE_GUARDED_RESTART_LIFECYCLE_EVIDENCE_SHA256="$(sha256sum "$d/lifecycle.json" | cut -d' ' -f1)" \
    POLYEDGE_GUARDED_RESTART_SIGNER_IMAGE="$signer_image" POLYEDGE_GUARDED_RESTART_SIGNER_REVISION="$signer_revision" POLYEDGE_GUARDED_RESTART_SIGNER_USER="$(id -u):$(id -g)" \
    POLYEDGE_GUARDED_RESTART_PRODUCER_IMAGE="$producer_image" POLYEDGE_GUARDED_RESTART_PRODUCER_USER="$(id -u):$(id -g)" \
    POLYEDGE_GUARDED_RESTART_RECEIPT="$d/ring/activation/receipt.json" POLYEDGE_GUARDED_RESTART_TOKEN_FILE="$d/tokens/token" \
    POLYEDGE_GUARDED_RESTART_PAUSE_FILE="$d/pause" POLYEDGE_GUARDED_RESTART_LOCK_FILE="$d/utility.lock" \
    POLYEDGE_GUARDED_RESTART_DISK_GUARD="$d/bin/disk" POLYEDGE_GUARDED_RESTART_SYSTEMCTL="$d/bin/systemctl" \
    POLYEDGE_GUARDED_RESTART_PODMAN="$d/bin/podman" POLYEDGE_GUARDED_RESTART_JOURNALCTL="$d/bin/journalctl" \
    POLYEDGE_GUARDED_RESTART_RUNUSER="$d/bin/runuser" POLYEDGE_GUARDED_RESTART_AZ="$d/bin/az" \
    POLYEDGE_GUARDED_RESTART_UID="$(id -u)" POLYEDGE_GUARDED_RESTART_GID="$(id -g)" \
    POLYEDGE_GUARDED_RESTART_WAIT_ATTEMPTS=1 POLYEDGE_GUARDED_RESTART_WAIT_SECONDS=0 "$@" "$helper"
}

ok=$root/ok; fixture "$ok"; chmod 755 "$helper"
run "$ok"
test "$(cat "$ok/state/producer-active")" = 1
jq -e '.status == "validated" and .signer.oldInvocationId != .signer.newInvocationId and .queue.existingDlqPreserved == true and .azureDeletionAuthorized == false and (.producer.invocationId | test("^[0-9a-f]{32}$")) and .disk.minAvailableBytes == 16106127360' "$ok/ring/activation/receipt.json" >/dev/null

repair=$root/repair; fixture "$repair"; bind_repair_evidence "$repair"
run "$repair" FAKE_PRIOR_SIGNER_IMAGE="$prior_signer_image" FAKE_REPAIR_PRE=1 FAKE_FAILED_MESSAGES=1 FAKE_FAILED_ATTEMPTS=3 POLYEDGE_GUARDED_RESTART_PRIOR_SIGNER_IMAGE="$prior_signer_image" POLYEDGE_GUARDED_RESTART_REPAIR_MODE=true POLYEDGE_GUARDED_RESTART_REPAIR_FAILED_MESSAGES=1 POLYEDGE_GUARDED_RESTART_REPAIR_FAILED_ATTEMPTS=3 POLYEDGE_GUARDED_RESTART_DEPLOY="$repair/bin/deploy" POLYEDGE_GUARDED_RESTART_QUADLET="$repair/quadlet" POLYEDGE_GUARDED_RESTART_ROLLBACK_DIR="$repair/rollback"
test "$(cat "$repair/state/producer-active")" = 1; grep -Fx "Image=$signer_image" "$repair/quadlet" >/dev/null
jq -e --arg prior "$prior_signer_image" --arg image "$signer_image" --arg revision "$signer_revision" '.status == "validated" and .startMode == "repair_rollout" and .signer.repairMode == true and .signer.priorImage == $prior and .signer.image == $image and .signer.revision == $revision and .signer.priorFailedMessages == 1 and .signer.priorFailedAttempts == 3 and (.signer.rollbackCopy | endswith("fixture-polyedge-funded-signer.container")) and .producer.stoppedForRestart == true' "$repair/ring/activation/receipt.json" >/dev/null

repair_bad=$root/repair-bad; fixture "$repair_bad"; bind_repair_evidence "$repair_bad"
if run "$repair_bad" FAKE_PRIOR_SIGNER_IMAGE="$prior_signer_image" FAKE_REPAIR_PRE=1 FAKE_FAILED_MESSAGES=1 FAKE_FAILED_ATTEMPTS=3 FAKE_BAD_POST=1 POLYEDGE_GUARDED_RESTART_PRIOR_SIGNER_IMAGE="$prior_signer_image" POLYEDGE_GUARDED_RESTART_REPAIR_MODE=true POLYEDGE_GUARDED_RESTART_REPAIR_FAILED_MESSAGES=1 POLYEDGE_GUARDED_RESTART_REPAIR_FAILED_ATTEMPTS=3 POLYEDGE_GUARDED_RESTART_DEPLOY="$repair_bad/bin/deploy" POLYEDGE_GUARDED_RESTART_QUADLET="$repair_bad/quadlet" POLYEDGE_GUARDED_RESTART_ROLLBACK_DIR="$repair_bad/rollback"; then exit 1; fi
test "$(cat "$repair_bad/state/producer-active")" = 0; test "$(cat "$repair_bad/state/signer-active")" = 0; grep -Fx "Image=$prior_signer_image" "$repair_bad/quadlet" >/dev/null; test ! -e "$repair_bad/ring/activation/receipt.json"

bad=$root/bad; fixture "$bad"
if run "$bad" FAKE_BAD_POST=1; then exit 1; fi
test "$(cat "$bad/state/producer-active")" = 0
test ! -e "$bad/ring/activation/receipt.json"
bad_binding=$root/bad-binding; fixture "$bad_binding"; echo "1|unresolved|run|order" >"$bad_binding/state/binding"; if run "$bad_binding"; then exit 1; fi; test "$(cat "$bad_binding/state/producer-active")" = 1
bad_health=$root/bad-health; fixture "$bad_health"; if run "$bad_health" FAKE_PRODUCER_HEALTH=unhealthy; then exit 1; fi; test "$(cat "$bad_health/state/producer-active")" = 1
bad_messages=$root/bad-messages; fixture "$bad_messages"; if run "$bad_messages" FAKE_FAILED_MESSAGES=1; then exit 1; fi; test "$(cat "$bad_messages/state/producer-active")" = 1
bad_auto=$root/bad-auto; fixture "$bad_auto"; if run "$bad_auto" FAKE_AUTO_REDEMPTION_ENABLED=false; then exit 1; fi; test "$(cat "$bad_auto/state/producer-active")" = 1
stopped=$root/stopped; fixture "$stopped"; printf '0\n' >"$stopped/state/signer-active"; printf '0\n' >"$stopped/state/producer-active"
run "$stopped" FAKE_PRESTART_PARTIAL=1 FAKE_PRODUCER_HEALTH_FIRST=starting POLYEDGE_GUARDED_RESTART_WAIT_ATTEMPTS=2 POLYEDGE_GUARDED_RESTART_ALLOW_STOPPED=true POLYEDGE_GUARDED_RESTART_STOPPED_PREFLIGHT="$stopped/preflight.json" POLYEDGE_GUARDED_RESTART_STOPPED_PREFLIGHT_SHA256="$(sha256sum "$stopped/preflight.json" | cut -d' ' -f1)" POLYEDGE_GUARDED_RESTART_STOPPED_BINDING="$stopped/binding-proof.json" POLYEDGE_GUARDED_RESTART_STOPPED_BINDING_SHA256="$(sha256sum "$stopped/binding-proof.json" | cut -d' ' -f1)"
test "$(cat "$stopped/state/signer-active")" = 1; test "$(cat "$stopped/state/producer-active")" = 1
jq -e '.status == "validated" and .startMode == "stopped_restore" and .signer.oldInvocationId == null and .producer.stoppedForRestart == false and (.stoppedPreflight.sha256 | startswith("sha256:")) and (.stoppedBinding.sha256 | startswith("sha256:"))' "$stopped/ring/activation/receipt.json" >/dev/null
stopped_repair=$root/stopped-repair; fixture "$stopped_repair"; bind_repair_evidence "$stopped_repair"; automatic_preflight "$stopped_repair" "$approved_condition"; printf '0\n' >"$stopped_repair/state/signer-active"; printf '0\n' >"$stopped_repair/state/producer-active"
run "$stopped_repair" FAKE_PRIOR_SIGNER_IMAGE="$prior_signer_image" FAKE_AUTOMATIC_REDEMPTION=1 FAKE_APPROVED_CONDITION="$approved_condition" POLYEDGE_GUARDED_RESTART_APPROVED_AUTOMATIC_REDEMPTION_CONDITION="$approved_condition" POLYEDGE_GUARDED_RESTART_PRIOR_SIGNER_IMAGE="$prior_signer_image" POLYEDGE_GUARDED_RESTART_REPAIR_MODE=true POLYEDGE_GUARDED_RESTART_ALLOW_STOPPED=true POLYEDGE_GUARDED_RESTART_STOPPED_PREFLIGHT="$stopped_repair/preflight.json" POLYEDGE_GUARDED_RESTART_STOPPED_PREFLIGHT_SHA256="$(sha256sum "$stopped_repair/preflight.json" | cut -d' ' -f1)" POLYEDGE_GUARDED_RESTART_STOPPED_BINDING="$stopped_repair/binding-proof.json" POLYEDGE_GUARDED_RESTART_STOPPED_BINDING_SHA256="$(sha256sum "$stopped_repair/binding-proof.json" | cut -d' ' -f1)" POLYEDGE_GUARDED_RESTART_DEPLOY="$stopped_repair/bin/deploy" POLYEDGE_GUARDED_RESTART_QUADLET="$stopped_repair/quadlet" POLYEDGE_GUARDED_RESTART_ROLLBACK_DIR="$stopped_repair/rollback"
test "$(cat "$stopped_repair/state/signer-active")" = 1; test "$(cat "$stopped_repair/state/producer-active")" = 1; grep -Fx "Image=$signer_image" "$stopped_repair/quadlet" >/dev/null
jq -e --arg prior "$prior_signer_image" --arg image "$signer_image" --arg condition "$approved_condition" '.status == "validated" and .startMode == "stopped_repair_rollout" and .signer.repairMode == true and .signer.priorImage == $prior and .signer.image == $image and .signer.oldInvocationId != null and .producer.stoppedForRestart == false and .approvedAutomaticRedemptionCondition == $condition and .automaticRedemption.conditionId == $condition and .automaticRedemption.automatic == true and .automaticRedemption.verified == true and (.automaticRedemption.transactionHash | test("^0x[0-9a-f]{64}$")) and (.signer.rollbackCopy | endswith("fixture-polyedge-funded-signer.container"))' "$stopped_repair/ring/activation/receipt.json" >/dev/null
stopped_repair_bad=$root/stopped-repair-bad; fixture "$stopped_repair_bad"; bind_repair_evidence "$stopped_repair_bad"; printf '0\n' >"$stopped_repair_bad/state/signer-active"; printf '0\n' >"$stopped_repair_bad/state/producer-active"
if run "$stopped_repair_bad" FAKE_BAD_POST=1 FAKE_PRIOR_SIGNER_IMAGE="$prior_signer_image" POLYEDGE_GUARDED_RESTART_PRIOR_SIGNER_IMAGE="$prior_signer_image" POLYEDGE_GUARDED_RESTART_REPAIR_MODE=true POLYEDGE_GUARDED_RESTART_ALLOW_STOPPED=true POLYEDGE_GUARDED_RESTART_STOPPED_PREFLIGHT="$stopped_repair_bad/preflight.json" POLYEDGE_GUARDED_RESTART_STOPPED_PREFLIGHT_SHA256="$(sha256sum "$stopped_repair_bad/preflight.json" | cut -d' ' -f1)" POLYEDGE_GUARDED_RESTART_STOPPED_BINDING="$stopped_repair_bad/binding-proof.json" POLYEDGE_GUARDED_RESTART_STOPPED_BINDING_SHA256="$(sha256sum "$stopped_repair_bad/binding-proof.json" | cut -d' ' -f1)" POLYEDGE_GUARDED_RESTART_DEPLOY="$stopped_repair_bad/bin/deploy" POLYEDGE_GUARDED_RESTART_QUADLET="$stopped_repair_bad/quadlet" POLYEDGE_GUARDED_RESTART_ROLLBACK_DIR="$stopped_repair_bad/rollback"; then exit 1; fi
test "$(cat "$stopped_repair_bad/state/signer-active")" = 0; test "$(cat "$stopped_repair_bad/state/producer-active")" = 0; grep -Fx "Image=$prior_signer_image" "$stopped_repair_bad/quadlet" >/dev/null; test ! -e "$stopped_repair_bad/ring/activation/receipt.json"
stopped_bad=$root/stopped-bad; fixture "$stopped_bad"; printf '0\n' >"$stopped_bad/state/signer-active"; printf '0\n' >"$stopped_bad/state/producer-active"; printf '{"status":"unsafe"}\n' >"$stopped_bad/preflight.json"; chmod 640 "$stopped_bad/preflight.json"
if run "$stopped_bad" POLYEDGE_GUARDED_RESTART_ALLOW_STOPPED=true POLYEDGE_GUARDED_RESTART_STOPPED_PREFLIGHT="$stopped_bad/preflight.json" POLYEDGE_GUARDED_RESTART_STOPPED_PREFLIGHT_SHA256="$(sha256sum "$stopped_bad/preflight.json" | cut -d' ' -f1)" POLYEDGE_GUARDED_RESTART_STOPPED_BINDING="$stopped_bad/binding-proof.json" POLYEDGE_GUARDED_RESTART_STOPPED_BINDING_SHA256="$(sha256sum "$stopped_bad/binding-proof.json" | cut -d' ' -f1)"; then exit 1; fi
test "$(cat "$stopped_bad/state/signer-active")" = 0; test "$(cat "$stopped_bad/state/producer-active")" = 0
recovery_fixture() {
  local d=$1 now
  fixture "$d"; bind_repair_evidence "$d"; now=$(date -u +%s)
  jq -n '{documents:(["authorization","consumption","intent","legacy_completion","redemption","reservation","settlement"] | map({key:.,value:{container:"test",path:.,etag:"0xABC",sha256:("a"*64)}}) | from_entries)}' >"$d/bundle.json"
  jq --argjson now "$now" --arg user "$(id -u):$(id -g)" --arg producer "$producer_image" '
    .createdAtUtc=($now|todateiso8601) | .servicesMutated=false | .helperSha256=("sha256:"+("f"*64)) |
    .runtime.signer += {revision:"4208d541f193c85bd121692bdff46b8898f2c2fc",user:$user,restartCount:0} |
    .runtime.producer={invocationId:.runtime.signer.invocationId,image:$producer,user:$user,restartCount:0} |
    .heartbeat={capturedAtEpoch:($now-1)} | .redemption={transactionHash:"transaction",settlementBlob:"settlement"} |
    .evidence={liveSummary:{sha256:"live"},internalSettlement:{sha256:"settlement"}}
  ' "$d/lifecycle.json" >"$d/lifecycle.tmp"; mv "$d/lifecycle.tmp" "$d/lifecycle.json"
  jq -n --slurpfile life "$d/lifecycle.json" --slurpfile bundle "$d/bundle.json" --arg path "$d/lifecycle.json" --arg sha "sha256:$(sha256sum "$d/lifecycle.json" | cut -d' ' -f1)" \
    --arg bundle_path "$d/bundle.json" --arg bundle_sha "sha256:$(sha256sum "$d/bundle.json" | cut -d' ' -f1)" --arg target "$signer_image" --arg revision "$signer_revision" --argjson now "$now" '
    $life[0] as $l | {schema:"polyedge.funded_signer_recording_recovery.v1",status:"recording_recovered",createdAtUtc:$l.createdAtUtc,
    originalGuardedDeploymentClaimed:false,servicesMutated:false,azureDeletionAllowed:false,helperSha256:$l.helperSha256,
    targetSigner:{image:$target,revision:$revision},lifecycle:{path:$path,sha256:$sha},runtime:$l.runtime,
    sourceBundle:{path:$bundle_path,sha256:$bundle_sha},proof:{schema:"polyedge.terminal_no_order_proof.v1",status:"verified_terminal_no_order",
    authenticatedSourcesVerified:true,readOnly:true,originalRolloutReceiptPresent:false,verifiedAtUtc:$l.createdAtUtc,
    sourceBundleSha256:$bundle_sha,sources:$bundle[0].documents,runtime:($l.runtime.signer|{image,revision,invocationId,containerId}),
    decisionId:("a"*64),reason:"post_only_crosses_book",cleanSinceEpoch:($now-120),
    redemption:{summarySha256:"live",settlementSha256:"settlement",transactionHash:"transaction",settlementBlob:"settlement"}}}' >"$d/recovery.json"
  chmod 640 "$d/lifecycle.json" "$d/bundle.json" "$d/recovery.json"
}
run_recovery() {
  local d=$1; shift
  run "$d" FAKE_PRIOR_SIGNER_IMAGE="$prior_signer_image" POLYEDGE_GUARDED_RESTART_PRIOR_SIGNER_IMAGE="$prior_signer_image" \
    POLYEDGE_GUARDED_RESTART_PRIOR_RECEIPT= POLYEDGE_GUARDED_RESTART_PRIOR_RECEIPT_SHA256= \
    POLYEDGE_GUARDED_RESTART_RECORDING_RECOVERY="$d/recovery.json" POLYEDGE_GUARDED_RESTART_RECORDING_RECOVERY_SHA256="$(sha256sum "$d/recovery.json" | cut -d' ' -f1)" \
    POLYEDGE_GUARDED_RESTART_REPAIR_MODE=true POLYEDGE_GUARDED_RESTART_DEPLOY="$d/bin/deploy" POLYEDGE_GUARDED_RESTART_QUADLET="$d/quadlet" POLYEDGE_GUARDED_RESTART_ROLLBACK_DIR="$d/rollback" "$@"
}
prior_signer_image=ghcr.io/aldoapicella/polyedge-venue-probe@sha256:cf701ac5ebdf1a66c10ed52feab9fbca3dfb6eb7937e2501c5a41f812a29f28f
d=$root/recovery; recovery_fixture "$d"; run_recovery "$d"
jq -e '.status=="validated" and .priorRollout==null and .recordingRecovery.path!=null and .producer.restored==true' "$d/ring/activation/receipt.json" >/dev/null
for mutation in '.createdAtUtc="2026-01-01T00:00:00Z"' '.proof.verifiedAtUtc="2026-01-01T00:00:00Z"' '.runtime.signer.containerId=("0"*64)' '.targetSigner.revision=("0"*40)' '.proof.sources.intent.etag="changed"' '.proof.sourceBundleSha256=("sha256:"+("0"*64))'; do
  d=$root/recovery-bad-$RANDOM; recovery_fixture "$d"; jq "$mutation" "$d/recovery.json" >"$d/mutated"; mv "$d/mutated" "$d/recovery.json"; chmod 640 "$d/recovery.json"
  if run_recovery "$d"; then echo "invalid recovery accepted: $mutation" >&2; exit 1; fi
  test "$(cat "$d/state/producer-active")" = 1; test "$(cat "$d/state/phase")" = before
done
for bad_env in FAKE_LATE_ALERT=1 FAKE_FAILED_MESSAGES=1 FAKE_EVIDENCE_CHANGE=1 POLYEDGE_GUARDED_RESTART_PRIOR_RECEIPT=claimed POLYEDGE_GUARDED_RESTART_RECORDING_RECOVERY_SHA256=0000000000000000000000000000000000000000000000000000000000000000; do
  d=$root/recovery-bad-$RANDOM; recovery_fixture "$d"
  if run_recovery "$d" "$bad_env"; then echo "unsafe recovery accepted: $bad_env" >&2; exit 1; fi
  test "$(cat "$d/state/producer-active")" = 1; test "$(cat "$d/state/phase")" = before
done
d=$root/recovery-runtime-change; recovery_fixture "$d"
if run_recovery "$d" FAKE_RUNTIME_CHANGE=1; then echo 'runtime change unexpectedly deployed' >&2; exit 1; fi
test "$(cat "$d/state/phase")" = before; test "$(cat "$d/state/producer-active")" = 0; test ! -e "$d/ring/activation/receipt.json"
# Active recovery uses a complete, twice-read chain preflight instead of the old
# first-page nothing_to_redeem summary, and keeps the producer off on uncertainty.
approved_recovery_fixture() {
  local d=$1
  recovery_fixture "$d"
  cp "$repo/ops/conduit/bin/polyedge-funded-redemption-preflight.mjs" "$d/bin/collector"; chmod 755 "$d/bin/collector"
  jq -n --arg condition "$approved_condition" --arg image "$signer_image" --arg revision "$signer_revision" --argjson now "$(date -u +%s)" '
    {schema:"polyedge.approved_redemption_preflight.v1",status:"approved_redemption_ready",read_only:true,redemption_submitted:false,
     funder:"0x3d701b05d7c36afab01a06fd26ebe789c0b7bad8",condition_id:$condition,target_image:$image,target_revision:$revision,
     started_ts:($now|todateiso8601),finished_ts:($now|todateiso8601),block:{chain_id:137,number:"99",hash:("0x"+("d"*64)),timestamp:($now-2)},
     open_order_count:0,unresolved_position_count:0,unresolved_reservation_count:0,
     position_inventory:{reader:"loadAccountPositions",complete:true,readbacks:2,rows:131,sha256:("a"*64)},payout_base_units:"5000000",
     selection:{payout_source:"onchain_balances_and_payout_vector",available_winner_conditions:1,skipped_winner_conditions:0,selected_gross_payout:5,
       selected:[{condition_id:$condition,asset_ids:["11","22"],onchain_balances_base_units:["5000000","0"],payout_numerators:["1","0"],
                  payout_denominator:"1",onchain_expected_payout:5,gross_payout:5}]}}
  ' >"$d/approved-preflight.json"
  jq --slurpfile proof "$d/approved-preflight.json" --arg path "$d/bin/collector" --arg sha "sha256:$(sha256sum "$d/bin/collector"|cut -d' ' -f1)" \
    '.evidence.followUpDryRun=null | .evidence.approvedRedemptionPreflight={collector:{path:$path,sha256:$sha},proof:$proof[0]}' "$d/lifecycle.json" >"$d/lifecycle.tmp"; mv "$d/lifecycle.tmp" "$d/lifecycle.json"
  jq --slurpfile life "$d/lifecycle.json" --arg sha "sha256:$(sha256sum "$d/lifecycle.json"|cut -d' ' -f1)" \
    '.lifecycle.sha256=$sha | .approvedRedemptionPreflight=$life[0].evidence.approvedRedemptionPreflight' "$d/recovery.json" >"$d/recovery.tmp"; mv "$d/recovery.tmp" "$d/recovery.json"
  chmod 640 "$d/lifecycle.json" "$d/recovery.json" "$d/approved-preflight.json"
}
run_approved_recovery() {
  local d=$1; shift
  run_recovery "$d" FAKE_APPROVED_CONDITION="$approved_condition" FAKE_AUTOMATIC_REDEMPTION=1 \
    POLYEDGE_GUARDED_RESTART_APPROVED_AUTOMATIC_REDEMPTION_CONDITION="$approved_condition" \
    POLYEDGE_GUARDED_RESTART_REDEMPTION_PREFLIGHT_COLLECTOR="$d/bin/collector" "$@"
}
d=$root/approved-recovery; approved_recovery_fixture "$d"; run_approved_recovery "$d" FAKE_COLD_AFTER_REPAIR=1
jq -e --arg condition "$approved_condition" '.status=="validated" and .recordingRecovery!=null and .priorRollout==null and
  .approvedAutomaticRedemptionCondition==$condition and .automaticRedemption.conditionId==$condition and .automaticRedemption.grossPayout==5 and
  .approvedRedemptionPreflight.beforeProducerStop.position_inventory.complete==true and
  .approvedRedemptionPreflight.afterProducerStop.unresolved_reservation_count==0 and .producer.restored==true' "$d/ring/activation/receipt.json" >/dev/null
for mutation in '.unresolved_position_count=1' '.unresolved_reservation_count=1' '.open_order_count=1' '.position_inventory.complete=false' \
  '.selection.available_winner_conditions=2' '.condition_id=("0x"+("b"*64))' '.target_revision=("0"*40)' \
  '.started_ts="2026-01-01T00:00:00Z"' '.payout_base_units="6000000" | .selection.selected_gross_payout=6 | .selection.selected[0].gross_payout=6 | .selection.selected[0].onchain_expected_payout=6'; do
  d=$root/approved-bad-$RANDOM; approved_recovery_fixture "$d"
  if run_approved_recovery "$d" "FAKE_PREFLIGHT_MUTATION=$mutation"; then echo "unsafe approved preflight accepted: $mutation" >&2; exit 1; fi
  test "$(cat "$d/state/phase")" = before; test "$(cat "$d/state/producer-active")" = 1; test ! -e "$d/ring/activation/receipt.json"
done
d=$root/approved-changed-after-stop; approved_recovery_fixture "$d"
if run_approved_recovery "$d" FAKE_PREFLIGHT_MUTATION=.unresolved_reservation_count=1 FAKE_PREFLIGHT_MUTATE_AFTER_STOP=1; then exit 1; fi
test "$(cat "$d/state/phase")" = before; test "$(cat "$d/state/producer-active")" = 0; test "$(cat "$d/state/signer-active")" = 1
d=$root/approved-collector-changed; approved_recovery_fixture "$d"; printf '\n' >>"$d/bin/collector"
if run_approved_recovery "$d"; then exit 1; fi
test "$(cat "$d/state/producer-active")" = 1; test "$(cat "$d/state/phase")" = before
for bad_env in FAKE_AUTOMATIC_REDEMPTION=0 FAKE_AUTOMATIC_PAYOUT=6 POLYEDGE_GUARDED_RESTART_APPROVED_AUTOMATIC_REDEMPTION_CONDITION=; do
  d=$root/approved-incomplete-$RANDOM; approved_recovery_fixture "$d"
  if run_approved_recovery "$d" "$bad_env"; then echo "incomplete redemption accepted: $bad_env" >&2; exit 1; fi
  test ! -e "$d/ring/activation/receipt.json"
  if [ "$bad_env" = POLYEDGE_GUARDED_RESTART_APPROVED_AUTOMATIC_REDEMPTION_CONDITION= ]; then test "$(cat "$d/state/phase")" = before
  else test "$(cat "$d/state/producer-active")" = 0; test "$(cat "$d/state/signer-active")" = 0; grep -Fx "Image=$prior_signer_image" "$d/quadlet" >/dev/null; fi
done
oci_recovery_fixture() {
  local d=$1
  approved_recovery_fixture "$d"
  cat >"$d/bin/queue-snapshot" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
[ "${FAKE_OCI_SNAPSHOT_FAIL:-0}" = 0 ] || exit 1
count=1824
if [ "$(cat "$FAKE/state/producer-active")" = 0 ]; then count=${FAKE_OCI_ARCHIVE_AFTER_STOP:-1824}; fi
jq -c --argjson count "$count" '.archiveDlq.objectCount=$count' "$FAKE/oci-snapshot.json"
EOF
  chmod 755 "$d/bin/queue-snapshot"
  jq -n --arg path "$d/bin/queue-snapshot" --arg sha "sha256:$(sha256sum "$d/bin/queue-snapshot"|cut -d' ' -f1)" \
    '{backend:"oci",schema:"polyedge.funded_oci_queue_snapshot.v1",status:"observed_zero",readOnly:true,
      verifier:{path:$path,sha256:$sha},archiveDlq:{objectCount:1824,inventorySha256:("a"*64),exhaustiveListing:true}}' >"$d/oci-snapshot.json"
  jq --slurpfile q "$d/oci-snapshot.json" '.queue={before:$q[0],after:$q[0]}' "$d/lifecycle.json" >"$d/lifecycle.tmp"; mv "$d/lifecycle.tmp" "$d/lifecycle.json"
  jq --arg sha "sha256:$(sha256sum "$d/lifecycle.json"|cut -d' ' -f1)" '.lifecycle.sha256=$sha' "$d/recovery.json" >"$d/recovery.tmp"; mv "$d/recovery.tmp" "$d/recovery.json"
  chmod 640 "$d/lifecycle.json" "$d/recovery.json"
}
run_oci_recovery() { local d=$1; shift; run_approved_recovery "$d" POLYEDGE_GUARDED_RESTART_QUEUE_BACKEND=oci POLYEDGE_GUARDED_RESTART_QUEUE_SNAPSHOT_HELPER="$d/bin/queue-snapshot" "$@"; }
d=$root/oci-recovery; oci_recovery_fixture "$d"; run_oci_recovery "$d" FAKE_COLD_AFTER_REPAIR=1
jq -e '.queue.before.backend=="oci" and .queue.before.archiveDlq.objectCount==1824 and .queue.existingDlqPreserved==true and .queue.producerStoppedQuietSeconds>=10 and .producer.restored==true and (.queue.before|has("scheduledMessageCount")|not)' "$d/ring/activation/receipt.json" >/dev/null
d=$root/oci-dlq-changed; oci_recovery_fixture "$d"
if run_oci_recovery "$d" FAKE_OCI_ARCHIVE_AFTER_STOP=1825; then echo 'changed OCI DLQ accepted' >&2; exit 1; fi
test "$(cat "$d/state/producer-active")" = 0; test "$(cat "$d/state/phase")" = before
for cause in helper_changed unsafe_mode failed_read; do
  d=$root/oci-bad-$cause; oci_recovery_fixture "$d"
  args=()
  case "$cause" in helper_changed) printf '\n' >>"$d/bin/queue-snapshot";; unsafe_mode) chmod 777 "$d/bin/queue-snapshot";; failed_read) args=(FAKE_OCI_SNAPSHOT_FAIL=1);; esac
  if run_oci_recovery "$d" "${args[@]}"; then echo "unsafe OCI queue proof accepted: $cause" >&2; exit 1; fi
  test "$(cat "$d/state/producer-active")" = 1; test "$(cat "$d/state/phase")" = before
done
printf 'funded guarded signer restart tests passed\n'
