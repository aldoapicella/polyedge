#!/usr/bin/env bash
set -euo pipefail

repo=$(cd "$(dirname "$0")/../../.." && pwd)
helper=$repo/ops/conduit/bin/polyedge-funded-signer-guarded-restart-20260824
signer_image=ghcr.io/aldoapicella/polyedge-venue-probe@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc
prior_signer_image=ghcr.io/aldoapicella/polyedge-venue-probe@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
signer_revision=eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
producer_image=ghcr.io/aldoapicella/polyedge-rust-backend@sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd
root=$(mktemp -d)
trap 'rm -rf "$root"' EXIT
grep -F '"$journalctl" -u polyedge-funded-signer.service -n 5000 -o json --no-pager' "$helper" >/dev/null

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
  /usr/bin/jq -n --arg finished "$(date -u +%Y-%m-%dT%H:%M:%SZ)" '{schema_version:1,status:"nothing_to_redeem",dry_run:true,redemption_submitted:false,zero_open_orders_confirmed:true,finished_ts:$finished,portfolio:{redeemable_winner_count:0},selection:{selected_gross_payout:0,selected:[]}}' >"$d/preflight.json"
  chmod 640 "$d/prior.json" "$d/lifecycle.json" "$d/preflight.json"
  cat >"$d/bin/systemctl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
case "$1" in
 is-active) case "$3" in polyedge-funded-signer.service) test "$(cat "$FAKE/state/signer-active")" = 1;; polyedge-funded-intent-producer.service) test "$(cat "$FAKE/state/producer-active")" = 1;; polyedge-parity-hourly.timer) exit 3;; esac ;;
 stop) case "$2" in polyedge-funded-signer.service) printf '0\n' >"$FAKE/state/signer-active";; polyedge-funded-intent-producer.service) printf '0\n' >"$FAKE/state/producer-active";; esac ;;
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
 pull) : ;;
 image) printf "%s\n" "linux/arm64|$FAKE_SIGNER_REVISION" ;;
 inspect)
  image=$FAKE_SIGNER_IMAGE; [ "$(cat "$FAKE/state/phase")" = after ] || image=${FAKE_PRIOR_SIGNER_IMAGE:-$FAKE_SIGNER_IMAGE}
  if [ "$4" = polyedge-funded-signer ]; then case "$3" in '{{.Config.Image}}|{{.Config.User}}|{{.State.Status}}') printf '%s|%s|running\n' "$image" "$FAKE_USER";; '{{.Id}}') cat "$FAKE/state/container";; esac
  else test "$(cat "$FAKE/state/producer-active")" = 1; health=${FAKE_PRODUCER_HEALTH:-healthy}
    if [ "${FAKE_PRODUCER_HEALTH_FIRST:-}" = starting ] && [ ! -e "$FAKE/state/producer-health-seen" ]; then touch "$FAKE/state/producer-health-seen"; health=starting; fi
    printf '%s|%s|running|%s\n' "$FAKE_PRODUCER_IMAGE" "$FAKE_USER" "$health"
  fi ;;
 exec) [[ "${6:-}" == *loadCampaignUnresolvedRiskReservationRecords* ]]; cat "$FAKE/state/binding" ;;
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
message=$(/usr/bin/jq -nc --argjson attempts "$attempts" --argjson failed_messages "$failed_messages" --argjson redemption_failures "$redemption_failures" --argjson partial "$partial" '{schema:"polyedge.funded_direct_service.v2",status:"persistent_service_heartbeat",failed_attempts:$attempts,failed_messages:$failed_messages,redemption_failures:$redemption_failures,processed_messages:0,executor:{busy:false,user_channel_ready:true,market_channel_ready:($partial|not),user_channel_gaps:0,market_channel_gaps:0,user_channel_unparsed:0,market_channel_unparsed:0,reconnect_reconciliation_required:false,safety_snapshot_cache_ready:($partial|not),safety_snapshot_cache_age_ms:(if $partial then null else 1 end),safety_snapshot_open_order_count:(if $partial then null else 0 end),safety_snapshot_unresolved_position_count:(if $partial then null else 0 end),safety_snapshot_unresolved_risk_reservation_count:(if $partial then null else 0 end),safety_snapshot_cache_error:null,risk_reservation_index_ready:true}}')
started=$(/usr/bin/jq -nc --argjson enabled "${FAKE_AUTO_REDEMPTION_ENABLED:-true}" '{schema:"polyedge.funded_direct_service.v2",status:"persistent_service_started",automatic_redemption_enabled:$enabled}')
if [ "${FAKE_REPAIR_PRE:-0}" = 1 ] && [ "$(cat "$FAKE/state/phase")" = before ]; then
  alert=$(/usr/bin/jq -nc '{schema:"polyedge.funded_direct_alert.v1",status:"known_repair_trigger"}')
  /usr/bin/jq -nc --arg ts "$(date -u +%s)000000" --arg inv "$(cat "$FAKE/state/invocation")" --arg container "$(cat "$FAKE/state/container")" --arg message "$alert" '{__REALTIME_TIMESTAMP:$ts,_SYSTEMD_INVOCATION_ID:$inv,CONTAINER_ID_FULL:$container,MESSAGE:$message}'
fi
/usr/bin/jq -nc --arg ts "$(date -u +%s)000000" --arg inv "$(cat "$FAKE/state/invocation")" --arg container "$(cat "$FAKE/state/container")" --arg message "$started" '{__REALTIME_TIMESTAMP:$ts,_SYSTEMD_INVOCATION_ID:$inv,CONTAINER_ID_FULL:$container,MESSAGE:$message}'
/usr/bin/jq -nc --arg ts "$(date -u +%s)000000" --arg inv "$(cat "$FAKE/state/invocation")" --arg container "$(cat "$FAKE/state/container")" --arg message "$message" '{__REALTIME_TIMESTAMP:$ts,_SYSTEMD_INVOCATION_ID:$inv,CONTAINER_ID_FULL:$container,MESSAGE:$message}'
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
run "$stopped" FAKE_PRESTART_PARTIAL=1 FAKE_PRODUCER_HEALTH_FIRST=starting POLYEDGE_GUARDED_RESTART_WAIT_ATTEMPTS=2 POLYEDGE_GUARDED_RESTART_ALLOW_STOPPED=true POLYEDGE_GUARDED_RESTART_STOPPED_PREFLIGHT="$stopped/preflight.json" POLYEDGE_GUARDED_RESTART_STOPPED_PREFLIGHT_SHA256="$(sha256sum "$stopped/preflight.json" | cut -d' ' -f1)"
test "$(cat "$stopped/state/signer-active")" = 1; test "$(cat "$stopped/state/producer-active")" = 1
jq -e '.status == "validated" and .startMode == "stopped_restore" and .signer.oldInvocationId == null and .producer.stoppedForRestart == false and (.stoppedPreflight.sha256 | startswith("sha256:"))' "$stopped/ring/activation/receipt.json" >/dev/null
stopped_bad=$root/stopped-bad; fixture "$stopped_bad"; printf '0\n' >"$stopped_bad/state/signer-active"; printf '0\n' >"$stopped_bad/state/producer-active"; printf '{"status":"unsafe"}\n' >"$stopped_bad/preflight.json"; chmod 640 "$stopped_bad/preflight.json"
if run "$stopped_bad" POLYEDGE_GUARDED_RESTART_ALLOW_STOPPED=true POLYEDGE_GUARDED_RESTART_STOPPED_PREFLIGHT="$stopped_bad/preflight.json" POLYEDGE_GUARDED_RESTART_STOPPED_PREFLIGHT_SHA256="$(sha256sum "$stopped_bad/preflight.json" | cut -d' ' -f1)"; then exit 1; fi
test "$(cat "$stopped_bad/state/signer-active")" = 0; test "$(cat "$stopped_bad/state/producer-active")" = 0
printf 'funded guarded signer restart tests passed\n'
