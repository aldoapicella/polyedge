#!/usr/bin/env bash
set -euo pipefail

repo=$(cd "$(dirname "$0")/../../.." && pwd)
helper=$repo/ops/conduit/bin/polyedge-funded-signer-guarded-restart-20260824
signer_image=ghcr.io/aldoapicella/polyedge-venue-probe@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc
producer_image=ghcr.io/aldoapicella/polyedge-rust-backend@sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd
root=$(mktemp -d)
trap 'rm -rf "$root"' EXIT

fixture() {
  local d=$1
  mkdir -p "$d/bin" "$d/state" "$d/ring/activation" "$d/tokens"
  printf '1\n' >"$d/state/producer-active"; printf 'before\n' >"$d/state/phase"; printf '0|||\n' >"$d/state/binding"
  printf '%032d\n' 1 >"$d/state/invocation"; printf '%064d\n' 2 >"$d/state/container"
  printf '%s\n' '{"status":"Active","active":0,"scheduled":0,"dlq":1311}' >"$d/state/queue"
  printf token >"$d/tokens/token"; chmod 600 "$d/tokens/token"
  printf '%s\n' '{"status":"validated","azureDeletionAuthorized":false}' >"$d/prior.json"
  printf '%s\n' '{"status":"succeeded","azureDeletionAuthorized":false}' >"$d/lifecycle.json"
  chmod 640 "$d/prior.json" "$d/lifecycle.json"
  cat >"$d/bin/systemctl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
case "$1" in
 is-active) case "$3" in polyedge-funded-signer.service) exit 0;; polyedge-funded-intent-producer.service) test "$(cat "$FAKE/state/producer-active")" = 1;; polyedge-parity-hourly.timer) exit 3;; esac ;;
 stop) test "$2" = polyedge-funded-intent-producer.service; printf '0\n' >"$FAKE/state/producer-active" ;;
 start) test "$2" = polyedge-funded-intent-producer.service; printf '1\n' >"$FAKE/state/producer-active" ;;
 restart) test "$2" = polyedge-funded-signer.service; printf 'after\n' >"$FAKE/state/phase"; printf '%032d\n' 3 >"$FAKE/state/invocation"; printf '%064d\n' 4 >"$FAKE/state/container" ;;
 show) case "$4" in InvocationID) cat "$FAKE/state/invocation";; NRestarts) printf '0\n';; esac ;;
esac
EOF
  cat >"$d/bin/podman" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
case "$1" in
 inspect)
  if [ "$4" = polyedge-funded-signer ]; then case "$3" in '{{.Config.Image}}|{{.Config.User}}|{{.State.Status}}') printf '%s|%s|running\n' "$FAKE_SIGNER_IMAGE" "$FAKE_USER";; '{{.Id}}') cat "$FAKE/state/container";; esac
  else test "$(cat "$FAKE/state/producer-active")" = 1; printf '%s|%s|running|%s\n' "$FAKE_PRODUCER_IMAGE" "$FAKE_USER" "${FAKE_PRODUCER_HEALTH:-healthy}"; fi ;;
 exec) [[ "${6:-}" == *loadCampaignUnresolvedRiskReservationRecords* ]]; cat "$FAKE/state/binding" ;;
esac
EOF
  cat >"$d/bin/journalctl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
attempts=0; [ "${FAKE_BAD_POST:-0}" = 1 ] && [ "$(cat "$FAKE/state/phase")" = after ] && attempts=1
message=$(/usr/bin/jq -nc --argjson attempts "$attempts" --argjson failed_messages "${FAKE_FAILED_MESSAGES:-0}" --argjson failed_messages "${FAKE_FAILED_MESSAGES:-0}" '{schema:"polyedge.funded_direct_service.v2",status:"persistent_service_heartbeat",failed_attempts:$attempts,failed_messages:$failed_messages,processed_messages:0,executor:{busy:false,user_channel_ready:true,market_channel_ready:true,user_channel_gaps:0,market_channel_gaps:0,user_channel_unparsed:0,market_channel_unparsed:0,reconnect_reconciliation_required:false,safety_snapshot_cache_ready:true,safety_snapshot_cache_age_ms:1,safety_snapshot_open_order_count:0,safety_snapshot_unresolved_position_count:0,safety_snapshot_unresolved_risk_reservation_count:0,safety_snapshot_cache_error:null,risk_reservation_index_ready:true}}')
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
  printf '#!/usr/bin/env bash\nexit 0\n' >"$d/bin/disk"; chmod 755 "$d/bin/"*
}

run() {
  local d=$1; shift
  env FAKE="$d" FAKE_SIGNER_IMAGE="$signer_image" FAKE_USER="$(id -u):$(id -g)" FAKE_PRODUCER_IMAGE="$producer_image" \
    POLYEDGE_GUARDED_RESTART_PRIOR_RECEIPT="$d/prior.json" POLYEDGE_GUARDED_RESTART_PRIOR_RECEIPT_SHA256="$(sha256sum "$d/prior.json" | cut -d' ' -f1)" \
    POLYEDGE_GUARDED_RESTART_LIFECYCLE_EVIDENCE="$d/lifecycle.json" POLYEDGE_GUARDED_RESTART_LIFECYCLE_EVIDENCE_SHA256="$(sha256sum "$d/lifecycle.json" | cut -d' ' -f1)" \
    POLYEDGE_GUARDED_RESTART_SIGNER_IMAGE="$signer_image" POLYEDGE_GUARDED_RESTART_SIGNER_USER="$(id -u):$(id -g)" \
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

bad=$root/bad; fixture "$bad"
if run "$bad" FAKE_BAD_POST=1; then exit 1; fi
test "$(cat "$bad/state/producer-active")" = 0
test ! -e "$bad/ring/activation/receipt.json"
bad_binding=$root/bad-binding; fixture "$bad_binding"; echo "1|unresolved|run|order" >"$bad_binding/state/binding"; if run "$bad_binding"; then exit 1; fi; test "$(cat "$bad_binding/state/producer-active")" = 1
bad_health=$root/bad-health; fixture "$bad_health"; if run "$bad_health" FAKE_PRODUCER_HEALTH=unhealthy; then exit 1; fi; test "$(cat "$bad_health/state/producer-active")" = 1
bad_messages=$root/bad-messages; fixture "$bad_messages"; if run "$bad_messages" FAKE_FAILED_MESSAGES=1; then exit 1; fi; test "$(cat "$bad_messages/state/producer-active")" = 1
printf 'funded guarded signer restart tests passed\n'
