#!/usr/bin/env bash
set -euo pipefail

repo=$(cd "$(dirname "$0")/../../.." && pwd)
helper=$repo/ops/conduit/bin/polyedge-funded-signer-post-recovery-rollout
old_image=ghcr.io/aldoapicella/polyedge-venue-probe@sha256:ab7caf1990755289a134533654a4d5b61432c1b39a45bbf1d9665b51de237c03
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
  printf '1\n' >"$case_root/state/producer-active"
  printf '0|||\n' >"$case_root/state/binding"
  printf '0\n' >"$case_root/state/deploy-count"

  /usr/bin/jq -n --arg old "$old_image" --arg producer "$producer_image" '{
    schema:"polyedge.acknowledged_no_fill_reconciliation.v1",status:"finalized_no_fill",
    cutoverCompletedAtUtc:"2026-08-22T19:50:13Z",
    decision_id:"96f92c50f5c583cfaa0bc3be5db780a742ec87140610ba9e0e1d4874dd9e0810",
    run_id:"funded-direct-20260821194914083-5cc133ce",
    order_id:"0xb239b7c3c104d591a3eae9d87922313c78b274ebfcc0885e313253124b3386a9",
    order_submission_attempted:true,recovery_order_submission_attempted:false,
    recovery_grant_consumed:false,recovery_risk_reservation_created:false,
    reconciliation_reason:"acknowledged_evicted_order_no_fill",evidence:{observation_ms:10000},
    recoveryImage:$old,signerImageUnchanged:$old,producerImageUnchanged:$producer,
    reservationEvidence:{blob:"reports/research/venue-probe/risk-reservations/2026-08-21/funded-direct-96f92c50f5c583cfaa0bc3be5db780a742ec87140610ba9e0e1d4874dd9e0810.json",sha256:"sha256:c9486181df09a2d1bdd0cc90b74836e529e70f73479f15290663e6db0fe6e9d7"},
    completionEvidence:{blob:"reports/funded/dynamic-quote/sessions/dynamic-quote-funded-2026-08-13-v10/completed/96f92c50f5c583cfaa0bc3be5db780a742ec87140610ba9e0e1d4874dd9e0810.json",sha256:"sha256:32c6a39c0336ceb1902bcf8b92008d27d27d05bb9add23dd8c608389ed1c9477"},
    summaryEvidence:{blob:"reports/research/venue-probe/runs/2026-08-21/funded-direct-20260821194914083-5cc133ce/summary.json",sha256:"sha256:59025de4af33db8bc7cda1d9c6505575f0ad7f43620c5f93ff1f82898e7dd347"},
    unresolvedReservationsAfter:0,queueActiveMessages:0,queueScheduledMessages:0,queueDeadLetterMessages:1018,
    parityTimerRemainsPaused:true,azureDeletionAllowed:false
  }' >"$case_root/ring/funded-recovery/recovery.json"

  printf '#!/usr/bin/env bash\nexit 0\n' >"$case_root/bin/recovery"
  printf '#!/usr/bin/env bash\nexit 0\n' >"$case_root/bin/disk-guard"
  printf '[Container]\nImage=%s\n' "$old_image" >"$case_root/signer.container"
  sha256sum "$case_root/signer.container" | cut -d' ' -f1 >"$case_root/state/old-quadlet-sha"

  cat >"$case_root/bin/systemctl" <<'EOF'
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
EOF

  cat >"$case_root/bin/podman" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
if [ "$1" = inspect ]; then
  if [ "$4" = polyedge-funded-signer ]; then
    case "$3" in
      '{{.Config.Image}}|{{.Config.User}}|{{.State.Status}}') printf '%s|986:982|running\n' "$(cat "$FAKE_STATE/signer-image")" ;;
      '{{.Id}}') cat "$FAKE_STATE/signer-container" ;;
    esac
  else
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
EOF

  cat >"$case_root/bin/journalctl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
message=$(/usr/bin/jq -nc '{schema:"polyedge.funded_direct_service.v2",status:"persistent_service_heartbeat",failed_messages:0,executor:{busy:false,user_channel_ready:true,market_channel_ready:true,user_channel_gaps:0,market_channel_gaps:0,user_channel_unparsed:0,market_channel_unparsed:0,reconnect_reconciliation_required:false,safety_snapshot_cache_ready:true,safety_snapshot_cache_age_ms:1,safety_snapshot_open_order_count:0,safety_snapshot_unresolved_position_count:0,safety_snapshot_unresolved_risk_reservation_count:0,safety_snapshot_cache_error:null,risk_reservation_index_ready:true}}')
/usr/bin/jq -nc --arg ts "$(/usr/bin/date -u +%s)000000" --arg inv "$(cat "$FAKE_STATE/signer-invocation")" --arg container "$(cat "$FAKE_STATE/signer-container")" --arg message "$message" '{__REALTIME_TIMESTAMP:$ts,_SYSTEMD_INVOCATION_ID:$inv,CONTAINER_ID_FULL:$container,MESSAGE:$message}'
EOF

  cat >"$case_root/bin/runuser" <<'EOF'
#!/usr/bin/env bash
shift 3
exec "$@"
EOF
  cat >"$case_root/bin/az" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' '{"status":"Active","countDetails":{"activeMessageCount":0,"scheduledMessageCount":0,"deadLetterMessageCount":1018}}'
EOF
  cat >"$case_root/bin/deploy" <<'EOF'
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
if [ "${FAKE_DEPLOY_INTERRUPT:-0}" = 1 ]; then
  kill -KILL "$PPID"
fi
EOF

  chmod 0700 "$case_root/bin/recovery"
  chmod 0755 "$case_root/bin/disk-guard" "$case_root/bin/systemctl" "$case_root/bin/podman" \
    "$case_root/bin/journalctl" "$case_root/bin/runuser" "$case_root/bin/az" "$case_root/bin/deploy"
  chmod 0600 "$case_root/signer.container"
  chmod 0640 "$case_root/ring/funded-recovery/recovery.json"
}

run_helper() {
  case_root=$1
  shift
  env FAKE_STATE="$case_root/state" FAKE_NEW_IMAGE="$new_image" FAKE_REVISION="$revision" \
    FAKE_PRODUCER_IMAGE="$producer_image" FAKE_QUADLET="$case_root/signer.container" "$@" \
    POLYEDGE_TEST_ALLOW_UNPRIVILEGED=1 POLYEDGE_TEST_RECOVERY="$case_root/ring/funded-recovery/recovery.json" \
    POLYEDGE_TEST_ROLLOUT="$case_root/ring/activation/rollout.json" \
    POLYEDGE_TEST_RECOVERY_SCRIPT="$case_root/bin/recovery" POLYEDGE_TEST_QUADLET="$case_root/signer.container" \
    POLYEDGE_TEST_DEPLOY="$case_root/bin/deploy" POLYEDGE_TEST_DISK_GUARD="$case_root/bin/disk-guard" \
    POLYEDGE_TEST_SYSTEMCTL="$case_root/bin/systemctl" POLYEDGE_TEST_PODMAN="$case_root/bin/podman" \
    POLYEDGE_TEST_JOURNALCTL="$case_root/bin/journalctl" POLYEDGE_TEST_RUNUSER="$case_root/bin/runuser" \
    POLYEDGE_TEST_AZ="$case_root/bin/az" POLYEDGE_TEST_UID="$(id -u)" POLYEDGE_TEST_GID="$(id -g)" \
    POLYEDGE_TEST_RECOVERY_SHA="$(sha256sum "$case_root/bin/recovery" | cut -d' ' -f1)" \
    POLYEDGE_TEST_QUADLET_SHA="$(cat "$case_root/state/old-quadlet-sha")" \
    POLYEDGE_TEST_DEPLOY_SHA="$(sha256sum "$case_root/bin/deploy" | cut -d' ' -f1)" \
    POLYEDGE_TEST_LOCK_FILE="$case_root/utility.lock" POLYEDGE_TEST_WAIT_ATTEMPTS=2 POLYEDGE_TEST_WAIT_SECONDS=0 \
    "$helper" "$new_image" "$revision"
}

chmod 0755 "$helper"

success=$root/success
make_fixture "$success"
run_helper "$success"
test "$(cat "$success/state/producer-active")" = 1
test "$(cat "$success/state/deploy-count")" = 1
test "$(stat -c %a "$success/ring/activation/rollout.json")" = 640
pending=$success/ring/activation/20260822T195013Z-funded-signer-rollout.pending.json
test "$(stat -c %a "$pending")" = 640
/usr/bin/jq -e --arg image "$new_image" --arg revision "$revision" \
  --arg pending "$pending" --arg pending_sha "sha256:$(sha256sum "$pending" | cut -d' ' -f1)" '
  .status == "validated" and .newImage == $image and .newRevision == $revision and
  .producerRestored == true and .unresolvedReservationsAfter == 0 and
  .pendingRollout == {path:$pending,sha256:$pending_sha} and
  (.recoveryScriptSha256 | test("^sha256:[0-9a-f]{64}$"))' "$success/ring/activation/rollout.json" >/dev/null
run_helper "$success"
test "$(cat "$success/state/deploy-count")" = 1

safe=$root/safe-failure
cp "$success/signer.container" "$success/signer.container.valid"
/usr/bin/sed -i "s|^Image=.*|Image=$old_image|" "$success/signer.container"
if run_helper "$success"; then exit 1; fi
mv "$success/signer.container.valid" "$success/signer.container"
chmod 0600 "$success/signer.container"
run_helper "$success"
test "$(cat "$success/state/deploy-count")" = 1

make_fixture "$safe"
if run_helper "$safe" FAKE_DEPLOY_FAIL=1; then exit 1; fi
test "$(cat "$safe/state/producer-active")" = 1
test ! -e "$safe/ring/activation/rollout.json"

unsafe=$root/unsafe-failure
make_fixture "$unsafe"
if run_helper "$unsafe" FAKE_DEPLOY_FAIL=1 FAKE_DEPLOY_UNSAFE=1; then exit 1; fi
test "$(cat "$unsafe/state/producer-active")" = 0
test ! -e "$unsafe/ring/activation/rollout.json"

interrupted=$root/interrupted-resume
make_fixture "$interrupted"
if run_helper "$interrupted" FAKE_DEPLOY_INTERRUPT=1; then exit 1; fi
interrupted_pending=$interrupted/ring/activation/20260822T195013Z-funded-signer-rollout.pending.json
test "$(cat "$interrupted/state/producer-active")" = 0
test "$(cat "$interrupted/state/deploy-count")" = 1
test "$(cat "$interrupted/state/signer-image")" = "$new_image"
test -e "$interrupted_pending"
test ! -e "$interrupted/ring/activation/rollout.json"
run_helper "$interrupted"
test "$(cat "$interrupted/state/producer-active")" = 1
test "$(cat "$interrupted/state/deploy-count")" = 1
test -e "$interrupted_pending"
test -e "$interrupted/ring/activation/rollout.json"
/usr/bin/jq -e --arg path "$interrupted_pending" \
  --arg sha "sha256:$(sha256sum "$interrupted_pending" | cut -d' ' -f1)" \
  '.pendingRollout == {path:$path,sha256:$sha} and .producerRestored == true' \
  "$interrupted/ring/activation/rollout.json" >/dev/null

drift=$root/interrupted-drift
make_fixture "$drift"
if run_helper "$drift" FAKE_DEPLOY_INTERRUPT=1; then exit 1; fi
printf '# drift\n' >>"$drift/signer.container"
if run_helper "$drift"; then exit 1; fi
test "$(cat "$drift/state/producer-active")" = 0
test "$(cat "$drift/state/deploy-count")" = 1
test -e "$drift/ring/activation/20260822T195013Z-funded-signer-rollout.pending.json"
test ! -e "$drift/ring/activation/rollout.json"

printf 'funded post-recovery rollout tests passed\n'
