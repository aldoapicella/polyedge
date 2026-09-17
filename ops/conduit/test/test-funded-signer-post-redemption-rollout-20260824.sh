#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "$0")/../../.." && pwd); script="$root/ops/conduit/bin/polyedge-funded-signer-post-redemption-rollout-20260824"
bash -n "$script"; tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT; mkdir -p "$tmp/bin" "$tmp/out" "$tmp/out-live-noop" "$tmp/out-gap" "$tmp/out-bad" "$tmp/out-alert"
for n in systemctl podman journalctl runuser az guard; do : >"$tmp/bin/$n"; chmod 755 "$tmp/bin/$n"; done
cat >"$tmp/bin/systemctl" <<'EOF'
#!/usr/bin/env bash
case "$1" in is-active) [ "$3" = polyedge-parity-hourly.timer ] && exit 1; exit 0;; show) [ "$4" = InvocationID ] && printf '%032d\n' 1 || echo 0;; esac
EOF
cat >"$tmp/bin/podman" <<'EOF'
#!/usr/bin/env bash
if [ "${FAKE_RECOVERY_TEST:-0}" = 1 ]; then
  case "$1" in
    run) /usr/bin/jq -e '.bundle==(.bundleBytesBase64|@base64d|fromjson) and .runtime.invocationId!=null and (.journal|length)>0' >/dev/null; cat "$FAKE_RECOVERY_PROOF"; exit;;
    exec)
      if [ "$2" = -i ]; then
        test "$3:$4:$5:$6:$7:$8:$9" = '--workdir:/app:polyedge-funded-signer:node:--input-type=module:-:--collect' || exit 1
        test "${10}:${11}:${12}" = "$POLYEDGE_POST_REDEMPTION_APPROVED_AUTOMATIC_REDEMPTION_CONDITION:$POLYEDGE_POST_REDEMPTION_RECORDING_RECOVERY_IMAGE:$POLYEDGE_POST_REDEMPTION_RECORDING_RECOVERY_REVISION" || exit 1
        cat >/dev/null
        [ "${FAKE_PREFLIGHT_FAIL:-0}" = 0 ] || exit 1
        jq "${FAKE_PREFLIGHT_MUTATION:-.}" "$FAKE_APPROVED_PREFLIGHT"
      else case "${6:-}" in *tenant*) echo '{"tenant":"11111111-1111-1111-1111-111111111111","client":"22222222-2222-2222-2222-222222222222"}';; *) echo "${FAKE_UNRESOLVED:-0}";; esac; fi; exit;;
    image) if [[ "${4:-}" == *'{{.Os}}/{{.Architecture}}|'* ]]; then echo "linux/arm64|$POLYEDGE_POST_REDEMPTION_RECORDING_RECOVERY_REVISION"; exit; fi;;
  esac
fi
case "$1:$3" in
inspect:'{{.Id}}') [ "$4" = polyedge-funded-intent-producer ] && printf "%064d\n" 2 || printf "%064d\n" 1;;
inspect:'{{.Config.Image}}') [ "$4" = polyedge-funded-intent-producer ] && echo ghcr.io/aldoapicella/polyedge-rust-backend@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc || echo ghcr.io/aldoapicella/polyedge-venue-probe@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa;;
inspect:'{{.Config.User}}') [ "$4" = polyedge-funded-intent-producer ] && echo 984:980 || echo 986:982;;
inspect:'{{.State.Status}}|{{.State.Health.Status}}') echo running\|healthy;;
image:--format) case "$4" in '{{ index .Labels "org.opencontainers.image.revision" }}') echo bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb;; '{{.Os}}/{{.Architecture}}') echo linux/arm64;; esac;;
esac
EOF
cat >"$tmp/bin/journalctl" <<'EOF'
#!/usr/bin/env bash
now=$(( $(date +%s)*1000000 )); started='{"schema":"polyedge.funded_direct_service.v2","status":"persistent_service_started","automatic_redemption_enabled":true}'; heartbeat='{"schema":"polyedge.funded_direct_service.v2","status":"persistent_service_heartbeat","processed_messages":2,"failed_messages":0,"failed_attempts":0,"redemption_failures":0,"executor":{"busy":false,"user_channel_ready":true,"market_channel_ready":true,"user_channel_gaps":0,"market_channel_gaps":0,"user_channel_unparsed":0,"market_channel_unparsed":0,"reconnect_reconciliation_required":false,"safety_snapshot_cache_ready":true,"safety_snapshot_cache_age_ms":0,"safety_snapshot_open_order_count":0,"safety_snapshot_unresolved_position_count":0,"safety_snapshot_unresolved_risk_reservation_count":0,"safety_snapshot_cache_error":null,"risk_reservation_index_ready":true}}'; /usr/bin/jq -cn --argjson n "$now" --arg m "$started" '{__REALTIME_TIMESTAMP:(($n-1000000)|tostring),_SYSTEMD_INVOCATION_ID:"00000000000000000000000000000001",CONTAINER_ID_FULL:"0000000000000000000000000000000000000000000000000000000000000001",MESSAGE:$m}'; /usr/bin/jq -cn --argjson n "$now" --arg m "$heartbeat" '{__REALTIME_TIMESTAMP:($n|tostring),_SYSTEMD_INVOCATION_ID:"00000000000000000000000000000001",CONTAINER_ID_FULL:"0000000000000000000000000000000000000000000000000000000000000001",MESSAGE:$m}'
EOF
cat >"$tmp/bin/runuser" <<'EOF'
#!/usr/bin/env bash
shift 3; exec "$@"
EOF
cat >"$tmp/bin/az" <<'EOF'
#!/usr/bin/env bash
echo '{"status":"Active","countDetails":{"activeMessageCount":0,"scheduledMessageCount":0,"deadLetterMessageCount":7}}'
EOF
printf '#!/usr/bin/env bash\nexit 0\n' >"$tmp/bin/guard"
printf '%s' '{"schema_version":1,"status":"redeemed_and_verified","dry_run":false,"run_id":"venue-redemption-20260824182234412-7ef7b79f","transaction_hash":"0x676417d72ea63cb7346569caf8d1b2585332a5be97506919aba5e9ed4b51ded1","redemption_submitted":true,"finished_ts":"2026-08-24T18:25:10Z","liquid_collateral_before":26.171637,"liquid_collateral_after":31.171637,"realized_payout":5,"zero_open_orders_confirmed":true,"selection":{"selected_gross_payout":5,"selected":[{"condition_id":"0xe79defc301b116f6f911912194a98e4d88be1d37dd37d5ff27be85dc4795600b"}]},"internal_settlement_blobs":["reports/funded/dynamic-quote/sessions/dynamic-quote-funded-2026-08-13-v10/internal-settlements/ef2057fff0b51ea73541efdd05bf378c18249c51b4bc9f72bcb774d5b9008e37.json"]}' >"$tmp/live"
printf '%s' '{"schema_version":1,"status":"nothing_to_redeem","dry_run":true,"finished_ts":"2026-08-24T18:27:22Z","zero_open_orders_confirmed":true,"portfolio":{"redeemable_winner_count":0},"selection":{"selected_gross_payout":0,"selected":[]},"redemption_submitted":false}' >"$tmp/dry"
printf '%s' '{"schema":"polyedge.verified_internal_settlement.v1","condition_id":"0xe79defc301b116f6f911912194a98e4d88be1d37dd37d5ff27be85dc4795600b","transaction_hash":"0x676417d72ea63cb7346569caf8d1b2585332a5be97506919aba5e9ed4b51ded1","payout":5,"receipt_confirmations":2}' >"$tmp/settlement"
chmod 640 "$tmp/live" "$tmp/dry" "$tmp/settlement"
cat >>"$tmp/bin/journalctl" <<'EOF'
old_alert='{"schema":"polyedge.funded_direct_alert.v1","status":"recovered_before_health_baseline"}'; /usr/bin/jq -cn --argjson n "$now" --arg m "$old_alert" '{__REALTIME_TIMESTAMP:(($n-120000000)|tostring),_SYSTEMD_INVOCATION_ID:"00000000000000000000000000000001",CONTAINER_ID_FULL:"0000000000000000000000000000000000000000000000000000000000000001",MESSAGE:$m}'
EOF
run(){ local output_dir=$1; shift; env POLYEDGE_TEST_ALLOW_UNPRIVILEGED=1 POLYEDGE_TEST_SYSTEMCTL="$tmp/bin/systemctl" POLYEDGE_TEST_PODMAN="$tmp/bin/podman" POLYEDGE_TEST_JOURNALCTL="$tmp/bin/journalctl" POLYEDGE_TEST_RUNUSER="$tmp/bin/runuser" POLYEDGE_TEST_AZ="$tmp/bin/az" POLYEDGE_TEST_DISK_GUARD="$tmp/bin/guard" POLYEDGE_TEST_UID="$(id -u)" POLYEDGE_TEST_GID="$(id -g)" POLYEDGE_TEST_LOCK_FILE="$tmp/lock" POLYEDGE_POST_REDEMPTION_RUN_ID=venue-redemption-20260824182234412-7ef7b79f POLYEDGE_POST_REDEMPTION_TRANSACTION_HASH=0x676417d72ea63cb7346569caf8d1b2585332a5be97506919aba5e9ed4b51ded1 POLYEDGE_POST_REDEMPTION_CONDITION_ID=0xe79defc301b116f6f911912194a98e4d88be1d37dd37d5ff27be85dc4795600b POLYEDGE_POST_REDEMPTION_SETTLEMENT_BLOB=reports/funded/dynamic-quote/sessions/dynamic-quote-funded-2026-08-13-v10/internal-settlements/ef2057fff0b51ea73541efdd05bf378c18249c51b4bc9f72bcb774d5b9008e37.json POLYEDGE_POST_REDEMPTION_FRESH_AFTER=2026-08-24T18:22:34Z POLYEDGE_POST_REDEMPTION_HEALTH_AFTER="$(date -u -d '60 seconds ago' +%Y-%m-%dT%H:%M:%SZ)" POLYEDGE_POST_REDEMPTION_LIQUID_BEFORE=26.171637 POLYEDGE_POST_REDEMPTION_LIQUID_AFTER=31.171637 POLYEDGE_POST_REDEMPTION_PAYOUT=5 POLYEDGE_POST_REDEMPTION_LIVE_SUMMARY="$tmp/live" POLYEDGE_POST_REDEMPTION_DRY_RUN="$tmp/dry" POLYEDGE_POST_REDEMPTION_SETTLEMENT="$tmp/settlement" POLYEDGE_POST_REDEMPTION_ATTESTATION_DIR="$output_dir" "$@" "$script"; }
run "$tmp/out" >/dev/null
receipt="$tmp/out/post-redemption-venue-redemption-20260824182234412-7ef7b79f-attestation.json"; /usr/bin/jq -e '.status=="attested" and (.helperSha256|test("^sha256:[0-9a-f]{64}$")) and .authorizedDeadLetterBaseline==7 and .runtime.signer.restartCount==0 and .runtime.producer.revision=="bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" and .runtime.producer.status=="running" and .heartbeat.automaticRedemptionEnabled==true and (.heartbeat.healthBaselineAtEpoch|type=="number") and .heartbeat.redemptionFailures==0 and .evidence.liveSummary.path != null and .servicesMutated==false' "$receipt" >/dev/null
sed -i 's/"dry_run":true/"dry_run":false,"redemption_enabled":true/' "$tmp/dry"
run "$tmp/out-live-noop" >/dev/null
sed -i 's/"dry_run":false,"redemption_enabled":true/"dry_run":true/' "$tmp/dry"
sed -i 's/"market_channel_gaps":0/"market_channel_gaps":1/' "$tmp/bin/journalctl"
if run "$tmp/out-gap" >/dev/null 2>&1; then echo "websocket gap unexpectedly attested" >&2; exit 1; fi
sed -i 's/"market_channel_gaps":1/"market_channel_gaps":0/' "$tmp/bin/journalctl"
sed -i 's/nothing_to_redeem/not_redeemed/' "$tmp/dry"
if run "$tmp/out-bad" >/dev/null 2>&1; then echo "bad follow-up unexpectedly attested" >&2; exit 1; fi
sed -i 's/not_redeemed/nothing_to_redeem/' "$tmp/dry"
# Exercise the proof-producing branch with bounded process/storage mocks.
printf 'token' >"$tmp/token"; chmod 600 "$tmp/token"
printf '{"documents":{}}\n' >"$tmp/source-bundle"; chmod 640 "$tmp/source-bundle"
cat >"$tmp/bin/stat" <<'EOF'
#!/usr/bin/env bash
if [ "$2" = '%u:%g:%a:%h' ]; then echo '986:982:600:1'; else exec /usr/bin/stat "$@"; fi
EOF
chmod 755 "$tmp/bin/stat"
jq --arg ts "$(date -u +%Y-%m-%dT%H:%M:%SZ)" '.finished_ts=$ts' "$tmp/dry" >"$tmp/dry-fresh"; mv "$tmp/dry-fresh" "$tmp/dry"; chmod 640 "$tmp/dry"
jq -n --arg live "sha256:$(sha256sum "$tmp/live"|cut -d' ' -f1)" --arg settlement "sha256:$(sha256sum "$tmp/settlement"|cut -d' ' -f1)" --arg bundle "sha256:$(sha256sum "$tmp/source-bundle"|cut -d' ' -f1)" --argjson since "$(( $(date -u +%s) - 120 ))" '{status:"verified_terminal_no_order",authenticatedSourcesVerified:true,readOnly:true,cleanSinceEpoch:$since,sourceBundleSha256:$bundle,redemption:{summarySha256:$live,settlementSha256:$settlement}}' >"$tmp/proof"
run_recovery(){ local output=$1; shift; run "$output" PATH="$tmp/bin:$PATH" FAKE_RECOVERY_TEST=1 FAKE_RECOVERY_PROOF="$tmp/proof" POLYEDGE_TEST_TOKEN_FILE="$tmp/token" POLYEDGE_POST_REDEMPTION_RECORDING_RECOVERY_INPUTS="$tmp/source-bundle" POLYEDGE_POST_REDEMPTION_RECORDING_RECOVERY_IMAGE=ghcr.io/aldoapicella/polyedge-venue-probe@sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd POLYEDGE_POST_REDEMPTION_RECORDING_RECOVERY_REVISION=eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee "$@"; }
run_recovery "$tmp/out-recovery" >/dev/null
jq -e '.status=="recording_recovered" and .sourceBundle.path!=null and .proof.authenticatedSourcesVerified==true and .originalGuardedDeploymentClaimed==false and .servicesMutated==false' "$tmp/out-recovery/recording-recovery-00000000000000000000000000000001.json" >/dev/null
if run_recovery "$tmp/out-unresolved" FAKE_UNRESOLVED=1 >/dev/null 2>&1; then echo 'unresolved risk unexpectedly attested' >&2; exit 1; fi
jq '.finished_ts="2026-01-01T00:00:00Z"' "$tmp/dry" >"$tmp/dry-stale"; mv "$tmp/dry-stale" "$tmp/dry"; chmod 640 "$tmp/dry"
if run_recovery "$tmp/out-stale" >/dev/null 2>&1; then echo 'stale follow-up unexpectedly attested' >&2; exit 1; fi
jq --arg ts "$(date -u +%Y-%m-%dT%H:%M:%SZ)" '.finished_ts=$ts' "$tmp/dry" >"$tmp/dry-fresh"; mv "$tmp/dry-fresh" "$tmp/dry"; chmod 640 "$tmp/dry"
approved_condition=0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
jq -n --arg condition "$approved_condition" '{schema:"polyedge.approved_redemption_preflight.v1",status:"approved_redemption_ready",
  read_only:true,redemption_submitted:false,condition_id:$condition,
  target_image:("ghcr.io/aldoapicella/polyedge-venue-probe@sha256:"+("d"*64)),target_revision:("e"*40),
  open_order_count:0,unresolved_position_count:0,unresolved_reservation_count:0,position_inventory:{complete:true,readbacks:2},
  selection:{available_winner_conditions:1,skipped_winner_conditions:0,selected_gross_payout:5,selected:[{condition_id:$condition}]}}' >"$tmp/approved-preflight"
run_approved(){ local output=$1; shift; run_recovery "$output" FAKE_APPROVED_PREFLIGHT="$tmp/approved-preflight" \
  POLYEDGE_POST_REDEMPTION_DRY_RUN= POLYEDGE_POST_REDEMPTION_APPROVED_AUTOMATIC_REDEMPTION_CONDITION="$approved_condition" \
  POLYEDGE_POST_REDEMPTION_REDEMPTION_PREFLIGHT_COLLECTOR="$root/ops/conduit/bin/polyedge-funded-redemption-preflight.mjs" "$@"; }
run_approved "$tmp/out-approved" >/dev/null
jq -e --arg condition "$approved_condition" '.evidence.followUpDryRun==null and
  .evidence.approvedRedemptionPreflight.proof.condition_id==$condition and
  (.evidence.approvedRedemptionPreflight.collector.sha256|test("^sha256:[0-9a-f]{64}$")) and .servicesMutated==false' \
  "$tmp/out-approved/post-redemption-venue-redemption-20260824182234412-7ef7b79f-attestation.json" >/dev/null
jq -se '.[0].approvedRedemptionPreflight==.[1].evidence.approvedRedemptionPreflight' \
  "$tmp/out-approved/recording-recovery-00000000000000000000000000000001.json" \
  "$tmp/out-approved/post-redemption-venue-redemption-20260824182234412-7ef7b79f-attestation.json" >/dev/null
for bad_env in FAKE_PREFLIGHT_FAIL=1 FAKE_PREFLIGHT_MUTATION=.unresolved_position_count=1 FAKE_PREFLIGHT_MUTATION=.position_inventory.complete=false POLYEDGE_POST_REDEMPTION_RECORDING_RECOVERY_INPUTS=; do
  output=$tmp/out-approved-bad-$RANDOM
  if run_approved "$output" "$bad_env" >/dev/null 2>&1; then echo "unsafe active approved preflight attested: $bad_env" >&2; exit 1; fi
  test ! -e "$output/recording-recovery-00000000000000000000000000000001.json"
done
cat >"$tmp/bin/queue-snapshot" <<'EOF'
#!/usr/bin/env bash
[ "${FAKE_OCI_SNAPSHOT_FAIL:-0}" = 0 ] || exit 1
printf '%s\n' '{"backend":"oci","status":"observed_zero","readOnly":true,"archiveDlq":{"objectCount":1824,"inventorySha256":"fixed","exhaustiveListing":true}}'
EOF
chmod 755 "$tmp/bin/queue-snapshot"
run_approved "$tmp/out-oci" POLYEDGE_POST_REDEMPTION_QUEUE_BACKEND=oci POLYEDGE_POST_REDEMPTION_QUEUE_SNAPSHOT_HELPER="$tmp/bin/queue-snapshot" >/dev/null
jq -e '.queue.before.backend=="oci" and .queue.before==.queue.after and .authorizedDeadLetterBaseline==1824 and .servicesMutated==false' "$tmp/out-oci/post-redemption-venue-redemption-20260824182234412-7ef7b79f-attestation.json" >/dev/null
for cause in unsafe_mode failed_read; do
  chmod 755 "$tmp/bin/queue-snapshot"; args=()
  if [ "$cause" = unsafe_mode ]; then chmod 777 "$tmp/bin/queue-snapshot"; else args=(FAKE_OCI_SNAPSHOT_FAIL=1); fi
  if run_approved "$tmp/out-oci-$cause" POLYEDGE_POST_REDEMPTION_QUEUE_BACKEND=oci POLYEDGE_POST_REDEMPTION_QUEUE_SNAPSHOT_HELPER="$tmp/bin/queue-snapshot" "${args[@]}" >/dev/null 2>&1; then echo "unsafe OCI snapshot attested: $cause" >&2; exit 1; fi
done
cat >>"$tmp/bin/journalctl" <<'EOF'
alert='{"schema":"polyedge.funded_direct_alert.v1","status":"websocket_gap_or_reconciliation_required"}'; /usr/bin/jq -cn --argjson n "$now" --arg m "$alert" '{__REALTIME_TIMESTAMP:($n|tostring),_SYSTEMD_INVOCATION_ID:"00000000000000000000000000000001",CONTAINER_ID_FULL:"0000000000000000000000000000000000000000000000000000000000000001",MESSAGE:$m}'
EOF
if run "$tmp/out-alert" >/dev/null 2>&1; then echo "funded alert unexpectedly attested" >&2; exit 1; fi
echo "post-deployment attestation mocked test passed"
