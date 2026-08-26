#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "$0")/../../.." && pwd); script="$root/ops/conduit/bin/polyedge-funded-signer-post-redemption-rollout-20260824"
bash -n "$script"; tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT; mkdir -p "$tmp/bin" "$tmp/out" "$tmp/out-gap" "$tmp/out-bad" "$tmp/out-alert"
for n in systemctl podman journalctl runuser az guard; do : >"$tmp/bin/$n"; chmod 755 "$tmp/bin/$n"; done
cat >"$tmp/bin/systemctl" <<'EOF'
#!/usr/bin/env bash
case "$1" in is-active) [ "$3" = polyedge-parity-hourly.timer ] && exit 1; exit 0;; show) [ "$4" = InvocationID ] && printf '%032d\n' 1 || echo 0;; esac
EOF
cat >"$tmp/bin/podman" <<'EOF'
#!/usr/bin/env bash
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
run(){ env POLYEDGE_TEST_ALLOW_UNPRIVILEGED=1 POLYEDGE_TEST_SYSTEMCTL="$tmp/bin/systemctl" POLYEDGE_TEST_PODMAN="$tmp/bin/podman" POLYEDGE_TEST_JOURNALCTL="$tmp/bin/journalctl" POLYEDGE_TEST_RUNUSER="$tmp/bin/runuser" POLYEDGE_TEST_AZ="$tmp/bin/az" POLYEDGE_TEST_DISK_GUARD="$tmp/bin/guard" POLYEDGE_TEST_UID="$(id -u)" POLYEDGE_TEST_GID="$(id -g)" POLYEDGE_TEST_LOCK_FILE="$tmp/lock" POLYEDGE_POST_REDEMPTION_RUN_ID=venue-redemption-20260824182234412-7ef7b79f POLYEDGE_POST_REDEMPTION_TRANSACTION_HASH=0x676417d72ea63cb7346569caf8d1b2585332a5be97506919aba5e9ed4b51ded1 POLYEDGE_POST_REDEMPTION_CONDITION_ID=0xe79defc301b116f6f911912194a98e4d88be1d37dd37d5ff27be85dc4795600b POLYEDGE_POST_REDEMPTION_SETTLEMENT_BLOB=reports/funded/dynamic-quote/sessions/dynamic-quote-funded-2026-08-13-v10/internal-settlements/ef2057fff0b51ea73541efdd05bf378c18249c51b4bc9f72bcb774d5b9008e37.json POLYEDGE_POST_REDEMPTION_FRESH_AFTER=2026-08-24T18:22:34Z POLYEDGE_POST_REDEMPTION_LIQUID_BEFORE=26.171637 POLYEDGE_POST_REDEMPTION_LIQUID_AFTER=31.171637 POLYEDGE_POST_REDEMPTION_PAYOUT=5 POLYEDGE_POST_REDEMPTION_LIVE_SUMMARY="$tmp/live" POLYEDGE_POST_REDEMPTION_DRY_RUN="$tmp/dry" POLYEDGE_POST_REDEMPTION_SETTLEMENT="$tmp/settlement" POLYEDGE_POST_REDEMPTION_ATTESTATION_DIR="$1" "$script"; }
run "$tmp/out" >/dev/null
receipt="$tmp/out/post-redemption-venue-redemption-20260824182234412-7ef7b79f-attestation.json"; /usr/bin/jq -e '.status=="attested" and (.helperSha256|test("^sha256:[0-9a-f]{64}$")) and .authorizedDeadLetterBaseline==7 and .runtime.signer.restartCount==0 and .runtime.producer.revision=="bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" and .runtime.producer.status=="running" and .heartbeat.automaticRedemptionEnabled==true and .heartbeat.redemptionFailures==0 and .evidence.liveSummary.path != null and .servicesMutated==false' "$receipt" >/dev/null
sed -i 's/"market_channel_gaps":0/"market_channel_gaps":1/' "$tmp/bin/journalctl"
if run "$tmp/out-gap" >/dev/null 2>&1; then echo "websocket gap unexpectedly attested" >&2; exit 1; fi
sed -i 's/"market_channel_gaps":1/"market_channel_gaps":0/' "$tmp/bin/journalctl"
sed -i 's/nothing_to_redeem/not_redeemed/' "$tmp/dry"
if run "$tmp/out-bad" >/dev/null 2>&1; then echo "bad follow-up unexpectedly attested" >&2; exit 1; fi
sed -i 's/not_redeemed/nothing_to_redeem/' "$tmp/dry"
cat >>"$tmp/bin/journalctl" <<'EOF'
alert='{"schema":"polyedge.funded_direct_alert.v1","status":"websocket_gap_or_reconciliation_required"}'; /usr/bin/jq -cn --argjson n "$now" --arg m "$alert" '{__REALTIME_TIMESTAMP:($n|tostring),_SYSTEMD_INVOCATION_ID:"00000000000000000000000000000001",CONTAINER_ID_FULL:"0000000000000000000000000000000000000000000000000000000000000001",MESSAGE:$m}'
EOF
if run "$tmp/out-alert" >/dev/null 2>&1; then echo "funded alert unexpectedly attested" >&2; exit 1; fi
echo "post-deployment attestation mocked test passed"
