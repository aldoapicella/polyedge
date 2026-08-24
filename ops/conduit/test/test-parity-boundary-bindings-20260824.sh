#!/usr/bin/env bash
set -euo pipefail

repo=$(cd "$(dirname "$0")/../../.." && pwd)
generator=$repo/ops/conduit/bin/polyedge-parity-generate-boundary-bindings-20260824
root=$(mktemp -d)
trap 'rm -rf "$root"' EXIT

ring=$root/ring
activation=$ring/parity/activation
evidence_root=$root/evidence
rollback=$root/rollback
formal=$root/parity-hourly.env
hourly_env=$root/hourly.env
old_ledger=$ring/parity/20260824T080000Z-funded-active.json
mkdir -p "$activation" "$evidence_root" "$rollback"
printf '%s\n' '{"schemaVersion":1,"status":"in_progress","acceptedCleanLiveHours":0}' >"$old_ledger"
printf 'POLYEDGE_PARITY_LEDGER=%s\n' "$old_ledger" >"$formal"
printf 'POLYEDGE_PARITY_WINDOW_START_UTC=2026-08-24T08:00:00Z\nPOLYEDGE_PARITY_LEDGER=%s\n' "$old_ledger" >"$hourly_env"

now_epoch=$(date -u +%s)
window_epoch=$(( (now_epoch / 3600 + 2) * 3600 ))
window=$(date -u -d "@$window_epoch" +%Y-%m-%dT%H:%M:%SZ)
created=$(date -u -d "@$now_epoch" +%Y-%m-%dT%H:%M:%SZ)
live_finished=$(date -u -d "@$((now_epoch - 120))" +%Y-%m-%dT%H:%M:%S.123Z)
dry_finished=$(date -u -d "@$((now_epoch - 60))" +%Y-%m-%dT%H:%M:%S.456Z)
transaction=0x$(printf 'a%.0s' {1..64})
settlement_blob=reports/funded/session/internal-settlements/fixture.json
live=$evidence_root/live-summary.json
dry=$evidence_root/follow-up-dry-run.json
settlement=$evidence_root/internal-settlement.json
jq -n --arg finished "$live_finished" --arg transaction "$transaction" --arg blob "$settlement_blob" '{
  schema_version:1,status:"redeemed_and_verified",dry_run:false,finished_ts:$finished,
  run_id:"fixture-redemption",transaction_hash:$transaction,redemption_submitted:true,
  liquid_collateral_before:10,liquid_collateral_after:10.02,realized_payout:0.02,zero_open_orders_confirmed:true,
  internal_settlement_blobs:[$blob]
}' >"$live"
jq -n --arg finished "$dry_finished" '{schema_version:1,status:"nothing_to_redeem",dry_run:true,
  finished_ts:$finished,zero_open_orders_confirmed:true,selection:{selected:[]},redemption_submitted:false}' >"$dry"
jq -n --arg transaction "$transaction" '{schema:"polyedge.verified_internal_settlement.v1",
  transaction_hash:$transaction,payout:0.02,receipt_confirmations:3}' >"$settlement"
live_sha=sha256:$(sha256sum "$live" | cut -d' ' -f1)
dry_sha=sha256:$(sha256sum "$dry" | cut -d' ' -f1)
settlement_sha=sha256:$(sha256sum "$settlement" | cut -d' ' -f1)

attestation=$activation/post-redemption-fixture-attestation.json
jq -n --arg created "$created" --arg transaction "$transaction" --arg blob "$settlement_blob" \
  --arg live "$live" --arg live_sha "$live_sha" --arg dry "$dry" --arg dry_sha "$dry_sha" \
  --arg settlement "$settlement" --arg settlement_sha "$settlement_sha" --argjson heartbeat "$((now_epoch - 5))" '{
  schema:"polyedge.funded_signer_post_redemption_attestation.v1",status:"attested",createdAtUtc:$created,
  helperSha256:("sha256:" + ("d" * 64)),authorizedDeadLetterBaseline:1332,
  redemption:{transactionHash:$transaction,settlementBlob:$blob},
  evidence:{liveSummary:{path:$live,sha256:$live_sha},followUpDryRun:{path:$dry,sha256:$dry_sha},
    internalSettlement:{path:$settlement,sha256:$settlement_sha}},
  runtime:{signer:{invocationId:("e" * 32),containerId:("f" * 64),restartCount:0,
      image:("ghcr.io/aldoapicella/polyedge-venue-probe@sha256:" + ("b" * 64)),revision:("c" * 40),user:"986:982"},
    producer:{invocationId:("1" * 32),containerId:("2" * 64),restartCount:0,
      image:("ghcr.io/aldoapicella/polyedge-rust-backend@sha256:" + ("3" * 64)),revision:("4" * 40),
      user:"984:980",status:"running",health:"healthy"}},
  heartbeat:{capturedAtEpoch:$heartbeat,processedMessages:9,failedMessages:0,failedAttempts:0,
    executor:{busy:false,user_channel_ready:true,market_channel_ready:true,user_channel_gaps:0,market_channel_gaps:0,
      user_channel_unparsed:0,market_channel_unparsed:0,reconnect_reconciliation_required:false,
      safety_snapshot_cache_ready:true,safety_snapshot_cache_age_ms:100,safety_snapshot_open_order_count:0,
      safety_snapshot_unresolved_position_count:0,safety_snapshot_unresolved_risk_reservation_count:0,
      safety_snapshot_cache_error:null,risk_reservation_index_ready:true}},
  queue:{before:{status:"Active",activeMessageCount:0,scheduledMessageCount:0,deadLetterMessageCount:1333},
    after:{status:"Active",activeMessageCount:0,scheduledMessageCount:0,deadLetterMessageCount:1332},deadLetterNonGrowth:true},
  servicesMutated:false,staleRecoveryReceiptsAccepted:false,parityTimerRemainsPaused:true,azureDeletionAllowed:false
}' >"$attestation"

run_generator() {
  POLYEDGE_PARITY_RING_ROOT=$ring POLYEDGE_PARITY_ROLLBACK_ROOT=$rollback \
  POLYEDGE_PARITY_FORMAL_ENV_FILE=$formal POLYEDGE_PARITY_HOURLY_ENV_FILE=$hourly_env \
    "$generator" "$@"
}

bundle=$root/bundle.json
run_generator "$window" "$attestation" >"$bundle"

window_tag=$(date -u -d "@$window_epoch" +%Y%m%dT%H%M%SZ)
jq -e --arg window "$window" --argjson epoch "$window_epoch" --arg old "$old_ledger" \
  --arg ledger "$ring/parity/${window_tag}-funded-active.json" --arg attestation "$attestation" '
  .schema == "polyedge.parity_boundary_binding_review.v1" and
  .activationAllowed == false and .reviewRequired == true and
  .windowStartUtc == $window and .windowStartEpoch == $epoch and
  .collectNotBeforeEpoch == ($epoch + 4680) and .enableDeadlineEpoch == ($epoch + 7200) and
  .paths.ledger == $ledger and .rollbackSource.ledger.path == $old and
  .evidence.postRedemptionAttestation.path == $attestation and .evidence.authorizedDeadLetterBaseline == 1332 and
  .runtime.signer.invocationId == ("e" * 32) and .runtime.signer.containerId == ("f" * 64) and
  .runtime.producer.invocationId == ("1" * 32) and .runtime.producer.containerId == ("2" * 64) and
  .runtime.producer.revision == ("4" * 40) and
  (.sourceBasis | keys | sort) == ["daily","firstHour","hourly","reboot","stage"]
' "$bundle" >/dev/null

bad_queue=$activation/bad-queue-attestation.json
jq '.queue.after.deadLetterMessageCount = 1333' "$attestation" >"$bad_queue"
if run_generator "$window" "$bad_queue" >/dev/null 2>&1; then
  echo 'generator accepted an attestation whose queue differs from its DLQ baseline' >&2
  exit 1
fi

bad_runtime=$activation/bad-runtime-attestation.json
jq '.runtime.signer.restartCount = 1' "$attestation" >"$bad_runtime"
if run_generator "$window" "$bad_runtime" >/dev/null 2>&1; then
  echo 'generator accepted restarted signer provenance' >&2
  exit 1
fi

bad_producer_revision=$activation/bad-producer-revision-attestation.json
jq '.runtime.producer.revision = "not-a-source-revision"' "$attestation" >"$bad_producer_revision"
if run_generator "$window" "$bad_producer_revision" >/dev/null 2>&1; then
  echo 'generator accepted malformed producer source provenance' >&2
  exit 1
fi

bad_zero=$activation/bad-zero-state-attestation.json
jq '.heartbeat.executor.safety_snapshot_open_order_count = 1' "$attestation" >"$bad_zero"
if run_generator "$window" "$bad_zero" >/dev/null 2>&1; then
  echo 'generator accepted a non-zero signer safety snapshot' >&2
  exit 1
fi

if run_generator "${window/:00:00Z/:15:00Z}" "$attestation" >/dev/null 2>&1; then
  echo 'generator accepted a non-hour UTC boundary' >&2
  exit 1
fi

bad_dry=$evidence_root/out-of-order-dry-run.json
jq --arg finished "$(date -u -d "@$((now_epoch - 180))" +%Y-%m-%dT%H:%M:%SZ)" '.finished_ts = $finished' "$dry" >"$bad_dry"
bad_order=$activation/bad-order-attestation.json
jq --arg path "$bad_dry" --arg sha "sha256:$(sha256sum "$bad_dry" | cut -d' ' -f1)" \
  '.evidence.followUpDryRun = {path:$path,sha256:$sha}' "$attestation" >"$bad_order"
if run_generator "$window" "$bad_order" >/dev/null 2>&1; then
  echo 'generator accepted out-of-order source evidence' >&2
  exit 1
fi

printf '%s\n' '{"schema_version":1,"status":"changed"}' >"$live"
if run_generator "$window" "$attestation" >/dev/null 2>&1; then
  echo 'generator accepted changed live-redemption evidence bytes' >&2
  exit 1
fi

! grep -Eq '20260824T083626Z|funded_signer_post_recovery_rollout|authorizedDeadLetterBaseline:1311' "$generator"
echo 'parity boundary binding generator tests passed'
