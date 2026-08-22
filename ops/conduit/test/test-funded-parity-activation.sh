#!/usr/bin/env bash
set -euo pipefail

repo=$(cd "$(dirname "$0")/../../.." && pwd)
stage=$repo/ops/conduit/bin/polyedge-parity-stage-funded-active
first=$repo/ops/conduit/bin/polyedge-parity-collect-first-hour
root=$(mktemp -d)
trap 'rm -rf "$root"' EXIT

fixture_recovery=$root/recovery.json
fixture_rollout=$root/rollout.json
jq -n '{
  schema:"polyedge.acknowledged_no_fill_reconciliation.v1",status:"finalized_no_fill",
  decision_id:"96f92c50f5c583cfaa0bc3be5db780a742ec87140610ba9e0e1d4874dd9e0810",
  run_id:"funded-direct-20260821194914083-5cc133ce",
  order_id:"0xb239b7c3c104d591a3eae9d87922313c78b274ebfcc0885e313253124b3386a9",
  order_submission_attempted:true,recovery_order_submission_attempted:false,
  recovery_grant_consumed:false,recovery_risk_reservation_created:false,
  reconciliation_reason:"acknowledged_evicted_order_no_fill",evidence:{observation_ms:10000},
  recoveryImage:"ghcr.io/aldoapicella/polyedge-venue-probe@sha256:ab7caf1990755289a134533654a4d5b61432c1b39a45bbf1d9665b51de237c03",
  reservationEvidence:{blob:"reports/research/venue-probe/risk-reservations/2026-08-21/funded-direct-96f92c50f5c583cfaa0bc3be5db780a742ec87140610ba9e0e1d4874dd9e0810.json",sha256:"sha256:c9486181df09a2d1bdd0cc90b74836e529e70f73479f15290663e6db0fe6e9d7"},
  completionEvidence:{blob:"reports/funded/dynamic-quote/sessions/dynamic-quote-funded-2026-08-13-v10/completed/96f92c50f5c583cfaa0bc3be5db780a742ec87140610ba9e0e1d4874dd9e0810.json",sha256:"sha256:32c6a39c0336ceb1902bcf8b92008d27d27d05bb9add23dd8c608389ed1c9477"},
  summaryEvidence:{blob:"reports/research/venue-probe/runs/2026-08-21/funded-direct-20260821194914083-5cc133ce/summary.json",sha256:"sha256:59025de4af33db8bc7cda1d9c6505575f0ad7f43620c5f93ff1f82898e7dd347"},
  unresolvedReservationsAfter:0,queueActiveMessages:0,queueScheduledMessages:0,queueDeadLetterMessages:1037,
  parityTimerRemainsPaused:true,azureDeletionAllowed:false,cutoverCompletedAtUtc:"2026-08-22T19:51:00Z"
}' >"$fixture_recovery"
fixture_recovery_sha=sha256:$(sha256sum "$fixture_recovery" | cut -d' ' -f1)
new_image=ghcr.io/aldoapicella/polyedge-venue-probe@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc
revision=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
jq -n --arg image "$new_image" --arg revision "$revision" --arg recovery "$fixture_recovery" --arg recovery_sha "$fixture_recovery_sha" '{
  schema:"polyedge.funded_signer_post_recovery_rollout.v1",status:"validated",
  oldImage:"ghcr.io/aldoapicella/polyedge-venue-probe@sha256:ab7caf1990755289a134533654a4d5b61432c1b39a45bbf1d9665b51de237c03",
  newImage:$image,newRevision:$revision,recoveryReceipt:{path:$recovery,sha256:$recovery_sha},
  producerRestored:true,unresolvedReservationsAfter:0,
  queue:{status:"Active",activeMessageCount:0,scheduledMessageCount:0,deadLetterMessageCount:1037},
  parityTimerRemainsPaused:true,azureDeletionAllowed:false
}' >"$fixture_rollout"

(
  POLYEDGE_TEST_SOURCE_ONLY=1 . "$stage"
  validate_recovery_receipt "$fixture_recovery"
  validate_rollout_receipt "$fixture_recovery" "$fixture_rollout"
  test "$signer_image" = "$new_image"
  test "$signer_revision" = "$revision"
  test "$service_bus_dlq" = 1037

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
  grep -qx 'POLYEDGE_PARITY_EXPECTED_FUNDED_SERVICE_BUS_DLQ=1037' "$rendered"
  printf '%s\n' 'POLYEDGE_PARITY_FUNDED_MODE=active' >>"$source_env"
  if render_env "$source_env" "$root/duplicate.env" "${keys[@]}"; then exit 1; fi

  jq '.producerRestored=false' "$fixture_rollout" >"$root/bad-rollout.json"
  if validate_rollout_receipt "$fixture_recovery" "$root/bad-rollout.json"; then exit 1; fi
)

(
  POLYEDGE_TEST_SOURCE_ONLY=1 . "$first"
  staged_fixture=$root/staged.json
  jq -n --arg window "$window" --arg ledger "$ledger" --arg recovery "$fixture_recovery" --arg recovery_sha "$fixture_recovery_sha" \
    --arg rollout "$fixture_rollout" --arg rollout_sha "sha256:$(sha256sum "$fixture_rollout" | cut -d' ' -f1)" \
    --arg collector "$collector_sha" --arg validator "$validator_sha" '{
      schemaVersion:1,status:"staged",windowStartUtc:$window,ledger:{path:$ledger},
      recovery:{path:$recovery,sha256:$recovery_sha},rollout:{path:$rollout,sha256:$rollout_sha},
      bindings:{collectorSha256:$collector,validatorSha256:$validator},acceptedCleanLiveHours:0,
      completedDailyCycles:0,rebootRecoveryPassed:false,recurrenceEnabled:false,
      azureAuthoritative:true,azureDeletionAllowed:false
    }' >"$staged_fixture"
  validate_staged_receipt "$staged_fixture" "$fixture_recovery" "$fixture_rollout"
  jq '.bindings.validatorSha256="sha256:0000000000000000000000000000000000000000000000000000000000000000"' \
    "$staged_fixture" >"$root/bad-staged.json"
  if validate_staged_receipt "$root/bad-staged.json" "$fixture_recovery" "$fixture_rollout"; then exit 1; fi
)

printf 'funded parity activation fixture tests passed\n'
