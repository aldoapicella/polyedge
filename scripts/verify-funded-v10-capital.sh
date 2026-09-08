#!/usr/bin/env bash

(
set -Eeuo pipefail

: "${workspace_customer_id:?}"
: "${FUNDED_APP:?}"
: "${funded_revision:?}"
: "${producer_enabled_at:?}"
: "${decision_id:?}"
: "${STORAGE_ACCOUNT:?}"
: "${FUNDED_CONTAINER:?}"

readonly session_id="dynamic-quote-funded-2026-08-13-v10"
readonly settlement_prefix="reports/funded/dynamic-quote/sessions/${session_id}/internal-settlements/"
readonly state_blob="reports/funded/dynamic-quote/sessions/${session_id}/capital-reserve-state.json"
work_dir=$(mktemp -d)
trap 'rm -rf -- "$work_dir"' EXIT

capital_snapshot_ready=false
for attempt in $(seq 1 36); do
  az monitor log-analytics query \
    --workspace "$workspace_customer_id" \
    --analytics-query "ContainerAppConsoleLogs_CL
      | where ContainerAppName_s == '$FUNDED_APP'
      | where RevisionName_s == '$funded_revision'
      | where TimeGenerated >= datetime($producer_enabled_at)
      | extend record = parse_json(Log_s)
      | where tostring(record.schema) == 'polyedge.funded_capital_snapshot.v1'
      | where tostring(record.session_id) == '$session_id'
      | where tostring(record.decision_id) == '$decision_id'
      | top 1 by TimeGenerated asc
      | project TimeGenerated, Log_s" \
    -o json > "$work_dir/capital-snapshot.json"
  if jq -e --arg decision "$decision_id" '
    length == 1
    and (.[0].Log_s | fromjson
      | .session_id == "dynamic-quote-funded-2026-08-13-v10"
      and .decision_id == $decision
      and (.snapshot_source == "persistent_safety_cache"
        or .snapshot_source == "live_preflight")
      and .snapshot_completed_wall_ms > 0
      and .snapshot_age_ms >= 0
      and .snapshot_age_ms <= 650
      and .risk_passed == true
      and (.blockers | length) == 0
      and .open_order_count == 0
      and .unresolved_position_count == 0
      and .unresolved_risk_reservation_count == 0
      and .account_equity > 0
      and .historical_high_water_equity >= 102.78112
      and .reserve_basis == "fully_reconciled_current_equity"
      and .continue_after_loss == true
      and (.protected_reserve - ([2, (.account_equity * 0.1)] | max) | fabs) <= 0.0000011
      and (.operating_buffer - (.account_equity * 0.01) | fabs) <= 0.0000011
      and (.operable_capital - ([0, (.account_equity - .protected_reserve - .operating_buffer)] | max) | fabs) <= 0.0000011
      and .order_notional > 0
      and .order_notional < 10.5
      and .proposed_notional >= .order_notional
      and .proposed_notional <= .operable_capital)
  ' "$work_dir/capital-snapshot.json" >/dev/null; then
    capital_snapshot_ready=true
    break
  fi
  sleep 10
done
test "$capital_snapshot_ready" = true
snapshot_equity=$(jq -r '.[0].Log_s | fromjson | .account_equity' \
  "$work_dir/capital-snapshot.json")

capture_settlement_inventory() {
  local output=$1
  az storage blob list \
    --account-name "$STORAGE_ACCOUNT" \
    --container-name "$FUNDED_CONTAINER" \
    --prefix "$settlement_prefix" \
    --num-results '*' \
    --auth-mode login \
    --only-show-errors -o json 2>/dev/null \
    | jq -eS --arg prefix "$settlement_prefix" '
        . as $inventory
        | (([.[].name] | length == (unique | length))
        and all(.[].name; test("^" + $prefix + "[0-9a-f]{64}\\.json$"))
        and all(.[].properties;
          (.etag | type == "string") and (.etag | length) > 0
          and (.contentLength | type == "number")
          and .contentLength > 0 and .contentLength <= 16384))
        | if . then
            [ $inventory[] | {
              name,
              etag: .properties.etag,
              size: .properties.contentLength
            } ] | sort_by(.name)
          else error("invalid settlement inventory")
          end
      ' > "$output"
}

capture_state_metadata() {
  local output=$1
  az storage blob show \
    --account-name "$STORAGE_ACCOUNT" \
    --container-name "$FUNDED_CONTAINER" \
    --name "$state_blob" \
    --auth-mode login \
    --only-show-errors -o json 2>/dev/null \
    | jq -eS '
        .properties.etag as $etag
        | .properties.contentLength as $size
        | select(($etag | type == "string") and ($etag | length) > 0)
        | select(($size | type == "number") and $size > 0 and $size <= 16384)
        | {etag: $etag, size: $size}
      ' > "$output"
}

verify_consistent_capital_state() {
  local attempt_dir=$1
  local settlement_ids settlement_pnl state_etag
  mkdir -p "$attempt_dir" || return 1
  capture_settlement_inventory "$attempt_dir/settlements-before.json" || return 1
  capture_state_metadata "$attempt_dir/state-before.json" || return 1
  : > "$attempt_dir/accounting.jsonl"
  jq -r '.[] | [.name, .etag, .size] | @tsv' \
    "$attempt_dir/settlements-before.json" \
    > "$attempt_dir/settlements.tsv" || return 1

  while IFS=$'\t' read -r settlement_blob settlement_etag settlement_size; do
    local settlement_file
    settlement_file=$(mktemp "$attempt_dir/settlement.XXXXXX") || return 1
    az storage blob download \
      --account-name "$STORAGE_ACCOUNT" \
      --container-name "$FUNDED_CONTAINER" \
      --name "$settlement_blob" \
      --file "$settlement_file" \
      --if-match "$settlement_etag" \
      --auth-mode login \
      --overwrite \
      --only-show-errors -o none 2>/dev/null || return 1
    test "$(wc -c < "$settlement_file")" = "$settlement_size" || return 1
    node .github/workflows/verify-funded-v10-settlement.mjs \
      "$settlement_file" "$settlement_blob" \
      >> "$attempt_dir/accounting.jsonl" 2>/dev/null || return 1
  done < "$attempt_dir/settlements.tsv"

  jq -se '([.[].id] | length == (unique | length))' \
    "$attempt_dir/accounting.jsonl" >/dev/null || return 1
  settlement_ids=$(jq -sc '[.[].id] | sort' \
    "$attempt_dir/accounting.jsonl") || return 1
  settlement_pnl=$(jq -s '[.[].realized_pnl] | add // 0' \
    "$attempt_dir/accounting.jsonl") || return 1
  state_etag=$(jq -r '.etag' "$attempt_dir/state-before.json") || return 1
  az storage blob download \
    --account-name "$STORAGE_ACCOUNT" \
    --container-name "$FUNDED_CONTAINER" \
    --name "$state_blob" \
    --file "$attempt_dir/capital-state.json" \
    --if-match "$state_etag" \
    --auth-mode login \
    --overwrite \
    --only-show-errors -o none 2>/dev/null || return 1
  test "$(wc -c < "$attempt_dir/capital-state.json")" = \
    "$(jq -r '.size' "$attempt_dir/state-before.json")" || return 1

  capture_settlement_inventory "$attempt_dir/settlements-after.json" || return 1
  capture_state_metadata "$attempt_dir/state-after.json" || return 1
  cmp "$attempt_dir/settlements-before.json" \
    "$attempt_dir/settlements-after.json" >/dev/null || return 1
  cmp "$attempt_dir/state-before.json" \
    "$attempt_dir/state-after.json" >/dev/null || return 1

  jq -e \
    --argjson snapshot_equity "$snapshot_equity" \
    --argjson settlement_ids "$settlement_ids" \
    --argjson settlement_pnl "$settlement_pnl" '
    .schema == "polyedge.protected_compounding_state.v2"
    and .session_id == "dynamic-quote-funded-2026-08-13-v10"
    and .reconciliation_complete == true
    and .reserve_ratio == 0.1
    and .minimum_reserve == 2
    and .target_order_ratio == 0.05
    and .operating_buffer_ratio == 0.01
    and .minimum_order_notional == 1
    and .reserve_basis == "fully_reconciled_current_equity"
    and .loss_response == "resize_from_fully_reconciled_current_equity"
    and .prior_state_session_id == "dynamic-quote-funded-2026-07-29-v5"
    and .prior_state_blob_name == "reports/funded/dynamic-quote/sessions/dynamic-quote-funded-2026-07-29-v5/capital-reserve-state.json"
    and .prior_state_sha256 == "sha256:9e63db7ef8c22d3af53a14f1858b817b8769ca7ba737e6668a900ffb73330c15"
    and .reserve_monotonic == false
    and .continue_after_loss == true
    and .last_reconciled_equity > 0
    and ((.authorized_equity_ceiling - (29.505501 + $settlement_pnl)) | fabs) <= 0.0000011
    and .last_reconciled_equity <= .authorized_equity_ceiling
    and ((.verified_realized_pnl - $settlement_pnl) | fabs) <= 0.0000011
    and .verified_settlement_ids == $settlement_ids
    and .high_water_equity >= 102.78112
    and .historical_high_water_equity >= 102.78112
    and .high_water_equity >= .authorized_equity_ceiling
    and .high_water_equity >= .last_reconciled_equity
    and .historical_high_water_equity >= .high_water_equity
    and (.last_reconciled_equity - $snapshot_equity | fabs) <= 0.0000011
    and (.protected_reserve - ([2, (.last_reconciled_equity * .reserve_ratio)] | max) | fabs) <= 0.0000011
    and (.operating_buffer - (.last_reconciled_equity * .operating_buffer_ratio) | fabs) <= 0.0000011
    and (.operable_capital - ([0, (.last_reconciled_equity - .protected_reserve - .operating_buffer)] | max) | fabs) <= 0.0000011
    and .operable_capital >= .minimum_order_notional
    and ([keys[] | select(startswith("migration_"))] | length) == 0
  ' "$attempt_dir/capital-state.json" >/dev/null || return 1

  jq -e --slurpfile state "$attempt_dir/capital-state.json" '
    length == 1
    and (.[0].Log_s | fromjson) as $snapshot
    | ($state[0]) as $durable
    | ($snapshot.account_equity - $durable.last_reconciled_equity | fabs) <= 0.0000011
      and ($snapshot.protected_reserve - $durable.protected_reserve | fabs) <= 0.0000011
      and ($snapshot.operating_buffer - $durable.operating_buffer | fabs) <= 0.0000011
      and ($snapshot.operable_capital - $durable.operable_capital | fabs) <= 0.0000011
      and ($snapshot.historical_high_water_equity - $durable.historical_high_water_equity | fabs) <= 0.0000011
      and $snapshot.open_order_count == 0
      and $snapshot.unresolved_position_count == 0
      and $snapshot.unresolved_risk_reservation_count == 0
  ' "$work_dir/capital-snapshot.json" >/dev/null || return 1
}

capital_state_ready=false
for attempt in 1 2 3; do
  if verify_consistent_capital_state "$work_dir/read-$attempt"; then
    capital_state_ready=true
    break
  fi
  sleep 5
done
if [ "$capital_state_ready" != true ]; then
  echo "funded capital verification failed closed" >&2
  exit 1
fi
)
