#!/usr/bin/env bash

: "${workspace_customer_id:?}"
: "${FUNDED_APP:?}"
: "${funded_revision:?}"
: "${activation_started_at:?}"
: "${decision_id:?}"
: "${STORAGE_ACCOUNT:?}"
: "${FUNDED_CONTAINER:?}"

capital_snapshot_ready=false
for attempt in $(seq 1 36); do
  az monitor log-analytics query \
    --workspace "$workspace_customer_id" \
    --analytics-query "ContainerAppConsoleLogs_CL
      | where ContainerAppName_s == '$FUNDED_APP'
      | where RevisionName_s == '$funded_revision'
      | where TimeGenerated >= datetime($activation_started_at)
      | extend record = parse_json(Log_s)
      | where tostring(record.schema) == 'polyedge.funded_capital_snapshot.v1'
      | where tostring(record.session_id) == 'dynamic-quote-funded-2026-08-12-v9'
      | where tostring(record.decision_id) == '$decision_id'
      | top 1 by TimeGenerated asc
      | project TimeGenerated, Log_s" \
    -o json > funded-v9-capital-snapshot.json
  if jq -e --arg decision "$decision_id" '
    length == 1
    and (.[0].Log_s | fromjson
      | .session_id == "dynamic-quote-funded-2026-08-12-v9"
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
      and (.account_equity - 31.655501 | fabs) <= 0.01
      and .historical_high_water_equity >= 102.78112
      and .reserve_basis == "fully_reconciled_current_equity"
      and .continue_after_loss == true
      and (.protected_reserve - ([2, (.account_equity * 0.1)] | max) | fabs) <= 0.0000011
      and (.operating_buffer - (.account_equity * 0.01) | fabs) <= 0.0000011
      and (.operable_capital - ([0, (.account_equity - .protected_reserve - .operating_buffer)] | max) | fabs) <= 0.0000011
      and .order_notional > 0
      and .order_notional < 10.5
      and .proposed_notional >= .order_notional)
  ' funded-v9-capital-snapshot.json >/dev/null; then
    capital_snapshot_ready=true
    break
  fi
  sleep 10
done
test "$capital_snapshot_ready" = true

snapshot_equity=$(jq -r '.[0].Log_s | fromjson | .account_equity' funded-v9-capital-snapshot.json)
v9_state="reports/funded/dynamic-quote/sessions/dynamic-quote-funded-2026-08-12-v9/capital-reserve-state.json"
az storage blob download \
  --account-name "$STORAGE_ACCOUNT" \
  --container-name "$FUNDED_CONTAINER" \
  --name "$v9_state" \
  --file v9-capital-reserve-state.json \
  --auth-mode login \
  --overwrite \
  --only-show-errors -o none
state_last_modified=$(az storage blob show \
  --account-name "$STORAGE_ACCOUNT" \
  --container-name "$FUNDED_CONTAINER" \
  --name "$v9_state" \
  --auth-mode login \
  --query properties.lastModified -o tsv)
test "$(date -u -d "$state_last_modified" +%s)" -ge \
  "$(date -u -d "$activation_started_at" +%s)"
jq -e --argjson snapshot_equity "$snapshot_equity" '
  .schema == "polyedge.protected_compounding_state.v2"
  and .session_id == "dynamic-quote-funded-2026-08-12-v9"
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
  and .prior_state_sha256 == "sha256:617b0bd69466dc7d6ff7d61b26f5a4ed1bcfd557d5d9e4b62688b7fb13bf28c6"
  and .reserve_monotonic == false
  and .continue_after_loss == true
  and .high_water_equity >= 102.78112
  and .historical_high_water_equity >= 102.78112
  and (.last_reconciled_equity - $snapshot_equity | fabs) <= 0.0000011
  and (.protected_reserve - ([2, (.last_reconciled_equity * .reserve_ratio)] | max) | fabs) <= 0.0000011
  and (.operating_buffer - (.last_reconciled_equity * .operating_buffer_ratio) | fabs) <= 0.0000011
  and (.operable_capital - ([0, (.last_reconciled_equity - .protected_reserve - .operating_buffer)] | max) | fabs) <= 0.0000011
  and ([keys[] | startswith("migration_")] | length) == 0
' v9-capital-reserve-state.json >/dev/null
