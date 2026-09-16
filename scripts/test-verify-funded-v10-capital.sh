#!/usr/bin/env bash
set -Eeuo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
test_dir=$(mktemp -d)
trap 'rm -rf -- "$test_dir"' EXIT
mkdir -p "$test_dir/bin" "$test_dir/fixtures"

FIXTURE_DIR="$test_dir/fixtures" node --input-type=module <<'NODE'
import { writeFileSync } from "node:fs";
import { join } from "node:path";
import { internalSettlementBlobName } from "./venue-probe/src/compounding-risk.mjs";

const dir = process.env.FIXTURE_DIR;
const session = "dynamic-quote-funded-2026-08-13-v10";
const hash = (value) => `0x${value.repeat(64)}`;
const zeroHash = hash("0");
const manual = {
  schema: "polyedge.verified_internal_settlement.v1",
  id: "manual-gain",
  type: "internal_manual_settlement",
  session_id: session,
  transaction_hash: hash("1"),
  condition_id: hash("2"),
  payout: 10,
  principal: 2,
  realized_pnl: 8,
  fill_transaction_hashes: [hash("3")],
  evidence_source: "polymarket_data_api_fills_plus_polygon_receipt",
  receipt_block_number: "1",
  receipt_confirmations: 2
};
const loss = {
  schema: "polyedge.verified_internal_settlement.v1",
  id: "resolved-loss",
  type: "internal_resolved_loss",
  session_id: session,
  transaction_hash: zeroHash,
  condition_id: hash("4"),
  payout: 0,
  principal: 2,
  realized_pnl: -2,
  evidence_source: "polymarket_data_api_resolved_zero_payout",
  resolution_verified: true
};
const duplicate = { ...loss, id: manual.id, condition_id: hash("5") };
const largeGain = {
  ...manual,
  id: "large-gain",
  transaction_hash: hash("6"),
  condition_id: hash("7"),
  payout: 102,
  realized_pnl: 100
};

function settlementFixture(value, file) {
  const name = internalSettlementBlobName(
    session,
    value.transaction_hash,
    value.condition_id
  );
  const bytes = `${JSON.stringify(value, null, 2)}\n`;
  writeFileSync(join(dir, file), bytes);
  return { name, file, properties: { etag: `"${file}-etag"`, contentLength: Buffer.byteLength(bytes) } };
}

const manualRow = settlementFixture(manual, "manual.json");
const lossRow = settlementFixture(loss, "loss.json");
const duplicateRow = settlementFixture(duplicate, "duplicate.json");
const largeGainRow = settlementFixture(largeGain, "large-gain.json");
writeFileSync(join(dir, "inventory.json"), JSON.stringify([manualRow, lossRow]));
writeFileSync(join(dir, "loss-inventory.json"), JSON.stringify([lossRow]));
writeFileSync(join(dir, "large-gain-inventory.json"), JSON.stringify([largeGainRow]));
writeFileSync(join(dir, "duplicate-inventory.json"), JSON.stringify([manualRow, duplicateRow]));

function state({
  equity = 26.941401,
  pnl = 6,
  ceiling = 35.505501,
  ids = [manual.id, loss.id].sort(),
  highWater = 102.78112,
  historicalHighWater = 102.78112
} = {}) {
  const reserve = Math.max(2, equity * 0.1);
  const buffer = equity * 0.01;
  return {
    schema: "polyedge.protected_compounding_state.v2",
    session_id: session,
    reconciliation_complete: true,
    reserve_ratio: 0.1,
    minimum_reserve: 2,
    target_order_ratio: 0.05,
    operating_buffer_ratio: 0.01,
    minimum_order_notional: 1,
    reserve_basis: "fully_reconciled_current_equity",
    loss_response: "resize_from_fully_reconciled_current_equity",
    prior_state_session_id: "dynamic-quote-funded-2026-07-29-v5",
    prior_state_blob_name: "reports/funded/dynamic-quote/sessions/dynamic-quote-funded-2026-07-29-v5/capital-reserve-state.json",
    prior_state_sha256: "sha256:9e63db7ef8c22d3af53a14f1858b817b8769ca7ba737e6668a900ffb73330c15",
    reserve_monotonic: false,
    continue_after_loss: true,
    last_reconciled_equity: equity,
    authorized_equity_ceiling: ceiling,
    verified_realized_pnl: pnl,
    verified_settlement_ids: ids,
    high_water_equity: highWater,
    historical_high_water_equity: historicalHighWater,
    protected_reserve: reserve,
    operating_buffer: buffer,
    operable_capital: Math.max(0, equity - reserve - buffer)
  };
}

function snapshot({
  equity = 26.941401,
  reserve,
  unresolved = 0,
  historicalHighWater = 102.78112
} = {}) {
  const protectedReserve = reserve ?? Math.max(2, equity * 0.1);
  const operatingBuffer = equity * 0.01;
  return [{
    TimeGenerated: "2026-08-15T00:00:00Z",
    Log_s: JSON.stringify({
      schema: "polyedge.funded_capital_snapshot.v1",
      session_id: session,
      decision_id: "d".repeat(64),
      snapshot_source: "live_preflight",
      snapshot_completed_wall_ms: 1,
      snapshot_age_ms: 0,
      risk_passed: true,
      blockers: [],
      open_order_count: 0,
      unresolved_position_count: 0,
      unresolved_risk_reservation_count: unresolved,
      account_equity: equity,
      historical_high_water_equity: historicalHighWater,
      reserve_basis: "fully_reconciled_current_equity",
      continue_after_loss: true,
      protected_reserve: protectedReserve,
      operating_buffer: operatingBuffer,
      operable_capital: Math.max(0, equity - protectedReserve - operatingBuffer),
      order_notional: 1.5,
      proposed_notional: 2
    })
  }];
}

const cases = {
  "pass-ledger": { state: state(), snapshot: snapshot() },
  "pass-zero": {
    state: state({ pnl: 0, ceiling: 29.505501, ids: [] }),
    snapshot: snapshot()
  },
  "pass-loss": {
    state: state({ equity: 5, pnl: -2, ceiling: 27.505501, ids: [loss.id] }),
    snapshot: snapshot({ equity: 5 })
  },
  "fail-pnl": { state: state({ pnl: 7 }), snapshot: snapshot() },
  "fail-ceiling": { state: state({ ceiling: 34 }), snapshot: snapshot() },
  "fail-ids": { state: state({ ids: [manual.id] }), snapshot: snapshot() },
  "fail-extra-ids": { state: state({ ids: [manual.id, loss.id, "unexpected"] }), snapshot: snapshot() },
  "fail-duplicate": { state: state({ ids: [manual.id] }), snapshot: snapshot() },
  "fail-binding": { state: state(), snapshot: snapshot() },
  "fail-etag": { state: state(), snapshot: snapshot() },
  "fail-state-etag": { state: state(), snapshot: snapshot() },
  "fail-snapshot": { state: state(), snapshot: snapshot({ equity: 25 }) },
  "fail-high-water": {
    state: state({
      equity: 120,
      pnl: 100,
      ceiling: 129.505501,
      ids: [largeGain.id],
      historicalHighWater: 150
    }),
    snapshot: snapshot({ equity: 120, historicalHighWater: 150 })
  },
  "fail-history-order": {
    state: state({ highWater: 110, historicalHighWater: 102.78112 }),
    snapshot: snapshot()
  },
  "fail-insolvency": { state: state({ equity: 36 }), snapshot: snapshot({ equity: 36 }) },
  "fail-reserve": { state: state(), snapshot: snapshot({ reserve: 3 }) },
  "fail-unresolved": { state: state(), snapshot: snapshot({ unresolved: 1 }) }
};
for (const [name, value] of Object.entries(cases)) {
  writeFileSync(join(dir, `state-${name}.json`), `${JSON.stringify(value.state)}\n`);
  writeFileSync(join(dir, `snapshot-${name}.json`), `${JSON.stringify(value.snapshot)}\n`);
}
NODE

cat > "$test_dir/bin/az" <<'FAKE_AZ'
#!/usr/bin/env bash
set -Eeuo pipefail

case "$1 $2 $3" in
  "monitor log-analytics query")
    cat "$CAPITAL_VERIFY_FIXTURES/snapshot-$CAPITAL_VERIFY_SCENARIO.json"
    ;;
  "storage blob list")
    inventory="$CAPITAL_VERIFY_FIXTURES/inventory.json"
    [ "$CAPITAL_VERIFY_SCENARIO" != pass-zero ] || inventory=/dev/null
    [ "$CAPITAL_VERIFY_SCENARIO" != pass-loss ] || \
      inventory="$CAPITAL_VERIFY_FIXTURES/loss-inventory.json"
    [ "$CAPITAL_VERIFY_SCENARIO" != fail-high-water ] || \
      inventory="$CAPITAL_VERIFY_FIXTURES/large-gain-inventory.json"
    [ "$CAPITAL_VERIFY_SCENARIO" != fail-duplicate ] || \
      inventory="$CAPITAL_VERIFY_FIXTURES/duplicate-inventory.json"
    if [ "$inventory" = /dev/null ]; then
      printf '[]\n'
    elif [ "$CAPITAL_VERIFY_SCENARIO" = fail-etag ]; then
      counter="$CAPITAL_VERIFY_FIXTURES/list-counter"
      count=0
      [ ! -f "$counter" ] || count=$(<"$counter")
      count=$((count + 1))
      printf '%s\n' "$count" > "$counter"
      if (( count % 2 == 0 )); then
        jq '.[0].properties.etag = "\\\"mutated-etag\\\""' "$inventory"
      else
        cat "$inventory"
      fi
    else
      cat "$inventory"
    fi
    ;;
  "storage blob show")
    state="$CAPITAL_VERIFY_FIXTURES/state-$CAPITAL_VERIFY_SCENARIO.json"
    size=$(wc -c < "$state")
    state_etag='"state-etag"'
    if [ "$CAPITAL_VERIFY_SCENARIO" = fail-state-etag ]; then
      counter="$CAPITAL_VERIFY_FIXTURES/state-counter"
      count=0
      [ ! -f "$counter" ] || count=$(<"$counter")
      count=$((count + 1))
      printf '%s\n' "$count" > "$counter"
      (( count % 2 == 1 )) || state_etag='"mutated-state-etag"'
    fi
    jq -cn --arg etag "$state_etag" --argjson size "$size" \
      '{properties: {etag: $etag, contentLength: $size}}'
    ;;
  "storage blob download")
    name= file= etag=
    shift 3
    while [ "$#" -gt 0 ]; do
      case "$1" in
        --name) name=$2; shift 2 ;;
        --file) file=$2; shift 2 ;;
        --if-match) etag=$2; shift 2 ;;
        *) shift ;;
      esac
    done
    test -n "$name" && test -n "$file" && test -n "$etag"
    if [[ "$name" == */capital-reserve-state.json ]]; then
      test "$etag" = '"state-etag"'
      cp "$CAPITAL_VERIFY_FIXTURES/state-$CAPITAL_VERIFY_SCENARIO.json" "$file"
      exit
    fi
    inventory="$CAPITAL_VERIFY_FIXTURES/inventory.json"
    [ "$CAPITAL_VERIFY_SCENARIO" != pass-loss ] || \
      inventory="$CAPITAL_VERIFY_FIXTURES/loss-inventory.json"
    [ "$CAPITAL_VERIFY_SCENARIO" != fail-high-water ] || \
      inventory="$CAPITAL_VERIFY_FIXTURES/large-gain-inventory.json"
    [ "$CAPITAL_VERIFY_SCENARIO" != fail-duplicate ] || \
      inventory="$CAPITAL_VERIFY_FIXTURES/duplicate-inventory.json"
    fixture=$(jq -r --arg name "$name" '.[] | select(.name == $name) | .file' "$inventory")
    expected=$(jq -r --arg name "$name" '.[] | select(.name == $name) | .properties.etag' "$inventory")
    test -n "$fixture" && test "$etag" = "$expected"
    if [ "$CAPITAL_VERIFY_SCENARIO" = fail-binding ] && [ "$fixture" = manual.json ]; then
      fixture=loss.json
    fi
    cp "$CAPITAL_VERIFY_FIXTURES/$fixture" "$file"
    ;;
  *)
    exit 2
    ;;
esac
FAKE_AZ
cat > "$test_dir/bin/sleep" <<'FAKE_SLEEP'
#!/usr/bin/env bash
exit 0
FAKE_SLEEP
chmod +x "$test_dir/bin/az" "$test_dir/bin/sleep"

run_case() {
  local scenario=$1 expected=$2
  rm -f "$test_dir/fixtures/list-counter" "$test_dir/fixtures/state-counter"
  if (
    cd "$repo_root"
    PATH="$test_dir/bin:$PATH" \
    CAPITAL_VERIFY_FIXTURES="$test_dir/fixtures" \
    CAPITAL_VERIFY_SCENARIO="$scenario" \
    workspace_customer_id=test \
    FUNDED_APP=test \
    funded_revision=test \
    producer_enabled_at=2026-08-15T00:00:00Z \
    decision_id="$(printf 'd%.0s' {1..64})" \
    STORAGE_ACCOUNT=test \
    FUNDED_CONTAINER=test \
      bash scripts/verify-funded-v10-capital.sh >/dev/null 2>&1
  ); then
    result=pass
  else
    result=fail
  fi
  test "$result" = "$expected"
}

run_case pass-ledger pass
run_case pass-zero pass
run_case pass-loss pass
for scenario in \
  fail-pnl fail-ceiling fail-ids fail-extra-ids fail-duplicate fail-binding \
  fail-etag fail-state-etag fail-snapshot fail-high-water fail-history-order \
  fail-insolvency fail-reserve fail-unresolved; do
  run_case "$scenario" fail
done
echo "funded capital verifier self-test passed"
