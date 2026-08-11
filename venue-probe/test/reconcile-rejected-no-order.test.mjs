import test from "node:test";
import assert from "node:assert/strict";
import {
  runRejectedNoOrderReconciliation,
  validateRejectedReservationRecovery
} from "../src/reconcile-rejected-no-order.mjs";

function fixture() {
  return {
    reservation: {
      state: "reserved",
      run_id: "funded-direct-run-1",
      probe_id: `funded-direct-${"a".repeat(64)}`,
      order_submitted: null,
      matched_notional: null,
      created_ts: "2026-07-29T15:05:21.000Z"
    },
    expectedRunId: "funded-direct-run-1",
    openOrders: [],
    authenticatedTrades: [{
      match_time: "2026-07-29T14:59:00.000Z",
      condition_id: "older-trade"
    }],
    positions: [{ size: "1", redeemable: true }]
  };
}

test("exact deterministic rejection recovery accepts only reconciled no-order state", () => {
  assert.equal(
    validateRejectedReservationRecovery(fixture()).authenticated_post_reservation_trade_count,
    0
  );
});

test("rejected-order recovery preserves risk for every ambiguous or exposed state", () => {
  const cases = [
    (value) => { value.expectedRunId = "another-run"; },
    (value) => { value.reservation.state = "submitted_pending_reconciliation"; },
    (value) => { value.reservation.order_submitted = true; },
    (value) => { value.openOrders.push({ id: "order-1" }); },
    (value) => { value.authenticatedTrades.push({ match_time: "2026-07-29T15:05:22.000Z" }); },
    (value) => { value.authenticatedTrades.push({ id: "timestamp-missing" }); },
    (value) => { value.positions.push({ size: "1", redeemable: false }); }
  ];
  for (const mutate of cases) {
    const value = fixture();
    mutate(value);
    assert.throws(() => validateRejectedReservationRecovery(value), /fail closed/);
  }
});

test("post-only rejection recovery updates the exact campaign-scoped reservation index", async () => {
  const value = fixture();
  let unresolved = [value.reservation];
  const result = await runRejectedNoOrderReconciliation({
    env: {
      FUNDED_DIRECT_RECONCILIATION_ENABLED: "true",
      FUNDED_DIRECT_RECONCILE_RUN_ID: value.expectedRunId,
      FUNDED_DIRECT_RECONCILE_REJECTION_CODE: "post_only_crosses_book",
      VENUE_PROBE_FUNDED_CAMPAIGN_ID: "dynamic-quote-funded-2026-08-03-v7",
      AZURE_STORAGE_ACCOUNT_NAME: "storage",
      AZURE_STORAGE_CONTAINER_NAME: "funded",
      AZURE_CLIENT_ID: "client",
      POLYMARKET_FUNDER_ADDRESS: "0xfunder"
    },
    createClient: async () => ({
      getOpenOrders: async () => [],
      getTrades: async () => value.authenticatedTrades,
      getBalanceAllowance: async () => ({ balance: "10357051" })
    }),
    fetchImpl: async () => ({ ok: true, json: async () => value.positions }),
    loadReservations: async (config) => {
      assert.equal(config.campaignId, "dynamic-quote-funded-2026-08-03-v7");
      assert.equal(config.operatorDirect, true);
      assert.equal(config.dryRun, false);
      return unresolved;
    },
    finalize: async (config, reservation, settlement) => {
      assert.equal(config.campaignId, "dynamic-quote-funded-2026-08-03-v7");
      assert.equal(reservation.run_id, value.expectedRunId);
      assert.equal(settlement.state, "released_no_order");
      assert.equal(settlement.reconciliation_reason, "post_only_crosses_book");
      unresolved = [];
      return reservation;
    }
  });
  assert.equal(result.status, "released_no_order");
  assert.equal(result.rejection_code, "post_only_crosses_book");
  assert.equal(result.run_id, value.expectedRunId);
});

test("rejected-order recovery refuses unknown codes and missing campaign binding", async () => {
  const base = {
    FUNDED_DIRECT_RECONCILIATION_ENABLED: "true",
    FUNDED_DIRECT_RECONCILE_RUN_ID: "funded-direct-run-1"
  };
  await assert.rejects(
    runRejectedNoOrderReconciliation({
      env: { ...base, FUNDED_DIRECT_RECONCILE_REJECTION_CODE: "gateway_timeout" }
    }),
    /not an exact deterministic no-order code/
  );
  await assert.rejects(
    runRejectedNoOrderReconciliation({
      env: { ...base, FUNDED_DIRECT_RECONCILE_REJECTION_CODE: "post_only_crosses_book" }
    }),
    /exact VENUE_PROBE_FUNDED_CAMPAIGN_ID/
  );
});
