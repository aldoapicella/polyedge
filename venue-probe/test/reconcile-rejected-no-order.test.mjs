import test from "node:test";
import assert from "node:assert/strict";
import { validateRejectedReservationRecovery } from "../src/reconcile-rejected-no-order.mjs";

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
