import test from "node:test";
import assert from "node:assert/strict";
import {
  discoverVerifiedAutomaticInternalSettlements,
  loadDurableInternalSettlements,
  putVerifiedInternalSettlement,
  reconcileProtectedCompoundingState,
  sizeProtectedOrder,
  validateProtectedCompoundingManifest,
  verifyAutomaticSettlementEvidence
} from "../src/compounding-risk.mjs";

function manifest() {
  return {
    schema_version: "polyedge.operator_funded_session.v2",
    session_id: "dynamic-quote-funded-test-v5",
    allow_compounding: true,
    starting_collateral: 11.09862,
    max_reconciliation_discrepancy: 0.01,
    created_at: "2026-07-30T00:00:00.000Z",
    capital_policy: {
      reserve_ratio: 0.3,
      operating_buffer_ratio: 0.01,
      minimum_order_notional: 1,
      high_water_update: "full_reconciliation_only",
      reserve_monotonic: true,
      state_blob_name:
        "reports/funded/dynamic-quote/sessions/dynamic-quote-funded-test-v5/capital-reserve-state.json"
    },
    internal_settlements: []
  };
}

class Container {
  constructor() {
    this.values = new Map();
    this.etags = new Map();
  }
  async *listBlobsFlat({ prefix }) {
    for (const name of this.values.keys()) {
      if (name.startsWith(prefix)) yield { name };
    }
  }
  getBlobClient(name) {
    return {
      download: async () => {
        if (!this.values.has(name)) {
          throw Object.assign(new Error("missing"), { statusCode: 404 });
        }
        return {
          readableStreamBody: stream(this.values.get(name)),
          etag: this.etags.get(name)
        };
      }
    };
  }
  getBlockBlobClient(name) {
    return {
      download: async () => this.getBlobClient(name).download(),
      uploadData: async (bytes, options = {}) => {
        const current = this.etags.get(name);
        if (options.conditions?.ifNoneMatch === "*" && this.values.has(name)) {
          throw Object.assign(new Error("exists"), { statusCode: 412 });
        }
        if (options.conditions?.ifMatch && options.conditions.ifMatch !== current) {
          throw Object.assign(new Error("etag mismatch"), { statusCode: 412 });
        }
        const next = `"${Number(String(current || "\"0\"").replaceAll("\"", "")) + 1}"`;
        this.values.set(name, Buffer.from(bytes));
        this.etags.set(name, next);
      }
    };
  }
}

function stream(value) {
  return (async function* () { yield Buffer.from(value); })();
}

test("protected compounding contract fixes the reserve at 30% with a 1% buffer", () => {
  assert.deepEqual(validateProtectedCompoundingManifest(manifest()), {
    reserveRatio: 0.3,
    operatingBufferRatio: 0.01,
    minimumOrderNotional: 1,
    stateBlobName:
      "reports/funded/dynamic-quote/sessions/dynamic-quote-funded-test-v5/capital-reserve-state.json",
    internalSettlements: []
  });
});

test("a loss cannot lower the reconciled high-water reserve", async () => {
  const container = new Container();
  const verifiedConfiguredSettlements = [{
    id: "manual-redeem-1",
    type: "internal_manual_settlement",
    transaction_hash: `0x${"a".repeat(64)}`,
    condition_id: `0x${"b".repeat(64)}`,
    payout: 17.015,
    principal: 10.209,
    realized_pnl: 6.806,
    fill_transaction_hashes: [`0x${"c".repeat(64)}`]
  }];
  const fundedManifest = manifest();
  fundedManifest.internal_settlements = verifiedConfiguredSettlements;
  const first = await reconcileProtectedCompoundingState({
    container,
    manifest: fundedManifest,
    accountEquity: 17.90462,
    fullyReconciled: true,
    verifiedConfiguredSettlements
  });
  const afterLoss = await reconcileProtectedCompoundingState({
    container,
    manifest: fundedManifest,
    accountEquity: 7.57122,
    fullyReconciled: true,
    verifiedConfiguredSettlements
  });
  assert.equal(first.high_water_equity, 17.90462);
  assert.equal(afterLoss.high_water_equity, 17.90462);
  assert.equal(afterLoss.protected_reserve, 5.371386);
  assert.equal(afterLoss.operating_buffer, 0.075712);
  assert.equal(afterLoss.operable_capital, 2.124122);
});

test("current-funds sizing rounds down and never breaches the reserve", () => {
  const sizing = sizeProtectedOrder({
    state: {
      high_water_equity: 17.90462,
      protected_reserve: 5.371386,
      operating_buffer_ratio: 0.01,
      minimum_order_notional: 1,
      authorized_equity_ceiling: 17.90462
    },
    accountEquity: 7.57122,
    price: 0.2,
    requestedShares: 52.5,
    requestedNotional: 10.5,
    minimumOrderSize: 5,
    maximumOrderNotional: 10.5,
    feePerShare: 0.001
  });
  assert.equal(sizing.executable, true);
  assert.equal(sizing.shares, 10.56);
  assert.equal(sizing.notional, 2.112);
  assert.equal(sizing.fee_risk_upper_bound, 0.01056);
  assert.ok(sizing.reserved_notional <= sizing.operable_capital);
  assert.ok(sizing.shares <= sizing.source_shares);
});

test("venue or policy minimums produce a no-trade instead of reserve leakage", () => {
  const sizing = sizeProtectedOrder({
    state: {
      high_water_equity: 17.90462,
      protected_reserve: 5.371386,
      operating_buffer_ratio: 0.01,
      minimum_order_notional: 1,
      authorized_equity_ceiling: 17.90462
    },
    accountEquity: 7.57122,
    price: 0.5,
    requestedShares: 21,
    requestedNotional: 10.5,
    minimumOrderSize: 5,
    maximumOrderNotional: 10.5,
    feePerShare: 0
  });
  assert.equal(sizing.executable, false);
  assert.ok(sizing.blockers.includes("protected_order_below_venue_minimum"));
});

const automaticCondition = `0x${"d".repeat(64)}`;
const automaticRedemption = `0x${"e".repeat(64)}`;
const automaticFillTransaction = `0x${"f".repeat(64)}`;
const fillTimestampMs = Date.parse("2026-07-30T00:01:00.000Z");

function automaticReservation(overrides = {}) {
  return {
    campaign_id: "dynamic-quote-funded-test-v5",
    run_id: "run-funded-1",
    probe_id: "probe-funded-1",
    order_id: "order-funded-1",
    condition_id: automaticCondition,
    order_submission_intended: true,
    order_submitted: true,
    matched_notional: 2,
    fee_risk_upper_bound: 0.1,
    created_ts: "2026-07-30T00:00:30.000Z",
    ...overrides
  };
}

function automaticActivity(overrides = {}) {
  return [
    {
      type: "TRADE",
      side: "BUY",
      transactionHash: automaticFillTransaction,
      conditionId: automaticCondition,
      size: 10,
      usdcSize: 2,
      timestamp: fillTimestampMs / 1_000,
      ...overrides
    },
    {
      type: "REDEEM",
      transactionHash: automaticRedemption,
      conditionId: automaticCondition,
      usdcSize: 10,
      timestamp: Date.parse("2026-07-30T00:10:00.000Z") / 1_000
    }
  ];
}

function automaticFills() {
  return [{
    id: "authenticated-clob-fill-1",
    size: 10,
    price: 0.2,
    timestampMs: fillTimestampMs,
    orderRole: "MAKER"
  }];
}

function confirmedReceipt() {
  return {
    status: "success",
    chain_id: 137,
    block_number: "12345678",
    confirmations: 3
  };
}

test("automatic settlement binds exact reservation, maker fill, Data API evidence, and receipt", async () => {
  const settlements = await discoverVerifiedAutomaticInternalSettlements({
    manifest: manifest(),
    reservations: [automaticReservation()],
    activity: automaticActivity(),
    getOrderFills: async (reservation) => {
      assert.equal(reservation.order_id, "order-funded-1");
      return automaticFills();
    },
    getTransactionReceipt: async (transactionHash) => {
      assert.equal(transactionHash, automaticRedemption);
      return confirmedReceipt();
    }
  });
  assert.equal(settlements.length, 1);
  assert.deepEqual(settlements[0], {
    id: `automatic-redeem-${"e".repeat(16)}`,
    type: "internal_automatic_settlement",
    session_id: "dynamic-quote-funded-test-v5",
    campaign_id: "dynamic-quote-funded-test-v5",
    run_id: "run-funded-1",
    probe_id: "probe-funded-1",
    order_id: "order-funded-1",
    transaction_hash: automaticRedemption,
    condition_id: automaticCondition,
    payout: 10,
    principal: 2,
    realized_pnl: 8,
    fill_transaction_hashes: [automaticFillTransaction],
    authenticated_clob_fill_ids: ["authenticated-clob-fill-1"],
    reservation_matched_notional: 2,
    reservation_fee_risk_upper_bound: 0.1,
    evidence_source: "polymarket_data_api_plus_onchain_redemption",
    receipt_block_number: "12345678",
    receipt_confirmations: 3,
    settled_at: "2026-07-30T00:10:00.000Z"
  });

  const container = new Container();
  await putVerifiedInternalSettlement(container, settlements[0]);
  await putVerifiedInternalSettlement(container, settlements[0]);
  const durable = await loadDurableInternalSettlements(
    container,
    "dynamic-quote-funded-test-v5"
  );
  assert.equal(durable.length, 1);
  assert.deepEqual(durable[0], {
    schema: "polyedge.verified_internal_settlement.v1",
    ...settlements[0]
  });
  const state = await reconcileProtectedCompoundingState({
    container,
    manifest: manifest(),
    accountEquity: 19.09862,
    fullyReconciled: true
  });
  assert.equal(state.verified_realized_pnl, 8);
  assert.equal(state.authorized_equity_ceiling, 19.09862);
  assert.equal(state.high_water_equity, 19.09862);
  assert.equal(state.protected_reserve, 5.729586);
});

test("an existing manual redemption identity is idempotently excluded", async () => {
  const calls = [];
  const settlements = await discoverVerifiedAutomaticInternalSettlements({
    manifest: manifest(),
    reservations: [automaticReservation()],
    activity: automaticActivity(),
    durableSettlements: [{
      schema: "polyedge.verified_internal_settlement.v1",
      id: "manual-redeem-2026-07-31-0109z",
      type: "internal_manual_settlement",
      session_id: "dynamic-quote-funded-test-v5",
      transaction_hash: automaticRedemption,
      condition_id: automaticCondition,
      payout: 10,
      principal: 2,
      realized_pnl: 8,
      fill_transaction_hashes: [automaticFillTransaction],
      evidence_source: "polymarket_data_api_fills_plus_polygon_receipt",
      receipt_block_number: "12345678",
      receipt_confirmations: 3
    }],
    getOrderFills: async () => { calls.push("fills"); return automaticFills(); },
    getTransactionReceipt: async () => { calls.push("receipt"); return confirmedReceipt(); }
  });
  assert.deepEqual(settlements, []);
  assert.deepEqual(calls, []);
});

test("automatic settlement fails closed on ambiguous reservations or mismatched evidence", async () => {
  await assert.rejects(
    discoverVerifiedAutomaticInternalSettlements({
      manifest: manifest(),
      reservations: [
        automaticReservation(),
        automaticReservation({ probe_id: "probe-funded-2", order_id: "order-funded-2" })
      ],
      activity: automaticActivity(),
      getOrderFills: async () => automaticFills(),
      getTransactionReceipt: async () => confirmedReceipt()
    }),
    /does not bind one exact funded reservation/
  );
  assert.throws(
    () => verifyAutomaticSettlementEvidence({
      manifest: manifest(),
      reservation: automaticReservation(),
      redemption: automaticActivity()[1],
      activity: automaticActivity({ size: 9 }),
      orderFills: automaticFills(),
      receipt: confirmedReceipt()
    }),
    /Data API fill binding is missing or ambiguous/
  );
  assert.throws(
    () => verifyAutomaticSettlementEvidence({
      manifest: manifest(),
      reservation: automaticReservation(),
      redemption: automaticActivity()[1],
      activity: automaticActivity(),
      orderFills: automaticFills(),
      receipt: { ...confirmedReceipt(), confirmations: 1 }
    }),
    /Polygon receipt is invalid/
  );
});
