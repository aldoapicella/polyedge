import test from "node:test";
import assert from "node:assert/strict";
import {
  reconcileProtectedCompoundingState,
  sizeProtectedOrder,
  validateProtectedCompoundingManifest
} from "../src/compounding-risk.mjs";

function manifest() {
  return {
    schema_version: "polyedge.operator_funded_session.v2",
    session_id: "dynamic-quote-funded-test-v5",
    allow_compounding: true,
    starting_collateral: 11.09862,
    max_reconciliation_discrepancy: 0.01,
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
