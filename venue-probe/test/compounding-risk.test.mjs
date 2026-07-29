import test from "node:test";
import assert from "node:assert/strict";
import {
  protectedCapitalSnapshot,
  putVerifiedInternalSettlement,
  reconcileProtectedCompoundingState,
  verifyConfiguredInternalSettlements
} from "../src/compounding-risk.mjs";

const transactionHash = `0x${"ab".repeat(32)}`;
const conditionId = `0x${"cd".repeat(32)}`;
const fillHashes = [
  `0x${"11".repeat(32)}`,
  `0x${"22".repeat(32)}`,
  `0x${"33".repeat(32)}`
];

function manifest() {
  return {
    schema_version: "polyedge.operator_funded_session.v2",
    session_id: "dynamic-quote-funded-2026-07-29-v5",
    allow_compounding: true,
    starting_collateral: 11.09862,
    max_reconciliation_discrepancy: 0.01,
    capital_policy: {
      reserve_ratio: 0.3,
      operating_buffer_ratio: 0.01,
      minimum_order_notional: 1,
      high_water_update: "full_reconciliation_only",
      reserve_monotonic: true,
      state_blob_name: "reports/funded/dynamic-quote/sessions/dynamic-quote-funded-2026-07-29-v5/capital-reserve-state.json"
    },
    internal_settlements: [{
      id: "manual-redeem-2026-07-29-1530-et",
      type: "internal_manual_settlement",
      transaction_hash: transactionHash,
      condition_id: conditionId,
      payout: 17.015,
      principal: 10.209,
      realized_pnl: 6.806,
      fill_transaction_hashes: fillHashes
    }]
  };
}

function activity() {
  return [
    { type: "REDEEM", transactionHash, conditionId, usdcSize: 17.015 },
    { type: "TRADE", transactionHash: fillHashes[0], conditionId, size: 0.02, usdcSize: 0.012 },
    { type: "TRADE", transactionHash: fillHashes[1], conditionId, size: 13.325, usdcSize: 7.995 },
    { type: "TRADE", transactionHash: fillHashes[2], conditionId, size: 3.67, usdcSize: 2.202 }
  ];
}

test("manual redemption is internal only when payout, fills, condition, and Polygon receipt agree", async () => {
  const verified = await verifyConfiguredInternalSettlements({
    manifest: manifest(),
    activity: activity(),
    getTransactionReceipt: async () => ({
      status: "success",
      chain_id: 137,
      block_number: "123",
      confirmations: 2
    })
  });
  assert.equal(verified.length, 1);
  assert.equal(verified[0].type, "internal_manual_settlement");
  assert.equal(verified[0].realized_pnl, 6.806);
  await assert.rejects(
    verifyConfiguredInternalSettlements({
      manifest: manifest(),
      activity: activity().filter((row) => row.transactionHash !== fillHashes[2]),
      getTransactionReceipt: async () => ({ status: "success", chain_id: 137, block_number: "123", confirmations: 2 })
    }),
    /does not match its authenticated fills/
  );
});

test("durable verification keeps an old manual settlement valid after it leaves the activity window", async () => {
  const value = manifest();
  const initial = await verifyConfiguredInternalSettlements({
    manifest: value,
    activity: activity(),
    getTransactionReceipt: async () => ({
      status: "success",
      chain_id: 137,
      block_number: "123",
      confirmations: 2
    })
  });
  const container = new Container();
  const durable = (await putVerifiedInternalSettlement(container, {
    ...initial[0],
    session_id: value.session_id
  })).value;
  const reused = await verifyConfiguredInternalSettlements({
    manifest: value,
    activity: [],
    durableSettlements: [durable],
    getTransactionReceipt: async () => assert.fail("durable verification must not depend on the old receipt endpoint")
  });
  assert.equal(reused[0].id, initial[0].id);
  assert.equal(reused[0].realized_pnl, 6.806);
});

test("30 percent high-water reserve is monotonic and leaves the requested operable capital", async () => {
  const container = new Container();
  const value = manifest();
  const verified = await verifyConfiguredInternalSettlements({
    manifest: value,
    activity: activity(),
    getTransactionReceipt: async () => ({ status: "success", chain_id: 137, block_number: "123", confirmations: 3 })
  });
  const first = await reconcileProtectedCompoundingState({
    container,
    manifest: value,
    accountEquity: 17.90462,
    fullyReconciled: true,
    verifiedConfiguredSettlements: verified
  });
  assert.equal(first.high_water_equity, 17.90462);
  assert.equal(first.protected_reserve, 5.371386);
  assert.equal(first.operating_buffer, 0.179046);
  assert.equal(first.operable_capital, 12.354188);

  const afterLoss = await reconcileProtectedCompoundingState({
    container,
    manifest: value,
    accountEquity: 12,
    fullyReconciled: true,
    verifiedConfiguredSettlements: verified
  });
  assert.equal(afterLoss.high_water_equity, 17.90462);
  assert.equal(afterLoss.protected_reserve, 5.371386);
  assert.equal(afterLoss.operable_capital, 6.508614);
});

test("external capital above starting collateral plus verified PnL is blocked", async () => {
  const value = manifest();
  const verified = await verifyConfiguredInternalSettlements({
    manifest: value,
    activity: activity(),
    getTransactionReceipt: async () => ({ status: "success", chain_id: 137, block_number: "123", confirmations: 2 })
  });
  await assert.rejects(
    reconcileProtectedCompoundingState({
      container: new Container(),
      manifest: value,
      accountEquity: 18,
      fullyReconciled: true,
      verifiedConfiguredSettlements: verified
    }),
    /unauthorized external deposit/
  );
});

test("a verified zero-payout loss lowers the authorized equity ceiling so replenishment stays blocked", async () => {
  const value = manifest();
  const container = new Container();
  const verified = await verifyConfiguredInternalSettlements({
    manifest: value,
    activity: activity(),
    getTransactionReceipt: async () => ({ status: "success", chain_id: 137, block_number: "123", confirmations: 2 })
  });
  await putVerifiedInternalSettlement(container, {
    id: `internal_resolved_loss:resolution:${`0x${"ef".repeat(32)}`}`,
    type: "internal_resolved_loss",
    session_id: value.session_id,
    transaction_hash: `0x${"0".repeat(64)}`,
    condition_id: `0x${"ef".repeat(32)}`,
    payout: 0,
    principal: 5,
    realized_pnl: -5,
    evidence_source: "polymarket_data_api_resolved_zero_payout",
    resolution_verified: true
  });
  const reconciled = await reconcileProtectedCompoundingState({
    container,
    manifest: value,
    accountEquity: 12.90462,
    fullyReconciled: true,
    verifiedConfiguredSettlements: verified
  });
  assert.equal(reconciled.authorized_equity_ceiling, 12.90462);
  await assert.rejects(
    reconcileProtectedCompoundingState({
      container,
      manifest: value,
      accountEquity: 13,
      fullyReconciled: true,
      verifiedConfiguredSettlements: verified
    }),
    /unauthorized external deposit/
  );
});

test("each order is authorized against operable capital and reserve floor", () => {
  const state = {
    high_water_equity: 17.90462,
    protected_reserve: 5.371386,
    operating_buffer_ratio: 0.01,
    minimum_order_notional: 1
  };
  assert.deepEqual(
    protectedCapitalSnapshot({ state, accountEquity: 17.90462, proposedNotional: 12.354188 }).blockers,
    []
  );
  assert.ok(
    protectedCapitalSnapshot({ state, accountEquity: 17.90462, proposedNotional: 12.354189 })
      .blockers.includes("operable_capital_exceeded")
  );
  assert.ok(
    protectedCapitalSnapshot({ state, accountEquity: 6.371386, proposedNotional: 0 })
      .blockers.includes("protected_reserve_order_floor_reached")
  );
});

class Container {
  constructor() {
    this.values = new Map();
    this.versions = new Map();
  }
  async *listBlobsFlat({ prefix }) {
    for (const name of this.values.keys()) if (name.startsWith(prefix)) yield { name };
  }
  getBlobClient(name) {
    return this.client(name);
  }
  getBlockBlobClient(name) {
    return this.client(name);
  }
  client(name) {
    return {
      download: async () => {
        if (!this.values.has(name)) {
          const error = new Error("missing");
          error.statusCode = 404;
          throw error;
        }
        return {
          readableStreamBody: stream(this.values.get(name)),
          etag: `"${this.versions.get(name)}"`
        };
      },
      uploadData: async (bytes, options = {}) => {
        const exists = this.values.has(name);
        if (options.conditions?.ifNoneMatch === "*" && exists) return conflict();
        if (options.conditions?.ifMatch
            && options.conditions.ifMatch !== `"${this.versions.get(name)}"`) return conflict();
        this.values.set(name, Buffer.from(bytes));
        this.versions.set(name, (this.versions.get(name) || 0) + 1);
      }
    };
  }
}

function conflict() {
  const error = new Error("condition failed");
  error.statusCode = 412;
  throw error;
}

async function* stream(value) {
  if (value) yield value;
}
