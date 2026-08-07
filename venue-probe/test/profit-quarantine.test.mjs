import test from "node:test";
import assert from "node:assert/strict";
import {
  initializeProfitQuarantine,
  validateProfitQuarantineManifest
} from "../src/profit-quarantine.mjs";
import { summarizeCampaignRisk } from "../src/lib.mjs";

const transactionHash = `0x${"ab".repeat(32)}`;
const conditionId = `0x${"cd".repeat(32)}`;
const fillHashes = [
  `0x${"11".repeat(32)}`,
  `0x${"22".repeat(32)}`,
  `0x${"33".repeat(32)}`
];

function manifest() {
  return {
    schema_version: "polyedge.operator_funded_session.v1",
    session_id: "dynamic-quote-funded-test-v6",
    allow_compounding: false,
    starting_collateral: 11.09862,
    profit_quarantine: {
      enabled: true,
      mode: "verified_internal_profit_quarantine",
      risk_headroom: "starting_collateral_only",
      settlement_ledger_prefix:
        "reports/funded/dynamic-quote/sessions/dynamic-quote-funded-test-v6/verified-internal-profits"
    },
    verified_internal_settlements: [{
      id: "manual-redeem-1",
      type: "internal_manual_settlement",
      transaction_hash: transactionHash,
      condition_id: conditionId,
      payout: 17.015,
      principal: 10.209,
      realized_pnl: 6.806,
      fill_transaction_hashes: fillHashes,
      settled_at: "2026-07-29T19:47:17Z"
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

test("verified internal profit is durable and remains excluded from risk headroom", async () => {
  const container = new Container();
  const snapshot = await initializeProfitQuarantine({
    container,
    manifest: manifest(),
    activity: activity(),
    getTransactionReceipt: async () => ({
      status: "success",
      chain_id: 137,
      block_number: "123",
      confirmations: 2
    })
  });
  assert.equal(snapshot.verified_internal_realized_pnl, 6.806);
  assert.equal(snapshot.authorized_equity_ceiling, 17.90462);
  assert.equal(snapshot.allow_compounding, false);

  const control = {
    campaign_id: manifest().session_id,
    baseline_equity: 11.09862,
    equity_floor: 0,
    max_campaign_drawdown: 11.09862,
    max_order_notional: 10.5,
    max_reconciliation_discrepancy: 0.01,
    net_external_cash_flow: 0,
    cash_flow_count: 0,
    cash_flow_ids: []
  };
  const risk = summarizeCampaignRisk({
    control,
    liquidCollateral: 17.90462,
    summedPositionValue: 0,
    reportedPositionValue: 0,
    proposedNotional: 10.4052,
    orderNotional: 10.4052,
    authorizedStartingCollateral: 11.09862,
    requireZeroExternalCashFlows: true,
    profitQuarantineSnapshot: snapshot
  });
  assert.equal(risk.passed, true);
  assert.equal(risk.no_compounding, true);
  assert.equal(risk.risk_eligible_equity, 11.09862);
  assert.equal(risk.quarantined_internal_profit, 6.806);
  assert.equal(risk.projected_equity, 0.69342);

  const deposit = summarizeCampaignRisk({
    control,
    liquidCollateral: 17.914621,
    summedPositionValue: 0,
    reportedPositionValue: 0,
    authorizedStartingCollateral: 11.09862,
    requireZeroExternalCashFlows: true,
    profitQuarantineSnapshot: snapshot
  });
  assert.ok(deposit.blockers.includes("authorized_starting_collateral_exceeded"));
});

test("profit evidence fails closed on missing fills, receipts, or manifest drift", async () => {
  await assert.rejects(
    initializeProfitQuarantine({
      container: new Container(),
      manifest: manifest(),
      activity: activity().slice(0, -1),
      getTransactionReceipt: async () => ({
        status: "success",
        chain_id: 137,
        block_number: "123",
        confirmations: 2
      })
    }),
    /authenticated fills/
  );
  await assert.rejects(
    initializeProfitQuarantine({
      container: new Container(),
      manifest: manifest(),
      activity: activity(),
      getTransactionReceipt: async () => ({
        status: "success",
        chain_id: 137,
        block_number: "123",
        confirmations: 1
      })
    }),
    /confirmed Polygon receipt/
  );
  assert.throws(
    () => validateProfitQuarantineManifest({
      ...manifest(),
      allow_compounding: true
    }),
    /allow_compounding must remain false/
  );
});

test("durable proof is reusable but immutable tampering is rejected", async () => {
  const container = new Container();
  await initializeProfitQuarantine({
    container,
    manifest: manifest(),
    activity: activity(),
    getTransactionReceipt: async () => ({
      status: "success",
      chain_id: 137,
      block_number: "123",
      confirmations: 3
    })
  });
  const reused = await initializeProfitQuarantine({
    container,
    manifest: manifest(),
    activity: [],
    getTransactionReceipt: async () => assert.fail("durable proof must avoid an RPC dependency")
  });
  assert.equal(reused.quarantined_internal_profit, 6.806);
  const changed = manifest();
  changed.verified_internal_settlements[0].principal = 10.208;
  changed.verified_internal_settlements[0].realized_pnl = 6.807;
  await assert.rejects(
    initializeProfitQuarantine({
      container,
      manifest: changed,
      activity: [],
      getTransactionReceipt: async () => assert.fail("tampering must fail before RPC")
    }),
    /durable internal profit mismatch/
  );
});

class Container {
  constructor() {
    this.values = new Map();
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
        return { readableStreamBody: stream(this.values.get(name)) };
      },
      uploadData: async (bytes, options = {}) => {
        if (options.conditions?.ifNoneMatch === "*" && this.values.has(name)) {
          const error = new Error("condition failed");
          error.statusCode = 412;
          throw error;
        }
        this.values.set(name, Buffer.from(bytes));
      }
    };
  }
}

async function* stream(value) {
  if (value) yield value;
}
