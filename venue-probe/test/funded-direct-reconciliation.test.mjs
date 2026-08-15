import test from "node:test";
import assert from "node:assert/strict";
import {
  runFundedDirectReconciliation,
  validateFundedReconciliationSnapshot
} from "../src/funded-direct-reconciliation.mjs";

const sessionId = "dynamic-quote-funded-v8";
const clean = {
  schema: "polyedge.funded_capital_snapshot.v1",
  snapshot_age_ms: 10,
  session_id: sessionId,
  snapshot_source: "persistent_safety_cache",
  risk_passed: true,
  blockers: [],
  open_order_count: 0,
  unresolved_position_count: 0,
  unresolved_risk_reservation_count: 0
};

test("bounded reconciliation emits one clean no-order safety snapshot", async () => {
  let closed = false;
  let warmed = false;
  const logs = [];
  const snapshot = await runFundedDirectReconciliation({
    env: {
      VENUE_PROBE_FUNDED_CAMPAIGN_ID: sessionId,
      FUNDED_DIRECT_SESSION_MANIFEST_JSON: JSON.stringify({ execution_model: {} })
    },
    createExecutor: async ({ env, readOnly }) => {
      assert.equal(readOnly, true);
      assert.equal(env.STRATEGY_CANARY_DRY_RUN, "true");
      assert.equal(env.ALLOW_LIVE, "false");
      assert.equal(env.ALLOW_STRATEGY_CANARY, "false");
      assert.equal(env.ALLOW_FUNDED_DIRECT, "true");
      assert.equal(env.ENABLE_TAKER_ORDERS, "false");
      assert.equal(env.FUNDED_EVIDENCE_TRUST_BOUNDARY_READY, "false");
      return {
        warmMarket: async () => { warmed = true; },
        status: () => ({
          safety_snapshot_cache_ready: warmed,
          safety_snapshot_cache_in_flight: 0,
          safety_snapshot_cache_error: null
        }),
        reconciliationSnapshot: () => clean,
        close: async () => { closed = true; }
      };
    },
    discoverMarket: async () => ({
      id: "market",
      conditionId: "condition",
      clobTokenIds: ["up", "down"],
      endDate: "2026-08-12T08:00:00Z"
    }),
    sleep: async () => {},
    logger: (value) => logs.push(value)
  });
  assert.equal(snapshot, clean);
  assert.deepEqual(logs, [clean]);
  assert.equal(closed, true);
});

test("opt-in state reconciliation writes only through the no-order warmup path", async () => {
  let closed = false;
  const snapshot = await runFundedDirectReconciliation({
    env: {
      VENUE_PROBE_FUNDED_CAMPAIGN_ID: sessionId,
      FUNDED_DIRECT_SESSION_MANIFEST_JSON: JSON.stringify({ execution_model: {} })
    },
    writeState: true,
    createExecutor: async ({ env, readOnly }) => {
      assert.equal(readOnly, false);
      assert.equal(env.STRATEGY_CANARY_DRY_RUN, "true");
      assert.equal(env.ALLOW_LIVE, "false");
      assert.equal(env.ALLOW_STRATEGY_CANARY, "false");
      assert.equal(env.ENABLE_TAKER_ORDERS, "false");
      return {
        warmMarket: async () => {},
        status: () => ({
          safety_snapshot_cache_ready: true,
          safety_snapshot_cache_in_flight: 0,
          safety_snapshot_cache_error: null
        }),
        reconciliationSnapshot: () => clean,
        close: async () => { closed = true; }
      };
    },
    discoverMarket: async () => ({
      id: "market",
      conditionId: "condition",
      clobTokenIds: ["up", "down"],
      endDate: "2026-08-12T08:00:00Z"
    }),
    logger: () => {}
  });
  assert.equal(snapshot, clean);
  assert.equal(closed, true);
});

test("reconciliation waits through a transient cache error but not a persistent one", async () => {
  const env = {
    VENUE_PROBE_FUNDED_CAMPAIGN_ID: sessionId,
    FUNDED_DIRECT_SESSION_MANIFEST_JSON: JSON.stringify({ execution_model: {} })
  };
  const market = {
    id: "market",
    conditionId: "condition",
    clobTokenIds: ["up", "down"],
    endDate: "2026-08-12T08:00:00Z"
  };
  let statusCalls = 0;
  const snapshot = await runFundedDirectReconciliation({
    env,
    createExecutor: async () => ({
      warmMarket: async () => {},
      status: () => (++statusCalls === 1 ? {
        safety_snapshot_cache_error: "temporary venue timeout"
      } : {
        safety_snapshot_cache_ready: true,
        safety_snapshot_cache_in_flight: 0,
        safety_snapshot_cache_error: null
      }),
      reconciliationSnapshot: () => clean,
      close: async () => {}
    }),
    discoverMarket: async () => market,
    sleep: async () => {},
    logger: () => {}
  });
  assert.equal(snapshot, clean);

  await assert.rejects(runFundedDirectReconciliation({
    env,
    createExecutor: async () => ({
      warmMarket: async () => {},
      status: () => ({ safety_snapshot_cache_error: "persistent risk blocker" }),
      reconciliationSnapshot: () => { throw new Error("must not read a blocked snapshot"); },
      close: async () => {}
    }),
    discoverMarket: async () => market,
    sleep: async () => {},
    logger: () => {}
  }), /persistent risk blocker/);
});

test("reconciliation remains fail-closed on any live risk blocker", () => {
  assert.throws(
    () => validateFundedReconciliationSnapshot({
      ...clean,
      risk_passed: false,
      blockers: ["open_order"]
    }, sessionId),
    /live reconciliation is not clean/
  );
  assert.throws(
    () => validateFundedReconciliationSnapshot({ ...clean, snapshot_age_ms: 651 }, sessionId),
    /live reconciliation is not clean/
  );
});

test("cleanup failure prevents reconciliation proof from being logged", async () => {
  const logs = [];
  await assert.rejects(
    runFundedDirectReconciliation({
      env: {
        VENUE_PROBE_FUNDED_CAMPAIGN_ID: sessionId,
        FUNDED_DIRECT_SESSION_MANIFEST_JSON: JSON.stringify({ execution_model: {} })
      },
      createExecutor: async () => ({
        warmMarket: async () => {},
        status: () => ({
          safety_snapshot_cache_ready: true,
          safety_snapshot_cache_in_flight: 0,
          safety_snapshot_cache_error: null
        }),
        reconciliationSnapshot: () => clean,
        close: async () => { throw new Error("cleanup failed"); }
      }),
      discoverMarket: async () => ({
        id: "market",
        conditionId: "condition",
        clobTokenIds: ["up", "down"],
        endDate: "2026-08-12T08:00:00Z"
      }),
      sleep: async () => {},
      logger: (value) => logs.push(value)
    }),
    /cleanup failed/
  );
  assert.deepEqual(logs, []);
});
