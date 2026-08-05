import test from "node:test";
import assert from "node:assert/strict";
import {
  fundedRedemptionMaintenanceWindow,
  loadFundedDirectServiceConfig,
  runFundedDirectService,
  runPersistentFundedDirectService
} from "../src/funded-direct-service.mjs";

function env(overrides = {}) {
  return {
    FUNDED_DIRECT_SERVICE_ENABLED: "true",
    FUNDED_DIRECT_SERVICE_RESTART_DELAY_MS: "1000",
    FUNDED_DIRECT_SERVICE_RISK_PAUSE_MS: "60000",
    FUNDED_DIRECT_SERVICE_HEARTBEAT_MS: "60000",
    FUNDED_DIRECT_SERVICE_MAX_CYCLES: "2",
    FUNDED_DIRECT_POLL_INTERVAL_MS: "5000",
    ...overrides
  };
}

test("continuous funded service is disabled by default", () => {
  assert.throws(() => loadFundedDirectServiceConfig({}), /FUNDED_DIRECT_SERVICE_ENABLED/);
});

test("funded service validates the one-second Service Bus receive interval", () => {
  assert.equal(loadFundedDirectServiceConfig(env({ FUNDED_DIRECT_POLL_INTERVAL_MS: "1000" })).pollIntervalMs, 1_000);
  assert.throws(
    () => loadFundedDirectServiceConfig(env({ FUNDED_DIRECT_POLL_INTERVAL_MS: "999" })),
    /FUNDED_DIRECT_POLL_INTERVAL_MS must be in \[1000, 60000\]/
  );
});

test("continuous funded service immediately restarts bounded worker cycles", async () => {
  const sleeps = [];
  const logs = [];
  const result = await runFundedDirectService({
    env: env(),
    runWorker: async () => ({ status: "idle_waiting_for_fresh_intent" }),
    sleep: async (ms) => sleeps.push(ms),
    logger: (value) => logs.push(value)
  });
  assert.deepEqual(sleeps, [1000]);
  assert.equal(result.status, "bounded_test_complete");
  assert.equal(result.cycles, 2);
  assert.equal(logs[0].status, "continuous_service_started");
  assert.equal(logs.filter((value) => value.status === "worker_cycle_completed").length, 2);
});

test("continuous funded service survives a failed-closed worker cycle", async () => {
  let attempts = 0;
  const result = await runFundedDirectService({
    env: env(),
    runWorker: async () => {
      attempts += 1;
      if (attempts === 1) throw new Error("transient fail-closed child");
      return { status: "idle_waiting_for_fresh_intent" };
    },
    sleep: async () => {},
    logger: () => {}
  });
  assert.equal(result.cycles, 2);
  assert.equal(result.worker_failures, 1);
});

function persistentEnv(overrides = {}) {
  return env({
    FUNDED_DIRECT_ENGINE: "persistent_v1",
    FUNDED_DIRECT_SERVICE_BUS_NAMESPACE: "sb-funded",
    FUNDED_DIRECT_SERVICE_BUS_QUEUE: "funded-intents",
    FUNDED_DIRECT_SIGNAL_TO_SEND_SLO_MS: "2000",
    FUNDED_DIRECT_SERVICE_MAX_MESSAGES: "2",
    ...overrides
  });
}

function automaticRedemptionEnv(overrides = {}) {
  const session = {
    session_id: "dynamic-quote-funded-2026-07-29-v5",
    starting_collateral: 11.09862,
    allow_compounding: true,
    no_deposits: true,
    allow_automatic_replenishment: false
  };
  return persistentEnv({
    FUNDED_DIRECT_AUTO_REDEMPTION_ENABLED: "true",
    FUNDED_DIRECT_AUTO_REDEMPTION_INTERVAL_MS: "60000",
    FUNDED_DIRECT_AUTO_REDEMPTION_MIN_SECONDS_TO_EXPIRY: "30",
    FUNDED_DIRECT_AUTO_REDEMPTION_MAX_SECONDS_TO_EXPIRY: "350",
    FUNDED_DIRECT_SESSION_MANIFEST_JSON: JSON.stringify(session),
    VENUE_PROBE_FUNDED_CAMPAIGN_ID: session.session_id,
    POLYMARKET_RELAYER_API_KEY: "relayer-key",
    POLYMARKET_RELAYER_API_KEY_ADDRESS: "0xc9f6f0D01e5eEf2446819Ce21C4f1F9b688A9921",
    ...overrides
  });
}

test("automatic redemption remains strictly inside the final no-trade window", () => {
  const config = loadFundedDirectServiceConfig(automaticRedemptionEnv());
  const status = (marketEndTs = "2026-07-30T12:15:00Z") => ({
    warmed_market: {
      market_id: "btc-market",
      market_end_ts: marketEndTs
    }
  });
  assert.equal(fundedRedemptionMaintenanceWindow(
    status(),
    Date.parse("2026-07-30T12:09:00Z"),
    config
  ).eligible, false);
  assert.equal(fundedRedemptionMaintenanceWindow(
    status(),
    Date.parse("2026-07-30T12:09:10Z"),
    config
  ).eligible, true);
  assert.equal(fundedRedemptionMaintenanceWindow(
    status(),
    Date.parse("2026-07-30T12:14:30Z"),
    config
  ).eligible, true);
  assert.equal(fundedRedemptionMaintenanceWindow(
    status(),
    Date.parse("2026-07-30T12:14:31Z"),
    config
  ).eligible, false);
  const futureWarmup = fundedRedemptionMaintenanceWindow(
    status("2026-07-30T12:30:00Z"),
    Date.parse("2026-07-30T12:10:00Z"),
    config
  );
  assert.equal(futureWarmup.eligible, true);
  assert.equal(futureWarmup.market_id, null);
  assert.equal(futureWarmup.market_end_ts, "2026-07-30T12:15:00.000Z");
  assert.equal(futureWarmup.clock_source, "btc_15m_utc_boundary");
  assert.throws(
    () => loadFundedDirectServiceConfig(automaticRedemptionEnv({
      FUNDED_DIRECT_AUTO_REDEMPTION_MAX_SECONDS_TO_EXPIRY: "360"
    })),
    /strictly inside the final 360 seconds/
  );
});

function fakeBus(messages) {
  const completed = [];
  const deadLettered = [];
  const receiveCalls = [];
  const receiver = {
    async receiveMessages(maxMessages, options) {
      receiveCalls.push({ maxMessages, options });
      const message = messages.shift();
      return message ? [message] : [];
    },
    async renewMessageLock() {},
    async completeMessage(message) { completed.push(message.messageId); },
    async deadLetterMessage(message) { deadLettered.push(message.messageId); },
    async abandonMessage() {},
    async close() {}
  };
  return {
    completed,
    deadLettered,
    receiveCalls,
    client: {
      createReceiver: () => receiver,
      async close() {}
    }
  };
}

test("persistent service reuses one warm executor and processes warmup plus intent without spawning", async () => {
  const decisionTs = new Date(Date.now() - 500).toISOString();
  const bus = fakeBus([
    {
      messageId: "warmup",
      deliveryCount: 1,
      body: {
        schema: "polyedge.funded_market_warmup.v1",
        market_id: "btc-market",
        condition_id: "condition",
        token_id: "token-up",
        token_ids: ["token-up", "token-down"],
        market_end_ts: new Date(Date.now() + 600_000).toISOString()
      }
    },
    {
      messageId: "decision",
      deliveryCount: 1,
      body: {
        schema: "polyedge.funded_intent_handoff.v1",
        decision_id: "a".repeat(64),
        decision_ts: decisionTs
      }
    }
  ]);
  let executorCreations = 0;
  let warmups = 0;
  let executions = 0;
  const result = await runPersistentFundedDirectService({
    env: persistentEnv({ FUNDED_DIRECT_POLL_INTERVAL_MS: "1000" }),
    createBusClient: () => bus.client,
    createExecutor: async () => {
      executorCreations += 1;
      return {
        warmMarket: async () => { warmups += 1; },
        execute: async () => { executions += 1; },
        status: () => ({ ready: true }),
        close: async () => {}
      };
    },
    createProcessor: async ({ executeCanary }) => ({
      process: async () => {
        await executeCanary({});
        return {
          execution: {
            order_submission_attempted: true,
            lifecycle: { send_wall_ms: Date.parse(decisionTs) + 750 }
          }
        };
      }
    }),
    logger: () => {}
  });
  assert.equal(result.status, "persistent_service_stopped");
  assert.equal(executorCreations, 1);
  assert.equal(warmups, 1);
  assert.equal(executions, 1);
  assert.deepEqual(bus.completed, ["warmup", "decision"]);
  assert.deepEqual(bus.deadLettered, []);
  assert.equal(bus.receiveCalls.length, 2);
  assert.ok(bus.receiveCalls.every(({ maxMessages, options }) =>
    maxMessages === 1 && options?.maxWaitTimeInMs === 1_000
  ));
});

test("persistent service waits for the prior revision lease before receiving messages", async () => {
  const bus = fakeBus([{
    messageId: "warmup-after-handoff",
    deliveryCount: 1,
    body: {
      schema: "polyedge.funded_market_warmup.v1",
      market_id: "btc-market",
      condition_id: "condition",
      token_id: "token-up",
      token_ids: ["token-up", "token-down"],
      market_end_ts: new Date(Date.now() + 600_000).toISOString()
    }
  }]);
  let executorCreations = 0;
  let sleeps = 0;
  const logs = [];
  const result = await runPersistentFundedDirectService({
    env: persistentEnv({ FUNDED_DIRECT_SERVICE_MAX_MESSAGES: "1" }),
    createBusClient: () => bus.client,
    createExecutor: async () => {
      executorCreations += 1;
      if (executorCreations < 3) {
        throw new Error("fail closed: another venue probe owns the campaign lease (409)");
      }
      return {
        warmMarket: async () => {},
        execute: async () => {},
        status: () => ({ ready: true }),
        close: async () => {}
      };
    },
    createProcessor: async () => ({ process: async () => ({}) }),
    sleep: async () => { sleeps += 1; },
    logger: (value) => logs.push(value)
  });

  assert.equal(result.processed_messages, 1);
  assert.equal(executorCreations, 3);
  assert.equal(sleeps, 2);
  assert.deepEqual(bus.completed, ["warmup-after-handoff"]);
  assert.deepEqual(
    logs.filter((value) => value.status === "awaiting_campaign_lease_handoff")
      .map((value) => value.attempt),
    [1, 2]
  );
});

test("persistent service runs redemption under the inherited lease after entering the no-trade window", async () => {
  const now = Date.parse("2026-07-30T12:10:00Z");
  const bus = fakeBus([{
    messageId: "warmup-next-market",
    deliveryCount: 1,
    body: {
      schema: "polyedge.funded_market_warmup.v1",
      market_id: "btc-market",
      condition_id: "condition",
      token_id: "token-up",
      token_ids: ["token-up", "token-down"],
      market_end_ts: "2026-07-30T12:30:00Z"
    }
  }]);
  const lease = { assertHealthy() {} };
  let warmedMarket = null;
  let maintenanceRuns = 0;
  let redemptionRuns = 0;
  const logs = [];
  const result = await runPersistentFundedDirectService({
    env: automaticRedemptionEnv({
      FUNDED_DIRECT_SERVICE_MAX_MESSAGES: "1",
      VENUE_REDEMPTION_MAX_PAYOUT: "25"
    }),
    now: () => now,
    createBusClient: () => bus.client,
    createExecutor: async () => ({
      warmMarket: async (value) => { warmedMarket = value; },
      execute: async () => {},
      runMaintenance: async (task) => {
        maintenanceRuns += 1;
        return task({ lease });
      },
      status: () => ({ warmed_market: warmedMarket }),
      close: async () => {}
    }),
    createProcessor: async () => ({ process: async () => ({}) }),
    runRedemption: async ({ env: redemptionEnv, inheritedLease }) => {
      redemptionRuns += 1;
      assert.equal(redemptionEnv.EXECUTION_MODE, "venue_redemption");
      assert.equal(redemptionEnv.VENUE_REDEMPTION_DRY_RUN, "false");
      assert.equal(redemptionEnv.FUNDED_EVIDENCE_TRUST_BOUNDARY_READY, "true");
      assert.equal("VENUE_REDEMPTION_MAX_PAYOUT" in redemptionEnv, false);
      assert.equal(inheritedLease, lease);
      return { status: "nothing_to_redeem", redemption_submitted: false };
    },
    logger: (value) => logs.push(value)
  });
  assert.equal(result.redemption_results, 1);
  assert.equal(maintenanceRuns, 1);
  assert.equal(redemptionRuns, 1);
  const completion = logs.find((value) => value.status === "automatic_redemption_cycle_completed");
  assert.equal(completion?.market_end_ts, "2026-07-30T12:15:00.000Z");
  assert.equal(completion?.clock_source, "btc_15m_utc_boundary");
});

test("persistent service preserves attempted-order observability for an idempotent completion", async () => {
  const decisionTs = new Date(Date.now() - 500).toISOString();
  const bus = fakeBus([{
    messageId: "duplicate-post-submit",
    deliveryCount: 2,
    body: {
      schema: "polyedge.funded_intent_handoff.v1",
      decision_id: "b".repeat(64),
      decision_ts: decisionTs
    }
  }]);
  const logs = [];
  await runPersistentFundedDirectService({
    env: persistentEnv({ FUNDED_DIRECT_SERVICE_MAX_MESSAGES: "1" }),
    createBusClient: () => bus.client,
    createExecutor: async () => ({
      warmMarket: async () => {},
      execute: async () => {},
      status: () => ({ ready: true }),
      close: async () => {}
    }),
    createProcessor: async () => ({
      process: async () => ({
        status: "already_completed_idempotent",
        completion: {
          status: "child_failed_closed_post_submission_unresolved",
          order_submission_attempted: true
        }
      })
    }),
    logger: (value) => logs.push(value)
  });

  const completion = logs.find((value) =>
    value.schema === "polyedge.funded_direct_latency.v1" &&
    value.decision_id === "b".repeat(64)
  );
  assert.equal(completion.order_submission_attempted, true);
  assert.equal(completion.worker_status, "already_completed_idempotent");
  assert.deepEqual(bus.completed, ["duplicate-post-submit"]);
});

test("persistent service pauses after three consecutive transitions above three seconds", async () => {
  const messages = Array.from({ length: 3 }, (_, index) => {
    const decisionTs = new Date(Date.now() - 500 - index).toISOString();
    return {
      messageId: `decision-${index}`,
      deliveryCount: 1,
      body: {
        schema: "polyedge.funded_intent_handoff.v1",
        decision_id: String(index).repeat(64),
        decision_ts: decisionTs
      },
      decisionTs
    };
  });
  const bus = fakeBus(messages.map(({ decisionTs: _, ...message }) => message));
  const logs = [];
  const result = await runPersistentFundedDirectService({
    env: persistentEnv({ FUNDED_DIRECT_SERVICE_MAX_MESSAGES: "0" }),
    createBusClient: () => bus.client,
    createExecutor: async () => ({
      warmMarket: async () => {},
      execute: async () => {},
      status: () => ({ ready: true }),
      close: async () => {}
    }),
    createProcessor: async () => ({
      process: async (handoff) => ({
        execution: {
          order_submission_attempted: true,
          lifecycle: { send_wall_ms: Date.parse(handoff.decision_ts) + 3_500 }
        }
      })
    }),
    logger: (value) => logs.push(value)
  });
  assert.equal(result.processed_messages, 3);
  assert.ok(logs.some((value) => value.status === "engine_paused_by_consecutive_latency_breaches"));
  assert.deepEqual(bus.completed, ["decision-0", "decision-1", "decision-2"]);
});
