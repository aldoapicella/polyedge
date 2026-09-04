import test from "node:test";
import assert from "node:assert/strict";
import {
  createOciQueueBridgeReceiver,
  fundedRedemptionMaintenanceWindow,
  loadFundedDirectServiceConfig,
  runFundedDirectService,
  runPersistentFundedDirectService
} from "../src/funded-direct-service.mjs";

const OCI_QUEUE_BRIDGE_URL = "http://10.89.0.1:8182/v1/messages";

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

test("funded service caps the SLO to retain eight seconds of immutable intent TTL", () => {
  assert.equal(loadFundedDirectServiceConfig(persistentEnv()).signalToSendSloMs, 7_000);
  assert.throws(
    () => loadFundedDirectServiceConfig(persistentEnv({ FUNDED_DIRECT_SIGNAL_TO_SEND_SLO_MS: "7001" })),
    /FUNDED_DIRECT_SIGNAL_TO_SEND_SLO_MS must be in \[500, 7000\]/
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
    FUNDED_DIRECT_SIGNAL_TO_SEND_SLO_MS: "7000",
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
    FUNDED_DIRECT_AUTO_REDEMPTION_MAX_SECONDS_TO_EXPIRY: "300",
    FUNDED_DIRECT_SESSION_MANIFEST_JSON: JSON.stringify(session),
    VENUE_PROBE_FUNDED_CAMPAIGN_ID: session.session_id,
    POLYMARKET_RELAYER_API_KEY: "relayer-key",
    POLYMARKET_RELAYER_API_KEY_ADDRESS: "0xc9f6f0D01e5eEf2446819Ce21C4f1F9b688A9921",
    ...overrides
  });
}

test("OCI queue bridge is exclusive and preserves receive settlement semantics", async () => {
  const bridgeEnv = persistentEnv({
    FUNDED_DIRECT_SERVICE_BUS_NAMESPACE: "",
    FUNDED_DIRECT_SERVICE_BUS_QUEUE: "",
    FUNDED_DIRECT_OCI_QUEUE_BRIDGE_URL: OCI_QUEUE_BRIDGE_URL
  });
  assert.equal(loadFundedDirectServiceConfig(bridgeEnv).ociQueueBridgeUrl, OCI_QUEUE_BRIDGE_URL);
  assert.throws(
    () => loadFundedDirectServiceConfig({
      ...bridgeEnv,
      FUNDED_DIRECT_SERVICE_BUS_QUEUE: "stale-azure-queue"
    }),
    /must not retain a Service Bus binding/
  );

  const calls = [];
  const receiver = createOciQueueBridgeReceiver(OCI_QUEUE_BRIDGE_URL, async (url, options = {}) => {
    calls.push({ url: String(url), method: options.method || "GET", body: options.body && JSON.parse(options.body) });
    if (String(url).includes("/messages?")) {
      return new Response(JSON.stringify({ messages: [{
        message_id: "decision-1",
        delivery_count: 2,
        receipt: "receipt-1",
        body: { schema: "polyedge.funded_intent_handoff.v1", decision_id: "decision-1" }
      }] }), { status: 200, headers: { "content-type": "application/json" } });
    }
    return new Response(null, { status: 204 });
  });
  const [message] = await receiver.receiveMessages(1, { maxWaitTimeInMs: 1_000 });
  assert.equal(message.messageId, "decision-1");
  assert.equal(message.deliveryCount, 2);
  await receiver.completeMessage(message);
  await receiver.abandonMessage(message);
  await receiver.deadLetterMessage(message, {
    deadLetterReason: "terminal",
    deadLetterErrorDescription: "failed closed"
  });
  assert.deepEqual(calls.map(({ url, method }) => [new URL(url).pathname, method]), [
    ["/v1/messages", "GET"],
    ["/v1/complete", "POST"],
    ["/v1/abandon", "POST"],
    ["/v1/dead-letter", "POST"]
  ]);
  assert.equal(calls[3].body.receipt, "receipt-1");
  assert.equal(calls[3].body.reason, "terminal");
});

test("automatic redemption remains strictly inside the final no-trade window", () => {
  const config = loadFundedDirectServiceConfig(automaticRedemptionEnv());
  assert.equal(config.autoRedemptionMaxSecondsToExpiry, 300);
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
    Date.parse("2026-07-30T12:10:00Z"),
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
      FUNDED_DIRECT_AUTO_REDEMPTION_MAX_SECONDS_TO_EXPIRY: "301"
    })),
    /300 seconds of the final 360 seconds/
  );
});

function fakeBus(messages) {
  const completed = [];
  const deadLettered = [];
  const receiveCalls = [];
  const received = [];
  const renewed = [];
  const receiver = {
    async receiveMessages(maxMessages, options) {
      receiveCalls.push({ maxMessages, options });
      const result = messages.splice(0, maxMessages);
      received.push(...result.map((message) => message.messageId));
      return result;
    },
    async renewMessageLock(message) { renewed.push(message.messageId); },
    async completeMessage(message) { completed.push(message.messageId); },
    async deadLetterMessage(message) { deadLettered.push(message.messageId); },
    async abandonMessage() {},
    async close() {}
  };
  return {
    completed,
    deadLettered,
    receiveCalls,
    received,
    renewed,
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
  const logs = [];
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
            order_submitted: true,
            lifecycle: {
              order_id: "acknowledged-order",
              send_wall_ms: Date.parse(decisionTs) + 750,
              ack_wall_ms: Date.parse(decisionTs) + 751
            }
          }
        };
      }
    }),
    logger: (value) => logs.push(value)
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
  assert.deepEqual(bus.renewed, []);
  assert.equal(logs.find((value) => value.schema === "polyedge.funded_direct_latency.v1")?.order_submitted, true);
});

test("persistent service coalesces only duplicate market-token warmups", async () => {
  const decisionTs = new Date(Date.now() - 500).toISOString();
  const bus = fakeBus([
    {
      messageId: "warmup-first",
      deliveryCount: 1,
      body: {
        schema: "polyedge.funded_market_warmup.v1",
        market_id: "btc-market",
        token_id: "token-up"
      }
    },
    {
      messageId: "warmup-duplicate",
      deliveryCount: 1,
      body: {
        schema: "polyedge.funded_market_warmup.v1",
        market_id: "btc-market",
        token_id: "token-up"
      }
    },
    {
      messageId: "warmup-other-token",
      deliveryCount: 1,
      body: {
        schema: "polyedge.funded_market_warmup.v1",
        market_id: "btc-market",
        token_id: "token-down"
      }
    },
    {
      messageId: "intent-first",
      deliveryCount: 1,
      body: {
        schema: "polyedge.funded_intent_handoff.v1",
        decision_id: "c".repeat(64),
        decision_ts: decisionTs
      }
    }
  ]);
  const order = [];
  const result = await runPersistentFundedDirectService({
    env: persistentEnv({ FUNDED_DIRECT_SERVICE_MAX_MESSAGES: "4" }),
    createBusClient: () => bus.client,
    createExecutor: async () => ({
      warmMarket: async () => { order.push("warmup"); },
      execute: async () => {},
      status: () => ({ ready: true }),
      close: async () => {}
    }),
    createProcessor: async () => ({
      process: async () => {
        order.push("intent");
        return { execution: { lifecycle: { send_wall_ms: Date.parse(decisionTs) + 1 } } };
      }
    }),
    logger: () => {}
  });
  assert.equal(result.processed_messages, 4);
  assert.deepEqual(order, ["warmup", "warmup", "intent"]);
  assert.deepEqual(bus.completed.sort(), ["intent-first", "warmup-duplicate", "warmup-first", "warmup-other-token"]);
  assert.deepEqual(bus.renewed, []);
});

test("persistent service seals a fresh intent busy while the first child is delayed", async () => {
  const decisionTs = new Date(Date.now() - 500).toISOString();
  const bus = fakeBus(["first", "fresh"].map((messageId) => ({
    messageId,
    deliveryCount: 1,
    body: {
      schema: "polyedge.funded_intent_handoff.v1",
      decision_id: messageId.repeat(64),
      decision_ts: decisionTs
    }
  })));
  const processed = [];
  const logs = [];
  let releaseFirst;
  let firstStarted;
  const firstStartedPromise = new Promise((resolve) => { firstStarted = resolve; });
  const resultPromise = runPersistentFundedDirectService({
    env: persistentEnv({ FUNDED_DIRECT_SERVICE_MAX_MESSAGES: "2" }),
    createBusClient: () => bus.client,
    createExecutor: async () => ({
      warmMarket: async () => {},
      execute: async () => {},
      status: () => ({ ready: true }),
      close: async () => {}
    }),
    createProcessor: async () => ({
      process: async (handoff) => {
        processed.push(handoff.decision_id);
        if (handoff.decision_id.startsWith("first")) {
          firstStarted();
          await new Promise((resolve) => { releaseFirst = resolve; });
        }
        return { execution: { lifecycle: { send_wall_ms: Date.parse(decisionTs) + 1 } } };
      },
      rejectBusy: async () => ({ status: "one_workflow_busy" })
    }),
    logger: (value) => logs.push(value)
  });
  await firstStartedPromise;
  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(bus.received, ["first", "fresh"]);
  assert.deepEqual(bus.renewed, []);
  releaseFirst();
  const result = await resultPromise;
  assert.equal(result.processed_messages, 2, JSON.stringify(logs));
  assert.deepEqual(bus.received, ["first", "fresh"]);
  assert.equal(processed.length, 1);
});

test("persistent service warms a new market after the active intent finishes", async () => {
  const decisionTs = new Date(Date.now() - 500).toISOString();
  const bus = fakeBus([
    {
      messageId: "active-intent",
      deliveryCount: 1,
      body: {
        schema: "polyedge.funded_intent_handoff.v1",
        decision_id: "e".repeat(64),
        decision_ts: decisionTs
      }
    },
    {
      messageId: "next-market",
      deliveryCount: 1,
      body: {
        schema: "polyedge.funded_market_warmup.v1",
        market_id: "next-market",
        condition_id: "next-condition",
        token_id: "next-token",
        token_ids: ["next-token", "other-token"],
        market_end_ts: new Date(Date.now() + 600_000).toISOString()
      }
    }
  ]);
  let releaseIntent;
  let intentStarted;
  const intentStartedPromise = new Promise((resolve) => { intentStarted = resolve; });
  const warmups = [];
  const logs = [];
  const resultPromise = runPersistentFundedDirectService({
    env: persistentEnv({ FUNDED_DIRECT_SERVICE_MAX_MESSAGES: "2" }),
    createBusClient: () => bus.client,
    createExecutor: async () => ({
      warmMarket: async (value) => { warmups.push(value.market_id); },
      execute: async () => {},
      status: () => ({ ready: true }),
      close: async () => {}
    }),
    createProcessor: async () => ({
      process: async () => {
        intentStarted();
        await new Promise((resolve) => { releaseIntent = resolve; });
        return { execution: { lifecycle: { send_wall_ms: Date.parse(decisionTs) + 1 } } };
      }
    }),
    logger: (value) => logs.push(value)
  });
  await intentStartedPromise;
  for (let attempt = 0; attempt < 40 && bus.received.length < 2; attempt += 1) {
    await new Promise((resolve) => setImmediate(resolve));
  }
  assert.deepEqual(bus.received, ["active-intent", "next-market"]);
  assert.deepEqual(warmups, []);
  releaseIntent();
  const result = await resultPromise;
  assert.equal(result.processed_messages, 2);
  assert.deepEqual(warmups, ["next-market"]);
  assert.deepEqual(bus.completed, ["active-intent", "next-market"]);
  assert.equal(logs.some((value) => value.status === "market_warmup_deferred"), false);
  assert.equal(logs.some((value) => value.status === "market_warmup_waiting"), true);
  assert.equal(logs.some((value) => value.status === "market_warmed"), true);
});

test("persistent service counts only terminal warmup failures after a successful redelivery", async () => {
  const body = {
    schema: "polyedge.funded_market_warmup.v1",
    market_id: "retry-market",
    condition_id: "retry-condition",
    token_id: "retry-token",
    token_ids: ["retry-token", "other-token"],
    market_end_ts: new Date(Date.now() + 600_000).toISOString()
  };
  const messages = [0, 1].map((deliveryCount) => ({
    messageId: "retry-warmup",
    deliveryCount,
    body
  }));
  const abandoned = [];
  const completed = [];
  const deadLettered = [];
  const receiver = {
    async receiveMessages(maxMessages) { return messages.splice(0, maxMessages); },
    async renewMessageLock() {},
    async completeMessage(message) { completed.push(message.deliveryCount); },
    async abandonMessage(message) { abandoned.push(message.deliveryCount); },
    async deadLetterMessage(message) { deadLettered.push(message.deliveryCount); },
    async close() {}
  };
  let attempts = 0;
  const logs = [];
  const result = await runPersistentFundedDirectService({
    env: persistentEnv({ FUNDED_DIRECT_SERVICE_MAX_MESSAGES: "2" }),
    createBusClient: () => ({
      createReceiver: () => receiver,
      async close() {}
    }),
    createExecutor: async () => ({
      warmMarket: async () => {
        attempts += 1;
        if (attempts === 1) throw new Error("transient warmup disagreement");
      },
      execute: async () => {},
      status: () => ({ ready: true }),
      close: async () => {}
    }),
    createProcessor: async () => ({ process: async () => ({}) }),
    logger: (value) => logs.push(value)
  });
  assert.equal(attempts, 2);
  assert.equal(result.processed_messages, 1);
  assert.equal(result.failed_messages, 0);
  assert.equal(result.failed_attempts, 1);
  assert.deepEqual(abandoned, [0]);
  assert.deepEqual(completed, [1]);
  assert.deepEqual(deadLettered, []);
  const failure = logs.find((value) => value.status === "persistent_message_failed_closed");
  assert.equal(failure.terminal_failure, false);
  assert.equal(failure.settlement_action, "abandoned_for_retry");
  assert.equal(failure.settlement_succeeded, true);
  assert.equal(logs.some((value) => value.status === "market_warmed"), true);
});

test("persistent service counts a terminal failure only after dead-letter settlement succeeds", async () => {
  const logs = [];
  const deadLettered = [];
  const result = await runPersistentFundedDirectService({
    env: persistentEnv({ FUNDED_DIRECT_SERVICE_MAX_MESSAGES: "1" }),
    createBusClient: () => ({
      createReceiver: () => ({
        receiveMessages: async () => [{
          messageId: "terminal-warmup",
          deliveryCount: 2,
          body: { schema: "polyedge.funded_market_warmup.v1" }
        }],
        renewMessageLock: async () => {},
        completeMessage: async () => {},
        abandonMessage: async () => {},
        deadLetterMessage: async (message) => { deadLettered.push(message.deliveryCount); },
        close: async () => {}
      }),
      close: async () => {}
    }),
    createExecutor: async () => ({
      warmMarket: async () => { throw new Error("transient warmup disagreement"); },
      execute: async () => {},
      status: () => ({ ready: true }),
      close: async () => {}
    }),
    createProcessor: async () => ({ process: async () => ({}) }),
    logger: (value) => logs.push(value)
  });
  assert.equal(result.failed_attempts, 1);
  assert.equal(result.failed_messages, 1);
  assert.deepEqual(deadLettered, [2]);
  const failure = logs.find((value) => value.status === "persistent_message_failed_closed");
  assert.equal(failure.terminal_failure, true);
  assert.equal(failure.settlement_action, "dead_lettered");
  assert.equal(failure.settlement_succeeded, true);
});

test("persistent service falls back to retry when dead-letter settlement fails", async () => {
  const logs = [];
  const abandoned = [];
  const result = await runPersistentFundedDirectService({
    env: persistentEnv({ FUNDED_DIRECT_SERVICE_MAX_MESSAGES: "1" }),
    createBusClient: () => ({
      createReceiver: () => ({
        receiveMessages: async () => [{
          messageId: "terminal-settlement-failed",
          deliveryCount: 2,
          body: { schema: "polyedge.funded_market_warmup.v1" }
        }],
        renewMessageLock: async () => {},
        completeMessage: async () => {},
        abandonMessage: async (message) => { abandoned.push(message.deliveryCount); },
        deadLetterMessage: async () => { throw new Error("dead-letter lock lost"); },
        close: async () => {}
      }),
      close: async () => {}
    }),
    createExecutor: async () => ({
      warmMarket: async () => { throw new Error("transient warmup disagreement"); },
      execute: async () => {},
      status: () => ({ ready: true }),
      close: async () => {}
    }),
    createProcessor: async () => ({ process: async () => ({}) }),
    logger: (value) => logs.push(value)
  });
  assert.equal(result.failed_attempts, 1);
  assert.equal(result.failed_messages, 0);
  assert.deepEqual(abandoned, [2]);
  const failure = logs.find((value) => value.status === "persistent_message_failed_closed");
  assert.equal(failure.terminal_failure, false);
  assert.equal(failure.settlement_action, "abandoned_for_retry");
  assert.equal(failure.settlement_fallback_from, "dead_lettered");
  assert.equal(failure.settlement_succeeded, true);
  assert.equal(failure.broker_redelivery_expected, true);
});

test("persistent service bounds transient failed attempts with max messages", async () => {
  const messages = [0, 1, 1].map((deliveryCount, index) => ({
    messageId: `bounded-retry-${index}`,
    deliveryCount,
    body: { schema: "polyedge.funded_market_warmup.v1" }
  }));
  const abandoned = [];
  let attempts = 0;
  const result = await runPersistentFundedDirectService({
    env: persistentEnv({ FUNDED_DIRECT_SERVICE_MAX_MESSAGES: "2" }),
    createBusClient: () => ({
      createReceiver: () => ({
        receiveMessages: async (maxMessages) => messages.splice(0, maxMessages),
        renewMessageLock: async () => {},
        completeMessage: async () => {},
        abandonMessage: async (message) => { abandoned.push(message.messageId); },
        deadLetterMessage: async () => {},
        close: async () => {}
      }),
      close: async () => {}
    }),
    createExecutor: async () => ({
      warmMarket: async () => { attempts += 1; throw new Error("transient warmup disagreement"); },
      execute: async () => {},
      status: () => ({ ready: true }),
      close: async () => {}
    }),
    createProcessor: async () => ({ process: async () => ({}) }),
    logger: () => {}
  });
  assert.equal(attempts, 2);
  assert.equal(result.failed_attempts, 2);
  assert.equal(result.failed_messages, 0);
  assert.deepEqual(abandoned, ["bounded-retry-0", "bounded-retry-1"]);
  assert.equal(messages.length, 1);
});

test("persistent service redelivers idempotently after durable completion loses its broker lock", async () => {
  const decisionTs = new Date(Date.now() - 500).toISOString();
  const body = {
    schema: "polyedge.funded_intent_handoff.v1",
    decision_id: "d".repeat(64),
    decision_ts: decisionTs
  };
  const messages = [0, 1].map((deliveryCount) => ({
    messageId: "durable-redelivery",
    deliveryCount,
    body
  }));
  const completed = [];
  const abandoned = [];
  const deadLettered = [];
  let receiverOptions;
  let manualRenewals = 0;
  const receiver = {
    async receiveMessages(maxMessages) { return messages.splice(0, maxMessages); },
    async renewMessageLock() { manualRenewals += 1; },
    async completeMessage(message) {
      if (message.deliveryCount === 0) {
        const error = new Error("");
        error.name = "ServiceBusError";
        error.code = "GeneralError";
        error.reason = "MessageLockLost";
        throw error;
      }
      completed.push(message.deliveryCount);
    },
    async abandonMessage(message) { abandoned.push(message.deliveryCount); },
    async deadLetterMessage(message) { deadLettered.push(message.deliveryCount); },
    async close() {}
  };
  const logs = [];
  let executions = 0;
  let processingCalls = 0;
  const process = async () => {
    processingCalls += 1;
    if (processingCalls === 1) {
      executions += 1;
      return {
        status: "persistent_intent_completed",
        execution: {
          order_submission_attempted: true,
          lifecycle: {
            order_id: "durable-order",
            send_wall_ms: Date.parse(decisionTs) + 1,
            ack_wall_ms: Date.parse(decisionTs) + 2
          }
        }
      };
    }
    return {
      status: "already_completed_idempotent",
      completion: {
        schema: "polyedge.operator_funded_intent_completion.v1",
        order_submission_attempted: true
      }
    };
  };

  const result = await runPersistentFundedDirectService({
    env: persistentEnv({ FUNDED_DIRECT_SERVICE_MAX_MESSAGES: "1" }),
    createBusClient: () => ({
      createReceiver: (_queue, options) => {
        receiverOptions = options;
        return receiver;
      },
      async close() {}
    }),
    createExecutor: async () => ({
      warmMarket: async () => {}, execute: async () => {}, status: () => ({ ready: true }), close: async () => {}
    }),
    createProcessor: async () => ({ process, rejectBusy: process }),
    logger: (value) => logs.push(value)
  });

  assert.equal(result.processed_messages, 1);
  assert.equal(result.failed_messages, 0);
  assert.equal(executions, 1);
  assert.equal(processingCalls, 2);
  assert.deepEqual(completed, [1]);
  assert.deepEqual(abandoned, []);
  assert.deepEqual(deadLettered, []);
  assert.deepEqual(receiverOptions, {
    receiveMode: "peekLock",
    maxAutoLockRenewalDurationInMs: 300_000
  });
  assert.equal(manualRenewals, 0);
  const lost = logs.find((value) =>
    value.status === "persistent_message_settlement_lost_after_durable_completion"
  );
  assert.equal(lost?.message_id, "durable-redelivery");
  assert.equal(lost?.delivery_count, 0);
  assert.equal(lost?.broker_redelivery_expected, true);
  assert.equal(lost?.error, "MessageLockLost");
  assert.equal(lost?.error_detail?.name, "ServiceBusError");
  assert.equal(lost?.error_detail?.code, "GeneralError");
  assert.equal(lost?.error_detail?.reason, "MessageLockLost");
  assert.equal(lost?.error_detail?.message, null);
});

test("persistent service settles a twelve-intent burst with one actual workflow", async () => {
  const decisionTs = new Date(Date.now() - 500).toISOString();
  const messages = Array.from({ length: 12 }, (_, index) => {
    const decisionId = index.toString(16).padStart(64, "0");
    return {
      messageId: `intent-${index}`,
      deliveryCount: 1,
      body: {
        schema: "polyedge.funded_intent_handoff.v1",
        decision_id: decisionId,
        decision_ts: decisionTs
      }
    };
  });
  const bus = fakeBus(messages);
  let releaseFirst;
  let firstStarted;
  const firstStartedPromise = new Promise((resolve) => { firstStarted = resolve; });
  let executions = 0;
  let busyRejections = 0;
  const resultPromise = runPersistentFundedDirectService({
    env: persistentEnv({ FUNDED_DIRECT_SERVICE_MAX_MESSAGES: "12" }),
    createBusClient: () => bus.client,
    createExecutor: async () => ({
      warmMarket: async () => {}, execute: async () => {}, status: () => ({ ready: true }), close: async () => {}
    }),
    createProcessor: async () => ({
      process: async () => {
        executions += 1;
        firstStarted();
        await new Promise((resolve) => { releaseFirst = resolve; });
        return { execution: { lifecycle: { send_wall_ms: Date.parse(decisionTs) + 1 } } };
      },
      rejectBusy: async () => {
        busyRejections += 1;
        return { status: "one_workflow_busy" };
      }
    }),
    logger: () => {}
  });
  await firstStartedPromise;
  for (let attempt = 0; attempt < 40 && busyRejections < 11; attempt += 1) {
    await new Promise((resolve) => setImmediate(resolve));
  }
  assert.equal(executions, 1);
  assert.equal(busyRejections, 11);
  assert.equal(bus.received.length, 12);
  assert.equal(bus.completed.length, 11);
  releaseFirst();
  const result = await resultPromise;
  assert.equal(result.processed_messages, 12);
  assert.equal(result.failed_messages, 0);
  assert.equal(bus.completed.length, 12);
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

test("persistent service coalesces a duplicate warmup while redemption maintenance is running", async () => {
  const now = Date.parse("2026-07-30T12:10:00Z");
  const warmup = (messageId) => ({
    messageId,
    deliveryCount: 1,
    body: {
      schema: "polyedge.funded_market_warmup.v1",
      market_id: "btc-market",
      token_id: "token-up",
      market_end_ts: "2026-07-30T12:15:00Z"
    }
  });
  const bus = fakeBus([warmup("warmup-first"), warmup("warmup-duplicate")]);
  let warmedMarket = null;
  let releaseMaintenance;
  let maintenanceStarted;
  const maintenanceStartedPromise = new Promise((resolve) => { maintenanceStarted = resolve; });
  const resultPromise = runPersistentFundedDirectService({
    env: automaticRedemptionEnv({ FUNDED_DIRECT_SERVICE_MAX_MESSAGES: "2" }),
    now: () => now,
    createBusClient: () => bus.client,
    createExecutor: async () => ({
      warmMarket: async (value) => { warmedMarket = value; },
      execute: async () => {},
      runMaintenance: async (task) => {
        await new Promise((resolve) => {
          releaseMaintenance = resolve;
          maintenanceStarted();
        });
        return task({ lease: { assertHealthy() {} } });
      },
      status: () => ({ warmed_market: warmedMarket }),
      close: async () => {}
    }),
    createProcessor: async () => ({ process: async () => ({}) }),
    runRedemption: async () => ({ status: "nothing_to_redeem", redemption_submitted: false }),
    logger: () => {}
  });

  await maintenanceStartedPromise;
  for (let attempt = 0; attempt < 40 && bus.completed.length < 2; attempt += 1) {
    await new Promise((resolve) => setImmediate(resolve));
  }
  assert.deepEqual(bus.received, ["warmup-first", "warmup-duplicate"]);
  assert.deepEqual(bus.completed, ["warmup-first", "warmup-duplicate"]);
  assert.deepEqual(bus.deadLettered, []);
  releaseMaintenance();
  const result = await resultPromise;
  assert.equal(result.failed_messages, 0);
  assert.equal(result.redemption_results, 1);
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
  assert.equal(completion.order_submitted, false);
  assert.equal(completion.worker_status, "already_completed_idempotent");
  assert.deepEqual(bus.completed, ["duplicate-post-submit"]);
});

test("persistent service derives submission only from an acknowledged lifecycle", async () => {
  const decisionTs = new Date(Date.now() - 500).toISOString();
  const bus = fakeBus(["acknowledged", "send-only", "attempt-only", "explicit-false", "ambiguous-true", "terminal-no-fill"].map((messageId) => ({
    messageId,
    deliveryCount: 1,
    body: {
      schema: "polyedge.funded_intent_handoff.v1",
      decision_id: messageId.repeat(16),
      decision_ts: decisionTs
    }
  })));
  const logs = [];
  await runPersistentFundedDirectService({
    env: persistentEnv({ FUNDED_DIRECT_SERVICE_MAX_MESSAGES: "6" }),
    createBusClient: () => bus.client,
    createExecutor: async () => ({
      warmMarket: async () => {},
      execute: async () => {},
      status: () => ({ ready: true }),
      close: async () => {}
    }),
    createProcessor: async () => ({
      process: async (handoff) => ({
        execution: handoff.decision_id.startsWith("acknowledged")
          ? {
              order_submission_attempted: true,
              lifecycle: {
                order_id: "acknowledged-order",
                send_wall_ms: Date.parse(decisionTs) + 1,
                ack_wall_ms: Date.parse(decisionTs) + 2
              }
            }
          : handoff.decision_id.startsWith("send-only")
            ? {
                order_submission_attempted: true,
                lifecycle: { order_id: "send-only-order", send_wall_ms: Date.parse(decisionTs) + 1 }
              }
            : handoff.decision_id.startsWith("explicit-false")
              ? {
                  order_submission_attempted: true,
                  order_submitted: false,
                  lifecycle: {
                    order_id: "explicit-false-order",
                    send_wall_ms: Date.parse(decisionTs) + 1,
                    ack_wall_ms: Date.parse(decisionTs) + 2
                  }
                }
              : handoff.decision_id.startsWith("ambiguous-true")
                ? { order_submission_attempted: true, order_submitted: true, lifecycle: null }
                : handoff.decision_id.startsWith("terminal-no-fill")
                  ? {
                      status: "terminal_no_fill_evidence_degraded",
                      order_submission_attempted: true,
                      order_submitted: true,
                      lifecycle: {
                        order_id: "terminal-no-fill-order",
                        reconciliation_complete: true,
                        zero_open_orders_confirmed: true,
                        matched_notional: 0
                      }
                    }
                  : { order_submission_attempted: true }
      })
    }),
    logger: (value) => logs.push(value)
  });

  const latency = Object.fromEntries(logs
    .filter((value) => value.schema === "polyedge.funded_direct_latency.v1")
    .map((value) => [value.decision_id, value]));
  assert.equal(latency["acknowledged".repeat(16)].order_submitted, true);
  assert.equal(latency["send-only".repeat(16)].order_submitted, false);
  assert.equal(latency["attempt-only".repeat(16)].order_submitted, false);
  assert.equal(latency["explicit-false".repeat(16)].order_submitted, false);
  assert.equal(latency["ambiguous-true".repeat(16)].order_submitted, false);
  assert.equal(latency["terminal-no-fill".repeat(16)].order_submitted, true);
});

test("persistent service pauses after three consecutive transitions above the reviewed SLO", async () => {
  const messages = Array.from({ length: 4 }, (_, index) => {
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
          lifecycle: { send_wall_ms: Date.parse(handoff.decision_ts) + 7_500 }
        }
      }),
      rejectBusy: async () => ({ status: "one_workflow_busy" })
    }),
    logger: (value) => logs.push(value)
  });
  assert.equal(result.processed_messages, 3);
  assert.equal(result.failed_messages, 1);
  assert.ok(logs.some((value) => value.status === "engine_paused_by_consecutive_latency_breaches"));
  assert.deepEqual(bus.completed, ["decision-0", "decision-1", "decision-2"]);
  assert.deepEqual(bus.received, ["decision-0", "decision-1", "decision-2", "decision-3"]);
  assert.deepEqual(bus.deadLettered, ["decision-3"]);
});

test("persistent service uses the streaming receiver without SDK batch-drain polling", async () => {
  const message = {
    messageId: "streaming-intent",
    deliveryCount: 1,
    body: {
      schema: "polyedge.funded_intent_handoff.v1",
      decision_id: "f".repeat(64),
      decision_ts: new Date(Date.now() - 500).toISOString()
    }
  };
  let subscribeOptions;
  let subscriptionClosed = false;
  let completed = false;
  let handlerPromise;
  const receiver = {
    subscribe(handlers, options) {
      subscribeOptions = options;
      queueMicrotask(() => { handlerPromise = handlers.processMessage(message); });
      return { async close() { subscriptionClosed = true; } };
    },
    async receiveMessages() { throw new Error("batch receiver must not be used"); },
    async completeMessage() { completed = true; },
    async abandonMessage() {},
    async deadLetterMessage() {},
    async close() { await handlerPromise; }
  };
  const logs = [];
  const result = await runPersistentFundedDirectService({
    env: persistentEnv({ FUNDED_DIRECT_SERVICE_MAX_MESSAGES: "1" }),
    createBusClient: () => ({ createReceiver: () => receiver, async close() {} }),
    createExecutor: async () => ({
      warmMarket: async () => {}, execute: async () => {}, status: () => ({ ready: true }), close: async () => {}
    }),
    createProcessor: async () => ({
      process: async () => ({ status: "child_failed_closed_pre_submission" }),
      rejectBusy: async () => ({ status: "one_workflow_busy" })
    }),
    logger: (value) => logs.push(value)
  });

  assert.equal(result.processed_messages, 1);
  assert.equal(completed, true);
  assert.equal(subscriptionClosed, true);
  assert.deepEqual(subscribeOptions, { autoCompleteMessages: false, maxConcurrentCalls: 5 });
  assert.equal(logs.find((value) => value.status === "persistent_service_started")?.handoff,
    "azure_service_bus_streaming_peek_lock");
});
