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
    maxMessages === 1 &&
    options?.maxWaitTimeInMs === 1_000 &&
    options?.abortSignal instanceof AbortSignal
  ));
  assert.deepEqual(bus.renewed.sort(), ["decision", "warmup"]);
});

test("persistent service retries failed websocket reconciliation at poll cadence before processing a fresh handoff", async () => {
  const decisionId = "d".repeat(64);
  const decisionTs = new Date(Date.now() - 500).toISOString();
  const bus = fakeBus([{
    messageId: "intent-after-reconciliation",
    deliveryCount: 1,
    body: {
      schema: "polyedge.funded_intent_handoff.v1",
      decision_id: decisionId,
      decision_ts: decisionTs
    }
  }]);
  const order = [];
  const sleeps = [];
  const logs = [];
  let reconciliationRequired = true;
  let maintenanceAttempts = 0;
  const result = await runPersistentFundedDirectService({
    env: persistentEnv({
      FUNDED_DIRECT_POLL_INTERVAL_MS: "1000",
      FUNDED_DIRECT_SERVICE_MAX_MESSAGES: "1"
    }),
    createBusClient: () => ({
      createReceiver: () => ({
        ...bus.client.createReceiver(),
        async receiveMessages(...args) {
          order.push("receive");
          return bus.client.createReceiver().receiveMessages(...args);
        }
      }),
      async close() {}
    }),
    createExecutor: async () => ({
      runMaintenance: async (task) => {
        maintenanceAttempts += 1;
        order.push(`reconcile-${maintenanceAttempts}`);
        reconciliationRequired = false;
        if (maintenanceAttempts === 1) throw new Error("transient post-reconciliation account snapshot failure");
        return task({ lease: {} });
      },
      status: () => ({
        user_channel_ready: true,
        market_channel_ready: true,
        user_channel_gaps: 0,
        market_channel_gaps: reconciliationRequired ? 1 : 0,
        reconnect_reconciliation_required: reconciliationRequired,
        warmed_market: { condition_id: "condition" }
      }),
      close: async () => {}
    }),
    createProcessor: async () => ({
      process: async (handoff) => {
        order.push(`process-${handoff.decision_id}`);
        return { execution: { lifecycle: { send_wall_ms: Date.parse(decisionTs) + 1 } } };
      }
    }),
    sleep: async (ms) => { sleeps.push(ms); },
    logger: (value) => logs.push(value)
  });

  assert.equal(result.processed_messages, 1);
  assert.deepEqual(order, ["reconcile-1", "reconcile-2", "receive", `process-${decisionId}`]);
  assert.deepEqual(sleeps, [1_000]);
  assert.deepEqual(bus.completed, ["intent-after-reconciliation"]);
  assert.ok(logs.some((value) =>
    value.status === "websocket_reconciliation_failed_closed" &&
    value.account_risk_pause === true
  ));
  assert.ok(logs.some((value) => value.status === "websocket_reconciliation_completed"));
});

test("persistent service failure telemetry binds deterministic TTL rejection to its decision", async () => {
  const nowMs = Date.parse("2026-08-11T03:00:00Z");
  const decisionId = "e".repeat(64);
  const bus = fakeBus([{
    messageId: "expired-intent",
    deliveryCount: 1,
    body: {
      schema: "polyedge.funded_intent_handoff.v1",
      decision_id: decisionId,
      decision_ts: new Date(nowMs - 10_000).toISOString(),
      valid_until: new Date(nowMs - 1).toISOString()
    }
  }]);
  const logs = [];
  await runPersistentFundedDirectService({
    env: persistentEnv({ FUNDED_DIRECT_SERVICE_MAX_MESSAGES: "1" }),
    now: () => nowMs,
    createBusClient: () => bus.client,
    createExecutor: async () => ({
      status: () => ({ ready: true }),
      close: async () => {}
    }),
    createProcessor: async () => ({
      process: async () => { throw new Error("fail closed: funded intent handoff binding or TTL is invalid"); }
    }),
    logger: (value) => logs.push(value)
  });

  const failure = logs.find((value) => value.status === "persistent_message_failed_closed");
  assert.equal(failure.decision_id, decisionId);
  assert.equal(failure.intent_valid_until, "2026-08-11T02:59:59.999Z");
  assert.equal(failure.intent_remaining_ttl_ms, -1);
  assert.equal("body" in failure, false);
  assert.deepEqual(bus.deadLettered, ["expired-intent"]);
});

test("persistent service restarts before receiving if a channel gaps before first warmup", async () => {
  const bus = fakeBus([{
    messageId: "warmup-that-must-remain-queued",
    deliveryCount: 1,
    body: {
      schema: "polyedge.funded_market_warmup.v1",
      market_id: "btc-market",
      token_id: "token-up"
    }
  }]);
  const logs = [];
  await assert.rejects(runPersistentFundedDirectService({
    env: persistentEnv({ FUNDED_DIRECT_SERVICE_MAX_MESSAGES: "1" }),
    createBusClient: () => bus.client,
    createExecutor: async () => ({
      runMaintenance: async () => assert.fail("cold channel risk cannot be reconciled without a warmed market"),
      status: () => ({
        user_channel_ready: true,
        market_channel_ready: false,
        user_channel_gaps: 1,
        market_channel_gaps: 0,
        reconnect_reconciliation_required: true,
        warmed_market: null
      }),
      close: async () => {}
    }),
    createProcessor: async () => ({ process: async () => ({}) }),
    logger: (value) => logs.push(value)
  }), /websocket risk before first market warmup requires process restart/);

  assert.equal(bus.receiveCalls.length, 0);
  assert.ok(logs.some((value) =>
    value.status === "websocket_reconciliation_restart_required" &&
    value.account_risk_pause === true &&
    value.missed_signal_risk === true
  ));
});

test("persistent service recycles a receive link that outlives the one-second poll", async () => {
  const message = {
    messageId: "warmup-after-recycle",
    deliveryCount: 1,
    body: {
      schema: "polyedge.funded_market_warmup.v1",
      market_id: "btc-market",
      token_id: "token-up"
    }
  };
  const completed = [];
  const abandoned = [];
  const closed = [];
  const logs = [];
  let receiverCreations = 0;
  let stalledReceiveOptions;
  let stalledClosed = false;
  const receiverMethods = {
    async renewMessageLock() {},
    async deadLetterMessage() {},
    async abandonMessage() {}
  };
  const stalledReceiver = {
    ...receiverMethods,
    receiveMessages: async (_maxMessages, options) => {
      stalledReceiveOptions = options;
      return new Promise((resolve) => {
        setTimeout(() => resolve([{ messageId: "late-locked-intent" }]), 1_520);
      });
    },
    async abandonMessage(received) {
      assert.equal(stalledClosed, false);
      abandoned.push(received.messageId);
    },
    async completeMessage() {},
    async close() {
      stalledClosed = true;
      closed.push("stalled");
    }
  };
  const healthyReceiver = {
    ...receiverMethods,
    async receiveMessages() { return [message]; },
    async completeMessage(received) { completed.push(received.messageId); },
    async close() { closed.push("healthy"); }
  };
  const result = await runPersistentFundedDirectService({
    env: persistentEnv({
      FUNDED_DIRECT_POLL_INTERVAL_MS: "1000",
      FUNDED_DIRECT_SERVICE_MAX_MESSAGES: "1"
    }),
    createBusClient: () => ({
      createReceiver: () => receiverCreations++ === 0 ? stalledReceiver : healthyReceiver,
      async close() {}
    }),
    createExecutor: async () => ({
      warmMarket: async () => {},
      execute: async () => {},
      status: () => ({ ready: true }),
      close: async () => {}
    }),
    createProcessor: async () => ({ process: async () => ({}) }),
    logger: (value) => logs.push(value)
  });
  assert.equal(result.processed_messages, 1);
  assert.equal(receiverCreations, 2);
  assert.equal(stalledReceiveOptions.maxWaitTimeInMs, 1_000);
  assert.equal(stalledReceiveOptions.abortSignal.aborted, true);
  assert.deepEqual(completed, ["warmup-after-recycle"]);
  assert.deepEqual(abandoned, ["late-locked-intent"]);
  assert.deepEqual(closed.sort(), ["healthy", "stalled"]);
  assert.ok(logs.some((value) =>
    value.status === "service_bus_receive_watchdog_expired" &&
    value.watchdog_ms === 1_500 &&
    value.recovery_action === "new_client_receiver" &&
    value.missed_signal_risk === true
  ));
  assert.ok(logs.some((value) =>
    value.status === "service_bus_late_receive_abandoned" &&
    value.message_count === 1
  ));
});

test("persistent service recycles non-consecutive stalled receive links", async () => {
  const message = {
    messageId: "warmup-after-two-recycles",
    deliveryCount: 1,
    body: {
      schema: "polyedge.funded_market_warmup.v1",
      market_id: "btc-market",
      token_id: "token-up"
    }
  };
  const completed = [];
  const closed = [];
  const logs = [];
  let secondReceiveCalls = 0;
  let receiverCreations = 0;
  let terminateCalls = 0;
  const stalled = (id) => ({
    async receiveMessages() { return new Promise(() => {}); },
    async close() { closed.push(id); }
  });
  const receivers = [
    stalled(1),
    {
      async receiveMessages() {
        secondReceiveCalls += 1;
        return secondReceiveCalls === 1 ? [] : new Promise(() => {});
      },
      async close() { closed.push(2); }
    },
    {
      async receiveMessages() { return [message]; },
      async renewMessageLock() {},
      async completeMessage(received) { completed.push(received.messageId); },
      async close() { closed.push(3); }
    }
  ];
  const result = await runPersistentFundedDirectService({
    env: persistentEnv({
      FUNDED_DIRECT_POLL_INTERVAL_MS: "1000",
      FUNDED_DIRECT_SERVICE_MAX_MESSAGES: "1"
    }),
    createBusClient: () => ({
      createReceiver: () => receivers[receiverCreations++],
      async close() {}
    }),
    createExecutor: async () => ({
      warmMarket: async () => {},
      status: () => ({ ready: true }),
      close: async () => {}
    }),
    createProcessor: async () => ({ process: async () => ({}) }),
    logger: (value) => logs.push(value),
    terminate: () => { terminateCalls += 1; }
  });

  assert.equal(result.processed_messages, 1);
  assert.equal(receiverCreations, 3);
  assert.equal(terminateCalls, 0);
  assert.deepEqual(completed, ["warmup-after-two-recycles"]);
  assert.deepEqual(closed, [1, 2, 3]);
  assert.deepEqual(
    logs.filter((value) => value.status === "service_bus_receive_watchdog_expired")
      .map((value) => value.recovery_action),
    ["new_client_receiver", "new_client_receiver"]
  );
});

test("persistent service alerts if a locked message surfaces only after stale link closure", async () => {
  const warmup = {
    messageId: "warmup-after-late-cleanup",
    deliveryCount: 1,
    body: {
      schema: "polyedge.funded_market_warmup.v1",
      market_id: "btc-market",
      token_id: "token-up"
    }
  };
  let receiverCreations = 0;
  let staleClosed = false;
  let resolveLateAlert;
  const lateAlert = new Promise((resolve) => { resolveLateAlert = resolve; });
  const staleReceiver = {
    receiveMessages: async () => new Promise((resolve) => {
      setTimeout(() => resolve([{ messageId: "late-after-close" }]), 1_600);
    }),
    async abandonMessage() {
      if (staleClosed) throw new Error("receiver is closed");
    },
    async close() { staleClosed = true; }
  };
  const healthyReceiver = {
    async receiveMessages() { return [warmup]; },
    async completeMessage() {},
    async renewMessageLock() {},
    async close() {}
  };
  await runPersistentFundedDirectService({
    env: persistentEnv({
      FUNDED_DIRECT_POLL_INTERVAL_MS: "1000",
      FUNDED_DIRECT_SERVICE_MAX_MESSAGES: "1"
    }),
    createBusClient: () => ({
      createReceiver: () => receiverCreations++ === 0 ? staleReceiver : healthyReceiver,
      async close() {}
    }),
    createExecutor: async () => ({
      warmMarket: async () => {},
      status: () => ({ ready: true }),
      close: async () => {}
    }),
    createProcessor: async () => ({ process: async () => ({}) }),
    logger: (value) => {
      if (value.status === "service_bus_late_receive_cleanup_failed") resolveLateAlert(value);
    }
  });
  assert.deepEqual(await lateAlert, {
    schema: "polyedge.funded_direct_alert.v1",
    status: "service_bus_late_receive_cleanup_failed",
    missed_signal_risk: true,
    error: "receiver is closed"
  });
});

test("persistent service fails closed instead of recycling a second stalled receive link", async () => {
  const closed = [];
  let receiverCreations = 0;
  let resolveTermination;
  const termination = new Promise((resolve) => { resolveTermination = resolve; });
  const result = runPersistentFundedDirectService({
    env: persistentEnv({ FUNDED_DIRECT_POLL_INTERVAL_MS: "1000" }),
    createBusClient: () => ({
      createReceiver: () => {
        const id = ++receiverCreations;
        return {
          receiveMessages: async () => new Promise(() => {}),
          async close() { closed.push(id); }
        };
      },
      async close() {}
    }),
    createExecutor: async () => ({
      status: () => ({ ready: true }),
      close: async () => {}
    }),
    createProcessor: async () => ({ process: async () => ({}) }),
    logger: () => {},
    terminate: resolveTermination
  });
  await assert.rejects(result, /watchdog expired after receiver recycle/);
  assert.equal(await termination, 1);
  assert.equal(receiverCreations, 2);
  assert.deepEqual(closed.sort(), [1, 2]);
});

test("persistent service hard-stops when a stalled receive link cannot close", async () => {
  let receiverCreations = 0;
  let resolveTermination;
  const termination = new Promise((resolve) => { resolveTermination = resolve; });
  const result = runPersistentFundedDirectService({
    env: persistentEnv({ FUNDED_DIRECT_POLL_INTERVAL_MS: "1000" }),
    createBusClient: () => ({
      createReceiver: () => {
        receiverCreations += 1;
        return {
          receiveMessages: async () => new Promise(() => {}),
          async close() { return new Promise(() => {}); }
        };
      },
      async close() { return new Promise(() => {}); }
    }),
    createExecutor: async () => ({
      status: () => ({ ready: true }),
      close: async () => {}
    }),
    createProcessor: async () => ({ process: async () => ({}) }),
    logger: () => {},
    terminate: resolveTermination
  });
  await assert.rejects(result, /timed out closing stalled Service Bus receiver/);
  assert.equal(await termination, 1);
  assert.equal(receiverCreations, 1);
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
  assert.deepEqual(bus.renewed.sort(), ["intent-first", "warmup-duplicate", "warmup-first", "warmup-other-token"]);
});

test("persistent service leaves a fresh intent queued while the first child is active", async () => {
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
  let activeProcesses = 0;
  let maxActiveProcesses = 0;
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
        activeProcesses += 1;
        maxActiveProcesses = Math.max(maxActiveProcesses, activeProcesses);
        try {
          if (handoff.decision_id.startsWith("first")) {
            firstStarted();
            await new Promise((resolve) => { releaseFirst = resolve; });
          }
          return { execution: { lifecycle: { send_wall_ms: Date.parse(decisionTs) + 1 } } };
        } finally {
          activeProcesses -= 1;
        }
      },
      rejectBusy: async () => assert.fail("fresh intents must remain in Service Bus while a workflow is active")
    }),
    logger: (value) => logs.push(value)
  });
  await firstStartedPromise;
  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(bus.received, ["first"]);
  assert.deepEqual(bus.completed, []);
  assert.equal(maxActiveProcesses, 1);
  releaseFirst();
  const result = await resultPromise;
  assert.equal(result.processed_messages, 2, JSON.stringify(logs));
  assert.deepEqual(bus.received, ["first", "fresh"]);
  assert.deepEqual(bus.completed, ["first", "fresh"]);
  assert.deepEqual(bus.renewed.sort(), ["first", "fresh"]);
  assert.equal(processed.length, 2);
  assert.equal(maxActiveProcesses, 1);
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
          lifecycle: { send_wall_ms: Date.parse(handoff.decision_ts) + 3_500 }
        }
      }),
      rejectBusy: async () => ({ status: "one_workflow_busy" })
    }),
    logger: (value) => logs.push(value)
  });
  assert.equal(result.processed_messages, 3);
  assert.equal(result.failed_messages, 0);
  assert.ok(logs.some((value) => value.status === "engine_paused_by_consecutive_latency_breaches"));
  assert.deepEqual(bus.completed, ["decision-0", "decision-1", "decision-2"]);
  assert.deepEqual(bus.received, ["decision-0", "decision-1", "decision-2"]);
  assert.deepEqual(bus.deadLettered, []);
});
