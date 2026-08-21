import { DefaultAzureCredential } from "@azure/identity";
import { ServiceBusClient } from "@azure/service-bus";
import { pathToFileURL } from "node:url";
import { createPersistentCanaryExecutor } from "./canary.mjs";
import { runVenueRedemption } from "./redeem.mjs";
import {
  createFundedDirectProcessor,
  runFundedDirectWorker
} from "./funded-direct-worker.mjs";
import { sanitize } from "./lib.mjs";

const FUNDED_BTC_MARKET_INTERVAL_MS = 15 * 60 * 1_000;
const FUNDED_LOCK_RENEWAL_MS = 10_000;
const FUNDED_BUSY_VALIDATION_LIMIT = 4;

export function loadFundedDirectServiceConfig(env = process.env) {
  const config = {
    enabled: env.FUNDED_DIRECT_SERVICE_ENABLED === "true",
    restartDelayMs: integer(env.FUNDED_DIRECT_SERVICE_RESTART_DELAY_MS, 1_000),
    riskPauseMs: integer(env.FUNDED_DIRECT_SERVICE_RISK_PAUSE_MS, 60_000),
    heartbeatMs: integer(env.FUNDED_DIRECT_SERVICE_HEARTBEAT_MS, 60_000),
    maxCycles: integer(env.FUNDED_DIRECT_SERVICE_MAX_CYCLES, 0),
    pollIntervalMs: integer(env.FUNDED_DIRECT_POLL_INTERVAL_MS, 1_000),
    engine: String(env.FUNDED_DIRECT_ENGINE || "legacy_spawn").trim(),
    serviceBusNamespace: String(env.FUNDED_DIRECT_SERVICE_BUS_NAMESPACE || "").trim(),
    serviceBusQueue: String(env.FUNDED_DIRECT_SERVICE_BUS_QUEUE || "").trim(),
    signalToSendSloMs: integer(env.FUNDED_DIRECT_SIGNAL_TO_SEND_SLO_MS, 7_000),
    maxMessages: integer(env.FUNDED_DIRECT_SERVICE_MAX_MESSAGES, 0),
    autoRedemptionEnabled: env.FUNDED_DIRECT_AUTO_REDEMPTION_ENABLED === "true",
    autoRedemptionIntervalMs: integer(env.FUNDED_DIRECT_AUTO_REDEMPTION_INTERVAL_MS, 60_000),
    autoRedemptionMinSecondsToExpiry: integer(env.FUNDED_DIRECT_AUTO_REDEMPTION_MIN_SECONDS_TO_EXPIRY, 30),
    autoRedemptionMaxSecondsToExpiry: integer(env.FUNDED_DIRECT_AUTO_REDEMPTION_MAX_SECONDS_TO_EXPIRY, 300)
  };
  const errors = [];
  if (!config.enabled) errors.push("FUNDED_DIRECT_SERVICE_ENABLED must be true");
  if (!(config.restartDelayMs >= 1_000 && config.restartDelayMs <= 60_000)) {
    errors.push("FUNDED_DIRECT_SERVICE_RESTART_DELAY_MS must be in [1000, 60000]");
  }
  if (!(config.riskPauseMs >= 1_000 && config.riskPauseMs <= 900_000)) {
    errors.push("FUNDED_DIRECT_SERVICE_RISK_PAUSE_MS must be in [1000, 900000]");
  }
  if (!(config.heartbeatMs >= 10_000 && config.heartbeatMs <= 600_000)) {
    errors.push("FUNDED_DIRECT_SERVICE_HEARTBEAT_MS must be in [10000, 600000]");
  }
  if (!(config.maxCycles >= 0 && config.maxCycles <= 10_000)) {
    errors.push("FUNDED_DIRECT_SERVICE_MAX_CYCLES must be in [0, 10000]");
  }
  if (!(config.pollIntervalMs >= 1_000 && config.pollIntervalMs <= 60_000)) {
    errors.push("FUNDED_DIRECT_POLL_INTERVAL_MS must be in [1000, 60000]");
  }
  if (!["legacy_spawn", "persistent_v1"].includes(config.engine)) {
    errors.push("FUNDED_DIRECT_ENGINE must equal legacy_spawn or persistent_v1");
  }
  if (config.engine === "persistent_v1" && (!config.serviceBusNamespace || !config.serviceBusQueue)) {
    errors.push("persistent_v1 requires the exact Service Bus namespace and queue");
  }
  if (!(config.signalToSendSloMs >= 500 && config.signalToSendSloMs <= 7_000)) {
    errors.push("FUNDED_DIRECT_SIGNAL_TO_SEND_SLO_MS must be in [500, 7000]");
  }
  if (!(config.maxMessages >= 0 && config.maxMessages <= 10_000)) {
    errors.push("FUNDED_DIRECT_SERVICE_MAX_MESSAGES must be in [0, 10000]");
  }
  if (config.autoRedemptionEnabled) {
    let session = null;
    try {
      session = JSON.parse(String(env.FUNDED_DIRECT_SESSION_MANIFEST_JSON || ""));
    } catch {}
    if (config.engine !== "persistent_v1") {
      errors.push("automatic redemption requires FUNDED_DIRECT_ENGINE=persistent_v1");
    }
    if (!(config.autoRedemptionIntervalMs >= 30_000 && config.autoRedemptionIntervalMs <= 600_000)) {
      errors.push("FUNDED_DIRECT_AUTO_REDEMPTION_INTERVAL_MS must be in [30000, 600000]");
    }
    if (!(config.autoRedemptionMinSecondsToExpiry >= 10 &&
        config.autoRedemptionMaxSecondsToExpiry <= 300 &&
        config.autoRedemptionMinSecondsToExpiry < config.autoRedemptionMaxSecondsToExpiry)) {
      errors.push("automatic redemption window must remain within the configured minimum and 300 seconds of the final 360 seconds");
    }
    if (!env.POLYMARKET_RELAYER_API_KEY) {
      errors.push("POLYMARKET_RELAYER_API_KEY is required for automatic redemption");
    }
    if (!/^0x[0-9a-fA-F]{40}$/.test(String(env.POLYMARKET_RELAYER_API_KEY_ADDRESS || ""))) {
      errors.push("POLYMARKET_RELAYER_API_KEY_ADDRESS must be a valid address");
    }
    if (!session || typeof session !== "object" ||
        session.session_id !== env.VENUE_PROBE_FUNDED_CAMPAIGN_ID ||
        session.allow_compounding !== true ||
        session.no_deposits !== true ||
        session.allow_automatic_replenishment !== false) {
      errors.push("automatic redemption requires the matching protected-compounding operator session");
    }
  }
  if (errors.length) throw new Error(`funded_direct_service blocked: ${errors.join("; ")}`);
  return config;
}

export async function runFundedDirectService({
  env = process.env,
  runWorker = runFundedDirectWorker,
  sleep = delay,
  logger = (value) => console.log(JSON.stringify(sanitize(value)))
} = {}) {
  const config = loadFundedDirectServiceConfig(env);
  if (config.engine === "persistent_v1") {
    return runPersistentFundedDirectService({ env, logger });
  }
  let cycles = 0;
  let workerFailures = 0;
  logger({
    schema: "polyedge.funded_direct_service.v1",
    status: "continuous_service_started",
    poll_interval_ms: config.pollIntervalMs,
    cloud_only: true
  });

  while (true) {
    const heartbeat = setInterval(() => logger({
      schema: "polyedge.funded_direct_service.v1",
      status: "continuous_service_heartbeat",
      cycles,
      worker_failures: workerFailures
    }), config.heartbeatMs);
    heartbeat.unref?.();

    let result;
    try {
      result = await runWorker({ env });
      logger({
        schema: "polyedge.funded_direct_service.v1",
        status: "worker_cycle_completed",
        worker: result
      });
    } catch (error) {
      workerFailures += 1;
      logger({
        schema: "polyedge.funded_direct_service.v1",
        status: "worker_cycle_failed_closed",
        error: error.message
      });
    } finally {
      clearInterval(heartbeat);
    }

    cycles += 1;
    if (config.maxCycles > 0 && cycles >= config.maxCycles) {
      return {
        schema: "polyedge.funded_direct_service.v1",
        status: "bounded_test_complete",
        cycles,
        worker_failures: workerFailures
      };
    }
    const pauseMs = result?.status === "paused_by_account_risk_state"
      ? config.riskPauseMs
      : config.restartDelayMs;
    await sleep(pauseMs);
  }
}

export async function runPersistentFundedDirectService({
  env = process.env,
  logger = (value) => console.log(JSON.stringify(sanitize(value))),
  createExecutor = createPersistentCanaryExecutor,
  createProcessor = createFundedDirectProcessor,
  runRedemption = runVenueRedemption,
  now = Date.now,
  sleep = delay,
  createBusClient = ({ namespace, credential }) =>
    new ServiceBusClient(`${namespace}.servicebus.windows.net`, credential)
} = {}) {
  const config = loadFundedDirectServiceConfig(env);
  if (config.engine !== "persistent_v1") throw new Error("persistent funded service requires FUNDED_DIRECT_ENGINE=persistent_v1");
  const credential = new DefaultAzureCredential({ managedIdentityClientId: env.AZURE_CLIENT_ID });
  const bus = createBusClient({ namespace: config.serviceBusNamespace, credential });
  const receiver = bus.createReceiver(config.serviceBusQueue, { receiveMode: "peekLock" });
  let executor = null;
  let leaseHandoffAttempts = 0;
  while (!executor) {
    try {
      executor = await createExecutor({ env: persistentCanaryBootstrapEnv(env) });
    } catch (error) {
      if (!isCampaignLeaseHandoffConflict(error)) throw error;
      leaseHandoffAttempts += 1;
      logger({
        schema: "polyedge.funded_direct_service.v2",
        status: "awaiting_campaign_lease_handoff",
        attempt: leaseHandoffAttempts,
        retry_delay_ms: config.restartDelayMs
      });
      await sleep(config.restartDelayMs);
    }
  }
  const processor = await createProcessor({
    env,
    executeCanary: (childEnv) => executor.execute(childEnv)
  });
  let processedMessages = 0;
  let failedMessages = 0;
  let consecutiveLatencyBreaches = 0;
  let redemptionChecks = 0;
  let redemptionResults = 0;
  let redemptionFailures = 0;
  let lastRedemptionStatus = null;
  let lastRedemptionCheckMs = 0;
  const signalToSendSamples = [];
  let stopping = false;
  const stop = () => { stopping = true; };
  process.once("SIGTERM", stop);
  process.once("SIGINT", stop);
  logger({
    schema: "polyedge.funded_direct_service.v2",
    status: "persistent_service_started",
    engine: config.engine,
    handoff: "azure_service_bus_peek_lock",
    queue: config.serviceBusQueue,
    poll_interval_ms: config.pollIntervalMs,
    signal_to_send_slo_ms: config.signalToSendSloMs,
    automatic_redemption_enabled: config.autoRedemptionEnabled,
    automatic_redemption_window_seconds_to_expiry: config.autoRedemptionEnabled
      ? {
          minimum: config.autoRedemptionMinSecondsToExpiry,
          maximum: config.autoRedemptionMaxSecondsToExpiry
        }
      : null,
    cloud_only: true,
    executor: executor.status()
  });
  const runAutomaticRedemption = async (window) => {
    try {
      const result = await executor.runMaintenance(({ lease: inheritedLease }) =>
        runRedemption({
          env: fundedRedemptionEnv(env),
          inheritedLease,
          logger: (value) => logger({
            schema: "polyedge.funded_redemption_service.v1",
            status: "redemption_worker_summary",
            redemption: value
          })
        })
      );
      redemptionResults += 1;
      lastRedemptionStatus = result?.status || "unknown";
      logger({
        schema: "polyedge.funded_redemption_service.v1",
        status: "automatic_redemption_cycle_completed",
        market_id: window.market_id,
        market_end_ts: window.market_end_ts,
        clock_source: window.clock_source,
        remaining_seconds: window.remaining_seconds,
        redemption_status: lastRedemptionStatus,
        redemption_submitted: result?.redemption_submitted === true
      });
    } catch (error) {
      redemptionFailures += 1;
      lastRedemptionStatus = "failed_closed";
      logger({
        schema: "polyedge.funded_direct_alert.v1",
        status: "automatic_redemption_failed_closed",
        market_id: window.market_id,
        market_end_ts: window.market_end_ts,
        clock_source: window.clock_source,
        remaining_seconds: window.remaining_seconds,
        account_risk_pause: true,
        error: error.message
      });
    }
  };
  const heartbeat = setInterval(() => {
    const executorStatus = executor.status();
    logger({
      schema: "polyedge.funded_direct_service.v2",
      status: "persistent_service_heartbeat",
      processed_messages: processedMessages,
      failed_messages: failedMessages,
      consecutive_latency_breaches: consecutiveLatencyBreaches,
      redemption_checks: redemptionChecks,
      redemption_results: redemptionResults,
      redemption_failures: redemptionFailures,
      last_redemption_status: lastRedemptionStatus,
      executor: executorStatus
    });
    if (executorStatus.reconnect_reconciliation_required ||
        executorStatus.user_channel_gaps > 0 ||
        executorStatus.market_channel_gaps > 0) {
      logger({
        schema: "polyedge.funded_direct_alert.v1",
        status: "websocket_gap_or_reconciliation_required",
        account_risk_pause: true,
        executor: executorStatus
      });
    }
  }, config.heartbeatMs);
  heartbeat.unref?.();
  const warmedMarketTokens = new Set();
  const startLockRenewal = (message) => {
    const entry = {
      message,
      body: null,
      renewalError: null,
      renewal: null,
      queueReceiveMonotonicMs: performance.now()
    };
    const renew = async () => {
      try {
        await receiver.renewMessageLock(message);
      } catch (error) {
        entry.renewalError = error;
      }
    };
    void renew();
    entry.renewal = setInterval(renew, FUNDED_LOCK_RENEWAL_MS);
    entry.renewal.unref?.();
    return entry;
  };
  const settleStoppedMessage = async (entry) => {
    clearInterval(entry.renewal);
    failedMessages += 1;
    await receiver.deadLetterMessage(entry.message, {
      deadLetterReason: "FundedServiceStopping",
      deadLetterErrorDescription: "Funded service stopped before this locked message could be processed."
    }).catch(() => receiver.abandonMessage(entry.message).catch(() => null));
    logger({
      schema: "polyedge.funded_direct_service.v2",
      status: "persistent_message_stopped_fail_closed",
      message_id: entry.message.messageId || null,
      delivery_count: entry.message.deliveryCount || 0
    });
  };
  const failMessage = async (entry, error) => {
    failedMessages += 1;
    const errorText = safeErrorMessage(error);
    const deterministic = /unsupported|schema|binding|TTL|stale|SHA-256|does not qualify|already has an authorization|latency|risk|reservation|cash.flow|equity|position|open order/i.test(errorText);
    if (deterministic || Number(entry.message.deliveryCount || 0) >= 2) {
      await receiver.deadLetterMessage(entry.message, {
        deadLetterReason: "FundedIntentFailedClosed",
        deadLetterErrorDescription: errorText.slice(0, 4_000)
      }).catch(() => null);
    } else {
      await receiver.abandonMessage(entry.message).catch(() => null);
    }
    logger({
      schema: "polyedge.funded_direct_service.v2",
      status: "persistent_message_failed_closed",
      message_id: entry.message.messageId || null,
      delivery_count: entry.message.deliveryCount || 0,
      error: errorText,
      error_detail: safeErrorProjection(error)
    });
  };
  const recordDurableSettlementLoss = (entry, result, error) => {
    logger({
      schema: "polyedge.funded_direct_service.v2",
      status: "persistent_message_settlement_lost_after_durable_completion",
      message_id: entry.message.messageId || null,
      delivery_count: entry.message.deliveryCount || 0,
      worker_status: result?.status || null,
      order_submission_attempted: result?.execution?.order_submission_attempted === true ||
        result?.completion?.order_submission_attempted === true,
      broker_redelivery_expected: true,
      error: safeErrorMessage(error),
      error_detail: safeErrorProjection(error)
    });
  };
  const processIntent = async (entry, busy) => {
    const { message, body, queueReceiveMonotonicMs } = entry;
    try {
      if (entry.renewalError) {
        await failMessage(entry, entry.renewalError);
        return;
      }
      const receivedWallMs = Date.now();
      let result;
      try {
        result = busy ? await processor.rejectBusy(body) : await processor.process(body);
      } catch (error) {
        await failMessage(entry, error);
        return;
      }
      try {
        if (entry.renewalError) throw entry.renewalError;
        await receiver.completeMessage(message);
      } catch (error) {
        if (hasDurableCompletion(result)) {
          recordDurableSettlementLoss(entry, result, error);
          return;
        }
        await failMessage(entry, error);
        return;
      }
      processedMessages += 1;
      if (busy) {
        logger({
          schema: "polyedge.funded_direct_service.v2",
          status: "one_workflow_busy",
          message_id: message.messageId || null,
          decision_id: body.decision_id,
          order_submission_attempted: false,
          worker_status: result?.status || null
        });
        return;
      }
      const execution = result?.execution;
      const lifecycle = execution?.lifecycle;
      const sendWallMs = Number(lifecycle?.send_wall_ms);
      const ackWallMs = Number(lifecycle?.ack_wall_ms);
      const lifecycleAcknowledged = execution?.order_submission_attempted === true &&
        typeof lifecycle?.order_id === "string" && lifecycle.order_id.trim() !== "" &&
        Number.isFinite(sendWallMs) && sendWallMs > 0 &&
        Number.isFinite(ackWallMs) && ackWallMs > 0 && ackWallMs >= sendWallMs;
      const terminalNoFillSubmitted = execution?.status === "terminal_no_fill_evidence_degraded" &&
        execution?.order_submitted === true && execution?.order_submission_attempted === true &&
        typeof lifecycle?.order_id === "string" && lifecycle.order_id.trim() !== "" &&
        lifecycle?.reconciliation_complete === true &&
        lifecycle?.zero_open_orders_confirmed === true && lifecycle?.matched_notional === 0;
      const decisionWallMs = Date.parse(body.decision_ts);
      const signalToSendMs = Number.isFinite(sendWallMs) && Number.isFinite(decisionWallMs)
        ? Math.max(0, sendWallMs - decisionWallMs) : null;
      const queueToReceiveMs = Number.isFinite(decisionWallMs)
        ? Math.max(0, receivedWallMs - decisionWallMs) : null;
      const breached = signalToSendMs !== null && signalToSendMs > config.signalToSendSloMs;
      consecutiveLatencyBreaches = breached ? consecutiveLatencyBreaches + 1 : 0;
      if (signalToSendMs !== null) {
        signalToSendSamples.push(signalToSendMs);
        if (signalToSendSamples.length > 100) signalToSendSamples.shift();
      }
      const rollingP95Ms = percentile(signalToSendSamples, 0.95);
      logger({ schema: "polyedge.funded_direct_latency.v1", status: breached ? "signal_to_send_slo_breached" : "intent_completed",
        message_id: message.messageId || null, decision_id: body.decision_id, queue_to_receive_ms: queueToReceiveMs,
        queue_receive_wall_ms: receivedWallMs, queue_receive_monotonic_ms: queueReceiveMonotonicMs,
        signal_to_send_ms: signalToSendMs, signal_to_send_slo_ms: config.signalToSendSloMs,
        rolling_p95_signal_to_send_ms: rollingP95Ms,
        rolling_p95_slo_breached: rollingP95Ms !== null && rollingP95Ms > config.signalToSendSloMs,
        consecutive_latency_breaches: consecutiveLatencyBreaches,
        order_submission_attempted: result?.execution?.order_submission_attempted === true || result?.completion?.order_submission_attempted === true,
        order_submitted: execution?.order_submitted !== false &&
          (lifecycleAcknowledged || terminalNoFillSubmitted),
        worker_status: result?.status || null, worker_error: result?.error || null, execution_timing: result?.execution_timing || null });
      if (result?.status === "paused_by_account_risk_state") logger({ schema: "polyedge.funded_direct_alert.v1", status: "paused_by_account_risk_state", decision_id: body.decision_id, account_risk_pause: true, error: result.error || null });
      if (consecutiveLatencyBreaches >= 3) {
        stopping = true;
        logger({ schema: "polyedge.funded_direct_alert.v1", status: "engine_paused_by_consecutive_latency_breaches", decision_id: body.decision_id, consecutive_transitions_above_slo: consecutiveLatencyBreaches, signal_to_send_slo_ms: config.signalToSendSloMs, account_risk_pause: true });
      }
    } finally {
      clearInterval(entry.renewal);
    }
  };
  const processWarmup = async (entry) => {
    const { message, body } = entry;
    const key = body.market_id && body.token_id ? `${body.market_id}:${body.token_id}` : null;
    try {
      if (entry.renewalError) throw entry.renewalError;
      if (key && warmedMarketTokens.has(key)) {
        await receiver.completeMessage(message);
        processedMessages += 1;
        logger({ schema: "polyedge.funded_direct_service.v2", status: "market_warmup_coalesced", message_id: message.messageId || null, market_id: body.market_id, token_id: body.token_id });
        return;
      }
      await executor.warmMarket(body);
      if (entry.renewalError) throw entry.renewalError;
      await receiver.completeMessage(message);
      processedMessages += 1;
      if (key) warmedMarketTokens.add(key);
      logger({ schema: "polyedge.funded_direct_service.v2", status: "market_warmed", message_id: message.messageId || null, market_id: body.market_id, token_id: body.token_id });
    } catch (error) {
      await failMessage(entry, error);
    } finally {
      clearInterval(entry.renewal);
    }
  };
  let activeWorkflow = null;
  let activeWorkflowKind = null;
  const trackActiveWorkflow = (task, kind) => {
    activeWorkflow = task;
    activeWorkflowKind = kind;
    const clear = () => {
      if (activeWorkflow === task) {
        activeWorkflow = null;
        activeWorkflowKind = null;
      }
    };
    void task.then(clear, clear);
  };
  const maybeStartAutomaticRedemption = () => {
    if (!config.autoRedemptionEnabled || activeWorkflow) return;
    const checkedAt = now();
    if (checkedAt - lastRedemptionCheckMs < config.autoRedemptionIntervalMs) return;
    lastRedemptionCheckMs = checkedAt;
    redemptionChecks += 1;
    const window = fundedRedemptionMaintenanceWindow(executor.status(), checkedAt, config);
    if (window.eligible) trackActiveWorkflow(runAutomaticRedemption(window), "maintenance");
  };
  const busyValidations = new Set();
  const trackBusyValidation = (entry) => {
    const task = processIntent(entry, true);
    busyValidations.add(task);
    const clear = () => busyValidations.delete(task);
    void task.then(clear, clear);
  };
  try {
    while (!stopping) {
      if (activeWorkflow) await new Promise((resolve) => setImmediate(resolve));
      if (config.maxMessages > 0 && processedMessages + failedMessages >= config.maxMessages) {
        stopping = true;
        break;
      }
      if (busyValidations.size >= FUNDED_BUSY_VALIDATION_LIMIT ||
          (config.maxMessages > 0 && processedMessages + failedMessages + busyValidations.size + (activeWorkflowKind === "intent" ? 1 : 0) >= config.maxMessages)) {
        await Promise.race([activeWorkflow, ...busyValidations].filter(Boolean));
        continue;
      }
      const messages = await receiver.receiveMessages(1, { maxWaitTimeInMs: config.pollIntervalMs });
      if (!messages.length) {
        maybeStartAutomaticRedemption();
        await new Promise((resolve) => setImmediate(resolve));
        continue;
      }
      const message = messages[0];
      const entry = startLockRenewal(message);
      try {
        entry.body = parseMessageBody(message.body);
      } catch (error) {
        entry.renewalError = error;
      }
      const { body } = entry;
      if (stopping) {
        await settleStoppedMessage(entry);
        continue;
      }
      if (body?.schema === "polyedge.funded_intent_handoff.v1") {
        if (activeWorkflow) {
          trackBusyValidation(entry);
        } else {
          trackActiveWorkflow(processIntent(entry, false), "intent");
        }
        continue;
      }
      if (body?.schema === "polyedge.funded_market_warmup.v1") {
        if (activeWorkflow) {
          const key = body.market_id && body.token_id ? `${body.market_id}:${body.token_id}` : null;
          if (activeWorkflowKind === "maintenance" && key && warmedMarketTokens.has(key)) {
            await processWarmup(entry);
          } else {
            logger({ schema: "polyedge.funded_direct_service.v2", status: "market_warmup_waiting", message_id: message.messageId || null, market_id: body.market_id, token_id: body.token_id });
            await Promise.allSettled([activeWorkflow]);
            if (stopping) await settleStoppedMessage(entry);
            else await processWarmup(entry);
          }
        } else {
          await processWarmup(entry);
          maybeStartAutomaticRedemption();
        }
        continue;
      }
      await failMessage(entry, entry.renewalError || new Error("fail closed: unsupported funded intent handoff schema"));
      clearInterval(entry.renewal);
      continue;
    }
    await Promise.allSettled([activeWorkflow, ...busyValidations].filter(Boolean));
    return {
      schema: "polyedge.funded_direct_service.v2",
      status: "persistent_service_stopped",
      processed_messages: processedMessages,
      failed_messages: failedMessages,
      redemption_checks: redemptionChecks,
      redemption_results: redemptionResults,
      redemption_failures: redemptionFailures,
      last_redemption_status: lastRedemptionStatus
    };
  } finally {
    stopping = true;
    await Promise.allSettled([activeWorkflow, ...busyValidations].filter(Boolean));
    clearInterval(heartbeat);
    process.removeListener("SIGTERM", stop);
    process.removeListener("SIGINT", stop);
    await executor.close().catch(() => null);
    await receiver.close().catch(() => null);
    await bus.close().catch(() => null);
  }
}

function isCampaignLeaseHandoffConflict(error) {
  return /^fail closed: another venue probe owns the campaign lease \((409|412)\)$/.test(
    String(error?.message || "")
  );
}

export function fundedRedemptionMaintenanceWindow(executorStatus, nowMs, config) {
  const market = executorStatus?.warmed_market;
  const checkedAtMs = Number(nowMs);
  const endMs = Number.isFinite(checkedAtMs)
    ? (Math.floor(checkedAtMs / FUNDED_BTC_MARKET_INTERVAL_MS) + 1) *
      FUNDED_BTC_MARKET_INTERVAL_MS
    : Number.NaN;
  const warmedEndMs = Date.parse(String(market?.market_end_ts || ""));
  const remainingSeconds = (endMs - checkedAtMs) / 1_000;
  const eligible = Number.isFinite(checkedAtMs) &&
    Number.isFinite(endMs) &&
    Number.isFinite(remainingSeconds) &&
    remainingSeconds >= config.autoRedemptionMinSecondsToExpiry &&
    remainingSeconds <= config.autoRedemptionMaxSecondsToExpiry;
  return {
    eligible,
    market_id: warmedEndMs === endMs ? market?.market_id || null : null,
    market_end_ts: Number.isFinite(endMs) ? new Date(endMs).toISOString() : null,
    clock_source: "btc_15m_utc_boundary",
    remaining_seconds: Number.isFinite(remainingSeconds)
      ? Math.round(remainingSeconds * 1_000) / 1_000
      : null
  };
}

function fundedRedemptionEnv(env) {
  const session = JSON.parse(String(env.FUNDED_DIRECT_SESSION_MANIFEST_JSON));
  const { VENUE_REDEMPTION_MAX_PAYOUT: _obsoletePayoutCap, ...uncappedEnv } = env;
  return {
    ...uncappedEnv,
    EXECUTION_MODE: "venue_redemption",
    VENUE_REDEMPTION_ENABLED: "true",
    VENUE_REDEMPTION_DRY_RUN: "false",
    VENUE_REDEMPTION_MAX_CONDITIONS: "1",
    VENUE_PROBE_STARTING_CAPITAL: String(session.starting_collateral),
    FUNDED_EVIDENCE_TRUST_BOUNDARY_READY: "true",
    ALLOW_LIVE: "false",
    ENABLE_TAKER_ORDERS: "false"
  };
}

export function persistentCanaryBootstrapEnv(env) {
  const session = JSON.parse(String(env.FUNDED_DIRECT_SESSION_MANIFEST_JSON || "{}"));
  const placeholderHash = `sha256:${"0".repeat(64)}`;
  return {
    ...env,
    EXECUTION_MODE: "funded_direct",
    ALLOW_LIVE: "false",
    ALLOW_STRATEGY_CANARY: "false",
    ALLOW_FUNDED_DIRECT: "true",
    ENABLE_TAKER_ORDERS: "false",
    FUNDED_EVIDENCE_TRUST_BOUNDARY_READY: "false",
    STRATEGY_CANARY_DRY_RUN: "false",
    STRATEGY_CANARY_RUN_ID: "funded-direct-persistent-bootstrap",
    STRATEGY_CANARY_INTENT_BLOB_NAME: "bootstrap/intent.json",
    STRATEGY_CANARY_INTENT_SHA256: placeholderHash,
    STRATEGY_CANARY_PROMOTION_MANIFEST_BLOB_NAME: env.FUNDED_DIRECT_SESSION_MANIFEST_BLOB_NAME,
    STRATEGY_CANARY_PROMOTION_MANIFEST_SHA256: env.FUNDED_DIRECT_SESSION_MANIFEST_SHA256,
    STRATEGY_CANARY_AUTHORIZATION_BLOB_NAME: "bootstrap/authorization.json",
    STRATEGY_CANARY_AUTHORIZATION_SHA256: placeholderHash,
    STRATEGY_CANARY_EXECUTION_MODEL_BLOB_URI: session.execution_model?.blob_uri,
    STRATEGY_CANARY_EXECUTION_MODEL_SHA256: session.execution_model?.sha256,
    STRATEGY_CANARY_REQUIRED_FILL_MODEL_VERSION: session.execution_model?.model_version,
    STRATEGY_CANARY_MAX_ORDER_NOTIONAL: String(session.max_order_notional),
    STRATEGY_CANARY_MIN_REMAINING_TTL_MS: String(env.FUNDED_DIRECT_CHILD_MIN_REMAINING_TTL_MS || "2000"),
    VENUE_PROBE_CAMPAIGN_CASH_FLOWS: "[]"
  };
}

function parseMessageBody(value) {
  if (value && typeof value === "object" && !Buffer.isBuffer(value)) return value;
  const text = Buffer.isBuffer(value) ? value.toString("utf8") : String(value || "");
  try { return JSON.parse(text); }
  catch { throw new Error("fail closed: Service Bus message body is not valid JSON"); }
}

function hasDurableCompletion(result) {
  return result?.completion?.schema === "polyedge.operator_funded_intent_completion.v1" ||
    [
      "already_completed_idempotent",
      "child_failed_closed_pre_submission",
      "expired_before_child_launch",
      "one_workflow_busy",
      "paused_by_account_risk_state",
      "persistent_intent_completed"
    ].includes(result?.status);
}

function safeErrorMessage(error) {
  for (const value of [error?.message, error?.reason, error?.code, error?.name]) {
    if (typeof value === "string" && value.trim()) return value.trim().slice(0, 4_000);
  }
  return "unknown error";
}

function safeErrorProjection(error) {
  const text = (value) => typeof value === "string" && value.trim()
    ? value.trim().slice(0, 4_000)
    : null;
  return {
    name: text(error?.name),
    code: text(error?.code),
    reason: text(error?.reason),
    message: text(error?.message),
    status_code: Number.isFinite(Number(error?.statusCode)) ? Number(error.statusCode) : null
  };
}

function integer(value, fallback) {
  const parsed = Number(value);
  return Number.isInteger(parsed) ? parsed : fallback;
}

function percentile(values, quantile) {
  if (!values.length) return null;
  const sorted = [...values].sort((left, right) => left - right);
  return sorted[Math.min(sorted.length - 1, Math.ceil(sorted.length * quantile) - 1)];
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

const isMain = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isMain) {
  runFundedDirectService().catch((error) => {
    process.exitCode = 1;
    console.error(JSON.stringify(sanitize({
      schema: "polyedge.funded_direct_service.v1",
      status: "failed_closed",
      error: error.message
    })));
  });
}
