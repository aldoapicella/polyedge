import { DefaultAzureCredential } from "@azure/identity";
import { ServiceBusClient } from "@azure/service-bus";
import { pathToFileURL } from "node:url";
import { createPersistentCanaryExecutor } from "./canary.mjs";
import {
  createFundedDirectProcessor,
  runFundedDirectWorker
} from "./funded-direct-worker.mjs";
import { sanitize } from "./lib.mjs";

export function loadFundedDirectServiceConfig(env = process.env) {
  const config = {
    enabled: env.FUNDED_DIRECT_SERVICE_ENABLED === "true",
    restartDelayMs: integer(env.FUNDED_DIRECT_SERVICE_RESTART_DELAY_MS, 1_000),
    riskPauseMs: integer(env.FUNDED_DIRECT_SERVICE_RISK_PAUSE_MS, 60_000),
    heartbeatMs: integer(env.FUNDED_DIRECT_SERVICE_HEARTBEAT_MS, 60_000),
    maxCycles: integer(env.FUNDED_DIRECT_SERVICE_MAX_CYCLES, 0),
    engine: String(env.FUNDED_DIRECT_ENGINE || "legacy_spawn").trim(),
    serviceBusNamespace: String(env.FUNDED_DIRECT_SERVICE_BUS_NAMESPACE || "").trim(),
    serviceBusQueue: String(env.FUNDED_DIRECT_SERVICE_BUS_QUEUE || "").trim(),
    signalToSendSloMs: integer(env.FUNDED_DIRECT_SIGNAL_TO_SEND_SLO_MS, 2_000),
    maxMessages: integer(env.FUNDED_DIRECT_SERVICE_MAX_MESSAGES, 0)
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
  if (!["legacy_spawn", "persistent_v1"].includes(config.engine)) {
    errors.push("FUNDED_DIRECT_ENGINE must equal legacy_spawn or persistent_v1");
  }
  if (config.engine === "persistent_v1" && (!config.serviceBusNamespace || !config.serviceBusQueue)) {
    errors.push("persistent_v1 requires the exact Service Bus namespace and queue");
  }
  if (!(config.signalToSendSloMs >= 500 && config.signalToSendSloMs <= 10_000)) {
    errors.push("FUNDED_DIRECT_SIGNAL_TO_SEND_SLO_MS must be in [500, 10000]");
  }
  if (!(config.maxMessages >= 0 && config.maxMessages <= 10_000)) {
    errors.push("FUNDED_DIRECT_SERVICE_MAX_MESSAGES must be in [0, 10000]");
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
    poll_interval_ms: Number(env.FUNDED_DIRECT_POLL_INTERVAL_MS),
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
  createBusClient = ({ namespace, credential }) =>
    new ServiceBusClient(`${namespace}.servicebus.windows.net`, credential)
} = {}) {
  const config = loadFundedDirectServiceConfig(env);
  if (config.engine !== "persistent_v1") throw new Error("persistent funded service requires FUNDED_DIRECT_ENGINE=persistent_v1");
  const credential = new DefaultAzureCredential({ managedIdentityClientId: env.AZURE_CLIENT_ID });
  const bus = createBusClient({ namespace: config.serviceBusNamespace, credential });
  const receiver = bus.createReceiver(config.serviceBusQueue, { receiveMode: "peekLock" });
  const executor = await createExecutor({ env: persistentCanaryBootstrapEnv(env) });
  const processor = await createProcessor({
    env,
    executeCanary: (childEnv) => executor.execute(childEnv)
  });
  let processedMessages = 0;
  let failedMessages = 0;
  let consecutiveLatencyBreaches = 0;
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
    signal_to_send_slo_ms: config.signalToSendSloMs,
    cloud_only: true,
    executor: executor.status()
  });
  const heartbeat = setInterval(() => {
    const executorStatus = executor.status();
    logger({
      schema: "polyedge.funded_direct_service.v2",
      status: "persistent_service_heartbeat",
      processed_messages: processedMessages,
      failed_messages: failedMessages,
      consecutive_latency_breaches: consecutiveLatencyBreaches,
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
  try {
    while (!stopping) {
      const messages = await receiver.receiveMessages(1, { maxWaitTimeInMs: 10_000 });
      if (!messages.length) continue;
      const message = messages[0];
      const queueReceiveMonotonicMs = performance.now();
      let renewalError = null;
      const renewal = setInterval(async () => {
        try {
          await receiver.renewMessageLock(message);
        } catch (error) {
          renewalError = error;
        }
      }, 20_000);
      renewal.unref?.();
      try {
        const body = parseMessageBody(message.body);
        if (body?.schema === "polyedge.funded_market_warmup.v1") {
          await executor.warmMarket(body);
          if (renewalError) throw renewalError;
          await receiver.completeMessage(message);
          processedMessages += 1;
          logger({
            schema: "polyedge.funded_direct_service.v2",
            status: "market_warmed",
            message_id: message.messageId || null,
            market_id: body.market_id,
            token_id: body.token_id
          });
        } else if (body?.schema === "polyedge.funded_intent_handoff.v1") {
          const receivedWallMs = Date.now();
          const result = await processor.process(body);
          if (renewalError) throw renewalError;
          await receiver.completeMessage(message);
          processedMessages += 1;
          const sendWallMs = Number(result?.execution?.lifecycle?.send_wall_ms);
          const decisionWallMs = Date.parse(body.decision_ts);
          const signalToSendMs = Number.isFinite(sendWallMs) && Number.isFinite(decisionWallMs)
            ? Math.max(0, sendWallMs - decisionWallMs)
            : null;
          const queueToReceiveMs = Number.isFinite(decisionWallMs)
            ? Math.max(0, receivedWallMs - decisionWallMs)
            : null;
          const breached = signalToSendMs !== null && signalToSendMs > config.signalToSendSloMs;
          const severeBreach = signalToSendMs !== null && signalToSendMs > 3_000;
          consecutiveLatencyBreaches = severeBreach ? consecutiveLatencyBreaches + 1 : 0;
          if (signalToSendMs !== null) {
            signalToSendSamples.push(signalToSendMs);
            if (signalToSendSamples.length > 100) signalToSendSamples.shift();
          }
          const rollingP95Ms = percentile(signalToSendSamples, 0.95);
          logger({
            schema: "polyedge.funded_direct_latency.v1",
            status: breached ? "signal_to_send_slo_breached" : "intent_completed",
            message_id: message.messageId || null,
            decision_id: body.decision_id,
            queue_to_receive_ms: queueToReceiveMs,
            queue_receive_wall_ms: receivedWallMs,
            queue_receive_monotonic_ms: queueReceiveMonotonicMs,
            signal_to_send_ms: signalToSendMs,
            signal_to_send_slo_ms: config.signalToSendSloMs,
            rolling_p95_signal_to_send_ms: rollingP95Ms,
            rolling_p95_slo_breached: rollingP95Ms !== null && rollingP95Ms > config.signalToSendSloMs,
            consecutive_latency_breaches: consecutiveLatencyBreaches,
            order_submission_attempted: result?.execution?.order_submission_attempted === true,
            execution_timing: result?.execution_timing || null
          });
          if (result?.status === "paused_by_account_risk_state") {
            logger({
              schema: "polyedge.funded_direct_alert.v1",
              status: "paused_by_account_risk_state",
              decision_id: body.decision_id,
              account_risk_pause: true,
              error: result.error || null
            });
          }
          if (consecutiveLatencyBreaches >= 3) {
            stopping = true;
            logger({
              schema: "polyedge.funded_direct_alert.v1",
              status: "engine_paused_by_consecutive_latency_breaches",
              decision_id: body.decision_id,
              consecutive_transitions_above_3000_ms: consecutiveLatencyBreaches,
              account_risk_pause: true
            });
          }
        } else {
          await receiver.deadLetterMessage(message, {
            deadLetterReason: "UnsupportedSchema",
            deadLetterErrorDescription: "Message is not a funded warmup or exact intent handoff."
          });
          failedMessages += 1;
        }
      } catch (error) {
        failedMessages += 1;
        const deterministic = /unsupported|schema|binding|TTL|stale|SHA-256|does not qualify|already has an authorization|latency|risk|reservation|cash.flow|equity|position|open order/i.test(error.message);
        if (deterministic || Number(message.deliveryCount || 0) >= 2) {
          await receiver.deadLetterMessage(message, {
            deadLetterReason: "FundedIntentFailedClosed",
            deadLetterErrorDescription: String(error.message).slice(0, 4_000)
          }).catch(() => null);
        } else {
          await receiver.abandonMessage(message).catch(() => null);
        }
        logger({
          schema: "polyedge.funded_direct_service.v2",
          status: "persistent_message_failed_closed",
          message_id: message.messageId || null,
          delivery_count: message.deliveryCount || 0,
          error: error.message
        });
      } finally {
        clearInterval(renewal);
      }
      if (config.maxMessages > 0 && processedMessages + failedMessages >= config.maxMessages) break;
    }
    return {
      schema: "polyedge.funded_direct_service.v2",
      status: "persistent_service_stopped",
      processed_messages: processedMessages,
      failed_messages: failedMessages
    };
  } finally {
    clearInterval(heartbeat);
    process.removeListener("SIGTERM", stop);
    process.removeListener("SIGINT", stop);
    await executor.close().catch(() => null);
    await receiver.close().catch(() => null);
    await bus.close().catch(() => null);
  }
}

function persistentCanaryBootstrapEnv(env) {
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
    STRATEGY_CANARY_MIN_REMAINING_TTL_MS: String(env.FUNDED_DIRECT_CHILD_MIN_REMAINING_TTL_MS || "15000"),
    VENUE_PROBE_CAMPAIGN_CASH_FLOWS: "[]"
  };
}

function parseMessageBody(value) {
  if (value && typeof value === "object" && !Buffer.isBuffer(value)) return value;
  const text = Buffer.isBuffer(value) ? value.toString("utf8") : String(value || "");
  try { return JSON.parse(text); }
  catch { throw new Error("fail closed: Service Bus message body is not valid JSON"); }
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
