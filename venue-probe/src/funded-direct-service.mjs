import { pathToFileURL } from "node:url";
import { runFundedDirectWorker } from "./funded-direct-worker.mjs";
import { sanitize } from "./lib.mjs";

export function loadFundedDirectServiceConfig(env = process.env) {
  const config = {
    enabled: env.FUNDED_DIRECT_SERVICE_ENABLED === "true",
    restartDelayMs: integer(env.FUNDED_DIRECT_SERVICE_RESTART_DELAY_MS, 1_000),
    riskPauseMs: integer(env.FUNDED_DIRECT_SERVICE_RISK_PAUSE_MS, 60_000),
    heartbeatMs: integer(env.FUNDED_DIRECT_SERVICE_HEARTBEAT_MS, 60_000),
    maxCycles: integer(env.FUNDED_DIRECT_SERVICE_MAX_CYCLES, 0)
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

function integer(value, fallback) {
  const parsed = Number(value);
  return Number.isInteger(parsed) ? parsed : fallback;
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
