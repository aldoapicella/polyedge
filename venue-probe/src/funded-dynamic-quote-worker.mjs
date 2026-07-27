import { pathToFileURL } from "node:url";
import { runFundedLadderController } from "./funded-ladder-controller.mjs";
import { sanitize } from "./lib.mjs";

const EXPECTED_CANDIDATE = "dynamic_quote_style";
const EXPECTED_CANDIDATE_VERSION = "dynamic_quote_style@2026-06-14";

export function loadFundedDynamicQuoteWorkerConfig(env = process.env) {
  const config = {
    enabled: env.FUNDED_DYNAMIC_QUOTE_WORKER_ENABLED === "true",
    candidate: String(env.STRATEGY_CANARY_CANDIDATE_NAME || "").trim(),
    candidateVersion: String(env.STRATEGY_CANARY_CANDIDATE_VERSION || "").trim(),
    controllerEnabled: env.FUNDED_LADDER_CONTROLLER_ENABLED === "true",
    fundedAllowed: env.ALLOW_FUNDED_LADDER === "true",
    maxIterations: integer(env.FUNDED_DYNAMIC_QUOTE_MAX_ITERATIONS, 200),
    pollIntervalMs: integer(env.FUNDED_DYNAMIC_QUOTE_POLL_INTERVAL_MS, 5_000),
    maxIdleMs: integer(env.FUNDED_DYNAMIC_QUOTE_MAX_IDLE_MS, 300_000)
  };
  const errors = [];
  if (!config.enabled) errors.push("FUNDED_DYNAMIC_QUOTE_WORKER_ENABLED must be true");
  if (!config.controllerEnabled) errors.push("FUNDED_LADDER_CONTROLLER_ENABLED must be true");
  if (!config.fundedAllowed) errors.push("ALLOW_FUNDED_LADDER must be true");
  if (config.candidate !== EXPECTED_CANDIDATE) {
    errors.push(`STRATEGY_CANARY_CANDIDATE_NAME must equal ${EXPECTED_CANDIDATE}`);
  }
  if (config.candidateVersion !== EXPECTED_CANDIDATE_VERSION) {
    errors.push(`STRATEGY_CANARY_CANDIDATE_VERSION must equal ${EXPECTED_CANDIDATE_VERSION}`);
  }
  if (!(config.maxIterations >= 1 && config.maxIterations <= 2_000)) {
    errors.push("FUNDED_DYNAMIC_QUOTE_MAX_ITERATIONS must be in [1, 2000]");
  }
  if (!(config.pollIntervalMs >= 1_000 && config.pollIntervalMs <= 60_000)) {
    errors.push("FUNDED_DYNAMIC_QUOTE_POLL_INTERVAL_MS must be in [1000, 60000]");
  }
  if (!(config.maxIdleMs >= config.pollIntervalMs && config.maxIdleMs <= 3_600_000)) {
    errors.push("FUNDED_DYNAMIC_QUOTE_MAX_IDLE_MS must be between the poll interval and 3600000");
  }
  if (errors.length) {
    throw new Error(`funded_dynamic_quote_worker blocked: ${errors.join("; ")}`);
  }
  return config;
}

export async function runFundedDynamicQuoteWorker({
  env = process.env,
  runController = runFundedLadderController,
  sleep = delay,
  clock = () => new Date()
} = {}) {
  const config = loadFundedDynamicQuoteWorkerConfig(env);
  let idleSince = null;
  let orderCompletions = 0;
  let pendingReconciliations = 0;
  let lastResult = null;

  for (let iteration = 1; iteration <= config.maxIterations; iteration += 1) {
    const result = await runController({ env, clock });
    lastResult = result;

    if (result.status === "funded_stage_order_completed") {
      orderCompletions += 1;
      idleSince = null;
      if (Number(result.remaining) === 0) {
        return summary("funded_stage_completed", {
          iteration,
          orderCompletions,
          pendingReconciliations,
          result
        });
      }
      continue;
    }

    if (result.status === "funded_stage_pending_terminal") {
      pendingReconciliations += 1;
      idleSince ??= clock().getTime();
      await sleep(config.pollIntervalMs);
      continue;
    }

    if (result.status === "stage_waiting_for_fresh_intent") {
      idleSince ??= clock().getTime();
      if (clock().getTime() - idleSince >= config.maxIdleMs) {
        return summary("idle_waiting_for_fresh_intent", {
          iteration,
          orderCompletions,
          pendingReconciliations,
          result
        });
      }
      await sleep(config.pollIntervalMs);
      continue;
    }

    if (result.status === "funded_stage_checkpoint_recovered") {
      return summary("funded_stage_completed", {
        iteration,
        orderCompletions,
        pendingReconciliations,
        result
      });
    }

    if (result.status === "funded_stage_dry_run_validated") {
      return summary("dry_run_validated", {
        iteration,
        orderCompletions,
        pendingReconciliations,
        result
      });
    }

    throw new Error(`funded_dynamic_quote_worker fail closed: unexpected controller status ${result.status || "missing"}`);
  }

  return summary("iteration_limit_reached", {
    iteration: config.maxIterations,
    orderCompletions,
    pendingReconciliations,
    result: lastResult
  });
}

function summary(status, { iteration, orderCompletions, pendingReconciliations, result }) {
  return {
    schema: "polyedge.funded_dynamic_quote_worker.v1",
    status,
    candidate: EXPECTED_CANDIDATE,
    candidate_version: EXPECTED_CANDIDATE_VERSION,
    iterations: iteration,
    order_completions: orderCompletions,
    pending_reconciliations: pendingReconciliations,
    last_controller_status: result?.status || null,
    remaining: result?.remaining ?? null,
    checkpoint: result?.checkpoint || null
  };
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
  runFundedDynamicQuoteWorker()
    .then((result) => console.log(JSON.stringify(sanitize(result))))
    .catch((error) => {
      process.exitCode = 1;
      console.error(JSON.stringify({
        schema: "polyedge.funded_dynamic_quote_worker.v1",
        status: "failed_closed",
        error: error.message
      }));
    });
}
