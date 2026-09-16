import { pathToFileURL } from "node:url";
import {
  createPersistentCanaryExecutor,
  SAFETY_CACHE_MAX_SELECTION_AGE_MS
} from "./canary.mjs";
import {
  activeBtcFifteenMinuteMarket,
  fundedMarketWarmup,
  persistentCanaryBootstrapEnv
} from "./funded-direct-service.mjs";
import { sanitize } from "./lib.mjs";

export function validateFundedReconciliationSnapshot(value, sessionId) {
  if (value?.schema !== "polyedge.funded_capital_snapshot.v1" ||
      value.session_id !== sessionId ||
      !Number.isFinite(value.snapshot_age_ms) ||
      value.snapshot_age_ms < 0 ||
      value.snapshot_age_ms > SAFETY_CACHE_MAX_SELECTION_AGE_MS ||
      value.snapshot_source !== "persistent_safety_cache" ||
      value.risk_passed !== true ||
      !Array.isArray(value.blockers) || value.blockers.length !== 0 ||
      value.open_order_count !== 0 ||
      value.unresolved_position_count !== 0 ||
      value.unresolved_risk_reservation_count !== 0) {
    throw new Error("fail closed: funded live reconciliation is not clean");
  }
  return value;
}

export async function runFundedDirectReconciliation({
  env = process.env,
  writeState = false,
  createExecutor = createPersistentCanaryExecutor,
  discoverMarket = activeBtcFifteenMinuteMarket,
  sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
  logger = (value) => console.log(JSON.stringify(value))
} = {}) {
  if (typeof writeState !== "boolean") {
    throw new Error("fail closed: funded reconciliation write mode is invalid");
  }
  const sessionId = String(env.VENUE_PROBE_FUNDED_CAMPAIGN_ID || "");
  const executor = await createExecutor({
    readOnly: !writeState,
    env: {
      ...persistentCanaryBootstrapEnv(env),
      STRATEGY_CANARY_DRY_RUN: "true",
      STRATEGY_CANARY_RUN_ID: `funded-reconciliation-${Date.now()}`
    }
  });
  let snapshot;
  let lastCacheError = null;
  try {
    const market = await discoverMarket();
    await executor.warmMarket(fundedMarketWarmup(market));
    for (let attempt = 0; attempt < 100; attempt += 1) {
      const status = executor.status();
      if (status.safety_snapshot_cache_error) {
        lastCacheError = status.safety_snapshot_cache_error;
        await sleep(100);
        continue;
      }
      if (status.safety_snapshot_cache_ready &&
          status.safety_snapshot_cache_in_flight === 0) {
        snapshot = validateFundedReconciliationSnapshot(
          executor.reconciliationSnapshot(),
          sessionId
        );
        break;
      }
      await sleep(100);
    }
    if (!snapshot) {
      if (lastCacheError) throw new Error(`fail closed: ${lastCacheError}`);
      throw new Error("fail closed: funded live reconciliation timed out");
    }
  } finally {
    await executor.close();
  }
  logger(snapshot);
  return snapshot;
}

const isMain = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isMain) {
  const writeState = process.env.FUNDED_DIRECT_RECONCILE_STATE_WRITE === "true";
  runFundedDirectReconciliation({
    writeState,
    ...(writeState ? {
      logger: (value) => console.log(JSON.stringify({
        schema: "polyedge.funded_state_reconciliation.v1",
        status: "reconciled",
        risk_passed: value.risk_passed,
        open_order_count: value.open_order_count,
        unresolved_position_count: value.unresolved_position_count,
        unresolved_risk_reservation_count: value.unresolved_risk_reservation_count
      }))
    } : {})
  }).catch((error) => {
    process.exitCode = 1;
    console.error(JSON.stringify(sanitize({
      schema: "polyedge.funded_reconciliation_proof.v1",
      status: "failed_closed",
      order_submission_attempted: false,
      error: writeState ? "funded state reconciliation failed closed" : error.message
    })));
  });
}
