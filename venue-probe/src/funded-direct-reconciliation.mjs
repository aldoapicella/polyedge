import { pathToFileURL } from "node:url";
import {
  createPersistentCanaryExecutor,
  SAFETY_CACHE_MAX_SELECTION_AGE_MS
} from "./canary.mjs";
import { persistentCanaryBootstrapEnv } from "./funded-direct-service.mjs";
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
  try {
    const market = await discoverMarket();
    const tokenIds = parseTokenIds(market.clobTokenIds);
    await executor.warmMarket({
      market_id: String(market.id),
      condition_id: String(market.conditionId),
      token_id: tokenIds[0],
      token_ids: tokenIds,
      market_end_ts: new Date(market.endDate).toISOString()
    });
    for (let attempt = 0; attempt < 100; attempt += 1) {
      const status = executor.status();
      if (status.safety_snapshot_cache_error) {
        throw new Error(`fail closed: ${status.safety_snapshot_cache_error}`);
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
      throw new Error("fail closed: funded live reconciliation timed out");
    }
  } finally {
    await executor.close();
  }
  logger(snapshot);
  return snapshot;
}

export async function activeBtcFifteenMinuteMarket() {
  const nowSeconds = Math.floor(Date.now() / 1_000);
  const currentStart = Math.floor(nowSeconds / 900) * 900;
  for (const start of [currentStart, currentStart + 900]) {
    const response = await fetch(
      `https://gamma-api.polymarket.com/markets?slug=btc-updown-15m-${start}`,
      { signal: AbortSignal.timeout(10_000) }
    );
    if (!response.ok) throw new Error(`fail closed: market lookup failed (${response.status})`);
    const values = await response.json();
    const market = Array.isArray(values) ? values[0] : null;
    if (market?.active === true && market?.closed !== true &&
        market?.acceptingOrders === true &&
        Date.parse(market.endDate) > Date.now() + 30_000) return market;
  }
  throw new Error("fail closed: active BTC 15-minute market was not discoverable");
}

function parseTokenIds(value) {
  const values = Array.isArray(value) ? value : JSON.parse(String(value || "[]"));
  const tokens = values.map(String).filter(Boolean);
  if (tokens.length !== 2) {
    throw new Error("fail closed: reconciliation market must have exactly two token ids");
  }
  return tokens;
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
