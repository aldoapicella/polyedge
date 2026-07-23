import { createHash } from "node:crypto";
import { DefaultAzureCredential } from "@azure/identity";
import {
  BlobServiceClient,
  StorageSharedKeyCredential
} from "@azure/storage-blob";

export const HORIZONS_SECONDS = [1, 5, 30, 60];
export const MARKOUT_HORIZONS_SECONDS = [1, 5, 30];
export const EVIDENCE_PROTOCOL_VERSION = 3;
export const MIN_STABLE_FINALITY_OBSERVATION_MS = 10_000;

export function isEvidenceProtocolVersionEligible(value) {
  return Number(value || 0) === EVIDENCE_PROTOCOL_VERSION;
}
export const MAX_MARKOUT_OBSERVATION_DELAY_MS = 2000;
export const DEFAULT_CAMPAIGN_BASELINE_EQUITY = 5.030521;
export const DEFAULT_CAMPAIGN_EQUITY_FLOOR = 4.03;
export const DEFAULT_MAX_CAMPAIGN_DRAWDOWN = 1;
export const DEFAULT_MAX_RECONCILIATION_DISCREPANCY = 0.01;

export function loadProbeConfig(env = process.env) {
  const value = {
    action: env.VENUE_PROBE_ACTION || "probe",
    executionMode: env.EXECUTION_MODE,
    allowLive: parseBoolean(env.ALLOW_LIVE),
    allowVenueProbe: parseBoolean(env.ALLOW_VENUE_PROBE),
    enableTakerOrders: parseBoolean(env.ENABLE_TAKER_ORDERS),
    campaignEnabled: parseBoolean(env.VENUE_PROBE_CAMPAIGN_ENABLED),
    killSwitch: parseBoolean(env.VENUE_PROBE_KILL_SWITCH),
    dryRun: env.VENUE_PROBE_DRY_RUN !== "false",
    trustBoundaryReady: parseBoolean(env.FUNDED_EVIDENCE_TRUST_BOUNDARY_READY),
    maxOpenOrders: integer(env.MAX_OPEN_ORDERS, 1),
    maxOrderNotional: number(env.VENUE_PROBE_MAX_ORDER_NOTIONAL, 1),
    minOrderNotional: number(env.VENUE_PROBE_MIN_ORDER_NOTIONAL, 1),
    minOrderPrice: number(env.VENUE_PROBE_MIN_ORDER_PRICE, 0.05),
    maxDailyLoss: number(env.MAX_DAILY_LOSS, 5),
    campaignId: String(env.VENUE_PROBE_FUNDED_CAMPAIGN_ID || "funded-campaign-2026-07-12").trim(),
    campaignBaselineEquity: number(env.VENUE_PROBE_CAMPAIGN_BASELINE_EQUITY, DEFAULT_CAMPAIGN_BASELINE_EQUITY),
    campaignEquityFloor: number(env.VENUE_PROBE_CAMPAIGN_EQUITY_FLOOR, DEFAULT_CAMPAIGN_EQUITY_FLOOR),
    maxCampaignDrawdown: number(env.VENUE_PROBE_MAX_CAMPAIGN_DRAWDOWN, DEFAULT_MAX_CAMPAIGN_DRAWDOWN),
    maxReconciliationDiscrepancy: number(env.VENUE_PROBE_MAX_RECONCILIATION_DISCREPANCY, DEFAULT_MAX_RECONCILIATION_DISCREPANCY),
    campaignCashFlows: parseCampaignCashFlows(env.VENUE_PROBE_CAMPAIGN_CASH_FLOWS || "[]"),
    maximumOrders: integer(env.VENUE_PROBE_MAXIMUM_ORDERS, 25),
    cancelAfterAckMs: integer(env.VENUE_PROBE_CANCEL_AFTER_ACK_MS, 0),
    restHorizonsSeconds: parseNumberList(env.VENUE_PROBE_REST_HORIZONS_SECONDS || "1,5,30,60"),
    interOrderDelayMs: integer(env.VENUE_PROBE_INTER_ORDER_DELAY_MS, 1000),
    prewarmMs: integer(env.VENUE_PROBE_PREWARM_MS, 5000),
    maxClockDriftMs: integer(env.VENUE_PROBE_MAX_CLOCK_DRIFT_MS, 5000),
    expectedCountry: String(env.VENUE_PROBE_EXPECTED_COUNTRY || "").trim().toUpperCase(),
    expectedEgressIp: String(env.VENUE_PROBE_EXPECTED_EGRESS_IP || "").trim(),
    estimatedRoundTripCostPerShare: number(env.VENUE_PROBE_ESTIMATED_ROUND_TRIP_COST_PER_SHARE, 0),
    startingCapital: number(env.VENUE_PROBE_STARTING_CAPITAL, null),
    clobUrl: env.POLYMARKET_CLOB_URL || "https://clob.polymarket.com",
    gammaUrl: env.POLYMARKET_GAMMA_URL || "https://gamma-api.polymarket.com",
    marketWsUrl:
      env.POLYMARKET_MARKET_WS_URL ||
      "wss://ws-subscriptions-clob.polymarket.com/ws/market",
    userWsUrl:
      env.POLYMARKET_USER_WS_URL ||
      "wss://ws-subscriptions-clob.polymarket.com/ws/user",
    privateKey: env.POLYMARKET_PRIVATE_KEY,
    apiKey: env.POLYMARKET_API_KEY,
    apiSecret: env.POLYMARKET_API_SECRET,
    apiPassphrase: env.POLYMARKET_API_PASSPHRASE,
    funderAddress: env.POLYMARKET_FUNDER_ADDRESS,
    signatureType: integer(env.POLYMARKET_SIGNATURE_TYPE, 3),
    storageAccount: env.AZURE_STORAGE_ACCOUNT_NAME,
    storageContainer: env.AZURE_STORAGE_CONTAINER_NAME || "bot-events",
    storageAccountKey: env.AZURE_STORAGE_ACCOUNT_KEY,
    azureClientId: env.AZURE_CLIENT_ID,
    targetSlug: env.VENUE_PROBE_MARKET_SLUG,
    outputDir: env.VENUE_PROBE_OUTPUT_DIR
  };
  validateProbeConfig(value);
  return value;
}

export function validateProbeConfig(config) {
  const errors = [];
  if (config.executionMode !== "venue_probe") errors.push("EXECUTION_MODE must equal venue_probe");
  if (config.allowLive) errors.push("ALLOW_LIVE must remain false");
  if (!config.allowVenueProbe) errors.push("ALLOW_VENUE_PROBE must be true");
  if (config.enableTakerOrders) errors.push("ENABLE_TAKER_ORDERS must remain false");
  if (config.killSwitch) errors.push("VENUE_PROBE_KILL_SWITCH is active");
  if (config.maxOpenOrders !== 1) errors.push("MAX_OPEN_ORDERS must equal 1");
  if (!(config.maximumOrders === 1 || (config.maximumOrders >= 25 && config.maximumOrders <= 50))) {
    errors.push("VENUE_PROBE_MAXIMUM_ORDERS must equal 1 for a canary or be in [25, 50] for a campaign");
  }
  if (config.campaignEnabled && config.maximumOrders < 25) {
    errors.push("VENUE_PROBE_CAMPAIGN_ENABLED requires at least 25 orders");
  }
  if (!(config.maxOrderNotional > 0 && config.maxOrderNotional <= 1)) {
    errors.push("VENUE_PROBE_MAX_ORDER_NOTIONAL must be in (0, 1]");
  }
  if (!(config.minOrderNotional >= 1 && config.minOrderNotional <= config.maxOrderNotional)) {
    errors.push("VENUE_PROBE_MIN_ORDER_NOTIONAL must be in [1, VENUE_PROBE_MAX_ORDER_NOTIONAL]");
  }
  if (!(config.minOrderPrice >= 0.01 && config.minOrderPrice <= 0.4)) {
    errors.push("VENUE_PROBE_MIN_ORDER_PRICE must be in [0.01, 0.4]");
  }
  if (!(config.maxDailyLoss > 0 && config.maxDailyLoss <= 5)) {
    errors.push("MAX_DAILY_LOSS must be in (0, 5]");
  }
  if (!/^[a-zA-Z0-9][a-zA-Z0-9._-]{0,79}$/.test(config.campaignId)) {
    errors.push("VENUE_PROBE_FUNDED_CAMPAIGN_ID must be a safe 1-80 character identifier");
  }
  if (!(config.campaignBaselineEquity > 0)) {
    errors.push("VENUE_PROBE_CAMPAIGN_BASELINE_EQUITY must be positive");
  }
  if (!(config.campaignEquityFloor >= 0 && config.campaignEquityFloor < config.campaignBaselineEquity)) {
    errors.push("VENUE_PROBE_CAMPAIGN_EQUITY_FLOOR must be non-negative and below the baseline equity");
  }
  if (!(config.maxCampaignDrawdown > 0 && config.maxCampaignDrawdown <= config.campaignBaselineEquity)) {
    errors.push("VENUE_PROBE_MAX_CAMPAIGN_DRAWDOWN must be positive and no greater than baseline equity");
  }
  if (config.campaignBaselineEquity - config.maxCampaignDrawdown + 1e-9 < config.campaignEquityFloor) {
    errors.push("campaign drawdown limit would breach VENUE_PROBE_CAMPAIGN_EQUITY_FLOOR");
  }
  if (!(config.maxReconciliationDiscrepancy >= 0 && config.maxReconciliationDiscrepancy <= 0.01)) {
    errors.push("VENUE_PROBE_MAX_RECONCILIATION_DISCREPANCY must be in [0, 0.01]");
  }
  if (config.startingCapital !== null && config.startingCapital <= 0) {
    errors.push("VENUE_PROBE_STARTING_CAPITAL must be positive when configured");
  }
  if (config.cancelAfterAckMs < 0) errors.push("VENUE_PROBE_CANCEL_AFTER_ACK_MS cannot be negative");
  if (!config.restHorizonsSeconds.length || config.restHorizonsSeconds.some((value) => ![1, 5, 30, 60].includes(value))) {
    errors.push("VENUE_PROBE_REST_HORIZONS_SECONDS must contain only 1,5,30,60");
  }
  if (config.maxClockDriftMs < 500 || config.maxClockDriftMs > 30000) {
    errors.push("VENUE_PROBE_MAX_CLOCK_DRIFT_MS must be in [500, 30000]");
  }
  if (!config.dryRun && !config.expectedCountry) {
    errors.push("VENUE_PROBE_EXPECTED_COUNTRY is required for order submission");
  }
  if (!config.dryRun && !config.expectedEgressIp) {
    errors.push("VENUE_PROBE_EXPECTED_EGRESS_IP is required for order submission");
  }
  if (!config.dryRun && !config.storageAccount) {
    errors.push("AZURE_STORAGE_ACCOUNT_NAME is required for durable live risk reservations");
  }
  if (!config.dryRun && !config.trustBoundaryReady) {
    errors.push("FUNDED_EVIDENCE_TRUST_BOUNDARY_READY must be true only after signer/control identities and containers are isolated");
  }
  for (const [name, value] of [
    ["POLYMARKET_PRIVATE_KEY", config.privateKey],
    ["POLYMARKET_API_KEY", config.apiKey],
    ["POLYMARKET_API_SECRET", config.apiSecret],
    ["POLYMARKET_API_PASSPHRASE", config.apiPassphrase],
    ["POLYMARKET_FUNDER_ADDRESS", config.funderAddress]
  ]) {
    if (!value) errors.push(`${name} is required`);
  }
  if (![0, 1, 2, 3].includes(config.signatureType)) {
    errors.push("POLYMARKET_SIGNATURE_TYPE must be 0, 1, 2, or 3");
  }
  if (errors.length) throw new Error(`venue_probe blocked: ${errors.join("; ")}`);
}

export function assertEligibleOrigin(geoblock, config) {
  if (!geoblock || geoblock.blocked !== false) {
    throw new Error(`venue execution origin blocked by Polymarket (${geoblock?.country || "unknown"}/${geoblock?.region || "unknown"})`);
  }
  const country = String(geoblock.country || "").toUpperCase();
  if (config.expectedCountry && country !== config.expectedCountry) {
    throw new Error(`fail closed: execution country ${country || "unknown"} does not match ${config.expectedCountry}`);
  }
  if (config.expectedEgressIp && geoblock.ip !== config.expectedEgressIp) {
    throw new Error("fail closed: observed egress IP does not match the configured static IP");
  }
  return true;
}

export function evaluateDailyRiskGate(consumed, limit, dryRun) {
  const lossLimitsOk = Number(consumed) < Number(limit);
  if (!lossLimitsOk && !dryRun) {
    throw new Error("fail closed: conservative daily loss budget is already exhausted");
  }
  return {
    loss_limits_ok: lossLimitsOk,
    diagnostics_only: !lossLimitsOk && dryRun,
    submission_allowed: lossLimitsOk && !dryRun
  };
}

export function summarizeCampaignRisk({
  control,
  liquidCollateral,
  summedPositionValue,
  reportedPositionValue,
  openOrderCount = 0,
  unresolvedPositionCount = 0,
  unresolvedReservationCount = 0,
  proposedNotional = 0,
  orderNotional = proposedNotional
}) {
  const liquid = Math.max(0, number(liquidCollateral, 0));
  const summed = Math.max(0, number(summedPositionValue, 0));
  const reported = Math.max(0, number(reportedPositionValue, 0));
  const discrepancy = Math.abs(summed - reported);
  // Until the two independent position-value views agree, use the lower value.
  const conservativePositionValue = Math.min(summed, reported);
  const accountEquity = liquid + conservativePositionValue;
  const adjustedBaseline = number(control?.baseline_equity, DEFAULT_CAMPAIGN_BASELINE_EQUITY) +
    number(control?.net_external_cash_flow, 0);
  const equityFloor = number(control?.equity_floor, DEFAULT_CAMPAIGN_EQUITY_FLOOR) +
    number(control?.net_external_cash_flow, 0);
  const maximumDrawdown = number(control?.max_campaign_drawdown, DEFAULT_MAX_CAMPAIGN_DRAWDOWN);
  const reserved = Math.max(0, number(proposedNotional, 0));
  const principal = Math.max(0, number(orderNotional, 0));
  const campaignDrawdown = Math.max(0, adjustedBaseline - accountEquity);
  const projectedEquity = accountEquity - reserved;
  const projectedDrawdown = Math.max(0, adjustedBaseline - projectedEquity);
  const blockers = [];
  if (discrepancy > number(control?.max_reconciliation_discrepancy, DEFAULT_MAX_RECONCILIATION_DISCREPANCY) + 1e-9) {
    blockers.push("account_reconciliation_discrepancy");
  }
  if (Number(openOrderCount) > 0) blockers.push("open_orders_present");
  if (Number(unresolvedReservationCount) > 0) blockers.push("unresolved_risk_reservation");
  if (Number(unresolvedPositionCount) > 1) blockers.push("unresolved_position_limit_exceeded");
  if (reserved > 0 && Number(unresolvedPositionCount) > 0) blockers.push("existing_unresolved_position_blocks_submission");
  if (accountEquity + 1e-9 < equityFloor) blockers.push("equity_floor_breached");
  if (campaignDrawdown > maximumDrawdown + 1e-9) blockers.push("campaign_drawdown_exhausted");
  if (principal > number(control?.max_order_notional, 1) + 1e-9) blockers.push("order_notional_limit_exceeded");
  if (projectedEquity + 1e-9 < equityFloor) blockers.push("projected_equity_floor_breach");
  if (projectedDrawdown > maximumDrawdown + 1e-9) blockers.push("projected_campaign_drawdown_breach");
  return {
    schema_version: 1,
    campaign_id: control?.campaign_id || null,
    baseline_equity: roundMoney(number(control?.baseline_equity, DEFAULT_CAMPAIGN_BASELINE_EQUITY)),
    net_external_cash_flow: roundMoney(number(control?.net_external_cash_flow, 0)),
    cash_flow_adjusted_baseline: roundMoney(adjustedBaseline),
    equity_floor: roundMoney(equityFloor),
    max_campaign_drawdown: roundMoney(maximumDrawdown),
    liquid_collateral: roundMoney(liquid),
    summed_position_value: roundMoney(summed),
    reported_position_value: roundMoney(reported),
    conservative_position_value: roundMoney(conservativePositionValue),
    account_equity: roundMoney(accountEquity),
    campaign_drawdown: roundMoney(campaignDrawdown),
    proposed_notional: roundMoney(reserved),
    order_notional: roundMoney(principal),
    projected_equity: roundMoney(projectedEquity),
    projected_campaign_drawdown: roundMoney(projectedDrawdown),
    account_reconciliation_discrepancy: roundMoney(discrepancy),
    maximum_reconciliation_discrepancy: roundMoney(number(control?.max_reconciliation_discrepancy, DEFAULT_MAX_RECONCILIATION_DISCREPANCY)),
    open_order_count: Number(openOrderCount),
    unresolved_position_count: Number(unresolvedPositionCount),
    unresolved_risk_reservation_count: Number(unresolvedReservationCount),
    blockers,
    passed: blockers.length === 0
  };
}

export function evaluateCampaignRiskGate(risk, dryRun) {
  const passed = risk?.passed === true;
  if (!passed && !dryRun) {
    throw new Error(`fail closed: funded campaign risk gate blocked (${(risk?.blockers || ["unknown"]).join(", ")})`);
  }
  return {
    campaign_risk_ok: passed,
    diagnostics_only: !passed && dryRun,
    submission_allowed: passed && !dryRun,
    blockers: risk?.blockers || []
  };
}

export function isTransientUnsafeMarket(error) {
  return /maker price .* below evidence floor|no non-marketable maker price|cannot satisfy minimum order notional|no usable bid|order book has no usable ask/i.test(String(error?.message || ""));
}

export function summarizePortfolio(positions, liquidCollateral, startingCapital = null) {
  const rows = Array.isArray(positions) ? positions : [];
  const liquid = number(liquidCollateral, 0);
  const currentPositionValue = rows.reduce((sum, row) => sum + number(row.currentValue, 0), 0);
  const redeemableRows = rows.filter((row) => row.redeemable === true);
  const grossRedeemableValue = redeemableRows.reduce((sum, row) => sum + number(row.currentValue, 0), 0);
  const resolvedPositionCost = redeemableRows.reduce((sum, row) => sum + number(row.initialValue, 0), 0);
  const resolvedLosingCost = redeemableRows
    .filter((row) => number(row.currentValue, 0) <= 0)
    .reduce((sum, row) => sum + number(row.initialValue, 0), 0);
  const accountEquity = liquid + currentPositionValue;
  return {
    status: "available",
    liquid_collateral: roundMoney(liquid),
    current_position_value: roundMoney(currentPositionValue),
    account_equity: roundMoney(accountEquity),
    starting_capital: startingCapital === null ? null : roundMoney(startingCapital),
    account_net_pnl: startingCapital === null ? null : roundMoney(accountEquity - startingCapital),
    gross_redeemable_value: roundMoney(grossRedeemableValue),
    resolved_position_cost: roundMoney(resolvedPositionCost),
    resolved_position_net_pnl: roundMoney(grossRedeemableValue - resolvedPositionCost),
    resolved_losing_cost: roundMoney(resolvedLosingCost),
    redeemable_position_count: redeemableRows.length,
    redeemable_winner_count: redeemableRows.filter((row) => number(row.currentValue, 0) > 0).length,
    gross_payout_is_profit: false,
    positions: rows.map((row) => ({
      title: row.title ?? null,
      slug: row.slug ?? null,
      outcome: row.outcome ?? null,
      size: number(row.size, 0),
      average_price: number(row.avgPrice, 0),
      initial_value: number(row.initialValue, 0),
      current_value: number(row.currentValue, 0),
      cash_pnl: number(row.cashPnl, 0),
      redeemable: row.redeemable === true
    }))
  };
}

export function campaignRestSchedule(count, horizons, seed = "polyedge") {
  const values = Array.from({ length: count }, (_, index) => horizons[index % horizons.length]);
  let state = [...Buffer.from(seed)].reduce((value, byte) => ((value * 33) ^ byte) >>> 0, 5381);
  for (let index = values.length - 1; index > 0; index -= 1) {
    state = (1664525 * state + 1013904223) >>> 0;
    const swap = state % (index + 1);
    [values[index], values[swap]] = [values[swap], values[index]];
  }
  return values;
}

export function sanitize(value) {
  if (Array.isArray(value)) return value.map(sanitize);
  if (!value || typeof value !== "object") return value;
  return Object.fromEntries(
    Object.entries(value).map(([key, child]) => [
      key,
      /secret|passphrase|private.?key|api.?key|signature|authorization|auth$|^(?:owner|order_owner)$/i.test(key)
        ? "[REDACTED]"
        : sanitize(child)
    ])
  );
}

export class EventLedger {
  constructor(runId) {
    this.runId = runId;
    this.startedMono = process.hrtime.bigint();
    this.events = [];
  }

  record(type, data = {}) {
    const elapsedNs = process.hrtime.bigint() - this.startedMono;
    const event = {
      schema_version: 1,
      run_id: this.runId,
      type,
      recorded_ts: new Date().toISOString(),
      monotonic_elapsed_ns: elapsedNs.toString(),
      data: sanitize(data)
    };
    this.events.push(event);
    return event;
  }

  jsonl() {
    return `${this.events.map((event) => JSON.stringify(event)).join("\n")}\n`;
  }
}

export function selectMakerOrder(book, maxNotional, minNotional = 1, minPrice = 0.05) {
  const tick = Number(book.tick_size || book.tickSize || "0.01");
  const minSize = Number(book.min_order_size || book.minOrderSize || "5");
  const bids = (book.bids || []).map(level).filter(Boolean);
  const asks = (book.asks || []).map(level).filter(Boolean);
  const bestBid = bids.length ? Math.max(...bids.map((row) => row.price)) : null;
  const bestAsk = asks.length ? Math.min(...asks.map((row) => row.price)) : null;
  let price = bestBid ?? (bestAsk === null ? tick : Math.max(tick, bestAsk - tick));
  if (bestAsk !== null && price >= bestAsk) price = bestAsk - tick;
  const cappedPrice = roundedTick(maxNotional / minSize, tick);
  price = Math.min(price, cappedPrice);
  price = roundedTick(Math.max(tick, price), tick);
  if (bestAsk !== null && price >= bestAsk) {
    throw new Error(`no non-marketable maker price exists below best ask ${bestAsk}`);
  }
  if (price < minPrice) {
    throw new Error(`maker price ${price} is below evidence floor ${minPrice}`);
  }
  const size = Math.max(minSize, Math.ceil((minNotional / price) * 100) / 100);
  const notional = price * size;
  if (!Number.isFinite(notional) || notional < minNotional - 1e-9 || notional > maxNotional + 1e-9) {
    throw new Error(`safe order notional ${notional} is outside [${minNotional}, ${maxNotional}]`);
  }
  const samePriceSize = bids
    .filter((row) => nearlyEqual(row.price, price))
    .reduce((sum, row) => sum + row.size, 0);
  const betterPriceSize = bids
    .filter((row) => row.price > price)
    .reduce((sum, row) => sum + row.size, 0);
  return {
    side: "BUY",
    price,
    size,
    notional,
    tickSize: String(book.tick_size || book.tickSize || "0.01"),
    negRisk: Boolean(book.neg_risk ?? book.negRisk ?? false),
    bestBid,
    bestAsk,
    spread: bestBid === null || bestAsk === null ? null : bestAsk - bestBid,
    samePricePublicSize: samePriceSize,
    betterPricePublicSize: betterPriceSize,
    inferredSizeAhead: samePriceSize + betterPriceSize
  };
}

export function marketContext(messages) {
  const prices = [];
  let tradeFlow = 0;
  let depthChanges = 0;
  for (const message of messages) {
    if (message.event_type === "last_trade_price") {
      const price = Number(message.price);
      const size = Number(message.size || 0);
      if (Number.isFinite(price)) prices.push(price);
      if (Number.isFinite(size)) tradeFlow += size;
    }
    if (message.event_type === "price_change" || message.event_type === "book") depthChanges += 1;
  }
  const returns = prices.slice(1).map((price, index) => price - prices[index]);
  const mean = average(returns);
  const variance = returns.length
    ? average(returns.map((value) => (value - mean) ** 2))
    : 0;
  return {
    observed_trade_count: prices.length,
    observed_trade_size: tradeFlow,
    observed_depth_changes: depthChanges,
    price_volatility: Math.sqrt(variance)
  };
}

export function modelObservations({ order, market, lifecycle, context, markouts = [] }) {
  const coverage = validateFillMarkouts(markouts, lifecycle.related_trade_ids || [], lifecycle.actual_matched_size);
  const markoutTimingValid = coverage.timing_valid;
  const markoutComplete = coverage.complete;
  const markout30 = weightedExecutableMarkout(markouts, 30);
  const derivedEntryFee30 = weightedMarkoutMetric(markouts, 30, "entry_fee_per_share");
  const derivedExitFee30 = weightedMarkoutMetric(markouts, 30, "hypothetical_exit_fee_per_share");
  const derivedRoundTripCost30 = weightedMarkoutMetric(markouts, 30, "round_trip_fee_per_share");
  const roundTripCost30 = Number(lifecycle.actual_matched_size) > 0 ? derivedRoundTripCost30 : 0;
  const entryFee30 = Number(lifecycle.actual_matched_size) > 0 ? derivedEntryFee30 : 0;
  const exitFee30 = Number(lifecycle.actual_matched_size) > 0 ? derivedExitFee30 : 0;
  const stableTerminalFinality = lifecycle.post_cancel_finality_stable === true &&
    Number(lifecycle.post_cancel_observation_ms) >= MIN_STABLE_FINALITY_OBSERVATION_MS;
  const cancellationEvidenceComplete = lifecycle.cancel_send_wall_ms === null
    ? lifecycle.fully_filled === true && lifecycle.client_cancel_round_trip_ms === null &&
      lifecycle.client_to_user_cancel_ack_ms === null
    : Number.isFinite(Number(lifecycle.cancel_send_wall_ms)) &&
      Number.isFinite(Number(lifecycle.client_cancel_round_trip_ms)) && Number(lifecycle.client_cancel_round_trip_ms) >= 0 &&
      Number.isFinite(Number(lifecycle.client_to_user_cancel_ack_ms)) && Number(lifecycle.client_to_user_cancel_ack_ms) >= 0;
  return HORIZONS_SECONDS.map((horizon) => {
    const liveSeconds = lifecycle.live_duration_ms / 1000;
    const filledAtMs = lifecycle.first_fill_after_ack_ms;
    const filled = filledAtMs !== null && filledAtMs <= horizon * 1000;
    // Once authenticated/REST reconciliation proves stable terminal finality
    // with a globally empty account, this order cannot acquire a later fill.
    // That makes all remaining horizon labels observable without pretending the
    // quote itself stayed live through those horizons.
    const labelObserved = liveSeconds >= horizon || filled || stableTerminalFinality;
    const qualityEligible = lifecycle.reconciliation_complete === true &&
      lifecycle.zero_open_orders_confirmed === true &&
      lifecycle.data_gap_detected !== true &&
      lifecycle.cancellation_failure !== true &&
      lifecycle.rest_order_returned === true &&
      lifecycle.matched_size_source_agreement === true &&
      lifecycle.trade_id_source_agreement === true &&
      lifecycle.markout_capture_complete === true &&
      stableTerminalFinality &&
      cancellationEvidenceComplete &&
      markoutTimingValid &&
      (lifecycle.actual_matched_size <= 0 || markoutComplete);
    return {
      horizon_seconds: horizon,
      order_submitted: true,
      eligible: labelObserved && qualityEligible,
      label_observed: labelObserved,
      quality_eligible: qualityEligible,
      filled,
      partial_fill: lifecycle.actual_matched_size > 0 && lifecycle.actual_matched_size < order.size,
      inferred_size_ahead: order.inferredSizeAhead,
      spread: order.spread,
      order_price: order.price,
      order_size: order.size,
      time_to_expiry_seconds: market.endTs
        ? Math.max(0, (new Date(market.endTs).getTime() - lifecycle.ack_wall_ms) / 1000)
        : null,
      pre_send_trade_size: context.observed_trade_size,
      pre_send_depth_changes: context.observed_depth_changes,
      pre_send_volatility: context.price_volatility,
      reconciliation_complete: lifecycle.reconciliation_complete === true,
      zero_open_orders_confirmed: lifecycle.zero_open_orders_confirmed === true,
      data_gap_detected: lifecycle.data_gap_detected === true,
      cancellation_failure: lifecycle.cancellation_failure === true,
      markout_complete: markoutComplete,
      markout_timing_valid: markoutTimingValid,
      executable_markout_30s_per_share: markout30,
      venue_fee_model: lifecycle.venue_fee_model,
      venue_fee_rate: lifecycle.venue_fee_rate,
      venue_fee_rate_bps: lifecycle.venue_fee_rate_bps,
      venue_fee_exponent: lifecycle.venue_fee_exponent,
      venue_fee_taker_only: lifecycle.venue_fee_taker_only,
      entry_fee_per_share: entryFee30,
      hypothetical_exit_fee_per_share: exitFee30,
      estimated_round_trip_cost_per_share: roundTripCost30
    };
  });
}

export function normalizeStoredObservation(row, probe) {
  const probeFilled = Number(probe?.lifecycle?.actual_matched_size || 0) > 0 ||
    (probe?.model_observations || []).some((candidate) => candidate.filled === true);
  if (!probeFilled) {
    const outcomeBound = row.executable_markout_30s_per_share === null
      && (row.estimated_round_trip_cost_per_share === null || Number(row.estimated_round_trip_cost_per_share) === 0);
    return {
      ...row,
      eligible: row.eligible === true && outcomeBound,
      quality_eligible: row.quality_eligible === true && outcomeBound,
      markout_timing_valid: row.markout_timing_valid !== false,
      executable_markout_30s_per_share: null,
      entry_fee_per_share: 0,
      hypothetical_exit_fee_per_share: 0,
      estimated_round_trip_cost_per_share: 0
    };
  }
  const markouts = Array.isArray(probe?.markouts) ? probe.markouts : [];
  const coverage = validateFillMarkouts(markouts, probe?.lifecycle?.related_trade_ids || [], probe?.lifecycle?.actual_matched_size || 0);
  const timingValid = coverage.timing_valid;
  const derivedMarkout = weightedExecutableMarkout(markouts, 30);
  const derivedEntryFee = weightedMarkoutMetric(markouts, 30, "entry_fee_per_share");
  const derivedExitFee = weightedMarkoutMetric(markouts, 30, "hypothetical_exit_fee_per_share");
  const derivedCost = weightedMarkoutMetric(markouts, 30, "round_trip_fee_per_share");
  const outcomeBound = finiteMetric(derivedMarkout) && finiteMetric(derivedCost)
    && Math.abs(Number(row.executable_markout_30s_per_share) - derivedMarkout) <= 1e-8
    && Math.abs(Number(row.entry_fee_per_share) - derivedEntryFee) <= 1e-8
    && Math.abs(Number(row.hypothetical_exit_fee_per_share) - derivedExitFee) <= 1e-8
    && Math.abs(Number(row.estimated_round_trip_cost_per_share) - derivedCost) <= 1e-8
    && row.venue_fee_model === probe?.lifecycle?.venue_fee_model
    && Math.abs(Number(row.venue_fee_rate) - Number(probe?.lifecycle?.venue_fee_rate)) <= 1e-8
    && Math.abs(Number(row.venue_fee_rate_bps) - Number(probe?.lifecycle?.venue_fee_rate_bps)) <= 1e-8
    && Math.abs(Number(row.venue_fee_exponent) - Number(probe?.lifecycle?.venue_fee_exponent)) <= 1e-8
    && row.venue_fee_taker_only === probe?.lifecycle?.venue_fee_taker_only;
  return {
    ...row,
    eligible: row.eligible === true && timingValid && outcomeBound,
    quality_eligible: row.quality_eligible === true && timingValid && outcomeBound,
    markout_complete: row.markout_complete === true && coverage.complete,
    markout_timing_valid: timingValid,
    executable_markout_30s_per_share: derivedMarkout,
    entry_fee_per_share: derivedEntryFee,
    hypothetical_exit_fee_per_share: derivedExitFee,
    estimated_round_trip_cost_per_share: derivedCost
  };
}

export function validateFillMarkouts(markouts, fillIds, matchedSize = 0, maxDelayMs = MAX_MARKOUT_OBSERVATION_DELAY_MS) {
  const rows = Array.isArray(markouts) ? markouts : [];
  const expected = [...new Set((fillIds || []).map(String).filter(Boolean))];
  if (Number(matchedSize) <= 0) {
    return { complete: true, timing_valid: true, expected_fill_count: 0, complete_fill_count: 0 };
  }
  if (!expected.length) {
    return { complete: false, timing_valid: false, expected_fill_count: 0, complete_fill_count: 0 };
  }
  const legacySingleFill = expected.length === 1 && rows.length > 0 && rows.every((row) => !row.fill_id);
  let timingValid = true;
  let completeFillCount = 0;
  for (const fillId of expected) {
    const fillRows = legacySingleFill ? rows : rows.filter((row) => String(row.fill_id || "") === fillId);
    const complete = MARKOUT_HORIZONS_SECONDS.every((horizon) => {
      const row = fillRows.find((candidate) => Number(candidate.horizon_seconds) === horizon);
      const delay = Number(row?.observation_delay_ms);
      const valuesPresent = finiteMetric(row?.midpoint) &&
        finiteMetric(row?.executable_price) &&
        finiteMetric(row?.midpoint_markout_per_share) &&
        finiteMetric(row?.executable_markout_per_share);
      const timing = Number.isFinite(delay) && delay >= 0 && delay <= maxDelayMs;
      timingValid &&= timing;
      return Boolean(row) && timing && valuesPresent;
    });
    if (complete) completeFillCount += 1;
  }
  return {
    complete: completeFillCount === expected.length,
    timing_valid: timingValid,
    expected_fill_count: expected.length,
    complete_fill_count: completeFillCount
  };
}

function weightedExecutableMarkout(markouts, horizon) {
  const rows = (markouts || []).filter((row) =>
    Number(row.horizon_seconds) === horizon &&
    finiteMetric(row.executable_markout_per_share) &&
    finiteMetric(row.fill_size) &&
    Number(row.fill_size) > 0
  );
  const totalSize = rows.reduce((total, row) => total + Number(row.fill_size), 0);
  if (!(totalSize > 0)) return null;
  return rows.reduce((total, row) => total + Number(row.executable_markout_per_share) * Number(row.fill_size), 0) / totalSize;
}

function weightedMarkoutMetric(markouts, horizon, field) {
  const rows = (markouts || []).filter((row) =>
    Number(row.horizon_seconds) === horizon && finiteMetric(row.fill_size) && Number(row.fill_size) > 0
      && finiteMetric(row[field])
  );
  const totalSize = rows.reduce((total, row) => total + Number(row.fill_size), 0);
  if (!(totalSize > 0)) return null;
  return rows.reduce((total, row) => total + Number(row[field]) * Number(row.fill_size), 0) / totalSize;
}

function finiteMetric(value) {
  return value !== null && value !== undefined && value !== "" && Number.isFinite(Number(value));
}

export function fitEffectiveQueueModel(observations, minimumSamples = 100) {
  const groups = groupObservationsByProbe(observations);
  const eligibleGroups = groups.filter((group) => group.rows.some((row) => row.eligible));
  const excludedGroups = groups.filter((group) => !group.rows.some((row) => row.eligible));
  const eligible = eligibleGroups.flatMap((group) => group.rows.filter((row) => row.eligible));
  const excluded = observations.filter((row) => !row.eligible);
  const positives = eligibleGroups.filter((group) => group.rows.some((row) => row.eligible && row.filled)).length;
  const negatives = eligibleGroups.length - positives;
  const trainingDataEndTs = eligibleGroups.at(-1)?.recorded_ts || null;
  if (eligibleGroups.length < minimumSamples || positives < 10 || negatives < 10) {
    return {
      model_version: "queue-calibration-v1",
      training_data_end_ts: trainingDataEndTs,
      status: "collecting",
      evidence_protocol_version: EVIDENCE_PROTOCOL_VERSION,
      queue_position_source: "authenticated_lifecycle_plus_public_l2",
      queue_position_metric: "inferred_size_ahead",
      literal_fifo_rank_available: false,
      sample_size: eligibleGroups.length,
      label_sample_size: eligible.length,
      positive_fills: positives,
      negative_non_fills: negatives,
      excluded_observations: excludedGroups.length,
      excluded_label_rows: excluded.length,
      legacy_protocol_observations: groups.filter((group) => group.rows.every((row) => row.protocol_eligible === false)).length,
      minimum_samples: minimumSamples,
      reason: "requires at least 100 distinct eligible order probes with at least 10 filled probes and 10 non-filled probes",
      target: "probability_of_fill_within_horizon",
      temporal_split: "required_before_training",
      promotion_allowed: false,
      promotion_ready: false,
      quality_gates: modelQualityGates(observations),
      research_only: true
    };
  }
  const split = Math.min(eligibleGroups.length - 1, Math.max(1, Math.floor(eligibleGroups.length * 0.8)));
  const trainGroups = eligibleGroups.slice(0, split);
  const testGroups = eligibleGroups.slice(split);
  const train = trainGroups.flatMap((group) => group.rows.filter((row) => row.eligible));
  const test = testGroups.flatMap((group) => group.rows.filter((row) => row.eligible));
  const featureNames = [
    "bias",
    "log_inferred_size_ahead",
    "spread",
    "order_price",
    "order_size",
    "log_time_to_expiry",
    "log_pre_send_trade_size",
    "pre_send_depth_changes",
    "pre_send_volatility",
    "horizon_seconds"
  ];
  const { normalization, weights } = fitLogisticRows(train, featureNames.length, 2500);
  const predictions = test.map((row) => sigmoid(dot(weights, normalizedFeatures(row, normalization))));
  const rolling = groupedTemporalValidation(eligibleGroups, featureNames.length);
  const validationRows = rolling.rows.length ? rolling.rows : test;
  const validationPredictions = rolling.predictions.length ? rolling.predictions : predictions;
  const naivePredictions = rolling.naive_predictions.length
    ? rolling.naive_predictions
    : naiveHorizonPredictions(train, test);
  const brier = brierScore(validationPredictions, validationRows);
  const naiveBrier = brierScore(naivePredictions, validationRows);
  const brierImprovement = naiveBrier > 0 ? (naiveBrier - brier) / naiveBrier : 0;
  const calibration = calibrationBins(validationPredictions, validationRows);
  const ece = expectedCalibrationError(calibration, validationRows.length);
  const horizonMetrics = Object.fromEntries(HORIZONS_SECONDS.map((horizon) => {
    const indexes = validationRows.map((row, index) => ({ row, index })).filter(({ row }) => Number(row.horizon_seconds) === horizon);
    const horizonPredictions = indexes.map(({ index }) => validationPredictions[index]);
    const horizonNaivePredictions = indexes.map(({ index }) => naivePredictions[index]);
    const horizonRows = indexes.map(({ row }) => row);
    const horizonBrier = brierScore(horizonPredictions, horizonRows);
    const horizonNaiveBrier = brierScore(horizonNaivePredictions, horizonRows);
    return [String(horizon), {
      sample_size: horizonRows.length,
      positive_fills: horizonRows.filter((row) => row.filled).length,
      brier_score: horizonBrier,
      naive_base_rate_brier_score: horizonNaiveBrier,
      brier_improvement_fraction: horizonNaiveBrier > 0 ? (horizonNaiveBrier - horizonBrier) / horizonNaiveBrier : 0,
      calibration_bins: calibrationBins(horizonPredictions, horizonRows)
    }];
  }));
  const qualityGates = modelQualityGates(observations);
  const netMarkouts = eligibleGroups.flatMap((group) => {
    const row = group.rows.find((candidate) => candidate.eligible && candidate.filled && Number.isFinite(Number(candidate.executable_markout_30s_per_share)));
    return row ? [Number(row.executable_markout_30s_per_share) - number(row.estimated_round_trip_cost_per_share, 0)] : [];
  });
  const meanNetMarkout = average(netMarkouts);
  const markoutLower95 = lowerConfidenceBound95(netMarkouts);
  const promotionReady = qualityGates.passed &&
    eligibleGroups.length >= minimumSamples && positives >= 10 && negatives >= 10 &&
    brierImprovement >= 0.05 && ece <= 0.10 &&
    netMarkouts.length >= 10 && markoutLower95 > 0;
  return {
    model_version: "queue-calibration-v1",
    training_data_end_ts: trainingDataEndTs,
    status: "trained_research_only",
    evidence_protocol_version: EVIDENCE_PROTOCOL_VERSION,
    queue_position_source: "authenticated_lifecycle_plus_public_l2",
    queue_position_metric: "inferred_size_ahead",
    literal_fifo_rank_available: false,
    target: "probability_of_fill_within_horizon",
    sample_size: eligibleGroups.length,
    label_sample_size: eligible.length,
    train_size: trainGroups.length,
    test_size: testGroups.length,
    train_label_size: train.length,
    test_label_size: test.length,
    positive_fills: positives,
    negative_non_fills: negatives,
    excluded_observations: excludedGroups.length,
    excluded_label_rows: excluded.length,
    legacy_protocol_observations: groups.filter((group) => group.rows.every((row) => row.protocol_eligible === false)).length,
    temporal_split: "first_80pct_train_last_20pct_test",
    validation_method: "grouped_expanding_window_temporal",
    validation_folds: rolling.folds,
    validation_probe_count: rolling.probe_count,
    feature_names: featureNames,
    weights,
    normalization,
    out_of_sample_brier_score: brier,
    naive_horizon_base_rate_brier_score: naiveBrier,
    brier_improvement_fraction: brierImprovement,
    brier_improvement_percent: brierImprovement * 100,
    expected_calibration_error: ece,
    maximum_expected_calibration_error: 0.10,
    calibration_bins: calibration,
    horizon_metrics: horizonMetrics,
    quality_gates: qualityGates,
    net_markout_30s_sample_size: netMarkouts.length,
    mean_net_executable_markout_30s_per_share: meanNetMarkout,
    net_executable_markout_30s_lower_confidence_bound_95: Number.isFinite(markoutLower95) ? markoutLower95 : null,
    markout_confidence_method: "normal_mean_lower_bound_1.96_se_grouped_by_probe",
    promotion_ready: promotionReady,
    promotion_allowed: false,
    promotion_block_reason: promotionReady
      ? "research gates passed; explicit human strategy approval is still required"
      : "requires complete data quality, at least 5% Brier improvement over horizon base rates, ECE <= 0.10, and a positive 95% lower bound for net 30-second executable markout",
    research_only: true
  };
}

function modelQualityGates(observations) {
  const allGroups = groupObservationsByProbe(observations);
  const groups = allGroups.filter((group) => group.rows.some((row) => row.protocol_eligible !== false));
  const submitted = groups.filter((group) => group.rows.some((row) => row.order_submitted === true || row.label_observed || row.filled));
  const eligible = submitted.filter((group) => group.rows.some((row) => row.eligible));
  const excluded = submitted.filter((group) => !group.rows.some((row) => row.eligible));
  const reconciled = eligible.filter((group) => group.rows.every((row) => row.reconciliation_complete && row.zero_open_orders_confirmed)).length;
  const eligibleGaps = eligible.filter((group) => group.rows.some((row) => row.data_gap_detected)).length;
  const excludedGaps = excluded.filter((group) => group.rows.some((row) => row.data_gap_detected)).length;
  const cancelFailures = submitted.filter((group) => group.rows.some((row) => row.cancellation_failure)).length;
  const filled = eligible.filter((group) => group.rows.some((row) => row.filled));
  const completeMarkouts = filled.filter((group) => group.rows.every((row) => !row.filled || row.markout_complete)).length;
  const earlyMarkouts = submitted.filter((group) => group.rows.some((row) => row.filled && row.markout_timing_valid === false)).length;
  return {
    submitted_observations: submitted.length,
    eligible_observations: eligible.length,
    excluded_observations: excluded.length,
    reconciled_observations: reconciled,
    data_gap_observations: eligibleGaps + excludedGaps,
    eligible_data_gap_observations: eligibleGaps,
    excluded_data_gap_observations: excludedGaps,
    cancellation_failure_observations: cancelFailures,
    filled_observations: filled.length,
    complete_markout_observations: completeMarkouts,
    early_markout_observations: earlyMarkouts,
    legacy_protocol_observations: allGroups.length - groups.length,
    passed: submitted.length > 0 && eligible.length === submitted.length && reconciled === submitted.length && eligibleGaps === 0 && excludedGaps === 0 && cancelFailures === 0 && earlyMarkouts === 0 && completeMarkouts === filled.length
  };
}

function groupObservationsByProbe(observations) {
  const groups = new Map();
  observations.forEach((row, index) => {
    const key = row.probe_id || (row.run_id || row.recorded_ts
      ? `${row.run_id || "legacy"}:${row.recorded_ts || "undated"}`
      : `legacy-undated:${index}`);
    if (!groups.has(key)) groups.set(key, { key, recorded_ts: String(row.recorded_ts || ""), rows: [] });
    const group = groups.get(key);
    group.rows.push(row);
    if (String(row.recorded_ts || "") < group.recorded_ts) group.recorded_ts = String(row.recorded_ts || "");
  });
  return [...groups.values()].sort((left, right) => left.recorded_ts.localeCompare(right.recorded_ts) || left.key.localeCompare(right.key));
}

export function storageContainer(config) {
  if (!config.storageAccount) return null;
  const credential = config.storageAccountKey
    ? new StorageSharedKeyCredential(config.storageAccount, config.storageAccountKey)
    : new DefaultAzureCredential({ managedIdentityClientId: config.azureClientId });
  const service = new BlobServiceClient(
    `https://${config.storageAccount}.blob.core.windows.net`,
    credential
  );
  return service.getContainerClient(config.storageContainer);
}

export async function acquireCampaignLease(config, runId) {
  const container = storageContainer(config);
  if (!container) throw new Error("fail closed: durable storage is required for the campaign lease");
  await container.createIfNotExists();
  const blob = container.getBlockBlobClient("reports/research/venue-probe/control/campaign.lock");
  try {
    await blob.uploadData(Buffer.from("polyedge venue probe campaign lock\n"), {
      conditions: { ifNoneMatch: "*" },
      blobHTTPHeaders: { blobContentType: "text/plain" }
    });
  } catch (error) {
    if (![409, 412].includes(Number(error.statusCode))) throw error;
  }
  const leaseClient = blob.getBlobLeaseClient();
  try {
    await leaseClient.acquireLease(60, { abortSignal: AbortSignal.timeout(10_000) });
  } catch (error) {
    throw new Error(`fail closed: another venue probe owns the campaign lease (${error.statusCode || "lease unavailable"})`);
  }
  let renewalError = null;
  let renewing = false;
  let lastConfirmedRenewalMs = monotonicMs();
  const timer = setInterval(async () => {
    if (renewing || renewalError) return;
    renewing = true;
    try {
      await leaseClient.renewLease({ abortSignal: AbortSignal.timeout(10_000) });
      lastConfirmedRenewalMs = monotonicMs();
    } catch (error) {
      renewalError = error;
    } finally {
      renewing = false;
    }
  }, 20_000);
  timer.unref();
  return {
    run_id: runId,
    assertHealthy() {
      if (renewalError) throw new Error(`fail closed: campaign lease renewal failed (${renewalError.message})`);
      const ageMs = monotonicMs() - lastConfirmedRenewalMs;
      if (ageMs > 45_000) throw new Error(`fail closed: campaign lease freshness exceeded 45 seconds (${ageMs.toFixed(0)}ms)`);
    },
    async release() {
      clearInterval(timer);
      try {
        await leaseClient.releaseLease({ abortSignal: AbortSignal.timeout(10_000) });
      } catch (error) {
        if (!renewalError) throw error;
      }
    }
  };
}

export async function loadCampaignRiskControl(config) {
  const container = storageContainer(config);
  if (!container) throw new Error("fail closed: durable storage is required for funded campaign risk control");
  await container.createIfNotExists();
  const prefix = `reports/research/venue-probe/control/campaign-risk/${config.campaignId}`;
  const baselineBlob = container.getBlockBlobClient(`${prefix}/baseline.json`);
  const expectedBaseline = {
    schema_version: 1,
    campaign_id: config.campaignId,
    baseline_equity: roundMoney(config.campaignBaselineEquity),
    equity_floor: roundMoney(config.campaignEquityFloor),
    max_campaign_drawdown: roundMoney(config.maxCampaignDrawdown),
    max_order_notional: roundMoney(config.maxOrderNotional),
    max_open_orders: 1,
    max_unresolved_positions: 1,
    max_reconciliation_discrepancy: roundMoney(config.maxReconciliationDiscrepancy)
  };
  try {
    await baselineBlob.uploadData(Buffer.from(JSON.stringify({ ...expectedBaseline, created_ts: new Date().toISOString() }, null, 2)), {
      conditions: { ifNoneMatch: "*" },
      blobHTTPHeaders: { blobContentType: "application/json" }
    });
  } catch (error) {
    if (![409, 412].includes(Number(error.statusCode))) throw error;
  }
  const baseline = await downloadJson(baselineBlob);
  for (const [field, expected] of Object.entries(expectedBaseline)) {
    if (baseline?.[field] !== expected) {
      throw new Error(`fail closed: immutable campaign baseline mismatch for ${field}`);
    }
  }

  for (const flow of config.campaignCashFlows) {
    const blob = container.getBlockBlobClient(`${prefix}/cash-flows/${flow.id}.json`);
    const expected = {
      schema_version: 1,
      campaign_id: config.campaignId,
      id: flow.id,
      amount: roundMoney(flow.amount),
      transaction_hash: flow.transaction_hash
    };
    try {
      await blob.uploadData(Buffer.from(JSON.stringify({ ...expected, recorded_ts: new Date().toISOString() }, null, 2)), {
        conditions: { ifNoneMatch: "*" },
        blobHTTPHeaders: { blobContentType: "application/json" }
      });
    } catch (error) {
      if (![409, 412].includes(Number(error.statusCode))) throw error;
      const existing = await downloadJson(blob);
      for (const [field, value] of Object.entries(expected)) {
        if (existing?.[field] !== value) throw new Error(`fail closed: immutable campaign cash-flow mismatch for ${flow.id}`);
      }
    }
  }

  const cashFlows = [];
  for await (const item of container.listBlobsFlat({ prefix: `${prefix}/cash-flows/` })) {
    if (!item.name.endsWith(".json")) continue;
    cashFlows.push(await downloadJson(container.getBlockBlobClient(item.name)));
  }
  return {
    ...baseline,
    net_external_cash_flow: roundMoney(cashFlows.reduce((sum, flow) => sum + number(flow?.amount, 0), 0)),
    cash_flow_count: cashFlows.length,
    cash_flow_ids: cashFlows.map((flow) => flow.id).sort()
  };
}

export async function reserveProbeRisk(config, reservation) {
  const container = storageContainer(config);
  if (!container) throw new Error("fail closed: durable storage is required before order submission");
  const date = reservation.date || new Date().toISOString().slice(0, 10);
  const blob = container.getBlockBlobClient(`reports/research/venue-probe/risk-reservations/${date}/${reservation.probe_id}.json`);
  const payload = {
    schema_version: 1,
    evidence_protocol_version: EVIDENCE_PROTOCOL_VERSION,
    state: "reserved",
    date,
    run_id: reservation.run_id,
    probe_id: reservation.probe_id,
    reserved_notional: number(reservation.reserved_notional, 0),
    principal_notional: number(reservation.principal_notional, 0),
    fee_rate_bps: number(reservation.fee_rate_bps, 0),
    fee_risk_upper_bound: number(reservation.fee_risk_upper_bound, 0),
    market_id: reservation.market_id || null,
    condition_id: reservation.condition_id || null,
    token_id: reservation.token_id || null,
    order_submission_intended: true,
    order_submitted: null,
    matched_notional: null,
    created_ts: new Date().toISOString(),
    updated_ts: new Date().toISOString()
  };
  await blob.uploadData(Buffer.from(JSON.stringify(payload, null, 2)), {
    conditions: { ifNoneMatch: "*" },
    blobHTTPHeaders: { blobContentType: "application/json" }
  });
  return payload;
}

export async function finalizeProbeRisk(config, reservation, result) {
  const container = storageContainer(config);
  if (!container) throw new Error("fail closed: durable storage is required to finalize order risk");
  const date = reservation.date || new Date().toISOString().slice(0, 10);
  const payload = {
    ...reservation,
    state: result.state || "finalized",
    order_submitted: result.order_submitted === true,
    order_id: result.order_id || null,
    matched_notional: Math.max(0, number(result.matched_notional, 0)),
    reconciliation_complete: result.reconciliation_complete === true,
    zero_open_orders_confirmed: result.zero_open_orders_confirmed === true,
    updated_ts: new Date().toISOString()
  };
  await container
    .getBlockBlobClient(`reports/research/venue-probe/risk-reservations/${date}/${reservation.probe_id}.json`)
    .uploadData(Buffer.from(JSON.stringify(payload, null, 2)), {
      blobHTTPHeaders: { blobContentType: "application/json" }
    });
  return payload;
}

export async function settleProbeRiskReservations(config, settlement) {
  const conditionIds = new Set((settlement?.condition_ids || []).map((value) => String(value).toLowerCase()));
  const redemptionVerified = settlement?.settlement_verified === true && Boolean(settlement?.transaction_hash);
  const terminalVerified = settlement?.terminal_settlement_verified === true &&
    settlement?.evidence_source === "polymarket_data_api_redeemable";
  if (!conditionIds.size || (!redemptionVerified && !terminalVerified)) {
    throw new Error("fail closed: verified settlement evidence is required to release filled risk reservations");
  }
  const container = storageContainer(config);
  if (!container) throw new Error("fail closed: durable storage is required to settle order risk");
  const campaign = settlement?.terminal_portfolio ? await loadCampaignRiskControl(config) : null;
  let settled = 0;
  const terminalReservations = [];
  for await (const item of container.listBlobsFlat({ prefix: "reports/research/venue-probe/risk-reservations/" })) {
    if (!item.name.endsWith(".json")) continue;
    const blob = container.getBlockBlobClient(item.name);
    const response = await blob.download();
    const reservation = JSON.parse(await streamToString(response.readableStreamBody));
    if (number(reservation?.matched_notional, 0) <= 0 || !conditionIds.has(String(reservation?.condition_id || "").toLowerCase())) continue;
    // A previous redemption pass may already have moved the reservation to
    // position_settled before terminal portfolio evidence was available.  Do
    // not skip that reservation: the trusted redemption path must still be
    // able to publish its exact immutable terminal evidence artifact.
    if (isRiskReservationResolved(reservation) && !settlement?.terminal_portfolio) continue;
    const payload = {
      ...reservation,
      state: "position_settled",
      settlement_verified: true,
      settlement_evidence_source: redemptionVerified ? "verified_onchain_redemption" : settlement.evidence_source,
      settlement_transaction_hash: settlement.transaction_hash ? String(settlement.transaction_hash) : null,
      settlement_run_id: settlement.run_id || null,
      settled_ts: settlement.settled_ts || new Date().toISOString(),
      updated_ts: new Date().toISOString()
    };
    await blob.uploadData(Buffer.from(JSON.stringify(payload, null, 2)), {
      conditions: response.etag ? { ifMatch: response.etag } : undefined,
      blobHTTPHeaders: { blobContentType: "application/json" }
    });
    if (settlement?.terminal_portfolio) terminalReservations.push(payload);
    settled += 1;
  }
  // Publish terminal artifacts only after every reservation covered by this
  // verified atomic settlement has been durably updated. Otherwise the first
  // artifact in a multi-condition redemption would correctly observe the
  // remaining members as unresolved and fail before they could be finalized.
  for (const reservation of terminalReservations) {
    await publishTerminalRiskPortfolioEvidence(container, {
      reservation,
      settlement,
      campaign
    });
  }
  return settled;
}

export async function publishTerminalRiskPortfolioEvidence(container, { reservation, settlement, campaign }) {
  const portfolio = settlement?.terminal_portfolio;
  const liquid = number(portfolio?.liquid_collateral, NaN);
  const positions = number(portfolio?.current_position_value, NaN);
  const accountEquity = number(portfolio?.account_equity, NaN);
  const discrepancy = Math.abs(accountEquity - liquid - positions);
  if (!reservation?.probe_id || !reservation?.run_id || !reservation?.order_id) {
    throw new Error("fail closed: terminal evidence requires bound reservation run/probe/order identity");
  }
  if (![liquid, positions, accountEquity].every(Number.isFinite) || discrepancy > 0.01) {
    throw new Error("fail closed: terminal portfolio reconciliation discrepancy exceeds $0.01");
  }
  const noFill = settlement?.evidence_source === "authenticated_no_fill" && number(reservation.matched_notional, 0) === 0;
  if (settlement?.settlement_verified !== true || settlement?.zero_open_orders_confirmed !== true || (!noFill && !settlement?.transaction_hash)) {
    throw new Error("fail closed: terminal evidence requires verified settlement and global zero-open confirmation; fills also require a transaction hash");
  }
  if (!noFill) {
    const transactionHash = String(settlement.transaction_hash || "");
    const settlementWallet = String(settlement.settlement_wallet || "");
    const conditions = new Set((settlement.condition_ids || []).map((value) => String(value).toLowerCase()));
    const confirmations = Number(settlement.transaction_receipt_confirmations);
    if (!/^0x[0-9a-fA-F]{64}$/.test(transactionHash)
        || Number(settlement.polygon_chain_id) !== 137
        || settlement.transaction_receipt_status !== "success"
        || !/^\d+$/.test(String(settlement.transaction_block_number || ""))
        || BigInt(settlement.transaction_block_number) <= 0n
        || !Number.isInteger(confirmations)
        || confirmations < 2
        || !/^0x[0-9a-fA-F]{40}$/.test(settlementWallet)
        || !conditions.has(String(reservation.condition_id || "").toLowerCase())) {
      throw new Error("fail closed: filled terminal evidence lacks exact confirmed Polygon receipt, wallet, or condition binding");
    }
  }
  const baseline = number(campaign?.baseline_equity, NaN);
  const cashFlows = number(campaign?.net_external_cash_flow, NaN);
  if (![baseline, cashFlows].every(Number.isFinite)) throw new Error("fail closed: terminal evidence requires immutable campaign baseline and cash-flow ledger");
  const adjustedStart = baseline + cashFlows;
  const observedAt = settlement.settled_ts || new Date().toISOString();
  const unresolvedRiskReservations = await countUnresolvedRiskReservations(container);
  if (unresolvedRiskReservations !== 0) {
    throw new Error(`fail closed: terminal evidence requires zero durable unresolved risk reservations (observed ${unresolvedRiskReservations})`);
  }
  const evidence = {
    schema: "polyedge.canary_terminal_risk_portfolio.v1",
    producer: "polyedge_node_authenticated_risk_terminal",
    source: settlement.evidence_source || "polymarket_data_api_plus_onchain_redemption",
    run_id: reservation.run_id,
    probe_id: reservation.probe_id,
    order_id: reservation.order_id,
    condition_id: reservation.condition_id,
    reservation_state: reservation.state,
    settlement_verified: true,
    trust_boundary_ready: settlement.trust_boundary_ready === true,
    settlement_transaction_hash: settlement.transaction_hash || null,
    polygon_chain_id: noFill ? null : 137,
    transaction_receipt_status: noFill ? null : settlement.transaction_receipt_status,
    transaction_block_number: noFill ? null : String(settlement.transaction_block_number),
    transaction_receipt_confirmations: noFill ? null : Number(settlement.transaction_receipt_confirmations),
    settlement_wallet: noFill ? null : String(settlement.settlement_wallet).toLowerCase(),
    settlement_signer: noFill || !settlement.settlement_signer ? null : String(settlement.settlement_signer).toLowerCase(),
    redemption_condition_ids: noFill ? [] : [...new Set(settlement.condition_ids.map((value) => String(value).toLowerCase()))].sort(),
    portfolio_reconciled: true,
    reconciliation_discrepancy: roundMoney(discrepancy),
    zero_open_orders_confirmed: true,
    unresolved_exposure: 0,
    unresolved_risk_reservations: unresolvedRiskReservations,
    campaign_starting_equity: baseline,
    net_external_cash_flows: cashFlows,
    liquid_collateral: liquid,
    summed_position_value: positions,
    cash_flow_adjusted_ending_equity: accountEquity,
    minimum_observed_equity: accountEquity,
    maximum_observed_equity: Math.max(adjustedStart, accountEquity),
    campaign_cash_flow_ids: campaign.cash_flow_ids || [],
    observed_at: observedAt
  };
  const date = String(observedAt).slice(0, 10);
  const blobName = `reports/research/venue-probe/terminal-risk-portfolio/${date}/${reservation.probe_id}.json`;
  const content = JSON.stringify(evidence, null, 2);
  const sha256 = `sha256:${createHash("sha256").update(content).digest("hex")}`;
  await uploadImmutable(container, blobName, content, "application/json");
  return { blob_name: blobName, sha256, evidence };
}

async function countUnresolvedRiskReservations(container) {
  let count = 0;
  for await (const item of container.listBlobsFlat({ prefix: "reports/research/venue-probe/risk-reservations/" })) {
    if (!item.name.endsWith(".json")) continue;
    const blob = typeof container.getBlobClient === "function"
      ? container.getBlobClient(item.name)
      : container.getBlockBlobClient(item.name);
    const reservation = await downloadJson(blob);
    if (!isRiskReservationResolved(reservation)) count += 1;
  }
  return count;
}

export async function uploadEvidence(config, runId, summary, ledger) {
  const date = new Date().toISOString().slice(0, 10);
  const prefix = `reports/research/venue-probe/runs/${date}/${runId}`;
  const safeSummary = sanitize(summary);
  const safeEventsJsonl = ledger.jsonl();
  if (config.outputDir) {
    const { mkdir, writeFile } = await import("node:fs/promises");
    await mkdir(config.outputDir, { recursive: true, mode: 0o700 });
    await writeFile(`${config.outputDir}/${runId}-summary.json`, JSON.stringify(safeSummary, null, 2), { mode: 0o600 });
    await writeFile(`${config.outputDir}/${runId}-events.jsonl`, safeEventsJsonl, { mode: 0o600 });
  }
  const container = storageContainer(config);
  if (!container) return { uploaded: false, prefix: null };
  await container.createIfNotExists();
  await uploadImmutable(container, `${prefix}/events.jsonl`, safeEventsJsonl, "application/x-ndjson");
  await uploadImmutable(container, `${prefix}/summary.json`, JSON.stringify(safeSummary, null, 2), "application/json");
  const payload = Buffer.from(JSON.stringify(safeSummary, null, 2));
  await container
    .getBlockBlobClient("reports/research/venue-probe/latest_attempt.json")
    .uploadData(payload, { blobHTTPHeaders: { blobContentType: "application/json" } });
  if (["completed", "campaign_completed", "campaign_stopped_safely"].includes(summary.status) && summary.order_submitted === true) {
    await container
      .getBlockBlobClient("reports/research/venue-probe/latest.json")
      .uploadData(payload, { blobHTTPHeaders: { blobContentType: "application/json" } });
  } else if (summary.status === "auth_validated_no_order") {
    await container
      .getBlockBlobClient("reports/research/venue-probe/latest_authenticated_dry_run.json")
      .uploadData(payload, { blobHTTPHeaders: { blobContentType: "application/json" } });
  }
  return { uploaded: true, prefix };
}

export async function loadProbeObservations(config) {
  const container = storageContainer(config);
  if (!container) return [];
  const observations = [];
  const observedProbeIds = new Set();
  for await (const blob of container.listBlobsFlat({ prefix: "reports/research/venue-probe/runs/" })) {
    if (!blob.name.endsWith("/summary.json")) continue;
    const response = await container.getBlobClient(blob.name).download();
    const bytes = await streamToBuffer(response.readableStreamBody);
    const summaryHash = digest(bytes);
    const summary = JSON.parse(bytes.toString("utf8"));
    const protocolEligible = isEvidenceProtocolVersionEligible(summary.evidence_protocol_version);
    const probes = Array.isArray(summary.probes) ? summary.probes : [summary];
    for (const probe of probes) {
      const probeRows = probe.model_observations || [];
      if (probe.probe_id && probeRows.length > 0) observedProbeIds.add(String(probe.probe_id));
      for (const row of probeRows) {
        const normalized = normalizeStoredObservation(row, probe);
        observations.push({
          ...normalized,
          eligible: normalized.eligible === true && protocolEligible,
          quality_eligible: normalized.quality_eligible === true && protocolEligible,
          protocol_eligible: protocolEligible,
          evidence_protocol_version: Number(summary.evidence_protocol_version || 0),
          recorded_ts: probe.finished_ts || summary.finished_ts,
          run_id: summary.run_id,
          probe_id: probe.probe_id || null,
          order_id: probe.lifecycle?.order_id || null,
          source_summary_blob_name: blob.name,
          source_summary_sha256: summaryHash
        });
      }
    }
  }
  for await (const blob of container.listBlobsFlat({ prefix: "reports/research/venue-probe/risk-reservations/" })) {
    if (!blob.name.endsWith(".json")) continue;
    const response = await container.getBlobClient(blob.name).download();
    const reservation = JSON.parse(await streamToString(response.readableStreamBody));
    if (!reservation?.probe_id || observedProbeIds.has(String(reservation.probe_id))) continue;
    const auditObservation = reservationAuditObservation(reservation);
    if (auditObservation) observations.push(auditObservation);
  }
  return observations;
}

/**
 * Loads the exact protocol-v3 order set bound by the checkpoint-100 artifact.
 * The model job never scans a mutable prefix when producing the stage-200
 * model: every source summary is named and SHA-bound by the checkpoint.
 */
export async function loadCheckpointProbeObservations(config, checkpointBlobName, checkpointSha256) {
  const container = storageContainer(config);
  if (!container) throw new Error("fail closed: durable Azure storage is required for queue-model training");
  const checkpoint = await downloadExactJson(container, checkpointBlobName, checkpointSha256, "checkpoint-100");
  const bindings = checkpoint.value?.protocol_v3_order_artifacts;
  if (checkpoint.value?.schema_version !== "funded_checkpoint_evidence_v1" ||
      Number(checkpoint.value?.evidence_protocol_version) !== EVIDENCE_PROTOCOL_VERSION ||
      Number(checkpoint.value?.stage_target_orders) !== 100 ||
      Number(checkpoint.value?.exact_funded_order_count) !== 100 ||
      Number(checkpoint.value?.exact_eligible_order_count) !== 100 ||
      !Array.isArray(bindings) || bindings.length !== 100) {
    throw new Error("fail closed: queue-model training requires the exact canonical checkpoint-100 evidence set");
  }
  const observations = [];
  const identities = new Set();
  for (const binding of bindings) {
    const summary = await downloadExactJson(container, binding?.blob_name, binding?.sha256, "protocol-v3 training summary");
    const value = summary.value;
    const probes = Array.isArray(value?.probes) ? value.probes : [];
    const probe = probes[0];
    const identity = `${value?.run_id || ""}\u0000${probe?.probe_id || ""}\u0000${probe?.lifecycle?.order_id || ""}`;
    if (value?.schema_version !== 3 || Number(value?.evidence_protocol_version) !== EVIDENCE_PROTOCOL_VERSION ||
        value?.order_submission_attempted !== true || Number(value?.submitted_order_count) !== 1 || probes.length !== 1 ||
        !value?.run_id || !probe?.probe_id || !probe?.lifecycle?.order_id || identities.has(identity) ||
        JSON.stringify(value?.candidate) !== JSON.stringify(checkpoint.value?.candidate)) {
      throw new Error("fail closed: checkpoint-100 contains invalid, duplicated, or candidate-mismatched protocol-v3 evidence");
    }
    identities.add(identity);
    for (const row of probe.model_observations || []) {
      const normalized = normalizeStoredObservation(row, probe);
      observations.push({
        ...normalized,
        eligible: normalized.eligible === true,
        quality_eligible: normalized.quality_eligible === true,
        protocol_eligible: true,
        evidence_protocol_version: EVIDENCE_PROTOCOL_VERSION,
        recorded_ts: probe.finished_ts || value.finished_ts,
        run_id: value.run_id,
        probe_id: probe.probe_id,
        order_id: probe.lifecycle.order_id,
        source_summary_blob_name: summary.blobName,
        source_summary_sha256: summary.hash
      });
    }
  }
  return {
    checkpoint: { blob_name: checkpoint.blobName, sha256: checkpoint.hash },
    candidate: checkpoint.value.candidate,
    observations
  };
}

export function reservationAuditObservation(reservation) {
  if (!reservation?.probe_id) return null;
  if (String(reservation.state) === "released_no_order" && reservation.order_submitted === false) return null;
  const protocolEligible = isEvidenceProtocolVersionEligible(reservation.evidence_protocol_version);
  return {
    probe_id: String(reservation.probe_id),
    run_id: reservation.run_id || null,
    recorded_ts: reservation.updated_ts || reservation.created_ts || "",
    evidence_protocol_version: Number(reservation.evidence_protocol_version || 0),
    protocol_eligible: protocolEligible,
    observation_source: "risk_reservation_manifest",
    order_submitted: reservation.order_submitted !== false && reservation.order_submission_intended === true,
    eligible: false,
    quality_eligible: false,
    label_observed: false,
    filled: number(reservation.matched_notional, 0) > 0,
    reconciliation_complete: false,
    zero_open_orders_confirmed: reservation.zero_open_orders_confirmed === true,
    data_gap_detected: true,
    cancellation_failure: reservation.zero_open_orders_confirmed !== true,
    markout_complete: false,
    markout_timing_valid: false,
    reservation_state: reservation.state || "unknown"
  };
}

export async function loadDailyCampaignRisk(config, date = new Date().toISOString().slice(0, 10)) {
  const container = storageContainer(config);
  if (!container) return { date, conservative_loss_budget_consumed: 0, submitted_orders: 0, filled_orders: 0 };
  const reservations = [];
  for await (const blob of container.listBlobsFlat({ prefix: `reports/research/venue-probe/risk-reservations/${date}/` })) {
    if (!blob.name.endsWith(".json")) continue;
    const response = await container.getBlobClient(blob.name).download();
    reservations.push(JSON.parse(await streamToString(response.readableStreamBody)));
  }
  const summaries = [];
  const prefix = `reports/research/venue-probe/runs/${date}/`;
  for await (const blob of container.listBlobsFlat({ prefix })) {
    if (!blob.name.endsWith("/summary.json")) continue;
    const response = await container.getBlobClient(blob.name).download();
    summaries.push(JSON.parse(await streamToString(response.readableStreamBody)));
  }
  return summarizeDailyRiskRecords(date, reservations, summaries);
}

export async function loadUnresolvedRiskReservations(config) {
  const container = storageContainer(config);
  if (!container) return [];
  const unresolved = [];
  for await (const blob of container.listBlobsFlat({ prefix: "reports/research/venue-probe/risk-reservations/" })) {
    if (!blob.name.endsWith(".json")) continue;
    const response = await container.getBlobClient(blob.name).download();
    const reservation = JSON.parse(await streamToString(response.readableStreamBody));
    if (!isRiskReservationResolved(reservation)) unresolved.push(reservation);
  }
  return unresolved;
}

export function summarizeDailyRiskRecords(date, reservations, summaries) {
  let consumed = 0;
  let submittedOrders = 0;
  let filledOrders = 0;
  let unresolvedReservations = 0;
  const reservationProbeIds = new Set();
  for (const reservation of reservations || []) {
    if (!reservation?.probe_id) continue;
    reservationProbeIds.add(String(reservation.probe_id));
    const finalized = isRiskReservationResolved(reservation);
    const matched = Math.max(0, number(reservation.matched_notional, 0));
    const reserved = Math.max(0, number(reservation.reserved_notional, 0));
    consumed += finalized ? matched : reserved;
    if (reservation.order_submission_intended === true) submittedOrders += 1;
    if (matched > 0) filledOrders += 1;
    if (!finalized) unresolvedReservations += 1;
  }
  for (const summary of summaries || []) {
    const probes = Array.isArray(summary.probes) ? summary.probes : [summary];
    for (const probe of probes) {
      if (probe.probe_id && reservationProbeIds.has(String(probe.probe_id))) continue;
      if (probe.order_submitted) submittedOrders += 1;
      const matched = number(probe.lifecycle?.actual_matched_size, 0);
      const price = number(probe.order?.price, 0);
      if (matched > 0) filledOrders += 1;
      consumed += Math.max(0, matched * price);
    }
  }
  return {
    date,
    conservative_loss_budget_consumed: consumed,
    submitted_orders: submittedOrders,
    filled_orders: filledOrders,
    unresolved_risk_reservations: unresolvedReservations
  };
}

export function isRiskReservationResolved(reservation) {
  const finalizedNoFill = ["finalized", "finalized_no_fill"].includes(String(reservation?.state)) &&
    number(reservation?.matched_notional, 0) <= 0 &&
    reservation?.reconciliation_complete === true &&
    reservation?.zero_open_orders_confirmed === true;
  const settledFill = String(reservation?.state) === "position_settled" &&
    number(reservation?.matched_notional, 0) > 0 &&
    reservation?.settlement_verified === true &&
    ["verified_onchain_redemption", "polymarket_data_api_redeemable"].includes(reservation?.settlement_evidence_source);
  const releasedNoOrder = String(reservation?.state) === "released_no_order" &&
    reservation?.order_submitted === false &&
    reservation?.reconciliation_complete === true &&
    reservation?.zero_open_orders_confirmed === true;
  return finalizedNoFill || settledFill || releasedNoOrder;
}

export function buildQueueCalibrationArtifact(model, observations, {
  generatedAt = new Date(),
  checkpoint,
  candidate
} = {}) {
  if (model?.model_version !== "queue-calibration-v1" || model?.status !== "trained_research_only" || Number(model?.sample_size) !== 100) {
    throw new Error("fail closed: checkpoint transition requires a trained queue-calibration-v1 model from exactly 100 orders");
  }
  if (!checkpoint?.blob_name || !validDigest(checkpoint?.sha256)) {
    throw new Error("fail closed: exact checkpoint-100 artifact binding is required");
  }
  const orders = modelTrainingOrders(observations);
  if (orders.length !== 100) throw new Error("fail closed: model training provenance must contain exactly 100 unique orders");
  const datasetBytes = Buffer.from(JSON.stringify(orders));
  const datasetHash = digest(datasetBytes);
  const generated = generatedAt instanceof Date ? generatedAt : new Date(generatedAt);
  if (!Number.isFinite(generated.getTime())) throw new Error("fail closed: model generated_at is invalid");
  const payload = {
    schema: "polyedge.execution_queue_model.v1",
    generated_at: generated.toISOString(),
    candidate,
    training_checkpoint: checkpoint,
    training_cutoff: orders.at(-1).observed_at,
    training_dataset: {
      schema: "polyedge.queue_calibration_training_dataset.v1",
      exact_order_count: orders.length,
      sha256: datasetHash,
      orders
    },
    training_horizon_base_rates: Object.fromEntries(HORIZONS_SECONDS.map((horizon) => {
      const rows = (observations || []).filter((row) => row.eligible === true && Number(row.horizon_seconds) === horizon);
      return [String(horizon), rows.length ? average(rows.map((row) => row.filled ? 1 : 0)) : 0];
    })),
    ...model
  };
  const bytes = Buffer.from(JSON.stringify(payload, null, 2));
  const hash = digest(bytes);
  return {
    value: payload,
    bytes,
    hash,
    blobName: `reports/research/venue-probe/models/queue-calibration-v1/${hash.slice("sha256:".length)}.json`
  };
}

export async function uploadModel(config, model, provenance) {
  const container = storageContainer(config);
  if (!container) return false;
  const artifact = buildQueueCalibrationArtifact(model, provenance?.observations, provenance);
  await uploadImmutableExact(container, artifact.blobName, artifact.bytes, "application/json");
  const pointer = {
    schema: "polyedge.execution_queue_model_pointer.v1",
    model_version: model.model_version,
    blob_uri: queueModelArtifactUri(config, artifact),
    sha256: artifact.hash,
    generated_at: artifact.value.generated_at,
    training_cutoff: artifact.value.training_cutoff,
    training_dataset_sha256: artifact.value.training_dataset.sha256,
    training_checkpoint: artifact.value.training_checkpoint,
    promotion_allowed: false
  };
  await container
    .getBlockBlobClient("reports/research/venue-probe/effective_queue_model.json")
    .uploadData(Buffer.from(JSON.stringify(pointer, null, 2)), { blobHTTPHeaders: { blobContentType: "application/json" } });
  return { blobName: artifact.blobName, hash: artifact.hash, pointer };
}

export function queueModelArtifactUri(config, artifact) {
  if (!config?.storageAccount || !config?.storageContainer || !artifact?.blobName) {
    throw new Error("fail closed: model Azure URI requires account, output container, and immutable blob");
  }
  return `azure://${config.storageAccount}/${config.storageContainer}/${artifact.blobName}`;
}

function modelTrainingOrders(observations) {
  const orders = new Map();
  for (const row of observations || []) {
    if (row.eligible !== true) continue;
    const record = {
      run_id: String(row.run_id || ""),
      probe_id: String(row.probe_id || ""),
      order_id: String(row.order_id || ""),
      observed_at: String(row.recorded_ts || ""),
      summary_blob_name: String(row.source_summary_blob_name || ""),
      summary_sha256: normalizeDigest(row.source_summary_sha256)
    };
    if (!record.run_id || !record.probe_id || !record.order_id || !record.observed_at || !record.summary_blob_name || !record.summary_sha256) {
      throw new Error("fail closed: model observation lacks exact order/source provenance");
    }
    const key = `${record.run_id}\u0000${record.probe_id}\u0000${record.order_id}`;
    const prior = orders.get(key);
    if (prior && JSON.stringify(prior) !== JSON.stringify(record)) throw new Error("fail closed: model order provenance is inconsistent across horizon rows");
    orders.set(key, record);
  }
  return [...orders.values()].sort((left, right) => left.observed_at.localeCompare(right.observed_at) || left.run_id.localeCompare(right.run_id) || left.probe_id.localeCompare(right.probe_id));
}

async function downloadExactJson(container, blobName, expectedSha256, label) {
  if (!blobName || !validDigest(expectedSha256)) throw new Error(`fail closed: exact ${label} binding is required`);
  const response = await container.getBlobClient(blobName).download();
  const bytes = await streamToBuffer(response.readableStreamBody);
  const actual = digest(bytes);
  if (actual !== normalizeDigest(expectedSha256)) throw new Error(`fail closed: ${label} SHA-256 mismatch`);
  return { value: JSON.parse(bytes.toString("utf8")), blobName, hash: actual };
}

async function uploadImmutableExact(container, name, bytes, contentType) {
  try {
    await container.getBlockBlobClient(name).uploadData(bytes, {
      conditions: { ifNoneMatch: "*" },
      blobHTTPHeaders: { blobContentType: contentType }
    });
  } catch (error) {
    if (![409, 412].includes(Number(error.statusCode))) throw error;
    const existing = await container.getBlobClient(name).download();
    const existingBytes = await streamToBuffer(existing.readableStreamBody);
    if (!existingBytes.equals(bytes)) throw new Error("fail closed: immutable queue-model content-address collision");
  }
}

function digest(bytes) { return `sha256:${createHash("sha256").update(bytes).digest("hex")}`; }
function normalizeDigest(value) { const text = String(value || "").trim().toLowerCase(); const prefixed = text.startsWith("sha256:") ? text : `sha256:${text}`; return /^sha256:[0-9a-f]{64}$/.test(prefixed) ? prefixed : ""; }
function validDigest(value) { return Boolean(normalizeDigest(value)); }

async function uploadImmutable(container, name, content, contentType) {
  await container.getBlockBlobClient(name).uploadData(Buffer.from(content), {
    conditions: { ifNoneMatch: "*" },
    blobHTTPHeaders: { blobContentType: contentType }
  });
}

async function downloadJson(blob) {
  const response = await blob.download();
  return JSON.parse(await streamToString(response.readableStreamBody));
}

async function streamToString(stream) {
  const chunks = [];
  for await (const chunk of stream) chunks.push(Buffer.from(chunk));
  return Buffer.concat(chunks).toString("utf8");
}

async function streamToBuffer(stream) {
  const chunks = [];
  for await (const chunk of stream) chunks.push(Buffer.from(chunk));
  return Buffer.concat(chunks);
}

function fitLogisticRows(train, featureCount, iterations = 1000) {
  const normalization = featureNormalization(train);
  const rows = train.map((row) => normalizedFeatures(row, normalization));
  let weights = Array(featureCount).fill(0);
  const rate = 0.08;
  const regularization = 0.002;
  for (let iteration = 0; iteration < iterations; iteration += 1) {
    const gradient = Array(weights.length).fill(0);
    for (let i = 0; i < rows.length; i += 1) {
      const prediction = sigmoid(dot(weights, rows[i]));
      const error = prediction - (train[i].filled ? 1 : 0);
      for (let j = 0; j < weights.length; j += 1) gradient[j] += error * rows[i][j];
    }
    for (let j = 0; j < weights.length; j += 1) {
      const penalty = j === 0 ? 0 : regularization * weights[j];
      weights[j] -= rate * (gradient[j] / rows.length + penalty);
    }
  }
  return { normalization, weights };
}

function groupedTemporalValidation(groups, featureCount) {
  const firstTest = Math.min(groups.length - 1, Math.max(20, Math.floor(groups.length * 0.6)));
  const foldSize = Math.max(1, Math.floor(groups.length * 0.1));
  const rows = [];
  const predictions = [];
  const naivePredictions = [];
  const folds = [];
  const testedProbeIds = new Set();
  for (let start = firstTest; start < groups.length; start += foldSize) {
    const trainGroups = groups.slice(0, start);
    const testGroups = groups.slice(start, Math.min(groups.length, start + foldSize));
    const train = trainGroups.flatMap((group) => group.rows.filter((row) => row.eligible));
    const test = testGroups.flatMap((group) => group.rows.filter((row) => row.eligible));
    if (!train.length || !test.length) continue;
    const fitted = fitLogisticRows(train, featureCount, 1000);
    predictions.push(...test.map((row) => sigmoid(dot(fitted.weights, normalizedFeatures(row, fitted.normalization)))));
    naivePredictions.push(...naiveHorizonPredictions(train, test));
    rows.push(...test);
    testGroups.forEach((group) => testedProbeIds.add(group.key));
    folds.push({
      train_probe_count: trainGroups.length,
      test_probe_count: testGroups.length,
      train_label_count: train.length,
      test_label_count: test.length,
      train_through_ts: trainGroups.at(-1)?.recorded_ts || null,
      test_from_ts: testGroups[0]?.recorded_ts || null,
      test_through_ts: testGroups.at(-1)?.recorded_ts || null
    });
  }
  return { rows, predictions, naive_predictions: naivePredictions, folds, probe_count: testedProbeIds.size };
}

function naiveHorizonPredictions(train, test) {
  const overall = average(train.map((row) => row.filled ? 1 : 0));
  const rates = new Map(HORIZONS_SECONDS.map((horizon) => {
    const rows = train.filter((row) => Number(row.horizon_seconds) === horizon);
    return [horizon, rows.length ? average(rows.map((row) => row.filled ? 1 : 0)) : overall];
  }));
  return test.map((row) => rates.get(Number(row.horizon_seconds)) ?? overall);
}

function brierScore(predictions, rows) {
  return average(predictions.map((prediction, index) => (prediction - (rows[index].filled ? 1 : 0)) ** 2));
}

function expectedCalibrationError(bins, sampleSize) {
  if (!sampleSize) return 1;
  return bins.reduce((total, bin) => total +
    (Number(bin.count) / sampleSize) * Math.abs(Number(bin.mean_prediction) - Number(bin.observed_fill_rate)), 0);
}

function lowerConfidenceBound95(values) {
  if (!values.length) return Number.NEGATIVE_INFINITY;
  const mean = average(values);
  if (values.length < 2) return Number.NEGATIVE_INFINITY;
  const variance = values.reduce((sum, value) => sum + (value - mean) ** 2, 0) / (values.length - 1);
  return mean - 1.96 * Math.sqrt(variance / values.length);
}

function featureNormalization(rows) {
  const raw = rows.map(rawFeatures);
  const means = raw[0].map((_, index) => average(raw.map((row) => row[index])));
  const scales = raw[0].map((_, index) => {
    const mean = means[index];
    const variance = average(raw.map((row) => (row[index] - mean) ** 2));
    return Math.sqrt(variance) || 1;
  });
  means[0] = 0;
  scales[0] = 1;
  return { means, scales };
}

function rawFeatures(row) {
  return [
    1,
    Math.log1p(number(row.inferred_size_ahead, 0)),
    number(row.spread, 0),
    number(row.order_price, 0),
    number(row.order_size, 0),
    Math.log1p(number(row.time_to_expiry_seconds, 0)),
    Math.log1p(number(row.pre_send_trade_size, 0)),
    number(row.pre_send_depth_changes, 0),
    number(row.pre_send_volatility, 0),
    number(row.horizon_seconds, 0)
  ];
}

function normalizedFeatures(row, normalization) {
  return rawFeatures(row).map((value, index) =>
    index === 0 ? 1 : (value - normalization.means[index]) / normalization.scales[index]
  );
}

function calibrationBins(predictions, rows) {
  return Array.from({ length: 10 }, (_, index) => {
    const low = index / 10;
    const high = (index + 1) / 10;
    const values = predictions
      .map((prediction, rowIndex) => ({ prediction, actual: rows[rowIndex].filled ? 1 : 0 }))
      .filter(({ prediction }) => prediction >= low && (index === 9 ? prediction <= high : prediction < high));
    return {
      low,
      high,
      count: values.length,
      mean_prediction: average(values.map((value) => value.prediction)),
      observed_fill_rate: average(values.map((value) => value.actual))
    };
  });
}

function calibrationSafeNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function level(value) {
  const price = calibrationSafeNumber(value.price);
  const size = calibrationSafeNumber(value.size);
  return Number.isFinite(price) && Number.isFinite(size) ? { price, size } : null;
}

function roundedTick(value, tick) {
  const decimals = String(tick).split(".")[1]?.length || 0;
  return Number((Math.floor((value + 1e-12) / tick) * tick).toFixed(decimals));
}

function nearlyEqual(left, right) {
  return Math.abs(left - right) < 1e-9;
}

function parseBoolean(value) {
  return String(value).toLowerCase() === "true";
}

function parseNumberList(value) {
  return [...new Set(String(value).split(",").map((item) => Number.parseInt(item.trim(), 10)).filter(Number.isFinite))];
}

function parseCampaignCashFlows(value) {
  let rows;
  try {
    rows = JSON.parse(String(value));
  } catch {
    throw new Error("venue_probe blocked: VENUE_PROBE_CAMPAIGN_CASH_FLOWS must be valid JSON");
  }
  if (!Array.isArray(rows)) throw new Error("venue_probe blocked: VENUE_PROBE_CAMPAIGN_CASH_FLOWS must be a JSON array");
  const ids = new Set();
  return rows.map((row) => {
    const id = String(row?.id || "").trim();
    const amount = Number(row?.amount);
    const transactionHash = String(row?.transaction_hash || "").trim();
    if (!/^[a-zA-Z0-9][a-zA-Z0-9._-]{0,79}$/.test(id) || ids.has(id)) {
      throw new Error("venue_probe blocked: campaign cash-flow ids must be unique safe identifiers");
    }
    if (!Number.isFinite(amount) || amount === 0) {
      throw new Error(`venue_probe blocked: campaign cash-flow ${id} must have a non-zero finite amount`);
    }
    if (!/^0x[a-fA-F0-9]{64}$/.test(transactionHash)) {
      throw new Error(`venue_probe blocked: campaign cash-flow ${id} requires a transaction_hash`);
    }
    ids.add(id);
    return { id, amount: roundMoney(amount), transaction_hash: transactionHash };
  });
}

function integer(value, fallback) {
  const parsed = Number.parseInt(value ?? "", 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function number(value, fallback) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function monotonicMs() {
  return Number(process.hrtime.bigint()) / 1_000_000;
}

function roundMoney(value) {
  return Math.round((Number(value) + Number.EPSILON) * 1_000_000) / 1_000_000;
}

function average(values) {
  return values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : 0;
}

function dot(left, right) {
  return left.reduce((sum, value, index) => sum + value * right[index], 0);
}

function sigmoid(value) {
  if (value >= 0) return 1 / (1 + Math.exp(-value));
  const exp = Math.exp(value);
  return exp / (1 + exp);
}
