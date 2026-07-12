import { DefaultAzureCredential } from "@azure/identity";
import {
  BlobServiceClient,
  StorageSharedKeyCredential
} from "@azure/storage-blob";

export const HORIZONS_SECONDS = [1, 5, 30, 60];
export const MARKOUT_HORIZONS_SECONDS = [1, 5, 30];
export const EVIDENCE_PROTOCOL_VERSION = 3;
export const MAX_MARKOUT_OBSERVATION_DELAY_MS = 2000;

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
    maxOpenOrders: integer(env.MAX_OPEN_ORDERS, 1),
    maxOrderNotional: number(env.VENUE_PROBE_MAX_ORDER_NOTIONAL, 2),
    minOrderNotional: number(env.VENUE_PROBE_MIN_ORDER_NOTIONAL, 1),
    minOrderPrice: number(env.VENUE_PROBE_MIN_ORDER_PRICE, 0.05),
    maxDailyLoss: number(env.MAX_DAILY_LOSS, 5),
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
  if (!(config.maxOrderNotional > 0 && config.maxOrderNotional <= 2)) {
    errors.push("VENUE_PROBE_MAX_ORDER_NOTIONAL must be in (0, 2]");
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
  return HORIZONS_SECONDS.map((horizon) => {
    const liveSeconds = lifecycle.live_duration_ms / 1000;
    const filledAtMs = lifecycle.first_fill_after_ack_ms;
    const filled = filledAtMs !== null && filledAtMs <= horizon * 1000;
    const labelObserved = liveSeconds >= horizon || filled;
    const qualityEligible = lifecycle.reconciliation_complete === true &&
      lifecycle.zero_open_orders_confirmed === true &&
      lifecycle.data_gap_detected !== true &&
      lifecycle.cancellation_failure !== true &&
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
      executable_markout_30s_per_share: markout30
    };
  });
}

export function normalizeStoredObservation(row, probe) {
  const probeFilled = Number(probe?.lifecycle?.actual_matched_size || 0) > 0 ||
    (probe?.model_observations || []).some((candidate) => candidate.filled === true);
  if (!probeFilled) return { ...row, markout_timing_valid: row.markout_timing_valid !== false };
  const markouts = Array.isArray(probe?.markouts) ? probe.markouts : [];
  const coverage = validateFillMarkouts(markouts, probe?.lifecycle?.related_trade_ids || [], probe?.lifecycle?.actual_matched_size || 0);
  const timingValid = coverage.timing_valid;
  return {
    ...row,
    eligible: row.eligible === true && timingValid,
    quality_eligible: row.quality_eligible === true && timingValid,
    markout_complete: row.markout_complete === true && coverage.complete,
    markout_timing_valid: timingValid
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
  if (eligibleGroups.length < minimumSamples || positives < 10 || negatives < 10) {
    return {
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
  const normalization = featureNormalization(train);
  const rows = train.map((row) => normalizedFeatures(row, normalization));
  let weights = Array(featureNames.length).fill(0);
  const rate = 0.08;
  const regularization = 0.002;
  for (let iteration = 0; iteration < 2500; iteration += 1) {
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
  const predictions = test.map((row) => sigmoid(dot(weights, normalizedFeatures(row, normalization))));
  const brier = average(predictions.map((prediction, index) => (prediction - (test[index].filled ? 1 : 0)) ** 2));
  const horizonMetrics = Object.fromEntries(HORIZONS_SECONDS.map((horizon) => {
    const indexes = test.map((row, index) => ({ row, index })).filter(({ row }) => Number(row.horizon_seconds) === horizon);
    const horizonPredictions = indexes.map(({ index }) => predictions[index]);
    const horizonRows = indexes.map(({ row }) => row);
    return [String(horizon), {
      sample_size: horizonRows.length,
      positive_fills: horizonRows.filter((row) => row.filled).length,
      brier_score: average(horizonPredictions.map((prediction, index) => (prediction - (horizonRows[index].filled ? 1 : 0)) ** 2)),
      calibration_bins: calibrationBins(horizonPredictions, horizonRows)
    }];
  }));
  const qualityGates = modelQualityGates(observations);
  const netMarkouts = eligibleGroups.flatMap((group) => {
    const row = group.rows.find((candidate) => candidate.eligible && candidate.filled && Number.isFinite(Number(candidate.executable_markout_30s_per_share)));
    return row ? [Number(row.executable_markout_30s_per_share) - number(row.estimated_round_trip_cost_per_share, 0)] : [];
  });
  const meanNetMarkout = average(netMarkouts);
  const promotionReady = qualityGates.passed && netMarkouts.length >= 10 && meanNetMarkout > 0;
  return {
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
    feature_names: featureNames,
    weights,
    normalization,
    out_of_sample_brier_score: brier,
    calibration_bins: calibrationBins(predictions, test),
    horizon_metrics: horizonMetrics,
    quality_gates: qualityGates,
    net_markout_30s_sample_size: netMarkouts.length,
    mean_net_executable_markout_30s_per_share: meanNetMarkout,
    promotion_ready: promotionReady,
    promotion_allowed: false,
    promotion_block_reason: promotionReady
      ? "research gates passed; explicit human strategy approval is still required"
      : "requires complete data quality and positive 30-second executable markouts after costs",
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

export async function uploadEvidence(config, runId, summary, ledger) {
  const date = new Date().toISOString().slice(0, 10);
  const prefix = `reports/research/venue-probe/runs/${date}/${runId}`;
  if (config.outputDir) {
    const { mkdir, writeFile } = await import("node:fs/promises");
    await mkdir(config.outputDir, { recursive: true, mode: 0o700 });
    await writeFile(`${config.outputDir}/${runId}-summary.json`, JSON.stringify(summary, null, 2), { mode: 0o600 });
    await writeFile(`${config.outputDir}/${runId}-events.jsonl`, ledger.jsonl(), { mode: 0o600 });
  }
  const container = storageContainer(config);
  if (!container) return { uploaded: false, prefix: null };
  await container.createIfNotExists();
  await uploadImmutable(container, `${prefix}/events.jsonl`, ledger.jsonl(), "application/x-ndjson");
  await uploadImmutable(container, `${prefix}/summary.json`, JSON.stringify(summary, null, 2), "application/json");
  const payload = Buffer.from(JSON.stringify(summary, null, 2));
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
    const text = await streamToString(response.readableStreamBody);
    const summary = JSON.parse(text);
    const protocolEligible = Number(summary.evidence_protocol_version || 0) >= EVIDENCE_PROTOCOL_VERSION;
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
          estimated_round_trip_cost_per_share: summary.estimated_round_trip_cost_per_share || 0
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

export function reservationAuditObservation(reservation) {
  if (!reservation?.probe_id) return null;
  if (String(reservation.state) === "released_no_order" && reservation.order_submitted === false) return null;
  const protocolEligible = Number(reservation.evidence_protocol_version || 0) >= EVIDENCE_PROTOCOL_VERSION;
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
    if (finalized && matched > 0) filledOrders += 1;
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
  const finalizedFill = String(reservation?.state) === "finalized" &&
    reservation?.reconciliation_complete === true &&
    reservation?.zero_open_orders_confirmed === true;
  const releasedNoOrder = String(reservation?.state) === "released_no_order" &&
    reservation?.order_submitted === false &&
    reservation?.reconciliation_complete === true &&
    reservation?.zero_open_orders_confirmed === true;
  return finalizedFill || releasedNoOrder;
}

export async function uploadModel(config, model) {
  const container = storageContainer(config);
  if (!container) return false;
  const payload = JSON.stringify({ generated_at: new Date().toISOString(), ...model }, null, 2);
  await container
    .getBlockBlobClient("reports/research/venue-probe/effective_queue_model.json")
    .uploadData(Buffer.from(payload), { blobHTTPHeaders: { blobContentType: "application/json" } });
  return true;
}

async function uploadImmutable(container, name, content, contentType) {
  await container.getBlockBlobClient(name).uploadData(Buffer.from(content), {
    conditions: { ifNoneMatch: "*" },
    blobHTTPHeaders: { blobContentType: contentType }
  });
}

async function streamToString(stream) {
  const chunks = [];
  for await (const chunk of stream) chunks.push(Buffer.from(chunk));
  return Buffer.concat(chunks).toString("utf8");
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
