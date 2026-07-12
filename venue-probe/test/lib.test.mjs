import test from "node:test";
import assert from "node:assert/strict";
import {
  assertEligibleOrigin,
  campaignRestSchedule,
  evaluateDailyRiskGate,
  fitEffectiveQueueModel,
  isTransientUnsafeMarket,
  isRiskReservationResolved,
  loadProbeConfig,
  modelObservations,
  normalizeStoredObservation,
  reservationAuditObservation,
  sanitize,
  selectMakerOrder,
  summarizeDailyRiskRecords,
  validateFillMarkouts,
  summarizePortfolio
} from "../src/lib.mjs";

const safeEnv = {
  EXECUTION_MODE: "venue_probe",
  ALLOW_LIVE: "false",
  ALLOW_VENUE_PROBE: "true",
  ENABLE_TAKER_ORDERS: "false",
  MAX_OPEN_ORDERS: "1",
  VENUE_PROBE_CAMPAIGN_ENABLED: "true",
  VENUE_PROBE_MAXIMUM_ORDERS: "25",
  VENUE_PROBE_MAX_ORDER_NOTIONAL: "2",
  MAX_DAILY_LOSS: "5",
  POLYMARKET_PRIVATE_KEY: "key",
  POLYMARKET_API_KEY: "api",
  POLYMARKET_API_SECRET: "secret",
  POLYMARKET_API_PASSPHRASE: "pass",
  POLYMARKET_FUNDER_ADDRESS: "0x123",
  AZURE_STORAGE_ACCOUNT_NAME: "storage"
};

test("safe venue probe gates load", () => {
  const config = loadProbeConfig(safeEnv);
  assert.equal(config.allowLive, false);
  assert.equal(config.maxOpenOrders, 1);
  assert.equal(config.maximumOrders, 25);
  assert.equal(config.maxOrderNotional, 2);
});

test("exhausted daily risk blocks submissions but still permits no-order diagnostics", () => {
  assert.deepEqual(evaluateDailyRiskGate(5, 5, true), {
    loss_limits_ok: false,
    diagnostics_only: true,
    submission_allowed: false
  });
  assert.throws(() => evaluateDailyRiskGate(5, 5, false), /daily loss budget is already exhausted/);
  assert.deepEqual(evaluateDailyRiskGate(4, 5, false), {
    loss_limits_ok: true,
    diagnostics_only: false,
    submission_allowed: true
  });
});

test("portfolio accounting distinguishes gross payout from true account profit", () => {
  const portfolio = summarizePortfolio([
    { initialValue: 2.3504, currentValue: 0, redeemable: true },
    { initialValue: 1.3, currentValue: 5, redeemable: true },
    { initialValue: 1.3487, currentValue: 0, redeemable: true }
  ], 4.230721, 9.23);
  assert.equal(portfolio.gross_redeemable_value, 5);
  assert.equal(portfolio.resolved_losing_cost, 3.6991);
  assert.equal(portfolio.account_equity, 9.230721);
  assert.equal(portfolio.account_net_pnl, 0.000721);
  assert.equal(portfolio.gross_payout_is_profit, false);
});

test("live cloud probe requires and verifies fixed country and egress IP", () => {
  assert.throws(() => loadProbeConfig({ ...safeEnv, VENUE_PROBE_DRY_RUN: "false" }), /EXPECTED_COUNTRY/);
  const config = loadProbeConfig({
    ...safeEnv,
    VENUE_PROBE_DRY_RUN: "false",
    VENUE_PROBE_EXPECTED_COUNTRY: "IE",
    VENUE_PROBE_EXPECTED_EGRESS_IP: "203.0.113.8"
  });
  assert.equal(assertEligibleOrigin({ blocked: false, country: "IE", ip: "203.0.113.8" }, config), true);
  assert.throws(() => assertEligibleOrigin({ blocked: false, country: "US", ip: "203.0.113.8" }, config), /country/);
  assert.throws(() => assertEligibleOrigin({ blocked: false, country: "IE", ip: "203.0.113.9" }, config), /static IP/);
});

test("campaign schedule is deterministic and covers all required resting horizons", () => {
  const schedule = campaignRestSchedule(25, [1, 5, 30, 60], "run-1");
  assert.equal(schedule.length, 25);
  assert.deepEqual([...new Set(schedule)].sort((a, b) => a - b), [1, 5, 30, 60]);
  assert.deepEqual(schedule, campaignRestSchedule(25, [1, 5, 30, 60], "run-1"));
});

test("venue probe rejects live or taker configuration", () => {
  assert.throws(() => loadProbeConfig({ ...safeEnv, ALLOW_LIVE: "true" }), /ALLOW_LIVE/);
  assert.throws(() => loadProbeConfig({ ...safeEnv, ENABLE_TAKER_ORDERS: "true" }), /ENABLE_TAKER_ORDERS/);
  assert.throws(() => loadProbeConfig({ ...safeEnv, MAX_OPEN_ORDERS: "2" }), /MAX_OPEN_ORDERS/);
});

test("maker order is postable below the notional cap and reports inferred size ahead", () => {
  const order = selectMakerOrder(
    {
      tick_size: "0.01",
      min_order_size: "5",
      bids: [{ price: "0.48", size: "12" }, { price: "0.47", size: "4" }],
      asks: [{ price: "0.52", size: "10" }]
    },
    5
  );
  assert.equal(order.price, 0.48);
  assert.equal(order.notional, 2.4);
  assert.equal(order.inferredSizeAhead, 12);
});

test("maker order enforces the venue one-dollar minimum without crossing the ask", () => {
  const order = selectMakerOrder(
    {
      tick_size: "0.01",
      min_order_size: "5",
      bids: [{ price: "0.20", size: "12" }],
      asks: [{ price: "0.22", size: "10" }]
    },
    2,
    1,
    0.05
  );
  assert.equal(order.price, 0.2);
  assert.equal(order.size, 5);
  assert.equal(order.notional, 1);
  assert.throws(() => selectMakerOrder({ tick_size: "0.01", min_order_size: "5", bids: [], asks: [{ price: "0.01", size: "10" }] }, 2, 1, 0.05), /non-marketable/);
});

test("maker order moves below best bid to preserve the strict two-dollar cap", () => {
  const order = selectMakerOrder(
    {
      tick_size: "0.01",
      min_order_size: "5",
      bids: [{ price: "0.58", size: "20" }, { price: "0.40", size: "10" }],
      asks: [{ price: "0.60", size: "10" }]
    },
    2,
    1,
    0.05
  );
  assert.equal(order.price, 0.4);
  assert.equal(order.notional, 2);
  assert.equal(order.betterPricePublicSize, 20);
  assert.equal(order.samePricePublicSize, 10);
  assert.equal(order.inferredSizeAhead, 30);
});

test("normal unsafe market transitions stop a campaign safely instead of failing the run", () => {
  assert.equal(isTransientUnsafeMarket(new Error("maker price 0.04 is below evidence floor 0.05")), true);
  assert.equal(isTransientUnsafeMarket(new Error("cannot satisfy minimum order notional 1")), true);
  assert.equal(isTransientUnsafeMarket(new Error("fail closed: account has 1 open orders")), false);
});

test("secret fields are recursively redacted", () => {
  assert.deepEqual(sanitize({ apiSecret: "x", apiKey: "k", nested: { passphrase: "y", owner: "o", order_owner: "oo", value: 1 } }), {
    apiSecret: "[REDACTED]",
    apiKey: "[REDACTED]",
    nested: { passphrase: "[REDACTED]", owner: "[REDACTED]", order_owner: "[REDACTED]", value: 1 }
  });
});

test("effective queue model remains collecting below evidence threshold", () => {
  const model = fitEffectiveQueueModel([{ eligible: true, filled: true }]);
  assert.equal(model.status, "collecting");
  assert.equal(model.literal_fifo_rank_available, false);
});

test("evidence thresholds count distinct order probes rather than repeated horizon labels", () => {
  const observations = Array.from({ length: 25 }, (_, probe) =>
    [1, 5, 30, 60].map((horizon) => ({
      probe_id: `probe-${probe}`,
      recorded_ts: new Date(1_700_000_000_000 + probe * 1000).toISOString(),
      eligible: true,
      label_observed: true,
      filled: probe < 10,
      horizon_seconds: horizon,
      reconciliation_complete: true,
      zero_open_orders_confirmed: true,
      data_gap_detected: false,
      cancellation_failure: false,
      markout_complete: true,
      markout_timing_valid: true
    }))
  ).flat();
  const model = fitEffectiveQueueModel(observations);
  assert.equal(model.status, "collecting");
  assert.equal(model.sample_size, 25);
  assert.equal(model.label_sample_size, 100);
  assert.equal(model.positive_fills, 10);
  assert.equal(model.negative_non_fills, 15);
});

test("pre-horizon markouts are retained as evidence but excluded from model eligibility", () => {
  const observations = modelObservations({
    order: { size: 5, inferredSizeAhead: 10, spread: 0.02, price: 0.4 },
    market: { endTs: null },
    lifecycle: {
      actual_matched_size: 5,
      live_duration_ms: 60_000,
      first_fill_after_ack_ms: 10_000,
      ack_wall_ms: Date.now(),
      reconciliation_complete: true,
      zero_open_orders_confirmed: true,
      data_gap_detected: false,
      cancellation_failure: false
    },
    context: { observed_trade_size: 1, observed_depth_changes: 1, price_volatility: 0.01 },
    markouts: [
      { horizon_seconds: 1, observation_delay_ms: -1 },
      { horizon_seconds: 5, observation_delay_ms: 0 },
      { horizon_seconds: 30, observation_delay_ms: 0, executable_markout_per_share: -0.1 }
    ]
  });
  assert.equal(observations.every((row) => row.markout_timing_valid === false), true);
  assert.equal(observations.every((row) => row.markout_complete === false), true);
  assert.equal(observations.every((row) => row.eligible === false), true);
});

test("stored legacy observations are revalidated against their recorded markout timing", () => {
  const normalized = normalizeStoredObservation(
    { filled: false, eligible: true, quality_eligible: true, markout_complete: true },
    { lifecycle: { actual_matched_size: 5 }, model_observations: [{ filled: false }, { filled: true }], markouts: [
      { horizon_seconds: 1, observation_delay_ms: -1 },
      { horizon_seconds: 5, observation_delay_ms: 0 },
      { horizon_seconds: 30, observation_delay_ms: 0 }
    ] }
  );
  assert.equal(normalized.eligible, false);
  assert.equal(normalized.quality_eligible, false);
  assert.equal(normalized.markout_complete, false);
  assert.equal(normalized.markout_timing_valid, false);
});

test("effective queue model trains with a temporal holdout", () => {
  const observations = Array.from({ length: 101 }, (_, probe) =>
    [1, 5, 30, 60].map((horizon) => ({
      probe_id: `probe-${String(probe).padStart(3, "0")}`,
      recorded_ts: new Date(1_700_000_000_000 + probe * 1000).toISOString(),
      eligible: true,
      label_observed: true,
      filled: probe % 3 === 0,
      horizon_seconds: horizon,
      inferred_size_ahead: probe % 20,
      spread: 0.02,
      order_price: 0.48,
      order_size: 5,
      time_to_expiry_seconds: 600 - (probe % 300),
      pre_send_trade_size: probe % 8,
      pre_send_depth_changes: probe % 5,
      pre_send_volatility: (probe % 4) / 100,
      reconciliation_complete: true,
      zero_open_orders_confirmed: true,
      data_gap_detected: false,
      cancellation_failure: false,
      markout_complete: true,
      markout_timing_valid: true,
      executable_markout_30s_per_share: 0.01
    }))
  ).flat();
  const model = fitEffectiveQueueModel(observations);
  assert.equal(model.status, "trained_research_only");
  assert.equal(model.temporal_split, "first_80pct_train_last_20pct_test");
  assert.equal(model.sample_size, 101);
  assert.equal(model.label_sample_size, 404);
  assert.equal(model.train_size, 80);
  assert.equal(model.test_size, 21);
  assert.equal(model.train_label_size, 320);
  assert.equal(model.test_label_size, 84);
  assert.equal(model.net_markout_30s_sample_size, 34);
  assert.ok(Number.isFinite(model.out_of_sample_brier_score));
});

test("any submitted incomplete probe fails the quality gate", () => {
  const clean = Array.from({ length: 10 }, (_, probe) => ({
    probe_id: `clean-${probe}`,
    eligible: true,
    label_observed: true,
    filled: false,
    reconciliation_complete: true,
    zero_open_orders_confirmed: true,
    data_gap_detected: false,
    cancellation_failure: false,
    markout_timing_valid: true
  }));
  const excluded = {
    probe_id: "gap",
    eligible: false,
    label_observed: true,
    filled: false,
    reconciliation_complete: false,
    zero_open_orders_confirmed: true,
    data_gap_detected: true,
    cancellation_failure: false,
    markout_timing_valid: true
  };
  const model = fitEffectiveQueueModel([...clean, excluded]);
  assert.equal(model.quality_gates.passed, false);
  assert.equal(model.quality_gates.eligible_observations, 10);
  assert.equal(model.quality_gates.excluded_data_gap_observations, 1);
});

test("every authenticated fill needs its own timely 1/5/30-second markouts", () => {
  const rows = ["trade-1", "trade-2"].flatMap((fillId) => [1, 5, 30].map((horizon) => ({
    fill_id: fillId,
    fill_size: 2.5,
    horizon_seconds: horizon,
    observation_delay_ms: 10,
    midpoint: 0.5,
    executable_price: 0.49,
    midpoint_markout_per_share: 0.01,
    executable_markout_per_share: 0
  })));
  assert.deepEqual(validateFillMarkouts(rows, ["trade-1", "trade-2"], 5), {
    complete: true,
    timing_valid: true,
    expected_fill_count: 2,
    complete_fill_count: 2
  });
  assert.equal(validateFillMarkouts(rows.slice(0, 3), ["trade-1", "trade-2"], 5).complete, false);
  assert.equal(validateFillMarkouts(rows.map((row) => ({ ...row, midpoint: null })), ["trade-1", "trade-2"], 5).complete, false);
  assert.equal(validateFillMarkouts(rows.map((row) => ({ ...row, observation_delay_ms: 2001 })), ["trade-1", "trade-2"], 5).timing_valid, false);
});

test("unresolved durable reservations consume their full notional and are not double counted", () => {
  const summary = {
    probes: [{
      probe_id: "probe-finalized",
      order_submitted: true,
      order: { price: 0.25 },
      lifecycle: { actual_matched_size: 5 }
    }, {
      probe_id: "legacy-probe",
      order_submitted: true,
      order: { price: 0.2 },
      lifecycle: { actual_matched_size: 5 }
    }]
  };
  const risk = summarizeDailyRiskRecords("2026-07-12", [{
    probe_id: "probe-finalized",
    state: "finalized",
    order_submission_intended: true,
    matched_notional: 1.25,
    reserved_notional: 2,
    reconciliation_complete: true,
    zero_open_orders_confirmed: true
  }, {
    probe_id: "probe-ambiguous",
    state: "submitted_pending_reconciliation",
    order_submission_intended: true,
    matched_notional: 0,
    reserved_notional: 2
  }], [summary]);
  assert.deepEqual(risk, {
    date: "2026-07-12",
    conservative_loss_budget_consumed: 4.25,
    submitted_orders: 3,
    filled_orders: 2,
    unresolved_risk_reservations: 1
  });
});

test("a v3 order reservation without probe observations is an ineligible submitted audit row", () => {
  const row = reservationAuditObservation({
    evidence_protocol_version: 3,
    state: "submitted_pending_reconciliation",
    run_id: "run-1",
    probe_id: "probe-1",
    order_submission_intended: true,
    order_submitted: true,
    reserved_notional: 1.25,
    matched_notional: 0,
    zero_open_orders_confirmed: false,
    created_ts: "2026-07-12T00:00:00Z"
  });
  assert.equal(row.order_submitted, true);
  assert.equal(row.protocol_eligible, true);
  assert.equal(row.eligible, false);
  assert.equal(row.data_gap_detected, true);
  const model = fitEffectiveQueueModel([row]);
  assert.equal(model.quality_gates.submitted_observations, 1);
  assert.equal(model.quality_gates.passed, false);
});

test("a confirmed no-order reservation release does not create a model audit row", () => {
  assert.equal(reservationAuditObservation({
    probe_id: "probe-rejected",
    state: "released_no_order",
    order_submitted: false
  }), null);
});

test("risk reservations resolve only with explicit reconciliation and zero-open proof", () => {
  assert.equal(isRiskReservationResolved({ state: "finalized", reconciliation_complete: true, zero_open_orders_confirmed: true }), true);
  assert.equal(isRiskReservationResolved({ state: "finalized", reconciliation_complete: false, zero_open_orders_confirmed: true }), false);
  assert.equal(isRiskReservationResolved({ state: "submitted_pending_reconciliation", reconciliation_complete: true, zero_open_orders_confirmed: true }), false);
  assert.equal(isRiskReservationResolved({ state: "released_no_order", order_submitted: false, reconciliation_complete: true, zero_open_orders_confirmed: true }), true);
});
