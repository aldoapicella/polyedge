import {
  AssetType,
  Chain,
  ClobClient,
  OrderType,
  Side
} from "@polymarket/clob-client-v2";
import { createWalletClient, http } from "viem";
import { privateKeyToAccount } from "viem/accounts";
import { polygon } from "viem/chains";
import WebSocket from "ws";
import {
  EVIDENCE_PROTOCOL_VERSION,
  EventLedger,
  MARKOUT_HORIZONS_SECONDS,
  acquireCampaignLease,
  assertEligibleOrigin,
  campaignRestSchedule,
  evaluateCampaignRiskGate,
  evaluateDailyRiskGate,
  finalizeProbeRisk,
  isTransientUnsafeMarket,
  loadCampaignRiskControl,
  loadDailyCampaignRisk,
  loadUnresolvedRiskReservations,
  loadProbeConfig,
  marketContext,
  modelObservations,
  reserveProbeRisk,
  sanitize,
  selectMakerOrder,
  summarizeCampaignRisk,
  summarizePortfolio,
  uploadEvidence,
  validateFillMarkouts
} from "./lib.mjs";

const config = loadProbeConfig();
const runId = `venue-probe-${new Date().toISOString().replace(/[-:.TZ]/g, "")}-${crypto.randomUUID().slice(0, 8)}`;
const ledger = new EventLedger(runId);
const openSockets = new Set();
const completedProbes = [];
let startupGeoblock = null;
let startingRisk = null;
let portfolioSnapshot = null;
let campaignLease = null;
let campaignRiskControl = null;
let activeClient = null;
let terminationRequested = false;
let shutdownStarted = false;
let summary;

process.once("SIGTERM", () => { void requestGracefulShutdown("SIGTERM"); });
process.once("SIGINT", () => { void requestGracefulShutdown("SIGINT"); });

try {
  summary = await runProbe();
} catch (error) {
  ledger.record("venue_probe_failed", { message: error.message });
  const latest = completedProbes.at(-1) || null;
  let durableRisk = null;
  try {
    const dailyTurnover = await loadDailyCampaignRisk(config);
    durableRisk = activeClient && campaignRiskControl
      ? {
          campaign: (await captureCampaignRiskSnapshot(activeClient, "failure_reconciliation")).risk,
          daily_turnover: dailyTurnover,
          primary_risk_source: "cash_flow_adjusted_campaign_equity"
        }
      : { campaign: null, daily_turnover: dailyTurnover };
  } catch (riskError) {
    ledger.record("venue_campaign_risk_reload_failed", { message: riskError.message });
  }
  summary = {
    schema_version: 3,
    evidence_protocol_version: EVIDENCE_PROTOCOL_VERSION,
    run_id: runId,
    status: "failed_closed",
    started_ts: ledger.events[0]?.recorded_ts || new Date().toISOString(),
    finished_ts: new Date().toISOString(),
    error: error.message,
    order_submission_attempted: ledger.events.some((event) => event.type === "venue_order_send"),
    order_submitted: ledger.events.some((event) => event.type === "venue_order_http_ack" && event.data?.response?.success === true),
    submitted_order_count: completedProbes.length,
    completed_probe_count: completedProbes.filter((probe) => probe.status === "completed").length,
    execution_origin: "azure_north_europe_static_egress",
    execution_country: startupGeoblock?.country || null,
    static_egress_verified: Boolean(config.expectedEgressIp && startupGeoblock?.ip === config.expectedEgressIp),
    risk_at_start: startingRisk,
    risk_at_end: durableRisk,
    portfolio: portfolioSnapshot,
    probes: completedProbes,
    market: latest?.market || null,
    order: latest?.order || null,
    pre_send_context: latest?.pre_send_context || null,
    lifecycle: latest?.lifecycle || null,
    markouts: latest?.markouts || [],
    model_observations: completedProbes.flatMap((probe) => probe.model_observations || []),
    queue_position_source: "authenticated_lifecycle_plus_public_l2",
    queue_position_metric: "inferred_size_ahead",
    literal_fifo_rank_available: false,
    research_only: true,
    live_trading_enabled: false
  };
  process.exitCode = 1;
}

if (summary) {
  closeOpenSockets();
  try {
    const upload = await uploadEvidence(config, runId, summary, ledger);
    const line = JSON.stringify(sanitize({ ...summary, evidence_upload: upload }));
    if (process.exitCode) console.error(line);
    else console.log(line);
  } catch (error) {
    process.exitCode = 1;
    console.error(JSON.stringify(sanitize({ ...summary, evidence_upload: { uploaded: false, error: error.message } })));
  }
}

if (campaignLease) {
  try {
    await campaignLease.release();
  } catch (error) {
    process.exitCode = 1;
    console.error(JSON.stringify({ status: "failed_closed", error: `campaign lease release failed: ${error.message}` }));
  }
}

async function runProbe() {
  campaignLease = await acquireCampaignLease(config, runId);
  ledger.record("venue_probe_started", {
    evidence_protocol_version: EVIDENCE_PROTOCOL_VERSION,
    distributed_campaign_lease: true,
    dry_run: config.dryRun,
    execution_mode: config.executionMode,
    allow_live: config.allowLive,
    maximum_orders: config.maximumOrders,
    maximum_open_orders: config.maxOpenOrders,
    maximum_order_notional: config.maxOrderNotional,
    maximum_daily_loss: config.maxDailyLoss,
    funded_campaign_id: config.campaignId,
    campaign_baseline_equity: config.campaignBaselineEquity,
    campaign_equity_floor: config.campaignEquityFloor,
    maximum_campaign_drawdown: config.maxCampaignDrawdown,
    taker_orders_enabled: config.enableTakerOrders,
    campaign_enabled: config.campaignEnabled,
    rest_horizons_seconds: config.restHorizonsSeconds,
    expected_country: config.expectedCountry,
    expected_egress_ip_configured: Boolean(config.expectedEgressIp)
  });
  const geoblock = await checkOrigin("startup");
  startupGeoblock = geoblock;

  const account = privateKeyToAccount(normalizePrivateKey(config.privateKey));
  const signer = createWalletClient({ account, chain: polygon, transport: http("https://polygon-bor-rpc.publicnode.com") });
  const creds = {
    key: config.apiKey,
    secret: config.apiSecret,
    passphrase: config.apiPassphrase
  };
  const client = new ClobClient({
    host: config.clobUrl,
    chain: Chain.POLYGON,
    signer,
    creds,
    signatureType: config.signatureType,
    funderAddress: config.funderAddress,
    useServerTime: true,
    throwOnError: true
  });
  activeClient = client;
  let openOrders = await getOpenOrdersStrict(client, "startup");
  if (openOrders.length) {
    ledger.record("venue_startup_open_orders_detected", { open_order_count: openOrders.length });
    await ensureNoOpenOrders(client, "startup_recovery");
    openOrders = await getOpenOrdersStrict(client, "startup_after_recovery");
  }
  const balance = await client.getBalanceAllowance({ asset_type: AssetType.COLLATERAL, signature_type: config.signatureType });
  const clock = await checkClock(client, "startup");
  ledger.record("venue_auth_validated", {
    signer_address: account.address,
    funder_address: config.funderAddress,
    signature_type: config.signatureType,
    clock,
    open_order_count: openOrders.length,
    collateral_balance: balance.balance,
    collateral_allowance_count: Object.keys(balance.allowances || {}).length
  });
  if (openOrders.length !== 0) throw new Error(`fail closed: account has ${openOrders.length} open orders`);
  campaignRiskControl = await loadCampaignRiskControl(config);
  ledger.record("venue_campaign_risk_control_loaded", campaignRiskControl);
  const campaignAtStart = await captureCampaignRiskSnapshot(client, "startup");
  const dailyAtStart = await loadDailyCampaignRisk(config);
  const dailyRiskGate = evaluateDailyRiskGate(
    dailyAtStart.conservative_loss_budget_consumed,
    config.maxDailyLoss,
    true
  );
  // The UTC ledger remains visible as turnover diagnostics only. It never grants
  // fresh funded risk at midnight.
  const riskAtStart = {
    campaign: campaignAtStart.risk,
    campaign_gate: campaignAtStart.gate,
    daily_turnover: dailyAtStart,
    daily_turnover_gate: dailyRiskGate,
    primary_risk_source: "cash_flow_adjusted_campaign_equity"
  };
  startingRisk = riskAtStart;
  ledger.record("venue_campaign_risk_loaded", riskAtStart);
  if (config.dryRun) {
    const selection = await discoverMarket(client, config, config.maxOrderNotional);
    ledger.record("venue_market_selected", selection.market);
    const channels = await openChannels(selection.market);
    await sleep(config.prewarmMs);
    await Promise.all([channels.user.ensureOpen(), channels.market.ensureOpen()]);
    const context = marketContext(channels.market.messages);
    channels.user.close();
    channels.market.close();
    ledger.record("venue_probe_dry_run_complete", { order_notional: selection.order.notional });
    return baseSummary({
      status: "auth_validated_no_order",
      geoblock,
      probes: [{
        probe_id: `${runId}-dry-run`,
        status: "auth_validated_no_order",
        order_submitted: false,
        market: selection.market,
        order: selection.order,
        pre_send_context: context,
        lifecycle: null,
        markouts: [],
        model_observations: []
      }],
      riskAtStart,
      riskAtEnd: riskAtStart,
      stopReason: campaignAtStart.gate.diagnostics_only ? "dry_run_campaign_risk_blocked" : "dry_run",
      orderSubmitted: false
    });
  }

  const schedule = campaignRestSchedule(config.maximumOrders, config.restHorizonsSeconds, runId);
  const probes = [];
  let stopReason = "maximum_orders_reached";
  let index = 0;
  let attempts = 0;
  while (index < config.maximumOrders && attempts < config.maximumOrders * 3) {
    if (terminationRequested) {
      stopReason = "termination_requested";
      break;
    }
    campaignLease.assertHealthy();
    attempts += 1;
    const currentCampaign = await captureCampaignRiskSnapshot(client, `before_probe_${index + 1}`);
    if (currentCampaign.risk.unresolved_position_count > 0) {
      stopReason = "existing_unresolved_position_blocks_submission";
      break;
    }
    const remainingRisk = Math.min(
      currentCampaign.risk.max_campaign_drawdown - currentCampaign.risk.campaign_drawdown,
      currentCampaign.risk.account_equity - currentCampaign.risk.equity_floor
    );
    if (remainingRisk <= 1e-9) {
      stopReason = "campaign_drawdown_or_equity_floor_exhausted";
      break;
    }
    const latestBalance = await client.getBalanceAllowance({ asset_type: AssetType.COLLATERAL, signature_type: config.signatureType });
    const availableCollateral = Number(latestBalance.balance) / 1_000_000;
    if (availableCollateral <= 0) {
      stopReason = "collateral_exhausted";
      break;
    }
    const perOrderCap = Math.min(config.maxOrderNotional, remainingRisk, availableCollateral);
    let selection;
    try {
      selection = await discoverMarket(client, config, perOrderCap);
    } catch (error) {
      ledger.record("venue_probe_skipped_no_safe_order", { index, per_order_cap: perOrderCap, message: error.message });
      stopReason = "no_safe_order_within_remaining_budget";
      break;
    }
    let probe;
    try {
      probe = await executeSingleProbe({
        client,
        index,
        restSeconds: schedule[index],
        market: selection.market,
        book: selection.book,
        order: selection.order,
        maxNotional: perOrderCap,
        remainingRisk
      });
    } catch (error) {
      if (/invalid post-only order: order crosses book/i.test(error.message)) {
        ledger.record("venue_post_only_race_retried", { index, attempt: attempts, message: error.message });
        await sleep(config.interOrderDelayMs);
        continue;
      }
      if (isTransientUnsafeMarket(error)) {
        ledger.record("venue_campaign_stopped_no_safe_order_after_prewarm", {
          index,
          attempt: attempts,
          message: error.message
        });
        stopReason = "no_safe_order_after_prewarm";
        break;
      }
      throw error;
    }
    probes.push(probe);
    completedProbes.push(probe);
    index += 1;
    if (probe.lifecycle?.reconciliation_complete !== true) {
      stopReason = "unresolved_probe_reconciliation";
      break;
    }
    if (index < config.maximumOrders) await sleep(config.interOrderDelayMs);
  }
  if (attempts >= config.maximumOrders * 3 && index < config.maximumOrders) stopReason = "post_only_retry_budget_exhausted";
  const finalOpenOrders = await getOpenOrdersStrict(client, "campaign_end");
  if (finalOpenOrders.length !== 0) {
    ledger.record("venue_batch_zero_open_orders_failed", { open_order_count: finalOpenOrders.length });
    await ensureNoOpenOrders(client, "campaign_end_recovery");
  }
  const campaignAtEnd = await captureCampaignRiskSnapshot(client, "campaign_end");
  const riskAtEnd = {
    campaign: campaignAtEnd.risk,
    campaign_gate: campaignAtEnd.gate,
    daily_turnover: await loadDailyCampaignRisk(config),
    primary_risk_source: "cash_flow_adjusted_campaign_equity"
  };
  ledger.record("venue_batch_zero_open_orders_confirmed", { probe_count: probes.length, risk_at_end: riskAtEnd });
  return baseSummary({
    status: probes.length === config.maximumOrders ? "campaign_completed" : "campaign_stopped_safely",
    geoblock,
    probes,
    riskAtStart,
    riskAtEnd,
    stopReason,
    orderSubmitted: probes.length > 0
  });
}

async function executeSingleProbe({ client, index, restSeconds, market, book, order, maxNotional, remainingRisk }) {
  const probeId = `${runId}-p${String(index + 1).padStart(2, "0")}`;
  const startedTs = new Date().toISOString();
  ledger.record("venue_single_probe_started", { probe_id: probeId, index, rest_seconds: restSeconds, market, order });
  const channels = await openChannels(market);
  const userChannel = channels.user;
  const marketChannel = channels.market;
  let orderId;
  let riskReservation = null;
  try {
    await sleep(config.prewarmMs);
    const context = marketContext(marketChannel.messages);
    ledger.record("public_l2_prewarm_complete", { probe_id: probeId, ...context });
    await Promise.all([userChannel.ensureOpen(), marketChannel.ensureOpen()]);
    book = await client.getOrderBook(market.tokenId);
    const feeRateBps = Number(await client.getFeeRateBps(market.tokenId));
    if (!Number.isFinite(feeRateBps) || feeRateBps < 0 || feeRateBps > 10_000) {
      throw new Error(`fail closed: invalid venue fee rate ${feeRateBps}`);
    }
    const feeRiskMultiplier = 1 + feeRateBps / 10_000;
    order = selectMakerOrder(
      book,
      Math.min(maxNotional, remainingRisk / feeRiskMultiplier),
      config.minOrderNotional,
      config.minOrderPrice
    );
    await checkOrigin(`pre_submit_${index + 1}`);
    await checkClock(client, `pre_submit_${index + 1}`);
    campaignLease.assertHealthy();
    if (terminationRequested) throw new Error("fail closed: termination requested before order submission");
    const [openOrders, balance] = await Promise.all([
      getOpenOrdersStrict(client, `pre_submit_${probeId}`),
      client.getBalanceAllowance({ asset_type: AssetType.COLLATERAL, signature_type: config.signatureType })
    ]);
    if (openOrders.length !== 0) throw new Error(`fail closed: account has ${openOrders.length} open orders before probe`);
    const availableCollateral = Number(balance.balance) / 1_000_000;
    order = selectMakerOrder(
      book,
      Math.min(maxNotional, remainingRisk / feeRiskMultiplier, availableCollateral / feeRiskMultiplier),
      config.minOrderNotional,
      config.minOrderPrice
    );
    const feeRiskUpperBound = order.notional * feeRateBps / 10_000;
    const reservedNotional = order.notional + feeRiskUpperBound;
    ledger.record("public_l2_before_send", {
      probe_id: probeId,
      book,
      order,
      venue_fee_rate_bps: feeRateBps,
      fee_risk_upper_bound: feeRiskUpperBound,
      reserved_notional: reservedNotional,
      refreshed_after_prewarm: true
    });
    if (availableCollateral + 1e-9 < reservedNotional) {
      throw new Error(`insufficient collateral for principal plus fee-risk bound: need ${reservedNotional}`);
    }
    await captureCampaignRiskSnapshot(client, `pre_reservation_${probeId}`, reservedNotional, order.notional);
    riskReservation = await reserveProbeRisk(config, {
      date: new Date().toISOString().slice(0, 10),
      run_id: runId,
      probe_id: probeId,
      reserved_notional: reservedNotional,
      principal_notional: order.notional,
      fee_rate_bps: feeRateBps,
      fee_risk_upper_bound: feeRiskUpperBound
    });
    ledger.record("venue_probe_risk_reserved", { probe_id: probeId, reserved_notional: reservedNotional, principal_notional: order.notional, fee_risk_upper_bound: feeRiskUpperBound });
    await captureCampaignRiskSnapshot(client, `pre_send_${probeId}`, reservedNotional, order.notional, probeId);
    campaignLease.assertHealthy();
    if (terminationRequested) throw new Error("fail closed: termination requested after risk reservation and before order submission");
    const sendWallMs = Date.now();
    const sendMono = process.hrtime.bigint();
    const expiration = Math.floor(sendWallMs / 1000) + Math.max(180, restSeconds + 120);
    ledger.record("venue_order_send", {
      probe_id: probeId,
      market_id: market.marketId,
      condition_id: market.conditionId,
      token_id: market.tokenId,
      side: "BUY",
      price: order.price,
      size: order.size,
      notional: order.notional,
      order_type: "GTD",
      expiration,
      post_only: true,
      planned_rest_seconds: restSeconds
    });
    let response;
    try {
      response = await client.createAndPostOrder(
        { tokenID: market.tokenId, price: order.price, size: order.size, side: Side.BUY, expiration },
        { tickSize: order.tickSize, negRisk: order.negRisk },
        OrderType.GTD,
        true
      );
    } catch (error) {
      ledger.record("venue_order_post_error", { probe_id: probeId, message: error.message });
      await ensureNoOpenOrders(client, `ambiguous_post_${probeId}`);
      throw new Error(`fail closed: ambiguous order submission; durable risk reservation remains unresolved: ${error.message}`);
    }
    const ackWallMs = Date.now();
    const ackLatencyMs = elapsedMs(sendMono);
    ledger.record("venue_order_http_ack", { probe_id: probeId, response, client_to_http_ack_ms: ackLatencyMs });
    if (!response?.success || !["live", "matched"].includes(String(response.status).toLowerCase()) || !response.orderID) {
      throw new Error(`post-only order was not acknowledged live: ${response?.status || response?.errorMsg || "unknown"}`);
    }
    orderId = response.orderID;
    riskReservation = await finalizeProbeRisk(config, riskReservation, {
      state: "submitted_pending_reconciliation",
      order_submitted: true,
      order_id: orderId,
      matched_notional: 0,
      reconciliation_complete: false,
      zero_open_orders_confirmed: false
    });
    ledger.record("venue_probe_order_id_persisted", { probe_id: probeId, order_id: orderId });
    const bookAfterAckPromise = client.getOrderBook(market.tokenId).catch((error) => ({ error: error.message }));
    const markoutTask = watchAndCaptureMarkouts(client, market.tokenId, order, userChannel, orderId, restSeconds, ledger);
    await waitForRestOrFullFill(userChannel.messages, orderId, order.size, restSeconds * 1000);

    const openBeforeCancel = (await getOpenOrdersStrict(client, `before_cancel_${probeId}`)).some((candidate) => candidate.id === orderId);
    const cancellation = openBeforeCancel
      ? await cancelWithRetries(client, orderId, ledger)
      : {
          cancelSendWallMs: null,
          cancelResponseWallMs: Date.now(),
          cancelRoundTripMs: null,
          cancelResponse: { already_terminal: true },
          failedAttempts: 0
        };
    const { cancelSendWallMs, cancelResponseWallMs, cancelRoundTripMs, cancelResponse, failedAttempts } = cancellation;
    const bookAfterAck = await bookAfterAckPromise;
    ledger.record("public_l2_after_ack", { probe_id: probeId, order_id: orderId, book: bookAfterAck });
    ledger.record("venue_cancel_http_response", { probe_id: probeId, order_id: orderId, response: cancelResponse, client_cancel_round_trip_ms: cancelRoundTripMs });
    if (openBeforeCancel) await waitForRelevantUserEvent(userChannel.messages, orderId, "CANCELLATION", 5000).catch(() => null);
    const stableReconciliation = await waitForStablePostCancelReconciliation(
      client,
      market.conditionId,
      orderId,
      userChannel.messages,
      probeId
    );
    const { finalOrder, relatedTrades, userEvents, zeroOpenOrders, stableFinality } = stableReconciliation;
    const userTradeFills = tradeFillsFromUserEvents(userEvents, orderId);
    const restTradeFills = tradeFillsFromRest(relatedTrades, orderId);
    const restOrderMatched = Number(finalOrder?.size_matched || 0);
    const userOrderMatched = userEvents
      .map((event) => Number(event.size_matched || 0))
      .reduce((maximum, value) => Math.max(maximum, Number.isFinite(value) ? value : 0), 0);
    const restTradesMatched = sum(restTradeFills.map((fill) => fill.size));
    const userTradesMatched = sum(userTradeFills.map((fill) => fill.size));
    const matchedSize = Math.max(restOrderMatched, userOrderMatched, restTradesMatched, userTradesMatched);
    const quantityAgreement = [restOrderMatched, userOrderMatched, restTradesMatched, userTradesMatched]
      .every((value) => nearlyEqualSize(value, matchedSize));
    const tradeIdAgreement = sameStringSet(restTradeFills.map((fill) => fill.id), userTradeFills.map((fill) => fill.id));
    const firstFillWallMs = firstFillTimestamp(userEvents, relatedTrades);
    const cancellationEvent = userEvents.find((event) => String(event.type).toUpperCase() === "CANCELLATION");
    const cancellationReceived = cancellationEvent?._received_wall_ms ?? null;
    const tradeThrough = publicTradeThroughStats(marketChannel.messages, order, ackWallMs, cancellationReceived ?? cancelResponseWallMs, firstFillWallMs);
    const markoutCapture = await markoutTask;
    const markouts = markoutCapture.markouts;
    const terminalStatus = String(finalOrder?.status || cancellationEvent?.type || "").toUpperCase();
    const restOrderReturned = !finalOrder?.error && finalOrder?.status !== "not_returned_after_cancel";
    const terminalConfirmed = ["CANCELED", "CANCELLED", "MATCHED", "FILLED", "CANCELLATION", "EXPIRED"].some((value) => terminalStatus.includes(value));
    const reconciliationComplete = zeroOpenOrders && stableFinality && restOrderReturned && terminalConfirmed && quantityAgreement && tradeIdAgreement;
    const dataGapDetected = !stableFinality || terminationRequested || userChannel.gapCount() > 0 || marketChannel.gapCount() > 0 ||
      (matchedSize > 0 && (!markoutCapture.fillObservedLive || !tradeIdAgreement));
    const terminalWallMs = cancellationReceived ?? cancelResponseWallMs ?? firstFillWallMs ?? Date.now();
    const lifecycle = {
      order_id: orderId,
      send_wall_ms: sendWallMs,
      ack_wall_ms: ackWallMs,
      client_to_http_ack_ms: ackLatencyMs,
      cancel_send_wall_ms: cancelSendWallMs,
      cancel_http_response_wall_ms: cancelResponseWallMs,
      client_cancel_round_trip_ms: cancelRoundTripMs,
      user_channel_cancel_received_wall_ms: cancellationReceived,
      client_to_user_cancel_ack_ms: cancelSendWallMs === null || cancellationReceived === null ? null : cancellationReceived - cancelSendWallMs,
      planned_rest_seconds: restSeconds,
      live_duration_ms: terminalWallMs - ackWallMs,
      first_fill_after_ack_ms: firstFillWallMs === null ? null : firstFillWallMs - ackWallMs,
      actual_matched_size: matchedSize,
      venue_fee_rate_bps: feeRateBps,
      matched_principal_notional: matchedSize * order.price,
      matched_fee_risk_upper_bound: matchedSize * order.price * feeRateBps / 10_000,
      conservative_matched_risk_notional: matchedSize * order.price * feeRiskMultiplier,
      partial_fill: matchedSize > 0 && matchedSize < order.size,
      fully_filled: matchedSize >= order.size,
      fill_raced_cancellation: firstFillWallMs !== null && cancelSendWallMs !== null && firstFillWallMs >= cancelSendWallMs,
      public_touch_trade_count: tradeThrough.touch_count,
      public_strict_trade_through_count: tradeThrough.strict_trade_through_count,
      public_trade_through_without_fill_count: tradeThrough.trade_through_without_fill_count,
      venue_status: finalOrder?.status || cancellationEvent?.type || "terminal_not_returned",
      related_trade_ids: restTradeFills.map((fill) => fill.id),
      live_user_trade_ids: userTradeFills.map((fill) => fill.id),
      rest_order_matched_size: restOrderMatched,
      user_order_matched_size: userOrderMatched,
      rest_trade_matched_size: restTradesMatched,
      user_trade_matched_size: userTradesMatched,
      matched_size_source_agreement: quantityAgreement,
      trade_id_source_agreement: tradeIdAgreement,
      rest_order_returned: restOrderReturned,
      post_cancel_finality_stable: stableFinality,
      post_cancel_observation_ms: stableReconciliation.observationMs,
      authenticated_user_channel_reconnects: userChannel.reconnectCount(),
      public_market_channel_reconnects: marketChannel.reconnectCount(),
      authenticated_user_channel_duplicates: userChannel.duplicateCount(),
      public_market_channel_duplicates: marketChannel.duplicateCount(),
      reconciliation_complete: reconciliationComplete,
      zero_open_orders_confirmed: zeroOpenOrders,
      data_gap_detected: dataGapDetected,
      cancellation_failure: failedAttempts > 0 && !zeroOpenOrders,
      markout_capture_complete: markoutCapture.complete === true
    };
    await finalizeProbeRisk(config, riskReservation, {
      state: reconciliationComplete ? "finalized" : "unresolved_reconciliation",
      order_submitted: true,
      order_id: orderId,
      matched_notional: matchedSize * order.price * feeRiskMultiplier,
      reconciliation_complete: reconciliationComplete,
      zero_open_orders_confirmed: zeroOpenOrders
    });
    ledger.record("venue_probe_risk_finalized", { probe_id: probeId, matched_notional: matchedSize * order.price * feeRiskMultiplier, fee_rate_bps: feeRateBps, reconciliation_complete: reconciliationComplete });
    await captureCampaignRiskSnapshot(client, `post_reconciliation_${probeId}`);
    const observations = modelObservations({ order, market, lifecycle, context, markouts });
    const result = {
      schema_version: 3,
      evidence_protocol_version: EVIDENCE_PROTOCOL_VERSION,
      probe_id: probeId,
      status: reconciliationComplete ? "completed" : "completed_ineligible",
      started_ts: startedTs,
      finished_ts: new Date().toISOString(),
      order_submitted: true,
      market,
      order,
      context,
      pre_send_context: context,
      lifecycle,
      markouts,
      model_observations: observations
    };
    ledger.record("venue_single_probe_completed", { probe_id: probeId, lifecycle, markouts, model_observations: observations });
    return result;
  } catch (error) {
    ledger.record("venue_single_probe_failed", { probe_id: probeId, order_id: orderId, message: error.message });
    await ensureNoOpenOrders(client, `probe_failure_${probeId}`);
    throw error;
  } finally {
    userChannel.close();
    marketChannel.close();
  }
}

function baseSummary({ status, geoblock, probes, riskAtStart, riskAtEnd, stopReason, orderSubmitted }) {
  const latest = probes.at(-1) || null;
  return {
    schema_version: 3,
    evidence_protocol_version: EVIDENCE_PROTOCOL_VERSION,
    run_id: runId,
    status,
    started_ts: ledger.events[0].recorded_ts,
    finished_ts: new Date().toISOString(),
    execution_origin: "azure_north_europe_static_egress",
    execution_country: geoblock?.country || null,
    static_egress_verified: Boolean(config.expectedEgressIp && geoblock?.ip === config.expectedEgressIp),
    execution_mode: "venue_probe",
    allow_live: false,
    allow_venue_probe: true,
    post_only: true,
    venue_order_type: "GTD",
    taker_orders_enabled: false,
    campaign_enabled: config.campaignEnabled,
    maximum_orders: config.maximumOrders,
    maximum_open_orders: 1,
    maximum_order_notional: config.maxOrderNotional,
    maximum_daily_loss: config.maxDailyLoss,
    funded_campaign_id: config.campaignId,
    campaign_baseline_equity: config.campaignBaselineEquity,
    campaign_equity_floor: config.campaignEquityFloor,
    maximum_campaign_drawdown: config.maxCampaignDrawdown,
    rest_horizons_seconds: config.restHorizonsSeconds,
    order_submitted: orderSubmitted,
    submitted_order_count: probes.filter((probe) => probe.order_submitted).length,
    completed_probe_count: probes.filter((probe) => probe.status === "completed").length,
    stop_reason: stopReason,
    risk_at_start: riskAtStart,
    risk_at_end: riskAtEnd,
    portfolio: portfolioSnapshot,
    estimated_round_trip_cost_per_share: config.estimatedRoundTripCostPerShare,
    probes,
    market: latest?.market || null,
    order: latest?.order || null,
    pre_send_context: latest?.pre_send_context || null,
    lifecycle: latest?.lifecycle || null,
    markouts: latest?.markouts || [],
    model_observations: probes.flatMap((probe) => probe.model_observations || []),
    queue_position_source: "authenticated_lifecycle_plus_public_l2",
    queue_position_metric: "inferred_size_ahead",
    literal_fifo_rank_available: false,
    practical_closure: {
      target: "empirical_probability_of_fill_within_1_5_30_60_seconds",
      model_status: "collecting",
      out_of_sample_validation_required: true
    },
    remaining_literal_fifo_limitations: [
      "venue does not expose exact matching rank",
      "same-price additions are not attributed ahead or behind",
      "cancellations ahead are not identified",
      "hidden liquidity and venue-internal priority changes are not observable"
    ],
    research_only: true,
    live_trading_enabled: false,
    strategy_promotion_allowed: false
  };
}

async function captureCampaignRiskSnapshot(client, stage, proposedNotional = 0, orderNotional = proposedNotional, ignoredReservationId = null) {
  if (!campaignRiskControl) throw new Error("fail closed: immutable funded campaign control is unavailable");
  const positionsUrl = new URL("https://data-api.polymarket.com/positions");
  positionsUrl.searchParams.set("user", config.funderAddress);
  positionsUrl.searchParams.set("sizeThreshold", "0");
  positionsUrl.searchParams.set("limit", "500");
  const valueUrl = new URL("https://data-api.polymarket.com/value");
  valueUrl.searchParams.set("user", config.funderAddress);
  const [balance, positionsResponse, valueResponse, openOrders, reservations] = await Promise.all([
    client.getBalanceAllowance({ asset_type: AssetType.COLLATERAL, signature_type: config.signatureType }),
    fetch(positionsUrl, { signal: AbortSignal.timeout(10_000) }),
    fetch(valueUrl, { signal: AbortSignal.timeout(10_000) }),
    getOpenOrdersStrict(client, `campaign_risk_${stage}`),
    loadUnresolvedRiskReservations(config)
  ]);
  if (!positionsResponse.ok) throw new Error(`fail closed: positions reconciliation returned HTTP ${positionsResponse.status}`);
  if (!valueResponse.ok) throw new Error(`fail closed: position-value reconciliation returned HTTP ${valueResponse.status}`);
  const positions = await positionsResponse.json();
  const reportedRows = await valueResponse.json();
  if (!Array.isArray(positions) || !Array.isArray(reportedRows)) {
    throw new Error("fail closed: account reconciliation returned an invalid payload");
  }
  const liquidCollateral = Number(balance.balance) / 1_000_000;
  const summedPositionValue = positions.reduce((sum, row) => sum + Math.max(0, Number(row.currentValue) || 0), 0);
  const reportedPositionValue = reportedRows.reduce((sum, row) => sum + Math.max(0, Number(row.value) || 0), 0);
  const unresolvedPositions = positions.filter((row) => Number(row.size) > 1e-9 && row.redeemable !== true);
  const relevantReservations = reservations.filter((reservation) => String(reservation.probe_id) !== String(ignoredReservationId || ""));
  portfolioSnapshot = summarizePortfolio(positions, liquidCollateral, config.startingCapital);
  portfolioSnapshot.stage = stage;
  portfolioSnapshot.captured_ts = new Date().toISOString();
  const risk = summarizeCampaignRisk({
    control: campaignRiskControl,
    liquidCollateral,
    summedPositionValue,
    reportedPositionValue,
    openOrderCount: openOrders.length,
    unresolvedPositionCount: unresolvedPositions.length,
    unresolvedReservationCount: relevantReservations.length,
    proposedNotional,
    orderNotional
  });
  const gate = evaluateCampaignRiskGate(risk, config.dryRun);
  ledger.record("venue_portfolio_snapshot", portfolioSnapshot);
  ledger.record("venue_campaign_risk_snapshot", { stage, risk, gate, ignored_reservation_id: ignoredReservationId });
  return { risk, gate };
}

async function discoverMarket(client, config, maxNotional) {
  const nowSeconds = Math.floor(Date.now() / 1000);
  const floor = Math.floor(nowSeconds / 900) * 900;
  const slugs = config.targetSlug
    ? [config.targetSlug]
    : [floor, floor + 900, floor - 900].map((epoch) => `btc-updown-15m-${epoch}`);
  const candidates = [];
  for (const slug of slugs) {
    const markets = await fetchJson(`${config.gammaUrl}/markets?slug=${encodeURIComponent(slug)}`);
    const market = Array.isArray(markets) ? markets[0] : null;
    if (!market || market.closed || market.acceptingOrders === false || market.enableOrderBook === false) continue;
    const tokenIds = parseArray(market.clobTokenIds);
    const outcomes = parseArray(market.outcomes);
    for (let index = 0; index < tokenIds.length; index += 1) {
      if (!tokenIds[index]) continue;
      const selectedMarket = {
        slug,
        marketId: String(market.id),
        conditionId: String(market.conditionId),
        tokenId: String(tokenIds[index]),
        outcome: String(outcomes[index] || `outcome-${index}`),
        question: market.question,
        endTs: market.endDate || market.end_date_iso
      };
      try {
        const book = await client.getOrderBook(selectedMarket.tokenId);
        const order = selectMakerOrder(book, maxNotional, config.minOrderNotional, config.minOrderPrice);
        candidates.push({ market: selectedMarket, book, order });
      } catch (error) {
        ledger.record("venue_market_candidate_rejected", { slug, outcome: selectedMarket.outcome, message: error.message });
      }
    }
  }
  if (!candidates.length) throw new Error(`no active BTC 15-minute maker order fits the ${maxNotional.toFixed(4)} cap`);
  candidates.sort((left, right) =>
    left.order.inferredSizeAhead / left.order.size - right.order.inferredSizeAhead / right.order.size ||
    right.order.price - left.order.price
  );
  return candidates[0];
}

async function openChannels(market) {
  const user = await connectChannel({
    url: config.userWsUrl,
    subscription: {
      auth: { apiKey: config.apiKey, secret: config.apiSecret, passphrase: config.apiPassphrase },
      markets: [market.conditionId],
      type: "user"
    },
    ledger,
    eventType: "venue_user_channel_event"
  });
  const marketChannel = await connectChannel({
    url: config.marketWsUrl,
    subscription: { assets_ids: [market.tokenId], type: "market", custom_feature_enabled: true },
    ledger,
    eventType: "venue_market_channel_event"
  });
  return { user, market: marketChannel };
}

async function checkOrigin(stage) {
  const geoblock = await fetchJson("https://polymarket.com/api/geoblock");
  ledger.record("venue_geoblock_check", { stage, ...geoblock, expected_country: config.expectedCountry, static_egress_match: !config.expectedEgressIp || geoblock.ip === config.expectedEgressIp });
  assertEligibleOrigin(geoblock, config);
  return geoblock;
}

async function checkClock(client, stage) {
  const requestStarted = Date.now();
  const response = await client.getServerTime();
  const requestFinished = Date.now();
  const value = Number(response?.server_time ?? response?.time ?? response);
  const serverMs = value < 1e12 ? value * 1000 : value;
  const midpoint = (requestStarted + requestFinished) / 2;
  const driftMs = Math.abs(midpoint - serverMs);
  const result = { stage, server_time_ms: serverMs, request_round_trip_ms: requestFinished - requestStarted, estimated_clock_drift_ms: driftMs };
  ledger.record("venue_clock_check", result);
  if (!Number.isFinite(driftMs) || driftMs > config.maxClockDriftMs) {
    throw new Error(`fail closed: clock drift ${Number.isFinite(driftMs) ? driftMs.toFixed(0) : "unknown"}ms exceeds ${config.maxClockDriftMs}ms`);
  }
  return result;
}

async function connectChannel({ url, subscription, ledger, eventType }) {
  let open = false;
  let stopped = false;
  let socket = null;
  let gaps = 0;
  let reconnects = 0;
  let duplicates = 0;
  let reconnectPromise = null;
  const messages = [];
  const fingerprints = new Set();

  async function openSocket(isReconnect = false) {
    const ws = new WebSocket(url);
    socket = ws;
    openSockets.add(ws);
    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`websocket open timeout: ${url}`)), 8000);
      ws.once("open", () => {
        clearTimeout(timer);
        open = true;
        resolve();
      });
      ws.once("error", reject);
    });
    ws.on("message", (buffer) => {
      const text = buffer.toString();
      if (text === "PONG") {
        messages.push({ _pong: true, _received_wall_ms: Date.now() });
        ledger.record(`${eventType}_pong`);
        return;
      }
      try {
        const parsed = JSON.parse(text);
        const values = Array.isArray(parsed) ? parsed : [parsed];
        for (const value of values) {
          const fingerprint = JSON.stringify(value);
          if (fingerprints.has(fingerprint)) {
            duplicates += 1;
            ledger.record(`${eventType}_duplicate_ignored`, { duplicate_count: duplicates });
            continue;
          }
          fingerprints.add(fingerprint);
          const captured = { ...value, _received_wall_ms: Date.now() };
          messages.push(captured);
          ledger.record(eventType, captured);
        }
      } catch {
        ledger.record(`${eventType}_unparsed`, { text });
      }
    });
    ws.on("close", (code, reason) => {
      open = false;
      openSockets.delete(ws);
      ledger.record(`${eventType}_closed`, { code, reason: reason.toString() });
      if (!stopped) {
        gaps += 1;
        reconnectPromise ||= reconnect().finally(() => { reconnectPromise = null; });
      }
    });
    ws.on("error", (error) => ledger.record(`${eventType}_error`, { message: error.message }));
    const subscribedAt = Date.now();
    ws.send(JSON.stringify(subscription));
    await sleep(250);
    ws.send("PING");
    await waitUntil(
      () => messages.some((message) => message._pong && message._received_wall_ms >= subscribedAt),
      5000,
      "websocket heartbeat timeout"
    );
    ledger.record(`${eventType}_ready`, { url, subscription_type: subscription.type, reconnect: isReconnect });
  }

  async function reconnect() {
    for (let attempt = 1; attempt <= 5 && !stopped; attempt += 1) {
      await sleep(Math.min(2000, 200 * attempt));
      try {
        await openSocket(true);
        reconnects += 1;
        ledger.record(`${eventType}_reconnected`, { attempt, reconnect_count: reconnects });
        return;
      } catch (error) {
        ledger.record(`${eventType}_reconnect_failed`, { attempt, message: error.message });
      }
    }
  }

  await openSocket(false);
  return {
    messages,
    isOpen: () => open && socket?.readyState === WebSocket.OPEN,
    ensureOpen: async () => {
      if (open && socket?.readyState === WebSocket.OPEN) return true;
      if (reconnectPromise) await reconnectPromise;
      await waitUntil(() => open && socket?.readyState === WebSocket.OPEN, 8000, "websocket reconnect timeout");
      return true;
    },
    gapCount: () => gaps,
    reconnectCount: () => reconnects,
    duplicateCount: () => duplicates,
    close: () => {
      stopped = true;
      if (socket) openSockets.delete(socket);
      open = false;
      socket?.close();
    }
  };
}

async function cancelWithRetries(client, orderId, ledger) {
  const cancelSendWallMs = Date.now();
  const cancelMono = process.hrtime.bigint();
  ledger.record("venue_cancel_send", { order_id: orderId });
  let lastError;
  for (let attempt = 1; attempt <= 3; attempt += 1) {
    try {
      const cancelResponse = await client.cancelOrder({ orderID: orderId });
      return {
        cancelSendWallMs,
        cancelResponseWallMs: Date.now(),
        cancelRoundTripMs: elapsedMs(cancelMono),
        cancelResponse,
        failedAttempts: attempt - 1
      };
    } catch (error) {
      lastError = error;
      ledger.record("venue_cancel_attempt_failed", { order_id: orderId, attempt, message: error.message });
      await sleep(200 * attempt);
    }
  }
  const openOrders = await getOpenOrdersStrict(client, `cancel_retries_${orderId}`);
  const matching = openOrders.filter((order) => order.id === orderId);
  if (!matching.length) {
    return {
      cancelSendWallMs,
      cancelResponseWallMs: Date.now(),
      cancelRoundTripMs: elapsedMs(cancelMono),
      cancelResponse: { terminal_before_cancel_confirmation: true, last_error: lastError?.message },
      failedAttempts: 3
    };
  }
  if (openOrders.length === 1 && matching.length === 1) {
    ledger.record("venue_emergency_cancel_all", { reason: "single probe order remained after three targeted cancel attempts" });
    const cancelResponse = await client.cancelAll();
    return {
      cancelSendWallMs,
      cancelResponseWallMs: Date.now(),
      cancelRoundTripMs: elapsedMs(cancelMono),
      cancelResponse: { emergency_cancel_all: true, response: cancelResponse },
      failedAttempts: 3
    };
  }
  throw new Error(`fail closed: probe order remained open after cancel retries: ${lastError?.message || "unknown"}`);
}

async function ensureNoOpenOrders(client, reason) {
  let openOrders = await getOpenOrdersStrict(client, `${reason}_initial`);
  if (!openOrders.length) {
    ledger.record("venue_zero_open_orders_confirmed", { reason });
    return true;
  }
  ledger.record("venue_recovery_cancel_all", { reason, open_order_count: openOrders.length });
  await client.cancelAll();
  await sleep(500);
  openOrders = await getOpenOrdersStrict(client, `${reason}_after_cancel_all`);
  if (openOrders.length) throw new Error(`fail closed: ${openOrders.length} orders remain after recovery`);
  ledger.record("venue_zero_open_orders_confirmed", { reason, after_recovery: true });
  return true;
}

async function getOpenOrdersStrict(client, reason) {
  let lastError;
  for (let attempt = 1; attempt <= 3; attempt += 1) {
    try {
      const orders = await client.getOpenOrders(undefined, true);
      if (!Array.isArray(orders)) throw new Error("open-order response was not an array");
      return orders;
    } catch (error) {
      lastError = error;
      ledger.record("venue_open_orders_query_failed", { reason, attempt, message: error.message });
      if (attempt < 3) await sleep(250 * attempt);
    }
  }
  throw new Error(`fail closed: unable to verify open orders for ${reason}: ${lastError?.message || "unknown"}`);
}

function uniqueTrades(trades) {
  const seen = new Set();
  return trades.filter((trade) => {
    const key = String(trade.id || trade.trade_id || JSON.stringify(trade));
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function tradeFillsFromUserEvents(events, orderId) {
  return normalizeTradeFills((events || []).filter((event) =>
    String(event.event_type || event.type || "").toLowerCase() === "trade"
  ), orderId);
}

function tradeFillsFromRest(trades, orderId) {
  return normalizeTradeFills(trades || [], orderId);
}

function normalizeTradeFills(trades, orderId) {
  const fills = [];
  for (const trade of uniqueTrades(trades)) {
    const id = String(trade.id || trade.trade_id || "");
    if (!id) continue;
    const maker = (trade.maker_orders || []).find((row) => String(row.order_id) === String(orderId));
    const isTaker = String(trade.taker_order_id || "") === String(orderId);
    if (!maker && !isTaker) continue;
    const size = Number(isTaker ? trade.size : maker.matched_amount);
    const price = Number(isTaker ? trade.price : maker.price);
    if (!(size > 0) || !Number.isFinite(price)) continue;
    const venueWallMs = epochMs(trade.match_time_nano || trade.match_time || trade.matchtime || trade.timestamp);
    const receivedWallMs = Number(trade._received_wall_ms);
    const wallMs = venueWallMs || (Number.isFinite(receivedWallMs) ? receivedWallMs : null);
    if (!Number.isFinite(wallMs)) continue;
    fills.push({ id, size, price, wall_ms: wallMs });
  }
  return fills;
}

async function waitForStablePostCancelReconciliation(client, conditionId, orderId, messages, probeId) {
  const started = Date.now();
  const minimumObservationMs = 10_000;
  const requiredStableMs = 5_000;
  const deadline = started + 30_000;
  let previousFingerprint = null;
  let stableSince = null;
  let latest = null;
  while (Date.now() < deadline) {
    campaignLease.assertHealthy();
    let finalOrder;
    let trades;
    try {
      [finalOrder, trades] = await Promise.all([
        client.getOrder(orderId),
        client.getTrades({ market: conditionId })
      ]);
    } catch (error) {
      ledger.record("venue_post_cancel_reconciliation_query_failed", { probe_id: probeId, order_id: orderId, message: error.message });
      previousFingerprint = null;
      stableSince = null;
      await sleep(500);
      continue;
    }
    const relatedTrades = uniqueTrades((trades || []).filter((trade) =>
      String(trade.taker_order_id || "") === String(orderId) ||
      (trade.maker_orders || []).some((maker) => String(maker.order_id) === String(orderId))
    ));
    const userEvents = relevantUserEvents(messages, orderId);
    const restFills = tradeFillsFromRest(relatedTrades, orderId);
    const userFills = tradeFillsFromUserEvents(userEvents, orderId);
    const userOrderMatched = userEvents
      .map((event) => Number(event.size_matched || 0))
      .reduce((maximum, value) => Math.max(maximum, Number.isFinite(value) ? value : 0), 0);
    const openOrders = await getOpenOrdersStrict(client, `post_cancel_stability_${probeId}`);
    const zeroOpenOrders = openOrders.length === 0;
    const terminalStatus = String(finalOrder?.status || "").toUpperCase();
    const terminalConfirmed = ["CANCELED", "CANCELLED", "MATCHED", "FILLED", "EXPIRED"]
      .some((value) => terminalStatus.includes(value));
    const fingerprint = JSON.stringify({
      status: terminalStatus,
      rest_order_matched: Number(finalOrder?.size_matched || 0),
      rest_fills: restFills.map((fill) => [fill.id, fill.size, fill.price]).sort(),
      user_order_matched: userOrderMatched,
      user_fills: userFills.map((fill) => [fill.id, fill.size, fill.price]).sort(),
      zero_open_orders: zeroOpenOrders
    });
    if (fingerprint === previousFingerprint) stableSince ??= Date.now();
    else {
      previousFingerprint = fingerprint;
      stableSince = Date.now();
    }
    latest = { finalOrder, relatedTrades, userEvents, zeroOpenOrders };
    const observationMs = Date.now() - started;
    const stableMs = Date.now() - stableSince;
    ledger.record("venue_post_cancel_reconciliation_snapshot", {
      probe_id: probeId,
      order_id: orderId,
      observation_ms: observationMs,
      stable_ms: stableMs,
      terminal_confirmed: terminalConfirmed,
      zero_open_orders_confirmed: zeroOpenOrders,
      rest_trade_count: restFills.length,
      user_trade_count: userFills.length
    });
    if (observationMs >= minimumObservationMs && stableMs >= requiredStableMs && terminalConfirmed && zeroOpenOrders) {
      return { ...latest, stableFinality: true, observationMs };
    }
    await sleep(500);
  }
  if (!latest) throw new Error("fail closed: no successful post-cancel reconciliation snapshot");
  return { ...latest, stableFinality: false, observationMs: Date.now() - started };
}

function sameStringSet(left, right) {
  const leftSet = new Set((left || []).map(String).filter(Boolean));
  const rightSet = new Set((right || []).map(String).filter(Boolean));
  return leftSet.size === rightSet.size && [...leftSet].every((value) => rightSet.has(value));
}

function nearlyEqualSize(left, right) {
  const a = Number(left);
  const b = Number(right);
  if (!Number.isFinite(a) || !Number.isFinite(b)) return false;
  return Math.abs(a - b) <= Math.max(1e-6, Math.max(Math.abs(a), Math.abs(b)) * 1e-6);
}

function sum(values) {
  return (values || []).reduce((total, value) => total + (Number.isFinite(Number(value)) ? Number(value) : 0), 0);
}

async function waitForRestOrFullFill(messages, orderId, orderSize, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (terminationRequested) return "termination_requested";
    campaignLease.assertHealthy();
    const matched = relevantUserEvents(messages, orderId)
      .map((event) => Number(event.size_matched || event.matched_amount || 0))
      .reduce((maximum, value) => Math.max(maximum, value), 0);
    if (matched >= orderSize) return "fully_filled";
    await sleep(50);
  }
  return "rest_elapsed";
}

async function watchAndCaptureMarkouts(client, tokenId, order, userChannel, orderId, restSeconds, ledger) {
  const deadline = Date.now() + restSeconds * 1000 + 30_000;
  const tasks = new Map();
  let terminalObservedAt = null;
  while (Date.now() < deadline && !terminationRequested) {
    const events = relevantUserEvents(userChannel.messages, orderId);
    for (const fill of tradeFillsFromUserEvents(events, orderId)) {
      if (!tasks.has(fill.id)) {
        tasks.set(fill.id, captureMarkouts(client, tokenId, fill, ledger));
        ledger.record("venue_fill_markout_schedule_started", { order_id: orderId, fill_id: fill.id, fill_size: fill.size, fill_price: fill.price });
      }
    }
    if (events.some((event) => {
      const status = String(event.status || event.type || "").toUpperCase();
      return ["MATCHED", "FILLED", "CANCELED", "CANCELLED", "CANCELLATION", "EXPIRED"].some((value) => status.includes(value));
    })) {
      terminalObservedAt ??= Date.now();
    }
    if (terminalObservedAt !== null && Date.now() - terminalObservedAt >= 15_000) break;
    await sleep(50);
  }
  const markouts = (await Promise.all([...tasks.values()])).flat();
  const fillIds = [...tasks.keys()];
  const coverage = validateFillMarkouts(markouts, fillIds, fillIds.length ? 1 : 0);
  return {
    fillObservedLive: fillIds.length > 0,
    fillIds,
    markouts,
    complete: coverage.complete
  };
}

function publicTradeThroughStats(messages, order, startWallMs, endWallMs, firstFillWallMs) {
  const trades = messages
    .filter((message) => message.event_type === "last_trade_price")
    .filter((message) => message._received_wall_ms >= startWallMs && message._received_wall_ms <= endWallMs)
    .map((message) => ({
      received_wall_ms: message._received_wall_ms,
      price: Number(message.price),
      size: Number(message.size || 0),
      side: message.side || null
    }))
    .filter((trade) => Number.isFinite(trade.price));
  const touches = trades.filter((trade) => trade.price <= order.price);
  const strict = trades.filter((trade) => trade.price < order.price);
  return {
    touch_count: touches.length,
    strict_trade_through_count: strict.length,
    trade_through_without_fill_count: strict.filter((trade) => firstFillWallMs === null || trade.received_wall_ms < firstFillWallMs).length,
    trades
  };
}

function closeOpenSockets() {
  for (const socket of openSockets) {
    try {
      socket.close();
    } catch {
      // Best effort only; process shutdown must never be blocked by a socket.
    }
  }
  openSockets.clear();
}

async function requestGracefulShutdown(signal) {
  terminationRequested = true;
  ledger.record("venue_probe_termination_requested", { signal });
  if (shutdownStarted) return;
  shutdownStarted = true;
  if (!activeClient) {
    closeOpenSockets();
    return;
  }
  try {
    await ensureNoOpenOrders(activeClient, `signal_${signal}`);
    ledger.record("venue_probe_signal_recovery_complete", { signal, zero_open_orders_confirmed: true });
  } catch (error) {
    process.exitCode = 1;
    ledger.record("venue_probe_signal_recovery_failed", { signal, message: error.message });
  } finally {
    closeOpenSockets();
  }
}

async function captureMarkouts(client, tokenId, fill, ledger) {
  const markouts = [];
  for (const horizon of MARKOUT_HORIZONS_SECONDS) {
    const target = fill.wall_ms + horizon * 1000;
    while (Date.now() < target) await sleep(Math.max(1, target - Date.now()));
    let result;
    try {
      const book = await client.getOrderBook(tokenId);
      const observed = Date.now();
      const bids = (book.bids || []).map((row) => Number(row.price)).filter(Number.isFinite);
      const asks = (book.asks || []).map((row) => Number(row.price)).filter(Number.isFinite);
      const bestBid = bids.length ? Math.max(...bids) : null;
      const bestAsk = asks.length ? Math.min(...asks) : null;
      const midpoint = bestBid === null || bestAsk === null ? null : (bestBid + bestAsk) / 2;
      result = {
        fill_id: fill.id,
        fill_size: fill.size,
        horizon_seconds: horizon,
        target_ts: new Date(target).toISOString(),
        observed_ts: new Date(observed).toISOString(),
        observation_delay_ms: observed - target,
        fill_price: fill.price,
        midpoint,
        executable_price: bestBid,
        midpoint_markout_per_share: midpoint === null ? null : midpoint - fill.price,
        executable_markout_per_share: bestBid === null ? null : bestBid - fill.price
      };
    } catch (error) {
      result = {
        fill_id: fill.id,
        fill_size: fill.size,
        horizon_seconds: horizon,
        target_ts: new Date(target).toISOString(),
        observed_ts: new Date().toISOString(),
        observation_delay_ms: Date.now() - target,
        fill_price: fill.price,
        midpoint: null,
        executable_price: null,
        midpoint_markout_per_share: null,
        executable_markout_per_share: null,
        error: error.message
      };
    }
    markouts.push(result);
    ledger.record("venue_fill_markout", result);
  }
  return markouts;
}

function relevantUserEvents(messages, orderId) {
  return messages.filter((message) =>
    message.id === orderId ||
    message.order_id === orderId ||
    message.taker_order_id === orderId ||
    (message.maker_orders || []).some((maker) => maker.order_id === orderId)
  );
}

async function waitForRelevantUserEvent(messages, orderId, type, timeoutMs) {
  return waitUntil(
    () => relevantUserEvents(messages, orderId).find((event) => String(event.type).toUpperCase() === type),
    timeoutMs,
    `user channel ${type} timeout`
  );
}

function firstFillTimestamp(userEvents, trades) {
  const values = [];
  for (const event of userEvents) {
    if (Number(event.size_matched || event.matched_amount || 0) > 0 || String(event.event_type || event.type || "").toLowerCase() === "trade") {
      values.push(epochMs(event.match_time_nano || event.match_time || event.matchtime || event.timestamp) || event._received_wall_ms);
    }
  }
  for (const trade of trades) values.push(epochMs(trade.match_time || trade.matchtime));
  const valid = values.filter((value) => Number.isFinite(value) && value > 0);
  return valid.length ? Math.min(...valid) : null;
}

function epochMs(value) {
  const number = Number(value);
  if (!Number.isFinite(number)) {
    const parsed = Date.parse(String(value || ""));
    return Number.isFinite(parsed) ? parsed : null;
  }
  if (number > 1e15) return number / 1e6;
  if (number > 1e12) return number;
  return number * 1000;
}

function parseArray(value) {
  if (Array.isArray(value)) return value;
  try {
    return JSON.parse(value || "[]");
  } catch {
    return [];
  }
}

function normalizePrivateKey(value) {
  const trimmed = String(value || "").trim();
  return trimmed.startsWith("0x") ? trimmed : `0x${trimmed}`;
}

async function fetchJson(url) {
  const response = await fetch(url, { headers: { accept: "application/json" } });
  if (!response.ok) throw new Error(`HTTP ${response.status} from ${new URL(url).host}`);
  return response.json();
}

async function waitUntil(fn, timeoutMs, message) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const result = fn();
    if (result) return result;
    await sleep(50);
  }
  throw new Error(message);
}

function elapsedMs(start) {
  return Number(process.hrtime.bigint() - start) / 1_000_000;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
