import { AssetType, Chain, ClobClient, OrderType, Side } from "@polymarket/clob-client-v2";
import { createWalletClient, http } from "viem";
import { privateKeyToAccount } from "viem/accounts";
import { polygon } from "viem/chains";
import {
  EVIDENCE_PROTOCOL_VERSION,
  EventLedger,
  HORIZONS_SECONDS,
  acquireCampaignLease,
  assertEligibleOrigin,
  finalizeProbeRisk,
  loadCampaignRiskControl,
  loadUnresolvedRiskReservations,
  marketContext,
  modelObservations,
  publishTerminalRiskPortfolioEvidence,
  reserveProbeRisk,
  settleProbeRiskReservations,
  sanitize,
  storageContainer,
  summarizeCampaignRisk,
  uploadEvidence,
  validateFillMarkouts
} from "./lib.mjs";
import {
  consumeOneShotAuthorization,
  beginFillMarkoutCapture,
  artifactLocationFromUri,
  executeStrategyCanary,
  loadCanaryConfig,
  loadHashedJson,
  polymarketV2FeePerShare,
  validateCanaryPreflight
} from "./canary-lib.mjs";
import {
  cancelOrderWithMetrics,
  cancellationEventReceivedAt,
  connectLifecycleChannel,
  firstFillTimestamp,
  hasExactEligibleHorizons,
  marketMessagesThrough,
  maximumMatchedSize,
  mergeTradeFills,
  nearlyEqualSize,
  postCancelFillStats,
  publicTradeThroughStats,
  sameStringSet,
  sum,
  tradeFillsFromRest,
  tradeFillsFromUserEvents,
  waitForStablePostCancelReconciliation
} from "./canary-lifecycle-lib.mjs";

const config = loadCanaryConfig();
const runId = process.env.STRATEGY_CANARY_RUN_ID || `strategy-canary-${new Date().toISOString().replace(/[-:.TZ]/g, "")}-${crypto.randomUUID().slice(0, 8)}`;
const ledger = new EventLedger(runId);
let lease;
let userChannel;
let marketChannel;
let orderSubmissionAttempted = false;

try {
  const result = await main();
  console.log(JSON.stringify(sanitize({ schema: "polyedge.strategy_canary_run.v1", run_id: runId, ...result })));
} catch (error) {
  process.exitCode = 1;
  console.error(JSON.stringify({ schema: "polyedge.strategy_canary_run.v1", run_id: runId, status: "failed_closed", order_submission_attempted: orderSubmissionAttempted, error: error.message }));
} finally {
  userChannel?.close();
  marketChannel?.close();
  if (lease) await lease.release().catch((error) => {
    process.exitCode = 1;
    console.error(JSON.stringify({ status: "failed_closed", error: `campaign lease release failed: ${error.message}` }));
  });
}

async function main() {
  const container = storageContainer(config);
  if (!container) throw new Error("fail closed: durable Azure Blob storage is unavailable");
  await container.createIfNotExists();
  const intentContainer = storageContainer({ ...config, storageContainer: config.intentContainerName });
  const manifestContainer = storageContainer({ ...config, storageContainer: config.manifestContainerName });
  if (!intentContainer || !manifestContainer) throw new Error("fail closed: intent or manifest source container is unavailable");
  const modelArtifact = artifactLocationFromUri(config.executionModelBlobUri, config.storageAccount);
  const modelContainer = storageContainer({ ...config, storageContainer: modelArtifact.container });
  if (!modelContainer) throw new Error("fail closed: execution model source container is unavailable");
  const [intentDocument, manifestDocument, authorizationDocument, executionModelDocument] = await Promise.all([
    loadHashedJson(intentContainer, config.intentBlobName, config.intentBlobHash),
    loadHashedJson(manifestContainer, config.manifestBlobName, config.manifestBlobHash),
    loadHashedJson(container, config.authorizationBlobName, config.authorizationBlobHash),
    loadHashedJson(modelContainer, modelArtifact.blobName, config.executionModelHash)
  ]);
  const documents = {
    intent: intentDocument.value,
    manifest: manifestDocument.value,
    authorization: authorizationDocument.value,
    authorizationHash: authorizationDocument.hash,
    executionModel: executionModelDocument.value,
    executionModelHash: executionModelDocument.hash
  };
  const account = privateKeyToAccount(normalizePrivateKey(config.privateKey));
  const signer = createWalletClient({ account, chain: polygon, transport: http("https://polygon-bor-rpc.publicnode.com") });
  const client = new ClobClient({
    host: config.clobUrl,
    chain: Chain.POLYGON,
    signer,
    creds: { key: config.apiKey, secret: config.apiSecret, passphrase: config.apiPassphrase },
    signatureType: config.signatureType,
    funderAddress: config.funderAddress,
    useServerTime: true,
    throwOnError: true
  });
  if (!config.dryRun) lease = await acquireCampaignLease(config, runId);
  const runtime = await capturePreflight(client, documents.intent);
  const result = await executeStrategyCanary({
    config,
    documents,
    runtime,
    runId,
    reserveRisk: (reservation) => reserveProbeRisk(config, reservation),
    finalizeNoOrder: (reservation) => finalizeProbeRisk(config, reservation, {
      state: "released_no_order",
      order_submitted: false,
      matched_notional: 0,
      reconciliation_complete: true,
      zero_open_orders_confirmed: true
    }),
    consumeAuthorization: (value) => consumeOneShotAuthorization(container, value),
    executeLifecycle: (value) => executeLifecycle(client, value)
  });
  const evidenceProbe = result.lifecycle?.evidence_probe;
  if (!evidenceProbe) return result;
  let terminalEvidence = null;
  if (Number(evidenceProbe.lifecycle.actual_matched_size) === 0) {
    const terminalRuntime = await capturePreflight(client, documents.intent);
    const campaign = await loadCampaignRiskControl(config);
    terminalEvidence = await publishTerminalRiskPortfolioEvidence(container, {
      reservation: {
        run_id: runId,
        probe_id: evidenceProbe.probe_id,
        order_id: evidenceProbe.lifecycle.order_id,
        condition_id: documents.intent.condition_id,
        state: "finalized_no_fill",
        matched_notional: 0
      },
      settlement: {
        settlement_verified: true,
        trust_boundary_ready: config.trustBoundaryReady,
        zero_open_orders_confirmed: terminalRuntime.openOrderCount === 0,
        evidence_source: "authenticated_no_fill",
        settled_ts: new Date().toISOString(),
        terminal_portfolio: {
          liquid_collateral: terminalRuntime.risk.liquid_collateral,
          current_position_value: terminalRuntime.risk.conservative_position_value,
          account_equity: terminalRuntime.risk.account_equity
        }
      },
      campaign
    });
  }
  const summary = {
    schema_version: 3,
    evidence_protocol_version: EVIDENCE_PROTOCOL_VERSION,
    run_id: runId,
    status: "completed",
    started_ts: evidenceProbe.started_ts,
    finished_ts: evidenceProbe.finished_ts,
    order_submission_attempted: true,
    order_submitted: true,
    submitted_order_count: 1,
    completed_probe_count: evidenceProbe.status === "completed" ? 1 : 0,
    candidate: {
      name: documents.intent.candidate_name,
      candidate_version: documents.intent.candidate_version,
      config_hash: documents.intent.candidate_config_hash
    },
    prediction_model: {
      schema: documents.executionModel.schema,
      blob_uri: config.executionModelBlobUri,
      container_name: modelArtifact.container,
      blob_name: modelArtifact.blobName,
      sha256: documents.executionModelHash,
      model_version: documents.executionModel.model_version,
      generated_at: documents.executionModel.generated_at,
      training_data_end_ts: documents.executionModel.training_data_end_ts || null
    },
    provenance: {
      decision_id: documents.intent.decision_id,
      authorization_kind: documents.authorization.schema === "polyedge.funded_stage_intent_authorization.v1" ? "funded_stage" : "checkpoint_1_canary",
      human_grant_id: documents.authorization.human_grant_id || null,
      human_grant_consumption_blob_name: documents.authorization.human_grant_consumption_blob_name || null,
      human_grant_consumption_sha256: documents.authorization.human_grant_consumption_sha256 || null,
      funded_stage_grant_id: documents.authorization.schema === "polyedge.funded_stage_intent_authorization.v1" ? config.humanGrantId : null,
      funded_stage_grant_sha256: documents.authorization.schema === "polyedge.funded_stage_intent_authorization.v1" ? config.humanGrantHash : null,
      funded_stage_consumption_blob_name: documents.authorization.funded_stage_consumption_blob_name || null,
      funded_stage_consumption_sha256: documents.authorization.funded_stage_consumption_sha256 || null,
      funded_stage_source_state_sha256: documents.authorization.funded_stage_source_state_sha256 || null,
      funded_stage_target_orders: documents.authorization.funded_stage_target_orders || null,
      authorization_blob_name: config.authorizationBlobName,
      authorization_sha256: documents.authorizationHash,
      authorization_container_name: config.storageContainer,
      intent_container_name: config.intentContainerName,
      intent_blob_name: config.intentBlobName,
      intent_sha256: config.intentBlobHash,
      promotion_manifest_container_name: documents.authorization.source_promotion_manifest_container_name || config.manifestContainerName,
      promotion_manifest_blob_name: documents.authorization.source_promotion_manifest_blob_name || config.manifestBlobName,
      promotion_manifest_sha256: documents.authorization.source_promotion_manifest_sha256 || config.manifestBlobHash,
      execution_manifest_container_name: config.manifestContainerName,
      execution_manifest_blob_name: config.manifestBlobName,
      execution_manifest_sha256: config.manifestBlobHash,
      terminal_evidence_blob_name: terminalEvidence?.blob_name || null,
      terminal_evidence_sha256: terminalEvidence?.sha256 || null
    },
    execution_origin: "azure_north_europe_static_egress",
    execution_country: runtime.geoblock.country,
    funder_address: config.funderAddress,
    static_egress_verified: runtime.geoblock.ip === config.expectedEgressIp,
    probes: [evidenceProbe],
    market: evidenceProbe.market,
    order: evidenceProbe.order,
    pre_send_context: evidenceProbe.pre_send_context,
    lifecycle: evidenceProbe.lifecycle,
    markouts: evidenceProbe.markouts,
    model_observations: evidenceProbe.model_observations,
    queue_position_source: "authenticated_lifecycle_plus_public_l2",
    queue_position_metric: "inferred_size_ahead",
    literal_fifo_rank_available: false,
    research_only: true,
    live_trading_enabled: false
  };
  ledger.record("strategy_canary_protocol_v3_completed", {
    probe_id: evidenceProbe.probe_id,
    lifecycle: evidenceProbe.lifecycle,
    markouts: evidenceProbe.markouts,
    model_observations: evidenceProbe.model_observations
  });
  const evidenceUpload = await uploadEvidence(config, runId, summary, ledger);
  const { evidence_probe: _evidenceProbe, ...publicLifecycle } = result.lifecycle;
  return { ...result, lifecycle: publicLifecycle, evidence_upload: evidenceUpload };
}

async function capturePreflight(client, intent, ignoredReservationId = null) {
  const geoblock = await fetchJson("https://polymarket.com/api/geoblock");
  assertEligibleOrigin(geoblock, config);
  const requestStarted = Date.now();
  const serverTimeResponse = await client.getServerTime();
  const requestFinished = Date.now();
  const serverValue = Number(serverTimeResponse?.server_time ?? serverTimeResponse?.time ?? serverTimeResponse);
  const serverMs = serverValue < 1e12 ? serverValue * 1000 : serverValue;
  const clockRoundTripMs = requestFinished - requestStarted;
  const localMidpointMs = (requestStarted + requestFinished) / 2;
  const clockServerMinusLocalMs = serverMs - localMidpointMs;
  const serverClockQuantizationMs = serverValue < 1e12 && Number.isInteger(serverValue) ? 500 : 1;
  const clockUncertaintyMs = clockRoundTripMs / 2 + serverClockQuantizationMs;
  const clockDriftMs = Math.abs(clockServerMinusLocalMs);
  const market = await loadExactMarket(intent);
  const [book, clobMarketInfo, openOrders, riskControl, balance, positionsResponse, valueResponse] = await Promise.all([
    client.getOrderBook(String(intent.token_id)),
    client.getClobMarketInfo(String(intent.condition_id)),
    getOpenOrdersStrict(client),
    loadCampaignRiskControl(config),
    client.getBalanceAllowance({ asset_type: AssetType.COLLATERAL, signature_type: config.signatureType }),
    fetch(`https://data-api.polymarket.com/positions?user=${encodeURIComponent(config.funderAddress)}&sizeThreshold=0&limit=500`, { signal: AbortSignal.timeout(10_000) }),
    fetch(`https://data-api.polymarket.com/value?user=${encodeURIComponent(config.funderAddress)}`, { signal: AbortSignal.timeout(10_000) })
  ]);
  if (!Number.isFinite(clockDriftMs)) throw new Error("fail closed: venue clock is invalid");
  const feeRate = Number(clobMarketInfo?.fd?.r ?? 0);
  const feeExponent = Number(clobMarketInfo?.fd?.e ?? 0);
  const feeTakerOnly = clobMarketInfo?.fd?.to === true || feeRate === 0;
  const feeRateBps = feeRate * 10_000;
  if (!Number.isFinite(feeRate) || feeRate < 0 || feeRate > 1 ||
      !Number.isFinite(feeExponent) || feeExponent < 0 || feeExponent > 10 ||
      !Number.isFinite(feeRateBps) || feeRateBps < 0 || feeRateBps > 10_000 ||
      (feeRate > 0 && !feeTakerOnly)) {
    throw new Error("fail closed: Polymarket V2 market fee rate/exponent/taker-only parameters are invalid");
  }
  if (!positionsResponse.ok || !valueResponse.ok) throw new Error("fail closed: account reconciliation endpoint failed");
  const positions = await positionsResponse.json();
  const reportedValues = await valueResponse.json();
  if (!Array.isArray(positions) || !Array.isArray(reportedValues)) throw new Error("fail closed: account reconciliation payload is invalid");
  const terminalConditionIds = [...new Set(positions
    .filter((row) => row.redeemable === true && row.conditionId)
    .map((row) => String(row.conditionId)))];
  if (terminalConditionIds.length) {
    await settleProbeRiskReservations(config, {
      condition_ids: terminalConditionIds,
      terminal_settlement_verified: true,
      evidence_source: "polymarket_data_api_redeemable",
      run_id: runId
    });
  }
  const reservations = await loadUnresolvedRiskReservations(config);
  const principal = Number(intent.notional);
  const feeRisk = Number(intent.shares) * polymarketV2FeePerShare(intent.price, feeRate, feeExponent);
  const risk = summarizeCampaignRisk({
    control: riskControl,
    liquidCollateral: Number(balance.balance) / 1_000_000,
    summedPositionValue: positions.reduce((sum, row) => sum + Math.max(0, Number(row.currentValue) || 0), 0),
    reportedPositionValue: reportedValues.reduce((sum, row) => sum + Math.max(0, Number(row.value) || 0), 0),
    openOrderCount: openOrders.length,
    unresolvedPositionCount: positions.filter((row) => Number(row.size) > 1e-9 && row.redeemable !== true).length,
    unresolvedReservationCount: reservations.filter((row) => String(row.probe_id) !== String(ignoredReservationId || "")).length,
    proposedNotional: principal + feeRisk,
    orderNotional: principal
  });
  return {
    geoblock,
    clockDriftMs,
    clockServerMinusLocalMs,
    clockRoundTripMs,
    clockUncertaintyMs,
    market,
    book,
    feeModel: "polymarket_clob_v2_curve",
    feeRate,
    feeRateBps,
    feeExponent,
    feeTakerOnly,
    risk,
    openOrderCount: openOrders.length,
    fillModelVersion: config.requiredFillModelVersion,
    exactResolutionSource: intent.exact_resolution_source === true,
    resolutionSource: intent.resolution_source,
    client
  };
}

async function executeLifecycle(client, { intent, documents, runtime, reservation }) {
  let refreshed;
  let preSendCapturedWallMs;
  let preSendContext;
  try {
    lease.assertHealthy();
    // Both channels are opened before signing so partial fills, cancellation races,
    // public trade-through, and markout evidence have no intentional blind window.
    userChannel = await connectLifecycleChannel({
      url: config.userWsUrl,
      subscription: {
        auth: { apiKey: config.apiKey, secret: config.apiSecret, passphrase: config.apiPassphrase },
        markets: [intent.condition_id],
        type: "user"
      },
      ledger,
      eventType: "venue_user_channel"
    });
    marketChannel = await connectLifecycleChannel({
      url: config.marketWsUrl,
      subscription: {
        assets_ids: [intent.token_id],
        type: "market",
        custom_feature_enabled: true
      },
      ledger,
      eventType: "venue_market_channel"
    });
    refreshed = await capturePreflight(client, intent, reservation.probe_id);
    // Repeat the full immutable-intent, book, risk, clock, geoblock, model, and
    // authorization contract immediately before the only signing call.
    validateCanaryPreflight({ config, ...documents, runtime: refreshed, now: new Date() });
    lease.assertHealthy();
    await Promise.all([userChannel.ensureOpen(), marketChannel.ensureOpen()]);
    if (userChannel.gapCount() > 0 || marketChannel.gapCount() > 0 ||
        userChannel.unparsedCount() > 0 || marketChannel.unparsedCount() > 0) {
      throw new Error("fail closed: authenticated/public websocket completeness was lost before submission");
    }
    preSendCapturedWallMs = Date.now();
    preSendContext = {
      ...marketContext(marketMessagesThrough(marketChannel.messages, preSendCapturedWallMs)),
      source: "public_market_channel_before_submission",
      captured_wall_ms: preSendCapturedWallMs
    };
  } catch (error) {
    try {
      await finalizeProbeRisk(config, reservation, {
        state: "released_no_order",
        order_submitted: false,
        matched_notional: 0,
        reconciliation_complete: true,
        zero_open_orders_confirmed: true
      });
    } catch (releaseError) {
      throw new Error(`fail closed: pre-submit lifecycle failed and no-order risk release also failed (${error.message}; ${releaseError.message})`);
    }
    throw error;
  }
  const expiration = Math.floor(Date.parse(intent.gtd_expiry_ts) / 1000);
  while (Date.now() <= preSendCapturedWallMs) await sleep(1);
  const sentAt = new Date();
  const sentMonotonicMs = performance.now();
  let response;
  try {
    orderSubmissionAttempted = true;
    ledger.record("venue_order_send", {
      probe_id: reservation.probe_id,
      active_valid_until: intent.valid_until,
      venue_gtd_expiry_ts: intent.gtd_expiry_ts,
      order: { token_id: intent.token_id, price: intent.price, shares: intent.shares, post_only: true }
    });
    response = await client.createAndPostOrder(
      { tokenID: intent.token_id, price: Number(intent.price), size: Number(intent.shares), side: Side.BUY, expiration },
      { tickSize: String(refreshed.book.tick_size ?? refreshed.book.tickSize), negRisk: refreshed.book.neg_risk === true || refreshed.book.negRisk === true },
      OrderType.GTD,
      true
    );
  } catch (error) {
    await cancelAllAndConfirm(client);
    throw new Error(`fail closed: ambiguous strategy-canary submission; authorization is consumed and risk remains reserved (${error.message})`);
  }
  if (!response?.success || !response.orderID || !["live", "matched"].includes(String(response.status).toLowerCase())) {
    await cancelAllAndConfirm(client);
    throw new Error(`fail closed: canary order was not acknowledged (${response?.status || response?.errorMsg || "unknown"})`);
  }
  const acknowledgedAt = new Date();
  const acknowledgementLatencyMs = Math.max(0, performance.now() - sentMonotonicMs);
  const orderId = String(response.orderID);
  ledger.record("venue_order_http_ack", { probe_id: reservation.probe_id, order_id: orderId, response, client_to_http_ack_ms: acknowledgementLatencyMs });
  let markoutCapture;
  try {
    markoutCapture = beginFillMarkoutCapture(
      client,
      intent.token_id,
      () => normalizeFillClock(
        tradeFillsFromUserEvents(userChannel.messages, orderId),
        refreshed.clockServerMinusLocalMs
      ),
      {
        feeParameters: {
          rate: refreshed.feeRate,
          rateBps: refreshed.feeRateBps,
          exponent: refreshed.feeExponent,
          takerOnly: refreshed.feeTakerOnly
        }
      }
    );
    await finalizeProbeRisk(config, reservation, {
      state: "submitted_pending_reconciliation",
      order_submitted: true,
      order_id: orderId,
      matched_notional: 0,
      reconciliation_complete: false,
      zero_open_orders_confirmed: false
    });
    const plannedRestMs = Math.min(
      config.restSeconds * 1_000,
      Math.max(0, Date.parse(intent.valid_until) - Date.now())
    );
    await sleep(plannedRestMs);
    const openBeforeCancel = (await getOpenOrdersStrict(client)).some((row) => String(row.id) === orderId);
    const cancellation = openBeforeCancel
      ? await cancelOrderWithMetrics(client, orderId, ledger)
      : {
          cancelSendWallMs: null,
          cancelResponseWallMs: Date.now(),
          cancelRoundTripMs: null,
          cancelResponse: { already_terminal: true },
          failedAttempts: 0
        };
    ledger.record("venue_cancel_http_response", {
      probe_id: reservation.probe_id,
      order_id: orderId,
      response: cancellation.cancelResponse,
      client_cancel_round_trip_ms: cancellation.cancelRoundTripMs
    });
    const reconciliation = await waitForStablePostCancelReconciliation({
      client,
      conditionId: intent.condition_id,
      orderId,
      userChannel,
      ledger,
      assertHealthy: () => lease.assertHealthy()
    });
    await Promise.all([userChannel.ensureOpen(), marketChannel.ensureOpen()]);
    const userFills = normalizeFillClock(
      tradeFillsFromUserEvents(reconciliation.userEvents, orderId),
      refreshed.clockServerMinusLocalMs
    );
    const restFills = normalizeFillClock(
      tradeFillsFromRest(reconciliation.relatedTrades, orderId),
      refreshed.clockServerMinusLocalMs
    );
    const fills = mergeTradeFills(userFills, restFills);
    const markouts = await markoutCapture.finish(fills);
    const restOrderMatched = Number(reconciliation.finalOrder?.size_matched || 0);
    const userOrderMatched = maximumMatchedSize(reconciliation.userEvents);
    const restTradesMatched = sum(restFills.map((fill) => fill.size));
    const userTradesMatched = sum(userFills.map((fill) => fill.size));
    const matchedShares = Math.max(restOrderMatched, userOrderMatched, restTradesMatched, userTradesMatched);
    const matchedSizeSourceAgreement = [restOrderMatched, userOrderMatched, restTradesMatched, userTradesMatched]
      .every((value) => nearlyEqualSize(value, matchedShares));
    const tradeIdSourceAgreement = sameStringSet(restFills.map((fill) => fill.id), userFills.map((fill) => fill.id));
    const restOrderReturned = Boolean(reconciliation.finalOrder);
    const reconciliationComplete = reconciliation.zeroOpenOrders && reconciliation.stableFinality &&
      reconciliation.terminalConfirmed && restOrderReturned && matchedSizeSourceAgreement && tradeIdSourceAgreement;
    const matchedRisk = matchedShares * (Number(intent.price) +
      polymarketV2FeePerShare(intent.price, refreshed.feeRate, refreshed.feeExponent));
    await finalizeProbeRisk(config, reservation, {
      state: reconciliationComplete
        ? (matchedShares > 0 ? "position_unresolved" : "finalized_no_fill")
        : "unresolved_reconciliation",
      order_submitted: true,
      order_id: orderId,
      matched_notional: matchedRisk,
      reconciliation_complete: reconciliationComplete,
      zero_open_orders_confirmed: reconciliation.zeroOpenOrders
    });
    if (!reconciliationComplete) throw new Error("canary lifecycle did not reconcile across REST and authenticated user channel");

    const terminalAt = new Date();
    const firstFillWallMs = firstFillTimestamp(fills);
    const cancellationReceivedWallMs = cancellationEventReceivedAt(reconciliation.userEvents);
    const order = evidenceOrder(intent, refreshed.book);
    const tradeThrough = publicTradeThroughStats(
      marketChannel.messages,
      order,
      acknowledgedAt.getTime(),
      cancellationReceivedWallMs ?? cancellation.cancelResponseWallMs ?? terminalAt.getTime(),
      fills
    );
    const cancelRace = postCancelFillStats(fills, cancellation.cancelSendWallMs);
    const fullContext = marketContext(marketChannel.messages);
    const dataGapDetected = !reconciliation.stableFinality ||
      userChannel.gapCount() > 0 || marketChannel.gapCount() > 0 ||
      userChannel.unparsedCount() > 0 || marketChannel.unparsedCount() > 0 ||
      (cancellation.cancelSendWallMs !== null && cancellationReceivedWallMs === null) ||
      (cancellation.cancelSendWallMs === null && matchedShares < Number(intent.shares)) ||
      (matchedShares > 0 && (!tradeIdSourceAgreement || !matchedSizeSourceAgreement));
    const terminalWallMs = cancellationReceivedWallMs ?? cancellation.cancelResponseWallMs ?? firstFillWallMs ?? terminalAt.getTime();
    const markoutCoverage = validateFillMarkouts(markouts, restFills.map((fill) => fill.id), matchedShares);
    const markoutCaptureComplete = markoutCoverage.complete && markoutCoverage.timing_valid;
    const lifecycle = {
      order_id: orderId,
      send_wall_ms: sentAt.getTime(),
      ack_wall_ms: acknowledgedAt.getTime(),
      submitted_ts: sentAt.toISOString(),
      acknowledged_ts: acknowledgedAt.toISOString(),
      client_to_http_ack_ms: acknowledgementLatencyMs,
      acknowledgement_latency_ms: acknowledgementLatencyMs,
      acknowledgement_latency_clock: "monotonic_performance_now",
      clock_server_minus_local_ms: refreshed.clockServerMinusLocalMs,
      clock_round_trip_ms: refreshed.clockRoundTripMs,
      clock_uncertainty_ms: refreshed.clockUncertaintyMs,
      fill_timestamp_clock: "venue_timestamp_normalized_to_local_wall_clock",
      cancel_send_wall_ms: cancellation.cancelSendWallMs,
      cancel_http_response_wall_ms: cancellation.cancelResponseWallMs,
      client_cancel_round_trip_ms: cancellation.cancelRoundTripMs,
      user_channel_cancel_received_wall_ms: cancellationReceivedWallMs,
      client_to_user_cancel_ack_ms: cancellation.cancelSendWallMs === null || cancellationReceivedWallMs === null
        ? null
        : cancellationReceivedWallMs - cancellation.cancelSendWallMs,
      cancel_requested_ts: cancellation.cancelSendWallMs === null ? null : new Date(cancellation.cancelSendWallMs).toISOString(),
      cancel_acknowledged_ts: cancellation.cancelSendWallMs === null
        ? null
        : new Date(cancellation.cancelResponseWallMs).toISOString(),
      cancel_failed_attempts: cancellation.failedAttempts,
      planned_rest_seconds: plannedRestMs / 1_000,
      planned_rest_until_ts: intent.valid_until,
      live_duration_ms: Math.max(0, terminalWallMs - acknowledgedAt.getTime()),
      first_fill_after_ack_ms: firstFillWallMs === null ? null : Math.max(0, firstFillWallMs - acknowledgedAt.getTime()),
      actual_matched_size: matchedShares,
      partial_fill: matchedShares > 0 && matchedShares < Number(intent.shares),
      fully_filled: matchedShares >= Number(intent.shares),
      post_cancel_fill_count: cancelRace.postCancelFillCount,
      first_fill_after_cancel_ms: cancelRace.firstFillAfterCancelMs,
      fill_raced_cancellation: cancelRace.postCancelFillCount > 0,
      public_touch_trade_count: tradeThrough.touch_count,
      public_strict_trade_through_count: tradeThrough.strict_trade_through_count,
      public_trade_through_without_fill_count: tradeThrough.trade_through_without_fill_count,
      venue_status: reconciliation.finalOrder?.status || "terminal_not_returned",
      venue_fee_model: refreshed.feeModel,
      venue_fee_rate: refreshed.feeRate,
      venue_fee_rate_bps: Number(refreshed.feeRateBps || 0),
      venue_fee_exponent: refreshed.feeExponent,
      venue_fee_taker_only: refreshed.feeTakerOnly,
      related_trade_ids: restFills.map((fill) => fill.id),
      live_user_trade_ids: userFills.map((fill) => fill.id),
      rest_order_matched_size: restOrderMatched,
      user_order_matched_size: userOrderMatched,
      rest_trade_matched_size: restTradesMatched,
      user_trade_matched_size: userTradesMatched,
      matched_size_source_agreement: matchedSizeSourceAgreement,
      trade_id_source_agreement: tradeIdSourceAgreement,
      rest_user_trade_ids_agree: tradeIdSourceAgreement,
      rest_order_returned: restOrderReturned,
      post_cancel_finality_stable: reconciliation.stableFinality,
      post_cancel_observation_ms: reconciliation.observationMs,
      authenticated_user_channel_reconnects: userChannel.reconnectCount(),
      public_market_channel_reconnects: marketChannel.reconnectCount(),
      authenticated_user_channel_duplicates: userChannel.duplicateCount(),
      public_market_channel_duplicates: marketChannel.duplicateCount(),
      authenticated_user_channel_unparsed: userChannel.unparsedCount(),
      public_market_channel_unparsed: marketChannel.unparsedCount(),
      reconciliation_complete: reconciliationComplete,
      zero_open_orders_confirmed: reconciliation.zeroOpenOrders,
      data_gap_detected: dataGapDetected,
      cancellation_failure: cancellation.failedAttempts > 0 && !reconciliation.zeroOpenOrders,
      markout_capture_complete: markoutCaptureComplete,
      public_trade_messages: marketChannel.messages.filter((row) => String(row.event_type || row.type).toLowerCase().includes("trade")).length
    };
    const market = {
      id: String(refreshed.market.marketId),
      conditionId: String(refreshed.market.conditionId),
      tokenId: String(refreshed.market.tokenId),
      endTs: refreshed.market.endTs || null
    };
    const observations = modelObservations({ order, market, lifecycle, context: preSendContext, markouts });
    lifecycle.estimated_round_trip_cost_per_share = observations[0]?.estimated_round_trip_cost_per_share ?? null;
    const exactEligibleHorizons = hasExactEligibleHorizons(observations, HORIZONS_SECONDS);
    const evidenceStatus = !dataGapDetected && markoutCaptureComplete && exactEligibleHorizons
      ? "completed"
      : "completed_ineligible";
    const evidenceProbe = {
      schema_version: 3,
      evidence_protocol_version: EVIDENCE_PROTOCOL_VERSION,
      probe_id: reservation.probe_id,
      status: evidenceStatus,
      started_ts: sentAt.toISOString(),
      finished_ts: terminalAt.toISOString(),
      order_submitted: true,
      market,
      order,
      context: fullContext,
      pre_send_context: preSendContext,
      lifecycle,
      markouts,
      model_observations: observations
    };
    return {
      ...lifecycle,
      fills: fills.map((fill) => ({ ...fill, markouts: markouts.filter((row) => row.fill_id === fill.id) })),
      evidence_probe: evidenceProbe
    };
  } catch (error) {
    if (markoutCapture) await markoutCapture.abort().catch(() => null);
    const emergency = await emergencyReconcileAfterAck(client, intent.condition_id, orderId);
    const matchedRisk = emergency.matchedShares * (Number(intent.price) +
      polymarketV2FeePerShare(intent.price, refreshed.feeRate, refreshed.feeExponent));
    let reservationPersistenceError = null;
    try {
      await finalizeProbeRisk(config, reservation, {
        state: "unresolved_reconciliation",
        order_submitted: true,
        order_id: orderId,
        matched_notional: matchedRisk,
        reconciliation_complete: false,
        zero_open_orders_confirmed: emergency.zeroOpenOrders
      });
    } catch (persistenceError) {
      reservationPersistenceError = persistenceError;
    }
    ledger.record("strategy_canary_post_ack_failed_closed", {
      probe_id: reservation.probe_id,
      order_id: orderId,
      zero_open_orders_confirmed: emergency.zeroOpenOrders,
      matched_shares: emergency.matchedShares,
      error: error.message
    });
    await uploadFailedPostAckEvidence({
      intent,
      runtime: refreshed,
      reservation,
      orderId,
      acknowledgedAt,
      sentAt,
      acknowledgementLatencyMs,
      preSendContext,
      emergency,
      originalError: error
    }).catch((uploadError) => {
      ledger.record("strategy_canary_failed_evidence_upload", { error: uploadError.message });
    });
    if (reservationPersistenceError) {
      throw new Error(`fail closed: post-ack error and unresolved reservation persistence failed; prior durable reservation remains blocking (${error.message}; ${reservationPersistenceError.message})`);
    }
    if (!emergency.zeroOpenOrders) {
      throw new Error(`fail closed: post-ack error and emergency zero-open confirmation failed; unresolved risk preserved (${error.message})`);
    }
    throw new Error(`fail closed: post-ack error; tracked order canceled, zero open orders confirmed, unresolved risk preserved (${error.message})`);
  }
}

async function uploadFailedPostAckEvidence({ intent, runtime, reservation, orderId, acknowledgedAt, sentAt, acknowledgementLatencyMs, preSendContext, emergency, originalError }) {
  const finishedAt = new Date();
  const context = marketChannel ? marketContext(marketChannel.messages) : {
    observed_trade_count: 0,
    observed_trade_size: 0,
    observed_depth_changes: 0,
    price_volatility: 0
  };
  const order = evidenceOrder(intent, runtime.book);
  const lifecycle = {
    order_id: orderId,
    send_wall_ms: sentAt.getTime(),
    ack_wall_ms: acknowledgedAt.getTime(),
    submitted_ts: sentAt.toISOString(),
    acknowledged_ts: acknowledgedAt.toISOString(),
    client_to_http_ack_ms: acknowledgementLatencyMs,
    acknowledgement_latency_ms: acknowledgementLatencyMs,
    live_duration_ms: Math.max(0, finishedAt.getTime() - acknowledgedAt.getTime()),
    first_fill_after_ack_ms: null,
    actual_matched_size: emergency.matchedShares,
    related_trade_ids: [],
    venue_fee_model: runtime.feeModel,
    venue_fee_rate: runtime.feeRate,
    venue_fee_rate_bps: Number(runtime.feeRateBps || 0),
    venue_fee_exponent: runtime.feeExponent,
    venue_fee_taker_only: runtime.feeTakerOnly,
    reconciliation_complete: false,
    zero_open_orders_confirmed: emergency.zeroOpenOrders,
    data_gap_detected: true,
    cancellation_failure: !emergency.zeroOpenOrders
  };
  const market = {
    id: String(runtime.market.marketId),
    conditionId: String(runtime.market.conditionId),
    tokenId: String(runtime.market.tokenId),
    endTs: runtime.market.endTs || null
  };
  const observations = modelObservations({ order, market, lifecycle, context: preSendContext, markouts: [] });
  lifecycle.estimated_round_trip_cost_per_share = observations[0]?.estimated_round_trip_cost_per_share ?? null;
  const probe = {
    schema_version: 3,
    evidence_protocol_version: EVIDENCE_PROTOCOL_VERSION,
    probe_id: reservation.probe_id,
    status: "completed_ineligible",
    started_ts: sentAt.toISOString(),
    finished_ts: finishedAt.toISOString(),
    order_submitted: true,
    market,
    order,
    context,
    pre_send_context: preSendContext,
    lifecycle,
    markouts: [],
    model_observations: observations,
    error: originalError.message
  };
  const summary = {
    schema_version: 3,
    evidence_protocol_version: EVIDENCE_PROTOCOL_VERSION,
    run_id: runId,
    status: "failed_closed",
    started_ts: sentAt.toISOString(),
    finished_ts: finishedAt.toISOString(),
    order_submission_attempted: true,
    order_submitted: true,
    submitted_order_count: 1,
    completed_probe_count: 0,
    probes: [probe],
    market,
    order,
    pre_send_context: preSendContext,
    lifecycle,
    markouts: [],
    model_observations: observations,
    queue_position_source: "authenticated_lifecycle_plus_public_l2",
    queue_position_metric: "inferred_size_ahead",
    literal_fifo_rank_available: false,
    research_only: true,
    live_trading_enabled: false
  };
  return uploadEvidence(config, runId, summary, ledger);
}

function evidenceOrder(intent, book) {
  const price = Number(intent.price);
  const bids = (book.bids || []).map((row) => ({ price: Number(row.price), size: Number(row.size) }));
  const asks = (book.asks || []).map((row) => ({ price: Number(row.price), size: Number(row.size) }));
  const bestBid = bids.length ? Math.max(...bids.map((row) => row.price)) : null;
  const bestAsk = asks.length ? Math.min(...asks.map((row) => row.price)) : null;
  const samePrice = bids.filter((row) => row.price === price).reduce((sum, row) => sum + row.size, 0);
  const betterPrice = bids.filter((row) => row.price > price).reduce((sum, row) => sum + row.size, 0);
  return {
    side: "BUY",
    price,
    size: Number(intent.shares),
    notional: Number(intent.notional),
    post_only: true,
    spread: bestBid === null || bestAsk === null ? null : bestAsk - bestBid,
    samePricePublicSize: samePrice,
    betterPricePublicSize: betterPrice,
    inferredSizeAhead: samePrice + betterPrice,
    minimumOrderSize: Number(intent.minimum_order_size)
  };
}

async function emergencyReconcileAfterAck(client, conditionId, orderId) {
  await client.cancelOrder({ orderID: orderId }).catch(() => null);
  await client.cancelAll().catch(() => null);
  const reconciliation = await waitForStablePostCancelReconciliation({
    client,
    conditionId,
    orderId,
    userChannel: userChannel || { messages: [], ensureOpen: async () => true },
    ledger
  }).catch(() => null);
  const openOrders = await getOpenOrdersStrict(client).catch(() => null);
  const zeroOpenOrders = Array.isArray(openOrders) && openOrders.length === 0;
  const restFills = reconciliation ? tradeFillsFromRest(reconciliation.relatedTrades, orderId) : [];
  const matchedShares = Math.max(
    Number(reconciliation?.finalOrder?.size_matched || 0),
    restFills.reduce((sum, fill) => sum + fill.size, 0)
  );
  return { zeroOpenOrders, matchedShares };
}

async function loadExactMarket(intent) {
  const values = await fetchJson(`${config.gammaUrl}/markets?id=${encodeURIComponent(intent.market_id)}`);
  const market = Array.isArray(values) ? values[0] : null;
  if (!market) throw new Error("fail closed: intent market was not found at the venue");
  const tokenIds = parseArray(market.clobTokenIds).map(String);
  if (!tokenIds.includes(String(intent.token_id))) throw new Error("fail closed: intent token is not part of the venue market");
  return {
    marketId: String(market.id),
    conditionId: String(market.conditionId),
    tokenId: String(intent.token_id),
    endTs: market.endDate || market.end_date || null,
    closed: market.closed === true,
    acceptingOrders: market.acceptingOrders !== false && market.enableOrderBook !== false
  };
}

async function cancelAllAndConfirm(client) {
  await client.cancelAll().catch(() => null);
  if ((await getOpenOrdersStrict(client)).length) throw new Error("fail closed: emergency cancellation did not produce zero open orders");
}

async function getOpenOrdersStrict(client) {
  const value = await client.getOpenOrders();
  if (!Array.isArray(value)) throw new Error("fail closed: venue open-order response is invalid");
  return value;
}

function normalizeFillClock(fills, serverMinusLocalMs) {
  if (!Number.isFinite(Number(serverMinusLocalMs))) {
    throw new Error("fail closed: signed venue clock offset is unavailable for authenticated fills");
  }
  return (fills || []).map((fill) => {
    const venueTimestampMs = Number(fill.timestampMs);
    if (!Number.isFinite(venueTimestampMs)) {
      throw new Error("fail closed: authenticated fill timestamp is invalid");
    }
    return {
      ...fill,
      venueTimestampMs,
      timestampMs: venueTimestampMs - Number(serverMinusLocalMs)
    };
  });
}

function normalizePrivateKey(value) { const clean = String(value || "").trim(); return clean.startsWith("0x") ? clean : `0x${clean}`; }
function parseArray(value) { if (Array.isArray(value)) return value; try { return JSON.parse(value || "[]"); } catch { return []; } }
async function fetchJson(url) { const response = await fetch(url, { signal: AbortSignal.timeout(10_000) }); if (!response.ok) throw new Error(`HTTP ${response.status} from ${url}`); return response.json(); }
function sleep(ms) { return new Promise((resolve) => setTimeout(resolve, ms)); }
