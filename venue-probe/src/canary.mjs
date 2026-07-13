import { AssetType, Chain, ClobClient, OrderType, Side } from "@polymarket/clob-client-v2";
import { createWalletClient, http } from "viem";
import { privateKeyToAccount } from "viem/accounts";
import { polygon } from "viem/chains";
import WebSocket from "ws";
import {
  EVIDENCE_PROTOCOL_VERSION,
  EventLedger,
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
  uploadEvidence
} from "./lib.mjs";
import {
  consumeOneShotAuthorization,
  beginFillMarkoutCapture,
  artifactLocationFromUri,
  executeStrategyCanary,
  loadCanaryConfig,
  loadHashedJson,
  validateCanaryPreflight
} from "./canary-lib.mjs";

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
  const clockDriftMs = Math.abs((requestStarted + requestFinished) / 2 - serverMs);
  const market = await loadExactMarket(intent);
  const [book, feeRateBps, openOrders, riskControl, balance, positionsResponse, valueResponse] = await Promise.all([
    client.getOrderBook(String(intent.token_id)),
    client.getFeeRateBps(String(intent.token_id)).then(Number),
    getOpenOrdersStrict(client),
    loadCampaignRiskControl(config),
    client.getBalanceAllowance({ asset_type: AssetType.COLLATERAL, signature_type: config.signatureType }),
    fetch(`https://data-api.polymarket.com/positions?user=${encodeURIComponent(config.funderAddress)}&sizeThreshold=0&limit=500`, { signal: AbortSignal.timeout(10_000) }),
    fetch(`https://data-api.polymarket.com/value?user=${encodeURIComponent(config.funderAddress)}`, { signal: AbortSignal.timeout(10_000) })
  ]);
  if (!Number.isFinite(clockDriftMs)) throw new Error("fail closed: venue clock is invalid");
  if (!Number.isFinite(feeRateBps) || feeRateBps < 0 || feeRateBps > 10_000) throw new Error("fail closed: venue fee rate is invalid");
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
  const feeRisk = principal * feeRateBps / 10_000;
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
    market,
    book,
    feeRateBps,
    risk,
    openOrderCount: openOrders.length,
    fillModelVersion: config.requiredFillModelVersion,
    exactResolutionSource: intent.exact_resolution_source === true,
    resolutionSource: intent.resolution_source,
    client
  };
}

async function executeLifecycle(client, { intent, documents, runtime, reservation }) {
  lease.assertHealthy();
  // Both channels are opened before signing so partial fills, cancellation races,
  // public trade-through, and markout evidence have no intentional blind window.
  userChannel = await openChannel(config.userWsUrl, {
    auth: { apiKey: config.apiKey, secret: config.apiSecret, passphrase: config.apiPassphrase },
    markets: [intent.condition_id],
    type: "user"
  });
  marketChannel = await openChannel(config.marketWsUrl, {
    assets_ids: [intent.token_id],
    type: "market",
    custom_feature_enabled: true
  });
  const refreshed = await capturePreflight(client, intent, reservation.probe_id);
  // Repeat the full immutable-intent, book, risk, clock, geoblock, model, and
  // authorization contract immediately before the only signing call.
  validateCanaryPreflight({ config, ...documents, runtime: refreshed, now: new Date() });
  lease.assertHealthy();
  const expiration = Math.floor(Date.parse(intent.gtd_expiry_ts) / 1000);
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
      { tickSize: String(runtime.book.tick_size ?? runtime.book.tickSize), negRisk: runtime.book.neg_risk === true || runtime.book.negRisk === true },
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
  ledger.record("venue_order_http_ack", { probe_id: reservation.probe_id, order_id: orderId, response, acknowledgement_latency_ms: acknowledgementLatencyMs });
  let markoutCapture;
  try {
    await finalizeProbeRisk(config, reservation, {
      state: "submitted_pending_reconciliation",
      order_submitted: true,
      order_id: orderId,
      matched_notional: 0,
      reconciliation_complete: false,
      zero_open_orders_confirmed: false
    });
    markoutCapture = beginFillMarkoutCapture(
      client,
      intent.token_id,
      () => fillsFromUserChannel(userChannel.messages, orderId)
    );
    await sleep(Math.min(config.restSeconds * 1000, Math.max(0, Date.parse(intent.valid_until) - Date.now() - 1000)));
    const openBeforeCancel = (await getOpenOrdersStrict(client)).some((row) => String(row.id) === orderId);
    const cancelRequestedAt = openBeforeCancel ? new Date() : null;
    if (openBeforeCancel) await cancelWithRetries(client, orderId);
    const cancelAcknowledgedAt = openBeforeCancel ? new Date() : null;
    const reconciliation = await reconcile(client, intent.condition_id, orderId);
    const orderTerminalAt = new Date();
    const userFills = fillsFromUserChannel(userChannel.messages, orderId);
    const restFills = fillsFromTrades(reconciliation.trades, orderId);
    const fills = mergeFills(userFills, restFills);
    const markouts = await markoutCapture.finish(fills);
    const matchedShares = Math.max(
      Number(reconciliation.order?.size_matched || 0),
      userFills.reduce((sum, fill) => sum + fill.size, 0),
      restFills.reduce((sum, fill) => sum + fill.size, 0)
    );
    const restIds = new Set(restFills.map((fill) => fill.id));
    const userIds = new Set(userFills.map((fill) => fill.id));
    const tradeIdsAgree = restIds.size === userIds.size && [...restIds].every((id) => userIds.has(id));
    const reconciliationComplete = reconciliation.zeroOpenOrders && reconciliation.terminal && tradeIdsAgree;
    const matchedRisk = matchedShares * Number(intent.price) * (1 + Number(runtime.feeRateBps || 0) / 10_000);
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
    const firstFillWallMs = fills.length ? Math.min(...fills.map((fill) => fill.timestampMs)) : null;
    const context = marketContext(marketChannel.messages);
    const order = evidenceOrder(intent, runtime.book);
    const lifecycle = {
      order_id: orderId,
      send_wall_ms: sentAt.getTime(),
      ack_wall_ms: acknowledgedAt.getTime(),
      submitted_ts: sentAt.toISOString(),
      acknowledged_ts: acknowledgedAt.toISOString(),
      acknowledgement_latency_ms: acknowledgementLatencyMs,
      acknowledgement_latency_clock: "monotonic_performance_now",
      cancel_requested_ts: cancelRequestedAt?.toISOString() || null,
      cancel_acknowledged_ts: cancelAcknowledgedAt?.toISOString() || null,
      live_duration_ms: orderTerminalAt.getTime() - acknowledgedAt.getTime(),
      first_fill_after_ack_ms: firstFillWallMs === null ? null : Math.max(0, firstFillWallMs - acknowledgedAt.getTime()),
      actual_matched_size: matchedShares,
      partial_fill: matchedShares > 0 && matchedShares < Number(intent.shares),
      fully_filled: matchedShares >= Number(intent.shares),
      venue_fee_rate_bps: Number(runtime.feeRateBps || 0),
      related_trade_ids: fills.map((fill) => fill.id),
      rest_user_trade_ids_agree: tradeIdsAgree,
      zero_open_orders_confirmed: true,
      reconciliation_complete: true,
      data_gap_detected: false,
      cancellation_failure: false,
      public_trade_messages: marketChannel.messages.filter((row) => String(row.event_type || row.type).toLowerCase().includes("trade")).length
    };
    const market = {
      id: String(runtime.market.marketId),
      conditionId: String(runtime.market.conditionId),
      tokenId: String(runtime.market.tokenId),
      endTs: runtime.market.endTs || null
    };
    const observations = modelObservations({ order, market, lifecycle, context, markouts });
    const evidenceProbe = {
      schema_version: 3,
      evidence_protocol_version: EVIDENCE_PROTOCOL_VERSION,
      probe_id: reservation.probe_id,
      status: "completed",
      started_ts: sentAt.toISOString(),
      finished_ts: terminalAt.toISOString(),
      order_submitted: true,
      market,
      order,
      context,
      pre_send_context: context,
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
    const matchedRisk = emergency.matchedShares * Number(intent.price) * (1 + Number(runtime.feeRateBps || 0) / 10_000);
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
      runtime,
      reservation,
      orderId,
      acknowledgedAt,
      sentAt,
      acknowledgementLatencyMs,
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

async function uploadFailedPostAckEvidence({ intent, runtime, reservation, orderId, acknowledgedAt, sentAt, acknowledgementLatencyMs, emergency, originalError }) {
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
    acknowledgement_latency_ms: acknowledgementLatencyMs,
    live_duration_ms: Math.max(0, finishedAt.getTime() - acknowledgedAt.getTime()),
    first_fill_after_ack_ms: null,
    actual_matched_size: emergency.matchedShares,
    related_trade_ids: [],
    venue_fee_rate_bps: Number(runtime.feeRateBps || 0),
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
  const observations = modelObservations({ order, market, lifecycle, context, markouts: [] });
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
    pre_send_context: context,
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
    pre_send_context: context,
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
  const reconciliation = await reconcile(client, conditionId, orderId).catch(() => null);
  const openOrders = await getOpenOrdersStrict(client).catch(() => null);
  const zeroOpenOrders = Array.isArray(openOrders) && openOrders.length === 0;
  const restFills = reconciliation ? fillsFromTrades(reconciliation.trades, orderId) : [];
  const matchedShares = Math.max(
    Number(reconciliation?.order?.size_matched || 0),
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

async function openChannel(url, subscription) {
  const ws = new WebSocket(url);
  const messages = [];
  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("fail closed: websocket open timeout")), 8000);
    ws.once("open", () => { clearTimeout(timer); resolve(); });
    ws.once("error", reject);
  });
  ws.on("message", (data) => {
    const text = data.toString();
    if (text === "PONG") return;
    try { const value = JSON.parse(text); messages.push(...(Array.isArray(value) ? value : [value]).map((row) => ({ ...row, _received_wall_ms: Date.now() }))); }
    catch { /* Unparseable messages are excluded and reconciliation fails closed. */ }
  });
  ws.send(JSON.stringify(subscription));
  await sleep(250);
  return { messages, close: () => ws.close() };
}

async function cancelWithRetries(client, orderId) {
  let lastError;
  for (let attempt = 1; attempt <= 3; attempt += 1) {
    try { return await client.cancelOrder({ orderID: orderId }); }
    catch (error) { lastError = error; await sleep(200 * attempt); }
  }
  await client.cancelAll();
  if ((await getOpenOrdersStrict(client)).length) throw new Error(`fail closed: canary cancellation failed (${lastError?.message || "unknown"})`);
}

async function cancelAllAndConfirm(client) {
  await client.cancelAll().catch(() => null);
  if ((await getOpenOrdersStrict(client)).length) throw new Error("fail closed: emergency cancellation did not produce zero open orders");
}

async function reconcile(client, conditionId, orderId) {
  let last;
  for (let attempt = 0; attempt < 15; attempt += 1) {
    const [openOrders, order, trades] = await Promise.all([
      getOpenOrdersStrict(client),
      client.getOrder(orderId).catch(() => null),
      client.getTrades({ market: conditionId }).catch(() => [])
    ]);
    const status = String(order?.status || "").toUpperCase();
    last = { order, trades: Array.isArray(trades) ? trades : [], zeroOpenOrders: !openOrders.some((row) => String(row.id) === orderId), terminal: ["CANCELED", "CANCELLED", "MATCHED", "FILLED", "EXPIRED"].some((value) => status.includes(value)) };
    if (last.zeroOpenOrders && last.terminal) return last;
    await sleep(400);
  }
  return last || { order: null, trades: [], zeroOpenOrders: false, terminal: false };
}

function fillsFromUserChannel(messages, orderId) {
  return messages.flatMap((row) => {
    const type = String(row.event_type || row.type || "").toLowerCase();
    if (!type.includes("trade") || ![row.maker_order_id, row.taker_order_id, row.order_id].map(String).includes(String(orderId))) return [];
    return [{ id: String(row.id || row.trade_id || row.transaction_hash || ""), size: Number(row.size || row.matched_amount || 0), price: Number(row.price || 0), timestampMs: epochMs(row.timestamp || row.match_time || row.created_at) }];
  }).filter(validFill);
}

function fillsFromTrades(trades, orderId) {
  return (trades || []).flatMap((row) => {
    const ids = [row.maker_order_id, row.taker_order_id, row.order_id, ...(row.maker_orders || []).map((item) => item.order_id)].map(String);
    if (!ids.includes(String(orderId))) return [];
    return [{ id: String(row.id || row.trade_id || row.transaction_hash || ""), size: Number(row.size || row.amount || 0), price: Number(row.price || 0), timestampMs: epochMs(row.match_time || row.timestamp || row.created_at) }];
  }).filter(validFill);
}

function mergeFills(left, right) {
  return [...new Map([...left, ...right].map((row) => [row.id, row])).values()];
}

async function getOpenOrdersStrict(client) {
  const value = await client.getOpenOrders();
  if (!Array.isArray(value)) throw new Error("fail closed: venue open-order response is invalid");
  return value;
}

function validFill(row) { return row.id && row.size > 0 && row.price > 0 && Number.isFinite(row.timestampMs); }
function epochMs(value) { const number = Number(value); if (Number.isFinite(number)) return number < 1e12 ? number * 1000 : number; return Date.parse(value); }
function normalizePrivateKey(value) { const clean = String(value || "").trim(); return clean.startsWith("0x") ? clean : `0x${clean}`; }
function parseArray(value) { if (Array.isArray(value)) return value; try { return JSON.parse(value || "[]"); } catch { return []; } }
async function fetchJson(url) { const response = await fetch(url, { signal: AbortSignal.timeout(10_000) }); if (!response.ok) throw new Error(`HTTP ${response.status} from ${url}`); return response.json(); }
function sleep(ms) { return new Promise((resolve) => setTimeout(resolve, ms)); }
