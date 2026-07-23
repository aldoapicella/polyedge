import { createHash } from "node:crypto";

const EXECUTION_INTENT_SCHEMA = "polyedge.execution_intent.v1";
const AUTHORIZATION_SCHEMA = "polyedge.strategy_canary_authorization.v1";
const FUNDED_AUTHORIZATION_SCHEMA = "polyedge.funded_stage_intent_authorization.v1";
const PROMOTION_MANIFEST_SCHEMA = "promotion_manifest_v1";
const VENUE_GTD_SECURITY_BUFFER_MS = 60_000;
const MAX_ACTIVE_INTENT_TTL_MS = 30_000;

export function loadCanaryConfig(env = process.env) {
  const config = {
    executionMode: env.EXECUTION_MODE || "strategy_canary",
    allowLive: boolean(env.ALLOW_LIVE),
    allowCanary: boolean(env.ALLOW_STRATEGY_CANARY),
    enableTakerOrders: boolean(env.ENABLE_TAKER_ORDERS),
    dryRun: env.STRATEGY_CANARY_DRY_RUN !== "false",
    trustBoundaryReady: boolean(env.FUNDED_EVIDENCE_TRUST_BOUNDARY_READY),
    intentBlobName: clean(env.STRATEGY_CANARY_INTENT_BLOB_NAME),
    intentBlobHash: normalizeHash(env.STRATEGY_CANARY_INTENT_SHA256),
    manifestBlobName: clean(env.STRATEGY_CANARY_PROMOTION_MANIFEST_BLOB_NAME),
    manifestBlobHash: normalizeHash(env.STRATEGY_CANARY_PROMOTION_MANIFEST_SHA256),
    authorizationBlobName: clean(env.STRATEGY_CANARY_AUTHORIZATION_BLOB_NAME),
    authorizationBlobHash: normalizeHash(env.STRATEGY_CANARY_AUTHORIZATION_SHA256),
    humanGrantId: clean(env.STRATEGY_CANARY_HUMAN_GRANT_ID),
    humanGrantHash: normalizeHash(env.STRATEGY_CANARY_HUMAN_GRANT_SHA256),
    humanGrantConsumptionBlobName: clean(env.STRATEGY_CANARY_HUMAN_GRANT_CONSUMPTION_BLOB_NAME),
    humanGrantConsumptionHash: normalizeHash(env.STRATEGY_CANARY_HUMAN_GRANT_CONSUMPTION_SHA256),
    candidateName: clean(env.STRATEGY_CANARY_CANDIDATE_NAME || "dynamic_quote_style"),
    candidateVersion: clean(env.STRATEGY_CANARY_CANDIDATE_VERSION || "dynamic_quote_style@2026-06-14"),
    candidateConfigHash: normalizeHash(env.STRATEGY_CANARY_CANDIDATE_CONFIG_HASH),
    requiredFillModelVersion: clean(env.STRATEGY_CANARY_REQUIRED_FILL_MODEL_VERSION),
    executionModelBlobUri: clean(env.STRATEGY_CANARY_EXECUTION_MODEL_BLOB_URI),
    executionModelHash: normalizeHash(env.STRATEGY_CANARY_EXECUTION_MODEL_SHA256),
    requiredResolutionSource: clean(env.STRATEGY_CANARY_REQUIRED_RESOLUTION_SOURCE || "chainlink_reference"),
    expectedCountry: clean(env.VENUE_PROBE_EXPECTED_COUNTRY).toUpperCase(),
    expectedEgressIp: clean(env.VENUE_PROBE_EXPECTED_EGRESS_IP),
    maxClockDriftMs: integer(env.VENUE_PROBE_MAX_CLOCK_DRIFT_MS, 5000),
    maxClockUncertaintyMs: integer(env.VENUE_PROBE_MAX_CLOCK_UNCERTAINTY_MS, 750),
    maxOrderNotional: number(env.STRATEGY_CANARY_MAX_ORDER_NOTIONAL, 1),
    maxReferenceAgeMs: integer(env.STRATEGY_CANARY_MAX_REFERENCE_AGE_MS, 2000),
    maxBookAgeMs: integer(env.STRATEGY_CANARY_MAX_BOOK_AGE_MS, 1000),
    restSeconds: integer(env.STRATEGY_CANARY_REST_SECONDS, 1),
    storageAccount: env.AZURE_STORAGE_ACCOUNT_NAME,
    storageContainer: env.AZURE_STORAGE_CONTAINER_NAME || "bot-events",
    intentContainerName: clean(env.STRATEGY_CANARY_INTENT_CONTAINER_NAME || env.AZURE_STORAGE_CONTAINER_NAME || "bot-events"),
    manifestContainerName: clean(env.STRATEGY_CANARY_MANIFEST_CONTAINER_NAME || env.AZURE_STORAGE_CONTAINER_NAME || "bot-events"),
    storageAccountKey: env.AZURE_STORAGE_ACCOUNT_KEY,
    azureClientId: env.AZURE_CLIENT_ID,
    clobUrl: env.POLYMARKET_CLOB_URL || "https://clob.polymarket.com",
    gammaUrl: env.POLYMARKET_GAMMA_URL || "https://gamma-api.polymarket.com",
    marketWsUrl: env.POLYMARKET_MARKET_WS_URL || "wss://ws-subscriptions-clob.polymarket.com/ws/market",
    userWsUrl: env.POLYMARKET_USER_WS_URL || "wss://ws-subscriptions-clob.polymarket.com/ws/user",
    privateKey: env.POLYMARKET_PRIVATE_KEY,
    apiKey: env.POLYMARKET_API_KEY,
    apiSecret: env.POLYMARKET_API_SECRET,
    apiPassphrase: env.POLYMARKET_API_PASSPHRASE,
    funderAddress: env.POLYMARKET_FUNDER_ADDRESS,
    signatureType: integer(env.POLYMARKET_SIGNATURE_TYPE, 3),
    campaignId: clean(env.VENUE_PROBE_FUNDED_CAMPAIGN_ID || "funded-campaign-2026-07-12"),
    campaignBaselineEquity: number(env.VENUE_PROBE_CAMPAIGN_BASELINE_EQUITY, 5.030521),
    campaignEquityFloor: number(env.VENUE_PROBE_CAMPAIGN_EQUITY_FLOOR, 4.03),
    maxCampaignDrawdown: number(env.VENUE_PROBE_MAX_CAMPAIGN_DRAWDOWN, 1),
    maxReconciliationDiscrepancy: number(env.VENUE_PROBE_MAX_RECONCILIATION_DISCREPANCY, 0.01),
    campaignCashFlows: parseJson(env.VENUE_PROBE_CAMPAIGN_CASH_FLOWS || "[]")
  };
  validateCanaryConfig(config);
  return config;
}

export function validateCanaryConfig(config) {
  const errors = [];
  if (config.executionMode !== "strategy_canary") errors.push("EXECUTION_MODE must equal strategy_canary");
  if (!config.dryRun && config.trustBoundaryReady !== true) errors.push("FUNDED_EVIDENCE_TRUST_BOUNDARY_READY must be true only after signer/control isolation");
  if (config.allowLive) errors.push("ALLOW_LIVE must remain false");
  if (!config.allowCanary) errors.push("ALLOW_STRATEGY_CANARY must be true");
  if (config.enableTakerOrders) errors.push("ENABLE_TAKER_ORDERS must remain false");
  if (!(config.maxOrderNotional > 0 && config.maxOrderNotional <= 1)) errors.push("STRATEGY_CANARY_MAX_ORDER_NOTIONAL must be in (0, 1]");
  if (!config.candidateConfigHash) errors.push("STRATEGY_CANARY_CANDIDATE_CONFIG_HASH is required");
  if (!config.requiredFillModelVersion) errors.push("STRATEGY_CANARY_REQUIRED_FILL_MODEL_VERSION is required");
  if (!config.executionModelBlobUri) errors.push("STRATEGY_CANARY_EXECUTION_MODEL_BLOB_URI is required");
  if (!config.executionModelHash) errors.push("STRATEGY_CANARY_EXECUTION_MODEL_SHA256 is required");
  for (const [name, value] of [
    ["STRATEGY_CANARY_INTENT_BLOB_NAME", config.intentBlobName],
    ["STRATEGY_CANARY_INTENT_SHA256", config.intentBlobHash],
    ["STRATEGY_CANARY_PROMOTION_MANIFEST_BLOB_NAME", config.manifestBlobName],
    ["STRATEGY_CANARY_PROMOTION_MANIFEST_SHA256", config.manifestBlobHash],
    ["STRATEGY_CANARY_AUTHORIZATION_BLOB_NAME", config.authorizationBlobName],
    ["STRATEGY_CANARY_AUTHORIZATION_SHA256", config.authorizationBlobHash],
    ["STRATEGY_CANARY_HUMAN_GRANT_ID", config.humanGrantId],
    ["STRATEGY_CANARY_HUMAN_GRANT_SHA256", config.humanGrantHash],
    ["STRATEGY_CANARY_HUMAN_GRANT_CONSUMPTION_BLOB_NAME", config.humanGrantConsumptionBlobName],
    ["STRATEGY_CANARY_HUMAN_GRANT_CONSUMPTION_SHA256", config.humanGrantConsumptionHash]
  ]) if (!value) errors.push(`${name} is required`);
  if (!config.expectedCountry) errors.push("VENUE_PROBE_EXPECTED_COUNTRY is required");
  if (!config.expectedEgressIp) errors.push("VENUE_PROBE_EXPECTED_EGRESS_IP is required");
  if (!config.storageAccount) errors.push("AZURE_STORAGE_ACCOUNT_NAME is required");
  if (!config.intentContainerName) errors.push("STRATEGY_CANARY_INTENT_CONTAINER_NAME is required");
  if (!config.manifestContainerName) errors.push("STRATEGY_CANARY_MANIFEST_CONTAINER_NAME is required");
  for (const [name, value] of [
    ["POLYMARKET_PRIVATE_KEY", config.privateKey],
    ["POLYMARKET_API_KEY", config.apiKey],
    ["POLYMARKET_API_SECRET", config.apiSecret],
    ["POLYMARKET_API_PASSPHRASE", config.apiPassphrase],
    ["POLYMARKET_FUNDER_ADDRESS", config.funderAddress]
  ]) if (!value) errors.push(`${name} is required`);
  if (errors.length) throw new Error(`strategy_canary blocked: ${errors.join("; ")}`);
}

export async function loadHashedJson(container, blobName, expectedHash) {
  const response = await container.getBlobClient(blobName).download();
  const bytes = await streamToBuffer(response.readableStreamBody);
  const actualHash = sha256(bytes);
  if (actualHash !== normalizeHash(expectedHash)) {
    throw new Error(`fail closed: blob SHA-256 mismatch for ${blobName}`);
  }
  let value;
  try { value = JSON.parse(bytes.toString("utf8")); }
  catch { throw new Error(`fail closed: blob is not valid JSON (${blobName})`); }
  return { value, hash: actualHash, blobName };
}

export function canonicalBookHash(book, tokenId) {
  const levels = (values) => (values || [])
    .map((row) => ({ price: decimal(row.price), size: decimal(row.size) }))
    .filter((row) => row.price && row.size)
    .sort((left, right) => Number(left.price) - Number(right.price) || Number(left.size) - Number(right.size));
  return sha256(Buffer.from(stableJson({
    token_id: String(tokenId),
    tick_size: decimal(book?.tick_size ?? book?.tickSize),
    min_order_size: decimal(book?.min_order_size ?? book?.minOrderSize),
    bids: levels(book?.bids),
    asks: levels(book?.asks)
  })));
}

/**
 * Immutable, lossless-enough order-book evidence used by protocol-v3 markouts.
 * Numeric strings are normalized before hashing so the independent admission
 * validator can recompute prices and the exact content digest without trusting
 * producer-derived best-price fields.
 */
export function canonicalMarkoutBookSnapshot(book, tokenId) {
  const levels = (values) => (values || [])
    .map((row) => ({ price: decimal(row?.price), size: decimal(row?.size) }))
    .filter((row) => row.price !== null && row.size !== null)
    .sort((left, right) => Number(left.price) - Number(right.price) || Number(left.size) - Number(right.size));
  return {
    token_id: String(tokenId),
    tick_size: decimal(book?.tick_size ?? book?.tickSize),
    min_order_size: decimal(book?.min_order_size ?? book?.minOrderSize),
    bids: levels(book?.bids),
    asks: levels(book?.asks),
    venue_hash: clean(book?.venue_hash ?? book?.hash) || null
  };
}

export function canonicalMarkoutBookHash(rawBook) {
  return sha256(Buffer.from(stableJson(rawBook)));
}

export function polymarketV2FeePerShare(price, feeRate, feeExponent) {
  const p = Number(price);
  const rate = Number(feeRate);
  const exponent = Number(feeExponent);
  if (!Number.isFinite(p) || p < 0 || p > 1 ||
      !Number.isFinite(rate) || rate < 0 || rate > 1 ||
      !Number.isFinite(exponent) || exponent < 0 || exponent > 10) {
    throw new Error("fail closed: Polymarket V2 fee parameters are invalid");
  }
  if (rate === 0) return 0;
  return rate * (p * (1 - p)) ** exponent;
}

export function validateCanaryPreflight({ config, intent, manifest, authorization, executionModel, executionModelHash, runtime, now = new Date() }) {
  const fail = (message) => { throw new Error(`fail closed: ${message}`); };
  const nowMs = now.getTime();
  if (intent?.schema !== EXECUTION_INTENT_SCHEMA) fail("unsupported execution intent schema");
  for (const field of ["decision_id", "candidate_name", "candidate_version", "candidate_config_hash", "market_id", "condition_id", "token_id", "book_hash", "regime", "features_digest", "required_fill_model_version", "execution_model_blob_uri", "execution_model_sha256", "resolution_source"]) {
    if (!clean(intent?.[field])) fail(`execution intent ${field} is required`);
  }
  if (intent.candidate_name !== config.candidateName || intent.candidate_version !== config.candidateVersion || intent.candidate_config_hash !== config.candidateConfigHash) fail("execution intent candidate identity mismatch");
  if (String(intent.side).toUpperCase() !== "BUY") fail("execution intent must be BUY");
  if (intent.post_only !== true || intent.order_kind !== "post_only_gtd") fail("execution intent must be post-only GTD");
  const price = finite(intent.price, "price");
  const shares = finite(intent.shares, "shares");
  const notional = finite(intent.notional, "notional");
  const intentMinimumOrderSize = finite(intent.minimum_order_size, "minimum_order_size");
  if (!(price > 0 && price < 1 && shares > 0 && notional > 0)) fail("execution intent price, shares, and notional must be positive");
  if (Math.abs(price * shares - notional) > 1e-9) fail("execution intent notional does not reconcile");
  if (notional > config.maxOrderNotional + 1e-9 || notional > 1 + 1e-9) fail("execution intent exceeds the one-dollar notional cap");
  if (!(intentMinimumOrderSize > 0) || shares + 1e-9 < intentMinimumOrderSize) fail("execution intent is below its bound venue minimum_order_size");
  const decisionMs = Date.parse(intent.decision_ts);
  const validUntilMs = Date.parse(intent.valid_until);
  const expiryMs = Date.parse(intent.gtd_expiry_ts);
  if (![decisionMs, validUntilMs, expiryMs].every(Number.isFinite)) fail("execution intent timestamps are invalid");
  if (nowMs < decisionMs || nowMs >= validUntilMs) fail("execution intent is stale or not yet valid");
  if (Number(intent.ttl_ms) !== validUntilMs - decisionMs || Number(intent.ttl_ms) > MAX_ACTIVE_INTENT_TTL_MS) fail("active intent TTL does not reconcile or exceeds the short-lifecycle limit");
  if (expiryMs !== validUntilMs + VENUE_GTD_SECURITY_BUFFER_MS) fail("venue GTD expiry must include the exact 60-second security buffer");
  const referenceAgeMs = Number(intent.reference_age_ms);
  const bookAgeMs = Number(intent.book_age_ms);
  if (!Number.isFinite(referenceAgeMs) || referenceAgeMs < 0 || referenceAgeMs > config.maxReferenceAgeMs) fail("reference source is stale");
  if (!Number.isFinite(bookAgeMs) || bookAgeMs < 0 || bookAgeMs > config.maxBookAgeMs) fail("intent book is stale");
  const gross = finite(intent.gross_edge, "gross_edge");
  const q = finite(intent.q, "q");
  const fees = finite(intent.fee_allowance, "fee_allowance");
  const slippage = finite(intent.slippage_allowance, "slippage_allowance");
  const toxicity = finite(intent.toxicity_allowance, "toxicity_allowance");
  const net = finite(intent.net_edge_lower_bound, "net_edge_lower_bound");
  if (q < 0 || q > 1) fail("q must be between zero and one");
  if ([fees, slippage, toxicity].some((value) => value < 0) || Math.abs(gross - fees - slippage - toxicity - net) > 1e-9 || net <= 0) fail("net edge lower bound is not positive and reconciled");
  if (intent.required_fill_model_version !== config.requiredFillModelVersion || runtime.fillModelVersion !== config.requiredFillModelVersion) fail("required fill-model version mismatch");
  if (intent.execution_model_blob_uri !== config.executionModelBlobUri || normalizeHash(intent.execution_model_sha256) !== config.executionModelHash) fail("execution intent exact model artifact binding mismatch");
  if (normalizeHash(executionModelHash) !== config.executionModelHash || executionModel?.model_version !== config.requiredFillModelVersion) fail("downloaded execution model hash or version mismatch");
  const modelGeneratedMs = Date.parse(executionModel?.generated_at);
  if (!Number.isFinite(modelGeneratedMs) || modelGeneratedMs >= decisionMs) fail("execution model must be an immutable temporal prior to the order decision");
  if (executionModel.model_version === "conservative-execution-prior-v1") {
    if (executionModel.status !== "frozen_conservative_prior" || executionModel.generated_at !== "2026-07-12T00:00:00Z" || executionModel.evidence_protocol_version !== 3 || executionModel.prediction_policy !== "zero_fill_probability_until_authenticated_calibration" || executionModel.sample_size !== 0 || executionModel.promotion_allowed !== false || executionModel.funded_execution_allowed !== false) fail("invalid frozen conservative execution prior artifact");
  } else if (executionModel.model_version === "queue-calibration-v1") {
    const trainingEndMs = Date.parse(executionModel.training_data_end_ts);
    if (executionModel.schema !== "polyedge.execution_queue_model.v1" || !Number.isFinite(trainingEndMs) || trainingEndMs >= decisionMs) fail("trained queue model has invalid temporal training lineage");
  } else {
    fail("unsupported execution model artifact schema");
  }
  if (intent.exact_resolution_source !== true || intent.resolution_source !== config.requiredResolutionSource || runtime.exactResolutionSource !== true || runtime.resolutionSource !== config.requiredResolutionSource) fail("exact resolution source is not confirmed");

  const fundedStage = authorization?.schema === FUNDED_AUTHORIZATION_SCHEMA;
  const manifestPhaseValid = fundedStage
    ? manifest?.phase === "limited_live" && manifest?.gate_metrics?.phase === "shadow_passed" && manifest?.funded_ladder?.phase === "limited_live" && manifest?.funded_ladder?.stage_authorized === true && manifest?.funded_ladder?.human_grant_required === false
    : manifest?.phase === "canary_ready" && manifest?.gate_metrics?.phase === "canary_ready";
  if (manifest?.schema_version !== PROMOTION_MANIFEST_SCHEMA || !manifestPhaseValid) fail("promotion manifest phase or funded-stage authorization state is invalid");
  if (manifest.promotion_allowed !== false || manifest.gate_metrics?.promotion_allowed !== true || manifest.human_authorization_required !== true) fail("promotion manifest gates are not passing or the research manifest is directly executable");
  const manifestCreatedMs = Date.parse(manifest.created_at);
  const manifestExpiresMs = Date.parse(manifest.expires_at);
  if (!Number.isFinite(manifestCreatedMs) || !Number.isFinite(manifestExpiresMs) || manifestCreatedMs > nowMs || manifestExpiresMs <= nowMs || manifestExpiresMs <= manifestCreatedMs) fail("promotion manifest is expired or has an invalid validity window");
  if (manifest.candidate?.name !== intent.candidate_name || manifest.candidate?.candidate_version !== intent.candidate_version || manifest.candidate?.config_hash !== intent.candidate_config_hash) fail("promotion manifest candidate mismatch");
  if (manifest.execution_model?.blob_uri !== config.executionModelBlobUri || normalizeHash(manifest.execution_model?.sha256) !== config.executionModelHash || manifest.execution_model?.model_version !== config.requiredFillModelVersion) fail("promotion manifest exact model artifact binding mismatch");

  if (![AUTHORIZATION_SCHEMA, FUNDED_AUTHORIZATION_SCHEMA].includes(authorization?.schema) || authorization.single_use !== true) fail("invalid one-shot authorization");
  for (const field of ["authorization_id", "human_authorization_reference", "authorized_at", "expires_at"]) if (!clean(authorization?.[field])) fail(`authorization ${field} is required`);
  if (authorization.decision_id !== intent.decision_id) fail("authorization decision mismatch");
  const authorizationStartedMs = Date.parse(authorization.authorized_at);
  const authorizationExpiresMs = Date.parse(authorization.expires_at);
  if (!Number.isFinite(authorizationStartedMs) || !Number.isFinite(authorizationExpiresMs) || authorizationStartedMs > nowMs || authorizationExpiresMs <= nowMs || authorizationExpiresMs <= authorizationStartedMs) fail("authorization is stale, not yet valid, or has an invalid validity window");
  if (authorization.intent_blob_name !== config.intentBlobName || normalizeHash(authorization.intent_sha256) !== config.intentBlobHash || authorization.promotion_manifest_blob_name !== config.manifestBlobName || normalizeHash(authorization.promotion_manifest_sha256) !== config.manifestBlobHash) fail("authorization artifact binding mismatch");
  if (fundedStage) {
    if (authorization.funded_stage_consumption_blob_name !== config.humanGrantConsumptionBlobName || normalizeHash(authorization.funded_stage_consumption_sha256) !== config.humanGrantConsumptionHash || !normalizeHash(authorization.funded_stage_source_state_sha256) || Number(authorization.funded_stage_target_orders) !== Number(manifest.funded_ladder?.active_target_orders)) fail("funded-stage consumption, state, or target binding mismatch");
  } else if (authorization.human_grant_id !== config.humanGrantId || normalizeHash(authorization.human_grant_sha256) !== config.humanGrantHash || authorization.human_grant_consumption_blob_name !== config.humanGrantConsumptionBlobName || normalizeHash(authorization.human_grant_consumption_sha256) !== config.humanGrantConsumptionHash) fail("authorization human-grant consumption binding mismatch");
  if (authorization.candidate_name !== intent.candidate_name || authorization.candidate_version !== intent.candidate_version || authorization.candidate_config_hash !== intent.candidate_config_hash || authorization.required_fill_model_version !== intent.required_fill_model_version) fail("authorization candidate or fill-model binding mismatch");
  if (authorization.execution_model_blob_uri !== config.executionModelBlobUri || normalizeHash(authorization.execution_model_sha256) !== config.executionModelHash) fail("authorization exact model artifact binding mismatch");
  const modelArtifact = artifactLocationFromUri(config.executionModelBlobUri, config.storageAccount);
  if (authorization.execution_model_container_name !== modelArtifact.container || authorization.execution_model_blob_name !== modelArtifact.blobName) fail("authorization execution model container/blob provenance mismatch");
  if (!fundedStage && (manifest.controller_transition?.human_grant_id !== authorization.human_grant_id || normalizeHash(manifest.controller_transition?.human_grant_sha256) !== normalizeHash(authorization.human_grant_sha256) || manifest.controller_transition?.human_grant_consumption_blob_name !== authorization.human_grant_consumption_blob_name || normalizeHash(manifest.controller_transition?.human_grant_consumption_sha256) !== normalizeHash(authorization.human_grant_consumption_sha256))) fail("canary-ready manifest is not bound to the consumed human grant");

  if (runtime.geoblock?.blocked !== false || String(runtime.geoblock?.country || "").toUpperCase() !== config.expectedCountry || runtime.geoblock?.ip !== config.expectedEgressIp) fail("geoblock country or static egress validation failed");
  if (!Number.isFinite(runtime.clockDriftMs) || runtime.clockDriftMs > config.maxClockDriftMs) fail("clock drift exceeds limit");
  if (!Number.isFinite(runtime.clockServerMinusLocalMs) || !Number.isFinite(runtime.clockRoundTripMs)
      || !Number.isFinite(runtime.clockUncertaintyMs) || runtime.clockRoundTripMs < 0
      || runtime.clockUncertaintyMs < 0 || runtime.clockUncertaintyMs > config.maxClockUncertaintyMs) {
    fail("clock uncertainty exceeds limit");
  }
  if (runtime.risk?.passed !== true) fail(`campaign equity/risk gate failed (${(runtime.risk?.blockers || ["unknown"]).join(", ")})`);
  if (Number(runtime.openOrderCount) !== 0) fail("account has open orders");
  if (String(runtime.market?.marketId) !== String(intent.market_id) || String(runtime.market?.conditionId) !== String(intent.condition_id) || String(runtime.market?.tokenId) !== String(intent.token_id)) fail("market, condition, or token identity mismatch");
  if (runtime.market?.closed === true || runtime.market?.acceptingOrders !== true) fail("market is not accepting orders");
  const actualBookHash = canonicalBookHash(runtime.book, intent.token_id);
  if (actualBookHash !== normalizeHash(intent.book_hash)) fail("current order book hash disagrees with the intent");
  const bestAsk = Math.min(...(runtime.book?.asks || []).map((row) => Number(row.price)).filter(Number.isFinite));
  if (!Number.isFinite(bestAsk) || price >= bestAsk) fail("post-only BUY would cross the current ask");
  const tick = Number(runtime.book?.tick_size ?? runtime.book?.tickSize);
  if (!(tick > 0) || Math.abs(price / tick - Math.round(price / tick)) > 1e-7) fail("intent price does not agree with the current tick size");
  const minimumOrderSize = Number(runtime.book?.min_order_size ?? runtime.book?.minOrderSize);
  if (!(minimumOrderSize > 0) || Math.abs(minimumOrderSize - intentMinimumOrderSize) > 1e-9 || shares + 1e-9 < minimumOrderSize) fail("intent shares or bound minimum_order_size disagree with the venue");
  const feeRate = Number(runtime.feeRate);
  const feeRateBps = Number(runtime.feeRateBps);
  const feeExponent = Number(runtime.feeExponent);
  if (runtime.feeModel !== "polymarket_clob_v2_curve" ||
      !Number.isFinite(feeRate) || feeRate < 0 || feeRate > 1 ||
      !Number.isFinite(feeRateBps) || Math.abs(feeRate * 10_000 - feeRateBps) > 1e-6 ||
      !Number.isFinite(feeExponent) || feeExponent < 0 || feeExponent > 10 ||
      (feeRate > 0 && runtime.feeTakerOnly !== true)) {
    fail("exact Polymarket V2 fee rate/exponent/taker-only parameters are required");
  }
  return { price, shares, notional, validUntilMs, venueExpiryMs: expiryMs, actualBookHash };
}

export async function consumeOneShotAuthorization(container, { authorization, authorizationHash, decisionId, runId, now = new Date() }) {
  const id = clean(authorization?.authorization_id);
  if (!/^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$/.test(id)) throw new Error("fail closed: unsafe authorization id");
  const name = `reports/research/venue-probe/control/strategy-canary/consumed/${id}.json`;
  const payload = {
    schema: "polyedge.strategy_canary_authorization_consumption.v1",
    authorization_id: id,
    authorization_sha256: normalizeHash(authorizationHash),
    decision_id: decisionId,
    run_id: runId,
    consumed_at: now.toISOString()
  };
  try {
    await container.getBlockBlobClient(name).uploadData(Buffer.from(JSON.stringify(payload, null, 2)), {
      conditions: { ifNoneMatch: "*" },
      blobHTTPHeaders: { blobContentType: "application/json" }
    });
  } catch (error) {
    if ([409, 412].includes(Number(error.statusCode))) throw new Error("fail closed: one-shot authorization was already consumed");
    throw error;
  }
  return { ...payload, blob_name: name };
}

export async function executeStrategyCanary({ config, documents, runtime, runId, now = new Date(), reserveRisk, finalizeNoOrder, consumeAuthorization, executeLifecycle }) {
  const validated = validateCanaryPreflight({ config, ...documents, runtime, now });
  if (config.dryRun) {
    return { status: "strategy_intent_validated_no_order", order_submission_attempted: false, authorization_consumed: false, decision_id: documents.intent.decision_id };
  }
  const feeRiskUpperBound = validated.shares * polymarketV2FeePerShare(
    validated.price,
    runtime.feeRate,
    runtime.feeExponent
  );
  const reservation = await reserveRisk({
    run_id: runId,
    probe_id: `strategy-canary-${documents.intent.decision_id}`,
    reserved_notional: validated.notional + feeRiskUpperBound,
    principal_notional: validated.notional,
    fee_model: "polymarket_clob_v2_curve",
    fee_rate: runtime.feeRate,
    fee_rate_bps: runtime.feeRateBps,
    fee_exponent: runtime.feeExponent,
    fee_taker_only: runtime.feeTakerOnly,
    fee_risk_upper_bound: feeRiskUpperBound,
    market_id: documents.intent.market_id,
    condition_id: documents.intent.condition_id,
    token_id: documents.intent.token_id
  });
  let consumption;
  try {
    consumption = await consumeAuthorization({
      authorization: documents.authorization,
      authorizationHash: documents.authorizationHash,
      decisionId: documents.intent.decision_id,
      runId,
      now
    });
  } catch (error) {
    if (finalizeNoOrder) await finalizeNoOrder(reservation);
    throw error;
  }
  const lifecycle = await executeLifecycle({ intent: documents.intent, documents, runtime, reservation, consumption, validated });
  return { status: "strategy_canary_executed", order_submission_attempted: true, authorization_consumed: true, decision_id: documents.intent.decision_id, lifecycle };
}

export function beginFillMarkoutCapture(client, tokenId, currentFills, options = {}) {
  const horizons = options.horizons || [1, 5, 30];
  const horizonScaleMs = options.horizonScaleMs ?? 1000;
  const pollMs = options.pollMs ?? 50;
  const nowMs = options.nowMs || Date.now;
  const feeParameters = normalizeV2FeeParameters(options.feeParameters);
  const scheduled = new Map();
  const abortController = new AbortController();
  let stopping = false;
  const schedule = (fill) => {
    if (!validMarkoutFill(fill) || scheduled.has(fill.id)) return;
    const capture = Promise.all(horizons.map(async (horizon) => {
      const deadlineMs = fill.timestampMs + horizon * horizonScaleMs;
      await sleepAbortable(Math.max(0, deadlineMs - nowMs()), abortController.signal);
      const requestStartedAt = nowMs();
      const book = await client.getOrderBook(tokenId);
      const responseCompletedAt = nowMs();
      const bids = (book.bids || []).map((row) => Number(row.price)).filter(Number.isFinite);
      const asks = (book.asks || []).map((row) => Number(row.price)).filter(Number.isFinite);
      const bestBid = bids.length ? Math.max(...bids) : null;
      const bestAsk = asks.length ? Math.min(...asks) : null;
      const midpoint = bestBid === null || bestAsk === null ? null : (bestBid + bestAsk) / 2;
      const venueBookTimestampMs = finiteTimestampMs(book?.timestamp ?? book?.ts);
      const rawOrderbook = canonicalMarkoutBookSnapshot(book, tokenId);
      if (!/^[0-9a-f]{40}$/i.test(rawOrderbook.venue_hash || "")) {
        throw new Error("fail closed: exact venue SHA-1 order-book hash is required for markout evidence");
      }
      const fees = fillFeeEvidence(fill, bestBid, feeParameters);
      return {
        fill_id: fill.id,
        horizon_seconds: horizon,
        fill_timestamp: new Date(fill.timestampMs).toISOString(),
        venue_fill_timestamp: Number.isFinite(Number(fill.venueTimestampMs))
          ? new Date(Number(fill.venueTimestampMs)).toISOString()
          : null,
        target_observation_ts: new Date(deadlineMs).toISOString(),
        request_started_at: new Date(requestStartedAt).toISOString(),
        response_completed_at: new Date(responseCompletedAt).toISOString(),
        observed_at: new Date(responseCompletedAt).toISOString(),
        response_duration_ms: responseCompletedAt - requestStartedAt,
        observation_delay_ms: responseCompletedAt - deadlineMs,
        venue_book_timestamp: venueBookTimestampMs === null ? null : new Date(venueBookTimestampMs).toISOString(),
        venue_book_hash: book?.hash ? String(book.hash) : null,
        raw_orderbook: rawOrderbook,
        book_hash: canonicalMarkoutBookHash(rawOrderbook),
        fill_size: fill.size,
        fill_price: fill.price,
        trader_side: fees.traderSide,
        authenticated_order_role: fees.orderRole,
        authenticated_fee_rate_bps: fees.authenticatedFeeRateBps,
        authenticated_fee_amount: fees.authenticatedFeeAmount,
        authenticated_fee_raw: fees.authenticatedFeeRaw,
        entry_fee_per_share: fees.entryFeePerShare,
        hypothetical_exit_fee_per_share: fees.hypotheticalExitFeePerShare,
        round_trip_fee_per_share: fees.roundTripFeePerShare,
        midpoint,
        executable_price: bestBid,
        midpoint_markout_per_share: midpoint === null ? null : midpoint - fill.price,
        executable_markout_per_share: bestBid === null ? null : bestBid - fill.price
      };
    }));
    // Attach a rejection observer immediately; finish() still propagates it.
    capture.catch(() => {});
    scheduled.set(fill.id, capture);
  };
  const monitor = (async () => {
    while (!stopping) {
      for (const fill of currentFills()) schedule(fill);
      await sleep(pollMs);
    }
  })();
  return {
    async finish(finalFills) {
      for (const fill of finalFills) schedule(fill);
      stopping = true;
      await monitor;
      return (await Promise.all([...scheduled.values()])).flat()
        .sort((left, right) => left.fill_id.localeCompare(right.fill_id) || left.horizon_seconds - right.horizon_seconds);
    },
    async abort() {
      stopping = true;
      abortController.abort();
      await monitor;
      await Promise.allSettled([...scheduled.values()]);
    }
  };
}

function normalizeV2FeeParameters(value) {
  const rate = Number(value?.rate);
  const exponent = Number(value?.exponent);
  const rateBps = Number(value?.rateBps);
  const takerOnly = value?.takerOnly;
  if (!Number.isFinite(rate) || rate < 0 || rate > 1 ||
      !Number.isFinite(exponent) || exponent < 0 || exponent > 10 ||
      !Number.isFinite(rateBps) || rateBps < 0 || rateBps > 10_000 ||
      Math.abs(rate * 10_000 - rateBps) > 1e-6 ||
      (rate > 0 && takerOnly !== true)) {
    throw new Error("fail closed: exact Polymarket V2 market fee parameters are required for markouts");
  }
  return { rate, exponent, rateBps, takerOnly: takerOnly === true };
}

function fillFeeEvidence(fill, executablePrice, feeParameters) {
  const traderSide = clean(fill?.traderSide).toUpperCase() || null;
  const orderRole = clean(fill?.orderRole).toUpperCase() || null;
  const authenticatedFeeRateBps = optionalFiniteNumber(fill?.authenticatedFeeRateBps);
  const authenticatedFeeAmount = optionalFiniteNumber(fill?.authenticatedFeeAmount);
  if (traderSide !== "MAKER" && traderSide !== "TAKER" && feeParameters.rate > 0) {
    throw new Error("fail closed: fee-bearing authenticated fill is missing trader_side");
  }
  if (feeParameters.rate > 0 &&
      (authenticatedFeeRateBps === null || (traderSide === "TAKER"
        ? Math.abs(authenticatedFeeRateBps - feeParameters.rateBps) > 1e-6
        : authenticatedFeeRateBps > 1e-6 && Math.abs(authenticatedFeeRateBps - feeParameters.rateBps) > 1e-6))) {
    throw new Error("fail closed: fee-bearing authenticated fill fee_rate_bps disagrees with market fee parameters");
  }
  if ((orderRole === "MAKER" || orderRole === "TAKER") && traderSide !== orderRole) {
    throw new Error("fail closed: authenticated trader_side contradicts the order's matched role");
  }
  if (authenticatedFeeAmount !== null && authenticatedFeeAmount < 0) {
    throw new Error("fail closed: authenticated fill fee amount is negative");
  }
  if (traderSide === "MAKER" && authenticatedFeeAmount !== null && authenticatedFeeAmount > 1e-12) {
    throw new Error("fail closed: post-only maker fill reports a nonzero authenticated fee amount");
  }
  const curveEntry = traderSide === "TAKER"
    ? polymarketV2FeePerShare(fill.price, feeParameters.rate, feeParameters.exponent)
    : 0;
  const reportedEntry = traderSide !== "TAKER" || authenticatedFeeAmount === null
    ? 0
    : authenticatedFeeAmount / Number(fill.size);
  const entryFeePerShare = Math.max(curveEntry, reportedEntry);
  const hypotheticalExitFeePerShare = executablePrice === null
    ? null
    : polymarketV2FeePerShare(executablePrice, feeParameters.rate, feeParameters.exponent);
  return {
    traderSide,
    orderRole,
    authenticatedFeeRateBps,
    authenticatedFeeAmount,
    authenticatedFeeRaw: fill?.authenticatedFeeRaw || null,
    entryFeePerShare,
    hypotheticalExitFeePerShare,
    roundTripFeePerShare: hypotheticalExitFeePerShare === null
      ? null
      : entryFeePerShare + hypotheticalExitFeePerShare
  };
}

function optionalFiniteNumber(value) {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) throw new Error("fail closed: authenticated fill fee evidence is invalid");
  return parsed;
}

export function sha256(value) {
  return `sha256:${createHash("sha256").update(value).digest("hex")}`;
}

export function artifactLocationFromUri(uri, storageAccount) {
  const match = /^azure:\/\/([^/]+)\/([^/]+)\/(.+)$/.exec(String(uri || ""));
  if (!match || match[1] !== storageAccount) throw new Error("fail closed: execution model URI is outside configured Azure storage account");
  const [, account, container, blobName] = match;
  if (!container || !blobName || container.includes("..") || blobName.includes("..") || blobName.startsWith("/")) throw new Error("fail closed: unsafe execution model blob URI");
  return { account, container, blobName };
}

export function blobNameFromArtifactUri(uri, storageAccount, storageContainer) {
  const location = artifactLocationFromUri(uri, storageAccount);
  if (location.container !== storageContainer) throw new Error("fail closed: execution model URI container mismatch");
  return location.blobName;
}

function normalizeHash(value) {
  const cleanValue = clean(value).toLowerCase();
  if (!cleanValue) return "";
  const prefixed = cleanValue.startsWith("sha256:") ? cleanValue : `sha256:${cleanValue}`;
  return /^sha256:[0-9a-f]{64}$/.test(prefixed) ? prefixed : "";
}

function stableJson(value) {
  if (Array.isArray(value)) return `[${value.map(stableJson).join(",")}]`;
  if (value && typeof value === "object") return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${stableJson(value[key])}`).join(",")}}`;
  return JSON.stringify(value);
}

function validMarkoutFill(fill) {
  return Boolean(fill?.id) && Number(fill?.size) > 0 && Number(fill?.price) > 0 && Number.isFinite(Number(fill?.timestampMs));
}

function finiteTimestampMs(value) {
  if (value === null || value === undefined || value === "") return null;
  const numeric = Number(value);
  if (Number.isFinite(numeric)) return numeric < 10_000_000_000 ? numeric * 1000 : numeric;
  const parsed = Date.parse(String(value));
  return Number.isFinite(parsed) ? parsed : null;
}

function sleep(ms) { return new Promise((resolve) => setTimeout(resolve, ms)); }

function sleepAbortable(ms, signal) {
  if (signal.aborted) return Promise.reject(new Error("markout capture aborted"));
  return new Promise((resolve, reject) => {
    const timer = setTimeout(resolve, ms);
    signal.addEventListener("abort", () => {
      clearTimeout(timer);
      reject(new Error("markout capture aborted"));
    }, { once: true });
  });
}

function decimal(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return null;
  return parsed.toFixed(12).replace(/0+$/, "").replace(/\.$/, "");
}

function finite(value, field) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) throw new Error(`fail closed: execution intent ${field} is invalid`);
  return parsed;
}

async function streamToBuffer(stream) {
  const chunks = [];
  for await (const chunk of stream) chunks.push(Buffer.from(chunk));
  return Buffer.concat(chunks);
}

function clean(value) { return String(value || "").trim(); }
function boolean(value) { return String(value || "").toLowerCase() === "true"; }
function integer(value, fallback) { const parsed = Number.parseInt(value, 10); return Number.isFinite(parsed) ? parsed : fallback; }
function number(value, fallback) { const parsed = Number(value); return Number.isFinite(parsed) ? parsed : fallback; }
function parseJson(value) { try { return JSON.parse(value); } catch { throw new Error("strategy_canary blocked: campaign cash flows must be valid JSON"); } }
