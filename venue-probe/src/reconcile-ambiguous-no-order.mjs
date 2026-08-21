import { AssetType } from "@polymarket/clob-client-v2";
import { createHash } from "node:crypto";
import { pathToFileURL } from "node:url";
import {
  EVIDENCE_PROTOCOL_VERSION,
  finalizeProbeRisk,
  loadCampaignUnresolvedRiskReservationRecords,
  sanitize,
  storageContainer
} from "./lib.mjs";
import {
  validateRejectedReservationRecovery,
  venueClient
} from "./reconcile-rejected-no-order.mjs";
import { orderIds } from "./canary-lifecycle-lib.mjs";
import { loadAccountPositions } from "./canary.mjs";
import { validateTerminalSettlementFederatedTokenFile } from "./funded-terminal-settlement.mjs";

const TENANT_ID = "9767f0dc-e83f-4cc1-94e1-0d5f9d287d32";
const STORAGE_ACCOUNT = "stpolyedge6urdjr5nmwx7w";
const STORAGE_CONTAINER = "polyedge-funded-evidence";
const FUNDED_UAMI_CLIENT_ID = "d9ce9154-66a6-4bdb-839f-0da7b02b38da";
const CAMPAIGN_ID = "dynamic-quote-funded-2026-08-13-v10";
const FUNDER_ADDRESS = "0x3d701b05d7c36afab01a06fd26ebe789c0b7bad8";
const AMBIGUOUS_REASON = "ambiguous_submission_no_fill";
const ACKNOWLEDGED_REASON = "acknowledged_terminal_no_fill";
const MINIMUM_COMPLETION_AGE_MS = 24 * 60 * 60 * 1_000;
const MINIMUM_SNAPSHOT_INTERVAL_MS = 10_000;
const SHA256 = /^sha256:[0-9a-f]{64}$/;
const ORDER_ID = /^0x[0-9a-f]{64}$/i;
const TERMINAL_NO_FILL_STATES = new Set(["CANCELED", "CANCELLED", "EXPIRED"]);

function fail(message) {
  throw new Error("fail closed: ambiguous no-order reconciliation " + message);
}

export function ambiguousNoOrderConfig(env = process.env) {
  return recoveryConfig(env, AMBIGUOUS_REASON, fail);
}

export function acknowledgedNoFillConfig(
  env = process.env,
  { requireFederatedToken = false } = {}
) {
  const config = recoveryConfig(env, ACKNOWLEDGED_REASON, failAcknowledged);
  const orderId = String(env.FUNDED_DIRECT_RECONCILE_ORDER_ID || "").trim();
  if (!ORDER_ID.test(orderId)) failAcknowledged("order ID is not exact");
  if (requireFederatedToken) {
    validateTerminalSettlementFederatedTokenFile(env.AZURE_FEDERATED_TOKEN_FILE);
  }
  return { ...config, orderId };
}

function recoveryConfig(env, reason, failClosed) {
  const decisionId = String(env.FUNDED_DIRECT_RECONCILE_DECISION_ID || "").trim();
  const runId = String(env.FUNDED_DIRECT_RECONCILE_RUN_ID || "").trim();
  const reservationBlobName =
    String(env.FUNDED_DIRECT_RECONCILE_RESERVATION_BLOB_NAME || "").trim();
  const reservationSha256 =
    String(env.FUNDED_DIRECT_RECONCILE_RESERVATION_SHA256 || "").trim();
  const completionBlobName =
    String(env.FUNDED_DIRECT_RECONCILE_COMPLETION_BLOB_NAME || "").trim();
  const completionSha256 =
    String(env.FUNDED_DIRECT_RECONCILE_COMPLETION_SHA256 || "").trim();
  if (env.FUNDED_DIRECT_RECONCILIATION_ENABLED !== "true" ||
      env.FUNDED_DIRECT_RECONCILIATION_REASON !== reason ||
      env.VENUE_PROBE_FUNDED_CAMPAIGN_ID !== CAMPAIGN_ID ||
      String(env.POLYMARKET_FUNDER_ADDRESS || "").toLowerCase() !== FUNDER_ADDRESS ||
      env.AZURE_TENANT_ID !== TENANT_ID ||
      env.AZURE_STORAGE_ACCOUNT_NAME !== STORAGE_ACCOUNT ||
      env.AZURE_STORAGE_CONTAINER_NAME !== STORAGE_CONTAINER ||
      env.AZURE_CLIENT_ID !== FUNDED_UAMI_CLIENT_ID ||
      env.AZURE_TOKEN_CREDENTIALS !== "WorkloadIdentityCredential" ||
      (env.POLYMARKET_CLOB_URL !== undefined &&
        env.POLYMARKET_CLOB_URL !== "https://clob.polymarket.com") ||
      env.POLYMARKET_SIGNATURE_TYPE !== "3" ||
      Object.hasOwn(env, "AZURE_STORAGE_ACCOUNT_KEY") ||
      !/^[0-9a-f]{64}$/.test(decisionId) ||
      !/^funded-direct-[0-9]{17}-[0-9a-f]{8}$/.test(runId) ||
      !SHA256.test(reservationSha256) ||
      !SHA256.test(completionSha256)) {
    failClosed("environment is not exact");
  }
  const probeId = "funded-direct-" + decisionId;
  const completionPath =
    "reports/funded/dynamic-quote/sessions/" + CAMPAIGN_ID + "/completed/" +
    decisionId + ".json";
  if (!new RegExp(
    "^reports/research/venue-probe/risk-reservations/\\d{4}-\\d{2}-\\d{2}/" +
      probeId + "\\.json$"
  ).test(reservationBlobName) || completionBlobName !== completionPath) {
    failClosed("blob namespace is not exact");
  }
  return {
    campaignId: CAMPAIGN_ID,
    decisionId,
    runId,
    probeId,
    reservationBlobName,
    reservationSha256,
    completionBlobName,
    completionSha256,
    storageAccount: STORAGE_ACCOUNT,
    storageContainer: STORAGE_CONTAINER,
    azureClientId: FUNDED_UAMI_CLIENT_ID,
    operatorDirect: true,
    dryRun: false,
    funderAddress: FUNDER_ADDRESS
  };
}

function failAcknowledged(message) {
  throw new Error("fail closed: acknowledged no-fill reconciliation " + message);
}

function finiteNumeric(value) {
  return ["number", "string"].includes(typeof value) &&
    String(value).trim() !== "" && Number.isFinite(Number(value));
}

function exactZero(value) {
  return finiteNumeric(value) && Number(value) === 0;
}

export function validateAmbiguousNoOrderBinding({
  config,
  record,
  reservationDocument,
  completionDocument,
  nowMs = Date.now()
}) {
  const reservation = reservationDocument?.value;
  const completion = completionDocument?.value;
  const createdMs = Date.parse(reservation?.created_ts);
  const completedMs = Date.parse(completion?.completed_at);
  const createdDate = Number.isFinite(createdMs)
    ? new Date(createdMs).toISOString().slice(0, 10)
    : "";
  const expectedReservationPath =
    "reports/research/venue-probe/risk-reservations/" + createdDate + "/" +
    config.probeId + ".json";
  if (record?.blob_name !== config.reservationBlobName ||
      typeof record?.etag !== "string" || !record.etag ||
      reservationDocument?.blobName !== config.reservationBlobName ||
      reservationDocument?.sha256 !== config.reservationSha256 ||
      config.reservationBlobName !== expectedReservationPath ||
      reservation?.schema_version !== 1 ||
      reservation?.campaign_id !== config.campaignId ||
      reservation?.run_id !== config.runId ||
      reservation?.probe_id !== config.probeId ||
      reservation?.date !== createdDate ||
      reservation?.state !== "reserved" ||
      reservation?.order_submission_intended !== true ||
      reservation?.order_submitted !== null ||
      reservation?.order_id != null ||
      ![null, 0].includes(reservation?.matched_notional) ||
      !Number.isFinite(createdMs)) {
    fail("reservation binding is invalid");
  }
  if (completionDocument?.blobName !== config.completionBlobName ||
      completionDocument?.sha256 !== config.completionSha256 ||
      completion?.schema !== "polyedge.operator_funded_intent_completion.v1" ||
      completion?.session_id !== config.campaignId ||
      completion?.decision_id !== config.decisionId ||
      completion?.child_run_id !== config.runId ||
      completion?.status !== "child_failed_closed_post_submission_unresolved" ||
      completion?.order_submission_attempted !== true ||
      completion?.authorization_consumed !== true ||
      completion?.risk_reservation_created !== true ||
      completion?.order_id != null ||
      !exactZero(completion?.matched_notional) ||
      completion?.reconciliation_complete !== false ||
      completion?.zero_open_orders_confirmed !== false ||
      !String(completion?.post_submission_error || "").startsWith(
        "fail closed: ambiguous strategy-canary submission; authorization is consumed and risk remains reserved ("
      ) ||
      !Number.isFinite(completedMs) ||
      completedMs < createdMs ||
      !Number.isFinite(nowMs) ||
      nowMs - completedMs < MINIMUM_COMPLETION_AGE_MS) {
    fail("completion binding is invalid");
  }
  return { reservation, completion, etag: record.etag };
}

export function validateAmbiguousNoOrderVenueEvidence({
  reservation,
  runId,
  openOrders,
  authenticatedTrades,
  positions
}) {
  const evidence = validateRejectedReservationRecovery({
    reservation,
    expectedRunId: runId,
    openOrders,
    authenticatedTrades,
    positions
  });
  const exactPositions = positions.filter((position) =>
    Number(position?.size) > 1e-9 &&
    String(position?.conditionId || "").toLowerCase() ===
      String(reservation.condition_id || "").toLowerCase() &&
    String(position?.asset || "") === String(reservation.token_id || "")
  );
  if (exactPositions.length !== 0) fail("found an exact account position");
  return { ...evidence, exact_position_count: 0 };
}


export function validateAcknowledgedNoFillBinding({
  config,
  record,
  reservationDocument,
  completionDocument,
  nowMs = Date.now()
}) {
  const reservation = reservationDocument?.value;
  const completion = completionDocument?.value;
  const createdMs = Date.parse(reservation?.created_ts);
  const updatedMs = Date.parse(reservation?.updated_ts);
  const completedMs = Date.parse(completion?.completed_at);
  const date = Number.isFinite(createdMs) ? new Date(createdMs).toISOString().slice(0, 10) : "";
  const expectedReservationPath =
    `reports/research/venue-probe/risk-reservations/${date}/${config.probeId}.json`;
  if (record?.blob_name !== config.reservationBlobName ||
      typeof record?.etag !== "string" || !record.etag ||
      record?.reservation_sha256 !== config.reservationSha256 ||
      JSON.stringify(record?.reservation) !== JSON.stringify(reservation) ||
      reservationDocument?.blobName !== config.reservationBlobName ||
      reservationDocument?.sha256 !== config.reservationSha256 ||
      config.reservationBlobName !== expectedReservationPath ||
      reservation?.schema_version !== 1 ||
      reservation?.evidence_protocol_version !== EVIDENCE_PROTOCOL_VERSION ||
      reservation?.campaign_id !== config.campaignId ||
      reservation?.run_id !== config.runId ||
      reservation?.probe_id !== config.probeId ||
      reservation?.date !== date ||
      reservation?.state !== "unresolved_reconciliation" ||
      reservation?.order_submission_intended !== true ||
      reservation?.order_submitted !== true ||
      reservation?.order_id !== config.orderId ||
      !exactZero(reservation?.matched_notional) ||
      reservation?.reconciliation_complete !== false ||
      reservation?.zero_open_orders_confirmed !== true ||
      !/^0x[0-9a-f]{64}$/i.test(String(reservation?.condition_id || "")) ||
      !/^[1-9]\d{0,77}$/.test(String(reservation?.token_id || "")) ||
      !Number.isFinite(createdMs) || !Number.isFinite(updatedMs)) {
    failAcknowledged("reservation binding is invalid");
  }
  if (completionDocument?.blobName !== config.completionBlobName ||
      completionDocument?.sha256 !== config.completionSha256 ||
      completion?.schema !== "polyedge.operator_funded_intent_completion.v1" ||
      completion?.session_id !== config.campaignId ||
      completion?.decision_id !== config.decisionId ||
      completion?.child_run_id !== config.runId ||
      completion?.status !== "child_failed_closed_post_submission_unresolved" ||
      completion?.order_submission_attempted !== true ||
      completion?.authorization_consumed !== true ||
      completion?.risk_reservation_created !== true ||
      completion?.order_id !== config.orderId ||
      !exactZero(completion?.matched_notional) ||
      completion?.reconciliation_complete !== false ||
      completion?.zero_open_orders_confirmed !== true ||
      !String(completion?.post_submission_error || "").startsWith(
        "fail closed: post-ack error; tracked order canceled, zero open orders confirmed, unresolved risk preserved ("
      ) ||
      !Number.isFinite(completedMs) || completedMs < updatedMs ||
      !Number.isFinite(nowMs) || nowMs - completedMs < MINIMUM_COMPLETION_AGE_MS) {
    failAcknowledged("completion binding is invalid");
  }
  return { reservation, completion, etag: record.etag };
}

export function validateAcknowledgedNoFillSnapshot({
  reservation,
  config,
  order,
  openOrders,
  authenticatedTrades,
  positions,
  observedAtMs
}) {
  const status = String(order?.status || "").toUpperCase();
  const exactTradeCount = Array.isArray(authenticatedTrades)
    ? authenticatedTrades.filter((trade) => orderIds(trade).includes(config.orderId)).length
    : -1;
  const positionsValid = Array.isArray(positions) && positions.every((position) =>
    finiteNumeric(position?.size) && Number(position.size) >= 0
  );
  const unresolvedPositions = positionsValid ? positions.filter((position) =>
    Number(position?.size) > 1e-9 && position?.redeemable !== true
  ) : [null];
  const exactPositions = positionsValid ? positions.filter((position) =>
    Number(position?.size) > 1e-9 &&
    String(position?.conditionId || "").toLowerCase() ===
      String(reservation?.condition_id || "").toLowerCase() &&
    String(position?.asset || "") === String(reservation?.token_id || "")
  ) : [null];
  if (!TERMINAL_NO_FILL_STATES.has(status) ||
      order?.id !== config.orderId ||
      String(order?.market || "").toLowerCase() !== String(reservation?.condition_id || "").toLowerCase() ||
      String(order?.asset_id || "") !== String(reservation?.token_id || "") ||
      !exactZero(order?.size_matched) ||
      !Array.isArray(order?.associate_trades) || order.associate_trades.length !== 0 ||
      !Array.isArray(openOrders) || openOrders.length !== 0 ||
      exactTradeCount !== 0 || unresolvedPositions.length !== 0 || exactPositions.length !== 0 ||
      !Number.isFinite(observedAtMs)) {
    failAcknowledged("venue snapshot did not prove terminal zero-fill and zero exposure");
  }
  return {
    observed_at: new Date(observedAtMs).toISOString(),
    terminal_order_status: status,
    rest_order_matched_size: 0,
    authenticated_open_order_count: 0,
    authenticated_exact_trade_count: 0,
    unresolved_position_count: 0,
    exact_position_count: 0
  };
}

export function validateAcknowledgedNoFillSnapshots(first, second) {
  const firstMs = Date.parse(first?.observed_at);
  const secondMs = Date.parse(second?.observed_at);
  const facts = (snapshot) => ({ ...snapshot, observed_at: undefined });
  if (!Number.isFinite(firstMs) || !Number.isFinite(secondMs) ||
      secondMs - firstMs < MINIMUM_SNAPSHOT_INTERVAL_MS ||
      JSON.stringify(facts(first)) !== JSON.stringify(facts(second))) {
    failAcknowledged("venue snapshots were not stable for ten seconds");
  }
  return { first, second, observation_ms: secondMs - firstMs };
}

export async function observeAcknowledgedNoFill({
  client,
  reservation,
  config,
  fetchImpl = fetch,
  now = () => Date.now(),
  sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))
}) {
  const snapshot = async () => {
    const [order, openOrders, authenticatedTrades, positions] = await Promise.all([
      client.getOrder(config.orderId),
      client.getOpenOrders(),
      client.getTrades({ market: reservation.condition_id }),
      loadAccountPositions({
        user: config.funderAddress,
        fetcher: async (url) => {
          const response = await fetchImpl(url, { signal: AbortSignal.timeout(10_000) });
          if (!response?.ok) failAcknowledged("position reconciliation endpoint failed");
          return response.json();
        }
      })
    ]);
    return validateAcknowledgedNoFillSnapshot({
      reservation,
      config,
      order,
      openOrders,
      authenticatedTrades,
      positions,
      observedAtMs: now()
    });
  };
  const first = await snapshot();
  await sleep(MINIMUM_SNAPSHOT_INTERVAL_MS);
  const second = await snapshot();
  return validateAcknowledgedNoFillSnapshots(first, second);
}

export async function runAcknowledgedNoFillReconciliation({
  env = process.env,
  now = () => Date.now(),
  containerFactory = storageContainer,
  loadRecords = loadCampaignUnresolvedRiskReservationRecords,
  loadDocument = downloadBlobDocument,
  createClient = () => venueClient(env),
  observe = observeAcknowledgedNoFill,
  fetchImpl = fetch,
  finalize = finalizeProbeRisk,
  logger = (value) => console.log(JSON.stringify(value))
} = {}) {
  const config = acknowledgedNoFillConfig(env, {
    requireFederatedToken: containerFactory === storageContainer
  });
  const container = containerFactory(config);
  if (!container) failAcknowledged("storage is unavailable");
  const records = await loadRecords(config, { container });
  const exact = records.filter((record) => record?.blob_name === config.reservationBlobName);
  if (records.length !== 1 || exact.length !== 1) {
    failAcknowledged("did not resolve exactly one account reservation");
  }
  const [reservationDocument, completionDocument] = await Promise.all([
    loadDocument(container, config.reservationBlobName),
    loadDocument(container, config.completionBlobName)
  ]);
  const binding = validateAcknowledgedNoFillBinding({
    config,
    record: exact[0],
    reservationDocument,
    completionDocument,
    nowMs: now()
  });
  const evidence = await observe({
    client: await createClient(),
    reservation: binding.reservation,
    config,
    fetchImpl,
    now
  });
  const refreshedRecords = await loadRecords(config, { container });
  const refreshedExact = refreshedRecords.filter((record) =>
    record?.blob_name === config.reservationBlobName
  );
  const [refreshedReservationDocument, refreshedCompletionDocument] = await Promise.all([
    loadDocument(container, config.reservationBlobName),
    loadDocument(container, config.completionBlobName)
  ]);
  const refreshed = validateAcknowledgedNoFillBinding({
    config,
    record: refreshedExact[0],
    reservationDocument: refreshedReservationDocument,
    completionDocument: refreshedCompletionDocument,
    nowMs: now()
  });
  if (refreshedRecords.length !== 1 || refreshedExact.length !== 1 ||
      refreshed.etag !== binding.etag ||
      JSON.stringify(refreshed.reservation) !== JSON.stringify(binding.reservation) ||
      JSON.stringify(refreshed.completion) !== JSON.stringify(binding.completion)) {
    failAcknowledged("durable evidence changed during venue observation");
  }
  const finalized = await finalize(config, binding.reservation, {
    state: "finalized_no_fill",
    order_submitted: true,
    order_id: config.orderId,
    matched_notional: 0,
    reconciliation_complete: true,
    zero_open_orders_confirmed: true,
    reconciliation_reason: ACKNOWLEDGED_REASON,
    reconciliation_evidence: {
      source: "authenticated_clob_rest_and_polymarket_data_api",
      reservation_blob_name: config.reservationBlobName,
      reservation_sha256: config.reservationSha256,
      completion_blob_name: config.completionBlobName,
      completion_sha256: config.completionSha256,
      observations: [evidence.first, evidence.second],
      observation_ms: evidence.observation_ms
    }
  }, { container, ifMatch: binding.etag });
  const [remaining, terminalDocument] = await Promise.all([
    loadRecords(config, { container }),
    loadDocument(container, config.reservationBlobName)
  ]);
  validateAcknowledgedTerminalReadback(config, terminalDocument, finalized);
  if (remaining.length !== 0) {
    failAcknowledged("reservation remained unresolved after CAS finalization");
  }
  const result = sanitize({
    schema: "polyedge.acknowledged_no_fill_reconciliation.v1",
    status: "finalized_no_fill",
    decision_id: config.decisionId,
    run_id: config.runId,
    probe_id: finalized.probe_id,
    order_id: config.orderId,
    order_submission_attempted: true,
    source_grant_consumed: true,
    risk_reservation_created: true,
    recovery_order_submission_attempted: false,
    recovery_grant_consumed: false,
    recovery_risk_reservation_created: false,
    reconciliation_reason: ACKNOWLEDGED_REASON,
    evidence
  });
  logger(result);
  return result;
}

function validateAcknowledgedTerminalReadback(config, document, finalized) {
  const value = document?.value;
  const source = value?.reconciliation_evidence;
  const expectedSha = "sha256:" + createHash("sha256")
    .update(Buffer.from(JSON.stringify(finalized, null, 2)))
    .digest("hex");
  if (document?.blobName !== config.reservationBlobName || document?.sha256 !== expectedSha ||
      JSON.stringify(value) !== JSON.stringify(finalized) ||
      value?.state !== "finalized_no_fill" || value?.order_submitted !== true ||
      value?.order_id !== config.orderId || !exactZero(value?.matched_notional) ||
      value?.reconciliation_complete !== true || value?.zero_open_orders_confirmed !== true ||
      value?.reconciliation_reason !== ACKNOWLEDGED_REASON ||
      source?.reservation_sha256 !== config.reservationSha256 ||
      source?.completion_sha256 !== config.completionSha256 ||
      !Array.isArray(source?.observations) || source.observations.length !== 2) {
    failAcknowledged("terminal reservation readback is invalid");
  }
}

export async function runAmbiguousNoOrderReconciliation({
  env = process.env,
  now = () => Date.now(),
  containerFactory = storageContainer,
  loadRecords = loadCampaignUnresolvedRiskReservationRecords,
  loadDocument = downloadBlobDocument,
  createClient = () => venueClient(env),
  fetchImpl = fetch,
  finalize = finalizeProbeRisk,
  logger = (value) => console.log(JSON.stringify(value))
} = {}) {
  const config = ambiguousNoOrderConfig(env);
  const container = containerFactory(config);
  if (!container) fail("storage is unavailable");
  const records = await loadRecords(config, { container });
  const exact = records.filter((record) => record?.blob_name === config.reservationBlobName);
  if (records.length !== 1 || exact.length !== 1) fail("did not resolve exactly one account reservation");
  const [reservationDocument, completionDocument] = await Promise.all([
    loadDocument(container, config.reservationBlobName),
    loadDocument(container, config.completionBlobName)
  ]);
  const binding = validateAmbiguousNoOrderBinding({
    config,
    record: exact[0],
    reservationDocument,
    completionDocument,
    nowMs: now()
  });
  const client = await createClient();
  const [openOrders, authenticatedTrades, balance, positionsResponse] = await Promise.all([
    client.getOpenOrders(),
    client.getTrades({ market: binding.reservation.condition_id }),
    client.getBalanceAllowance({
      asset_type: AssetType.COLLATERAL,
      signature_type: Number(env.POLYMARKET_SIGNATURE_TYPE || 3)
    }),
    fetchImpl(
      "https://data-api.polymarket.com/positions?user=" +
        encodeURIComponent(config.funderAddress) + "&sizeThreshold=0&limit=500",
      { signal: AbortSignal.timeout(10_000) }
    )
  ]);
  if (!positionsResponse.ok) fail("position reconciliation endpoint failed");
  const positions = await positionsResponse.json();
  const evidence = validateAmbiguousNoOrderVenueEvidence({
    reservation: binding.reservation,
    runId: config.runId,
    openOrders,
    authenticatedTrades,
    positions
  });
  const finalized = await finalize(config, binding.reservation, {
    state: "finalized_no_fill",
    order_submitted: true,
    matched_notional: 0,
    reconciliation_complete: true,
    zero_open_orders_confirmed: true,
    reconciliation_reason: AMBIGUOUS_REASON,
    reconciliation_evidence: {
      ...evidence,
      submission_outcome: "ambiguous",
      source: "authenticated_clob_rest_and_polymarket_data_api",
      reservation_blob_name: config.reservationBlobName,
      reservation_sha256: config.reservationSha256,
      completion_blob_name: config.completionBlobName,
      completion_sha256: config.completionSha256,
      collateral_balance_base_units: String(balance?.balance ?? "")
    }
  }, { container, ifMatch: binding.etag });
  const remaining = await loadRecords(config, { container });
  if (remaining.length !== 0) fail("reservation remained unresolved after CAS finalization");
  const result = sanitize({
    schema: "polyedge.ambiguous_no_order_reconciliation.v1",
    status: "finalized_no_fill",
    decision_id: config.decisionId,
    run_id: config.runId,
    probe_id: finalized.probe_id,
    order_submission_attempted: true,
    original_submission_outcome: "ambiguous",
    reconciliation_reason: AMBIGUOUS_REASON,
    evidence
  });
  logger(result);
  return result;
}

async function downloadBlobDocument(container, blobName) {
  const response = await container.getBlobClient(blobName).download();
  const chunks = [];
  for await (const chunk of response.readableStreamBody) chunks.push(chunk);
  const bytes = Buffer.concat(chunks);
  return {
    blobName,
    sha256: "sha256:" + createHash("sha256").update(bytes).digest("hex"),
    value: JSON.parse(bytes.toString("utf8"))
  };
}

const isMain = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isMain) {
  const run = process.env.FUNDED_DIRECT_RECONCILIATION_REASON === ACKNOWLEDGED_REASON
    ? runAcknowledgedNoFillReconciliation
    : runAmbiguousNoOrderReconciliation;
  run().catch((error) => {
    process.exitCode = 1;
    console.error(JSON.stringify(sanitize({
      schema: process.env.FUNDED_DIRECT_RECONCILIATION_REASON === ACKNOWLEDGED_REASON
        ? "polyedge.acknowledged_no_fill_reconciliation.v1"
        : "polyedge.ambiguous_no_order_reconciliation.v1",
      status: "failed_closed",
      order_submission_attempted: false,
      authorization_consumed: false,
      risk_reservation_created: false,
      error: error.message
    })));
  });
}
