import { AssetType } from "@polymarket/clob-client-v2";
import { createHash } from "node:crypto";
import { pathToFileURL } from "node:url";
import {
  finalizeProbeRisk,
  loadCampaignUnresolvedRiskReservationRecords,
  sanitize,
  storageContainer
} from "./lib.mjs";
import {
  validateRejectedReservationRecovery,
  venueClient
} from "./reconcile-rejected-no-order.mjs";

const TENANT_ID = "9767f0dc-e83f-4cc1-94e1-0d5f9d287d32";
const STORAGE_ACCOUNT = "stpolyedge6urdjr5nmwx7w";
const STORAGE_CONTAINER = "polyedge-funded-evidence";
const FUNDED_UAMI_CLIENT_ID = "d9ce9154-66a6-4bdb-839f-0da7b02b38da";
const CAMPAIGN_ID = "dynamic-quote-funded-2026-08-13-v10";
const FUNDER_ADDRESS = "0x3d701b05d7c36afab01a06fd26ebe789c0b7bad8";
const REASON = "ambiguous_submission_no_fill";
const MINIMUM_COMPLETION_AGE_MS = 24 * 60 * 60 * 1_000;
const SHA256 = /^sha256:[0-9a-f]{64}$/;

function fail(message) {
  throw new Error("fail closed: ambiguous no-order reconciliation " + message);
}

export function ambiguousNoOrderConfig(env = process.env) {
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
      env.FUNDED_DIRECT_RECONCILIATION_REASON !== REASON ||
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
    fail("environment is not exact");
  }
  const probeId = "funded-direct-" + decisionId;
  const completionPath =
    "reports/funded/dynamic-quote/sessions/" + CAMPAIGN_ID + "/completed/" +
    decisionId + ".json";
  if (!new RegExp(
    "^reports/research/venue-probe/risk-reservations/\\d{4}-\\d{2}-\\d{2}/" +
      probeId + "\\.json$"
  ).test(reservationBlobName) || completionBlobName !== completionPath) {
    fail("blob namespace is not exact");
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
      Number(completion?.matched_notional) !== 0 ||
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
    reconciliation_reason: REASON,
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
    reconciliation_reason: REASON,
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
  runAmbiguousNoOrderReconciliation().catch((error) => {
    process.exitCode = 1;
    console.error(JSON.stringify(sanitize({
      schema: "polyedge.ambiguous_no_order_reconciliation.v1",
      status: "failed_closed",
      order_submission_attempted: false,
      error: error.message
    })));
  });
}
