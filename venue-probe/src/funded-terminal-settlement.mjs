import { lstatSync, readFileSync } from "node:fs";
import { isAbsolute } from "node:path";
import { pathToFileURL } from "node:url";
import {
  loadCampaignUnresolvedRiskReservationRecords,
  sanitize,
  settleProbeRiskReservations,
  storageContainer
} from "./lib.mjs";

const PREFIX = "reports/research/venue-probe/risk-reservations/";
const TENANT_ID = "9767f0dc-e83f-4cc1-94e1-0d5f9d287d32";
const STORAGE_ACCOUNT = "stpolyedge6urdjr5nmwx7w";
const STORAGE_CONTAINER = "polyedge-funded-evidence";
const FUNDED_UAMI_CLIENT_ID = "e98d6475-681c-4f75-81f1-0eff9ea5e332";
const CAMPAIGN_ID = "dynamic-quote-funded-2026-08-12-v9";
const FUNDER_ADDRESS = "0x3d701b05d7c36afab01a06fd26ebe789c0b7bad8";
const JWT_SHAPE = /^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/;

export function validateTerminalSettlementFederatedTokenFile(
  tokenPath,
  { stat = lstatSync, read = readFileSync, uid = process.getuid?.() } = {}
) {
  if (typeof tokenPath !== "string" || !isAbsolute(tokenPath)) {
    throw new Error("fail closed: AZURE_FEDERATED_TOKEN_FILE must be an absolute path");
  }
  let tokenFile;
  let assertion;
  try {
    tokenFile = stat(tokenPath);
    assertion = read(tokenPath, "utf8");
  } catch {
    throw new Error("fail closed: AZURE_FEDERATED_TOKEN_FILE must be a readable regular file");
  }
  if (!tokenFile.isFile() || tokenFile.isSymbolicLink() || (tokenFile.mode & 0o077) !== 0 ||
      (tokenFile.mode & 0o400) === 0 || (typeof uid === "number" && tokenFile.uid !== uid) ||
      typeof assertion !== "string" || assertion.length === 0 || assertion.length > 16_384 ||
      !JWT_SHAPE.test(assertion)) {
    throw new Error("fail closed: AZURE_FEDERATED_TOKEN_FILE must be an owner-only regular JWT file");
  }
}

export function terminalSettlementConfig(env = process.env, { requireFederatedToken = false } = {}) {
  const campaignId = String(env.VENUE_PROBE_FUNDED_CAMPAIGN_ID || "").trim();
  const decisionId = String(env.FUNDED_TERMINAL_SETTLEMENT_DECISION_ID || "").trim();
  const blobName = String(env.FUNDED_TERMINAL_SETTLEMENT_RESERVATION_BLOB_NAME || "").trim();
  const funderAddress = String(env.POLYMARKET_FUNDER_ADDRESS || "").trim();
  if (env.FUNDED_TERMINAL_SETTLEMENT_ENABLED !== "true" ||
      campaignId !== CAMPAIGN_ID || !/^[0-9a-f]{64}$/.test(decisionId) ||
      funderAddress !== FUNDER_ADDRESS ||
      !new RegExp(`^${PREFIX}\\d{4}-\\d{2}-\\d{2}/funded-direct-${decisionId}\\.json$`).test(blobName) ||
      env.AZURE_TENANT_ID !== TENANT_ID ||
      env.AZURE_STORAGE_ACCOUNT_NAME !== STORAGE_ACCOUNT ||
      env.AZURE_STORAGE_CONTAINER_NAME !== STORAGE_CONTAINER ||
      env.AZURE_CLIENT_ID !== FUNDED_UAMI_CLIENT_ID ||
      env.AZURE_TOKEN_CREDENTIALS !== "WorkloadIdentityCredential" ||
      Object.hasOwn(env, "AZURE_STORAGE_ACCOUNT_KEY")) {
    throw new Error("fail closed: exact terminal settlement binding is invalid");
  }
  if (requireFederatedToken) validateTerminalSettlementFederatedTokenFile(env.AZURE_FEDERATED_TOKEN_FILE);
  return {
    campaignId,
    decisionId,
    blobName,
    funderAddress,
    operatorDirect: true,
    dryRun: false,
    storageAccount: STORAGE_ACCOUNT,
    storageContainer: STORAGE_CONTAINER,
    azureClientId: FUNDED_UAMI_CLIENT_ID
  };
}

function exactReservation(record, config) {
  const reservation = record?.reservation;
  if (record?.blob_name !== config.blobName || reservation?.campaign_id !== config.campaignId ||
      reservation?.probe_id !== `funded-direct-${config.decisionId}` ||
      reservation?.order_submitted !== true ||
      !/^0x[0-9a-f]{64}$/i.test(String(reservation?.order_id || "")) ||
      !(Number(reservation?.matched_notional) > 0) ||
      reservation?.zero_open_orders_confirmed !== true ||
      !/^0x[0-9a-f]{64}$/i.test(String(reservation?.condition_id || ""))) {
    throw new Error("fail closed: terminal settlement reservation evidence is invalid");
  }
  return reservation;
}

async function terminalPosition(fetchImpl, config, reservation) {
  const url = new URL("https://data-api.polymarket.com/positions");
  url.searchParams.set("user", config.funderAddress);
  url.searchParams.set("sizeThreshold", "0");
  url.searchParams.set("limit", "500");
  const response = await fetchImpl(url, { signal: AbortSignal.timeout(10_000) });
  if (!response.ok) throw new Error(`fail closed: terminal position lookup failed (${response.status})`);
  const rows = await response.json();
  const matches = Array.isArray(rows) ? rows.filter((row) =>
    String(row?.conditionId || "").toLowerCase() === String(reservation.condition_id).toLowerCase() &&
    row?.redeemable === true && Number(row?.size) > 0 && Number(row?.currentValue) === 0
  ) : [];
  if (matches.length !== 1) {
    throw new Error("fail closed: Data API does not prove one redeemable terminal position");
  }
  return matches[0];
}

export async function runFundedTerminalSettlement({
  env = process.env,
  containerFactory = storageContainer,
  loadUnresolved = loadCampaignUnresolvedRiskReservationRecords,
  fetchImpl = fetch,
  settle = settleProbeRiskReservations,
  logger = (value) => console.log(JSON.stringify(value))
} = {}) {
  const config = terminalSettlementConfig(env, { requireFederatedToken: containerFactory === storageContainer });
  const container = containerFactory(config);
  if (!container) throw new Error("fail closed: terminal settlement storage is unavailable");
  const records = await loadUnresolved(config, { container });
  const exact = records.filter((record) => record?.blob_name === config.blobName);
  if (exact.length !== 1) throw new Error("fail closed: exact unresolved terminal reservation was not found");
  const reservation = exactReservation(exact[0], config);
  const position = await terminalPosition(fetchImpl, config, reservation);
  const settled = await settle(config, {
    condition_ids: [reservation.condition_id],
    terminal_settlement_verified: true,
    evidence_source: "polymarket_data_api_redeemable",
    run_id: `terminal-settlement-${config.decisionId}`
  }, { container, reservationRecords: exact });
  if (settled !== 1) throw new Error("fail closed: exact terminal reservation was not settled");
  const stillUnresolved = await loadUnresolved(config, { container });
  if (stillUnresolved.some((record) => record?.blob_name === config.blobName)) {
    throw new Error("fail closed: exact terminal reservation remains unresolved after settlement");
  }
  const proof = sanitize({
    schema: "polyedge.funded_terminal_settlement.v1",
    status: "position_settled",
    campaign_id: config.campaignId,
    decision_id: config.decisionId,
    reservation_blob_name: config.blobName,
    order_submission_attempted: false,
    authorization_consumed: false,
    risk_reservation_created: false,
    source: "polymarket_data_api_redeemable",
    terminal_position_size: Number(position.size),
    terminal_position_value: Number(position.currentValue),
    zero_open_orders_confirmed: true
  });
  logger(proof);
  return proof;
}

const isMain = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isMain) {
  runFundedTerminalSettlement().catch((error) => {
    process.exitCode = 1;
    console.error(JSON.stringify(sanitize({
      schema: "polyedge.funded_terminal_settlement.v1",
      status: "failed_closed",
      order_submission_attempted: false,
      authorization_consumed: false,
      risk_reservation_created: false,
      error: error.message
    })));
  });
}
