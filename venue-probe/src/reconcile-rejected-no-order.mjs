import { AssetType, Chain, ClobClient } from "@polymarket/clob-client-v2";
import { pathToFileURL } from "node:url";
import { createWalletClient, http } from "viem";
import { privateKeyToAccount } from "viem/accounts";
import { polygon } from "viem/chains";
import {
  finalizeProbeRisk,
  loadUnresolvedRiskReservations,
  sanitize
} from "./lib.mjs";

const EXPECTED_REJECTION_CODES = new Set([
  "invalid_gtd_expiration",
  "post_only_crosses_book"
]);

export function validateRejectedReservationRecovery({
  reservation,
  expectedRunId,
  openOrders,
  authenticatedTrades,
  positions
}) {
  const fail = (message) => { throw new Error(`fail closed: rejected-order recovery ${message}`); };
  if (!reservation || reservation.run_id !== expectedRunId) fail("did not resolve the exact run");
  if (reservation.state !== "reserved" ||
      ![null, false].includes(reservation.order_submitted) ||
      ![null, 0].includes(reservation.matched_notional)) {
    fail("requires an untouched no-ack reservation");
  }
  const createdMs = Date.parse(reservation.created_ts);
  if (!Number.isFinite(createdMs)) fail("reservation creation time is invalid");
  if (!Array.isArray(openOrders) || openOrders.length !== 0) fail("found an authenticated open order");
  if (!Array.isArray(authenticatedTrades)) fail("authenticated trade history is unavailable");
  const postReservationTrades = authenticatedTrades.filter((trade) => {
    const timestampMs = tradeTimestampMs(trade);
    if (!Number.isFinite(timestampMs)) fail("encountered an authenticated trade without a timestamp");
    return timestampMs >= createdMs - 5_000;
  });
  if (postReservationTrades.length !== 0) fail("found an authenticated trade after risk reservation");
  if (!Array.isArray(positions)) fail("position reconciliation is unavailable");
  const unresolvedPositions = positions.filter((position) =>
    Number(position?.size) > 1e-9 && position?.redeemable !== true
  );
  if (unresolvedPositions.length !== 0) fail("found an unresolved account position");
  return {
    created_ms: createdMs,
    authenticated_open_order_count: 0,
    authenticated_post_reservation_trade_count: 0,
    unresolved_position_count: 0
  };
}

export async function runRejectedNoOrderReconciliation({
  env = process.env,
  createClient = () => venueClient(env),
  fetchImpl = fetch,
  loadReservations = loadUnresolvedRiskReservations,
  finalize = finalizeProbeRisk
} = {}) {
  if (env.FUNDED_DIRECT_RECONCILIATION_ENABLED !== "true") {
    throw new Error("fail closed: FUNDED_DIRECT_RECONCILIATION_ENABLED must be true");
  }
  const expectedRunId = String(env.FUNDED_DIRECT_RECONCILE_RUN_ID || "").trim();
  if (!expectedRunId) throw new Error("fail closed: exact FUNDED_DIRECT_RECONCILE_RUN_ID is required");
  const expectedRejectionCode = String(env.FUNDED_DIRECT_RECONCILE_REJECTION_CODE || "").trim();
  if (!EXPECTED_REJECTION_CODES.has(expectedRejectionCode)) {
    throw new Error("fail closed: rejection code is not an exact deterministic no-order code");
  }
  const campaignId = String(env.VENUE_PROBE_FUNDED_CAMPAIGN_ID || "").trim();
  if (!campaignId) throw new Error("fail closed: exact VENUE_PROBE_FUNDED_CAMPAIGN_ID is required");
  const config = {
    storageAccount: env.AZURE_STORAGE_ACCOUNT_NAME,
    storageContainer: env.AZURE_STORAGE_CONTAINER_NAME,
    storageAccountKey: env.AZURE_STORAGE_ACCOUNT_KEY,
    azureClientId: env.AZURE_CLIENT_ID,
    campaignId,
    operatorDirect: true,
    dryRun: false
  };
  const reservations = await loadReservations(config);
  const matches = reservations.filter((reservation) => reservation.run_id === expectedRunId);
  if (matches.length !== 1) {
    throw new Error(`fail closed: expected exactly one unresolved reservation for ${expectedRunId}, found ${matches.length}`);
  }
  const reservation = matches[0];
  const client = await createClient();
  const [openOrders, authenticatedTrades, balance, positionsResponse] = await Promise.all([
    client.getOpenOrders(),
    client.getTrades({ market: reservation.condition_id }),
    client.getBalanceAllowance({
      asset_type: AssetType.COLLATERAL,
      signature_type: integer(env.POLYMARKET_SIGNATURE_TYPE, 3)
    }),
    fetchImpl(
      `https://data-api.polymarket.com/positions?user=${encodeURIComponent(env.POLYMARKET_FUNDER_ADDRESS)}&sizeThreshold=0&limit=500`,
      { signal: AbortSignal.timeout(10_000) }
    )
  ]);
  if (!positionsResponse.ok) throw new Error("fail closed: account position reconciliation endpoint failed");
  const positions = await positionsResponse.json();
  const evidence = validateRejectedReservationRecovery({
    reservation,
    expectedRunId,
    openOrders,
    authenticatedTrades,
    positions
  });
  const finalized = await finalize(config, reservation, {
    state: "released_no_order",
    order_submitted: false,
    matched_notional: 0,
    reconciliation_complete: true,
    zero_open_orders_confirmed: true,
    reconciliation_reason: expectedRejectionCode,
    reconciliation_evidence: {
      ...evidence,
      source: "authenticated_clob_rest_and_polymarket_data_api",
      collateral_balance_base_units: String(balance?.balance ?? "")
    }
  });
  const remaining = await loadReservations(config);
  if (remaining.some((value) => value.probe_id === reservation.probe_id)) {
    throw new Error("fail closed: risk reservation remained unresolved after recovery");
  }
  return {
    schema: "polyedge.rejected_no_order_reconciliation.v1",
    status: "released_no_order",
    run_id: expectedRunId,
    probe_id: finalized.probe_id,
    rejection_code: expectedRejectionCode,
    evidence
  };
}

export function venueClient(env) {
  const account = privateKeyToAccount(normalizePrivateKey(env.POLYMARKET_PRIVATE_KEY));
  const signer = createWalletClient({
    account,
    chain: polygon,
    transport: http("https://polygon-bor-rpc.publicnode.com")
  });
  return new ClobClient({
    host: env.POLYMARKET_CLOB_URL || "https://clob.polymarket.com",
    chain: Chain.POLYGON,
    signer,
    creds: {
      key: env.POLYMARKET_API_KEY,
      secret: env.POLYMARKET_API_SECRET,
      passphrase: env.POLYMARKET_API_PASSPHRASE
    },
    signatureType: integer(env.POLYMARKET_SIGNATURE_TYPE, 3),
    funderAddress: env.POLYMARKET_FUNDER_ADDRESS,
    useServerTime: true,
    throwOnError: true
  });
}

function tradeTimestampMs(trade) {
  for (const value of [
    trade?.match_time,
    trade?.timestamp,
    trade?.created_at,
    trade?.createdAt
  ]) {
    if (value === null || value === undefined || value === "") continue;
    const numeric = Number(value);
    if (Number.isFinite(numeric)) return numeric < 1e12 ? numeric * 1_000 : numeric;
    const parsed = Date.parse(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  return null;
}

function normalizePrivateKey(value) {
  const clean = String(value || "").trim();
  return clean.startsWith("0x") ? clean : `0x${clean}`;
}

function integer(value, fallback) {
  const parsed = Number(value);
  return Number.isInteger(parsed) ? parsed : fallback;
}

const isMain = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isMain) {
  runRejectedNoOrderReconciliation()
    .then((result) => console.log(JSON.stringify(sanitize(result))))
    .catch((error) => {
      process.exitCode = 1;
      console.error(JSON.stringify(sanitize({
        schema: "polyedge.rejected_no_order_reconciliation.v1",
        status: "failed_closed",
        error: error.message
      })));
    });
}
