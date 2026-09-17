#!/usr/bin/env node
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { createRequire } from "node:module";
import { pathToFileURL } from "node:url";

const wallet = "0x3d701b05d7c36afab01a06fd26ebe789c0b7bad8";
const campaign = "dynamic-quote-funded-2026-09-16-v11";
const digest = value => createHash("sha256").update(JSON.stringify(value)).digest("hex");

export function validateClobRead(path, body) {
  assert(!(body && typeof body === "object" && "error" in body), "CLOB error response");
  if (path === "/time") {
    assert(Number.isSafeInteger(body) && body > 0, "invalid server time");
  } else {
    assert.equal(path, "/data/orders");
    assert(body && Array.isArray(body.data) && body.data.length === 0, "open orders");
    assert.equal(body.next_cursor, "LTE=", "incomplete open-order inventory");
  }
  return body;
}

export function validateTerminalRedemptionControl(value) {
  assert.equal(value.schema_version, 1);
  assert.equal(value.state, "confirmed_and_verified", "unresolved redemption control");
  assert.equal(String(value.funder).toLowerCase(), wallet);
  assert(value.recovery_journal_blob_name == null, "unfinished recovery publication");
  assert.equal(value.submission_attempted, true);
  assert(typeof value.transaction_id === "string" && value.transaction_id.trim());
  assert.match(value.run_id, /^venue-redemption-[0-9]{17}-[0-9a-f]{8}$/);
  assert.match(value.transaction_hash, /^0x[0-9a-f]{64}$/);
  assert(Array.isArray(value.condition_ids) && value.condition_ids.length > 0);
  for (const condition of value.condition_ids) assert.match(condition, /^0x[0-9a-f]{64}$/);
  assert.equal(new Set(value.condition_ids).size, value.condition_ids.length);
  assert(Array.isArray(value.internal_settlement_blobs) && value.internal_settlement_blobs.length > 0);
  assert.equal(value.internal_settlement_blobs.length, value.condition_ids.length);
  assert.equal(new Set(value.internal_settlement_blobs).size, value.internal_settlement_blobs.length);
  for (const path of value.internal_settlement_blobs) {
    assert.match(path, new RegExp(`^reports/funded/dynamic-quote/sessions/${campaign}/internal-settlements/[0-9a-f]{64}\\.json$`));
  }
  return value.state;
}

// Called through stdin in the current signer. This helper only reads; it never
// runs redemption, acquires a campaign lease, or publishes control records.
export async function collectApprovedRedemptionPreflight({
  condition, targetImage, targetRevision, readOrders, readPositions,
  readReservations, discover, block, now = () => new Date(),
  wait = milliseconds => new Promise(resolve => setTimeout(resolve, milliseconds))
}) {
  assert.match(condition, /^0x[0-9a-f]{64}$/);
  assert.match(targetImage, /^ghcr\.io\/aldoapicella\/polyedge-venue-probe@sha256:[0-9a-f]{64}$/);
  assert.match(targetRevision, /^[0-9a-f]{40}$/);
  assert.equal(block.chain_id, 137);
  assert.match(block.hash, /^0x[0-9a-f]{64}$/);
  assert.match(block.number, /^\d+$/);
  let started = now();
  assert(Number.isFinite(started.getTime()));
  // A just-produced block can lead the local clock by a fraction of a second.
  // Begin evidence only after its timestamp; never accept a future block.
  const ahead = block.timestamp * 1000 - started.getTime();
  if (ahead > 0) {
    assert(ahead <= 1000, "block clock leads local time by more than one second");
    await wait(Math.ceil(ahead));
    started = now();
    assert(Number.isFinite(started.getTime()));
  }
  assert(block.timestamp <= started.getTime() / 1000 && block.timestamp >= started.getTime() / 1000 - 60);
  const snapshot = async () => {
    const [orders, positions, reservations] = await Promise.all([
      readOrders(), readPositions(), readReservations()
    ]);
    assert(Array.isArray(orders) && orders.length === 0, "open orders");
    assert(Array.isArray(reservations) && reservations.length === 0, "unresolved reservations");
    assert(Array.isArray(positions), "missing position inventory");
    const assets = new Set();
    for (const row of positions) {
      assert(typeof row.asset === "string" && /^[1-9][0-9]*$/.test(row.asset) && BigInt(row.asset) < 2n ** 256n,
        "invalid position asset");
      assert(!assets.has(row.asset), "overlapping position pages");
      assets.add(row.asset);
      assert(typeof row.size === "number" && Number.isFinite(row.size) && row.size >= 0);
      assert(typeof row.currentValue === "number" && Number.isFinite(row.currentValue) && row.currentValue >= 0);
      assert(row.redeemable === true || (row.size === 0 && row.currentValue === 0), "live position");
    }
    return positions;
  };
  const before = await snapshot();
  const selection = await discover(before);
  assert.equal(selection.payout_source, "onchain_balances_and_payout_vector");
  assert.equal(selection.available_winner_conditions, 1, "not exactly one winner");
  assert.equal(selection.skipped_winner_conditions, 0);
  assert.equal(selection.selected.length, 1);
  const selected = selection.selected[0];
  assert.equal(selected.condition_id, condition, "wrong winning condition");
  for (const key of ["asset_ids", "onchain_balances_base_units", "payout_numerators"]) {
    assert(Array.isArray(selected[key]) && selected[key].length === 2);
    assert(selected[key].every(value => typeof value === "string" && /^\d+$/.test(value)));
  }
  assert.equal(new Set(selected.asset_ids).size, 2);
  assert.match(selected.payout_denominator, /^\d+$/);
  const denominator = BigInt(selected.payout_denominator);
  const numerators = selected.payout_numerators.map(BigInt);
  assert(denominator > 0n && numerators[0] + numerators[1] === denominator);
  const payout = selected.onchain_balances_base_units.reduce((sum, balance, index) =>
    sum + BigInt(balance) * numerators[index] / denominator, 0n);
  assert(payout > 0n && payout <= BigInt(Number.MAX_SAFE_INTEGER));
  assert.equal(selected.gross_payout, Number(payout) / 1e6);
  assert.equal(selected.onchain_expected_payout, selected.gross_payout);
  assert.equal(selection.selected_gross_payout, selected.gross_payout);
  const after = await snapshot();
  assert.equal(digest(after), digest(before), "position inventory changed during preflight");
  const finished = now();
  assert(finished >= started && finished - started <= 120000, "preflight exceeded its time bound");
  return {
    schema: "polyedge.approved_redemption_preflight.v1", status: "approved_redemption_ready",
    read_only: true, redemption_submitted: false, funder: wallet,
    condition_id: condition, target_image: targetImage, target_revision: targetRevision,
    started_ts: started.toISOString(), finished_ts: finished.toISOString(), block,
    open_order_count: 0, unresolved_position_count: 0, unresolved_reservation_count: 0,
    position_inventory: { reader: "loadAccountPositions", complete: true, readbacks: 2,
      rows: after.length, sha256: digest(after) },
    payout_base_units: String(payout), selection
  };
}

async function main() {
  const [mode, condition, targetImage, targetRevision, ...extra] = process.argv.slice(2);
  assert.equal(mode, "--collect"); assert.equal(extra.length, 0);
  assert(!process.env.AZURE_STORAGE_ACCOUNT_KEY, "storage keys forbidden");
  assert.equal(String(process.env.POLYMARKET_FUNDER_ADDRESS).toLowerCase(), wallet);
  assert.equal(process.env.VENUE_PROBE_FUNDED_CAMPAIGN_ID, campaign);
  assert.equal(process.env.AZURE_STORAGE_ACCOUNT_NAME, "stpolyedge6urdjr5nmwx7w");
  assert.equal(process.env.AZURE_STORAGE_CONTAINER_NAME, "polyedge-funded-evidence");
  const require = createRequire("/app/package.json");
  const { createPublicClient, http } = require("viem");
  const { polygon } = require("viem/chains");
  const { venueClient } = await import("/app/src/reconcile-rejected-no-order.mjs");
  const { loadAccountPositions } = await import("/app/src/canary.mjs");
  const { loadCampaignUnresolvedRiskReservationRecords, storageContainer } = await import("/app/src/lib.mjs");
  const { discoverOnchainRedeemableConditions } = await import("/app/src/redeem.mjs");
  const { deriveLegacyUupsDepositWallet } = await import("/app/src/redemption.mjs");
  const clob = venueClient(process.env);
  assert.equal(deriveLegacyUupsDepositWallet(clob.signer.account.address).toLowerCase(), wallet);
  const container = storageContainer({ storageAccount: process.env.AZURE_STORAGE_ACCOUNT_NAME,
    storageContainer: process.env.AZURE_STORAGE_CONTAINER_NAME, azureClientId: process.env.AZURE_CLIENT_ID });
  const controlPath = `reports/funded/dynamic-quote/sessions/${process.env.VENUE_PROBE_FUNDED_CAMPAIGN_ID}/control/redemption-state.json`;
  const readControl = async etag => {
    try {
      const response = await container.getBlobClient(controlPath).download(0, undefined,
        { ...(etag ? { conditions: { ifMatch: etag } } : {}), abortSignal: AbortSignal.timeout(20000) });
      const parts = []; let size = 0;
      for await (const chunk of response.readableStreamBody) {
        size += chunk.length; assert(size <= 1048576); parts.push(chunk);
      }
      const body = Buffer.concat(parts), value = JSON.parse(body);
      assert(response.etag);
      return { path: controlPath, exists: true, state: validateTerminalRedemptionControl(value),
        etag: response.etag, sha256: createHash("sha256").update(body).digest("hex") };
    } catch (error) {
      if (error.statusCode !== 404 || error.code !== "BlobNotFound" || etag) throw error;
      return { path: controlPath, exists: false };
    }
  };
  const controlBefore = await readControl();
  // Keep SDK authentication/query construction while preventing error logging of headers.
  clob.get = async (endpoint, options = {}) => {
    const url = new URL(endpoint);
    assert.equal(url.origin, "https://clob.polymarket.com");
    assert(["/time", "/data/orders"].includes(url.pathname));
    for (const [key, value] of Object.entries(options.params ?? {})) {
      if (value !== undefined && value !== null) url.searchParams.set(key, String(value));
    }
    const response = await fetch(url, { method: "GET", headers: options.headers,
      redirect: "error", signal: AbortSignal.timeout(20000) });
    assert(response.ok, "authenticated order read failed");
    return validateClobRead(url.pathname, await response.json());
  };
  const rpc = createPublicClient({ chain: polygon, batch: { multicall: true },
    transport: http("https://polygon-bor-rpc.publicnode.com", { timeout: 15000, retryCount: 1 }) });
  assert.equal(await rpc.getChainId(), 137);
  const block = await rpc.getBlock();
  const pinned = { readContract: args => rpc.readContract({ ...args, blockNumber: block.number }) };
  const result = await collectApprovedRedemptionPreflight({ condition, targetImage, targetRevision,
    block: { chain_id: 137, number: String(block.number), hash: block.hash, timestamp: Number(block.timestamp) },
    readOrders: () => clob.getOpenOrders(),
    readPositions: () => loadAccountPositions({ user: wallet }),
    readReservations: async () => (await Promise.all([
      "dynamic-quote-funded-2026-08-13-v10", "dynamic-quote-funded-2026-09-16-v11"
    ].map(campaignId => loadCampaignUnresolvedRiskReservationRecords({ campaignId,
      operatorDirect: true, dryRun: false, storageAccount: process.env.AZURE_STORAGE_ACCOUNT_NAME,
      storageContainer: process.env.AZURE_STORAGE_CONTAINER_NAME, azureClientId: process.env.AZURE_CLIENT_ID })))).flat(),
    discover: positions => discoverOnchainRedeemableConditions(pinned, positions,
      { funderAddress: wallet, maxPayout: null, maxConditions: Number.MAX_SAFE_INTEGER })
  });
  assert.deepEqual(await readControl(controlBefore.etag), controlBefore, "redemption control changed during preflight");
  result.redemption_control = { ...controlBefore, terminal: true, readbacks: 2 };
  assert.equal((await rpc.getBlock({ blockNumber: block.number })).hash, block.hash, "block reorganized");
  process.stdout.write(JSON.stringify(result) + "\n");
}

if ((process.argv[1] === "-" && process.argv[2] === "--collect") ||
    (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href)) {
  main().catch(error => {
    const location = String(error.stack).match(/(?:\[eval1\]|file:\/\/\/app\/[^\s:]+):[0-9]+:[0-9]+/)?.[0] ?? "unavailable";
    console.error(`approved redemption preflight failed: ${error.name} at ${location}`);
    process.exitCode = 1;
  });
}
