import { createHash } from "node:crypto";

const STATE_SCHEMA = "polyedge.protected_compounding_state.v1";
const MANUAL_SETTLEMENT_TYPE = "internal_manual_settlement";
const AUTOMATIC_SETTLEMENT_TYPE = "internal_automatic_settlement";
const RESOLVED_LOSS_TYPE = "internal_resolved_loss";
const ZERO_TRANSACTION_HASH = `0x${"0".repeat(64)}`;
const MONEY_SCALE = 1_000_000;
const SIZE_SCALE = 100;
const ACTIVITY_MATCH_TOLERANCE_MS = 120_000;

export function validateProtectedCompoundingManifest(manifest) {
  const policy = manifest?.capital_policy;
  const settlements = Array.isArray(manifest?.internal_settlements)
    ? manifest.internal_settlements
    : [];
  const errors = [];
  if (manifest?.allow_compounding !== true) errors.push("allow_compounding must be true");
  if (policy?.reserve_monotonic !== true) errors.push("capital_policy.reserve_monotonic must be true");
  if (policy?.high_water_update !== "full_reconciliation_only") {
    errors.push("capital_policy.high_water_update must equal full_reconciliation_only");
  }
  if (Number(policy?.reserve_ratio) !== 0.3) errors.push("capital_policy.reserve_ratio must equal 0.3");
  if (Number(policy?.operating_buffer_ratio) !== 0.01) {
    errors.push("capital_policy.operating_buffer_ratio must equal 0.01");
  }
  if (!(Number(policy?.minimum_order_notional) >= 1)) {
    errors.push("capital_policy.minimum_order_notional must be at least 1");
  }
  const expectedStateBlobName = `reports/funded/dynamic-quote/sessions/${manifest?.session_id}/capital-reserve-state.json`;
  if (!safeBlobName(policy?.state_blob_name) || policy?.state_blob_name !== expectedStateBlobName) {
    errors.push("capital_policy.state_blob_name must be scoped to this exact session");
  }
  if (new Set(settlements.map((row) => row?.id)).size !== settlements.length) {
    errors.push("internal_settlements identities must be unique");
  }
  if (settlements.some((row) => !validInternalSettlement(row))) {
    errors.push("internal_settlements contains an invalid settlement");
  }
  if (errors.length) throw new Error(`protected compounding policy is invalid: ${errors.join("; ")}`);
  return {
    reserveRatio: Number(policy.reserve_ratio),
    operatingBufferRatio: Number(policy.operating_buffer_ratio),
    minimumOrderNotional: Number(policy.minimum_order_notional),
    stateBlobName: String(policy.state_blob_name),
    internalSettlements: settlements
  };
}

export async function verifyConfiguredInternalSettlements({
  manifest,
  activity,
  getTransactionReceipt,
  durableSettlements = []
}) {
  const policy = validateProtectedCompoundingManifest(manifest);
  const rows = Array.isArray(activity) ? activity : [];
  const verified = [];
  for (const settlement of policy.internalSettlements) {
    const durable = durableSettlements.find((row) =>
      row.id === settlement.id
      && row.type === MANUAL_SETTLEMENT_TYPE
      && validDurableInternalSettlement(row)
      && settlementAccountingEqual(row, settlement)
    );
    if (durable) {
      verified.push(durable);
      continue;
    }
    const transactionHash = normalizedHash(settlement.transaction_hash);
    const conditionId = normalizedHash(settlement.condition_id);
    const redemption = rows.find((row) =>
      String(row?.type || "").toUpperCase() === "REDEEM"
      && normalizedHash(row?.transactionHash) === transactionHash
      && normalizedHash(row?.conditionId) === conditionId
    );
    if (!redemption || !moneyEqual(redemption.usdcSize, settlement.payout)) {
      throw new Error(`manual settlement ${settlement.id} does not match Data API redemption evidence`);
    }
    const fillHashes = new Set(settlement.fill_transaction_hashes.map(normalizedHash));
    const fills = rows.filter((row) =>
      String(row?.type || "").toUpperCase() === "TRADE"
      && normalizedHash(row?.conditionId) === conditionId
      && fillHashes.has(normalizedHash(row?.transactionHash))
    );
    if (fills.length !== fillHashes.size
        || !moneyEqual(sum(fills, "size"), settlement.payout)
        || !moneyEqual(sum(fills, "usdcSize"), settlement.principal)
        || !moneyEqual(Number(settlement.payout) - Number(settlement.principal), settlement.realized_pnl)) {
      throw new Error(`manual settlement ${settlement.id} does not match its authenticated fills`);
    }
    const receipt = await getTransactionReceipt(transactionHash);
    if (receipt?.status !== "success"
        || Number(receipt.chain_id) !== 137
        || !Number.isInteger(Number(receipt.confirmations))
        || Number(receipt.confirmations) < 2) {
      throw new Error(`manual settlement ${settlement.id} lacks a confirmed successful Polygon receipt`);
    }
    verified.push({
      ...settlement,
      type: MANUAL_SETTLEMENT_TYPE,
      transaction_hash: transactionHash,
      condition_id: conditionId,
      payout: money(settlement.payout),
      principal: money(settlement.principal),
      realized_pnl: money(settlement.realized_pnl),
      evidence_source: "polymarket_data_api_fills_plus_polygon_receipt",
      receipt_block_number: String(receipt.block_number),
      receipt_confirmations: Number(receipt.confirmations)
    });
  }
  return verified;
}

export async function discoverVerifiedAutomaticInternalSettlements({
  manifest,
  reservations,
  activity,
  durableSettlements = [],
  getOrderFills,
  getTransactionReceipt
}) {
  validateProtectedCompoundingManifest(manifest);
  if (typeof getOrderFills !== "function" || typeof getTransactionReceipt !== "function") {
    throw new Error("fail closed: automatic settlement authenticated evidence readers are unavailable");
  }
  const sessionId = String(manifest.session_id || "");
  const sessionStartedMs = activityTimestampMs(manifest.created_at);
  if (!sessionId || !Number.isFinite(sessionStartedMs)) {
    throw new Error("fail closed: automatic settlement session time binding is invalid");
  }
  const rows = Array.isArray(activity) ? activity : [];
  const durable = Array.isArray(durableSettlements) ? durableSettlements : [];
  if (durable.some((row) => !validDurableInternalSettlement(row) || row.session_id !== sessionId)) {
    throw new Error("fail closed: automatic settlement durable ledger binding is invalid");
  }
  const redemptions = rows.filter((row) =>
    String(row?.type || "").toUpperCase() === "REDEEM"
      && normalizedHash(row?.transactionHash)
      && normalizedHash(row?.conditionId)
      && Number(row?.usdcSize) > 0
      && activityTimestampMs(row?.timestamp) >= sessionStartedMs
  );
  const identities = new Set();
  for (const redemption of redemptions) {
    const identity = `${normalizedHash(redemption.transactionHash)}:${normalizedHash(redemption.conditionId)}`;
    if (identities.has(identity)) {
      throw new Error("fail closed: automatic settlement Data API redemption evidence is duplicated");
    }
    identities.add(identity);
  }
  const pending = redemptions.filter((redemption) => !durable.some((settlement) =>
    normalizedHash(settlement.transaction_hash) === normalizedHash(redemption.transactionHash)
      && normalizedHash(settlement.condition_id) === normalizedHash(redemption.conditionId)
  ));
  const values = await Promise.all(pending.map(async (redemption) => {
    const conditionId = normalizedHash(redemption.conditionId);
    const matchingReservations = (Array.isArray(reservations) ? reservations : []).filter((reservation) =>
      reservation?.campaign_id === sessionId
        && normalizedHash(reservation?.condition_id) === conditionId
        && reservation?.order_submission_intended === true
        && reservation?.order_submitted === true
        && typeof reservation?.order_id === "string"
        && reservation.order_id.length > 0
        && typeof reservation?.probe_id === "string"
        && reservation.probe_id.length > 0
        && typeof reservation?.run_id === "string"
        && reservation.run_id.length > 0
        && Number(reservation?.matched_notional) > 0
    );
    if (matchingReservations.length !== 1) {
      throw new Error("fail closed: automatic settlement redemption does not bind one exact funded reservation");
    }
    const reservation = matchingReservations[0];
    const [orderFills, receipt] = await Promise.all([
      getOrderFills(reservation),
      getTransactionReceipt(normalizedHash(redemption.transactionHash))
    ]);
    return verifyAutomaticSettlementEvidence({
      manifest,
      reservation,
      redemption,
      activity: rows,
      orderFills,
      receipt
    });
  }));
  return values;
}

export function verifyAutomaticSettlementEvidence({
  manifest,
  reservation,
  redemption,
  activity,
  orderFills,
  receipt
}) {
  validateProtectedCompoundingManifest(manifest);
  const sessionId = String(manifest.session_id || "");
  const conditionId = normalizedHash(redemption?.conditionId);
  const transactionHash = normalizedHash(redemption?.transactionHash);
  const reservationConditionId = normalizedHash(reservation?.condition_id);
  if (!sessionId
      || reservation?.campaign_id !== sessionId
      || reservationConditionId !== conditionId
      || !transactionHash
      || reservation?.order_submission_intended !== true
      || reservation?.order_submitted !== true
      || !(Number(reservation?.matched_notional) > 0)
      || !reservation?.run_id
      || !reservation?.probe_id
      || !reservation?.order_id) {
    throw new Error("fail closed: automatic settlement reservation/session/order binding is invalid");
  }
  const fills = Array.isArray(orderFills) ? orderFills : [];
  if (!fills.length || fills.some((fill) =>
    typeof fill?.id !== "string"
      || !fill.id
      || !(Number(fill?.size) > 0)
      || !(Number(fill?.price) > 0 && Number(fill.price) < 1)
      || !Number.isFinite(Number(fill?.timestampMs))
      || fill?.orderRole !== "MAKER"
  ) || new Set(fills.map((fill) => fill.id)).size !== fills.length) {
    throw new Error("fail closed: automatic settlement exact authenticated maker fills are invalid");
  }
  const activityTrades = (Array.isArray(activity) ? activity : []).filter((row) =>
    String(row?.type || "").toUpperCase() === "TRADE"
      && normalizedHash(row?.conditionId) === conditionId
      && normalizedHash(row?.transactionHash)
      && String(row?.side || "BUY").toUpperCase() === "BUY"
  );
  const matchedTrades = [];
  const usedTransactionHashes = new Set();
  for (const fill of fills) {
    const candidates = activityTrades.filter((row) => {
      const hash = normalizedHash(row.transactionHash);
      const rowSize = Number(row.size);
      const rowPrincipal = Number(row.usdcSize);
      const rowTimestampMs = activityTimestampMs(row.timestamp);
      return !usedTransactionHashes.has(hash)
        && moneyEqual(rowSize, fill.size)
        && moneyEqual(rowPrincipal, Number(fill.size) * Number(fill.price))
        && Number.isFinite(rowTimestampMs)
        && Math.abs(rowTimestampMs - Number(fill.timestampMs)) <= ACTIVITY_MATCH_TOLERANCE_MS;
    });
    if (candidates.length !== 1) {
      throw new Error("fail closed: automatic settlement Data API fill binding is missing or ambiguous");
    }
    const trade = candidates[0];
    usedTransactionHashes.add(normalizedHash(trade.transactionHash));
    matchedTrades.push(trade);
  }
  const principal = money(sum(matchedTrades, "usdcSize"));
  const authenticatedPrincipal = money(fills.reduce(
    (total, fill) => total + Number(fill.size) * Number(fill.price),
    0
  ));
  const matchedRisk = money(reservation.matched_notional);
  const feeRiskUpperBound = money(Math.max(0, Number(reservation.fee_risk_upper_bound) || 0));
  if (!moneyEqual(principal, authenticatedPrincipal)
      || matchedRisk + 1 / MONEY_SCALE < principal
      || matchedRisk > principal + feeRiskUpperBound + 1 / MONEY_SCALE) {
    throw new Error("fail closed: automatic settlement principal does not reconcile to reservation risk");
  }
  const payout = money(redemption.usdcSize);
  const redemptionTimestampMs = activityTimestampMs(redemption.timestamp);
  const latestFillTimestampMs = Math.max(...fills.map((fill) => Number(fill.timestampMs)));
  const reservationCreatedMs = activityTimestampMs(reservation.created_ts);
  if (!(payout > 0)
      || !Number.isFinite(redemptionTimestampMs)
      || redemptionTimestampMs < latestFillTimestampMs
      || !Number.isFinite(reservationCreatedMs)
      || latestFillTimestampMs + ACTIVITY_MATCH_TOLERANCE_MS < reservationCreatedMs
      || receipt?.status !== "success"
      || Number(receipt?.chain_id) !== 137
      || !/^\d+$/.test(String(receipt?.block_number || ""))
      || BigInt(receipt.block_number) <= 0n
      || !Number.isInteger(Number(receipt?.confirmations))
      || Number(receipt.confirmations) < 2) {
    throw new Error("fail closed: automatic settlement redemption timing or Polygon receipt is invalid");
  }
  return {
    id: `automatic-redeem-${transactionHash.slice(2, 18)}`,
    type: AUTOMATIC_SETTLEMENT_TYPE,
    session_id: sessionId,
    campaign_id: reservation.campaign_id,
    run_id: reservation.run_id,
    probe_id: reservation.probe_id,
    order_id: reservation.order_id,
    transaction_hash: transactionHash,
    condition_id: conditionId,
    payout,
    principal,
    realized_pnl: money(payout - principal),
    fill_transaction_hashes: matchedTrades.map((row) =>
      normalizedHash(row.transactionHash)).sort(),
    authenticated_clob_fill_ids: fills.map((fill) => fill.id).sort(),
    reservation_matched_notional: matchedRisk,
    reservation_fee_risk_upper_bound: feeRiskUpperBound,
    evidence_source: "polymarket_data_api_plus_onchain_redemption",
    receipt_block_number: String(receipt.block_number),
    receipt_confirmations: Number(receipt.confirmations),
    settled_at: new Date(redemptionTimestampMs).toISOString()
  };
}

export async function reconcileProtectedCompoundingState({
  container,
  manifest,
  accountEquity,
  fullyReconciled,
  verifiedConfiguredSettlements = [],
  now = () => new Date()
}) {
  const policy = validateProtectedCompoundingManifest(manifest);
  if (!container) throw new Error("fail closed: durable storage is required for protected compounding");
  if (!fullyReconciled) {
    const current = await readState(container, policy.stateBlobName);
    if (!current) throw new Error("fail closed: protected compounding state is unavailable before full reconciliation");
    return current.value;
  }
  const ledgerSettlements = await loadDurableInternalSettlements(container, manifest.session_id);
  if (verifiedConfiguredSettlements.some((row) =>
    !policy.internalSettlements.some((configured) =>
      settlementAccountingEqual(configured, row)))) {
    throw new Error("fail closed: verified settlement is not bound to the operator session");
  }
  const settlements = uniqueSettlements([...verifiedConfiguredSettlements, ...ledgerSettlements]);
  const verifiedRealizedPnl = money(settlements.reduce((total, row) => total + Number(row.realized_pnl), 0));
  const authorizedEquityCeiling = money(Number(manifest.starting_collateral) + verifiedRealizedPnl);
  const equity = money(accountEquity);
  const tolerance = Number(manifest.max_reconciliation_discrepancy);
  if (equity > authorizedEquityCeiling + tolerance + 1e-9) {
    throw new Error("fail closed: unauthorized external deposit detected above verified trading PnL");
  }

  for (let attempt = 0; attempt < 8; attempt += 1) {
    const current = await readState(container, policy.stateBlobName);
    const prior = current?.value;
    assertCompatibleState(prior, manifest, policy);
    // The verified internal-profit ceiling is itself a fully reconciled
    // historical equity observation. Including it here prevents a fresh
    // process or session repair after a loss from lowering the 30% reserve.
    const highWater = money(Math.max(
      Number(manifest.starting_collateral),
      authorizedEquityCeiling,
      Number(prior?.high_water_equity || 0),
      equity
    ));
    const protectedReserve = money(Math.max(
      Number(prior?.protected_reserve || 0),
      highWater * policy.reserveRatio
    ));
    const operatingBuffer = money(equity * policy.operatingBufferRatio);
    const operableCapital = money(Math.max(0, equity - protectedReserve - operatingBuffer));
    const value = {
      schema: STATE_SCHEMA,
      session_id: manifest.session_id,
      reserve_ratio: policy.reserveRatio,
      operating_buffer_ratio: policy.operatingBufferRatio,
      minimum_order_notional: policy.minimumOrderNotional,
      high_water_equity: highWater,
      protected_reserve: protectedReserve,
      last_reconciled_equity: equity,
      operating_buffer: operatingBuffer,
      operable_capital: operableCapital,
      authorized_equity_ceiling: authorizedEquityCeiling,
      verified_realized_pnl: verifiedRealizedPnl,
      verified_settlement_ids: settlements.map((row) => row.id).sort(),
      reconciliation_complete: true,
      reserve_monotonic: true,
      created_at: prior?.created_at || now().toISOString(),
      updated_at: now().toISOString()
    };
    if (prior && sameCapitalState(prior, value)) return prior;
    try {
      await container.getBlockBlobClient(policy.stateBlobName).uploadData(
        Buffer.from(JSON.stringify(value, null, 2)),
        {
          conditions: current?.etag ? { ifMatch: current.etag } : { ifNoneMatch: "*" },
          blobHTTPHeaders: { blobContentType: "application/json" }
        }
      );
      return value;
    } catch (error) {
      if (![409, 412].includes(Number(error.statusCode))) throw error;
    }
  }
  throw new Error("fail closed: protected compounding state CAS retries exhausted");
}

export function protectedCapitalSnapshot({ state, accountEquity, proposedNotional = 0 }) {
  const equity = money(accountEquity);
  const highWater = money(state?.high_water_equity);
  const protectedReserve = money(state?.protected_reserve);
  const bufferRatio = Number(state?.operating_buffer_ratio);
  const minimumOrderNotional = Number(state?.minimum_order_notional);
  if (!(highWater >= 0)
      || !(protectedReserve >= 0)
      || !(bufferRatio >= 0 && bufferRatio < 1)
      || !(minimumOrderNotional > 0)) {
    throw new Error("fail closed: protected compounding state is invalid");
  }
  const operatingBuffer = money(equity * bufferRatio);
  const operableCapital = money(Math.max(0, equity - protectedReserve - operatingBuffer));
  const blockers = [];
  if (equity <= protectedReserve + minimumOrderNotional + 1e-9) {
    blockers.push("protected_reserve_order_floor_reached");
  }
  if (Number(proposedNotional) > operableCapital + 1e-9) {
    blockers.push("operable_capital_exceeded");
  }
  return {
    allow_compounding: true,
    high_water_equity: highWater,
    protected_reserve: protectedReserve,
    operating_buffer_ratio: bufferRatio,
    operating_buffer: operatingBuffer,
    operable_capital: operableCapital,
    minimum_order_notional: minimumOrderNotional,
    authorized_equity_ceiling: money(state?.authorized_equity_ceiling),
    blockers
  };
}

export function sizeProtectedOrder({
  state,
  accountEquity,
  price,
  requestedShares,
  requestedNotional,
  minimumOrderSize,
  maximumOrderNotional,
  feePerShare
}) {
  const capital = protectedCapitalSnapshot({ state, accountEquity });
  const p = Number(price);
  const sourceShares = Number(requestedShares);
  const sourceNotional = Number(requestedNotional);
  const venueMinimum = Number(minimumOrderSize);
  const maxPrincipal = Number(maximumOrderNotional);
  const fee = Number(feePerShare);
  if (!(p > 0 && p < 1)
      || !(sourceShares > 0)
      || !(sourceNotional > 0)
      || Math.abs(p * sourceShares - sourceNotional) > 1e-9
      || !(venueMinimum > 0)
      || !(maxPrincipal > 0)
      || !(fee >= 0)) {
    throw new Error("fail closed: protected order sizing input is invalid");
  }
  const affordableShares = Math.min(
    sourceShares,
    maxPrincipal / p,
    capital.operable_capital / (p + fee)
  );
  const shares = Math.floor((affordableShares + 1e-12) * SIZE_SCALE) / SIZE_SCALE;
  const notional = money(shares * p);
  const feeRiskUpperBound = money(shares * fee);
  const reservedNotional = money(notional + feeRiskUpperBound);
  const blockers = [...capital.blockers];
  if (shares + 1e-9 < venueMinimum) blockers.push("protected_order_below_venue_minimum");
  if (notional + 1e-9 < capital.minimum_order_notional) {
    blockers.push("protected_order_below_policy_minimum");
  }
  if (shares > sourceShares + 1e-9 || notional > sourceNotional + 1e-9) {
    blockers.push("protected_order_exceeds_source_intent");
  }
  if (reservedNotional > capital.operable_capital + 1e-9) {
    blockers.push("operable_capital_exceeded");
  }
  return {
    schema: "polyedge.protected_order_sizing.v1",
    executable: blockers.length === 0,
    source_shares: sourceShares,
    source_notional: sourceNotional,
    price: p,
    shares,
    notional,
    fee_risk_upper_bound: feeRiskUpperBound,
    reserved_notional: reservedNotional,
    venue_minimum_order_size: venueMinimum,
    policy_minimum_order_notional: capital.minimum_order_notional,
    operable_capital: capital.operable_capital,
    protected_reserve: capital.protected_reserve,
    blockers: [...new Set(blockers)]
  };
}

export function internalSettlementBlobName(sessionId, transactionHash, conditionId) {
  const identity = createHash("sha256")
    .update(`${normalizedHash(transactionHash)}\u0000${normalizedHash(conditionId)}`)
    .digest("hex");
  return `reports/funded/dynamic-quote/sessions/${sessionId}/internal-settlements/${identity}.json`;
}

export async function putVerifiedInternalSettlement(container, settlement) {
  if (!validDurableInternalSettlement(settlement)) {
    throw new Error("fail closed: invalid verified internal settlement record");
  }
  const name = internalSettlementBlobName(
    settlement.session_id,
    settlement.transaction_hash,
    settlement.condition_id
  );
  const value = {
    schema: "polyedge.verified_internal_settlement.v1",
    ...settlement,
    transaction_hash: normalizedHash(settlement.transaction_hash),
    condition_id: normalizedHash(settlement.condition_id),
    payout: money(settlement.payout),
    principal: money(settlement.principal),
    realized_pnl: money(settlement.realized_pnl)
  };
  const bytes = Buffer.from(JSON.stringify(value, null, 2));
  try {
    await container.getBlockBlobClient(name).uploadData(bytes, {
      conditions: { ifNoneMatch: "*" },
      blobHTTPHeaders: { blobContentType: "application/json" }
    });
  } catch (error) {
    if (![409, 412].includes(Number(error.statusCode))) throw error;
    const existing = await readBlob(container, name);
    if (JSON.stringify(existing.value) !== JSON.stringify(value)) {
      throw new Error("fail closed: immutable internal settlement record mismatch");
    }
  }
  return { blob_name: name, value };
}

export async function loadDurableInternalSettlements(container, sessionId) {
  const prefix = `reports/funded/dynamic-quote/sessions/${sessionId}/internal-settlements/`;
  const values = [];
  for await (const item of container.listBlobsFlat({ prefix })) {
    if (!item.name.endsWith(".json")) continue;
    const row = (await readBlob(container, item.name)).value;
    if (!validDurableInternalSettlement(row) || row.session_id !== sessionId) {
      throw new Error("fail closed: durable internal settlement ledger is invalid");
    }
    values.push(row);
  }
  return values;
}

async function readState(container, name) {
  try {
    return await readBlob(container, name);
  } catch (error) {
    if (Number(error.statusCode) === 404) return null;
    throw error;
  }
}

async function readBlob(container, name) {
  const client = typeof container.getBlobClient === "function"
    ? container.getBlobClient(name)
    : container.getBlockBlobClient(name);
  const response = await client.download();
  return {
    value: JSON.parse(await streamToString(response.readableStreamBody)),
    etag: response.etag || null
  };
}

function assertCompatibleState(state, manifest, policy) {
  if (!state) return;
  if (state.schema !== STATE_SCHEMA
      || state.session_id !== manifest.session_id
      || Number(state.reserve_ratio) !== policy.reserveRatio
      || Number(state.operating_buffer_ratio) !== policy.operatingBufferRatio
      || Number(state.minimum_order_notional) !== policy.minimumOrderNotional
      || state.reserve_monotonic !== true) {
    throw new Error("fail closed: persisted protected compounding state is incompatible");
  }
}

function sameCapitalState(left, right) {
  return [
    "session_id",
    "reserve_ratio",
    "operating_buffer_ratio",
    "minimum_order_notional",
    "high_water_equity",
    "protected_reserve",
    "last_reconciled_equity",
    "operating_buffer",
    "operable_capital",
    "authorized_equity_ceiling",
    "verified_realized_pnl",
    "reconciliation_complete",
    "reserve_monotonic"
  ].every((field) => JSON.stringify(left?.[field]) === JSON.stringify(right?.[field]))
    && JSON.stringify(left?.verified_settlement_ids) === JSON.stringify(right?.verified_settlement_ids);
}

function uniqueSettlements(rows) {
  const values = new Map();
  for (const row of rows) {
    if (!validInternalSettlement(row)) throw new Error("fail closed: verified internal settlement is invalid");
    const existing = values.get(row.id);
    if (existing && !settlementAccountingEqual(existing, row)) {
      throw new Error(`fail closed: conflicting internal settlement identity ${row.id}`);
    }
    if (!existing) values.set(row.id, row);
  }
  return [...values.values()];
}

function validInternalSettlement(value) {
  return value
    && typeof value.id === "string"
    && value.id.length > 0
    && [MANUAL_SETTLEMENT_TYPE, AUTOMATIC_SETTLEMENT_TYPE, RESOLVED_LOSS_TYPE].includes(value.type)
    && /^0x[0-9a-fA-F]{64}$/.test(String(value.transaction_hash || ""))
    && /^0x[0-9a-fA-F]{64}$/.test(String(value.condition_id || ""))
    && Number(value.payout) >= 0
    && Number(value.principal) >= 0
    && moneyEqual(Number(value.payout) - Number(value.principal), value.realized_pnl)
    && (value.type !== MANUAL_SETTLEMENT_TYPE
      || (Array.isArray(value.fill_transaction_hashes)
        && value.fill_transaction_hashes.length > 0
        && value.fill_transaction_hashes.every((hash) => /^0x[0-9a-fA-F]{64}$/.test(String(hash)))));
}

function validDurableInternalSettlement(value) {
  if (!validInternalSettlement(value)
      || typeof value.session_id !== "string"
      || value.session_id.length === 0) return false;
  if (value.type === RESOLVED_LOSS_TYPE) {
    return normalizedHash(value.transaction_hash) === ZERO_TRANSACTION_HASH
      && Number(value.payout) === 0
      && Number(value.realized_pnl) <= 0
      && value.evidence_source === "polymarket_data_api_resolved_zero_payout"
      && value.resolution_verified === true;
  }
  if (value.type === AUTOMATIC_SETTLEMENT_TYPE
      && (value.evidence_source !== "polymarket_data_api_plus_onchain_redemption"
        || value.campaign_id !== value.session_id
        || typeof value.run_id !== "string"
        || !value.run_id
        || typeof value.probe_id !== "string"
        || !value.probe_id
        || typeof value.order_id !== "string"
        || !value.order_id
        || !Array.isArray(value.fill_transaction_hashes)
        || value.fill_transaction_hashes.length === 0
        || value.fill_transaction_hashes.some((hash) => !normalizedHash(hash))
        || new Set(value.fill_transaction_hashes.map(normalizedHash)).size !==
          value.fill_transaction_hashes.length
        || !Array.isArray(value.authenticated_clob_fill_ids)
        || value.authenticated_clob_fill_ids.length === 0
        || value.authenticated_clob_fill_ids.some((id) => typeof id !== "string" || !id)
        || new Set(value.authenticated_clob_fill_ids).size !==
          value.authenticated_clob_fill_ids.length
        || !Number.isFinite(Number(value.reservation_matched_notional))
        || !Number.isFinite(Number(value.reservation_fee_risk_upper_bound))
        || Number(value.reservation_fee_risk_upper_bound) < 0
        || Number(value.reservation_matched_notional) + 1 / MONEY_SCALE <
          Number(value.principal)
        || Number(value.reservation_matched_notional) >
          Number(value.principal) + Number(value.reservation_fee_risk_upper_bound) +
            1 / MONEY_SCALE)) {
    return false;
  }
  return [
      "polymarket_data_api_fills_plus_polygon_receipt",
      "polymarket_data_api_plus_onchain_redemption"
    ].includes(value.evidence_source)
    && /^\d+$/.test(String(value.receipt_block_number || ""))
    && BigInt(value.receipt_block_number) > 0n
    && Number.isInteger(Number(value.receipt_confirmations))
    && Number(value.receipt_confirmations) >= 2;
}

function settlementAccountingEqual(left, right) {
  return left?.id === right?.id
    && left?.type === right?.type
    && normalizedHash(left?.transaction_hash) === normalizedHash(right?.transaction_hash)
    && normalizedHash(left?.condition_id) === normalizedHash(right?.condition_id)
    && moneyEqual(left?.payout, right?.payout)
    && moneyEqual(left?.principal, right?.principal)
    && moneyEqual(left?.realized_pnl, right?.realized_pnl);
}

function normalizedHash(value) {
  const text = String(value || "").toLowerCase();
  return /^0x[0-9a-f]{64}$/.test(text) ? text : "";
}

function activityTimestampMs(value) {
  if (typeof value === "string" && /[T:-]/.test(value)) return Date.parse(value);
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return NaN;
  return parsed < 1e12 ? parsed * 1_000 : parsed;
}

function safeBlobName(value) {
  const text = String(value || "");
  return text.length > 0 && text.length <= 512 && !text.startsWith("/") && !text.includes("..");
}

function sum(rows, field) {
  return rows.reduce((total, row) => total + Number(row?.[field] || 0), 0);
}

function moneyEqual(left, right) {
  return Math.abs(Number(left) - Number(right)) <= 1 / MONEY_SCALE + 1e-12;
}

function money(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) throw new Error("fail closed: invalid monetary value");
  return Math.round((parsed + Number.EPSILON) * MONEY_SCALE) / MONEY_SCALE;
}

async function streamToString(stream) {
  const chunks = [];
  for await (const chunk of stream) chunks.push(Buffer.from(chunk));
  return Buffer.concat(chunks).toString("utf8");
}
