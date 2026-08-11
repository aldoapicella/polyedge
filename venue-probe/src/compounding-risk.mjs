import { createHash } from "node:crypto";

const STATE_SCHEMA_V1 = "polyedge.protected_compounding_state.v1";
const STATE_SCHEMA_V2 = "polyedge.protected_compounding_state.v2";
const SESSION_SCHEMA_V2 = "polyedge.operator_funded_session.v2";
const SESSION_SCHEMA_V3 = "polyedge.operator_funded_session.v3";
const MONOTONIC_RESERVE_BASIS = "fully_reconciled_high_water_equity";
const CURRENT_EQUITY_RESERVE_BASIS = "fully_reconciled_current_equity";
const LOSS_RESIZE_POLICY = "resize_from_fully_reconciled_current_equity";
const LOSS_TOLERANT_RESERVE_RATIO = 0.1;
const LOSS_TOLERANT_MINIMUM_RESERVE = 2;
const LOSS_TOLERANT_TARGET_ORDER_RATIO = 0.05;
const MANUAL_SETTLEMENT_TYPE = "internal_manual_settlement";
const AUTOMATIC_SETTLEMENT_TYPE = "internal_automatic_settlement";
const RESOLVED_LOSS_TYPE = "internal_resolved_loss";
const ZERO_TRANSACTION_HASH = `0x${"0".repeat(64)}`;
const ZERO_ADDRESS = `0x${"0".repeat(40)}`;
const CONDITIONAL_TOKENS_ADDRESS = "0x4d97dcd97ec945f40cf65f87097ace5ea0476045";
const USDCE_ADDRESS = "0x2791bca1f2de4661ed88a30c99a7a9449aa84174";
const PUSD_ADDRESS = "0xc011a7e12a19f7b1f670d46f03b03f3342e82dfb";
const PUSD_CTF_COLLATERAL_ADAPTER_ADDRESS =
  "0xada100db00ca00073811820692005400218fce1f";
const CONFIRMED_CLOB_STATUSES = new Set(["CONFIRMED", "TRADE_STATUS_CONFIRMED"]);
const MONEY_SCALE = 1_000_000;
const SIZE_SCALE = 100;
const ACTIVITY_MATCH_TOLERANCE_MS = 120_000;

export function validateProtectedCompoundingManifest(manifest) {
  const policy = manifest?.capital_policy;
  const settlements = Array.isArray(manifest?.internal_settlements)
    ? manifest.internal_settlements
    : [];
  const errors = [];
  const currentEquityPolicy = manifest?.schema_version === SESSION_SCHEMA_V3;
  const lossTolerantPolicy = currentEquityPolicy
    && (policy?.minimum_reserve !== undefined
      || policy?.target_order_ratio !== undefined);
  if (manifest?.schema_version !== undefined
      && ![SESSION_SCHEMA_V2, SESSION_SCHEMA_V3].includes(manifest?.schema_version)) {
    errors.push("schema_version must be a protected capital session schema");
  }
  if (manifest?.allow_compounding !== true) errors.push("allow_compounding must be true");
  if (currentEquityPolicy) {
    if (manifest?.continue_after_loss !== true) {
      errors.push("continue_after_loss must be true");
    }
    if (policy?.reserve_basis !== CURRENT_EQUITY_RESERVE_BASIS) {
      errors.push(`capital_policy.reserve_basis must equal ${CURRENT_EQUITY_RESERVE_BASIS}`);
    }
    if (policy?.loss_response !== LOSS_RESIZE_POLICY) {
      errors.push(`capital_policy.loss_response must equal ${LOSS_RESIZE_POLICY}`);
    }
    if (policy?.reserve_monotonic !== false) {
      errors.push("capital_policy.reserve_monotonic must be false for current-equity loss resizing");
    }
    if (!safeBlobName(policy?.prior_state_blob_name)
        || !safeSessionId(policy?.prior_state_session_id)
        || policy?.prior_state_session_id === manifest?.session_id
        || policy?.prior_state_blob_name !==
          `reports/funded/dynamic-quote/sessions/${policy?.prior_state_session_id}/capital-reserve-state.json`) {
      errors.push("capital_policy prior state must bind a different exact funded session");
    }
    if (!(Number(policy?.minimum_historical_high_water_equity) >=
        Number(manifest?.starting_collateral))) {
      errors.push("capital_policy.minimum_historical_high_water_equity must preserve prior funded high water");
    }
  } else if (policy?.reserve_monotonic !== true) {
    errors.push("capital_policy.reserve_monotonic must be true");
  }
  if (policy?.high_water_update !== "full_reconciliation_only") {
    errors.push("capital_policy.high_water_update must equal full_reconciliation_only");
  }
  if (lossTolerantPolicy) {
    if (Number(policy?.reserve_ratio) !== LOSS_TOLERANT_RESERVE_RATIO) {
      errors.push(`capital_policy.reserve_ratio must equal ${LOSS_TOLERANT_RESERVE_RATIO}`);
    }
    if (Number(policy?.minimum_reserve) !== LOSS_TOLERANT_MINIMUM_RESERVE) {
      errors.push(`capital_policy.minimum_reserve must equal ${LOSS_TOLERANT_MINIMUM_RESERVE}`);
    }
    if (Number(policy?.target_order_ratio) !== LOSS_TOLERANT_TARGET_ORDER_RATIO) {
      errors.push(`capital_policy.target_order_ratio must equal ${LOSS_TOLERANT_TARGET_ORDER_RATIO}`);
    }
    if (!normalizedSha256(policy?.prior_state_sha256)) {
      errors.push("capital_policy.prior_state_sha256 is required for loss-tolerant sizing");
    }
  } else if (Number(policy?.reserve_ratio) !== 0.3) {
    errors.push("capital_policy.reserve_ratio must equal 0.3");
  }
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
    reserveBasis: currentEquityPolicy
      ? CURRENT_EQUITY_RESERVE_BASIS
      : MONOTONIC_RESERVE_BASIS,
    reserveMonotonic: !currentEquityPolicy,
    lossResponse: currentEquityPolicy ? LOSS_RESIZE_POLICY : null,
    stateSchema: currentEquityPolicy ? STATE_SCHEMA_V2 : STATE_SCHEMA_V1,
    priorStateBlobName: currentEquityPolicy ? String(policy.prior_state_blob_name) : null,
    priorStateSessionId: currentEquityPolicy ? String(policy.prior_state_session_id) : null,
    priorStateSha256: lossTolerantPolicy
      ? normalizedSha256(policy.prior_state_sha256)
      : null,
    minimumHistoricalHighWaterEquity: currentEquityPolicy
      ? Number(policy.minimum_historical_high_water_equity)
      : null,
    ...(lossTolerantPolicy ? {
      minimumReserve: LOSS_TOLERANT_MINIMUM_RESERVE,
      targetOrderRatio: LOSS_TOLERANT_TARGET_ORDER_RATIO
    } : {}),
    stateBlobName: String(policy.state_blob_name),
    internalSettlements: settlements
  };
}

export function validateProtectedCompoundingPredecessorState(state, policy, actualHash = null) {
  if (state?.schema !== STATE_SCHEMA_V1
      || state?.session_id !== policy?.priorStateSessionId
      || state?.reconciliation_complete !== true
      || state?.reserve_monotonic !== true
      || Number(state?.high_water_equity) + 1e-9 <
        Number(policy?.minimumHistoricalHighWaterEquity)
      || Number(state?.protected_reserve) + 0.0000011 <
        Number(state?.high_water_equity) * Number(policy?.reserveRatio)
      || (policy?.priorStateSha256
        && normalizedSha256(actualHash) !== policy.priorStateSha256)) {
    throw new Error("fail closed: prior funded high-water state is unavailable or incompatible");
  }
  return state;
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
  expectedWallet,
  getOrderFills,
  getTransactionReceipt
}) {
  validateProtectedCompoundingManifest(manifest);
  const wallet = normalizedAddress(expectedWallet);
  if (!wallet
      || typeof getOrderFills !== "function"
      || typeof getTransactionReceipt !== "function") {
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
      && normalizedAddress(row?.proxyWallet) === wallet
      && normalizedHash(row?.transactionHash)
      && normalizedHash(row?.conditionId)
      && Number(row?.usdcSize) > 0
      && activityTimestampMs(row?.timestamp) >= sessionStartedMs
  );
  const identities = new Set();
  for (const redemption of redemptions) {
    const transactionHash = normalizedHash(redemption?.transactionHash);
    const conditionId = normalizedHash(redemption?.conditionId);
    const identity = `${transactionHash}:${conditionId}`;
    if (identities.has(identity)) {
      throw new Error("fail closed: Data API redemption evidence is duplicated");
    }
    identities.add(identity);
  }
  const pending = redemptions.filter((redemption) => !durable.some((settlement) =>
    normalizedHash(settlement.transaction_hash) === normalizedHash(redemption.transactionHash)
      && normalizedHash(settlement.condition_id) === normalizedHash(redemption.conditionId)
  ));
  const receiptPromises = new Map();
  const values = await Promise.all(pending.map(async (redemption) => {
    const conditionId = normalizedHash(redemption.conditionId);
    const transactionHash = normalizedHash(redemption.transactionHash);
    const matchingReservations = (Array.isArray(reservations) ? reservations : []).filter((reservation) =>
      reservation?.campaign_id === sessionId
        && normalizedHash(reservation?.condition_id) === conditionId
        && normalizedAsset(reservation?.token_id)
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
    if (!matchingReservations.length) {
      throw new Error("fail closed: automatic settlement redemption does not bind funded reservations");
    }
    const tokenIds = [...new Set(matchingReservations.map((row) =>
      normalizedAsset(row.token_id)))];
    if (tokenIds.length !== 1) {
      throw new Error("fail closed: automatic settlement reservations do not bind one exact asset");
    }
    if (!redemptionAssetMatches(redemption?.asset, tokenIds[0])) {
      throw new Error("fail closed: Data API redemption asset does not bind the reservation token");
    }
    const fillGroups = await Promise.all(matchingReservations.map((reservation) =>
      getOrderFills(reservation)));
    if (!receiptPromises.has(transactionHash)) {
      receiptPromises.set(transactionHash, getTransactionReceipt(transactionHash));
    }
    const receipt = await receiptPromises.get(transactionHash);
    return verifyAutomaticSettlementEvidence({
      manifest,
      reservations: matchingReservations,
      redemption,
      activity: rows,
      orderFills: fillGroups.flat(),
      receipt,
      expectedWallet: wallet
    });
  }));
  return values;
}

export function verifyAutomaticSettlementEvidence({
  manifest,
  reservations,
  redemption,
  activity,
  orderFills,
  receipt,
  expectedWallet
}) {
  validateProtectedCompoundingManifest(manifest);
  const sessionId = String(manifest.session_id || "");
  const conditionId = normalizedHash(redemption?.conditionId);
  const transactionHash = normalizedHash(redemption?.transactionHash);
  const wallet = normalizedAddress(expectedWallet);
  const boundReservations = Array.isArray(reservations) ? reservations : [];
  if (!sessionId
      || !transactionHash
      || !conditionId
      || !wallet
      || normalizedAddress(redemption?.proxyWallet) !== wallet
      || String(redemption?.type || "").toUpperCase() !== "REDEEM"
      || !(Number(redemption?.usdcSize) > 0)
      || !boundReservations.length
      || boundReservations.some((reservation) =>
        reservation?.campaign_id !== sessionId
          || normalizedHash(reservation?.condition_id) !== conditionId
          || !normalizedAsset(reservation?.token_id)
          || reservation?.order_submission_intended !== true
          || reservation?.order_submitted !== true
          || !(Number(reservation?.matched_notional) > 0)
          || !reservation?.run_id
          || !reservation?.probe_id
          || !reservation?.order_id
          || !Number.isFinite(activityTimestampMs(reservation?.created_ts)))
      || new Set(boundReservations.map((row) => row.order_id)).size !==
        boundReservations.length) {
    throw new Error("fail closed: automatic settlement reservation/session/order/redemption binding is invalid");
  }
  const tokenIds = [...new Set(boundReservations.map((row) =>
    normalizedAsset(row.token_id)))];
  if (tokenIds.length !== 1) {
    throw new Error("fail closed: automatic settlement reservations do not bind one exact asset");
  }
  const tokenId = tokenIds[0];
  if (!redemptionAssetMatches(redemption?.asset, tokenId)) {
    throw new Error("fail closed: Data API redemption asset does not bind the reservation token");
  }
  const reservationByOrder = new Map(boundReservations.map((row) => [row.order_id, row]));
  const fills = (Array.isArray(orderFills) ? orderFills : []).map((fill) => ({
    ...fill,
    trade_id: String(fill?.id || ""),
    id: `${String(fill?.id || "")}:${String(fill?.orderId || "")}`,
    transaction_hash: normalizedHash(fill?.transactionHash),
    asset_id: normalizedAsset(fill?.assetId),
    condition_id: normalizedHash(fill?.market),
    order_id: String(fill?.orderId || ""),
    maker_order_id: String(fill?.makerOrderId || ""),
    size: Number(fill?.size),
    price: Number(fill?.price),
    timestamp_ms: Number(fill?.timestampMs)
  }));
  if (fills.some((fill) => {
    const nestedMakerBound = fill.nestedMakerOrderMatchCount === 1 &&
      normalizedAsset(fill?.makerAssetId) === tokenId;
    const directMakerBound = fill.nestedMakerOrderMatchCount === 0 &&
      fill.directMakerOrder === true &&
      normalizedAsset(fill?.tradeAssetId) === tokenId;
    return !fill.trade_id
        || !fill.transaction_hash
        || fill.asset_id !== tokenId
        || !normalizedAsset(fill?.tradeAssetId)
        // For nested maker evidence, CLOB's top-level aggregate/taker asset can
        // be the complementary outcome. Flat maker evidence stays exact-token bound.
        || (!nestedMakerBound && !directMakerBound)
        || fill.condition_id !== conditionId
        || !CONFIRMED_CLOB_STATUSES.has(String(fill?.status || "").toUpperCase())
        || fill.orderRole !== "MAKER"
        || fill.order_id !== fill.maker_order_id
        || !reservationByOrder.has(fill.order_id)
        || String(fill?.orderSide || "").toUpperCase() !== "BUY"
        || !normalizedAddress(fill?.makerAddress)
        || normalizedAddress(fill.makerAddress) !== wallet
        || typeof fill?.owner !== "string"
        || !fill.owner
        || !(fill.size > 0)
        || !(fill.price > 0 && fill.price < 1)
        || !Number.isFinite(fill.timestamp_ms);
  })) {
    throw new Error("fail closed: authenticated CLOB trade hash/asset/market/maker/status binding is invalid");
  }
  if (!fills.length
      || new Set(fills.map((fill) => fill.id)).size !== fills.length
      || [...reservationByOrder.keys()].some((orderId) =>
        !fills.some((fill) => fill.order_id === orderId))) {
    throw new Error("fail closed: authenticated CLOB maker fills do not cover every reservation order");
  }
  const clobGroups = groupClobFills(fills);
  const matchedActivity = [];
  for (const group of clobGroups) {
    const candidates = (Array.isArray(activity) ? activity : []).filter((row) =>
      String(row?.type || "").toUpperCase() === "TRADE"
        && String(row?.side || "").toUpperCase() === "BUY"
        && normalizedAddress(row?.proxyWallet) === wallet
        && normalizedAsset(row?.asset) === tokenId
        && normalizedHash(row?.transactionHash) === group.transaction_hash
        && normalizedHash(row?.conditionId) === conditionId
        && Number.isFinite(activityTimestampMs(row?.timestamp))
        && Math.abs(activityTimestampMs(row.timestamp) - group.timestamp_ms) <=
          ACTIVITY_MATCH_TOLERANCE_MS
    );
    if (!candidates.length
        || !moneyEqual(sum(candidates, "size"), group.size)
        || !moneyEqual(sum(candidates, "usdcSize"), group.principal)) {
      throw new Error("fail closed: exact Data API proxy wallet/asset/transaction/fill binding is missing or ambiguous");
    }
    matchedActivity.push(...candidates);
  }
  const principal = money(sum(matchedActivity, "usdcSize"));
  const authenticatedPrincipal = money(fills.reduce(
    (total, fill) => total + Number(fill.size) * Number(fill.price),
    0
  ));
  const matchedRisk = money(boundReservations.reduce(
    (total, row) => total + Number(row.matched_notional),
    0
  ));
  const feeRiskUpperBound = money(boundReservations.reduce(
    (total, row) => total + Math.max(0, Number(row.fee_risk_upper_bound) || 0),
    0
  ));
  if (!moneyEqual(principal, authenticatedPrincipal)
      || matchedRisk + 1 / MONEY_SCALE < principal
      || matchedRisk > principal + feeRiskUpperBound + 1 / MONEY_SCALE) {
    throw new Error("fail closed: automatic settlement principal does not reconcile to reservation risk");
  }
  const payout = money(redemption.usdcSize);
  const payoutBaseUnits = String(Math.round(payout * MONEY_SCALE));
  const decodedRedemptions = Array.isArray(receipt?.redemptions)
    ? receipt.redemptions
    : [];
  const decodedMatches = decodedRedemptions.filter((row) =>
    normalizedAddress(row?.contract_address) === CONDITIONAL_TOKENS_ADDRESS
      && normalizedHash(row?.transaction_hash) === transactionHash
      && normalizedAddress(row?.redeemer) === PUSD_CTF_COLLATERAL_ADAPTER_ADDRESS
      && normalizedAddress(row?.collateral_token) === USDCE_ADDRESS
      && normalizedHash(row?.parent_collection_id) === ZERO_TRANSACTION_HASH
      && normalizedHash(row?.condition_id) === conditionId
      && moneyEqual(row?.payout, payout)
      && /^\d+$/.test(String(row?.payout_base_units || ""))
      && String(row.payout_base_units) === payoutBaseUnits
      && Array.isArray(row?.index_sets)
      && row.index_sets.length > 0
      && row.index_sets.every((index) => normalizedIndexSet(index))
  );
  if (receipt?.status !== "success"
      || Number(receipt?.chain_id) !== 137
      || normalizedHash(receipt?.transaction_hash) !== transactionHash
      || !/^\d+$/.test(String(receipt?.block_number || ""))
      || BigInt(receipt.block_number) <= 0n
      || !Number.isInteger(Number(receipt?.confirmations))
      || Number(receipt.confirmations) < 2
      || decodedMatches.length !== 1) {
    throw new Error("fail closed: decoded confirmed redemption wallet/condition/payout/transaction evidence is invalid");
  }
  const decodedRedemption = decodedMatches[0];
  const adapter = normalizedAddress(decodedRedemption.redeemer);
  const ctfTransfers = Array.isArray(receipt?.ctf_transfers) ? receipt.ctf_transfers : [];
  const erc20Transfers = Array.isArray(receipt?.erc20_transfers) ? receipt.erc20_transfers : [];
  const collateralWraps = Array.isArray(receipt?.collateral_wraps)
    ? receipt.collateral_wraps
    : [];
  const walletToAdapter = ctfTransfers.filter((row) =>
    row?.event === "TransferBatch"
      && normalizedAddress(row?.contract_address) === CONDITIONAL_TOKENS_ADDRESS
      && normalizedAddress(row?.operator) === adapter
      && normalizedAddress(row?.from) === wallet
      && normalizedAddress(row?.to) === adapter
      && transferContainsExact(row, tokenId, payoutBaseUnits)
  );
  const adapterBurn = ctfTransfers.filter((row) =>
    row?.event === "TransferSingle"
      && normalizedAddress(row?.contract_address) === CONDITIONAL_TOKENS_ADDRESS
      && normalizedAddress(row?.operator) === adapter
      && normalizedAddress(row?.from) === adapter
      && normalizedAddress(row?.to) === ZERO_ADDRESS
      && transferContainsOnly(row, tokenId, payoutBaseUnits)
  );
  const usdceCtfToAdapter = erc20Transfers.filter((row) =>
    normalizedAddress(row?.token) === USDCE_ADDRESS
      && normalizedAddress(row?.from) === CONDITIONAL_TOKENS_ADDRESS
      && normalizedAddress(row?.to) === adapter
      && canonicalBaseUnits(row?.value_base_units) === payoutBaseUnits
  );
  const usdceAdapterToPusd = erc20Transfers.filter((row) =>
    normalizedAddress(row?.token) === USDCE_ADDRESS
      && normalizedAddress(row?.from) === adapter
      && normalizedAddress(row?.to) === PUSD_ADDRESS
      && canonicalBaseUnits(row?.value_base_units) === payoutBaseUnits
  );
  const pusdMintToWallet = erc20Transfers.filter((row) =>
    normalizedAddress(row?.token) === PUSD_ADDRESS
      && normalizedAddress(row?.from) === ZERO_ADDRESS
      && normalizedAddress(row?.to) === wallet
      && canonicalBaseUnits(row?.value_base_units) === payoutBaseUnits
  );
  const pusdWrapToWallet = collateralWraps.filter((row) =>
    normalizedAddress(row?.contract_address) === PUSD_ADDRESS
      && normalizedAddress(row?.caller) === adapter
      && normalizedAddress(row?.asset) === USDCE_ADDRESS
      && normalizedAddress(row?.to) === wallet
      && canonicalBaseUnits(row?.amount_base_units) === payoutBaseUnits
  );
  if (walletToAdapter.length !== 1
      || adapterBurn.length !== 1
      || usdceCtfToAdapter.length !== 1
      || usdceAdapterToPusd.length !== 1
      || pusdMintToWallet.length !== 1
      || pusdWrapToWallet.length !== 1) {
    throw new Error("fail closed: decoded redemption adapter/CTF/USDC.e/pUSD transfer chain is invalid");
  }
  const filledShares = money(fills.reduce(
    (total, fill) => total + Number(fill.size),
    0
  ));
  const redemptionTimestampMs = activityTimestampMs(redemption.timestamp);
  const latestFillTimestampMs = Math.max(...fills.map((fill) => fill.timestamp_ms));
  const earliestReservationMs = Math.min(...boundReservations.map((row) =>
    activityTimestampMs(row.created_ts)));
  if (!(payout > 0) || !moneyEqual(payout, filledShares)) {
    throw new Error("fail closed: automatic settlement payout does not reconcile to exact filled shares");
  }
  if (!Number.isFinite(redemptionTimestampMs)
      || redemptionTimestampMs < latestFillTimestampMs
      || !Number.isFinite(earliestReservationMs)
      || latestFillTimestampMs + ACTIVITY_MATCH_TOLERANCE_MS < earliestReservationMs) {
    throw new Error("fail closed: automatic settlement redemption timing is invalid");
  }
  return {
    id: automaticSettlementId(transactionHash, conditionId),
    type: AUTOMATIC_SETTLEMENT_TYPE,
    session_id: sessionId,
    campaign_id: sessionId,
    run_ids: boundReservations.map((row) => row.run_id).sort(),
    probe_ids: boundReservations.map((row) => row.probe_id).sort(),
    order_ids: boundReservations.map((row) => row.order_id).sort(),
    token_ids: [tokenId],
    proxy_wallet: wallet,
    transaction_hash: transactionHash,
    condition_id: conditionId,
    payout,
    principal,
    realized_pnl: money(payout - principal),
    fill_transaction_hashes: [...new Set(fills.map((fill) =>
      fill.transaction_hash))].sort(),
    authenticated_clob_fill_ids: fills.map((fill) => fill.id).sort(),
    reservation_matched_notional: matchedRisk,
    reservation_fee_risk_upper_bound: feeRiskUpperBound,
    evidence_source: "polymarket_data_api_plus_onchain_redemption",
    redemption_evidence_decoded: true,
    redemption_adapter_address: adapter,
    redemption_contract_address: normalizedAddress(decodedRedemption.contract_address),
    redemption_collateral_token: normalizedAddress(decodedRedemption.collateral_token),
    redemption_parent_collection_id: normalizedHash(decodedRedemption.parent_collection_id),
    redemption_index_sets: decodedRedemption.index_sets.map(normalizedIndexSet),
    redemption_payout_base_units: payoutBaseUnits,
    redemption_transfer_chain_verified: true,
    redemption_token_id: tokenId,
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
    assertCompatibleState(current.value, manifest, policy);
    return current.value;
  }
  const [ledgerSettlements, priorHistoricalDocument, initialCurrent] = await Promise.all([
    loadDurableInternalSettlements(container, manifest.session_id),
    policy.reserveMonotonic
      ? Promise.resolve(null)
      : readState(container, policy.priorStateBlobName),
    readState(container, policy.stateBlobName)
  ]);
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
  let priorHistoricalState = null;
  if (!policy.reserveMonotonic) {
    priorHistoricalState = priorHistoricalDocument?.value;
    validateProtectedCompoundingPredecessorState(
      priorHistoricalState,
      policy,
      priorHistoricalDocument?.hash
    );
  }

  for (let attempt = 0; attempt < 8; attempt += 1) {
    const current = attempt === 0
      ? initialCurrent
      : await readState(container, policy.stateBlobName);
    const prior = current?.value;
    assertCompatibleState(prior, manifest, policy);
    // High water remains monotonic audit evidence in both policy versions.
    // The v3 reserve deliberately follows fully reconciled current equity so
    // a loss resizes the next order instead of permanently stopping trading.
    const highWater = money(Math.max(
      Number(manifest.starting_collateral),
      authorizedEquityCeiling,
      Number(priorHistoricalState?.high_water_equity || 0),
      Number(prior?.high_water_equity || 0),
      equity
    ));
    const protectedReserve = policy.reserveMonotonic
      ? money(Math.max(Number(prior?.protected_reserve || 0), highWater * policy.reserveRatio))
      : money(Math.max(Number(policy.minimumReserve || 0), equity * policy.reserveRatio));
    const operatingBuffer = money(equity * policy.operatingBufferRatio);
    const operableCapital = money(Math.max(0, equity - protectedReserve - operatingBuffer));
    const value = {
      schema: policy.stateSchema,
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
      historical_high_water_equity: highWater,
      prior_state_session_id: policy.priorStateSessionId,
      prior_state_blob_name: policy.priorStateBlobName,
      ...(policy.priorStateSha256 ? {
        prior_state_sha256: policy.priorStateSha256
      } : {}),
      reserve_basis: policy.reserveBasis,
      loss_response: policy.lossResponse,
      continue_after_loss: !policy.reserveMonotonic,
      reserve_monotonic: policy.reserveMonotonic,
      ...(policy.targetOrderRatio === undefined ? {} : {
        minimum_reserve: policy.minimumReserve,
        target_order_ratio: policy.targetOrderRatio
      }),
      ...migrationCheckpoint(prior),
      created_at: prior?.created_at || now().toISOString(),
      updated_at: now().toISOString()
    };
    assertCompatibleState(value, manifest, policy);
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

export async function migrateProtectedReserveState({
  container,
  manifest,
  source,
  accountEquity,
  fullyReconciled,
  openOrderCount,
  positionCount,
  unresolvedReservationCount,
  now = () => new Date()
}) {
  const policy = validateProtectedCompoundingManifest(manifest);
  const sourceHash = normalizedSha256(source?.sessionHash);
  const minimumHistoricalHighWaterEquity = Number(
    source?.minimumHistoricalHighWaterEquity
  );
  if (!container || manifest?.schema_version !== SESSION_SCHEMA_V2 || !policy.reserveMonotonic) {
    throw new Error("fail closed: protected reserve migration requires the monotonic target session");
  }
  if (!safeSessionId(source?.sessionId)
      || !safeBlobName(source?.sessionBlobName)
      || !safeBlobName(source?.stateBlobName)
      || !sourceHash
      || !Number.isFinite(minimumHistoricalHighWaterEquity)
      || minimumHistoricalHighWaterEquity < Number(manifest.starting_collateral)
      || source.sessionId === manifest.session_id
      || source.sessionBlobName !==
        `reports/funded/dynamic-quote/sessions/${source.sessionId}/session.json`
      || source.stateBlobName !==
        `reports/funded/dynamic-quote/sessions/${source.sessionId}/capital-reserve-state.json`) {
    throw new Error("fail closed: protected reserve migration source binding is invalid");
  }
  const sourceSessionDocument = await readBlob(container, source.sessionBlobName);
  if (sourceSessionDocument.hash !== sourceHash) {
    throw new Error("fail closed: protected reserve migration source session SHA-256 mismatch");
  }
  const sourceManifest = sourceSessionDocument.value;
  const sourcePolicy = validateProtectedCompoundingManifest(sourceManifest);
  if (sourceManifest?.schema_version !== SESSION_SCHEMA_V3
      || sourceManifest?.session_id !== source.sessionId
      || sourcePolicy.reserveMonotonic
      || sourcePolicy.stateBlobName !== source.stateBlobName
      || sourcePolicy.priorStateSessionId !== manifest.session_id
      || sourcePolicy.priorStateBlobName !== policy.stateBlobName) {
    throw new Error("fail closed: protected reserve migration source session is incompatible");
  }

  const [sourceStateDocument, initialTarget, sourceLedger, targetLedger] = await Promise.all([
    readState(container, source.stateBlobName),
    readState(container, policy.stateBlobName),
    loadDurableInternalSettlements(container, source.sessionId),
    loadDurableInternalSettlements(container, manifest.session_id)
  ]);
  if (!sourceStateDocument?.etag) {
    throw new Error("fail closed: protected reserve migration source state or ETag is unavailable");
  }
  assertMigrationTargetState(initialTarget?.value, manifest, policy, source, sourceHash);
  if (initialTarget && !initialTarget.etag) {
    throw new Error("fail closed: protected reserve migration target ETag is unavailable");
  }
  const uniqueSourceLedger = uniqueSettlements(sourceLedger);
  const uniqueTargetLedger = uniqueSettlements(targetLedger);
  if (uniqueSourceLedger.length !== sourceLedger.length
      || uniqueTargetLedger.length !== targetLedger.length) {
    throw new Error("fail closed: protected reserve migration ledger identities are duplicated");
  }
  for (const configured of sourcePolicy.internalSettlements) {
    if (!uniqueSourceLedger.some((row) => settlementAccountingEqual(row, configured))) {
      throw new Error("fail closed: protected reserve migration source configured settlement is not durable");
    }
  }
  for (const configured of policy.internalSettlements) {
    if (!uniqueTargetLedger.some((row) => settlementAccountingEqual(row, configured))) {
      throw new Error("fail closed: protected reserve migration target configured settlement is not durable");
    }
  }
  const uniqueSource = uniqueSettlements([
    ...sourcePolicy.internalSettlements,
    ...uniqueSourceLedger
  ]);
  const uniqueTarget = uniqueSettlements([
    ...policy.internalSettlements,
    ...uniqueTargetLedger
  ]);
  const sourceSettlementIds = uniqueSource.map((row) => row.id).sort();
  const sourceRealizedPnl = money(uniqueSource.reduce(
    (total, row) => total + Number(row.realized_pnl),
    0
  ));
  const sourceCeiling = money(Number(sourceManifest.starting_collateral) + sourceRealizedPnl);
  assertMigrationSourceState(
    sourceStateDocument.value,
    sourceManifest,
    sourcePolicy,
    sourceSettlementIds,
    sourceRealizedPnl,
    sourceCeiling,
    minimumHistoricalHighWaterEquity
  );

  const stableSourceState = await readState(container, source.stateBlobName);
  if (stableSourceState?.etag !== sourceStateDocument.etag
      || stableSourceState?.hash !== sourceStateDocument.hash) {
    throw new Error("fail closed: protected reserve migration source state changed during verification");
  }

  const checkpoint = initialTarget?.value;
  if (checkpoint?.migration_source_session_id !== undefined) {
    const checkpointIds = checkpoint.migration_verified_settlement_ids;
    const checkpointSettlements = checkpointIds.map((id) =>
      uniqueTargetLedger.find((row) => row.id === id));
    const targetSettlementIds = uniqueTargetLedger.map((row) => row.id).sort();
    const targetRealizedPnl = money(uniqueTargetLedger.reduce(
      (total, row) => total + Number(row.realized_pnl),
      0
    ));
    const targetCeiling = money(
      Number(manifest.starting_collateral) + targetRealizedPnl
    );
    const currentHighWater = Number(checkpoint.high_water_equity);
    const historicalHighWater = Number(checkpoint.historical_high_water_equity);
    const lastEquity = Number(checkpoint.last_reconciled_equity);
    const protectedReserve = Number(checkpoint.protected_reserve);
    const operatingBuffer = money(lastEquity * policy.operatingBufferRatio);
    const requiredSettlements = uniqueSettlements([
      ...policy.internalSettlements,
      ...uniqueSource
    ]);
    if (checkpoint.migration_source_state_etag !== sourceStateDocument.etag
        || Number(checkpoint.migration_minimum_historical_high_water_equity) !==
          minimumHistoricalHighWaterEquity
        || checkpointSettlements.some((row) => !row)
        || requiredSettlements.some((required) =>
          !checkpointIds.includes(required.id)
          || !checkpointSettlements.some((row) =>
            settlementAccountingEqual(row, required)))
        || JSON.stringify(checkpoint.verified_settlement_ids) !==
          JSON.stringify(targetSettlementIds)
        || !moneyEqual(checkpoint.verified_realized_pnl, targetRealizedPnl)
        || !moneyEqual(checkpoint.authorized_equity_ceiling, targetCeiling)
        || !Number.isFinite(lastEquity)
        || lastEquity < 0
        || lastEquity > targetCeiling + Number(manifest.max_reconciliation_discrepancy) + 1e-9
        || currentHighWater + 1e-9 < Math.max(
          Number(manifest.starting_collateral),
          targetCeiling,
          Number(sourceStateDocument.value.high_water_equity),
          Number(sourceStateDocument.value.historical_high_water_equity),
          lastEquity
        )
        || historicalHighWater + 1e-9 < currentHighWater
        || !moneyEqual(checkpoint.operating_buffer, operatingBuffer)
        || !moneyEqual(
          checkpoint.operable_capital,
          Math.max(0, lastEquity - protectedReserve - operatingBuffer)
        )) {
      throw new Error("fail closed: persisted protected reserve migration checkpoint is invalid");
    }
    const checkpointCeiling = money(Number(manifest.starting_collateral) +
      checkpointSettlements.reduce((total, row) =>
        total + Number(row.realized_pnl), 0));
    if (!moneyEqual(checkpoint.migration_verified_equity_ceiling, checkpointCeiling)) {
      throw new Error("fail closed: persisted protected reserve migration checkpoint is invalid");
    }
    return migrationResult(
      checkpoint,
      sourceStateDocument,
      sourceHash,
      checkpointCeiling
    );
  }

  if (fullyReconciled !== true
      || Number(openOrderCount) !== 0
      || Number(positionCount) !== 0
      || Number(unresolvedReservationCount) !== 0) {
    throw new Error("fail closed: protected reserve migration requires zero orders, positions, and unresolved reservations");
  }
  const equity = money(accountEquity);
  if (equity < 0) throw new Error("fail closed: protected reserve migration equity is invalid");

  const settlements = uniqueSettlements([...uniqueTarget, ...uniqueSource]);
  const settlementIds = settlements.map((row) => row.id).sort();
  const verifiedRealizedPnl = money(settlements.reduce(
    (total, row) => total + Number(row.realized_pnl),
    0
  ));
  const authorizedEquityCeiling = money(
    Number(manifest.starting_collateral) + verifiedRealizedPnl
  );
  if (equity > authorizedEquityCeiling + Number(manifest.max_reconciliation_discrepancy) + 1e-9) {
    throw new Error("fail closed: protected reserve migration equity exceeds the verified ceiling");
  }
  for (const settlement of uniqueSource) {
    if (uniqueTarget.some((row) => row.id === settlement.id)) continue;
    await putVerifiedInternalSettlement(container, {
      ...settlement,
      session_id: manifest.session_id,
      ...(settlement.campaign_id ? { campaign_id: manifest.session_id } : {}),
      migration_source_session_id: source.sessionId,
      migration_source_session_blob_name: source.sessionBlobName,
      migration_source_session_sha256: sourceHash,
      migration_source_settlement_blob_name: internalSettlementBlobName(
        source.sessionId,
        settlement.transaction_hash,
        settlement.condition_id
      ),
      migration_source_state_blob_name: source.stateBlobName,
      migration_source_state_etag: sourceStateDocument.etag
    });
  }
  const migratedLedger = await loadDurableInternalSettlements(container, manifest.session_id);
  const migratedSettlements = uniqueSettlements(migratedLedger);
  if (migratedSettlements.length !== migratedLedger.length
      || JSON.stringify(migratedSettlements.map((row) => row.id).sort()) !==
        JSON.stringify(settlementIds)
      || settlements.some((row) => !migratedSettlements.some((migrated) =>
        settlementAccountingEqual(row, migrated)))) {
    throw new Error("fail closed: protected reserve migration ledger read-back mismatch");
  }

  for (let attempt = 0; attempt < 8; attempt += 1) {
    const current = attempt === 0 ? initialTarget : await readState(container, policy.stateBlobName);
    const prior = current?.value;
    assertMigrationTargetState(prior, manifest, policy, source, sourceHash);
    if (prior && !current.etag) {
      throw new Error("fail closed: protected reserve migration target ETag is unavailable");
    }
    const highWater = money(Math.max(
      Number(manifest.starting_collateral),
      authorizedEquityCeiling,
      equity,
      Number(sourceStateDocument.value.high_water_equity),
      Number(sourceStateDocument.value.historical_high_water_equity || 0),
      Number(prior?.high_water_equity || 0),
      Number(prior?.historical_high_water_equity || 0)
    ));
    const protectedReserve = money(Math.max(
      highWater * policy.reserveRatio,
      Number(sourceStateDocument.value.protected_reserve),
      Number(prior?.protected_reserve || 0)
    ));
    const operatingBuffer = money(equity * policy.operatingBufferRatio);
    const checkpointMatches = prior
      && prior.migration_source_session_id === source.sessionId
      && prior.migration_source_session_blob_name === source.sessionBlobName
      && prior.migration_source_session_sha256 === sourceHash
      && prior.migration_source_state_blob_name === source.stateBlobName
      && prior.migration_source_state_etag === sourceStateDocument.etag
      && Number(prior.migration_minimum_historical_high_water_equity) ===
        minimumHistoricalHighWaterEquity
      && Number(prior.migration_verified_equity_ceiling) === authorizedEquityCeiling
      && JSON.stringify(prior.migration_verified_settlement_ids) === JSON.stringify(settlementIds);
    const value = {
      schema: policy.stateSchema,
      session_id: manifest.session_id,
      reserve_ratio: policy.reserveRatio,
      operating_buffer_ratio: policy.operatingBufferRatio,
      minimum_order_notional: policy.minimumOrderNotional,
      high_water_equity: highWater,
      protected_reserve: protectedReserve,
      last_reconciled_equity: equity,
      operating_buffer: operatingBuffer,
      operable_capital: money(Math.max(0, equity - protectedReserve - operatingBuffer)),
      authorized_equity_ceiling: authorizedEquityCeiling,
      verified_realized_pnl: verifiedRealizedPnl,
      verified_settlement_ids: settlementIds,
      reconciliation_complete: true,
      historical_high_water_equity: highWater,
      prior_state_session_id: null,
      prior_state_blob_name: null,
      reserve_basis: policy.reserveBasis,
      loss_response: null,
      continue_after_loss: false,
      reserve_monotonic: true,
      migration_source_session_id: source.sessionId,
      migration_source_session_blob_name: source.sessionBlobName,
      migration_source_session_sha256: sourceHash,
      migration_source_state_blob_name: source.stateBlobName,
      migration_source_state_etag: sourceStateDocument.etag,
      migration_minimum_historical_high_water_equity:
        minimumHistoricalHighWaterEquity,
      migration_verified_equity_ceiling: authorizedEquityCeiling,
      migration_verified_settlement_ids: settlementIds,
      migration_completed_at: checkpointMatches
        ? prior.migration_completed_at
        : now().toISOString(),
      created_at: prior?.created_at || now().toISOString(),
      updated_at: now().toISOString()
    };
    if (prior && checkpointMatches && sameCapitalState(prior, value)) {
      return migrationResult(prior, sourceStateDocument, sourceHash, authorizedEquityCeiling);
    }
    try {
      await container.getBlockBlobClient(policy.stateBlobName).uploadData(
        Buffer.from(JSON.stringify(value, null, 2)),
        {
          conditions: current ? { ifMatch: current.etag } : { ifNoneMatch: "*" },
          blobHTTPHeaders: { blobContentType: "application/json" }
        }
      );
      const verified = await readState(container, policy.stateBlobName);
      assertCompatibleState(verified?.value, manifest, policy);
      if (!verified?.etag
          || !sameCapitalState(verified.value, value)
          || verified.value.migration_source_state_etag !== sourceStateDocument.etag
          || JSON.stringify(verified.value.migration_verified_settlement_ids) !==
            JSON.stringify(settlementIds)) {
        throw new Error("fail closed: protected reserve migration state read-back mismatch");
      }
      return migrationResult(
        verified.value,
        sourceStateDocument,
        sourceHash,
        authorizedEquityCeiling
      );
    } catch (error) {
      if (![409, 412].includes(Number(error.statusCode))) throw error;
    }
  }
  throw new Error("fail closed: protected reserve migration state CAS retries exhausted");
}

export function protectedCapitalSnapshot({ state, accountEquity, proposedNotional = 0 }) {
  const equity = money(accountEquity);
  const highWater = money(state?.high_water_equity);
  const protectedReserve = money(state?.protected_reserve);
  const bufferRatio = Number(state?.operating_buffer_ratio);
  const minimumOrderNotional = Number(state?.minimum_order_notional);
  const minimumReserve = Number(state?.minimum_reserve ?? 0);
  const targetOrderRatio = state?.target_order_ratio === undefined
    ? null
    : Number(state.target_order_ratio);
  if (!(highWater >= 0)
      || !(protectedReserve >= 0)
      || !(bufferRatio >= 0 && bufferRatio < 1)
      || !(minimumOrderNotional > 0)
      || !(minimumReserve >= 0)
      || !(targetOrderRatio === null || (targetOrderRatio > 0 && targetOrderRatio < 1))) {
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
    minimum_reserve: minimumReserve,
    target_order_ratio: targetOrderRatio,
    authorized_equity_ceiling: money(state?.authorized_equity_ceiling),
    historical_high_water_equity: state?.historical_high_water_equity === undefined
      ? highWater
      : money(state.historical_high_water_equity),
    prior_state_session_id: state?.prior_state_session_id || null,
    prior_state_blob_name: state?.prior_state_blob_name || null,
    last_reconciled_equity: state?.last_reconciled_equity === undefined
      ? null
      : money(state.last_reconciled_equity),
    reserve_basis: state?.reserve_basis || MONOTONIC_RESERVE_BASIS,
    continue_after_loss: state?.continue_after_loss === true,
    reserve_monotonic: state?.reserve_monotonic !== false,
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
  const policyMinimumShares = Math.ceil(
    ((capital.minimum_order_notional / p) - 1e-12) * SIZE_SCALE
  ) / SIZE_SCALE;
  const minimumExecutableShares = Math.max(
    Math.ceil((venueMinimum - 1e-12) * SIZE_SCALE) / SIZE_SCALE,
    policyMinimumShares
  );
  const orderRiskBudget = capital.target_order_ratio === null
    ? Number.POSITIVE_INFINITY
    : Math.ceil((Math.max(
        minimumExecutableShares * (p + fee),
        Number(accountEquity) * capital.target_order_ratio
      ) - 1e-12) * MONEY_SCALE) / MONEY_SCALE;
  const affordableShares = Math.min(
    sourceShares,
    maxPrincipal / p,
    capital.operable_capital / (p + fee),
    orderRiskBudget / (p + fee)
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
    minimum_executable_shares: minimumExecutableShares,
    target_order_ratio: capital.target_order_ratio,
    order_risk_budget: Number.isFinite(orderRiskBudget) ? orderRiskBudget : null,
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
  const bytes = Buffer.from(await streamToString(response.readableStreamBody));
  return {
    value: JSON.parse(bytes.toString("utf8")),
    etag: response.etag || null,
    hash: `sha256:${createHash("sha256").update(bytes).digest("hex")}`
  };
}

function assertMigrationSourceState(
  state,
  manifest,
  policy,
  settlementIds,
  verifiedRealizedPnl,
  authorizedEquityCeiling,
  minimumHistoricalHighWaterEquity
) {
  const highWater = Number(state?.high_water_equity);
  const historicalHighWater = Number(state?.historical_high_water_equity);
  const lastEquity = Number(state?.last_reconciled_equity);
  const protectedReserve = Number(state?.protected_reserve);
  const operatingBuffer = Number(state?.operating_buffer);
  const operableCapital = Number(state?.operable_capital);
  if (state?.schema !== STATE_SCHEMA_V2
      || state?.session_id !== manifest.session_id
      || state?.reconciliation_complete !== true
      || Number(state?.reserve_ratio) !== policy.reserveRatio
      || Number(state?.operating_buffer_ratio) !== policy.operatingBufferRatio
      || Number(state?.minimum_order_notional) !== policy.minimumOrderNotional
      || state?.reserve_basis !== CURRENT_EQUITY_RESERVE_BASIS
      || state?.loss_response !== LOSS_RESIZE_POLICY
      || state?.reserve_monotonic !== false
      || state?.continue_after_loss !== true
      || state?.prior_state_session_id !== policy.priorStateSessionId
      || state?.prior_state_blob_name !== policy.priorStateBlobName
      || ![
        highWater,
        historicalHighWater,
        lastEquity,
        protectedReserve,
        operatingBuffer,
        operableCapital
      ]
        .every((value) => Number.isFinite(value) && value >= 0)
      || historicalHighWater + 1e-9 < highWater
      || highWater + 1e-9 < minimumHistoricalHighWaterEquity
      || historicalHighWater + 1e-9 < minimumHistoricalHighWaterEquity
      || highWater + 1e-9 < Math.max(
        Number(manifest.starting_collateral),
        authorizedEquityCeiling,
        lastEquity
      )
      || Math.abs(protectedReserve - lastEquity * policy.reserveRatio) > 0.0000011
      || Math.abs(operatingBuffer - lastEquity * policy.operatingBufferRatio) > 0.0000011
      || Math.abs(operableCapital - Math.max(
        0,
        lastEquity - protectedReserve - operatingBuffer
      )) > 0.0000011
      || !moneyEqual(state?.verified_realized_pnl, verifiedRealizedPnl)
      || !moneyEqual(state?.authorized_equity_ceiling, authorizedEquityCeiling)
      || JSON.stringify(state?.verified_settlement_ids) !== JSON.stringify(settlementIds)) {
    throw new Error("fail closed: protected reserve migration source state is not fully reconciled to its ledger");
  }
}

function assertMigrationTargetState(state, manifest, policy, source, sourceHash) {
  if (!state) return;
  const highWater = Number(state?.high_water_equity);
  const protectedReserve = Number(state?.protected_reserve);
  const checkpointSource = state?.migration_source_session_id;
  if (state?.schema !== STATE_SCHEMA_V1
      || state?.session_id !== manifest.session_id
      || state?.reconciliation_complete !== true
      || state?.reserve_monotonic !== true
      || !Number.isFinite(highWater)
      || highWater < 0
      || !Number.isFinite(protectedReserve)
      || protectedReserve < 0
      || protectedReserve + 0.0000011 < highWater * policy.reserveRatio
      || (state.reserve_ratio !== undefined
        && Number(state.reserve_ratio) !== policy.reserveRatio)
      || (state.operating_buffer_ratio !== undefined
        && Number(state.operating_buffer_ratio) !== policy.operatingBufferRatio)
      || (state.minimum_order_notional !== undefined
        && Number(state.minimum_order_notional) !== policy.minimumOrderNotional)
      || (state.reserve_basis !== undefined
        && state.reserve_basis !== MONOTONIC_RESERVE_BASIS)
      || (state.continue_after_loss !== undefined
        && state.continue_after_loss !== false)
      || (state.loss_response !== undefined && state.loss_response !== null)
      || (state.prior_state_session_id !== undefined
        && state.prior_state_session_id !== null)
      || (state.prior_state_blob_name !== undefined
        && state.prior_state_blob_name !== null)
      || (checkpointSource !== undefined
        && (checkpointSource !== source.sessionId
          || state.migration_source_session_blob_name !== source.sessionBlobName
          || state.migration_source_session_sha256 !== sourceHash
          || state.migration_source_state_blob_name !== source.stateBlobName))) {
    throw new Error("fail closed: protected reserve migration target state is incompatible");
  }
  assertMigrationCheckpoint(state);
}

function migrationResult(state, sourceStateDocument, sourceHash, authorizedEquityCeiling) {
  return {
    state,
    source_state_etag: sourceStateDocument.etag,
    source_session_sha256: sourceHash,
    verified_equity_ceiling: authorizedEquityCeiling,
    verified_settlement_ids: [...state.migration_verified_settlement_ids]
  };
}

function assertCompatibleState(state, manifest, policy) {
  if (!state) return;
  if (state.schema !== policy.stateSchema
      || state.session_id !== manifest.session_id
      || Number(state.reserve_ratio) !== policy.reserveRatio
      || Number(state.operating_buffer_ratio) !== policy.operatingBufferRatio
      || Number(state.minimum_order_notional) !== policy.minimumOrderNotional
      || (policy.targetOrderRatio !== undefined
        && (Number(state.minimum_reserve) !== policy.minimumReserve
          || Number(state.target_order_ratio) !== policy.targetOrderRatio))
      || (policy.targetOrderRatio === undefined
        && (state.minimum_reserve !== undefined || state.target_order_ratio !== undefined))
      || state.reserve_basis !== policy.reserveBasis
      || state.reserve_monotonic !== policy.reserveMonotonic
      || state.continue_after_loss !== !policy.reserveMonotonic
      || (!policy.reserveMonotonic
        && (state.prior_state_session_id !== policy.priorStateSessionId
          || state.prior_state_blob_name !== policy.priorStateBlobName
          || (policy.priorStateSha256
            && normalizedSha256(state.prior_state_sha256) !== policy.priorStateSha256)
          || Number(state.historical_high_water_equity) + 1e-9 <
            policy.minimumHistoricalHighWaterEquity))
      || (policy.lossResponse && state.loss_response !== policy.lossResponse)) {
    throw new Error("fail closed: persisted protected compounding state is incompatible");
  }
  assertMigrationCheckpoint(state);
}

function sameCapitalState(left, right) {
  return [
    "session_id",
    "reserve_ratio",
    "operating_buffer_ratio",
    "minimum_order_notional",
    "minimum_reserve",
    "target_order_ratio",
    "high_water_equity",
    "protected_reserve",
    "last_reconciled_equity",
    "operating_buffer",
    "operable_capital",
    "authorized_equity_ceiling",
    "verified_realized_pnl",
    "reconciliation_complete",
    "historical_high_water_equity",
    "prior_state_session_id",
    "prior_state_blob_name",
    "prior_state_sha256",
    "reserve_basis",
    "loss_response",
    "continue_after_loss",
    "reserve_monotonic",
    "migration_source_session_id",
    "migration_source_session_blob_name",
    "migration_source_session_sha256",
    "migration_source_state_blob_name",
    "migration_source_state_etag",
    "migration_minimum_historical_high_water_equity",
    "migration_verified_equity_ceiling",
    "migration_verified_settlement_ids",
    "migration_completed_at"
  ].every((field) => JSON.stringify(left?.[field]) === JSON.stringify(right?.[field]))
    && JSON.stringify(left?.verified_settlement_ids) === JSON.stringify(right?.verified_settlement_ids);
}

function assertMigrationCheckpoint(state) {
  const fields = [
    state?.migration_source_session_id,
    state?.migration_source_session_blob_name,
    state?.migration_source_session_sha256,
    state?.migration_source_state_blob_name,
    state?.migration_source_state_etag,
    state?.migration_minimum_historical_high_water_equity,
    state?.migration_verified_equity_ceiling,
    state?.migration_verified_settlement_ids,
    state?.migration_completed_at
  ];
  if (fields.every((value) => value === undefined)) return;
  const ids = state?.migration_verified_settlement_ids;
  const minimumHighWater = Number(
    state?.migration_minimum_historical_high_water_equity
  );
  const highWater = Number(state?.high_water_equity);
  const historicalHighWater = Number(state?.historical_high_water_equity);
  const protectedReserve = Number(state?.protected_reserve);
  const reserveRatio = Number(state?.reserve_ratio);
  const migrationCeiling = Number(state?.migration_verified_equity_ceiling);
  const currentCeiling = Number(state?.authorized_equity_ceiling);
  if (!safeSessionId(state?.migration_source_session_id)
      || state.migration_source_session_id === state.session_id
      || state?.migration_source_session_blob_name !==
        `reports/funded/dynamic-quote/sessions/${state.migration_source_session_id}/session.json`
      || !normalizedSha256(state?.migration_source_session_sha256)
      || state?.migration_source_state_blob_name !==
        `reports/funded/dynamic-quote/sessions/${state.migration_source_session_id}/capital-reserve-state.json`
      || typeof state?.migration_source_state_etag !== "string"
      || !state.migration_source_state_etag
      || !Number.isFinite(minimumHighWater)
      || minimumHighWater < 0
      || !Number.isFinite(highWater)
      || highWater + 1e-9 < minimumHighWater
      || !Number.isFinite(historicalHighWater)
      || historicalHighWater + 1e-9 < minimumHighWater
      || !Number.isFinite(reserveRatio)
      || !Number.isFinite(protectedReserve)
      || protectedReserve + 0.0000011 < highWater * reserveRatio
      || !Number.isFinite(migrationCeiling)
      || !Number.isFinite(currentCeiling)
      || migrationCeiling > currentCeiling + 1e-9
      || !Array.isArray(ids)
      || new Set(ids).size !== ids.length
      || JSON.stringify(ids) !== JSON.stringify([...ids].sort())
      || ids.some((id) => typeof id !== "string" || !id)
      || ids.some((id) => !state?.verified_settlement_ids?.includes(id))
      || !Number.isFinite(Date.parse(state?.migration_completed_at))) {
    throw new Error("fail closed: persisted protected reserve migration checkpoint is invalid");
  }
}

function migrationCheckpoint(state) {
  if (state?.migration_source_session_id === undefined) return {};
  return {
    migration_source_session_id: state.migration_source_session_id,
    migration_source_session_blob_name: state.migration_source_session_blob_name,
    migration_source_session_sha256: state.migration_source_session_sha256,
    migration_source_state_blob_name: state.migration_source_state_blob_name,
    migration_source_state_etag: state.migration_source_state_etag,
    migration_minimum_historical_high_water_equity:
      state.migration_minimum_historical_high_water_equity,
    migration_verified_equity_ceiling: state.migration_verified_equity_ceiling,
    migration_verified_settlement_ids: state.migration_verified_settlement_ids,
    migration_completed_at: state.migration_completed_at
  };
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
        || value.id !== automaticSettlementId(value.transaction_hash, value.condition_id)
        || !validUniqueStrings(value.run_ids)
        || !validUniqueStrings(value.probe_ids)
        || !validUniqueStrings(value.order_ids)
        || !validUniqueStrings(value.token_ids)
        || value.run_ids.length !== value.order_ids.length
        || value.probe_ids.length !== value.order_ids.length
        || value.token_ids.length !== 1
        || !normalizedAddress(value.proxy_wallet)
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
        || value.redemption_evidence_decoded !== true
        || normalizedAddress(value.redemption_adapter_address) !==
          PUSD_CTF_COLLATERAL_ADAPTER_ADDRESS
        || normalizedAddress(value.redemption_contract_address) !==
          CONDITIONAL_TOKENS_ADDRESS
        || normalizedAddress(value.redemption_collateral_token) !== USDCE_ADDRESS
        || normalizedHash(value.redemption_parent_collection_id) !== ZERO_TRANSACTION_HASH
        || !Array.isArray(value.redemption_index_sets)
        || value.redemption_index_sets.length === 0
        || value.redemption_index_sets.some((index) =>
          typeof index !== "string" || !normalizedIndexSet(index))
        || canonicalBaseUnits(value.redemption_payout_base_units) !==
          String(Math.round(Number(value.payout) * MONEY_SCALE))
        || value.redemption_transfer_chain_verified !== true
        || normalizedAsset(value.redemption_token_id) !== value.token_ids[0]
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

function normalizedSha256(value) {
  const text = String(value || "").toLowerCase();
  const prefixed = text.startsWith("sha256:") ? text : `sha256:${text}`;
  return /^sha256:[0-9a-f]{64}$/.test(prefixed) ? prefixed : "";
}

function normalizedAddress(value) {
  const text = String(value || "").toLowerCase();
  return /^0x[0-9a-f]{40}$/.test(text) ? text : "";
}

function normalizedAsset(value) {
  const text = String(value || "");
  return /^\d+$/.test(text) ? text : "";
}

function redemptionAssetMatches(value, tokenId) {
  const text = String(value ?? "").trim();
  return text === "" || normalizedAsset(text) === tokenId;
}

function normalizedIndexSet(value) {
  const text = String(value || "");
  if (!/^[1-9]\d*$/.test(text)) return "";
  return BigInt(text) < (1n << 256n) ? text : "";
}

function canonicalBaseUnits(value) {
  const text = String(value || "");
  return /^(0|[1-9]\d*)$/.test(text) ? text : "";
}

function transferContainsExact(transfer, tokenId, valueBaseUnits) {
  const ids = Array.isArray(transfer?.ids) ? transfer.ids.map(normalizedAsset) : [];
  const values = Array.isArray(transfer?.values)
    ? transfer.values.map(canonicalBaseUnits)
    : [];
  return ids.length === values.length
    && ids.filter((id) => id === tokenId).length === 1
    && values[ids.indexOf(tokenId)] === valueBaseUnits;
}

function transferContainsOnly(transfer, tokenId, valueBaseUnits) {
  return Array.isArray(transfer?.ids)
    && Array.isArray(transfer?.values)
    && transfer.ids.length === 1
    && transfer.values.length === 1
    && normalizedAsset(transfer.ids[0]) === tokenId
    && canonicalBaseUnits(transfer.values[0]) === valueBaseUnits;
}

function automaticSettlementId(transactionHash, conditionId) {
  const identity = createHash("sha256")
    .update(`${normalizedHash(transactionHash)}\u0000${normalizedHash(conditionId)}`)
    .digest("hex");
  return `automatic-redeem-${identity}`;
}

function groupClobFills(fills) {
  const groups = new Map();
  for (const fill of fills) {
    const key = `${fill.transaction_hash}:${fill.asset_id}:${fill.condition_id}`;
    const current = groups.get(key) || {
      transaction_hash: fill.transaction_hash,
      asset_id: fill.asset_id,
      condition_id: fill.condition_id,
      size: 0,
      principal: 0,
      timestamp_ms: fill.timestamp_ms
    };
    current.size += Number(fill.size);
    current.principal += Number(fill.size) * Number(fill.price);
    current.timestamp_ms = Math.max(current.timestamp_ms, fill.timestamp_ms);
    groups.set(key, current);
  }
  return [...groups.values()].map((group) => ({
    ...group,
    size: money(group.size),
    principal: money(group.principal)
  }));
}

function validUniqueStrings(values) {
  return Array.isArray(values)
    && values.length > 0
    && values.every((value) => typeof value === "string" && value.length > 0)
    && new Set(values).size === values.length;
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

function safeSessionId(value) {
  return /^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$/.test(String(value || ""));
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
