import { createHash } from "node:crypto";

const RECORD_SCHEMA = "polyedge.verified_internal_profit.v1";
const POLICY_MODE = "verified_internal_profit_quarantine";
const MONEY_SCALE = 1_000_000;

export function validateProfitQuarantineManifest(manifest) {
  const policy = manifest?.profit_quarantine;
  const settlements = Array.isArray(manifest?.verified_internal_settlements)
    ? manifest.verified_internal_settlements
    : [];
  const expectedPrefix =
    `reports/funded/dynamic-quote/sessions/${manifest?.session_id}/verified-internal-profits`;
  const errors = [];
  if (manifest?.allow_compounding !== false) errors.push("allow_compounding must remain false");
  if (policy?.enabled !== true) errors.push("profit_quarantine.enabled must be true");
  if (policy?.mode !== POLICY_MODE) {
    errors.push(`profit_quarantine.mode must equal ${POLICY_MODE}`);
  }
  if (policy?.risk_headroom !== "starting_collateral_only") {
    errors.push("profit_quarantine.risk_headroom must equal starting_collateral_only");
  }
  if (policy?.settlement_ledger_prefix !== expectedPrefix || !safeBlobName(expectedPrefix)) {
    errors.push("profit_quarantine.settlement_ledger_prefix must be scoped to the exact session");
  }
  if (settlements.length === 0) errors.push("verified_internal_settlements must not be empty");
  if (new Set(settlements.map((row) => row?.id)).size !== settlements.length) {
    errors.push("verified_internal_settlements identities must be unique");
  }
  if (settlements.some((row) => !validSettlement(row))) {
    errors.push("verified_internal_settlements contains an invalid settlement");
  }
  if (errors.length) {
    throw new Error(`profit quarantine policy is invalid: ${errors.join("; ")}`);
  }
  return {
    ledgerPrefix: expectedPrefix,
    settlements
  };
}

export async function initializeProfitQuarantine({
  container,
  manifest,
  activity,
  getTransactionReceipt
}) {
  if (!container) throw new Error("fail closed: durable storage is required for profit quarantine");
  const policy = validateProfitQuarantineManifest(manifest);
  const rows = Array.isArray(activity) ? activity : [];
  const verified = [];
  for (const settlement of policy.settlements) {
    const blobName = settlementBlobName(policy.ledgerPrefix, settlement);
    const durable = await readOptionalJson(container, blobName);
    if (durable) {
      assertDurableSettlement(durable, manifest, settlement);
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
      throw new Error(`fail closed: internal profit ${settlement.id} lacks matching redemption evidence`);
    }
    const fillHashes = new Set(settlement.fill_transaction_hashes.map(normalizedHash));
    const fills = rows.filter((row) =>
      String(row?.type || "").toUpperCase() === "TRADE"
      && normalizedHash(row?.conditionId) === conditionId
      && fillHashes.has(normalizedHash(row?.transactionHash))
    );
    if (fills.length !== fillHashes.size
        || !moneyEqual(sum(fills, "size"), settlement.payout)
        || !moneyEqual(sum(fills, "usdcSize"), settlement.principal)) {
      throw new Error(`fail closed: internal profit ${settlement.id} lacks matching authenticated fills`);
    }
    const receipt = await getTransactionReceipt(transactionHash);
    if (receipt?.status !== "success"
        || Number(receipt.chain_id) !== 137
        || !Number.isInteger(Number(receipt.confirmations))
        || Number(receipt.confirmations) < 2) {
      throw new Error(`fail closed: internal profit ${settlement.id} lacks a confirmed Polygon receipt`);
    }
    const record = {
      schema: RECORD_SCHEMA,
      session_id: manifest.session_id,
      id: settlement.id,
      type: "internal_manual_settlement",
      transaction_hash: transactionHash,
      condition_id: conditionId,
      payout: money(settlement.payout),
      principal: money(settlement.principal),
      realized_pnl: money(settlement.realized_pnl),
      fill_transaction_hashes: [...fillHashes].sort(),
      settled_at: settlement.settled_at,
      evidence_source: "polymarket_data_api_fills_plus_polygon_receipt",
      receipt_block_number: String(receipt.block_number),
      receipt_confirmations: Number(receipt.confirmations),
      quarantined: true,
      risk_headroom: "starting_collateral_only"
    };
    await putImmutableJson(container, blobName, record);
    verified.push(record);
  }
  const verifiedRealizedPnl = money(
    verified.reduce((total, row) => total + Number(row.realized_pnl), 0)
  );
  if (!(verifiedRealizedPnl > 0)) {
    throw new Error("fail closed: verified internal profit quarantine must be positive");
  }
  return {
    schema: "polyedge.profit_quarantine_snapshot.v1",
    session_id: manifest.session_id,
    verified_settlement_ids: verified.map((row) => row.id).sort(),
    verified_internal_realized_pnl: verifiedRealizedPnl,
    quarantined_internal_profit: verifiedRealizedPnl,
    authorized_equity_ceiling: money(Number(manifest.starting_collateral) + verifiedRealizedPnl),
    risk_headroom: "starting_collateral_only",
    allow_compounding: false
  };
}

function validSettlement(row) {
  const fillHashes = Array.isArray(row?.fill_transaction_hashes)
    ? row.fill_transaction_hashes.map(normalizedHash)
    : [];
  return safeId(row?.id)
    && row?.type === "internal_manual_settlement"
    && Boolean(normalizedHash(row?.transaction_hash))
    && Boolean(normalizedHash(row?.condition_id))
    && Number(row?.payout) > 0
    && Number(row?.principal) >= 0
    && Number(row?.realized_pnl) > 0
    && moneyEqual(Number(row.payout) - Number(row.principal), row.realized_pnl)
    && fillHashes.length > 0
    && fillHashes.every(Boolean)
    && new Set(fillHashes).size === fillHashes.length
    && Number.isFinite(Date.parse(row?.settled_at));
}

function assertDurableSettlement(record, manifest, settlement) {
  if (record?.schema !== RECORD_SCHEMA
      || record.session_id !== manifest.session_id
      || record.id !== settlement.id
      || record.type !== settlement.type
      || normalizedHash(record.transaction_hash) !== normalizedHash(settlement.transaction_hash)
      || normalizedHash(record.condition_id) !== normalizedHash(settlement.condition_id)
      || !moneyEqual(record.payout, settlement.payout)
      || !moneyEqual(record.principal, settlement.principal)
      || !moneyEqual(record.realized_pnl, settlement.realized_pnl)
      || record.settled_at !== settlement.settled_at
      || record.quarantined !== true
      || record.risk_headroom !== "starting_collateral_only"
      || record.evidence_source !== "polymarket_data_api_fills_plus_polygon_receipt"
      || Number(record.receipt_confirmations) < 2
      || JSON.stringify([...record.fill_transaction_hashes].sort()) !==
        JSON.stringify(settlement.fill_transaction_hashes.map(normalizedHash).sort())) {
    throw new Error(`fail closed: durable internal profit mismatch for ${settlement.id}`);
  }
}

function settlementBlobName(prefix, settlement) {
  const identity = createHash("sha256")
    .update(`${normalizedHash(settlement.transaction_hash)}\u0000${normalizedHash(settlement.condition_id)}`)
    .digest("hex");
  return `${prefix}/${identity}.json`;
}

async function readOptionalJson(container, name) {
  try {
    const response = await container.getBlobClient(name).download();
    return JSON.parse(await streamToString(response.readableStreamBody));
  } catch (error) {
    if (Number(error?.statusCode) === 404) return null;
    throw error;
  }
}

async function putImmutableJson(container, name, value) {
  const bytes = Buffer.from(JSON.stringify(value, null, 2));
  try {
    await container.getBlockBlobClient(name).uploadData(bytes, {
      conditions: { ifNoneMatch: "*" },
      blobHTTPHeaders: { blobContentType: "application/json" }
    });
  } catch (error) {
    if (![409, 412].includes(Number(error?.statusCode))) throw error;
    const existing = await readOptionalJson(container, name);
    if (JSON.stringify(existing) !== JSON.stringify(value)) {
      throw new Error(`fail closed: immutable internal profit record mismatch at ${name}`);
    }
  }
}

function sum(rows, field) {
  return rows.reduce((total, row) => total + Number(row?.[field] || 0), 0);
}

function money(value) {
  return Math.round(Number(value) * MONEY_SCALE) / MONEY_SCALE;
}

function moneyEqual(left, right) {
  return money(left) === money(right);
}

function normalizedHash(value) {
  const hash = String(value || "").trim().toLowerCase();
  return /^0x[0-9a-f]{64}$/.test(hash) ? hash : "";
}

function safeId(value) {
  return /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/.test(String(value || ""));
}

function safeBlobName(value) {
  return String(value || "").length <= 512
    && !String(value).startsWith("/")
    && !String(value).includes("..")
    && !String(value).includes("\\");
}

async function streamToString(stream) {
  let value = "";
  for await (const chunk of stream) value += Buffer.from(chunk).toString("utf8");
  return value;
}
