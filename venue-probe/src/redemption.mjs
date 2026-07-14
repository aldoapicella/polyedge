import {
  encodeFunctionData,
  getCreate2Address,
  keccak256,
  encodeAbiParameters,
  pad,
  concat,
  toHex,
  zeroHash
} from "viem";

export const POLYGON_CHAIN_ID = 137;
export const DEPOSIT_WALLET_FACTORY = "0x00000000000Fb5C9ADea0298D729A0CB3823Cc07";
export const DEPOSIT_WALLET_IMPLEMENTATION = "0x58CA52ebe0DadfdF531Cde7062e76746de4Db1eB";
export const CONDITIONAL_TOKENS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045";
export const PUSD = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB";
export const CTF_COLLATERAL_ADAPTER = "0xADa100874d00e3331D00F2007a9c336a65009718";
export const NEG_RISK_CTF_COLLATERAL_ADAPTER = "0xAdA200001000ef00D07553cEE7006808F895c6F1";

const ERC1967_CONST1 = "0xcc3735a920a3ca505d382bbc545af43d6000803e6038573d6000fd5b3d6000f3";
const ERC1967_CONST2 = "0x5155f3363d3d373d3d363d7f360894a13ba1a3210667c828492db98dca3e2076";
const ERC1967_PREFIX = 0x61003d3d8160233d3973n;

const ctfApprovalAbi = [{
  type: "function",
  name: "setApprovalForAll",
  stateMutability: "nonpayable",
  inputs: [{ name: "operator", type: "address" }, { name: "approved", type: "bool" }],
  outputs: []
}];

const redeemAbi = [{
  type: "function",
  name: "redeemPositions",
  stateMutability: "nonpayable",
  inputs: [
    { name: "collateralToken", type: "address" },
    { name: "parentCollectionId", type: "bytes32" },
    { name: "conditionId", type: "bytes32" },
    { name: "indexSets", type: "uint256[]" }
  ],
  outputs: []
}];

export const DEPOSIT_WALLET_BATCH_TYPES = {
  Call: [
    { name: "target", type: "address" },
    { name: "value", type: "uint256" },
    { name: "data", type: "bytes" }
  ],
  Batch: [
    { name: "wallet", type: "address" },
    { name: "nonce", type: "uint256" },
    { name: "deadline", type: "uint256" },
    { name: "calls", type: "Call[]" }
  ]
};

export function loadRedemptionConfig(env = process.env) {
  const config = {
    executionMode: env.EXECUTION_MODE,
    enabled: env.VENUE_REDEMPTION_ENABLED === "true",
    dryRun: env.VENUE_REDEMPTION_DRY_RUN !== "false",
    trustBoundaryReady: env.FUNDED_EVIDENCE_TRUST_BOUNDARY_READY === "true",
    allowLive: env.ALLOW_LIVE === "true",
    enableTakerOrders: env.ENABLE_TAKER_ORDERS === "true",
    expectedCountry: String(env.VENUE_PROBE_EXPECTED_COUNTRY || "").trim().toUpperCase(),
    expectedEgressIp: String(env.VENUE_PROBE_EXPECTED_EGRESS_IP || "").trim(),
    maxPayout: finiteNumber(env.VENUE_REDEMPTION_MAX_PAYOUT, 25),
    maxConditions: integer(env.VENUE_REDEMPTION_MAX_CONDITIONS, 5),
    startingCapital: finiteNumber(env.VENUE_PROBE_STARTING_CAPITAL, null),
    campaignId: String(env.VENUE_PROBE_FUNDED_CAMPAIGN_ID || "funded-campaign-2026-07-12"),
    campaignBaselineEquity: finiteNumber(env.VENUE_PROBE_CAMPAIGN_BASELINE_EQUITY, 5.030521),
    campaignEquityFloor: finiteNumber(env.VENUE_PROBE_CAMPAIGN_EQUITY_FLOOR, 4.03),
    maxCampaignDrawdown: finiteNumber(env.VENUE_PROBE_MAX_CAMPAIGN_DRAWDOWN, 1),
    maxOrderNotional: 1,
    maxReconciliationDiscrepancy: finiteNumber(env.VENUE_PROBE_MAX_RECONCILIATION_DISCREPANCY, 0.01),
    campaignCashFlows: JSON.parse(env.VENUE_PROBE_CAMPAIGN_CASH_FLOWS || "[]"),
    privateKey: env.POLYMARKET_PRIVATE_KEY,
    funderAddress: env.POLYMARKET_FUNDER_ADDRESS,
    relayerApiKey: env.POLYMARKET_RELAYER_API_KEY,
    relayerApiKeyAddress: env.POLYMARKET_RELAYER_API_KEY_ADDRESS,
    apiKey: env.POLYMARKET_API_KEY,
    apiSecret: env.POLYMARKET_API_SECRET,
    apiPassphrase: env.POLYMARKET_API_PASSPHRASE,
    signatureType: integer(env.POLYMARKET_SIGNATURE_TYPE, 3),
    clobUrl: env.POLYMARKET_CLOB_URL || "https://clob.polymarket.com",
    dataUrl: env.POLYMARKET_DATA_URL || "https://data-api.polymarket.com",
    relayerUrl: env.POLYMARKET_RELAYER_URL || "https://relayer-v2.polymarket.com",
    rpcUrl: env.POLYGON_RPC_URL || "https://polygon-bor-rpc.publicnode.com",
    storageAccount: env.AZURE_STORAGE_ACCOUNT_NAME,
    storageContainer: env.AZURE_STORAGE_CONTAINER_NAME || "bot-events",
    storageAccountKey: env.AZURE_STORAGE_ACCOUNT_KEY,
    azureClientId: env.AZURE_CLIENT_ID
  };
  validateRedemptionConfig(config);
  return config;
}

export function validateRedemptionConfig(config) {
  const errors = [];
  if (config.executionMode !== "venue_redemption") errors.push("EXECUTION_MODE must equal venue_redemption");
  if (config.allowLive) errors.push("ALLOW_LIVE must remain false");
  if (config.enableTakerOrders) errors.push("ENABLE_TAKER_ORDERS must remain false");
  if (config.signatureType !== 3) errors.push("POLYMARKET_SIGNATURE_TYPE must equal 3 for the deposit wallet");
  if (!(config.maxPayout > 0 && config.maxPayout <= 25)) errors.push("VENUE_REDEMPTION_MAX_PAYOUT must be in (0, 25]");
  if (!(config.maxConditions >= 1 && config.maxConditions <= 5)) errors.push("VENUE_REDEMPTION_MAX_CONDITIONS must be in [1, 5]");
  for (const [name, value] of [
    ["POLYMARKET_PRIVATE_KEY", config.privateKey],
    ["POLYMARKET_FUNDER_ADDRESS", config.funderAddress],
    ["POLYMARKET_API_KEY", config.apiKey],
    ["POLYMARKET_API_SECRET", config.apiSecret],
    ["POLYMARKET_API_PASSPHRASE", config.apiPassphrase],
    ["AZURE_STORAGE_ACCOUNT_NAME", config.storageAccount]
  ]) {
    if (!value) errors.push(`${name} is required`);
  }
  for (const [name, value, expected] of [
    ["POLYMARKET_CLOB_URL", config.clobUrl, "https://clob.polymarket.com"],
    ["POLYMARKET_DATA_URL", config.dataUrl, "https://data-api.polymarket.com"],
    ["POLYMARKET_RELAYER_URL", config.relayerUrl, "https://relayer-v2.polymarket.com"]
  ]) {
    if (value !== expected) errors.push(`${name} must equal ${expected}`);
  }
  if (!config.dryRun) {
    if (!config.trustBoundaryReady) errors.push("FUNDED_EVIDENCE_TRUST_BOUNDARY_READY must be true only after signer/control/attestor isolation");
    if (!config.enabled) errors.push("VENUE_REDEMPTION_ENABLED must be true for submission");
    if (!config.expectedCountry) errors.push("VENUE_PROBE_EXPECTED_COUNTRY is required for submission");
    if (!config.expectedEgressIp) errors.push("VENUE_PROBE_EXPECTED_EGRESS_IP is required for submission");
    if (!config.relayerApiKey) errors.push("POLYMARKET_RELAYER_API_KEY is required for submission");
    if (!config.relayerApiKeyAddress) errors.push("POLYMARKET_RELAYER_API_KEY_ADDRESS is required for submission");
  }
  if (errors.length) throw new Error(`venue_redemption blocked: ${errors.join("; ")}`);
}

export function deriveLegacyUupsDepositWallet(owner) {
  const args = encodeAbiParameters(
    [{ type: "address" }, { type: "bytes32" }],
    [DEPOSIT_WALLET_FACTORY, pad(owner, { dir: "left", size: 32 })]
  );
  const byteLength = BigInt((args.length - 2) / 2);
  const combined = ERC1967_PREFIX + (byteLength << 56n);
  const bytecodeHash = keccak256(concat([
    toHex(combined, { size: 10 }),
    DEPOSIT_WALLET_IMPLEMENTATION,
    "0x6009",
    ERC1967_CONST2,
    ERC1967_CONST1,
    args
  ]));
  return getCreate2Address({
    from: DEPOSIT_WALLET_FACTORY,
    salt: keccak256(args),
    bytecodeHash
  });
}

export function selectRedeemableConditions(positions, maxPayout = 25, maxConditions = 5) {
  const groups = new Map();
  for (const row of Array.isArray(positions) ? positions : []) {
    if (row?.redeemable !== true || !/^0x[0-9a-fA-F]{64}$/.test(String(row.conditionId || ""))) continue;
    const conditionId = String(row.conditionId);
    if (!groups.has(conditionId)) {
      groups.set(conditionId, {
        condition_id: conditionId,
        negative_risk: row.negativeRisk === true,
        gross_payout: 0,
        titles: new Set(),
        assets: new Map()
      });
    }
    const group = groups.get(conditionId);
    if (group.negative_risk !== (row.negativeRisk === true)) {
      throw new Error(`fail closed: inconsistent negativeRisk metadata for ${conditionId}`);
    }
    group.gross_payout += Math.max(0, finiteNumber(row.currentValue, 0));
    if (row.title) group.titles.add(String(row.title));
    const outcomeIndex = Number(row.outcomeIndex);
    if (/^\d+$/.test(String(row.asset || "")) && [0, 1].includes(outcomeIndex)) {
      addAsset(group.assets, String(row.asset), outcomeIndex);
    }
    if (/^\d+$/.test(String(row.oppositeAsset || "")) && [0, 1].includes(outcomeIndex)) {
      addAsset(group.assets, String(row.oppositeAsset), 1 - outcomeIndex);
    }
  }
  const winners = [...groups.values()]
    .filter((group) => group.gross_payout > 0)
    .sort((left, right) => left.condition_id.localeCompare(right.condition_id));
  const selected = [];
  let payout = 0;
  for (const winner of winners) {
    if (selected.length >= maxConditions) break;
    if (payout + winner.gross_payout > maxPayout + 1e-9) continue;
    selected.push({
      ...winner,
      gross_payout: roundMoney(winner.gross_payout),
      titles: [...winner.titles],
      assets: [...winner.assets.entries()]
        .map(([asset, outcome_index]) => ({ asset, outcome_index }))
        .sort((left, right) => left.outcome_index - right.outcome_index)
    });
    payout += winner.gross_payout;
  }
  return {
    selected,
    selected_gross_payout: roundMoney(payout),
    available_winner_conditions: winners.length,
    skipped_winner_conditions: winners.length - selected.length
  };
}

export function summarizeRecentRedemptions(activity, control = null, limit = 5) {
  const controlledHash = String(control?.transaction_hash || "").toLowerCase();
  return (Array.isArray(activity) ? activity : [])
    .filter((row) => String(row?.type || "").toUpperCase() === "REDEEM")
    .map((row) => {
      const timestampSeconds = finiteNumber(row?.timestamp, 0);
      const transactionHash = /^0x[0-9a-fA-F]{64}$/.test(String(row?.transactionHash || ""))
        ? String(row.transactionHash)
        : null;
      return {
        transaction_hash: transactionHash,
        condition_id: /^0x[0-9a-fA-F]{64}$/.test(String(row?.conditionId || ""))
          ? String(row.conditionId)
          : null,
        title: row?.title ? String(row.title) : null,
        gross_payout: roundMoney(Math.max(0, finiteNumber(row?.usdcSize, 0))),
        redeemed_ts: timestampSeconds > 0 ? new Date(timestampSeconds * 1000).toISOString() : null,
        attribution: transactionHash && controlledHash && transactionHash.toLowerCase() === controlledHash
          ? "azure_redemption_worker"
          : "external_or_manual"
      };
    })
    .filter((row) => row.gross_payout > 0)
    .sort((left, right) => String(right.redeemed_ts || "").localeCompare(String(left.redeemed_ts || "")))
    .slice(0, Math.max(1, Math.min(20, integer(limit, 5))));
}

function addAsset(assets, asset, outcomeIndex) {
  if (assets.has(asset) && assets.get(asset) !== outcomeIndex) {
    throw new Error(`fail closed: inconsistent outcome index for asset ${asset}`);
  }
  assets.set(asset, outcomeIndex);
}

export function buildRedemptionCalls(selection, approvals = {}) {
  const calls = [];
  const requiredAdapters = [...new Set(selection.selected.map((row) =>
    row.negative_risk ? NEG_RISK_CTF_COLLATERAL_ADAPTER : CTF_COLLATERAL_ADAPTER
  ))];
  for (const adapter of requiredAdapters) {
    if (approvals[adapter.toLowerCase()] === true) continue;
    calls.push({
      target: CONDITIONAL_TOKENS,
      value: "0",
      data: encodeFunctionData({ abi: ctfApprovalAbi, functionName: "setApprovalForAll", args: [adapter, true] }),
      purpose: "approve_official_collateral_adapter",
      adapter
    });
  }
  for (const row of selection.selected) {
    const target = row.negative_risk ? NEG_RISK_CTF_COLLATERAL_ADAPTER : CTF_COLLATERAL_ADAPTER;
    calls.push({
      target,
      value: "0",
      data: encodeFunctionData({
        abi: redeemAbi,
        functionName: "redeemPositions",
        args: [PUSD, zeroHash, row.condition_id, [1n, 2n]]
      }),
      purpose: "redeem_resolved_condition",
      condition_id: row.condition_id,
      negative_risk: row.negative_risk
    });
  }
  for (const adapter of requiredAdapters) {
    if (approvals[adapter.toLowerCase()] === true) continue;
    calls.push({
      target: CONDITIONAL_TOKENS,
      value: "0",
      data: encodeFunctionData({ abi: ctfApprovalAbi, functionName: "setApprovalForAll", args: [adapter, false] }),
      purpose: "revoke_official_collateral_adapter",
      adapter
    });
  }
  return calls;
}

export function depositWalletTypedData(wallet, nonce, deadline, calls) {
  return {
    domain: {
      name: "DepositWallet",
      version: "1",
      chainId: POLYGON_CHAIN_ID,
      verifyingContract: wallet
    },
    types: DEPOSIT_WALLET_BATCH_TYPES,
    primaryType: "Batch",
    message: {
      wallet,
      nonce: BigInt(nonce),
      deadline: BigInt(deadline),
      calls: calls.map((call) => ({ target: call.target, value: BigInt(call.value), data: call.data }))
    }
  };
}

export function depositWalletRequest(owner, wallet, nonce, deadline, calls, signature) {
  return {
    type: "WALLET",
    from: owner,
    to: DEPOSIT_WALLET_FACTORY,
    nonce: String(nonce),
    signature,
    depositWalletParams: {
      depositWallet: wallet,
      deadline: String(deadline),
      calls: calls.map((call) => ({ target: call.target, value: String(call.value), data: call.data }))
    }
  };
}

function finiteNumber(value, fallback) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function integer(value, fallback) {
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function roundMoney(value) {
  return Math.round(Number(value) * 1_000_000) / 1_000_000;
}
