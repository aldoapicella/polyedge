import {
  AssetType,
  Chain,
  ClobClient
} from "@polymarket/clob-client-v2";
import {
  createPublicClient,
  createWalletClient,
  decodeEventLog,
  formatUnits,
  http
} from "viem";
import { privateKeyToAccount } from "viem/accounts";
import { polygon } from "viem/chains";
import { pathToFileURL } from "node:url";
import {
  EventLedger,
  acquireCampaignLease,
  assertEligibleOrigin,
  loadCampaignRiskReservationRecords,
  sanitize,
  settleProbeRiskReservations,
  storageContainer,
  summarizePortfolio
} from "./lib.mjs";
import {
  CONDITIONAL_TOKENS,
  CTF_COLLATERAL_ADAPTER,
  NEG_RISK_CTF_COLLATERAL_ADAPTER,
  PUSD,
  buildRedemptionCalls,
  depositWalletRequest,
  depositWalletTypedData,
  deriveLegacyUupsDepositWallet,
  loadRedemptionConfig,
  hasPayoutCap,
  selectRedeemableConditions,
  summarizeRecentRedemptions
} from "./redemption.mjs";
import {
  discoverVerifiedAutomaticInternalSettlements,
  loadDurableInternalSettlements,
  putVerifiedInternalSettlement
} from "./compounding-risk.mjs";
import { decodeSettlementReceiptEvidence } from "./canary.mjs";
import { tradeFillsFromRest } from "./canary-lifecycle-lib.mjs";

let config;
let runId;
let ledger;
let lease;
let redemptionRunning = false;
export const RELAYER_DEADLINE_BUFFER_SECONDS = 600;
const APPROVAL_FOR_ALL_TOPIC =
  "0x17307eab39ab6107e8899845ad3d59bd9653f200f220920489ca2b5937696c31";
const APPROVAL_FOR_ALL_EVENT = [{
  type: "event",
  name: "ApprovalForAll",
  anonymous: false,
  inputs: [
    { name: "account", type: "address", indexed: true },
    { name: "operator", type: "address", indexed: true },
    { name: "approved", type: "bool", indexed: false }
  ]
}];

export async function runVenueRedemption({
  env = process.env,
  inheritedLease = null,
  logger = (value) => console.log(JSON.stringify(sanitize(value)))
} = {}) {
  if (redemptionRunning) throw new Error("fail closed: a venue redemption is already running");
  redemptionRunning = true;
  try {
    config = loadRedemptionConfig(env);
    runId = `venue-redemption-${new Date().toISOString().replace(/[-:.TZ]/g, "")}-${crypto.randomUUID().slice(0, 8)}`;
    ledger = new EventLedger(runId);
    lease = inheritedLease;
    let summary;
    let failure = null;
    try {
      summary = await run();
    } catch (error) {
      failure = error;
      ledger.record("venue_redemption_failed", { message: error.message });
      summary = {
        schema_version: 1,
        run_id: runId,
        status: "failed_closed",
        finished_ts: new Date().toISOString(),
        error: error.message,
        redemption_submitted: ledger.events.some((event) => event.type === "venue_redemption_send"),
        research_only: !config.fundedServiceManaged,
        live_strategy_enabled: config.fundedServiceManaged
      };
    }

    try {
      await uploadRedemptionEvidence(summary);
    } catch (error) {
      failure ||= new Error(`redemption evidence upload failed: ${error.message}`);
    }

    try {
      if (lease && !inheritedLease) await lease.release();
    } catch (error) {
      failure ||= new Error(`redemption lease release failed: ${error.message}`);
    }
    logger(sanitize(summary));
    if (failure) throw failure;
    return summary;
  } finally {
    lease = null;
    ledger = null;
    config = null;
    runId = null;
    redemptionRunning = false;
  }
}

async function run() {
  lease ||= await acquireCampaignLease(config, runId);
  ledger.record("venue_redemption_started", {
    dry_run: config.dryRun,
    enabled: config.enabled,
    max_payout: config.maxPayout,
    max_conditions: config.maxConditions,
    execution_origin: config.executionOrigin,
    funded_service_managed: config.fundedServiceManaged
  });

  const geoblock = await checkOrigin("startup");
  const account = privateKeyToAccount(normalizePrivateKey(config.privateKey));
  const derivedWallet = deriveLegacyUupsDepositWallet(account.address);
  if (derivedWallet.toLowerCase() !== config.funderAddress.toLowerCase()) {
    throw new Error("fail closed: signer does not derive the configured legacy UUPS deposit wallet");
  }
  const transport = http(config.rpcUrl, { timeout: 15_000, retryCount: 2 });
  const publicClient = createPublicClient({ chain: polygon, transport });
  const walletClient = createWalletClient({ account, chain: polygon, transport });
  const deployedCode = await publicClient.getCode({ address: config.funderAddress });
  if (!deployedCode || deployedCode === "0x") throw new Error("fail closed: configured deposit wallet is not deployed");

  const clob = new ClobClient({
    host: config.clobUrl,
    chain: Chain.POLYGON,
    signer: walletClient,
    creds: { key: config.apiKey, secret: config.apiSecret, passphrase: config.apiPassphrase },
    signatureType: config.signatureType,
    funderAddress: config.funderAddress,
    useServerTime: true,
    throwOnError: true
  });
  const openOrders = await clob.getOpenOrders(undefined, true);
  if (!Array.isArray(openOrders)) throw new Error("fail closed: CLOB open-order response is not an array");
  if (openOrders.length) throw new Error(`fail closed: account has ${openOrders.length} open order(s)`);

  const [positions, balance, activity, initialRedemptionControl] = await Promise.all([
    fetchPositions(),
    clob.getBalanceAllowance({ asset_type: AssetType.COLLATERAL, signature_type: config.signatureType }),
    fetchActivity(),
    readRedemptionControl()
  ]);
  const liquidBefore = Number(formatUnits(BigInt(balance.balance || "0"), 6));
  const onchainLiquidBefore = await readPusdBalance(publicClient);
  if (Math.abs(onchainLiquidBefore - liquidBefore) > 0.000001) {
    throw new Error("fail closed: CLOB and onchain liquid collateral disagree before redemption");
  }
  if (initialRedemptionControl?.state === "confirmed_pending_verification") {
    return recoverConfirmedRedemption({
      account,
      clob,
      publicClient,
      geoblock,
      control: initialRedemptionControl,
      liquidCollateral: liquidBefore
    });
  }
  const selection = await validateOnchainSelection(
    publicClient,
    selectRedeemableConditions(positions, config.maxPayout, config.maxConditions)
  );
  const redemptionControl = await reconcilePriorRejectedSubmission(
    initialRedemptionControl,
    selection
  );
  const approvals = await adapterApprovals(publicClient, selection);
  const calls = buildRedemptionCalls(selection, approvals);
  const recentRedemptions = summarizeRecentRedemptions(activity, redemptionControl);
  const portfolio = summarizePortfolio(positions, liquidBefore, config.startingCapital);
  portfolio.captured_ts = new Date().toISOString();
  ledger.record("venue_redemption_preflight", {
    signer_address: account.address,
    funder_address: config.funderAddress,
    derived_wallet_match: true,
    deposit_wallet_deployed: true,
    open_order_count: 0,
    liquid_collateral_before: liquidBefore,
    onchain_liquid_collateral_before: onchainLiquidBefore,
    selection,
    recent_redemptions: recentRedemptions,
    portfolio,
    approvals,
    call_plan: calls.map((call) => ({
      purpose: call.purpose,
      target: call.target,
      condition_id: call.condition_id,
      negative_risk: call.negative_risk
    }))
  });

  if (!selection.selected.length) {
    return baseSummary("nothing_to_redeem", geoblock, account.address, liquidBefore, selection, approvals, calls, recentRedemptions, portfolio);
  }
  if (config.dryRun) {
    return baseSummary("redemption_ready_no_transaction", geoblock, account.address, liquidBefore, selection, approvals, calls, recentRedemptions, portfolio);
  }

  await validateRelayerCredential();

  const pending = redemptionControl;
  if (pending && ![
    "confirmed_and_verified",
    "failed_before_submission",
    "relayer_failed",
    "relayer_rejected"
  ].includes(pending.state)) {
    throw new Error(`fail closed: prior redemption control record is unresolved (${pending.state || "unknown"})`);
  }

  lease.assertHealthy();
  await checkOrigin("pre_submit");
  const finalOrders = await clob.getOpenOrders(undefined, true);
  if (!Array.isArray(finalOrders) || finalOrders.length) throw new Error("fail closed: open-order state changed before redemption");
  const finalSelection = await validateOnchainSelection(
    publicClient,
    selectRedeemableConditions(await fetchPositions(), config.maxPayout, config.maxConditions)
  );
  if (JSON.stringify(finalSelection.selected) !== JSON.stringify(selection.selected)) {
    throw new Error("fail closed: redeemable position set changed after preflight");
  }

  const control = {
    schema_version: 1,
    state: "prepared",
    run_id: runId,
    owner: account.address,
    signer_address: account.address,
    funder: config.funderAddress,
    condition_ids: selection.selected.map((row) => row.condition_id),
    expected_gross_payout: selection.selected_gross_payout,
    submission_attempted: false,
    transaction_id: null,
    transaction_hash: null,
    created_ts: new Date().toISOString(),
    updated_ts: new Date().toISOString()
  };
  await writeRedemptionControl(control);

  let sent = false;
  try {
    const nonce = await relayerJson(`/nonce?address=${encodeURIComponent(account.address)}&type=WALLET`);
    const deadline = Math.floor(Date.now() / 1000) + RELAYER_DEADLINE_BUFFER_SECONDS;
    const typedData = depositWalletTypedData(config.funderAddress, nonce.nonce, deadline, calls);
    const signature = await account.signTypedData(typedData);
    const request = depositWalletRequest(account.address, config.funderAddress, nonce.nonce, deadline, calls, signature);
    await checkOrigin("immediate_pre_submit");
    lease.assertHealthy();
    const immediateOrders = await clob.getOpenOrders(undefined, true);
    if (!Array.isArray(immediateOrders) || immediateOrders.length) {
      throw new Error("fail closed: open-order state changed immediately before redemption");
    }
    control.state = "submission_attempted";
    control.submission_attempted = true;
    control.updated_ts = new Date().toISOString();
    await writeRedemptionControl(control);
    ledger.record("venue_redemption_send", {
      condition_count: selection.selected.length,
      expected_gross_payout: selection.selected_gross_payout,
      call_count: calls.length
    });
    sent = true;
    let accepted;
    try {
      accepted = await relayerJson("/submit", { method: "POST", body: JSON.stringify(request) });
    } catch (error) {
      if (Number(error.relayerHttpStatus) >= 400 && Number(error.relayerHttpStatus) < 500) {
        control.state = "relayer_rejected";
        control.relayer_http_status = Number(error.relayerHttpStatus);
        control.relayer_error_detail = error.relayerSafeDetail || null;
        control.updated_ts = new Date().toISOString();
        await writeRedemptionControl(control);
      }
      throw error;
    }
    if (!accepted?.transactionID) throw new Error("relayer accepted response did not contain transactionID");
    control.state = "relayer_accepted";
    control.transaction_id = accepted.transactionID;
    control.updated_ts = new Date().toISOString();
    await writeRedemptionControl(control);
    ledger.record("venue_redemption_relayer_ack", { transaction_id: accepted.transactionID, state: accepted.state });

    const transaction = await waitForRelayerConfirmation(accepted.transactionID);
    if (!transaction) {
      control.state = "relayer_failed";
      control.updated_ts = new Date().toISOString();
      await writeRedemptionControl(control);
      throw new Error("fail closed: relayer transaction reached failed state");
    }
    control.state = "confirmed_pending_verification";
    control.transaction_hash = transaction.transactionHash || null;
    control.updated_ts = new Date().toISOString();
    await writeRedemptionControl(control);

    if (!transaction.transactionHash) throw new Error("fail closed: confirmed relayer transaction has no transaction hash");
    const receipt = await publicClient.waitForTransactionReceipt({
      hash: transaction.transactionHash,
      confirmations: 2,
      timeout: 60_000
    });
    if (receipt.status !== "success") throw new Error("fail closed: redemption transaction receipt was not successful");
    await clob.updateBalanceAllowance({ asset_type: AssetType.COLLATERAL, signature_type: config.signatureType });

    const verified = await waitForSettlementVerification(clob, publicClient, selection, approvals, liquidBefore);
    const reservationRecords = await loadCampaignRiskReservationRecords(config);
    const settlementValues = await waitForAutomaticSettlementEvidence({
      clob,
      transactionHash: transaction.transactionHash,
      receipt,
      reservationRecords,
      conditionIds: selection.selected.map((row) => row.condition_id)
    });
    if (settlementValues.length !== selection.selected.length ||
        settlementValues.some((settlement) =>
          !selection.selected.some((row) =>
            row.condition_id.toLowerCase() === settlement.condition_id.toLowerCase()
          ))) {
      throw new Error("fail closed: automatic settlement does not match the exact redeemed condition set");
    }
    const settlements = [];
    for (const settlement of settlementValues) {
      settlements.push(await putVerifiedInternalSettlement(
        storageContainer(config),
        settlement
      ));
    }
    control.state = "confirmed_and_verified";
    control.liquid_collateral_after = verified.liquid_collateral_after;
    control.realized_payout = verified.realized_payout;
    control.internal_settlement_blobs = settlements.map((row) => row.blob_name);
    control.updated_ts = new Date().toISOString();
    await settleProbeRiskReservations(config, {
      condition_ids: selection.selected.map((row) => row.condition_id),
      settlement_verified: true,
      trust_boundary_ready: config.trustBoundaryReady,
      transaction_hash: transaction.transactionHash,
      polygon_chain_id: polygon.id,
      transaction_receipt_status: receipt.status,
      transaction_block_number: receipt.blockNumber.toString(),
      transaction_receipt_confirmations: 2,
      settlement_wallet: config.funderAddress,
      settlement_signer: account.address,
      run_id: runId,
      settled_ts: control.updated_ts,
      terminal_portfolio: verified.portfolio,
      zero_open_orders_confirmed: verified.zero_open_orders_confirmed,
      evidence_source: "polymarket_data_api_plus_onchain_redemption"
    }, {
      reservationRecords
    });
    await writeRedemptionControl(control);
    ledger.record("venue_redemption_verified", verified);
    return {
      ...baseSummary("redeemed_and_verified", geoblock, account.address, liquidBefore, selection, approvals, calls, recentRedemptions, verified.portfolio),
      redemption_submitted: true,
      transaction_id: accepted.transactionID,
      transaction_hash: transaction.transactionHash || null,
      liquid_collateral_after: verified.liquid_collateral_after,
      realized_payout: verified.realized_payout,
      zero_open_orders_confirmed: verified.zero_open_orders_confirmed,
      internal_settlement_blobs: settlements.map((row) => row.blob_name)
    };
  } catch (error) {
    if (!sent) {
      control.state = "failed_before_submission";
      control.updated_ts = new Date().toISOString();
      await writeRedemptionControl(control);
    }
    throw error;
  }
}

async function recoverConfirmedRedemption({
  account,
  clob,
  publicClient,
  geoblock,
  control,
  liquidCollateral
}) {
  if (!confirmedRedemptionControlMatches(control, {
    owner: account.address,
    funder: config.funderAddress,
    maxPayout: config.maxPayout,
    maxConditions: config.maxConditions
  })) {
    throw new Error("fail closed: confirmed redemption control binding is invalid");
  }
  const transactionHash = String(control.transaction_hash).toLowerCase();
  const conditionIds = control.condition_ids.map((value) => String(value).toLowerCase());
  ledger.record("venue_redemption_recovery_started", {
    prior_run_id: control.run_id,
    transaction_id: control.transaction_id,
    transaction_hash: transactionHash,
    condition_ids: conditionIds
  });

  const receipt = await publicClient.waitForTransactionReceipt({
    hash: transactionHash,
    confirmations: 2,
    timeout: 60_000
  });
  if (receipt.status !== "success") {
    throw new Error("fail closed: confirmed redemption recovery receipt was not successful");
  }
  await clob.updateBalanceAllowance({
    asset_type: AssetType.COLLATERAL,
    signature_type: config.signatureType
  });

  const container = storageContainer(config);
  const manifest = JSON.parse(config.fundedSessionManifestJson);
  const reservationRecords = await loadCampaignRiskReservationRecords(config);
  const durableSettlements = await loadDurableInternalSettlements(
    container,
    config.campaignId
  );
  const exactDurable = exactControlledSettlements(
    durableSettlements,
    transactionHash,
    conditionIds
  );
  let settlementValues = exactDurable;
  if (settlementValues.length !== conditionIds.length) {
    const discovered = await waitForAutomaticSettlementEvidence({
      clob,
      transactionHash,
      receipt,
      reservationRecords,
      conditionIds
    });
    settlementValues = exactControlledSettlements(
      [...exactDurable, ...discovered],
      transactionHash,
      conditionIds
    );
  }
  assertExactControlledSettlements(
    settlementValues,
    transactionHash,
    conditionIds,
    control.expected_gross_payout
  );

  const settlements = [];
  for (const settlement of settlementValues) {
    settlements.push(await putVerifiedInternalSettlement(container, settlement));
  }
  const verified = await waitForRecoveredSettlementState({
    clob,
    publicClient,
    conditionIds,
    settlementValues,
    expectedApprovals: expectedRecoveredAdapterApprovals(
      receipt,
      config.funderAddress,
      settlementValues.map((settlement) =>
        settlement.redemption_adapter_address)
    )
  });
  await settleProbeRiskReservations(config, {
    condition_ids: conditionIds,
    settlement_verified: true,
    trust_boundary_ready: config.trustBoundaryReady,
    transaction_hash: transactionHash,
    polygon_chain_id: polygon.id,
    transaction_receipt_status: receipt.status,
    transaction_block_number: receipt.blockNumber.toString(),
    transaction_receipt_confirmations: 2,
    settlement_wallet: config.funderAddress,
    settlement_signer: account.address,
    run_id: runId,
    settled_ts: new Date().toISOString(),
    zero_open_orders_confirmed: true,
    evidence_source: "polymarket_data_api_plus_onchain_redemption"
  }, {
    reservationRecords
  });

  const finalized = {
    ...control,
    state: "confirmed_and_verified",
    recovery_run_id: runId,
    liquid_collateral_after: verified.liquid_collateral_after,
    realized_payout: verified.realized_payout,
    internal_settlement_blobs: settlements.map((row) => row.blob_name),
    updated_ts: new Date().toISOString()
  };
  await writeRedemptionControl(finalized);
  ledger.record("venue_redemption_recovery_verified", {
    prior_run_id: control.run_id,
    transaction_hash: transactionHash,
    condition_ids: conditionIds,
    realized_payout: verified.realized_payout,
    zero_open_orders_confirmed: true
  });
  return {
    schema_version: 1,
    run_id: runId,
    status: "recovered_confirmed_and_verified",
    finished_ts: new Date().toISOString(),
    execution_origin: config.executionOrigin,
    execution_country: geoblock.country,
    static_egress_verified: geoblock.ip === config.expectedEgressIp,
    dry_run: false,
    redemption_enabled: true,
    owner: account.address,
    funder: config.funderAddress,
    wallet_type: "legacy_uups_deposit_wallet",
    derived_wallet_match: true,
    liquid_collateral_before: liquidCollateral,
    liquid_collateral_after: verified.liquid_collateral_after,
    realized_payout: verified.realized_payout,
    selection: {
      selected: settlementValues.map((settlement) => ({
        condition_id: settlement.condition_id,
        gross_payout: settlement.payout
      })),
      selected_gross_payout: verified.realized_payout,
      payout_source: "decoded_confirmed_recovery_receipt"
    },
    recent_redemptions: [],
    portfolio: verified.portfolio,
    approvals: verified.approvals,
    planned_calls: [],
    zero_open_orders_confirmed: true,
    redemption_submitted: true,
    new_submission_attempted: false,
    confirmed_transaction_reused: true,
    transaction_id: control.transaction_id,
    transaction_hash: transactionHash,
    internal_settlement_blobs: settlements.map((row) => row.blob_name),
    research_only: false,
    live_strategy_enabled: true
  };
}

export function confirmedRedemptionControlMatches(control, {
  owner,
  funder,
  maxPayout,
  maxConditions = 5
}) {
  const conditions = Array.isArray(control?.condition_ids)
    ? control.condition_ids.map((value) => String(value).toLowerCase())
    : [];
  const expectedOwner = String(owner || "").toLowerCase();
  const storedOwner = String(control?.owner || "").toLowerCase();
  const storedSigner = String(control?.signer_address || "").toLowerCase();
  const signerBound = storedSigner
    ? storedSigner === expectedOwner && /^0x[0-9a-f]{40}$/.test(storedSigner)
    : control?.schema_version === 1 && control?.owner === "[REDACTED]";
  const expectedGrossPayout = Number(control?.expected_gross_payout);
  const payoutWithinConfiguredLimit = Number.isFinite(expectedGrossPayout) &&
    expectedGrossPayout > 0 &&
    (!hasPayoutCap(maxPayout) || expectedGrossPayout <= Number(maxPayout) + 1e-9);
  return control?.state === "confirmed_pending_verification" &&
    control?.submission_attempted === true &&
    typeof control?.run_id === "string" &&
    control.run_id.startsWith("venue-redemption-") &&
    typeof control?.transaction_id === "string" &&
    control.transaction_id.length > 0 &&
    /^0x[0-9a-f]{64}$/.test(String(control?.transaction_hash || "").toLowerCase()) &&
    signerBound &&
    (storedOwner === expectedOwner || control?.owner === "[REDACTED]") &&
    String(control?.funder || "").toLowerCase() === String(funder || "").toLowerCase() &&
    /^0x[0-9a-f]{40}$/.test(String(control?.funder || "").toLowerCase()) &&
    conditions.length > 0 &&
    conditions.length <= Number(maxConditions) &&
    new Set(conditions).size === conditions.length &&
    conditions.every((value) => /^0x[0-9a-f]{64}$/.test(value)) &&
    payoutWithinConfiguredLimit &&
    Number.isFinite(Date.parse(String(control?.created_ts || ""))) &&
    Number.isFinite(Date.parse(String(control?.updated_ts || "")));
}

function exactControlledSettlements(settlements, transactionHash, conditionIds) {
  const expectedConditions = new Set(conditionIds.map((value) => String(value).toLowerCase()));
  return (Array.isArray(settlements) ? settlements : []).filter((settlement) =>
    String(settlement?.transaction_hash || "").toLowerCase() === transactionHash &&
    expectedConditions.has(String(settlement?.condition_id || "").toLowerCase())
  );
}

function assertExactControlledSettlements(
  settlements,
  transactionHash,
  conditionIds,
  expectedPayout
) {
  const identities = settlements.map((settlement) =>
    `${String(settlement?.transaction_hash || "").toLowerCase()}:` +
    String(settlement?.condition_id || "").toLowerCase()
  );
  const conditions = settlements.map((settlement) =>
    String(settlement?.condition_id || "").toLowerCase()
  ).sort();
  const expectedConditions = conditionIds.map((value) => String(value).toLowerCase()).sort();
  const payout = settlements.reduce((total, settlement) =>
    total + Number(settlement?.payout || 0), 0);
  if (settlements.length !== expectedConditions.length ||
      new Set(identities).size !== identities.length ||
      identities.some((identity) => !identity.startsWith(`${transactionHash}:`)) ||
      JSON.stringify(conditions) !== JSON.stringify(expectedConditions) ||
      Math.abs(payout - Number(expectedPayout)) > 0.01) {
    throw new Error("fail closed: recovered settlement does not match the exact durable control");
  }
}

async function waitForRecoveredSettlementState({
  clob,
  publicClient,
  conditionIds,
  settlementValues,
  expectedApprovals
}) {
  const selected = new Set(conditionIds.map((value) => String(value).toLowerCase()));
  const tokenIds = [...new Set(settlementValues.flatMap((settlement) =>
    settlement.token_ids || []
  ))].map(BigInt);
  const adapters = [...new Set(settlementValues.map((settlement) =>
    String(settlement.redemption_adapter_address || "")
  ))];
  if (!tokenIds.length || !adapters.length) {
    throw new Error("fail closed: recovered settlement token or adapter binding is unavailable");
  }
  if (adapters.some((adapter) =>
    typeof expectedApprovals?.[adapter.toLowerCase()] !== "boolean")) {
    throw new Error("fail closed: recovered settlement approval history is unavailable");
  }
  const deadline = Date.now() + 120_000;
  while (Date.now() < deadline) {
    const [positions, balance, openOrders, onchainLiquid, tokenBalances, approvals] =
      await Promise.all([
        fetchPositions(),
        clob.getBalanceAllowance({
          asset_type: AssetType.COLLATERAL,
          signature_type: config.signatureType
        }),
        clob.getOpenOrders(undefined, true),
        readPusdBalance(publicClient),
        Promise.all(tokenIds.map((assetId) =>
          readConditionalBalance(publicClient, assetId))),
        Promise.all(adapters.map((adapter) =>
          readAdapterApproval(publicClient, adapter)))
      ]);
    if (!Array.isArray(openOrders)) {
      throw new Error("fail closed: recovered redemption open-order response is not an array");
    }
    const remaining = positions.filter((row) =>
      row.redeemable === true &&
      Number(row.currentValue || 0) > 0 &&
      selected.has(String(row.conditionId || "").toLowerCase())
    );
    const liquidAfter = Number(formatUnits(BigInt(balance.balance || "0"), 6));
    if (!remaining.length &&
        Math.abs(liquidAfter - onchainLiquid) <= 0.000001 &&
        tokenBalances.every((value) => value === 0n) &&
        approvals.every((value, index) =>
          value === expectedApprovals[adapters[index].toLowerCase()]) &&
        openOrders.length === 0) {
      return {
        liquid_collateral_after: onchainLiquid,
        realized_payout: Math.round(settlementValues.reduce(
          (total, settlement) => total + Number(settlement.payout),
          0
        ) * 1_000_000) / 1_000_000,
        zero_open_orders_confirmed: true,
        conditional_token_balances_zero: true,
        approvals: Object.fromEntries(adapters.map((adapter, index) => [
          adapter.toLowerCase(),
          approvals[index]
        ])),
        portfolio: {
          ...summarizePortfolio(positions, onchainLiquid, config.startingCapital),
          captured_ts: new Date().toISOString()
        }
      };
    }
    await sleep(2_000);
  }
  throw new Error("fail closed: confirmed redemption recovery did not reconcile account state");
}

export function expectedRecoveredAdapterApprovals(receipt, wallet, adapters) {
  const expectedWallet = String(wallet || "").toLowerCase();
  const expectedAdapters = [...new Set((adapters || []).map((adapter) =>
    String(adapter || "").toLowerCase()
  ))];
  if (!Array.isArray(receipt?.logs) ||
      !/^0x[0-9a-f]{40}$/.test(expectedWallet) ||
      !expectedAdapters.length ||
      expectedAdapters.some((adapter) => !/^0x[0-9a-f]{40}$/.test(adapter))) {
    throw new Error("fail closed: recovered adapter approval binding is invalid");
  }
  const changes = new Map(expectedAdapters.map((adapter) => [adapter, []]));
  for (const log of receipt.logs) {
    if (String(log?.address || "").toLowerCase() !== CONDITIONAL_TOKENS.toLowerCase() ||
        String(log?.topics?.[0] || "").toLowerCase() !== APPROVAL_FOR_ALL_TOPIC) {
      continue;
    }
    const args = decodeEventLog({
      abi: APPROVAL_FOR_ALL_EVENT,
      data: log.data,
      topics: log.topics,
      strict: true
    }).args || {};
    if (String(args.account || "").toLowerCase() !== expectedWallet) continue;
    const operator = String(args.operator || "").toLowerCase();
    if (!changes.has(operator)) {
      throw new Error("fail closed: redemption changed an unexpected adapter approval");
    }
    changes.get(operator).push(args.approved === true);
  }
  return Object.fromEntries(expectedAdapters.map((adapter) => {
    const values = changes.get(adapter);
    if (!values.length) {
      // A successful adapter redemption with no ApprovalForAll event means
      // the wallet was already approved and the batch left it unchanged.
      return [adapter, true];
    }
    if (values.length === 2 && values[0] === true && values[1] === false) {
      return [adapter, false];
    }
    throw new Error("fail closed: redemption adapter approval sequence is invalid");
  }));
}

function baseSummary(status, geoblock, owner, liquidBefore, selection, approvals, calls, recentRedemptions = [], portfolio = null) {
  return {
    schema_version: 1,
    run_id: runId,
    status,
    finished_ts: new Date().toISOString(),
    execution_origin: config.executionOrigin,
    execution_country: geoblock.country,
    static_egress_verified: geoblock.ip === config.expectedEgressIp,
    dry_run: config.dryRun,
    redemption_enabled: config.enabled,
    owner,
    funder: config.funderAddress,
    wallet_type: "legacy_uups_deposit_wallet",
    derived_wallet_match: true,
    liquid_collateral_before: liquidBefore,
    selection,
    recent_redemptions: recentRedemptions,
    portfolio,
    approvals,
    planned_calls: calls.map((call) => ({
      purpose: call.purpose,
      target: call.target,
      condition_id: call.condition_id || null,
      negative_risk: call.negative_risk ?? null
    })),
    zero_open_orders_confirmed: true,
    redemption_submitted: false,
    research_only: !config.fundedServiceManaged,
    live_strategy_enabled: config.fundedServiceManaged
  };
}

async function adapterApprovals(publicClient, selection) {
  const targets = [...new Set(selection.selected.map((row) =>
    row.negative_risk ? NEG_RISK_CTF_COLLATERAL_ADAPTER : CTF_COLLATERAL_ADAPTER
  ))];
  const abi = [{
    type: "function",
    name: "isApprovedForAll",
    stateMutability: "view",
    inputs: [{ name: "account", type: "address" }, { name: "operator", type: "address" }],
    outputs: [{ name: "", type: "bool" }]
  }];
  const result = {};
  for (const target of targets) {
    const code = await publicClient.getCode({ address: target });
    if (!code || code === "0x") throw new Error(`fail closed: official collateral adapter is not deployed (${target})`);
    result[target.toLowerCase()] = await publicClient.readContract({
      address: CONDITIONAL_TOKENS,
      abi,
      functionName: "isApprovedForAll",
      args: [config.funderAddress, target]
    });
  }
  return result;
}

async function validateOnchainSelection(publicClient, selection) {
  const ctfAbi = [
    {
      type: "function", name: "getCollectionId", stateMutability: "view",
      inputs: [{ name: "parentCollectionId", type: "bytes32" }, { name: "conditionId", type: "bytes32" }, { name: "indexSet", type: "uint256" }],
      outputs: [{ name: "", type: "bytes32" }]
    },
    {
      type: "function", name: "getPositionId", stateMutability: "view",
      inputs: [{ name: "collateralToken", type: "address" }, { name: "collectionId", type: "bytes32" }],
      outputs: [{ name: "", type: "uint256" }]
    },
    {
      type: "function", name: "payoutDenominator", stateMutability: "view",
      inputs: [{ name: "conditionId", type: "bytes32" }], outputs: [{ name: "", type: "uint256" }]
    },
    {
      type: "function", name: "payoutNumerators", stateMutability: "view",
      inputs: [{ name: "conditionId", type: "bytes32" }, { name: "index", type: "uint256" }],
      outputs: [{ name: "", type: "uint256" }]
    },
    {
      type: "function", name: "balanceOf", stateMutability: "view",
      inputs: [{ name: "account", type: "address" }, { name: "id", type: "uint256" }],
      outputs: [{ name: "", type: "uint256" }]
    }
  ];
  const adapterAbi = [{
    type: "function", name: "WRAPPED_COLLATERAL", stateMutability: "view", inputs: [], outputs: [{ name: "", type: "address" }]
  }];
  const zero = `0x${"00".repeat(32)}`;
  const negRiskCollateral = selection.selected.some((row) => row.negative_risk)
    ? await publicClient.readContract({
        address: NEG_RISK_CTF_COLLATERAL_ADAPTER,
        abi: adapterAbi,
        functionName: "WRAPPED_COLLATERAL"
      })
    : null;
  const rows = [];
  let totalPayout = 0;
  for (const row of selection.selected) {
    if (!Array.isArray(row.assets) || row.assets.length !== 2 ||
        row.assets.some((asset, index) => asset.outcome_index !== index || !/^\d+$/.test(asset.asset))) {
      throw new Error(`fail closed: complete two-outcome asset metadata is required for ${row.condition_id}`);
    }
    const collateral = row.negative_risk ? negRiskCollateral : "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174";
    const collections = await Promise.all([1n, 2n].map((indexSet) => publicClient.readContract({
      address: CONDITIONAL_TOKENS,
      abi: ctfAbi,
      functionName: "getCollectionId",
      args: [zero, row.condition_id, indexSet]
    })));
    const derivedAssets = await Promise.all(collections.map((collectionId) => publicClient.readContract({
      address: CONDITIONAL_TOKENS,
      abi: ctfAbi,
      functionName: "getPositionId",
      args: [collateral, collectionId]
    })));
    if (derivedAssets.some((asset, index) => asset !== BigInt(row.assets[index].asset))) {
      throw new Error(`fail closed: Data API asset IDs do not match the selected onchain adapter for ${row.condition_id}`);
    }
    const [denominator, numerators, balances] = await Promise.all([
      publicClient.readContract({ address: CONDITIONAL_TOKENS, abi: ctfAbi, functionName: "payoutDenominator", args: [row.condition_id] }),
      Promise.all([0n, 1n].map((index) => publicClient.readContract({
        address: CONDITIONAL_TOKENS, abi: ctfAbi, functionName: "payoutNumerators", args: [row.condition_id, index]
      }))),
      Promise.all(derivedAssets.map((asset) => publicClient.readContract({
        address: CONDITIONAL_TOKENS, abi: ctfAbi, functionName: "balanceOf", args: [config.funderAddress, asset]
      })))
    ]);
    if (denominator === 0n) throw new Error(`fail closed: condition is not resolved onchain (${row.condition_id})`);
    const payoutBaseUnits = balances.reduce(
      (sum, balance, index) => sum + (balance * numerators[index]) / denominator,
      0n
    );
    const expectedPayout = Number(formatUnits(payoutBaseUnits, 6));
    if (!(expectedPayout > 0)) throw new Error(`fail closed: selected condition has no positive onchain payout (${row.condition_id})`);
    if (Math.abs(expectedPayout - row.gross_payout) > 0.01) {
      throw new Error(`fail closed: Data API payout disagrees with onchain payout for ${row.condition_id}`);
    }
    totalPayout += expectedPayout;
    rows.push({
      ...row,
      adapter: row.negative_risk ? NEG_RISK_CTF_COLLATERAL_ADAPTER : CTF_COLLATERAL_ADAPTER,
      underlying_collateral: collateral,
      asset_ids: derivedAssets.map(String),
      onchain_balances_base_units: balances.map(String),
      payout_numerators: numerators.map(String),
      payout_denominator: String(denominator),
      onchain_expected_payout: expectedPayout
    });
  }
  if (hasPayoutCap(config.maxPayout) && totalPayout > Number(config.maxPayout) + 1e-9) {
    throw new Error(`fail closed: exact onchain payout ${totalPayout} exceeds the configured redemption cap`);
  }
  return {
    ...selection,
    selected: rows,
    selected_gross_payout: Math.round(totalPayout * 1_000_000) / 1_000_000,
    payout_source: "onchain_balances_and_payout_vector"
  };
}

async function checkOrigin(stage) {
  const response = await fetch("https://polymarket.com/api/geoblock", {
    headers: { accept: "application/json" },
    signal: AbortSignal.timeout(15_000)
  });
  if (!response.ok) throw new Error(`fail closed: geoblock check returned HTTP ${response.status}`);
  const result = await response.json();
  assertEligibleOrigin(result, config);
  ledger.record("venue_redemption_origin_verified", { stage, country: result.country, region: result.region, ip: result.ip });
  return result;
}

async function fetchPositions() {
  const url = new URL("/positions", config.dataUrl);
  url.searchParams.set("user", config.funderAddress);
  url.searchParams.set("sizeThreshold", "0");
  url.searchParams.set("limit", "100");
  const response = await fetch(url, { headers: { accept: "application/json" }, signal: AbortSignal.timeout(20_000) });
  if (!response.ok) throw new Error(`Data API positions returned HTTP ${response.status}`);
  const positions = await response.json();
  if (!Array.isArray(positions)) throw new Error("Data API positions response is not an array");
  return positions;
}

async function fetchActivity() {
  const url = new URL("/activity", config.dataUrl);
  url.searchParams.set("user", config.funderAddress);
  url.searchParams.set("limit", "100");
  url.searchParams.set("offset", "0");
  const response = await fetch(url, { headers: { accept: "application/json" }, signal: AbortSignal.timeout(20_000) });
  if (!response.ok) throw new Error(`Data API activity returned HTTP ${response.status}`);
  const activity = await response.json();
  if (!Array.isArray(activity)) throw new Error("Data API activity response is not an array");
  return activity;
}

async function relayerJson(path, options = {}) {
  if (!path.startsWith("/")) throw new Error("relayer path must be relative");
  const response = await fetch(`${config.relayerUrl}${path}`, {
    ...options,
    headers: {
      accept: "application/json",
      "content-type": "application/json",
      RELAYER_API_KEY: config.relayerApiKey,
      RELAYER_API_KEY_ADDRESS: config.relayerApiKeyAddress,
      ...(options.headers || {})
    },
    signal: AbortSignal.timeout(20_000),
    redirect: "error"
  });
  const body = await response.text();
  let value;
  try { value = body ? JSON.parse(body) : null; } catch { value = null; }
  if (!response.ok) {
    const detail = safeRelayerErrorDetail(value ?? body, [
      config.relayerApiKey,
      config.apiSecret,
      config.apiPassphrase,
      config.privateKey
    ]);
    const error = new Error(
      `relayer ${path.split("?")[0]} returned HTTP ${response.status}` +
      (detail ? ` (${detail})` : "")
    );
    error.relayerHttpStatus = response.status;
    error.relayerSafeDetail = detail;
    throw error;
  }
  return value;
}

export function safeRelayerErrorDetail(value, secrets = []) {
  let detail = "";
  if (typeof value === "string") {
    detail = value;
  } else if (value && typeof value === "object") {
    for (const key of ["code", "error", "message", "reason", "detail"]) {
      if (typeof value[key] === "string" && value[key].trim()) {
        detail = `${key}=${value[key]}`;
        break;
      }
    }
  }
  detail = detail.replace(/[\u0000-\u001f\u007f]+/g, " ").trim();
  for (const secret of secrets) {
    if (typeof secret === "string" && secret.length >= 8) {
      detail = detail.split(secret).join("[redacted]");
    }
  }
  return detail
    .replace(/0x[0-9a-fA-F]{64,}/g, "[hex-redacted]")
    .replace(/[A-Za-z0-9+/=_-]{48,}/g, "[token-redacted]")
    .slice(0, 500);
}

async function validateRelayerCredential() {
  const rows = await relayerJson("/relayer/api/keys");
  if (!Array.isArray(rows)) throw new Error("fail closed: relayer API key inventory response is not an array");
  const match = rows.some((row) =>
    String(row?.apiKey || "") === config.relayerApiKey &&
    String(row?.address || "").toLowerCase() === config.relayerApiKeyAddress.toLowerCase()
  );
  if (!match) throw new Error("fail closed: relayer API key/address pair was not confirmed by the venue");
  ledger.record("venue_redemption_relayer_auth_validated", { address: config.relayerApiKeyAddress });
}

async function waitForRelayerConfirmation(transactionId) {
  const deadline = Date.now() + 180_000;
  while (Date.now() < deadline) {
    const rows = await relayerJson(`/transaction?id=${encodeURIComponent(transactionId)}`);
    const transaction = Array.isArray(rows) ? rows[0] : rows;
    const state = String(transaction?.state || "");
    ledger.record("venue_redemption_relayer_state", { transaction_id: transactionId, state });
    if (state === "STATE_CONFIRMED") return transaction;
    if (["STATE_FAILED", "STATE_INVALID"].includes(state)) return null;
    await sleep(2_000);
  }
  throw new Error("fail closed: relayer confirmation timed out");
}

async function waitForSettlementVerification(clob, publicClient, selection, initialApprovals, liquidBefore) {
  const selected = new Set(selection.selected.map((row) => row.condition_id.toLowerCase()));
  const assetIds = selection.selected.flatMap((row) => row.asset_ids || []).map(BigInt);
  if (assetIds.length !== selection.selected.length * 2) {
    throw new Error("fail closed: complete onchain asset IDs are required for settlement verification");
  }
  const requiredAdapters = [...new Set(selection.selected.map((row) =>
    row.negative_risk ? NEG_RISK_CTF_COLLATERAL_ADAPTER : CTF_COLLATERAL_ADAPTER
  ))];
  const deadline = Date.now() + 120_000;
  while (Date.now() < deadline) {
    const [positions, balance, openOrders, onchainLiquid, tokenBalances, approvals] = await Promise.all([
      fetchPositions(),
      clob.getBalanceAllowance({ asset_type: AssetType.COLLATERAL, signature_type: config.signatureType }),
      clob.getOpenOrders(undefined, true),
      readPusdBalance(publicClient),
      Promise.all(assetIds.map((assetId) => readConditionalBalance(publicClient, assetId))),
      Promise.all(requiredAdapters.map((adapter) => readAdapterApproval(publicClient, adapter)))
    ]);
    if (!Array.isArray(openOrders)) throw new Error("fail closed: post-redemption open-order response is not an array");
    const remaining = positions.filter((row) =>
      row.redeemable === true && Number(row.currentValue || 0) > 0 && selected.has(String(row.conditionId || "").toLowerCase())
    );
    const liquidAfter = Number(formatUnits(BigInt(balance.balance || "0"), 6));
    const payoutObserved = onchainLiquid + 1e-6 >= liquidBefore + selection.selected_gross_payout;
    if (!remaining.length && payoutObserved && Math.abs(liquidAfter - onchainLiquid) <= 0.000001 &&
        tokenBalances.every((value) => value === 0n) &&
        approvals.every((value, index) => value === initialApprovals[requiredAdapters[index].toLowerCase()]) &&
        openOrders.length === 0) {
      return {
        liquid_collateral_after: onchainLiquid,
        clob_liquid_collateral_after: liquidAfter,
        realized_payout: Math.round((onchainLiquid - liquidBefore) * 1_000_000) / 1_000_000,
        zero_open_orders_confirmed: true,
        conditional_token_balances_zero: true,
        adapter_approvals_restored: true,
        redeemed_condition_count: selection.selected.length,
        portfolio: {
          ...summarizePortfolio(positions, onchainLiquid, config.startingCapital),
          captured_ts: new Date().toISOString()
        }
      };
    }
    await sleep(2_000);
  }
  throw new Error("fail closed: confirmed relayer redemption did not reconcile in Data API/CLOB before timeout");
}

async function waitForAutomaticSettlementEvidence({
  clob,
  transactionHash,
  receipt,
  reservationRecords,
  conditionIds
}) {
  const container = storageContainer(config);
  const manifest = JSON.parse(config.fundedSessionManifestJson);
  const expectedTransactionHash = String(transactionHash).toLowerCase();
  const expectedConditions = new Set((conditionIds || []).map((value) =>
    String(value).toLowerCase()
  ));
  if (!/^0x[0-9a-f]{64}$/.test(expectedTransactionHash) ||
      !expectedConditions.size ||
      [...expectedConditions].some((value) => !/^0x[0-9a-f]{64}$/.test(value))) {
    throw new Error("fail closed: automatic settlement control binding is invalid");
  }
  const deadline = Date.now() + 120_000;
  let lastError = null;
  while (Date.now() < deadline) {
    try {
      const durableSettlements = await loadDurableInternalSettlements(
        container,
        config.campaignId
      );
      const settlements = await discoverVerifiedAutomaticInternalSettlements({
        manifest,
        reservations: reservationRecords.map((record) => record.reservation),
        activity: (await fetchActivity()).filter((row) =>
          String(row?.type || "").toUpperCase() !== "REDEEM" ||
          (
            String(row?.transactionHash || "").toLowerCase() === expectedTransactionHash &&
            expectedConditions.has(String(row?.conditionId || "").toLowerCase())
          )
        ),
        durableSettlements,
        expectedWallet: config.funderAddress,
        getOrderFills: async (reservation) => {
          const trades = await clob.getTrades({ market: reservation.condition_id });
          if (!Array.isArray(trades)) {
            throw new Error("fail closed: authenticated CLOB trade history is invalid");
          }
          return tradeFillsFromRest(trades, reservation.order_id);
        },
        getTransactionReceipt: async (hash) => {
          if (String(hash).toLowerCase() !== expectedTransactionHash) {
            throw new Error("fail closed: automatic settlement receipt hash is not the submitted redemption");
          }
          return automaticSettlementReceiptEvidence(receipt, transactionHash);
        }
      });
      if (settlements.length) return settlements;
      lastError = new Error("automatic settlement activity is not visible yet");
    } catch (error) {
      lastError = error;
    }
    await sleep(2_000);
  }
  throw new Error(`fail closed: automatic settlement evidence did not reconcile (${lastError?.message || "unknown"})`);
}

export function automaticSettlementReceiptEvidence(
  receipt,
  transactionHash,
  confirmations = 2
) {
  const decoded = decodeSettlementReceiptEvidence(receipt, transactionHash);
  const blockNumber = BigInt(receipt?.blockNumber ?? 0);
  const normalizedHash = String(transactionHash || "").toLowerCase();
  if (receipt?.status !== "success" ||
      !/^0x[0-9a-f]{64}$/.test(normalizedHash) ||
      blockNumber <= 0n ||
      !Number.isInteger(Number(confirmations)) ||
      Number(confirmations) < 2) {
    throw new Error("fail closed: automatic settlement receipt confirmation is invalid");
  }
  return {
    status: "success",
    chain_id: polygon.id,
    transaction_hash: normalizedHash,
    block_number: blockNumber.toString(),
    confirmations: Number(confirmations),
    ...decoded
  };
}

async function readPusdBalance(publicClient) {
  const value = await publicClient.readContract({
    address: PUSD,
    abi: [{
      type: "function",
      name: "balanceOf",
      stateMutability: "view",
      inputs: [{ name: "account", type: "address" }],
      outputs: [{ name: "", type: "uint256" }]
    }],
    functionName: "balanceOf",
    args: [config.funderAddress]
  });
  return Number(formatUnits(value, 6));
}

async function readConditionalBalance(publicClient, assetId) {
  return publicClient.readContract({
    address: CONDITIONAL_TOKENS,
    abi: [{
      type: "function",
      name: "balanceOf",
      stateMutability: "view",
      inputs: [{ name: "account", type: "address" }, { name: "id", type: "uint256" }],
      outputs: [{ name: "", type: "uint256" }]
    }],
    functionName: "balanceOf",
    args: [config.funderAddress, assetId]
  });
}

async function readAdapterApproval(publicClient, adapter) {
  return publicClient.readContract({
    address: CONDITIONAL_TOKENS,
    abi: [{
      type: "function",
      name: "isApprovedForAll",
      stateMutability: "view",
      inputs: [{ name: "account", type: "address" }, { name: "operator", type: "address" }],
      outputs: [{ name: "", type: "bool" }]
    }],
    functionName: "isApprovedForAll",
    args: [config.funderAddress, adapter]
  });
}

async function readRedemptionControl() {
  const container = storageContainer(config);
  const blob = container.getBlobClient(redemptionControlBlobName());
  try {
    const response = await blob.download();
    return JSON.parse(await streamToString(response.readableStreamBody));
  } catch (error) {
    if (Number(error.statusCode) === 404) return null;
    throw error;
  }
}

async function readRedemptionEvidenceForControl(control) {
  const createdMs = Date.parse(String(control?.created_ts || ""));
  if (!control?.run_id || !Number.isFinite(createdMs)) return null;
  const date = new Date(createdMs).toISOString().slice(0, 10);
  const container = storageContainer(config);
  const blob = container.getBlobClient(
    `${redemptionEvidencePrefix()}/redemptions/${date}/${control.run_id}.json`
  );
  try {
    const response = await blob.download();
    return JSON.parse(await streamToString(response.readableStreamBody));
  } catch (error) {
    if (Number(error.statusCode) === 404) return null;
    throw error;
  }
}

async function reconcilePriorRejectedSubmission(control, selection) {
  if (!control || control.state !== "submission_attempted" || control.transaction_id) {
    return control;
  }
  const evidence = await readRedemptionEvidenceForControl(control);
  if (!rejectedRelayerSubmissionMatches(control, evidence, selection)) {
    return control;
  }
  const reconciled = {
    ...control,
    state: "relayer_rejected",
    relayer_http_status: Number(
      String(evidence.error).match(/HTTP (4\d\d)/)?.[1]
    ),
    relayer_error_detail: "reconciled_from_immutable_failed_submission_evidence",
    updated_ts: new Date().toISOString()
  };
  await writeRedemptionControl(reconciled);
  ledger.record("venue_redemption_prior_rejection_reconciled", {
    run_id: control.run_id,
    condition_ids: control.condition_ids,
    relayer_http_status: reconciled.relayer_http_status
  });
  return reconciled;
}

export function rejectedRelayerSubmissionMatches(control, evidence, selection) {
  if (control?.state !== "submission_attempted" ||
      control?.transaction_id ||
      evidence?.run_id !== control.run_id ||
      evidence?.status !== "failed_closed" ||
      evidence?.redemption_submitted !== true ||
      !/^relayer \/submit returned HTTP 4\d\d(?:\s|\(|$)/.test(String(evidence?.error || ""))) {
    return false;
  }
  const expectedConditions = [...new Set(
    (control.condition_ids || []).map((value) => String(value).toLowerCase())
  )].sort();
  const currentConditions = [...new Set(
    (selection?.selected || []).map((value) => String(value.condition_id).toLowerCase())
  )].sort();
  return expectedConditions.length > 0 &&
    JSON.stringify(expectedConditions) === JSON.stringify(currentConditions) &&
    Math.abs(
      Number(control.expected_gross_payout) -
      Number(selection?.selected_gross_payout)
    ) <= 0.000001;
}

async function writeRedemptionControl(value) {
  const container = storageContainer(config);
  await container.getBlockBlobClient(redemptionControlBlobName()).uploadData(
    Buffer.from(JSON.stringify(sanitize(value), null, 2)),
    { blobHTTPHeaders: { blobContentType: "application/json" } }
  );
}

async function uploadRedemptionEvidence(value) {
  const container = storageContainer(config);
  await container.createIfNotExists();
  const payload = Buffer.from(JSON.stringify(sanitize(value), null, 2));
  const date = new Date().toISOString().slice(0, 10);
  const prefix = redemptionEvidencePrefix();
  await container.getBlockBlobClient(`${prefix}/redemptions/${date}/${runId}.json`).uploadData(payload, {
    conditions: { ifNoneMatch: "*" },
    blobHTTPHeaders: { blobContentType: "application/json" }
  });
  await container.getBlockBlobClient(`${prefix}/latest-redemption.json`).uploadData(payload, {
    blobHTTPHeaders: { blobContentType: "application/json" }
  });
}

function normalizePrivateKey(value) {
  const trimmed = String(value || "").trim();
  return trimmed.startsWith("0x") ? trimmed : `0x${trimmed}`;
}

function redemptionEvidencePrefix() {
  return config.fundedServiceManaged
    ? `reports/funded/dynamic-quote/sessions/${config.campaignId}`
    : "reports/research/venue-probe";
}

function redemptionControlBlobName() {
  return `${redemptionEvidencePrefix()}/control/redemption-state.json`;
}

async function streamToString(stream) {
  const chunks = [];
  for await (const chunk of stream) chunks.push(Buffer.from(chunk));
  return Buffer.concat(chunks).toString("utf8");
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

const isMain = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isMain) {
  runVenueRedemption().catch((error) => {
    process.exitCode = 1;
    console.error(JSON.stringify(sanitize({
      status: "failed_closed",
      error: error.message
    })));
  });
}
