import test from "node:test";
import assert from "node:assert/strict";
import {
  encodeAbiParameters,
  encodeEventTopics,
  verifyTypedData
} from "viem";
import { privateKeyToAccount } from "viem/accounts";
import {
  RELAYER_DEADLINE_BUFFER_SECONDS,
  confirmedRedemptionControlMatches,
  expectedRecoveredAdapterApprovals,
  rejectedRelayerSubmissionMatches,
  safeRelayerErrorDetail
} from "../src/redeem.mjs";
import {
  CONDITIONAL_TOKENS,
  CTF_COLLATERAL_ADAPTER,
  NEG_RISK_CTF_COLLATERAL_ADAPTER,
  buildRedemptionCalls,
  depositWalletRequest,
  depositWalletTypedData,
  deriveLegacyUupsDepositWallet,
  loadRedemptionConfig,
  selectRedeemableConditions,
  summarizeRecentRedemptions
} from "../src/redemption.mjs";

const owner = "0xc9f6f0D01e5eEf2446819Ce21C4f1F9b688A9921";
const funder = "0x3d701b05d7c36aFaB01a06Fd26eBe789c0B7baD8";
const conditionA = `0x${"11".repeat(32)}`;
const conditionB = `0x${"22".repeat(32)}`;

test("redemption uses the current Polymarket collateral adapters", () => {
  assert.equal(CTF_COLLATERAL_ADAPTER, "0xAdA100Db00Ca00073811820692005400218FcE1f");
  assert.equal(NEG_RISK_CTF_COLLATERAL_ADAPTER, "0xadA2005600Dec949baf300f4C6120000bDB6eAab");
});

test("redemption uses the documented ten-minute relayer deadline buffer", () => {
  assert.equal(RELAYER_DEADLINE_BUFFER_SECONDS, 600);
});

const safeEnv = {
  EXECUTION_MODE: "venue_redemption",
  ALLOW_LIVE: "false",
  ENABLE_TAKER_ORDERS: "false",
  VENUE_REDEMPTION_DRY_RUN: "true",
  POLYMARKET_SIGNATURE_TYPE: "3",
  POLYMARKET_PRIVATE_KEY: "key",
  POLYMARKET_FUNDER_ADDRESS: funder,
  POLYMARKET_API_KEY: "api",
  POLYMARKET_API_SECRET: "secret",
  POLYMARKET_API_PASSPHRASE: "pass",
  AZURE_STORAGE_ACCOUNT_NAME: "storage"
};

test("known email-login signer derives the funded legacy UUPS deposit wallet", () => {
  assert.equal(deriveLegacyUupsDepositWallet(owner), funder);
});

test("redemption defaults to disabled dry-run and requires separate relayer auth for live submission", () => {
  const config = loadRedemptionConfig(safeEnv);
  assert.equal(config.enabled, false);
  assert.equal(config.dryRun, true);
  assert.throws(() => loadRedemptionConfig({ ...safeEnv, VENUE_REDEMPTION_DRY_RUN: "false" }), /TRUST_BOUNDARY_READY.*ENABLED.*EXPECTED_COUNTRY.*EXPECTED_EGRESS_IP.*RELAYER_API_KEY/s);
  assert.throws(() => loadRedemptionConfig({
    ...safeEnv,
    VENUE_REDEMPTION_ENABLED: "true",
    VENUE_REDEMPTION_DRY_RUN: "false",
    VENUE_PROBE_EXPECTED_COUNTRY: "IE",
    VENUE_PROBE_EXPECTED_EGRESS_IP: "203.0.113.8",
    POLYMARKET_RELAYER_API_KEY: "relayer-key",
    POLYMARKET_RELAYER_API_KEY_ADDRESS: owner
  }), /FUNDED_EVIDENCE_TRUST_BOUNDARY_READY/);
  const live = loadRedemptionConfig({
    ...safeEnv,
    VENUE_REDEMPTION_ENABLED: "true",
    VENUE_REDEMPTION_DRY_RUN: "false",
    FUNDED_EVIDENCE_TRUST_BOUNDARY_READY: "true",
    VENUE_PROBE_EXPECTED_COUNTRY: "IE",
    VENUE_PROBE_EXPECTED_EGRESS_IP: "203.0.113.8",
    POLYMARKET_RELAYER_API_KEY: "relayer-key",
    POLYMARKET_RELAYER_API_KEY_ADDRESS: owner
  });
  assert.equal(live.enabled, true);
  assert.equal(live.dryRun, false);
});

test("a rejected relayer submission is retryable only from exact immutable and onchain evidence", () => {
  const control = {
    state: "submission_attempted",
    run_id: "venue-redemption-rejected",
    transaction_id: null,
    condition_ids: [conditionA],
    expected_gross_payout: 11.12
  };
  const evidence = {
    run_id: control.run_id,
    status: "failed_closed",
    error: "relayer /submit returned HTTP 400",
    redemption_submitted: true
  };
  const selection = {
    selected: [{ condition_id: conditionA }],
    selected_gross_payout: 11.12
  };
  assert.equal(rejectedRelayerSubmissionMatches(control, evidence, selection), true);
  assert.equal(rejectedRelayerSubmissionMatches(
    control,
    { ...evidence, error: "relayer confirmation timed out" },
    selection
  ), false);
  assert.equal(rejectedRelayerSubmissionMatches(
    control,
    evidence,
    { ...selection, selected: [{ condition_id: conditionB }] }
  ), false);
  assert.equal(rejectedRelayerSubmissionMatches(
    control,
    evidence,
    { ...selection, selected_gross_payout: 11.11 }
  ), false);
});

test("only an exact confirmed redemption control can enter no-resubmit recovery", () => {
  const control = {
    schema_version: 1,
    state: "confirmed_pending_verification",
    run_id: "venue-redemption-20260731072438297-c7ba96ce",
    owner,
    signer_address: owner,
    funder,
    condition_ids: [conditionA],
    expected_gross_payout: 11.12,
    submission_attempted: true,
    transaction_id: "relayer-transaction-1",
    transaction_hash: `0x${"ab".repeat(32)}`,
    created_ts: "2026-07-31T07:24:38.297Z",
    updated_ts: "2026-07-31T07:26:59.000Z"
  };
  const binding = { owner, funder, maxPayout: 25, maxConditions: 1 };
  assert.equal(confirmedRedemptionControlMatches(control, binding), true);
  assert.equal(confirmedRedemptionControlMatches({
    ...control,
    transaction_hash: null
  }, binding), false);
  assert.equal(confirmedRedemptionControlMatches({
    ...control,
    condition_ids: [conditionA, conditionA]
  }, binding), false);
  assert.equal(confirmedRedemptionControlMatches({
    ...control,
    condition_ids: [conditionA, conditionB]
  }, binding), false);
  assert.equal(confirmedRedemptionControlMatches({
    ...control,
    expected_gross_payout: 25.01
  }, binding), false);
  assert.equal(confirmedRedemptionControlMatches({
    ...control,
    funder: owner
  }, binding), false);
  assert.equal(confirmedRedemptionControlMatches({
    ...control,
    signer_address: funder
  }, binding), false);
  const legacySanitized = {
    ...control,
    owner: "[REDACTED]"
  };
  delete legacySanitized.signer_address;
  assert.equal(
    confirmedRedemptionControlMatches(legacySanitized, binding),
    true
  );
  assert.equal(confirmedRedemptionControlMatches({
    ...legacySanitized,
    schema_version: 2
  }, binding), false);
});

test("confirmed recovery preserves preapproval or proves temporary grant and revoke", () => {
  const approvalEvent = [{
    type: "event",
    name: "ApprovalForAll",
    anonymous: false,
    inputs: [
      { name: "account", type: "address", indexed: true },
      { name: "operator", type: "address", indexed: true },
      { name: "approved", type: "bool", indexed: false }
    ]
  }];
  const log = (operator, approved) => ({
    address: CONDITIONAL_TOKENS,
    topics: encodeEventTopics({
      abi: approvalEvent,
      eventName: "ApprovalForAll",
      args: { account: funder, operator }
    }),
    data: encodeAbiParameters([{ type: "bool" }], [approved])
  });
  assert.deepEqual(
    expectedRecoveredAdapterApprovals(
      { logs: [] },
      funder,
      [CTF_COLLATERAL_ADAPTER]
    ),
    { [CTF_COLLATERAL_ADAPTER.toLowerCase()]: true }
  );
  assert.deepEqual(
    expectedRecoveredAdapterApprovals({
      logs: [
        log(CTF_COLLATERAL_ADAPTER, true),
        log(CTF_COLLATERAL_ADAPTER, false)
      ]
    }, funder, [CTF_COLLATERAL_ADAPTER]),
    { [CTF_COLLATERAL_ADAPTER.toLowerCase()]: false }
  );
  assert.throws(() => expectedRecoveredAdapterApprovals({
    logs: [log(CTF_COLLATERAL_ADAPTER, true)]
  }, funder, [CTF_COLLATERAL_ADAPTER]), /approval sequence is invalid/);
  assert.throws(() => expectedRecoveredAdapterApprovals({
    logs: [log(NEG_RISK_CTF_COLLATERAL_ADAPTER, true)]
  }, funder, [CTF_COLLATERAL_ADAPTER]), /unexpected adapter approval/);
});

test("relayer error details retain bounded diagnostics without secrets or signed payloads", () => {
  const secret = "relayer-secret-value";
  const detail = safeRelayerErrorDetail({
    message: `invalid batch ${secret} 0x${"ab".repeat(65)}`
  }, [secret]);
  assert.equal(detail, "message=invalid batch [redacted] [hex-redacted]");
  assert.equal(safeRelayerErrorDetail({ request: "must not be retained" }), "");
  assert.equal(safeRelayerErrorDetail("x".repeat(700)), "[token-redacted]");
});

test("funded-service redemption is bound to the Chile protected-compounding session", () => {
  const session = {
    session_id: "dynamic-quote-funded-2026-07-29-v5",
    allow_compounding: true,
    no_deposits: true,
    allow_automatic_replenishment: false,
    target_order_notional: 10.5,
    max_order_notional: 10.5
  };
  const funded = loadRedemptionConfig({
    ...safeEnv,
    FUNDED_DIRECT_AUTO_REDEMPTION_ENABLED: "true",
    FUNDED_DIRECT_SESSION_MANIFEST_JSON: JSON.stringify(session),
    VENUE_PROBE_FUNDED_CAMPAIGN_ID: session.session_id,
    VENUE_PROBE_EXECUTION_ORIGIN: "azure_chile_central_static_egress",
    VENUE_REDEMPTION_MAX_CONDITIONS: "1"
  });
  assert.equal(funded.fundedServiceManaged, true);
  assert.equal(funded.executionOrigin, "azure_chile_central_static_egress");
  assert.equal(funded.maxOrderNotional, 10.5);
  assert.throws(() => loadRedemptionConfig({
    ...safeEnv,
    FUNDED_DIRECT_AUTO_REDEMPTION_ENABLED: "true",
    FUNDED_DIRECT_SESSION_MANIFEST_JSON: JSON.stringify(session),
    VENUE_PROBE_FUNDED_CAMPAIGN_ID: "other-session",
    VENUE_PROBE_EXECUTION_ORIGIN: "azure_chile_central_static_egress",
    VENUE_REDEMPTION_MAX_CONDITIONS: "1"
  }), /must match the funded operator session/);
  assert.throws(() => loadRedemptionConfig({
    ...safeEnv,
    FUNDED_DIRECT_AUTO_REDEMPTION_ENABLED: "true",
    FUNDED_DIRECT_SESSION_MANIFEST_JSON: JSON.stringify(session),
    VENUE_PROBE_FUNDED_CAMPAIGN_ID: session.session_id,
    VENUE_PROBE_EXECUTION_ORIGIN: "azure_north_europe_static_egress",
    VENUE_REDEMPTION_MAX_CONDITIONS: "1"
  }), /Azure Chile Central/);
  assert.throws(() => loadRedemptionConfig({
    ...safeEnv,
    FUNDED_DIRECT_AUTO_REDEMPTION_ENABLED: "true",
    FUNDED_DIRECT_SESSION_MANIFEST_JSON: JSON.stringify({
      ...session,
      target_order_notional: 1
    }),
    VENUE_PROBE_FUNDED_CAMPAIGN_ID: session.session_id,
    VENUE_PROBE_EXECUTION_ORIGIN: "azure_chile_central_static_egress",
    VENUE_REDEMPTION_MAX_CONDITIONS: "1"
  }), /target\/max order notional/);
});

test("only positive redeemable condition payouts are selected once and within the cap", () => {
  const selection = selectRedeemableConditions([
    { conditionId: conditionA, redeemable: true, currentValue: 5, initialValue: 3, negativeRisk: false, title: "A", asset: "101", oppositeAsset: "102", outcomeIndex: 0 },
    { conditionId: conditionA, redeemable: true, currentValue: 0, initialValue: 1, negativeRisk: false, title: "A", asset: "102", oppositeAsset: "101", outcomeIndex: 1 },
    { conditionId: conditionB, redeemable: true, currentValue: 30, negativeRisk: true, title: "B" },
    { conditionId: `0x${"33".repeat(32)}`, redeemable: false, currentValue: 4, negativeRisk: false }
  ], 25, 5);
  assert.equal(selection.selected.length, 1);
  assert.equal(selection.selected[0].condition_id, conditionA);
  assert.equal(selection.selected_gross_payout, 5);
  assert.equal(selection.selected[0].principal, 4);
  assert.equal(selection.skipped_winner_conditions, 1);
  assert.deepEqual(selection.selected[0].assets, [
    { asset: "101", outcome_index: 0 },
    { asset: "102", outcome_index: 1 }
  ]);
});

test("recent redemption activity is attributed only to a matching durable worker control record", () => {
  const transactionHash = `0x${"ab".repeat(32)}`;
  const rows = summarizeRecentRedemptions([
    { type: "TRADE", timestamp: 200, usdcSize: 99 },
    { type: "REDEEM", timestamp: 100, usdcSize: 5, transactionHash, conditionId: `0x${"cd".repeat(32)}`, title: "winner" }
  ]);
  assert.equal(rows.length, 1);
  assert.equal(rows[0].gross_payout, 5);
  assert.equal(rows[0].attribution, "external_or_manual");
  assert.equal(rows[0].redeemed_ts, "1970-01-01T00:01:40.000Z");

  const controlled = summarizeRecentRedemptions([
    { type: "REDEEM", timestamp: 100, usdcSize: 5, transactionHash }
  ], { transaction_hash: transactionHash.toUpperCase().replace("0X", "0x") });
  assert.equal(controlled[0].attribution, "azure_redemption_worker");
});

test("call plan grants only official adapter approval then redeems standard and neg-risk conditions", () => {
  const selection = {
    selected: [
      { condition_id: conditionA, negative_risk: false },
      { condition_id: conditionB, negative_risk: true }
    ]
  };
  const calls = buildRedemptionCalls(selection, {
    [CTF_COLLATERAL_ADAPTER.toLowerCase()]: false,
    [NEG_RISK_CTF_COLLATERAL_ADAPTER.toLowerCase()]: true
  });
  assert.equal(calls.length, 4);
  assert.equal(calls[0].target, CONDITIONAL_TOKENS);
  assert.equal(calls[0].adapter, CTF_COLLATERAL_ADAPTER);
  assert.equal(calls[1].target, CTF_COLLATERAL_ADAPTER);
  assert.equal(calls[2].target, NEG_RISK_CTF_COLLATERAL_ADAPTER);
  assert.equal(calls[3].target, CONDITIONAL_TOKENS);
  assert.equal(calls[3].purpose, "revoke_official_collateral_adapter");
});

test("deposit wallet batch uses the documented EIP-712 domain and WALLET wire type", () => {
  const calls = buildRedemptionCalls({ selected: [{ condition_id: conditionA, negative_risk: false }] }, {
    [CTF_COLLATERAL_ADAPTER.toLowerCase()]: true
  });
  const typed = depositWalletTypedData(funder, "7", "123456", calls);
  assert.equal(typed.domain.name, "DepositWallet");
  assert.equal(typed.domain.version, "1");
  assert.equal(typed.domain.verifyingContract, funder);
  assert.equal(typed.primaryType, "Batch");
  const request = depositWalletRequest(owner, funder, "7", "123456", calls, "0xsignature");
  assert.equal(request.type, "WALLET");
  assert.equal(request.from, owner);
  assert.equal(request.depositWalletParams.depositWallet, funder);
  assert.equal(request.depositWalletParams.calls.length, 1);
});

test("documented deposit wallet batch produces a recoverable 65-byte owner signature", async () => {
  const account = privateKeyToAccount(`0x${"01".repeat(32)}`);
  const calls = buildRedemptionCalls({ selected: [{ condition_id: conditionA, negative_risk: false }] }, {
    [CTF_COLLATERAL_ADAPTER.toLowerCase()]: true
  });
  const typed = depositWalletTypedData(funder, "3", "2000000000", calls);
  const signature = await account.signTypedData(typed);
  assert.match(signature, /^0x[0-9a-f]{130}$/);
  assert.equal(await verifyTypedData({ address: account.address, ...typed, signature }), true);
});
