import test from "node:test";
import assert from "node:assert/strict";
import { verifyTypedData } from "viem";
import { privateKeyToAccount } from "viem/accounts";
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

test("only positive redeemable condition payouts are selected once and within the cap", () => {
  const selection = selectRedeemableConditions([
    { conditionId: conditionA, redeemable: true, currentValue: 5, negativeRisk: false, title: "A", asset: "101", oppositeAsset: "102", outcomeIndex: 0 },
    { conditionId: conditionA, redeemable: true, currentValue: 0, negativeRisk: false, title: "A", asset: "102", oppositeAsset: "101", outcomeIndex: 1 },
    { conditionId: conditionB, redeemable: true, currentValue: 30, negativeRisk: true, title: "B" },
    { conditionId: `0x${"33".repeat(32)}`, redeemable: false, currentValue: 4, negativeRisk: false }
  ], 25, 5);
  assert.equal(selection.selected.length, 1);
  assert.equal(selection.selected[0].condition_id, conditionA);
  assert.equal(selection.selected_gross_payout, 5);
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
