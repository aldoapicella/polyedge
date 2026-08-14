import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import {
  runFundedTerminalSettlement,
  terminalSettlementConfig,
  validateTerminalSettlementFederatedTokenFile
} from "../src/funded-terminal-settlement.mjs";

const decisionId = "a".repeat(64);
const conditionId = `0x${"b".repeat(64)}`;
const blobName = `reports/research/venue-probe/risk-reservations/2026-08-12/funded-direct-${decisionId}.json`;
const env = {
  FUNDED_TERMINAL_SETTLEMENT_ENABLED: "true",
  FUNDED_TERMINAL_SETTLEMENT_DECISION_ID: decisionId,
  FUNDED_TERMINAL_SETTLEMENT_RESERVATION_BLOB_NAME: blobName,
  FUNDED_TERMINAL_SETTLEMENT_OUTCOME: "Down",
  VENUE_PROBE_FUNDED_CAMPAIGN_ID: "dynamic-quote-funded-2026-07-29-v5",
  POLYMARKET_FUNDER_ADDRESS: "0x3d701b05d7c36afab01a06fd26ebe789c0b7bad8",
  AZURE_TENANT_ID: "9767f0dc-e83f-4cc1-94e1-0d5f9d287d32",
  AZURE_STORAGE_ACCOUNT_NAME: "stpolyedge6urdjr5nmwx7w",
  AZURE_STORAGE_CONTAINER_NAME: "polyedge-funded-evidence",
  AZURE_CLIENT_ID: "d9ce9154-66a6-4bdb-839f-0da7b02b38da",
  AZURE_TOKEN_CREDENTIALS: "WorkloadIdentityCredential"
};
const record = {
  blob_name: blobName,
  etag: "etag",
  reservation: {
    schema_version: 1,
    evidence_protocol_version: 3,
    state: "position_unresolved",
    campaign_id: env.VENUE_PROBE_FUNDED_CAMPAIGN_ID,
    probe_id: `funded-direct-${decisionId}`,
    token_id: "123",
    order_submission_intended: true,
    order_submitted: true,
    order_id: `0x${"c".repeat(64)}`,
    matched_notional: 2.15,
    reconciliation_complete: true,
    zero_open_orders_confirmed: true,
    condition_id: conditionId
  }
};

test("settlement-only command requires exact terminal Data API and durable zero-open evidence", async () => {
  const calls = [];
  let positionUrl;
  const proof = await runFundedTerminalSettlement({
    env,
    containerFactory: () => ({}),
    loadUnresolved: async () => calls.length ? [] : [record],
    fetchImpl: async (url) => {
      positionUrl = new URL(url);
      return { ok: true, json: async () => [{
        conditionId, asset: record.reservation.token_id, proxyWallet: env.POLYMARKET_FUNDER_ADDRESS,
        outcome: env.FUNDED_TERMINAL_SETTLEMENT_OUTCOME, redeemable: true, size: 5, currentValue: 0
      }] };
    },
    settle: async (config, settlement, options) => {
      calls.push({ config, settlement, options });
      return 1;
    },
    logger: () => {}
  });
  assert.equal(proof.status, "position_settled");
  assert.equal(proof.order_submission_attempted, false);
  assert.equal(calls.length, 1);
  assert.deepEqual(calls[0].settlement.condition_ids, [conditionId]);
  assert.equal(calls[0].settlement.evidence_source, "polymarket_data_api_redeemable");
  assert.equal(positionUrl.searchParams.get("market"), conditionId);
  assert.equal(positionUrl.searchParams.get("redeemable"), "true");
});

test("nonterminal Data API evidence fails closed before settlement", async () => {
  let settled = false;
  await assert.rejects(runFundedTerminalSettlement({
    env,
    containerFactory: () => ({}),
    loadUnresolved: async () => [record],
    fetchImpl: async () => ({ ok: true, json: async () => [{
      conditionId, asset: record.reservation.token_id, proxyWallet: env.POLYMARKET_FUNDER_ADDRESS,
      outcome: env.FUNDED_TERMINAL_SETTLEMENT_OUTCOME, redeemable: false, size: 5, currentValue: 0
    }] }),
    settle: async () => { settled = true; return 1; },
    logger: () => {}
  }), /does not prove one redeemable terminal position/);
  assert.equal(settled, false);
});

test("settlement-only module imports no canary, order client, wallet, or signing code", async () => {
  const source = await readFile(new URL("../src/funded-terminal-settlement.mjs", import.meta.url), "utf8");
  assert.doesNotMatch(source, /canary\.mjs|clob-client|createAndPostOrder|POLYMARKET_PRIVATE_KEY|POLYMARKET_API_/);
  assert.throws(() => terminalSettlementConfig({ ...env, FUNDED_TERMINAL_SETTLEMENT_RESERVATION_BLOB_NAME: "*" }), /exact terminal settlement binding/);
  for (const [name, value] of Object.entries({
    AZURE_TENANT_ID: "wrong",
    AZURE_STORAGE_ACCOUNT_NAME: "wrong",
    AZURE_STORAGE_CONTAINER_NAME: "wrong",
    AZURE_CLIENT_ID: "wrong",
    VENUE_PROBE_FUNDED_CAMPAIGN_ID: "wrong",
    POLYMARKET_FUNDER_ADDRESS: env.POLYMARKET_FUNDER_ADDRESS.toUpperCase()
  })) {
    assert.throws(() => terminalSettlementConfig({ ...env, [name]: value }), /exact terminal settlement binding/);
  }
  assert.throws(() => terminalSettlementConfig({ ...env, AZURE_STORAGE_ACCOUNT_KEY: "forbidden" }), /exact terminal settlement binding/);
});

test("live credential path requires an owner-only regular workload JWT file", () => {
  const assertion = "header.payload.signature";
  const regular = { isFile: () => true, isSymbolicLink: () => false, mode: 0o100600, uid: 1000, size: assertion.length };
  validateTerminalSettlementFederatedTokenFile("/run/polyedge/funded.jwt", {
    stat: () => regular,
    read: () => assertion,
    uid: 1000
  });
  assert.throws(() => validateTerminalSettlementFederatedTokenFile("relative.jwt", {
    stat: () => regular, read: () => assertion, uid: 1000
  }), /absolute path/);
  assert.throws(() => validateTerminalSettlementFederatedTokenFile("/run/polyedge/funded.jwt", {
    stat: () => ({ ...regular, isSymbolicLink: () => true }), read: () => assertion, uid: 1000
  }), /owner-only regular JWT file/);
  assert.throws(() => validateTerminalSettlementFederatedTokenFile("/run/polyedge/funded.jwt", {
    stat: () => ({ ...regular, mode: 0o100644 }), read: () => assertion, uid: 1000
  }), /owner-only regular JWT file/);
  assert.throws(() => validateTerminalSettlementFederatedTokenFile("/run/polyedge/funded.jwt", {
    stat: () => ({ ...regular, uid: 0 }), read: () => assertion, uid: 1000
  }), /owner-only regular JWT file/);
  let readCalled = false;
  assert.throws(() => validateTerminalSettlementFederatedTokenFile("/run/polyedge/funded.jwt", {
    stat: () => ({ ...regular, size: 16_385 }), read: () => { readCalled = true; return assertion; }, uid: 1000
  }), /owner-only regular JWT file/);
  assert.equal(readCalled, false);
});

test("reservation and Data API binding adversaries fail closed before settlement", async () => {
  for (const mutate of [
    (value) => { value.reservation.state = "finalized"; },
    (value) => { value.reservation.reconciliation_complete = false; },
    (value) => { value.etag = ""; },
    (value) => { value.reservation.token_id = "not-a-token"; }
  ]) {
    const invalid = structuredClone(record);
    mutate(invalid);
    let settled = false;
    await assert.rejects(runFundedTerminalSettlement({
      env, containerFactory: () => ({}), loadUnresolved: async () => [invalid],
      fetchImpl: async () => { throw new Error("must not query Data API"); },
      settle: async () => { settled = true; return 1; }, logger: () => {}
    }), /terminal settlement reservation evidence is invalid/);
    assert.equal(settled, false);
  }
  for (const position of [
    { asset: "456" },
    { proxyWallet: "0x0000000000000000000000000000000000000000" },
    { outcome: "Up" },
    { currentValue: "" }
  ]) {
    let settled = false;
    await assert.rejects(runFundedTerminalSettlement({
      env, containerFactory: () => ({}), loadUnresolved: async () => [record],
      fetchImpl: async () => ({ ok: true, json: async () => [{
        conditionId, asset: record.reservation.token_id, proxyWallet: env.POLYMARKET_FUNDER_ADDRESS,
        outcome: env.FUNDED_TERMINAL_SETTLEMENT_OUTCOME, redeemable: true, size: 5, currentValue: 0, ...position
      }] }),
      settle: async () => { settled = true; return 1; }, logger: () => {}
    }), /does not prove one redeemable terminal position/);
    assert.equal(settled, false);
  }
});

test("default storage path requires workload federation before any storage operation", async () => {
  await assert.rejects(runFundedTerminalSettlement({ env }), /AZURE_FEDERATED_TOKEN_FILE must be an absolute path/);
});
