import test from "node:test";
import assert from "node:assert/strict";
import {
  ambiguousNoOrderConfig,
  runAmbiguousNoOrderReconciliation,
  validateAmbiguousNoOrderBinding,
  validateAmbiguousNoOrderVenueEvidence
} from "../src/reconcile-ambiguous-no-order.mjs";

const decision = "a".repeat(64);
const runId = "funded-direct-20260819090220456-0ccb696d";
const reservationBlob =
  "reports/research/venue-probe/risk-reservations/2026-08-19/funded-direct-" +
  decision + ".json";
const completionBlob =
  "reports/funded/dynamic-quote/sessions/dynamic-quote-funded-2026-08-13-v10/completed/" +
  decision + ".json";

function env() {
  return {
    FUNDED_DIRECT_RECONCILIATION_ENABLED: "true",
    FUNDED_DIRECT_RECONCILIATION_REASON: "ambiguous_submission_no_fill",
    FUNDED_DIRECT_RECONCILE_DECISION_ID: decision,
    FUNDED_DIRECT_RECONCILE_RUN_ID: runId,
    FUNDED_DIRECT_RECONCILE_RESERVATION_BLOB_NAME: reservationBlob,
    FUNDED_DIRECT_RECONCILE_RESERVATION_SHA256: "sha256:" + "b".repeat(64),
    FUNDED_DIRECT_RECONCILE_COMPLETION_BLOB_NAME: completionBlob,
    FUNDED_DIRECT_RECONCILE_COMPLETION_SHA256: "sha256:" + "c".repeat(64),
    VENUE_PROBE_FUNDED_CAMPAIGN_ID: "dynamic-quote-funded-2026-08-13-v10",
    POLYMARKET_FUNDER_ADDRESS: "0x3d701b05d7c36afab01a06fd26ebe789c0b7bad8",
    POLYMARKET_SIGNATURE_TYPE: "3",
    AZURE_TENANT_ID: "9767f0dc-e83f-4cc1-94e1-0d5f9d287d32",
    AZURE_STORAGE_ACCOUNT_NAME: "stpolyedge6urdjr5nmwx7w",
    AZURE_STORAGE_CONTAINER_NAME: "polyedge-funded-evidence",
    AZURE_CLIENT_ID: "d9ce9154-66a6-4bdb-839f-0da7b02b38da",
    AZURE_TOKEN_CREDENTIALS: "WorkloadIdentityCredential"
  };
}

function fixture() {
  const config = ambiguousNoOrderConfig(env());
  const reservation = {
    schema_version: 1,
    campaign_id: config.campaignId,
    run_id: config.runId,
    probe_id: config.probeId,
    date: "2026-08-19",
    state: "reserved",
    order_submission_intended: true,
    order_submitted: null,
    order_id: null,
    matched_notional: null,
    condition_id: "0x" + "d".repeat(64),
    token_id: "12345",
    created_ts: "2026-08-19T09:02:21.328Z"
  };
  const completion = {
    schema: "polyedge.operator_funded_intent_completion.v1",
    session_id: config.campaignId,
    decision_id: config.decisionId,
    child_run_id: config.runId,
    completed_at: "2026-08-19T09:02:24.083Z",
    status: "child_failed_closed_post_submission_unresolved",
    order_submission_attempted: true,
    authorization_consumed: true,
    risk_reservation_created: true,
    order_id: null,
    matched_notional: 0,
    reconciliation_complete: false,
    zero_open_orders_confirmed: false,
    post_submission_error:
      "fail closed: ambiguous strategy-canary submission; authorization is consumed and risk remains reserved (rpc unavailable)"
  };
  return {
    config,
    record: { blob_name: reservationBlob, etag: '"etag-1"', reservation },
    reservationDocument: {
      blobName: reservationBlob,
      sha256: config.reservationSha256,
      value: reservation
    },
    completionDocument: {
      blobName: completionBlob,
      sha256: config.completionSha256,
      value: completion
    },
    nowMs: Date.parse("2026-08-20T10:00:00Z")
  };
}

test("exact ambiguity binding requires immutable reservation and completion evidence older than 24h", () => {
  const value = fixture();
  assert.equal(validateAmbiguousNoOrderBinding(value).etag, '"etag-1"');
  const zeroMatched = fixture();
  zeroMatched.reservationDocument.value.matched_notional = 0;
  assert.equal(validateAmbiguousNoOrderBinding(zeroMatched).etag, '"etag-1"');
  const cases = [
    (row) => { row.record.etag = ""; },
    (row) => { row.reservationDocument.sha256 = "sha256:" + "0".repeat(64); },
    (row) => { row.reservationDocument.value.date = "2026-08-20"; },
    (row) => { row.reservationDocument.value.state = "position_unresolved"; },
    (row) => { row.reservationDocument.value.order_submitted = true; },
    (row) => { row.reservationDocument.value.matched_notional = 1; },
    (row) => { row.completionDocument.sha256 = "sha256:" + "0".repeat(64); },
    (row) => { row.completionDocument.value.child_run_id = "another-run"; },
    (row) => { row.completionDocument.value.status = "child_completed"; },
    (row) => { row.completionDocument.value.order_id = "0x" + "1".repeat(64); },
    (row) => { row.completionDocument.value.matched_notional = 1; },
    (row) => { row.completionDocument.value.completed_at = "2026-08-19T08:00:00Z"; },
    (row) => { row.nowMs = Date.parse("2026-08-20T08:00:00Z"); }
  ];
  for (const mutate of cases) {
    const row = fixture();
    mutate(row);
    assert.throws(() => validateAmbiguousNoOrderBinding(row), /fail closed/);
  }
});

test("reconciliation environment permits only the production CLOB and proxy signature", () => {
  assert.doesNotThrow(() => ambiguousNoOrderConfig(env()));
  const explicitProduction = env();
  explicitProduction.POLYMARKET_CLOB_URL = "https://clob.polymarket.com";
  assert.doesNotThrow(() => ambiguousNoOrderConfig(explicitProduction));
  for (const [name, value] of [
    ["POLYMARKET_CLOB_URL", "https://example.invalid"],
    ["POLYMARKET_SIGNATURE_TYPE", "0"]
  ]) {
    const row = env();
    row[name] = value;
    assert.throws(() => ambiguousNoOrderConfig(row), /environment is not exact/);
  }
});

test("venue proof rejects every open order, post-reservation trade, exact position, or unresolved position", () => {
  const { record } = fixture();
  const base = {
    reservation: record.reservation,
    runId,
    openOrders: [],
    authenticatedTrades: [{ match_time: "2026-08-19T08:00:00Z" }],
    positions: [{ conditionId: "other", asset: "9", size: "1", redeemable: true }]
  };
  assert.equal(validateAmbiguousNoOrderVenueEvidence(base).exact_position_count, 0);
  const cases = [
    (row) => { row.openOrders.push({ id: "open" }); },
    (row) => { row.authenticatedTrades.push({ match_time: "2026-08-19T09:02:22Z" }); },
    (row) => { row.positions.push({
      conditionId: record.reservation.condition_id,
      asset: record.reservation.token_id,
      size: "1",
      redeemable: true
    }); },
    (row) => { row.positions.push({
      conditionId: "other",
      asset: "8",
      size: "1",
      redeemable: false
    }); }
  ];
  for (const mutate of cases) {
    const row = structuredClone(base);
    mutate(row);
    assert.throws(() => validateAmbiguousNoOrderVenueEvidence(row), /fail closed/);
  }
});

test("exact ambiguity recovery finalizes conservatively with ETag CAS and zero unresolved readback", async () => {
  const value = fixture();
  let loads = 0;
  let finalized = null;
  let logged = null;
  const result = await runAmbiguousNoOrderReconciliation({
    env: env(),
    now: () => value.nowMs,
    containerFactory: () => ({}),
    loadRecords: async () => loads++ === 0 ? [value.record] : [],
    loadDocument: async (_container, name) =>
      name === reservationBlob ? value.reservationDocument : value.completionDocument,
    createClient: async () => ({
      getOpenOrders: async () => [],
      getTrades: async () => [{ match_time: "2026-08-19T08:00:00Z" }],
      getBalanceAllowance: async () => ({ balance: "25525438" })
    }),
    fetchImpl: async () => ({
      ok: true,
      json: async () => [{ conditionId: "other", asset: "9", size: "1", redeemable: true }]
    }),
    finalize: async (_config, reservation, settlement, options) => {
      finalized = { reservation, settlement, options };
      return { ...reservation, ...settlement };
    },
    logger: (row) => { logged = row; }
  });
  assert.equal(result.status, "finalized_no_fill");
  assert.equal(result.order_submission_attempted, true);
  assert.equal(logged.status, "finalized_no_fill");
  assert.equal(finalized.settlement.state, "finalized_no_fill");
  assert.equal(finalized.settlement.order_submitted, true);
  assert.equal(finalized.settlement.matched_notional, 0);
  assert.equal(finalized.settlement.reconciliation_reason, "ambiguous_submission_no_fill");
  assert.equal(finalized.settlement.reconciliation_evidence.submission_outcome, "ambiguous");
  assert.equal(finalized.options.ifMatch, '"etag-1"');
  assert.equal(loads, 2);
});

test("a stale ETag failure stops without a successful readback", async () => {
  const value = fixture();
  let loads = 0;
  await assert.rejects(runAmbiguousNoOrderReconciliation({
    env: env(),
    now: () => value.nowMs,
    containerFactory: () => ({}),
    loadRecords: async () => { loads += 1; return [value.record]; },
    loadDocument: async (_container, name) =>
      name === reservationBlob ? value.reservationDocument : value.completionDocument,
    createClient: async () => ({
      getOpenOrders: async () => [],
      getTrades: async () => [],
      getBalanceAllowance: async () => ({ balance: "25525438" })
    }),
    fetchImpl: async () => ({ ok: true, json: async () => [] }),
    finalize: async () => {
      throw Object.assign(new Error("condition not met"), { statusCode: 412 });
    },
    logger: () => assert.fail("must not log success")
  }), /condition not met/);
  assert.equal(loads, 1);
});
