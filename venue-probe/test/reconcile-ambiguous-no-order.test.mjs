import test from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import {
  acknowledgedNoFillConfig,
  ambiguousNoOrderConfig,
  observeAcknowledgedNoFill,
  runAcknowledgedNoFillReconciliation,
  runAmbiguousNoOrderReconciliation,
  validateAcknowledgedNoFillBinding,
  validateAcknowledgedNoFillSnapshot,
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


const acknowledgedDecision = "f".repeat(64);
const acknowledgedRunId = "funded-direct-20260821194914083-5cc133ce";
const acknowledgedOrderId = "0x" + "2".repeat(64);
const acknowledgedReservationBlob =
  "reports/research/venue-probe/risk-reservations/2026-08-21/funded-direct-" +
  acknowledgedDecision + ".json";
const acknowledgedCompletionBlob =
  "reports/funded/dynamic-quote/sessions/dynamic-quote-funded-2026-08-13-v10/completed/" +
  acknowledgedDecision + ".json";

function acknowledgedEnv() {
  return {
    ...env(),
    FUNDED_DIRECT_RECONCILIATION_REASON: "acknowledged_terminal_no_fill",
    FUNDED_DIRECT_RECONCILE_DECISION_ID: acknowledgedDecision,
    FUNDED_DIRECT_RECONCILE_RUN_ID: acknowledgedRunId,
    FUNDED_DIRECT_RECONCILE_ORDER_ID: acknowledgedOrderId,
    FUNDED_DIRECT_RECONCILE_RESERVATION_BLOB_NAME: acknowledgedReservationBlob,
    FUNDED_DIRECT_RECONCILE_RESERVATION_SHA256: "sha256:" + "e".repeat(64),
    FUNDED_DIRECT_RECONCILE_COMPLETION_BLOB_NAME: acknowledgedCompletionBlob,
    FUNDED_DIRECT_RECONCILE_COMPLETION_SHA256: "sha256:" + "d".repeat(64)
  };
}

function acknowledgedFixture() {
  const config = acknowledgedNoFillConfig(acknowledgedEnv());
  const reservation = {
    schema_version: 1,
    evidence_protocol_version: 3,
    campaign_id: config.campaignId,
    run_id: config.runId,
    probe_id: config.probeId,
    date: "2026-08-21",
    state: "unresolved_reconciliation",
    order_submission_intended: true,
    order_submitted: true,
    order_id: config.orderId,
    matched_notional: 0,
    reconciliation_complete: false,
    zero_open_orders_confirmed: true,
    condition_id: "0x" + "3".repeat(64),
    token_id: "123456789",
    created_ts: "2026-08-21T19:49:15.837Z",
    updated_ts: "2026-08-21T19:50:10.157Z"
  };
  const completion = {
    schema: "polyedge.operator_funded_intent_completion.v1",
    session_id: config.campaignId,
    decision_id: config.decisionId,
    child_run_id: config.runId,
    completed_at: "2026-08-21T19:50:12.505Z",
    status: "child_failed_closed_post_submission_unresolved",
    order_submission_attempted: true,
    authorization_consumed: true,
    risk_reservation_created: true,
    order_id: config.orderId,
    matched_notional: 0,
    reconciliation_complete: false,
    zero_open_orders_confirmed: true,
    post_submission_error:
      "fail closed: post-ack error; tracked order canceled, zero open orders confirmed, unresolved risk preserved (user channel did not reconcile)"
  };
  return {
    config,
    record: {
      blob_name: acknowledgedReservationBlob,
      etag: '"etag-known-1"',
      reservation_sha256: config.reservationSha256,
      reservation
    },
    reservationDocument: {
      blobName: acknowledgedReservationBlob,
      sha256: config.reservationSha256,
      value: reservation
    },
    completionDocument: {
      blobName: acknowledgedCompletionBlob,
      sha256: config.completionSha256,
      value: completion
    },
    nowMs: Date.parse("2026-08-22T20:00:00Z")
  };
}

function acknowledgedSnapshot(value = acknowledgedFixture()) {
  return {
    reservation: value.record.reservation,
    config: value.config,
    order: {
      id: value.config.orderId,
      status: "CANCELED",
      size_matched: "0",
      market: value.record.reservation.condition_id,
      asset_id: value.record.reservation.token_id,
      associate_trades: []
    },
    openOrders: [],
    authenticatedTrades: [],
    positions: [{ conditionId: "other", asset: "9", size: "1", redeemable: true }],
    observedAtMs: Date.parse("2026-08-22T20:00:00Z")
  };
}

test("acknowledged-order binding is exact and cannot run before the 24-hour evidence floor", () => {
  const value = acknowledgedFixture();
  assert.equal(validateAcknowledgedNoFillBinding(value).etag, '"etag-known-1"');
  const invalid = [
    (row) => { row.record.etag = ""; },
    (row) => { row.record.reservation_sha256 = "sha256:" + "0".repeat(64); },
    (row) => { row.reservationDocument.value.evidence_protocol_version = 2; },
    (row) => { row.reservationDocument.value.state = "reserved"; },
    (row) => { row.reservationDocument.value.order_id = "0x" + "4".repeat(64); },
    (row) => { row.reservationDocument.value.matched_notional = null; },
    (row) => { row.reservationDocument.value.matched_notional = ""; },
    (row) => { row.reservationDocument.value.matched_notional = 1; },
    (row) => { row.reservationDocument.value.reconciliation_complete = true; },
    (row) => { row.reservationDocument.value.zero_open_orders_confirmed = false; },
    (row) => { row.completionDocument.value.order_id = "0x" + "4".repeat(64); },
    (row) => { row.completionDocument.value.status = "child_completed"; },
    (row) => { row.completionDocument.value.matched_notional = null; },
    (row) => { row.completionDocument.value.matched_notional = 1; },
    (row) => { row.nowMs = Date.parse("2026-08-22T19:50:12.504Z"); }
  ];
  for (const mutate of invalid) {
    const row = acknowledgedFixture();
    mutate(row);
    assert.throws(() => validateAcknowledgedNoFillBinding(row), /fail closed/);
  }
});

test("acknowledged-order configuration pins the exact production order and forbids account keys", () => {
  assert.doesNotThrow(() => acknowledgedNoFillConfig(acknowledgedEnv()));
  for (const mutate of [
    (row) => { row.FUNDED_DIRECT_RECONCILE_ORDER_ID = "bad"; },
    (row) => { row.POLYMARKET_CLOB_URL = "https://example.invalid"; },
    (row) => { row.AZURE_STORAGE_ACCOUNT_KEY = "forbidden"; }
  ]) {
    const row = acknowledgedEnv();
    mutate(row);
    assert.throws(() => acknowledgedNoFillConfig(row), /fail closed/);
  }
});

test("acknowledged-order venue proof rejects any exposure, trade reference, or unstable snapshot", async () => {
  assert.equal(
    validateAcknowledgedNoFillSnapshot(acknowledgedSnapshot()).terminal_order_status,
    "CANCELED"
  );
  const invalid = [
    (row) => { row.order.status = "MATCHED"; },
    (row) => { row.order.id = "0x" + "4".repeat(64); },
    (row) => { row.order.market = "0x" + "4".repeat(64); },
    (row) => { row.order.asset_id = "987"; },
    (row) => { row.order.size_matched = null; },
    (row) => { row.order.size_matched = ""; },
    (row) => { row.order.size_matched = "1"; },
    (row) => { row.order.associate_trades.push("trade"); },
    (row) => { row.openOrders.push({ id: "open" }); },
    (row) => { row.authenticatedTrades.push({ id: "trade", maker_order_id: acknowledgedOrderId }); },
    (row) => { row.positions.push({
      conditionId: row.reservation.condition_id,
      asset: row.reservation.token_id,
      size: "1",
      redeemable: true
    }); },
    (row) => { row.positions.push({
      conditionId: "other",
      asset: "8",
      size: "1",
      redeemable: false
    }); },
    (row) => { row.positions.push({
      conditionId: row.reservation.condition_id,
      asset: row.reservation.token_id,
      size: "not-a-number",
      redeemable: true
    }); }
  ];
  for (const mutate of invalid) {
    const row = structuredClone(acknowledgedSnapshot());
    mutate(row);
    assert.throws(() => validateAcknowledgedNoFillSnapshot(row), /fail closed/);
  }

  let clock = Date.parse("2026-08-22T20:00:00Z");
  let orderCalls = 0;
  const value = acknowledgedFixture();
  const client = {
    getOrder: async () => {
      orderCalls += 1;
      return acknowledgedSnapshot(value).order;
    },
    getOpenOrders: async () => [],
    getTrades: async () => []
  };
  const evidence = await observeAcknowledgedNoFill({
    client,
    reservation: value.record.reservation,
    config: value.config,
    fetchImpl: async () => ({ ok: true, json: async () => [] }),
    now: () => clock,
    sleep: async (ms) => { clock += ms; }
  });
  assert.equal(evidence.observation_ms, 10_000);
  assert.equal(orderCalls, 2);

  let positionPageCalls = 0;
  clock = Date.parse("2026-08-22T20:00:00Z");
  await observeAcknowledgedNoFill({
    client,
    reservation: value.record.reservation,
    config: value.config,
    fetchImpl: async (url) => {
      positionPageCalls += 1;
      const rows = new URL(url).searchParams.get("offset") === "0"
        ? Array.from({ length: 500 }, () => ({ size: "0", currentValue: "0" }))
        : [];
      return { ok: true, json: async () => rows };
    },
    now: () => clock,
    sleep: async (ms) => { clock += ms; }
  });
  assert.equal(positionPageCalls, 4);

  clock = Date.parse("2026-08-22T20:00:00Z");
  orderCalls = 0;
  await assert.rejects(observeAcknowledgedNoFill({
    client: {
      ...client,
      getOrder: async () => ({
        ...acknowledgedSnapshot(value).order,
        status: orderCalls++ === 0 ? "CANCELED" : "EXPIRED"
      })
    },
    reservation: value.record.reservation,
    config: value.config,
    fetchImpl: async () => ({ ok: true, json: async () => [] }),
    now: () => clock,
    sleep: async (ms) => { clock += ms; }
  }), /not stable/);
});

test("acknowledged-order recovery re-reads immutable evidence and preserves the known order through ETag CAS", async () => {
  const value = acknowledgedFixture();
  const observations = {
    first: validateAcknowledgedNoFillSnapshot(acknowledgedSnapshot(value)),
    second: validateAcknowledgedNoFillSnapshot({
      ...acknowledgedSnapshot(value),
      observedAtMs: Date.parse("2026-08-22T20:00:10Z")
    }),
    observation_ms: 10_000
  };
  let recordLoads = 0;
  let finalized = null;
  let terminal = null;
  let logged = null;
  const result = await runAcknowledgedNoFillReconciliation({
    env: acknowledgedEnv(),
    now: () => value.nowMs,
    containerFactory: () => ({}),
    loadRecords: async () => recordLoads++ < 2 ? [value.record] : [],
    loadDocument: async (_container, name) => {
      if (name === acknowledgedCompletionBlob) return value.completionDocument;
      if (terminal) return {
        blobName: acknowledgedReservationBlob,
        sha256: canonicalSha(terminal),
        value: terminal
      };
      return value.reservationDocument;
    },
    createClient: async () => ({}),
    observe: async () => observations,
    finalize: async (_config, reservation, settlement, options) => {
      finalized = { reservation, settlement, options };
      terminal = { ...reservation, ...settlement, updated_ts: "2026-08-22T20:00:11Z" };
      return terminal;
    },
    logger: (row) => { logged = row; }
  });
  assert.equal(result.status, "finalized_no_fill");
  assert.equal(result.order_submission_attempted, true);
  assert.equal(result.source_grant_consumed, true);
  assert.equal(result.risk_reservation_created, true);
  assert.equal(result.recovery_order_submission_attempted, false);
  assert.equal(result.recovery_grant_consumed, false);
  assert.equal(result.order_id, acknowledgedOrderId);
  assert.equal(logged.status, "finalized_no_fill");
  assert.equal(finalized.settlement.order_id, acknowledgedOrderId);
  assert.equal(finalized.options.ifMatch, '"etag-known-1"');
  assert.equal(finalized.settlement.reconciliation_evidence.observations.length, 2);
  assert.equal(recordLoads, 3);
});

test("acknowledged-order recovery emits no success before stable evidence or after an ETag change", async () => {
  const value = acknowledgedFixture();
  let finalized = false;
  await assert.rejects(runAcknowledgedNoFillReconciliation({
    env: acknowledgedEnv(),
    now: () => value.nowMs,
    containerFactory: () => ({}),
    loadRecords: async () => [value.record],
    loadDocument: async (_container, name) =>
      name === acknowledgedReservationBlob ? value.reservationDocument : value.completionDocument,
    createClient: async () => ({}),
    observe: async () => { throw new Error("endpoint failed"); },
    finalize: async () => { finalized = true; },
    logger: () => assert.fail("must not log success")
  }), /endpoint failed/);
  assert.equal(finalized, false);

  let loads = 0;
  await assert.rejects(runAcknowledgedNoFillReconciliation({
    env: acknowledgedEnv(),
    now: () => value.nowMs,
    containerFactory: () => ({}),
    loadRecords: async () => loads++ === 0
      ? [value.record]
      : [{ ...value.record, etag: '"etag-known-2"' }],
    loadDocument: async (_container, name) =>
      name === acknowledgedReservationBlob ? value.reservationDocument : value.completionDocument,
    createClient: async () => ({}),
    observe: async () => ({
      first: validateAcknowledgedNoFillSnapshot(acknowledgedSnapshot(value)),
      second: validateAcknowledgedNoFillSnapshot({
        ...acknowledgedSnapshot(value),
        observedAtMs: Date.parse("2026-08-22T20:00:10Z")
      }),
      observation_ms: 10_000
    }),
    finalize: async () => { finalized = true; },
    logger: () => assert.fail("must not log success")
  }), /durable evidence changed/);
  assert.equal(finalized, false);
});


function canonicalSha(value) {
  return "sha256:" + createHash("sha256")
    .update(Buffer.from(JSON.stringify(value, null, 2)))
    .digest("hex");
}


test("acknowledged-order recovery requires exact terminal bytes and a zero unresolved index", async () => {
  for (const mode of ["hash", "value", "index"]) {
    const value = acknowledgedFixture();
    let recordLoads = 0;
    let terminal = null;
    await assert.rejects(runAcknowledgedNoFillReconciliation({
      env: acknowledgedEnv(),
      now: () => value.nowMs,
      containerFactory: () => ({}),
      loadRecords: async () => {
        recordLoads += 1;
        if (recordLoads < 3) return [value.record];
        return mode === "index" ? [value.record] : [];
      },
      loadDocument: async (_container, name) => {
        if (name === acknowledgedCompletionBlob) return value.completionDocument;
        if (!terminal) return value.reservationDocument;
        const readback = mode === "value"
          ? { ...terminal, order_id: "0x" + "4".repeat(64) }
          : terminal;
        return {
          blobName: acknowledgedReservationBlob,
          sha256: mode === "hash" ? "sha256:" + "0".repeat(64) : canonicalSha(readback),
          value: readback
        };
      },
      createClient: async () => ({}),
      observe: async () => ({
        first: validateAcknowledgedNoFillSnapshot(acknowledgedSnapshot(value)),
        second: validateAcknowledgedNoFillSnapshot({
          ...acknowledgedSnapshot(value),
          observedAtMs: Date.parse("2026-08-22T20:00:10Z")
        }),
        observation_ms: 10_000
      }),
      finalize: async (_config, reservation, settlement) => {
        terminal = { ...reservation, ...settlement, updated_ts: "2026-08-22T20:00:11Z" };
        return terminal;
      },
      logger: () => assert.fail("must not log success")
    }), /terminal reservation readback|remained unresolved/);
  }
});
