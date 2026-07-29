import test from "node:test";
import assert from "node:assert/strict";
import { Readable } from "node:stream";
import {
  beginFillMarkoutCapture,
  artifactLocationFromUri,
  canonicalBookHash,
  consumeOneShotAuthorization,
  deterministicNoOrderRejection,
  executeStrategyCanary,
  loadHashedJson,
  sha256,
  validateDeterministicNoOrderReconciliation
} from "../src/canary-lib.mjs";
import { streamBookEvidence } from "../src/canary.mjs";

const now = new Date("2026-07-12T12:00:20.000Z");
const book = {
  tick_size: "0.01",
  min_order_size: "5",
  bids: [{ price: "0.19", size: "10" }],
  asks: [{ price: "0.21", size: "10" }]
};
const intentHash = `sha256:${"1".repeat(64)}`;
const manifestHash = `sha256:${"2".repeat(64)}`;
const executionModelHash = `sha256:${"7".repeat(64)}`;

test("execution model URI resolves its exact cross-container artifact", () => {
  assert.deepEqual(
    artifactLocationFromUri(
      "azure://storage/polyedge-research/reports/research/venue-probe/conservative_execution_prior_v1.json",
      "storage"
    ),
    {
      account: "storage",
      container: "polyedge-research",
      blobName: "reports/research/venue-probe/conservative_execution_prior_v1.json"
    }
  );
  assert.throws(
    () => artifactLocationFromUri("azure://different/polyedge-research/prior.json", "storage"),
    /outside configured Azure storage account/
  );
});

test("final stream evidence follows the newest token-specific top-of-book update", () => {
  const messages = [
    {
      event_type: "book",
      asset_id: "token-up",
      asks: [{ price: "0.52", size: "5" }],
      _received_wall_ms: 1
    },
    {
      event_type: "price_change",
      price_changes: [
        { asset_id: "token-down", best_ask: "0.61" },
        { asset_id: "token-up", best_ask: "0.53" }
      ],
      _received_wall_ms: 2
    },
    {
      event_type: "best_bid_ask",
      asset_id: "token-up",
      best_ask: "0.54",
      _received_wall_ms: 3
    }
  ];
  assert.deepEqual(streamBookEvidence(messages, "token-up"), {
    bestAsk: 0.54,
    source: "best_bid_ask",
    receivedWallMs: 3
  });
  assert.equal(streamBookEvidence(messages, "unknown"), null);
});

function fixture(dryRun = true) {
  const config = {
    dryRun,
    trustBoundaryReady: true,
    candidateName: "dynamic_quote_style",
    candidateVersion: "dynamic_quote_style@2026-06-14",
    candidateConfigHash: "sha256:e76b8b54f52f79de91c43e007c45f347226d5b9e2e562f2bc40c3586855b0a0c",
    requiredFillModelVersion: "conservative-execution-prior-v1",
    executionModelBlobUri: "azure://storage/polyedge-research/reports/research/venue-probe/conservative_execution_prior_v1.json",
    executionModelHash,
    storageAccount: "storage",
    requiredResolutionSource: "chainlink_reference",
    maxOrderNotional: 1,
    maxReferenceAgeMs: 2000,
    maxBookAgeMs: 1000,
    maxClockDriftMs: 5000,
    minRemainingTtlMs: 5000,
    expectedCountry: "IE",
    expectedEgressIp: "203.0.113.8",
    intentBlobName: "intents/decision-1.json",
    intentBlobHash: intentHash,
    manifestBlobName: "promotion/canary.json",
    manifestBlobHash: manifestHash,
    humanGrantId: "grant-1",
    humanGrantHash: `sha256:${"5".repeat(64)}`,
    humanGrantConsumptionBlobName: "human-grants/consumed/grant-1.json",
    humanGrantConsumptionHash: `sha256:${"6".repeat(64)}`
  };
  const intent = {
    schema: "polyedge.execution_intent.v1",
    decision_id: "decision-1",
    candidate_name: config.candidateName,
    candidate_version: config.candidateVersion,
    candidate_config_hash: config.candidateConfigHash,
    market_id: "market-1",
    condition_id: "condition-1",
    token_id: "token-1",
    outcome: "up",
    side: "BUY",
    price: "0.20",
    shares: "5",
    notional: "1.00",
    minimum_order_size: "5",
    post_only: true,
    order_kind: "post_only_gtd",
    ttl_ms: 30000,
    decision_ts: "2026-07-12T12:00:00.000Z",
    valid_until: "2026-07-12T12:00:30.000Z",
    gtd_expiry_ts: "2026-07-12T12:05:30.000Z",
    book_hash: canonicalBookHash(book, "token-1"),
    q: "0.25",
    gross_edge: "0.05",
    fee_allowance: "0.005",
    slippage_allowance: "0.005",
    toxicity_allowance: "0.01",
    net_edge_lower_bound: "0.03",
    regime: "normal",
    features_digest: `sha256:${"3".repeat(64)}`,
    reference_age_ms: 100,
    book_age_ms: 80,
    required_fill_model_version: config.requiredFillModelVersion,
    execution_model_blob_uri: config.executionModelBlobUri,
    execution_model_sha256: config.executionModelHash,
    execution_model_container_name: "polyedge-research",
    execution_model_blob_name: "reports/research/venue-probe/conservative_execution_prior_v1.json",
    resolution_source: config.requiredResolutionSource,
    exact_resolution_source: true
  };
  const manifest = {
    schema_version: "promotion_manifest_v1",
    candidate: { name: config.candidateName, candidate_version: config.candidateVersion, config_hash: config.candidateConfigHash },
    phase: "canary_ready",
    gate_metrics: { phase: "canary_ready", promotion_allowed: true },
    human_authorization_required: true,
    promotion_allowed: false,
    created_at: "2026-07-12T11:00:00.000Z",
    expires_at: "2026-07-12T13:00:00.000Z",
    execution_model: { blob_uri: config.executionModelBlobUri, sha256: config.executionModelHash, model_version: config.requiredFillModelVersion },
    controller_transition: {
      human_grant_id: config.humanGrantId,
      human_grant_sha256: config.humanGrantHash,
      human_grant_consumption_blob_name: config.humanGrantConsumptionBlobName,
      human_grant_consumption_sha256: config.humanGrantConsumptionHash
    }
  };
  const authorization = {
    schema: "polyedge.strategy_canary_authorization.v1",
    authorization_id: "approval-1",
    decision_id: intent.decision_id,
    intent_blob_name: config.intentBlobName,
    intent_sha256: config.intentBlobHash,
    promotion_manifest_blob_name: config.manifestBlobName,
    promotion_manifest_sha256: config.manifestBlobHash,
    human_grant_id: config.humanGrantId,
    human_grant_sha256: config.humanGrantHash,
    human_grant_consumption_blob_name: config.humanGrantConsumptionBlobName,
    human_grant_consumption_sha256: config.humanGrantConsumptionHash,
    candidate_name: config.candidateName,
    candidate_version: config.candidateVersion,
    candidate_config_hash: config.candidateConfigHash,
    required_fill_model_version: config.requiredFillModelVersion,
    execution_model_blob_uri: config.executionModelBlobUri,
    execution_model_sha256: config.executionModelHash,
    execution_model_container_name: "polyedge-research",
    execution_model_blob_name: "reports/research/venue-probe/conservative_execution_prior_v1.json",
    human_authorization_reference: "human-review-2026-07-12-1",
    authorized_at: "2026-07-12T12:00:10.000Z",
    expires_at: "2026-07-12T12:01:30.000Z",
    single_use: true
  };
  const runtime = {
    geoblock: { blocked: false, country: "IE", ip: config.expectedEgressIp },
    clockDriftMs: 25,
    clockServerMinusLocalMs: 25,
    clockRoundTripMs: 20,
    clockUncertaintyMs: 11,
    risk: { passed: true, blockers: [] },
    openOrderCount: 0,
    market: { marketId: intent.market_id, conditionId: intent.condition_id, tokenId: intent.token_id, acceptingOrders: true, closed: false },
    book,
    feeModel: "polymarket_clob_v2_curve",
    feeRate: 0,
    feeRateBps: 0,
    feeExponent: 0,
    feeTakerOnly: true,
    fillModelVersion: config.requiredFillModelVersion,
    exactResolutionSource: true,
    resolutionSource: config.requiredResolutionSource
  };
  return {
    config,
    documents: {
      intent,
      manifest,
      authorization,
      authorizationHash: `sha256:${"4".repeat(64)}`,
      executionModel: {
        model_version: config.requiredFillModelVersion,
        status: "frozen_conservative_prior",
        generated_at: "2026-07-12T00:00:00Z",
        evidence_protocol_version: 3,
        prediction_policy: "zero_fill_probability_until_authenticated_calibration",
        sample_size: 0,
        promotion_allowed: false,
        funded_execution_allowed: false
      },
      executionModelHash: config.executionModelHash
    },
    runtime,
    runId: "run-1",
    now
  };
}

function spies() {
  const calls = { reserve: 0, consume: 0, execute: 0, finalize: 0 };
  return {
    calls,
    reserveRisk: async (value) => { calls.reserve += 1; return value; },
    consumeAuthorization: async () => { calls.consume += 1; return { consumed: true }; },
    executeLifecycle: async () => { calls.execute += 1; return { order_id: "order-1" }; },
    finalizeNoOrder: async () => { calls.finalize += 1; }
  };
}

test("successful dry-run validates the immutable intent and sends no order", async () => {
  const input = fixture(true);
  const controls = spies();
  const result = await executeStrategyCanary({ ...input, ...controls });
  assert.equal(result.status, "strategy_intent_validated_no_order");
  assert.deepEqual(controls.calls, { reserve: 0, consume: 0, execute: 0, finalize: 0 });
});

test("funded-stage child explicitly accepts exact stage consumption and limited-live state", async () => {
  const input = fixture(true);
  input.documents.manifest.phase = "limited_live";
  input.documents.manifest.gate_metrics.phase = "shadow_passed";
  input.documents.manifest.funded_ladder = {
    phase: "limited_live", active_target_orders: 5, stage_authorized: true,
    human_grant_required: false, promotion_allowed: false
  };
  input.documents.authorization = {
    ...input.documents.authorization,
    schema: "polyedge.funded_stage_intent_authorization.v1",
    funded_stage_consumption_blob_name: input.config.humanGrantConsumptionBlobName,
    funded_stage_consumption_sha256: input.config.humanGrantConsumptionHash,
    funded_stage_source_state_sha256: `sha256:${"9".repeat(64)}`,
    funded_stage_target_orders: 5
  };
  delete input.documents.authorization.human_grant_id;
  delete input.documents.authorization.human_grant_sha256;
  delete input.documents.authorization.human_grant_consumption_blob_name;
  delete input.documents.authorization.human_grant_consumption_sha256;
  const controls = spies();
  const result = await executeStrategyCanary({ ...input, ...controls });
  assert.equal(result.status, "strategy_intent_validated_no_order");
  assert.deepEqual(controls.calls, { reserve: 0, consume: 0, execute: 0, finalize: 0 });
});

test("operator-funded child executes above the old one-dollar cap without claiming research promotion", async () => {
  const input = fixture(false);
  input.config.operatorDirect = true;
  input.config.trustBoundaryReady = false;
  input.config.maxOrderNotional = 10.5;
  input.config.campaignBaselineEquity = 11.09862;
  input.config.maxReconciliationDiscrepancy = 0.01;
  input.documents.intent.shares = "20";
  input.documents.intent.notional = "4.00";
  input.documents.manifest = {
    schema_version: "polyedge.operator_funded_session.v1",
    session_id: "dynamic-quote-funded-2026-07-27",
    authorization_mode: "operator_direct",
    authorized_by_user_reference: "Codex task 2026-07-27 funded Dynamic Quote",
    research_promotion_bypassed: true,
    research_lane_isolated: true,
    maker_only: true,
    no_deposits: true,
    max_open_orders: 1,
    max_order_notional: 10.5,
    max_account_loss: 11.09862,
    starting_collateral: 11.09862,
    max_reconciliation_discrepancy: 0.01,
    allow_automatic_replenishment: false,
    allow_compounding: false,
    external_cash_flows: [],
    shadow_validation: {
      required: true,
      mode: "isolated_paper_shadow",
      split: "time_ordered_70_30",
      eligible_transitions: 100,
      minimum_distinct_markets: 20,
      maximum_listed_failures: 0
    },
    candidate: {
      name: input.config.candidateName,
      candidate_version: input.config.candidateVersion,
      config_hash: input.config.candidateConfigHash
    },
    execution_model: {
      blob_uri: input.config.executionModelBlobUri,
      sha256: input.config.executionModelHash,
      model_version: input.config.requiredFillModelVersion
    },
    created_at: "2026-07-12T11:00:00.000Z",
    expires_at: "2026-07-12T13:00:00.000Z"
  };
  input.documents.authorization = {
    schema: "polyedge.operator_funded_intent_authorization.v1",
    authorization_id: "operator-funded-decision-1",
    authorization_mode: "operator_direct",
    session_id: input.documents.manifest.session_id,
    decision_id: input.documents.intent.decision_id,
    intent_blob_name: input.config.intentBlobName,
    intent_sha256: input.config.intentBlobHash,
    promotion_manifest_blob_name: input.config.manifestBlobName,
    promotion_manifest_sha256: input.config.manifestBlobHash,
    operator_session_manifest_blob_name: input.config.manifestBlobName,
    operator_session_manifest_sha256: input.config.manifestBlobHash,
    research_promotion_bypassed: true,
    candidate_name: input.config.candidateName,
    candidate_version: input.config.candidateVersion,
    candidate_config_hash: input.config.candidateConfigHash,
    required_fill_model_version: input.config.requiredFillModelVersion,
    execution_model_blob_uri: input.config.executionModelBlobUri,
    execution_model_sha256: input.config.executionModelHash,
    execution_model_container_name: "polyedge-research",
    execution_model_blob_name: "reports/research/venue-probe/conservative_execution_prior_v1.json",
    max_order_notional: 10.5,
    human_authorization_reference: "Codex task 2026-07-27 funded Dynamic Quote",
    authorized_at: "2026-07-12T12:00:10.000Z",
    expires_at: "2026-07-12T12:00:30.000Z",
    single_use: true
  };
  // Full-depth churn is expected on the live BTC book. Operator-direct may
  // tolerate it only inside the short bound while the current book still
  // passes the post-only price, tick, and venue minimum checks.
  input.runtime.book = {
    ...book,
    bids: [{ price: "0.19", size: "11" }]
  };
  input.runtime.risk = {
    passed: true,
    blockers: [],
    baseline_equity: 11.09862,
    cash_flow_adjusted_baseline: 11.09862,
    authorized_starting_collateral: 11.09862,
    no_replenishment: true,
    no_compounding: true,
    net_external_cash_flow: 0,
    cash_flow_count: 0,
    cash_flow_ids: [],
    maximum_reconciliation_discrepancy: 0.01,
    account_equity: 11.09862
  };
  const controls = spies();
  const result = await executeStrategyCanary({ ...input, ...controls });
  assert.equal(result.status, "funded_direct_executed");
  assert.deepEqual(controls.calls, { reserve: 1, consume: 1, execute: 1, finalize: 0 });
  assert.equal(input.documents.manifest.research_promotion_bypassed, true);
});

test("operator-funded preflight blocks unexpected capital and cash-flow records before reservation", async (t) => {
  for (const [name, mutate] of [
    ["unexpected deposit or profit", (input) => { input.runtime.risk.account_equity = 11.108622; }],
    ["cash-flow record", (input) => {
      input.runtime.risk.cash_flow_count = 1;
      input.runtime.risk.cash_flow_ids = ["deposit-1"];
    }]
  ]) {
    await t.test(name, async () => {
      const input = fixture(false);
      input.config.operatorDirect = true;
      input.config.trustBoundaryReady = false;
      input.config.maxOrderNotional = 10.5;
      input.config.campaignBaselineEquity = 11.09862;
      input.config.maxReconciliationDiscrepancy = 0.01;
      input.documents.intent.shares = "20";
      input.documents.intent.notional = "4.00";
      input.documents.manifest = {
        schema_version: "polyedge.operator_funded_session.v1",
        session_id: "dynamic-quote-funded-2026-07-28-v3",
        authorization_mode: "operator_direct",
        authorized_by_user_reference: "Codex task 2026-07-27 funded Dynamic Quote",
        research_promotion_bypassed: true,
        research_lane_isolated: true,
        maker_only: true,
        no_deposits: true,
        allow_automatic_replenishment: false,
        allow_compounding: false,
        external_cash_flows: [],
        shadow_validation: {
          required: true,
          mode: "isolated_paper_shadow",
          split: "time_ordered_70_30",
          eligible_transitions: 100,
          minimum_distinct_markets: 20,
          maximum_listed_failures: 0
        },
        max_open_orders: 1,
        max_order_notional: 10.5,
        max_account_loss: 11.09862,
        starting_collateral: 11.09862,
        max_reconciliation_discrepancy: 0.01,
        candidate: {
          name: input.config.candidateName,
          candidate_version: input.config.candidateVersion,
          config_hash: input.config.candidateConfigHash
        },
        execution_model: {
          blob_uri: input.config.executionModelBlobUri,
          sha256: input.config.executionModelHash,
          model_version: input.config.requiredFillModelVersion
        },
        created_at: "2026-07-12T11:00:00.000Z",
        expires_at: "2026-07-12T13:00:00.000Z"
      };
      input.documents.authorization = {
        ...input.documents.authorization,
        schema: "polyedge.operator_funded_intent_authorization.v1",
        authorization_mode: "operator_direct",
        session_id: input.documents.manifest.session_id,
        operator_session_manifest_blob_name: input.config.manifestBlobName,
        operator_session_manifest_sha256: input.config.manifestBlobHash,
        research_promotion_bypassed: true,
        max_order_notional: 10.5,
        expires_at: input.documents.intent.valid_until
      };
      delete input.documents.authorization.human_grant_id;
      delete input.documents.authorization.human_grant_sha256;
      delete input.documents.authorization.human_grant_consumption_blob_name;
      delete input.documents.authorization.human_grant_consumption_sha256;
      input.runtime.risk = {
        passed: true,
        blockers: [],
        baseline_equity: 11.09862,
        cash_flow_adjusted_baseline: 11.09862,
        authorized_starting_collateral: 11.09862,
        no_replenishment: true,
        no_compounding: true,
        net_external_cash_flow: 0,
        cash_flow_count: 0,
        cash_flow_ids: [],
        maximum_reconciliation_discrepancy: 0.01,
        account_equity: 11.09862
      };
      mutate(input);
      const controls = spies();
      await assert.rejects(
        executeStrategyCanary({ ...input, ...controls }),
        /no-replenishment\/no-compounding/
      );
      assert.deepEqual(controls.calls, { reserve: 0, consume: 0, execute: 0, finalize: 0 });
    });
  }
});

test("stale, book-hash, geoblock, clock, equity, model, and authorization failures send no order", async (t) => {
  const cases = [
    ["stale intent", (value) => { value.now = new Date("2026-07-12T12:03:00Z"); }, /stale/],
    ["missing GTD security buffer", (value) => { value.documents.intent.gtd_expiry_ts = value.documents.intent.valid_until; }, /300-second security buffer/],
    ["book hash", (value) => { value.documents.intent.book_hash = `sha256:${"f".repeat(64)}`; }, /book hash/],
    ["geoblock", (value) => { value.runtime.geoblock.blocked = true; }, /geoblock/],
    ["clock", (value) => { value.runtime.clockDriftMs = 6000; }, /clock drift/],
    ["equity", (value) => { value.runtime.risk = { passed: false, blockers: ["equity_floor_breached"] }; }, /equity\/risk/],
    ["model", (value) => { value.runtime.fillModelVersion = "wrong-model"; }, /fill-model/],
    ["model artifact hash", (value) => { value.documents.executionModelHash = `sha256:${"8".repeat(64)}`; }, /model hash or version/],
    ["model artifact version", (value) => { value.documents.executionModel.model_version = "wrong-model"; }, /model hash or version/],
    ["model trained on this order", (value) => { value.documents.executionModel.generated_at = value.documents.intent.decision_ts; }, /temporal prior/],
    ["authorization", (value) => { value.documents.authorization.human_authorization_reference = ""; }, /authorization/]
  ];
  for (const [name, mutate, pattern] of cases) {
    await t.test(name, async () => {
      const input = fixture(false);
      const controls = spies();
      mutate(input);
      await assert.rejects(executeStrategyCanary({ ...input, ...controls }), pattern);
      assert.equal(controls.calls.execute, 0);
      assert.equal(controls.calls.reserve, 0);
      assert.equal(controls.calls.consume, 0);
    });
  }
});

test("only the exact venue GTD rejection is classified as deterministic no-order", () => {
  assert.deepEqual(
    deterministicNoOrderRejection(new Error("invalid expiration value, must be in the future for GTD orders")),
    {
      code: "invalid_gtd_expiration",
      message: "invalid expiration value, must be in the future for GTD orders"
    }
  );
  assert.equal(deterministicNoOrderRejection(new Error("request timed out after signing")), null);
  assert.equal(deterministicNoOrderRejection({ response: { data: { error: "gateway unavailable" } } }), null);
});

test("deterministic no-order release requires complete zero-order, zero-position, zero-fill proof", () => {
  const proof = {
    error: new Error("invalid expiration value, must be in the future for GTD orders"),
    openOrderCount: 0,
    unresolvedPositionCount: 0,
    userChannelGapCount: 0,
    userChannelUnparsedCount: 0,
    postSendTradeCount: 0
  };
  assert.equal(validateDeterministicNoOrderReconciliation(proof).code, "invalid_gtd_expiration");
  for (const field of [
    "openOrderCount",
    "unresolvedPositionCount",
    "userChannelGapCount",
    "userChannelUnparsedCount",
    "postSendTradeCount"
  ]) {
    assert.throws(
      () => validateDeterministicNoOrderReconciliation({ ...proof, [field]: 1 }),
      /did not prove zero orders/
    );
  }
  assert.equal(
    validateDeterministicNoOrderReconciliation({ ...proof, error: new Error("signed request timed out") }),
    null
  );
});

test("blob content hash mismatch fails before JSON can reach execution", async () => {
  const bytes = Buffer.from('{"decision_id":"decision-1"}');
  const container = { getBlobClient: () => ({ download: async () => ({ readableStreamBody: Readable.from([bytes]) }) }) };
  await assert.rejects(loadHashedJson(container, "intent.json", `sha256:${"0".repeat(64)}`), /SHA-256 mismatch/);
  assert.equal((await loadHashedJson(container, "intent.json", sha256(bytes))).value.decision_id, "decision-1");
});

test("shares below the venue minimum_order_size fail before risk reservation", async () => {
  const input = fixture(false);
  input.documents.intent.shares = "4";
  input.documents.intent.notional = "0.80";
  const controls = spies();
  await assert.rejects(executeStrategyCanary({ ...input, ...controls }), /minimum_order_size/);
  assert.deepEqual(controls.calls, { reserve: 0, consume: 0, execute: 0, finalize: 0 });
});

test("one-shot authorization is atomically consumed and cannot replay", async () => {
  const names = new Set();
  const container = {
    getBlockBlobClient: (name) => ({
      uploadData: async (_bytes, options) => {
        assert.equal(options.conditions.ifNoneMatch, "*");
        if (names.has(name)) throw Object.assign(new Error("exists"), { statusCode: 412 });
        names.add(name);
      }
    })
  };
  const value = { authorization: { authorization_id: "approval-1" }, authorizationHash: `sha256:${"4".repeat(64)}`, decisionId: "decision-1", runId: "run-1", now };
  await consumeOneShotAuthorization(container, value);
  await assert.rejects(consumeOneShotAuthorization(container, value), /already consumed/);
});

test("authorization replay failure releases the no-order reservation and never signs", async () => {
  const input = fixture(false);
  const controls = spies();
  controls.consumeAuthorization = async () => { controls.calls.consume += 1; throw new Error("fail closed: one-shot authorization was already consumed"); };
  await assert.rejects(executeStrategyCanary({ ...input, ...controls }), /already consumed/);
  assert.deepEqual(controls.calls, { reserve: 1, consume: 1, execute: 0, finalize: 1 });
});

test("all per-fill markout deadlines are scheduled concurrently", async () => {
  const started = Date.now();
  const fills = [
    { id: "fill-a", size: 1, price: 0.4, timestampMs: started },
    { id: "fill-b", size: 2, price: 0.5, timestampMs: started }
  ];
  let visible = fills;
  const calls = [];
  const capture = beginFillMarkoutCapture({
    async getOrderBook(tokenId) {
      calls.push({ tokenId, at: Date.now() });
      return { bids: [{ price: "0.45", size: "3" }], asks: [{ price: "0.55", size: "3" }], hash: "a".repeat(40) };
    }
  }, "token-1", () => visible, {
    horizons: [10, 20, 30], horizonScaleMs: 1, pollMs: 1,
    feeParameters: { rate: 0, rateBps: 0, exponent: 0, takerOnly: true }
  });
  await new Promise((resolve) => setTimeout(resolve, 2));
  visible = [];
  const rows = await capture.finish(fills);
  assert.equal(rows.length, 6);
  assert.deepEqual([...new Set(rows.map((row) => row.fill_id))], ["fill-a", "fill-b"]);
  assert.deepEqual([...new Set(rows.map((row) => row.horizon_seconds))], [10, 20, 30]);
  assert.equal(calls.length, 6);
  assert.ok(rows.every((row) => row.fill_size > 0));
  assert.ok(rows.every((row) => row.midpoint !== null && row.executable_price !== null));
  assert.ok(rows.every((row) => row.request_started_at <= row.response_completed_at));
  assert.ok(rows.every((row) => row.observed_at === row.response_completed_at));
  assert.ok(rows.every((row) => row.response_duration_ms >= 0));
  assert.ok(rows.every((row) => /^sha256:[0-9a-f]{64}$/.test(row.book_hash)));
  assert.ok(Date.now() - started < 100, "concurrent deadlines should complete near the longest horizon");
});

test("markout delay is measured after the order-book response completes", async () => {
  const clock = [0, 1, 10_001];
  const capture = beginFillMarkoutCapture({
    async getOrderBook() {
      return {
        timestamp: 10_001,
        hash: "b".repeat(40),
        bids: [{ price: "0.45", size: "1" }],
        asks: [{ price: "0.55", size: "1" }]
      };
    }
  }, "token-1", () => [], {
    horizons: [1],
    horizonScaleMs: 1,
    pollMs: 1,
    nowMs: () => clock.shift() ?? 10_001
    ,feeParameters: { rate: 0, rateBps: 0, exponent: 0, takerOnly: true }
  });
  const [row] = await capture.finish([{ id: "fill-slow", size: 1, price: 0.4, timestampMs: 0 }]);
  assert.equal(row.request_started_at, "1970-01-01T00:00:00.001Z");
  assert.equal(row.response_completed_at, "1970-01-01T00:00:10.001Z");
  assert.equal(row.response_duration_ms, 10_000);
  assert.equal(row.observation_delay_ms, 10_000);
});
