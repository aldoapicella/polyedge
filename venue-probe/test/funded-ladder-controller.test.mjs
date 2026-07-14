import test from "node:test";
import assert from "node:assert/strict";
import { Readable } from "node:stream";
import { fundedChildEnvironment, runFundedLadderController } from "../src/funded-ladder-controller.mjs";
import { modelObservations } from "../src/lib.mjs";
import {
  canonicalBookHash,
  canonicalMarkoutBookHash,
  canonicalMarkoutBookSnapshot,
  sha256,
  validateCanaryPreflight
} from "../src/canary-lib.mjs";
import {
  buildFundedIntentAuthorization,
  buildFundedCheckpointEvidence,
  buildStageBlock,
  buildStageConsumption,
  canonicalStateHash,
  checkpointOneChainRoot,
  cumulativeProgressHash,
  loadFundedLadderConfig,
  putImmutableJson,
  progressPayloadHash,
  validateBeforeEveryOrder,
  validateProtocolV3ChildEvidence,
  validateProtocolV3ChildSummary,
  validateStageResume
} from "../src/funded-ladder-controller-lib.mjs";

const h = (char) => `sha256:${char.repeat(64)}`;
const now = new Date("2026-07-13T12:00:00Z");
const candidate = { name: "dynamic_quote_style", candidate_version: "dynamic_quote_style@test", config_hash: h("a") };
const settlementWallet = "0x1111111111111111111111111111111111111111";

function state() {
  const value = {
    schema_version: "funded_ladder_state_v1", campaign_id: "campaign", candidate,
    phase: "limited_live", stage_targets: [1, 5, 25, 100, 200], active_stage_index: 1,
    active_target_orders: 5, completed_checkpoints: [1],
    metrics: { cumulative_funded_orders: 1 }, maximum_calendar_days: 60,
    maximum_funded_orders: 200, maximum_drawdown: "1", human_grant_required: true,
    stage_authorized: false, consumed_grant_ids: ["canary"], terminal: false,
    promotion_allowed: false, created_at: now.toISOString(), updated_at: now.toISOString()
  };
  value.checkpoint_1_protocol_v3_artifact = { blob_name: "runs/run-1/summary.json", sha256: h("1") };
  value.checkpoint_1_terminal_artifact = { blob_name: "terminal/probe-1.json", sha256: h("5") };
  return value;
}

function inputs() {
  const funded = state();
  const manifest = {
    schema_version: "promotion_manifest_v1", phase: "limited_live", promotion_allowed: false,
    created_at: now.toISOString(), expires_at: new Date(now.getTime() + 60 * 86_400_000).toISOString(),
    candidate, execution_model: { blob_uri: "azure://st/models/model.json", sha256: h("b"), model_version: "queue-v1" },
    gate_metrics: { phase: "shadow_passed", promotion_allowed: true }, funded_ladder: funded
  };
  const grant = {
    schema_version: "funded_stage_grant_v1", grant_id: "stage-5", source_state_sha256: canonicalStateHash(funded),
    candidate, stage_target_orders: 5, single_use: true,
    authorized_at: now.toISOString(), expires_at: new Date(now.getTime() + 300_000).toISOString()
  };
  const config = {
    controlPrefix: "control/funded", manifestBlobName: "profitability/latest.json",
    intentPrefix: "intents", maxOrderNotional: 1, storageAccount: "st"
  };
  return { manifest, manifestHash: h("c"), grant, grantHash: h("d"), config };
}

function memoryContainer(initial = new Map(), { afterPersist } = {}) {
  const documents = new Map(initial);
  const etags = new Map([...documents.keys()].map((name, index) => [name, `"v${index + 1}"`]));
  let etagSequence = documents.size + 1;
  let writes = 0;
  return {
    documents,
    get writes() { return writes; },
    getBlobClient: (name) => ({ download: async () => {
      const bytes = documents.get(name);
      if (!bytes) throw Object.assign(new Error(`missing ${name}`), { statusCode: 404 });
      return { readableStreamBody: Readable.from([bytes]), etag: etags.get(name) };
    } }),
    getBlockBlobClient: (name) => ({ uploadData: async (bytes, options = {}) => {
      const conditions = options.conditions || {};
      if (conditions.ifNoneMatch === "*" && documents.has(name)) {
        throw Object.assign(new Error("exists"), { statusCode: 412 });
      }
      if (conditions.ifMatch && etags.get(name) !== conditions.ifMatch) {
        throw Object.assign(new Error("stale etag"), { statusCode: 412 });
      }
      const persisted = Buffer.from(bytes);
      const etag = `"v${etagSequence++}"`;
      documents.set(name, persisted);
      etags.set(name, etag);
      writes += 1;
      await afterPersist?.({ name, bytes: persisted, options, documents });
      return { etag };
    } }),
    listBlobsFlat: async function *({ prefix }) {
      for (const name of [...documents.keys()].sort()) if (name.startsWith(prefix)) yield { name };
    }
  };
}

function shadowGate(manifest) {
  return {
    ...structuredClone(manifest),
    phase: "shadow_passed",
    promotion_allowed: false,
    gate_metrics: { phase: "shadow_passed", promotion_allowed: true },
    created_at: new Date(now.getTime() - 60_000).toISOString(),
    expires_at: new Date(now.getTime() + 60_000).toISOString()
  };
}

function stageInitializationFixture({ afterPersist } = {}) {
  const source = inputs();
  const terminal = {
    schema: "polyedge.canary_terminal_risk_portfolio.v1",
    portfolio_reconciled: true,
    zero_open_orders_confirmed: true,
    unresolved_exposure: 0,
    unresolved_risk_reservations: 0
  };
  const terminalBytes = Buffer.from(JSON.stringify(terminal));
  source.manifest.funded_ladder.last_verified_terminal_artifact = {
    blob_name: "terminal/checkpoint-1.json",
    sha256: sha256(terminalBytes)
  };
  source.grant.source_state_sha256 = canonicalStateHash(source.manifest.funded_ladder);
  const manifestBytes = Buffer.from(JSON.stringify(source.manifest));
  const grantBytes = Buffer.from(JSON.stringify(source.grant));
  const control = memoryContainer(new Map([
    ["profitability/latest.json", manifestBytes],
    ["grants/stage-5.json", grantBytes],
    ["terminal/checkpoint-1.json", terminalBytes]
  ]), { afterPersist });
  const research = memoryContainer(new Map([
    ["reports/research/profitability/latest.json", Buffer.from(JSON.stringify(shadowGate(source.manifest)))]
  ]));
  const intents = memoryContainer();
  const env = {
    FUNDED_LADDER_CONTROLLER_ENABLED: "true",
    ALLOW_FUNDED_LADDER: "true",
    FUNDED_LADDER_DRY_RUN: "false",
    FUNDED_EVIDENCE_TRUST_BOUNDARY_READY: "true",
    FUNDED_LADDER_MANIFEST_BLOB_NAME: "profitability/latest.json",
    FUNDED_LADDER_MANIFEST_SHA256: sha256(manifestBytes),
    FUNDED_LADDER_GRANT_BLOB_NAME: "grants/stage-5.json",
    FUNDED_LADDER_GRANT_SHA256: sha256(grantBytes),
    FUNDED_LADDER_RESEARCH_CONTAINER_NAME: "polyedge-research",
    FUNDED_LADDER_INTENT_CONTAINER_NAME: "polyedge-shadow-events",
    AZURE_STORAGE_ACCOUNT_NAME: "st"
  };
  return { source, control, research, intents, env };
}

function protocolV3CheckpointEntries(count = 5) {
  const entries = Array.from({ length: count }, (_, index) => {
    const sequence = index + 1;
    const run = `run-${sequence}`;
    const probe = `probe-${sequence}`;
    const order = `order-${sequence}`;
    const started = new Date(Date.UTC(2026, 6, 13 + index, 12)).toISOString();
    const observed = new Date(Date.UTC(2026, 6, 13 + index, 13)).toISOString();
    const expectedControlBinding = {
      child_run_id: run,
      consumption_blob_name: `control/consumption-${sequence}.json`,
      consumption_sha256: h("b"),
      authorization_blob_name: `control/authorization-${sequence}.json`,
      authorization_sha256: h("c"),
      intent_blob_name: `control/intent-${sequence}.json`,
      intent_sha256: h("d"),
      manifest_blob_name: "profitability/latest.json",
      manifest_sha256: h("e"),
      prediction_model: { blob_uri: "azure://st/models/model.json", sha256: h("b"), model_version: "queue-v1" }
    };
    const entry = {
      sequence,
      summaryBinding: { blob_name: `runs/${run}/summary.json`, sha256: h(String(sequence)) },
      terminalBinding: { blob_name: `terminal/${probe}.json`, sha256: h(String(sequence + 4)) },
      progressBinding: sequence === 1 ? null : { blob_name: `progress/${sequence}.json`, sha256: h("f") },
      progress: null,
      expectedControlBinding,
      summary: {
        schema_version: 3,
        evidence_protocol_version: 3,
        status: "completed",
        run_id: run,
        started_ts: started,
        finished_ts: new Date(Date.parse(started) + 31_000).toISOString(),
        funder_address: settlementWallet,
        order_submission_attempted: true,
        order_submitted: true,
        submitted_order_count: 1,
        completed_probe_count: 1,
        candidate,
        prediction_model: {
          ...expectedControlBinding.prediction_model,
          generated_at: "2026-07-12T00:00:00Z",
          training_data_end_ts: null
        },
        provenance: {
          decision_id: `decision-${sequence}`,
          funded_stage_consumption_blob_name: expectedControlBinding.consumption_blob_name,
          funded_stage_consumption_sha256: expectedControlBinding.consumption_sha256,
          authorization_blob_name: expectedControlBinding.authorization_blob_name,
          authorization_sha256: expectedControlBinding.authorization_sha256,
          intent_blob_name: expectedControlBinding.intent_blob_name,
          intent_sha256: expectedControlBinding.intent_sha256,
          promotion_manifest_blob_name: expectedControlBinding.manifest_blob_name,
          promotion_manifest_sha256: expectedControlBinding.manifest_sha256
        },
        probes: [completeProtocolV3Probe({ probeId: probe, orderId: order, started })]
      },
      terminal: {
        schema: "polyedge.canary_terminal_risk_portfolio.v1",
        producer: "polyedge_node_authenticated_risk_terminal",
        source: "polymarket_data_api_plus_onchain_redemption",
        run_id: run,
        probe_id: probe,
        order_id: order,
        condition_id: `condition-${sequence}`,
        settlement_verified: true,
        settlement_transaction_hash: `0x${String(sequence).padStart(64, "0")}`,
        polygon_chain_id: 137,
        transaction_receipt_status: "success",
        transaction_block_number: sequence,
        transaction_receipt_confirmations: 2,
        redemption_condition_ids: [`condition-${sequence}`],
        settlement_wallet: settlementWallet,
        trust_boundary_ready: true,
        portfolio_reconciled: true,
        zero_open_orders_confirmed: true,
        unresolved_exposure: 0,
        unresolved_risk_reservations: 0,
        reconciliation_discrepancy: 0,
        campaign_starting_equity: 5,
        net_external_cash_flows: 0,
        liquid_collateral: 5 + sequence / 100,
        summed_position_value: 0,
        cash_flow_adjusted_ending_equity: 5 + sequence / 100,
        minimum_observed_equity: 5,
        maximum_observed_equity: 5 + sequence / 100,
        observed_at: observed
      }
    };
    return entry;
  });
  let cumulative = checkpointOneChainRoot(state());
  for (const entry of entries.slice(1)) {
    const payload = progressPayloadHash({
      sequence: entry.sequence,
      decisionId: `decision-${entry.sequence}`,
      expectedControlBinding: entry.expectedControlBinding,
      summaryBinding: entry.summaryBinding,
      terminalBinding: entry.terminalBinding
    });
    const next = cumulativeProgressHash(cumulative, payload);
    entry.progress = {
      schema: "polyedge.funded_stage_order_progress.v1",
      campaign_id: "campaign",
      candidate,
      sequence: entry.sequence,
      decision_id: `decision-${entry.sequence}`,
      protocol_v3_summary_blob_name: entry.summaryBinding.blob_name,
      protocol_v3_summary_sha256: entry.summaryBinding.sha256,
      terminal_evidence_blob_name: entry.terminalBinding.blob_name,
      terminal_evidence_sha256: entry.terminalBinding.sha256,
      expected_control_binding: entry.expectedControlBinding,
      progress_payload_sha256: payload,
      prior_cumulative_evidence_sha256: cumulative,
      cumulative_evidence_sha256: next
    };
    cumulative = next;
  }
  return entries;
}

function completeProtocolV3Probe({ probeId = "probe-1", orderId = "order-1", started = now.toISOString() } = {}) {
  const sendWallMs = Date.parse(started);
  const ackWallMs = sendWallMs + 100;
  const fillWallMs = ackWallMs + 500;
  const tradeId = `trade-${probeId}`;
  const context = {
    source: "public_market_channel_before_submission",
    captured_wall_ms: sendWallMs - 50,
    observed_trade_count: 2,
    observed_trade_size: 3,
    observed_depth_changes: 4,
    price_volatility: 0.01
  };
  return {
    schema_version: 3,
    evidence_protocol_version: 3,
    probe_id: probeId,
    status: "completed",
    order_submitted: true,
    market: { conditionId: `condition-${probeId.replace("probe-", "") || "1"}`, tokenId: "token-1", endTs: null },
    order: { side: "BUY", size: 1, price: 0.2, spread: 0.02, inferredSizeAhead: 4 },
    pre_send_context: context,
    lifecycle: {
      order_id: orderId,
      send_wall_ms: sendWallMs,
      ack_wall_ms: ackWallMs,
      client_to_http_ack_ms: 100,
      clock_server_minus_local_ms: 0,
      clock_round_trip_ms: 10,
      clock_uncertainty_ms: 5,
      cancel_send_wall_ms: null,
      client_cancel_round_trip_ms: null,
      client_to_user_cancel_ack_ms: null,
      live_duration_ms: 1_000,
      first_fill_after_ack_ms: 500,
      actual_matched_size: 1,
      venue_fee_model: "polymarket_clob_v2_curve",
      venue_fee_rate: 0,
      venue_fee_rate_bps: 0,
      venue_fee_exponent: 0,
      venue_fee_taker_only: true,
      estimated_round_trip_cost_per_share: 0,
      partial_fill: false,
      fully_filled: true,
      fill_raced_cancellation: false,
      post_cancel_fill_count: 0,
      first_fill_after_cancel_ms: null,
      public_touch_trade_count: 1,
      public_strict_trade_through_count: 1,
      public_trade_through_without_fill_count: 0,
      related_trade_ids: [tradeId],
      live_user_trade_ids: [tradeId],
      rest_order_matched_size: 1,
      user_order_matched_size: 1,
      rest_trade_matched_size: 1,
      user_trade_matched_size: 1,
      matched_size_source_agreement: true,
      trade_id_source_agreement: true,
      rest_order_returned: true,
      authenticated_user_channel_reconnects: 0,
      public_market_channel_reconnects: 0,
      authenticated_user_channel_unparsed: 0,
      public_market_channel_unparsed: 0,
      authenticated_user_channel_duplicates: 0,
      public_market_channel_duplicates: 0,
      post_cancel_finality_stable: true,
      post_cancel_observation_ms: 10_000,
      reconciliation_complete: true,
      zero_open_orders_confirmed: true,
      data_gap_detected: false,
      cancellation_failure: false,
      markout_capture_complete: true
    },
    markouts: [1, 5, 30].map((horizon) => {
      const target = fillWallMs + horizon * 1_000;
      const rawOrderbook = canonicalMarkoutBookSnapshot({
        tick_size: "0.01", min_order_size: "1", hash: "1".repeat(40),
        bids: [{ price: "0.22", size: "2" }], asks: [{ price: "0.24", size: "2" }]
      }, "token-1");
      return {
        fill_id: tradeId,
        horizon_seconds: horizon,
        fill_timestamp: new Date(fillWallMs).toISOString(),
        venue_fill_timestamp: new Date(fillWallMs).toISOString(),
        target_observation_ts: new Date(target).toISOString(),
        request_started_at: new Date(target).toISOString(),
        response_completed_at: new Date(target + 100).toISOString(),
        observed_at: new Date(target + 100).toISOString(),
        response_duration_ms: 100,
        observation_delay_ms: 100,
        raw_orderbook: rawOrderbook,
        book_hash: canonicalMarkoutBookHash(rawOrderbook),
        venue_book_hash: "1".repeat(40),
        venue_book_timestamp: new Date(target + 100).toISOString(),
        fill_size: 1,
        fill_price: 0.2,
        trader_side: null,
        authenticated_order_role: null,
        authenticated_fee_rate_bps: null,
        authenticated_fee_amount: null,
        authenticated_fee_raw: null,
        entry_fee_per_share: 0,
        hypothetical_exit_fee_per_share: 0,
        round_trip_fee_per_share: 0,
        midpoint: 0.23,
        executable_price: 0.22,
        midpoint_markout_per_share: 0.03,
        executable_markout_per_share: 0.02
      };
    }),
    model_observations: [1, 5, 30, 60].map((horizon) => ({
      horizon_seconds: horizon,
      order_submitted: true,
      eligible: true,
      label_observed: true,
      quality_eligible: true,
      filled: true,
      reconciliation_complete: true,
      zero_open_orders_confirmed: true,
      data_gap_detected: false,
      cancellation_failure: false,
      markout_complete: true,
      markout_timing_valid: true,
      executable_markout_30s_per_share: 0.02,
      venue_fee_model: "polymarket_clob_v2_curve",
      venue_fee_rate: 0,
      venue_fee_rate_bps: 0,
      venue_fee_exponent: 0,
      venue_fee_taker_only: true,
      entry_fee_per_share: 0,
      hypothetical_exit_fee_per_share: 0,
      estimated_round_trip_cost_per_share: 0,
      inferred_size_ahead: 4,
      spread: 0.02,
      order_price: 0.2,
      order_size: 1,
      time_to_expiry_seconds: null,
      pre_send_trade_size: context.observed_trade_size,
      pre_send_depth_changes: context.observed_depth_changes,
      pre_send_volatility: context.price_volatility
    }))
  };
}

test("module is import-safe and deployed defaults remain disabled", () => {
  assert.throws(() => loadFundedLadderConfig({}), /ENABLED must be true/);
});

test("target-200 child inherits the canonical transitioned queue model version", () => {
  const child = fundedChildEnvironment({ STRATEGY_CANARY_REQUIRED_FILL_MODEL_VERSION: "conservative-execution-prior-v1" }, {
    config: { dryRun: false },
    manifestDocument: { blobName: "manifest.json", hash: h("a"), value: { execution_model: { blob_uri: "azure://st/models/model.json", sha256: h("b"), model_version: "queue-calibration-v1" } } },
    consumptionDocument: { blobName: "consumption.json", hash: h("c"), value: { grant_id: "grant", source_state_sha256: h("d") } },
    grantHash: h("e"), authorization: { blobName: "authorization.json", hash: h("f") },
    intent: { blobName: "intent.json", hash: h("1") }, childRunId: "funded-200"
  });
  assert.equal(child.STRATEGY_CANARY_REQUIRED_FILL_MODEL_VERSION, "queue-calibration-v1");
});

test("dry-run validates exact isolated inputs without writes or child execution", async () => {
  const source = inputs();
  const manifestBytes = Buffer.from(JSON.stringify(source.manifest));
  const grantBytes = Buffer.from(JSON.stringify(source.grant));
  const shadowBytes = Buffer.from(JSON.stringify({
    ...source.manifest, phase: "shadow_passed", promotion_allowed: false,
    gate_metrics: { phase: "shadow_passed", promotion_allowed: true },
    created_at: new Date(now.getTime() - 60_000).toISOString(),
    expires_at: new Date(now.getTime() + 60_000).toISOString()
  }));
  let writes = 0;
  const fake = (documents) => ({
    getBlobClient: (name) => ({ download: async () => {
      const bytes = documents.get(name);
      if (!bytes) throw Object.assign(new Error("missing"), { statusCode: 404 });
      return { readableStreamBody: Readable.from([bytes]), etag: '"etag"' };
    } }),
    getBlockBlobClient: () => ({ uploadData: async () => { writes += 1; } })
  });
  const research = fake(new Map([["reports/research/profitability/latest.json", shadowBytes]]));
  const control = fake(new Map([["profitability/latest.json", manifestBytes], ["grants/stage-5.json", grantBytes]]));
  const result = await runFundedLadderController({
    env: {
      FUNDED_LADDER_CONTROLLER_ENABLED: "true", ALLOW_FUNDED_LADDER: "true", FUNDED_LADDER_DRY_RUN: "true",
      FUNDED_LADDER_MANIFEST_BLOB_NAME: "profitability/latest.json", FUNDED_LADDER_MANIFEST_SHA256: sha256(manifestBytes),
      FUNDED_LADDER_GRANT_BLOB_NAME: "grants/stage-5.json", FUNDED_LADDER_GRANT_SHA256: sha256(grantBytes),
      FUNDED_LADDER_RESEARCH_CONTAINER_NAME: "polyedge-research", FUNDED_LADDER_INTENT_CONTAINER_NAME: "polyedge-shadow-events"
    },
    containers: { control, research, intents: fake(new Map()) },
    clock: () => now,
    invokeChild: async () => { throw new Error("child must not run in dry-run"); }
  });
  assert.equal(result.status, "funded_stage_dry_run_validated");
  assert.equal(result.writes, 0);
  assert.equal(writes, 0);
});

test("stage-init artifacts are byte-stable across retry-local clocks and run IDs", () => {
  const source = inputs();
  const first = buildStageConsumption({ ...source, runId: "retry-local-a", now });
  const second = buildStageConsumption({ ...source, runId: "retry-local-b", now: new Date(now.getTime() + 1_000) });
  assert.deepEqual(second, first);
  assert.equal(first.value.run_id, `stage-init-${source.grantHash.slice("sha256:".length, "sha256:".length + 24)}`);
  assert.equal(first.value.consumed_at, source.grant.authorized_at);
  assert.equal(first.authorizedManifest.funded_ladder.updated_at, source.grant.authorized_at);
});

test("stage initialization recovers exact immutable writes and an identical-writer CAS race", async (t) => {
  await t.test("retry reuses consumption and authorized manifest after a persisted-write crash", async () => {
    let crashOnce = true;
    const fixture = stageInitializationFixture({
      afterPersist: ({ name }) => {
        if (crashOnce && name.includes("/stage-manifests/")) {
          crashOnce = false;
          throw new Error("simulated crash after authorized manifest persistence");
        }
      }
    });
    const invoke = () => runFundedLadderController({
      env: fixture.env,
      containers: { control: fixture.control, research: fixture.research, intents: fixture.intents },
      clock: () => now,
      invokeChild: async () => { throw new Error("no child expected without an intent"); }
    });
    await assert.rejects(invoke(), /simulated crash after authorized manifest persistence/);
    const consumptionName = [...fixture.control.documents.keys()].find((name) => name.includes("/stage-consumptions/"));
    const authorizedName = [...fixture.control.documents.keys()].find((name) => name.includes("/stage-manifests/"));
    assert.ok(consumptionName);
    assert.ok(authorizedName);
    const firstConsumption = Buffer.from(fixture.control.documents.get(consumptionName));
    const firstAuthorized = Buffer.from(fixture.control.documents.get(authorizedName));

    const resumed = await invoke();
    assert.equal(resumed.status, "stage_waiting_for_fresh_intent");
    assert.deepEqual(fixture.control.documents.get(consumptionName), firstConsumption);
    assert.deepEqual(fixture.control.documents.get(authorizedName), firstAuthorized);

    // The original source-manifest hash remains pinned in the retry command,
    // while canonical latest now holds the authorized hash. Recovery must use
    // the exact persisted grant/consumption/authorized-manifest chain.
    const resumedAfterCas = await invoke();
    assert.equal(resumedAfterCas.status, "stage_waiting_for_fresh_intent");
    assert.deepEqual(fixture.control.documents.get(consumptionName), firstConsumption);
    assert.deepEqual(fixture.control.documents.get(authorizedName), firstAuthorized);
  });

  await t.test("a 412 after an identical writer committed canonical latest is accepted", async () => {
    let identicalCasRaceOnce = true;
    const fixture = stageInitializationFixture({
      afterPersist: ({ name, options }) => {
        if (identicalCasRaceOnce && name === "profitability/latest.json" && options.conditions?.ifMatch) {
          identicalCasRaceOnce = false;
          throw Object.assign(new Error("simulated identical-writer CAS race"), { statusCode: 412 });
        }
      }
    });
    const result = await runFundedLadderController({
      env: fixture.env,
      containers: { control: fixture.control, research: fixture.research, intents: fixture.intents },
      clock: () => now,
      invokeChild: async () => { throw new Error("no child expected without an intent"); }
    });
    assert.equal(result.status, "stage_waiting_for_fresh_intent");
    assert.equal(identicalCasRaceOnce, false);
  });
});

test("concurrent stage grants CAS canonical latest so exactly one authorization wins", async () => {
  const source = inputs();
  const terminal = {
    schema: "polyedge.canary_terminal_risk_portfolio.v1",
    portfolio_reconciled: true,
    zero_open_orders_confirmed: true,
    unresolved_exposure: 0,
    unresolved_risk_reservations: 0
  };
  const terminalBytes = Buffer.from(JSON.stringify(terminal));
  source.manifest.funded_ladder.last_verified_terminal_artifact = {
    blob_name: "terminal/canary.json",
    sha256: sha256(terminalBytes)
  };
  const manifestBytes = Buffer.from(JSON.stringify(source.manifest));
  const grants = ["stage-5-a", "stage-5-b"].map((grantId) => ({
    ...structuredClone(source.grant),
    grant_id: grantId,
    source_state_sha256: canonicalStateHash(source.manifest.funded_ladder)
  }));
  const grantBytes = grants.map((grant) => Buffer.from(JSON.stringify(grant)));
  const shadowBytes = Buffer.from(JSON.stringify({
    ...source.manifest,
    phase: "shadow_passed",
    promotion_allowed: false,
    gate_metrics: { phase: "shadow_passed", promotion_allowed: true },
    created_at: new Date(now.getTime() - 60_000).toISOString(),
    expires_at: new Date(now.getTime() + 60_000).toISOString()
  }));

  let releaseCanonicalReads;
  const canonicalReadsReleased = new Promise((resolve) => { releaseCanonicalReads = resolve; });
  let canonicalReadCount = 0;
  let canonicalCasAttempts = 0;
  let etagSequence = 1;
  const memoryContainer = (initial, { synchronizeCanonicalReads = false } = {}) => {
    const documents = new Map(initial);
    const etags = new Map([...documents.keys()].map((name) => [name, `"v${etagSequence}"`]));
    return {
      documents,
      getBlobClient: (name) => ({ download: async () => {
        const bytes = documents.get(name);
        const etag = etags.get(name);
        if (!bytes) throw Object.assign(new Error("missing"), { statusCode: 404 });
        if (synchronizeCanonicalReads && name === "profitability/latest.json" && canonicalReadCount < 2) {
          canonicalReadCount += 1;
          if (canonicalReadCount === 2) releaseCanonicalReads();
          await canonicalReadsReleased;
        }
        return { readableStreamBody: Readable.from([bytes]), etag };
      } }),
      getBlockBlobClient: (name) => ({ uploadData: async (bytes, options = {}) => {
        const conditions = options.conditions || {};
        if (conditions.ifNoneMatch === "*" && documents.has(name)) {
          throw Object.assign(new Error("exists"), { statusCode: 412 });
        }
        if (conditions.ifMatch) {
          canonicalCasAttempts += 1;
          if (etags.get(name) !== conditions.ifMatch) {
            throw Object.assign(new Error("stale etag"), { statusCode: 412 });
          }
        }
        const nextEtag = `"v${++etagSequence}"`;
        documents.set(name, Buffer.from(bytes));
        etags.set(name, nextEtag);
        return { etag: nextEtag };
      } }),
      listBlobsFlat: async function *({ prefix }) {
        for (const name of documents.keys()) if (name.startsWith(prefix)) yield { name };
      }
    };
  };

  const control = memoryContainer(new Map([
    ["profitability/latest.json", manifestBytes],
    ["grants/stage-5-a.json", grantBytes[0]],
    ["grants/stage-5-b.json", grantBytes[1]],
    ["terminal/canary.json", terminalBytes]
  ]), { synchronizeCanonicalReads: true });
  const research = memoryContainer(new Map([["reports/research/profitability/latest.json", shadowBytes]]));
  const intents = memoryContainer(new Map());
  const envFor = (index) => ({
    FUNDED_LADDER_CONTROLLER_ENABLED: "true",
    ALLOW_FUNDED_LADDER: "true",
    FUNDED_LADDER_DRY_RUN: "false",
    FUNDED_EVIDENCE_TRUST_BOUNDARY_READY: "true",
    FUNDED_LADDER_MANIFEST_BLOB_NAME: "profitability/latest.json",
    FUNDED_LADDER_MANIFEST_SHA256: sha256(manifestBytes),
    FUNDED_LADDER_GRANT_BLOB_NAME: `grants/${grants[index].grant_id}.json`,
    FUNDED_LADDER_GRANT_SHA256: sha256(grantBytes[index]),
    FUNDED_LADDER_RESEARCH_CONTAINER_NAME: "polyedge-research",
    FUNDED_LADDER_INTENT_CONTAINER_NAME: "polyedge-shadow-events"
  });

  const outcomes = await Promise.allSettled([0, 1].map((index) => runFundedLadderController({
    env: envFor(index),
    containers: { control, research, intents },
    clock: () => now,
    invokeChild: async () => { throw new Error("no fresh intent means no child execution"); }
  })));
  const winner = outcomes.find((outcome) => outcome.status === "fulfilled");
  const loser = outcomes.find((outcome) => outcome.status === "rejected");
  assert.equal(winner?.value.status, "stage_waiting_for_fresh_intent");
  assert.match(loser?.reason.message, /canonical manifest authorization CAS lost/);
  assert.equal(canonicalCasAttempts, 2);

  const canonicalBytes = control.documents.get("profitability/latest.json");
  const canonical = JSON.parse(canonicalBytes.toString("utf8"));
  const wonGrant = grants.find((grant) => canonical.funded_ladder.consumed_grant_ids.includes(grant.grant_id));
  const lostGrant = grants.find((grant) => grant.grant_id !== wonGrant?.grant_id);
  assert.ok(wonGrant);
  assert.ok(lostGrant);
  assert.equal(canonical.funded_ladder.stage_authorized, true);
  const campaign = sha256(Buffer.from(canonical.funded_ladder.campaign_id)).slice("sha256:".length);
  const losingConsumption = JSON.parse(control.documents.get(`reports/research/venue-probe/control/funded-ladder/campaigns/${campaign}/stage-consumptions/${lostGrant.grant_id}.json`).toString("utf8"));
  assert.throws(() => validateStageResume({
    manifest: canonical,
    manifestHash: sha256(canonicalBytes),
    consumption: losingConsumption,
    now
  }), /exact authorized stage manifest/);
});

test("five-minute grant atomically initiates a durable multi-day stage consumption", () => {
  const source = inputs();
  const document = buildStageConsumption({ ...source, runId: "run-1", now });
  assert.equal(document.value.quota_orders, 4);
  assert.equal(document.authorizedManifest.funded_ladder.stage_authorized, true);
  assert.equal(document.authorizedManifest.funded_ladder.human_grant_required, false);
  assert.match(document.value.authorized_manifest_sha256, /^sha256:[0-9a-f]{64}$/);
  assert.equal(document.value.authorized_state_sha256, canonicalStateHash(document.authorizedManifest.funded_ladder));
  assert.notEqual(document.value.authorized_state_sha256, document.value.source_state_sha256);
  const resumed = validateStageResume({
    manifest: document.authorizedManifest,
    manifestHash: document.value.authorized_manifest_sha256,
    consumption: document.value,
    now
  });
  assert.equal(resumed.remainingQuota, 4);
  // The five-minute grant can expire after durable consumption; the 60-day
  // canonical campaign window still applies before every order.
  assert.doesNotThrow(() => validateBeforeEveryOrder({
    manifest: document.authorizedManifest,
    manifestHash: document.value.authorized_manifest_sha256,
    consumption: document.value,
    completedDecisionIds: ["one", "two"],
    runtime: { openOrderCount: 0, unresolvedExposure: 0, unresolvedReservations: 0, riskPassed: true },
    now
  }));
});

test("campaign-scoped control paths prevent old progress from contaminating a new campaign", () => {
  const first = inputs();
  const firstDocument = buildStageConsumption({ ...first, runId: "run-1", now });
  const second = inputs();
  second.manifest.funded_ladder.campaign_id = "campaign-2";
  second.grant.source_state_sha256 = canonicalStateHash(second.manifest.funded_ladder);
  const secondDocument = buildStageConsumption({ ...second, runId: "run-2", now });
  assert.notEqual(firstDocument.value.campaign_control_id, secondDocument.value.campaign_control_id);
  assert.notEqual(firstDocument.blobName, secondDocument.blobName);
  assert.match(firstDocument.blobName, new RegExp(`/campaigns/${firstDocument.value.campaign_control_id}/`));
});

test("resume requires the exact authorized canonical manifest and rejects drift, quota exhaustion, and unresolved risk", () => {
  const source = inputs();
  const document = buildStageConsumption({ ...source, runId: "run-1", now });
  assert.doesNotThrow(() => validateStageResume({ manifest: document.authorizedManifest, manifestHash: document.value.authorized_manifest_sha256, consumption: document.value, now }));
  assert.throws(() => validateStageResume({ manifest: { ...document.authorizedManifest, created_at: "2026-07-14T00:00:00Z" }, manifestHash: h("e"), consumption: document.value, now }), /validity window|exact authorized stage manifest/);
  const drifted = structuredClone(document.authorizedManifest);
  drifted.funded_ladder.metrics.cumulative_funded_orders = 2;
  assert.throws(() => validateStageResume({ manifest: drifted, manifestHash: document.value.authorized_manifest_sha256, consumption: document.value, now }), /authorized canonical state changed/);
  const base = { manifest: document.authorizedManifest, manifestHash: document.value.authorized_manifest_sha256, consumption: document.value, now };
  assert.throws(() => validateBeforeEveryOrder({ ...base, completedDecisionIds: ["1", "2", "3", "4"], runtime: { openOrderCount: 0, unresolvedExposure: 0, unresolvedReservations: 0, riskPassed: true } }), /quota is exhausted/);
  assert.throws(() => validateBeforeEveryOrder({ ...base, completedDecisionIds: [], runtime: { openOrderCount: 0, unresolvedExposure: 1, unresolvedReservations: 0, riskPassed: true } }), /unresolved exposure/);
  assert.throws(() => validateBeforeEveryOrder({
    ...base,
    now: new Date(document.authorizedManifest.expires_at),
    completedDecisionIds: [],
    runtime: { openOrderCount: 0, unresolvedExposure: 0, unresolvedReservations: 0, riskPassed: true }
  }), /expired/);
});

test("every funded intent authorization binds stage consumption and canonical state", () => {
  const source = inputs();
  const consumption = buildStageConsumption({ ...source, runId: "run-1", now });
  const intent = { decision_id: "f".repeat(64), valid_until: new Date(now.getTime() + 30_000).toISOString() };
  const authorization = buildFundedIntentAuthorization({
    config: source.config, manifest: consumption.authorizedManifest,
    manifestHash: consumption.value.authorized_manifest_sha256,
    consumptionDocument: { value: consumption.value, blobName: consumption.blobName },
    consumptionHash: h("e"), intentDocument: { value: intent, blobName: `intents/${intent.decision_id}.json`, hash: h("f") },
    childRunId: "funded-5-test", now
  });
  assert.equal(authorization.value.funded_stage_target_orders, 5);
  assert.equal(authorization.value.funded_stage_source_state_sha256, consumption.value.source_state_sha256);
  assert.equal(authorization.value.promotion_manifest_sha256, consumption.value.authorized_manifest_sha256);
  assert.equal(authorization.value.child_run_id, "funded-5-test");
  assert.equal(authorization.value.execution_model_container_name, "models");
  assert.equal(authorization.value.execution_model_blob_name, "model.json");
});

test("checkpoint-100 target-200 queue-model authorization passes funded preflight and rejects exact model drift", async (t) => {
  const source = inputs();
  const modelHash = h("b");
  const modelBlobName = `reports/research/venue-probe/models/queue-calibration-v1-${modelHash.slice("sha256:".length)}.json`;
  const modelBlobUri = `azure://st/models/${modelBlobName}`;
  source.manifest.human_authorization_required = true;
  source.manifest.execution_model = {
    blob_uri: modelBlobUri,
    sha256: modelHash,
    model_version: "queue-calibration-v1"
  };
  Object.assign(source.manifest.funded_ladder, {
    active_stage_index: 4,
    active_target_orders: 200,
    completed_checkpoints: [1, 5, 25, 100],
    metrics: { cumulative_funded_orders: 100 }
  });
  source.grant = {
    ...source.grant,
    grant_id: "stage-200-after-checkpoint-100",
    source_state_sha256: canonicalStateHash(source.manifest.funded_ladder),
    stage_target_orders: 200
  };

  const consumption = buildStageConsumption({ ...source, runId: "target-200-run", now });
  const book = {
    tick_size: "0.01",
    min_order_size: "5",
    bids: [{ price: "0.19", size: "10" }],
    asks: [{ price: "0.21", size: "10" }]
  };
  const decisionTs = new Date(now.getTime() - 1_000);
  const validUntil = new Date(decisionTs.getTime() + 30_000);
  const intent = {
    schema: "polyedge.execution_intent.v1",
    decision_id: "f".repeat(64),
    candidate_name: candidate.name,
    candidate_version: candidate.candidate_version,
    candidate_config_hash: candidate.config_hash,
    market_id: "market-200",
    condition_id: "condition-200",
    token_id: "token-200",
    outcome: "up",
    side: "BUY",
    price: "0.20",
    shares: "5",
    notional: "1.00",
    minimum_order_size: "5",
    post_only: true,
    order_kind: "post_only_gtd",
    ttl_ms: 30_000,
    decision_ts: decisionTs.toISOString(),
    valid_until: validUntil.toISOString(),
    gtd_expiry_ts: new Date(validUntil.getTime() + 60_000).toISOString(),
    book_hash: canonicalBookHash(book, "token-200"),
    q: "0.25",
    gross_edge: "0.05",
    fee_allowance: "0.005",
    slippage_allowance: "0.005",
    toxicity_allowance: "0.01",
    net_edge_lower_bound: "0.03",
    regime: "normal",
    features_digest: h("3"),
    reference_age_ms: 100,
    book_age_ms: 80,
    required_fill_model_version: "queue-calibration-v1",
    execution_model_blob_uri: modelBlobUri,
    execution_model_sha256: modelHash,
    execution_model_container_name: "models",
    execution_model_blob_name: modelBlobName,
    resolution_source: "chainlink_reference",
    exact_resolution_source: true
  };
  const intentDocument = {
    value: intent,
    blobName: `intents/${intent.decision_id}.json`,
    hash: h("1")
  };
  const consumptionHash = h("e");
  const authorization = buildFundedIntentAuthorization({
    config: source.config,
    manifest: consumption.authorizedManifest,
    manifestHash: consumption.value.authorized_manifest_sha256,
    consumptionDocument: { value: consumption.value, blobName: consumption.blobName },
    consumptionHash,
    intentDocument,
    childRunId: "funded-200-integration",
    now
  }).value;
  const canaryConfig = {
    candidateName: candidate.name,
    candidateVersion: candidate.candidate_version,
    candidateConfigHash: candidate.config_hash,
    requiredFillModelVersion: "queue-calibration-v1",
    executionModelBlobUri: modelBlobUri,
    executionModelHash: modelHash,
    storageAccount: "st",
    requiredResolutionSource: "chainlink_reference",
    maxOrderNotional: 1,
    maxReferenceAgeMs: 2_000,
    maxBookAgeMs: 1_000,
    maxClockDriftMs: 5_000,
    maxClockUncertaintyMs: 750,
    expectedCountry: "IE",
    expectedEgressIp: "203.0.113.8",
    intentBlobName: intentDocument.blobName,
    intentBlobHash: intentDocument.hash,
    manifestBlobName: source.config.manifestBlobName,
    manifestBlobHash: consumption.value.authorized_manifest_sha256,
    humanGrantConsumptionBlobName: consumption.blobName,
    humanGrantConsumptionHash: consumptionHash
  };
  const executionModel = {
    schema: "polyedge.execution_queue_model.v1",
    model_version: "queue-calibration-v1",
    generated_at: new Date(decisionTs.getTime() - 60_000).toISOString(),
    training_data_end_ts: new Date(decisionTs.getTime() - 120_000).toISOString()
  };
  const runtime = {
    geoblock: { blocked: false, country: "IE", ip: canaryConfig.expectedEgressIp },
    clockDriftMs: 25,
    clockServerMinusLocalMs: 25,
    clockRoundTripMs: 100,
    clockUncertaintyMs: 550,
    risk: { passed: true, blockers: [] },
    openOrderCount: 0,
    market: { marketId: intent.market_id, conditionId: intent.condition_id, tokenId: intent.token_id, acceptingOrders: true, closed: false },
    book,
    feeModel: "polymarket_clob_v2_curve",
    feeRate: 0,
    feeRateBps: 0,
    feeExponent: 0,
    feeTakerOnly: true,
    fillModelVersion: "queue-calibration-v1",
    exactResolutionSource: true,
    resolutionSource: "chainlink_reference"
  };
  const preflight = {
    config: canaryConfig,
    intent,
    manifest: consumption.authorizedManifest,
    authorization,
    executionModel,
    executionModelHash: modelHash,
    runtime,
    now
  };

  assert.equal(consumption.authorizedManifest.funded_ladder.active_target_orders, 200);
  assert.deepEqual(consumption.authorizedManifest.funded_ladder.completed_checkpoints, [1, 5, 25, 100]);
  assert.deepEqual({
    container: authorization.execution_model_container_name,
    blob: authorization.execution_model_blob_name,
    version: authorization.required_fill_model_version,
    hash: authorization.execution_model_sha256
  }, {
    container: "models",
    blob: modelBlobName,
    version: "queue-calibration-v1",
    hash: modelHash
  });
  assert.doesNotThrow(() => validateCanaryPreflight(preflight));

  const drifts = [
    ["container", "execution_model_container_name", "wrong-models", /container\/blob provenance mismatch/],
    ["blob", "execution_model_blob_name", `${modelBlobName}.drift`, /container\/blob provenance mismatch/],
    ["version", "required_fill_model_version", "queue-calibration-v2", /candidate or fill-model binding mismatch/],
    ["hash", "execution_model_sha256", h("9"), /exact model artifact binding mismatch/]
  ];
  for (const [name, field, value, pattern] of drifts) {
    await t.test(`${name} drift fails closed`, () => {
      const drifted = { ...authorization, [field]: value };
      assert.throws(() => validateCanaryPreflight({ ...preflight, authorization: drifted }), pattern);
    });
  }
});

test("immutable conditional write fails closed after crash/replay", async () => {
  let writes = 0;
  const container = { getBlockBlobClient: () => ({ uploadData: async (_bytes, options) => {
    assert.equal(options.conditions.ifNoneMatch, "*");
    writes += 1;
    if (writes > 1) throw Object.assign(new Error("exists"), { statusCode: 412 });
  } }) };
  const document = { blobName: "control/once.json", value: { ok: true } };
  await putImmutableJson(container, document);
  await assert.rejects(() => putImmutableJson(container, document), /already exists/);
});

test("stage block is immutable-path and exact canonical manifest/state bound", () => {
  const source = inputs();
  const consumption = buildStageConsumption({ ...source, runId: "run-1", now });
  const block = buildStageBlock({
    config: source.config,
    consumption: consumption.value,
    decisionId: "decision-1",
    childRunId: "child-1",
    reason: " terminal reconciliation failed ",
    now
  });
  assert.match(block.blobName, /\/stage-blocks\/stage-5\/decision-1\.json$/);
  assert.equal(block.value.source_manifest_sha256, consumption.value.authorized_manifest_sha256);
  assert.equal(block.value.source_state_sha256, consumption.value.authorized_state_sha256);
  assert.equal(block.value.stage_target_orders, 5);
  assert.equal(block.value.reason, "terminal reconciliation failed");
  assert.throws(() => buildStageBlock({
    config: source.config,
    consumption: { ...consumption.value, authorized_state_sha256: "forged" },
    decisionId: "decision-1",
    reason: "failed",
    now
  }), /manifest\/state hashes/);
});

test("filled protocol-v3 evidence can pause pending terminal, but terminal admission is exact identity-bound", () => {
  const consumption = buildStageConsumption({ ...inputs(), runId: "run-1", now }).value;
  const decisionId = "f".repeat(64);
  const expectedBinding = {
    child_run_id: "funded-run",
    consumption_blob_name: "control/stage-consumption.json",
    consumption_sha256: h("e"),
    authorization_blob_name: "control/authorization.json",
    authorization_sha256: h("a"),
    intent_blob_name: "control/intent.json",
    intent_sha256: h("b"),
    manifest_blob_name: "profitability/latest.json",
    manifest_sha256: consumption.authorized_manifest_sha256,
    prediction_model: {
      blob_uri: consumption.execution_model.blob_uri,
      sha256: consumption.execution_model.sha256,
      model_version: consumption.execution_model.model_version
    }
  };
  const summary = {
    schema_version: 3, evidence_protocol_version: 3, run_id: "funded-run", status: "completed",
    started_ts: now.toISOString(), finished_ts: new Date(now.getTime() + 31_000).toISOString(),
    funder_address: settlementWallet, candidate,
    prediction_model: {
      blob_uri: consumption.execution_model.blob_uri, sha256: consumption.execution_model.sha256,
      model_version: consumption.execution_model.model_version, generated_at: "2026-07-12T00:00:00Z",
      training_data_end_ts: null
    },
    order_submission_attempted: true, order_submitted: true, submitted_order_count: 1, completed_probe_count: 1,
    provenance: {
      authorization_kind: "funded_stage", decision_id: decisionId,
      funded_stage_grant_id: consumption.grant_id, funded_stage_grant_sha256: consumption.grant_sha256,
      funded_stage_consumption_blob_name: expectedBinding.consumption_blob_name,
      funded_stage_consumption_sha256: expectedBinding.consumption_sha256,
      funded_stage_source_state_sha256: consumption.source_state_sha256,
      funded_stage_target_orders: consumption.stage_target_orders,
      authorization_blob_name: expectedBinding.authorization_blob_name,
      authorization_sha256: expectedBinding.authorization_sha256,
      intent_blob_name: expectedBinding.intent_blob_name,
      intent_sha256: expectedBinding.intent_sha256,
      promotion_manifest_blob_name: expectedBinding.manifest_blob_name,
      promotion_manifest_sha256: expectedBinding.manifest_sha256
    },
    probes: [completeProtocolV3Probe()]
  };
  const validateSummary = (value) => validateProtocolV3ChildSummary({
    summary: value, consumption, decisionId, expectedBinding
  });
  const validateEvidence = (value, terminalValue) => validateProtocolV3ChildEvidence({
    summary: value, terminal: terminalValue, consumption, decisionId, expectedBinding
  });
  const pending = validateSummary(summary);
  assert.equal(pending.filled, true);

  // Exercise the real JS model-row producer through the independent funded
  // admission validator. This prevents the producer and validator schemas from
  // silently drifting while hand-built fixtures continue to pass.
  const producerSummary = structuredClone(summary);
  const producerProbe = producerSummary.probes[0];
  producerProbe.model_observations = modelObservations({
    order: producerProbe.order,
    market: producerProbe.market,
    lifecycle: producerProbe.lifecycle,
    context: producerProbe.pre_send_context,
    markouts: producerProbe.markouts
  });
  assert.equal(producerProbe.model_observations[0].venue_fee_rate_bps, 0);
  assert.equal(producerProbe.model_observations[0].estimated_round_trip_cost_per_share, 0);
  assert.equal(validateSummary(producerSummary).filled, true);

  // V2 fees are a market curve and apply only to the taker. A post-only maker
  // entry therefore has zero entry fee, while a conservative hypothetical exit
  // pays the taker curve at the executable bid.
  const feeBearingSummary = structuredClone(summary);
  const feeProbe = feeBearingSummary.probes[0];
  const feeRate = 0.07;
  const feeRateBps = 700;
  const feeExponent = 1;
  const exitFee = feeRate * (0.22 * (1 - 0.22)) ** feeExponent;
  Object.assign(feeProbe.lifecycle, {
    venue_fee_rate: feeRate,
    venue_fee_rate_bps: feeRateBps,
    venue_fee_exponent: feeExponent,
    venue_fee_taker_only: true,
    estimated_round_trip_cost_per_share: exitFee
  });
  for (const row of feeProbe.markouts) Object.assign(row, {
    trader_side: "MAKER",
    authenticated_order_role: "MAKER",
    authenticated_fee_rate_bps: 0,
    authenticated_fee_amount: null,
    authenticated_fee_raw: { fee_rate_bps: "0", fee: null, fee_usdc: null, builder_fee: null },
    entry_fee_per_share: 0,
    hypothetical_exit_fee_per_share: exitFee,
    round_trip_fee_per_share: exitFee
  });
  feeProbe.model_observations = modelObservations({
    order: feeProbe.order,
    market: feeProbe.market,
    lifecycle: feeProbe.lifecycle,
    context: feeProbe.pre_send_context,
    markouts: feeProbe.markouts
  });
  assert.equal(feeProbe.model_observations[0].venue_fee_rate_bps, feeRateBps);
  assert.ok(Math.abs(feeProbe.model_observations[0].hypothetical_exit_fee_per_share - 0.012012) < 1e-12);
  assert.equal(validateSummary(feeBearingSummary).filled, true);

  // A genuine no-fill still has market fee parameters, but incurs no entry or
  // exit fee and has no per-fill markouts. It remains eligible once stable
  // authenticated terminal finality proves that no later fill can occur.
  const feeBearingNoFill = structuredClone(feeBearingSummary);
  const noFillProbe = feeBearingNoFill.probes[0];
  const noFillLifecycle = noFillProbe.lifecycle;
  Object.assign(noFillLifecycle, {
    cancel_send_wall_ms: noFillLifecycle.ack_wall_ms + 200,
    cancel_http_response_wall_ms: noFillLifecycle.ack_wall_ms + 250,
    user_channel_cancel_received_wall_ms: noFillLifecycle.ack_wall_ms + 270,
    client_cancel_round_trip_ms: 50,
    client_to_user_cancel_ack_ms: 70,
    first_fill_after_ack_ms: null,
    actual_matched_size: 0,
    partial_fill: false,
    fully_filled: false,
    fill_raced_cancellation: false,
    post_cancel_fill_count: 0,
    first_fill_after_cancel_ms: null,
    related_trade_ids: [],
    live_user_trade_ids: [],
    rest_order_matched_size: 0,
    user_order_matched_size: 0,
    rest_trade_matched_size: 0,
    user_trade_matched_size: 0,
    estimated_round_trip_cost_per_share: 0
  });
  noFillProbe.markouts = [];
  noFillProbe.model_observations = modelObservations({
    order: noFillProbe.order,
    market: noFillProbe.market,
    lifecycle: noFillLifecycle,
    context: noFillProbe.pre_send_context,
    markouts: []
  });
  assert.ok(noFillProbe.model_observations.every((row) => row.eligible && row.estimated_round_trip_cost_per_share === 0));
  assert.equal(validateSummary(feeBearingNoFill).filled, false);
  const canceled = structuredClone(summary);
  const canceledLifecycle = canceled.probes[0].lifecycle;
  canceledLifecycle.cancel_send_wall_ms = canceledLifecycle.ack_wall_ms + 200;
  canceledLifecycle.cancel_http_response_wall_ms = canceledLifecycle.cancel_send_wall_ms + 50;
  canceledLifecycle.user_channel_cancel_received_wall_ms = canceledLifecycle.cancel_send_wall_ms + 70;
  canceledLifecycle.client_cancel_round_trip_ms = 50;
  canceledLifecycle.client_to_user_cancel_ack_ms = 70;
  canceledLifecycle.fill_raced_cancellation = true;
  canceledLifecycle.post_cancel_fill_count = 1;
  canceledLifecycle.first_fill_after_cancel_ms = 300;
  assert.equal(validateSummary(canceled).filled, true);
  canceledLifecycle.client_to_user_cancel_ack_ms = 68;
  assert.throws(() => validateSummary(canceled), /chronology/);
  const terminal = {
    schema: "polyedge.canary_terminal_risk_portfolio.v1",
    producer: "polyedge_node_authenticated_risk_terminal",
    source: "polymarket_data_api_plus_onchain_redemption",
    settlement_verified: true,
    settlement_transaction_hash: `0x${"a".repeat(64)}`,
    condition_id: "condition-1",
    polygon_chain_id: 137,
    transaction_receipt_status: "success",
    transaction_block_number: 1,
    transaction_receipt_confirmations: 2,
    redemption_condition_ids: ["condition-1"],
    settlement_wallet: settlementWallet,
    trust_boundary_ready: true,
    run_id: "funded-run",
    probe_id: "probe-1",
    order_id: "order-1",
    portfolio_reconciled: true,
    zero_open_orders_confirmed: true,
    unresolved_exposure: 0,
    unresolved_risk_reservations: 0,
    reconciliation_discrepancy: 0,
    campaign_starting_equity: 5,
    net_external_cash_flows: 0,
    liquid_collateral: 5.1,
    summed_position_value: 0,
    cash_flow_adjusted_ending_equity: 5.1,
    minimum_observed_equity: 5,
    maximum_observed_equity: 5.1,
    observed_at: new Date(now.getTime() + 32_000).toISOString()
  };
  assert.equal(validateEvidence(summary, terminal).orderId, "order-1");
  assert.throws(() => validateEvidence(summary, { ...terminal, order_id: "other" }), /identity-mismatched/);

  const forgedBoolean = structuredClone(summary);
  forgedBoolean.probes[0].lifecycle.rest_trade_matched_size = 0;
  assert.throws(() => validateSummary(forgedBoolean), /independently reconcile/);

  const missingHorizon = structuredClone(summary);
  missingHorizon.probes[0].model_observations.pop();
  assert.throws(() => validateSummary(missingHorizon), /exactly 1\/5\/30\/60/);

  const lateMarkout = structuredClone(summary);
  lateMarkout.probes[0].markouts[0].observation_delay_ms = 2_001;
  assert.throws(() => validateSummary(lateMarkout), /timing/);

  const staleFinality = structuredClone(summary);
  staleFinality.probes[0].lifecycle.post_cancel_observation_ms = 9_999;
  assert.throws(() => validateSummary(staleFinality), /stable, zero-open/);

  const hiddenGap = structuredClone(summary);
  hiddenGap.probes[0].lifecycle.authenticated_user_channel_unparsed = 1;
  assert.throws(() => validateSummary(hiddenGap), /unclosed data-gap/);

  const producerOnlyEligibility = structuredClone(summary);
  producerOnlyEligibility.probes[0].pre_send_context.captured_wall_ms = producerOnlyEligibility.probes[0].lifecycle.send_wall_ms + 1;
  assert.throws(() => validateSummary(producerOnlyEligibility), /captured after/);

  const forgedFirstFill = structuredClone(summary);
  forgedFirstFill.probes[0].lifecycle.first_fill_after_ack_ms = 1;
  assert.throws(() => validateSummary(forgedFirstFill), /first-fill timing/);

  const forgedModelFeature = structuredClone(summary);
  forgedModelFeature.probes[0].model_observations[0].inferred_size_ahead = 999;
  assert.throws(() => validateSummary(forgedModelFeature), /raw order\/market features/);

  const forgedPositiveMarkout = structuredClone(summary);
  forgedPositiveMarkout.probes[0].markouts[2].executable_markout_per_share = 0.9;
  assert.throws(() => validateSummary(forgedPositiveMarkout), /claimed BUY markout/);

  const tamperedRawBook = structuredClone(summary);
  tamperedRawBook.probes[0].markouts[2].raw_orderbook.bids[0].price = "0.23";
  assert.throws(() => validateSummary(tamperedRawBook), /raw orderbook/);

  const wrongVenueHash = structuredClone(summary);
  wrongVenueHash.probes[0].markouts[2].venue_book_hash = "different-venue-hash";
  assert.throws(() => validateSummary(wrongVenueHash), /venue hash/);

  const missingVenueHash = structuredClone(summary);
  missingVenueHash.probes[0].markouts[2].raw_orderbook.venue_hash = null;
  missingVenueHash.probes[0].markouts[2].venue_book_hash = null;
  missingVenueHash.probes[0].markouts[2].book_hash = canonicalMarkoutBookHash(
    missingVenueHash.probes[0].markouts[2].raw_orderbook
  );
  assert.throws(() => validateSummary(missingVenueHash), /venue hash/);

  const inconsistentFill = structuredClone(summary);
  inconsistentFill.probes[0].markouts[1].fill_price = 0.19;
  inconsistentFill.probes[0].markouts[1].midpoint_markout_per_share = 0.04;
  inconsistentFill.probes[0].markouts[1].executable_markout_per_share = 0.03;
  assert.throws(() => validateSummary(inconsistentFill), /inconsistent prices/);

  const forgedRowMarkout = structuredClone(summary);
  forgedRowMarkout.probes[0].model_observations[0].executable_markout_30s_per_share = 0.9;
  assert.throws(() => validateSummary(forgedRowMarkout), /derived 30-second/);

  const forgedFeeCost = structuredClone(summary);
  forgedFeeCost.probes[0].lifecycle.venue_fee_rate = 0.01;
  forgedFeeCost.probes[0].lifecycle.venue_fee_rate_bps = 100;
  forgedFeeCost.probes[0].lifecycle.estimated_round_trip_cost_per_share = 0.001;
  for (const row of forgedFeeCost.probes[0].model_observations) {
    row.venue_fee_rate_bps = 100;
    row.estimated_round_trip_cost_per_share = 0.001;
  }
  assert.throws(() => validateSummary(forgedFeeCost), /fee-bearing|venue fee|Polymarket V2/);

  const missingFeeSide = structuredClone(feeBearingSummary);
  for (const row of missingFeeSide.probes[0].markouts) row.trader_side = null;
  assert.throws(() => validateSummary(missingFeeSide), /missing trader_side/);

  const makerChargedFee = structuredClone(feeBearingSummary);
  for (const row of makerChargedFee.probes[0].markouts) row.authenticated_fee_amount = 0.001;
  assert.throws(() => validateSummary(makerChargedFee), /maker fill reports a nonzero/);

  const forgedFeeExponent = structuredClone(feeBearingSummary);
  forgedFeeExponent.probes[0].lifecycle.venue_fee_exponent = 2;
  for (const row of forgedFeeExponent.probes[0].model_observations) row.venue_fee_exponent = 2;
  assert.throws(() => validateSummary(forgedFeeExponent), /independently recomputed Polymarket V2|round-trip cost/);

  const oversizedFill = structuredClone(summary);
  oversizedFill.probes[0].lifecycle.actual_matched_size = 2;
  for (const field of ["rest_order_matched_size", "user_order_matched_size", "rest_trade_matched_size", "user_trade_matched_size"]) {
    oversizedFill.probes[0].lifecycle[field] = 2;
  }
  assert.throws(() => validateSummary(oversizedFill), /exceeds submitted order size/);

  const preOrderFill = structuredClone(summary);
  const forgedFillAt = preOrderFill.probes[0].lifecycle.send_wall_ms - 10;
  for (const row of preOrderFill.probes[0].markouts) {
    row.fill_timestamp = new Date(forgedFillAt).toISOString();
    row.venue_fill_timestamp = row.fill_timestamp;
    row.target_observation_ts = new Date(forgedFillAt + row.horizon_seconds * 1_000).toISOString();
    row.request_started_at = row.target_observation_ts;
    row.response_completed_at = new Date(Date.parse(row.target_observation_ts) + 100).toISOString();
    row.observed_at = row.response_completed_at;
    row.venue_book_timestamp = row.response_completed_at;
  }
  assert.throws(() => validateSummary(preOrderFill), /predates order submission/);

  const forgedRequestChronology = structuredClone(summary);
  forgedRequestChronology.probes[0].markouts[0].request_started_at = new Date(
    Date.parse(forgedRequestChronology.probes[0].markouts[0].target_observation_ts) - 1
  ).toISOString();
  assert.throws(() => validateSummary(forgedRequestChronology), /raw timestamps/);

  const wrongCandidate = structuredClone(summary);
  wrongCandidate.candidate.config_hash = h("f");
  assert.throws(() => validateSummary(wrongCandidate), /candidate/);

  const wrongModel = structuredClone(summary);
  wrongModel.prediction_model.sha256 = h("f");
  assert.throws(() => validateSummary(wrongModel), /prediction model/);

  const terminalBeforeSummary = { ...terminal, observed_at: new Date(now.getTime() + 30_000).toISOString() };
  assert.throws(() => validateEvidence(summary, terminalBeforeSummary), /predates completion/);

  assert.throws(() => validateEvidence(summary, {
    ...terminal,
    transaction_receipt_confirmations: 1
  }), /settlement\/redemption proof/);
  assert.throws(() => validateEvidence(summary, {
    ...terminal,
    redemption_condition_ids: ["condition-other"]
  }), /settlement\/redemption proof/);
  assert.throws(() => validateEvidence(summary, {
    ...terminal,
    redemption_condition_ids: ["condition-1", "CONDITION-1"]
  }), /duplicate/);

  assert.throws(() => validateProtocolV3ChildSummary({
    summary, consumption, decisionId,
    expectedBinding: { ...expectedBinding, child_run_id: "wrong-child" }
  }), /exact loaded parent control artifacts/);
});

test("retry publishes the exact checkpoint when progress reached quota before checkpoint persistence", async () => {
  const source = inputs();
  const entries = protocolV3CheckpointEntries(5);
  const documents = new Map();
  for (const entry of entries) {
    const summaryBytes = Buffer.from(JSON.stringify(entry.summary));
    const terminalBytes = Buffer.from(JSON.stringify(entry.terminal));
    entry.summaryBinding = { blob_name: entry.summaryBinding.blob_name, sha256: sha256(summaryBytes) };
    entry.terminalBinding = { blob_name: entry.terminalBinding.blob_name, sha256: sha256(terminalBytes) };
    documents.set(entry.summaryBinding.blob_name, summaryBytes);
    documents.set(entry.terminalBinding.blob_name, terminalBytes);
  }
  source.manifest.funded_ladder.checkpoint_1_protocol_v3_artifact = entries[0].summaryBinding;
  source.manifest.funded_ladder.checkpoint_1_terminal_artifact = entries[0].terminalBinding;
  source.manifest.funded_ladder.last_verified_terminal_artifact = entries[0].terminalBinding;
  source.grant.source_state_sha256 = canonicalStateHash(source.manifest.funded_ladder);
  let cumulative = checkpointOneChainRoot(source.manifest.funded_ladder);
  for (const entry of entries.slice(1)) {
    const payload = progressPayloadHash({
      sequence: entry.sequence,
      decisionId: `decision-${entry.sequence}`,
      expectedControlBinding: entry.expectedControlBinding,
      summaryBinding: entry.summaryBinding,
      terminalBinding: entry.terminalBinding
    });
    const next = cumulativeProgressHash(cumulative, payload);
    Object.assign(entry.progress, {
      protocol_v3_summary_blob_name: entry.summaryBinding.blob_name,
      protocol_v3_summary_sha256: entry.summaryBinding.sha256,
      terminal_evidence_blob_name: entry.terminalBinding.blob_name,
      terminal_evidence_sha256: entry.terminalBinding.sha256,
      progress_payload_sha256: payload,
      prior_cumulative_evidence_sha256: cumulative,
      cumulative_evidence_sha256: next
    });
    cumulative = next;
  }
  const consumption = buildStageConsumption({ ...source, now });
  const canonicalBytes = Buffer.from(JSON.stringify(consumption.authorizedManifest, null, 2));
  const consumptionBytes = Buffer.from(JSON.stringify(consumption.value, null, 2));
  documents.set(source.config.manifestBlobName, canonicalBytes);
  documents.set(consumption.blobName, consumptionBytes);
  const campaignPrefix = `${source.config.controlPrefix}/campaigns/${consumption.value.campaign_control_id}`;
  const progressPrefix = `${campaignPrefix}/progress/${consumption.value.grant_id}`;
  for (const entry of entries.slice(1)) {
    const probe = entry.summary.probes[0];
    const progress = {
      ...entry.progress,
      schema: "polyedge.funded_stage_order_progress.v1",
      grant_id: consumption.value.grant_id,
      campaign_id: consumption.value.campaign_id,
      campaign_control_id: consumption.value.campaign_control_id,
      candidate,
      stage_target_orders: 5,
      sequence: entry.sequence,
      decision_id: `decision-${entry.sequence}`,
      completed_at: entry.terminal.observed_at
    };
    documents.set(`${progressPrefix}/decision-${entry.sequence}.json`, Buffer.from(JSON.stringify(progress)));
    documents.set(
      `${campaignPrefix}/intent-authorizations/${consumption.value.grant_id}/decision-${entry.sequence}.json`,
      Buffer.from(JSON.stringify({ decision_id: `decision-${entry.sequence}` }))
    );
  }
  const control = memoryContainer(documents);
  const research = memoryContainer(new Map([
    ["reports/research/profitability/latest.json", Buffer.from(JSON.stringify(shadowGate(consumption.authorizedManifest)))]
  ]));
  const env = {
    FUNDED_LADDER_CONTROLLER_ENABLED: "true",
    ALLOW_FUNDED_LADDER: "true",
    FUNDED_LADDER_DRY_RUN: "false",
    FUNDED_EVIDENCE_TRUST_BOUNDARY_READY: "true",
    FUNDED_LADDER_MANIFEST_BLOB_NAME: source.config.manifestBlobName,
    FUNDED_LADDER_MANIFEST_SHA256: sha256(canonicalBytes),
    FUNDED_LADDER_CONSUMPTION_BLOB_NAME: consumption.blobName,
    FUNDED_LADDER_CONSUMPTION_SHA256: sha256(consumptionBytes),
    FUNDED_LADDER_RESEARCH_CONTAINER_NAME: "polyedge-research",
    FUNDED_LADDER_INTENT_CONTAINER_NAME: "polyedge-shadow-events",
    FUNDED_LADDER_CONTROL_PREFIX: source.config.controlPrefix,
    AZURE_STORAGE_ACCOUNT_NAME: "st"
  };
  const invoke = () => runFundedLadderController({
    env,
    containers: { control, research, intents: memoryContainer() },
    clock: () => now,
    invokeChild: async () => { throw new Error("quota-complete retry must not invoke a funded child"); }
  });

  const recovered = await invoke();
  assert.equal(recovered.status, "funded_stage_checkpoint_recovered");
  assert.equal(recovered.remaining, 0);
  assert.equal(recovered.attempted, 4);
  assert.equal(recovered.submitted, 4);
  assert.equal(recovered.eligible, 4);
  assert.equal(recovered.checkpoint.stage_target_orders, 5);
  const checkpointBytes = control.documents.get(recovered.checkpoint.blob_name);
  assert.ok(checkpointBytes);
  assert.equal(sha256(checkpointBytes), recovered.checkpoint.sha256);
  const checkpoint = JSON.parse(checkpointBytes.toString("utf8"));
  assert.equal(checkpoint.exact_funded_order_count, 5);
  assert.deepEqual(checkpoint.protocol_v3_order_artifacts, entries.map((entry) => entry.summaryBinding));
  const writesAfterRecovery = control.writes;

  const replay = await invoke();
  assert.equal(replay.status, "funded_stage_checkpoint_recovered");
  assert.deepEqual(replay.checkpoint, recovered.checkpoint);
  assert.equal(control.writes, writesAfterRecovery, "checkpoint replay must verify, not rewrite, the immutable artifact");
});

test("target-5 checkpoint producer derives cumulative cross-grant evidence and rejects missing or duplicate sequences", () => {
  const manifest = inputs().manifest;
  const entries = protocolV3CheckpointEntries(5);
  const checkpoint = buildFundedCheckpointEvidence({ manifest, entries });
  assert.equal(checkpoint.exact_funded_order_count, 5);
  assert.equal(checkpoint.observed_calendar_days, 5);
  assert.ok(Math.abs(checkpoint.cumulative_net_pnl - 0.05) < 1e-9);
  assert.equal(checkpoint.markout_sample_size, 5);
  assert.equal(checkpoint.protocol_v3_order_artifacts[0].blob_name, "runs/run-1/summary.json");
  assert.equal(checkpoint.checkpoint_1_chain_root_sha256, checkpointOneChainRoot(manifest.funded_ladder));
  assert.equal(checkpoint.final_cumulative_evidence_sha256, entries.at(-1).progress.cumulative_evidence_sha256);
  assert.deepEqual(checkpoint.progress_artifacts, entries.slice(1).map((entry) => entry.progressBinding));
  assert.deepEqual(checkpoint.control_bindings, entries.map((entry) => entry.expectedControlBinding));
  assert.throws(() => buildFundedCheckpointEvidence({ manifest, entries: entries.slice(1) }), /exact cumulative sequence count/);
  const duplicateSequence = structuredClone(entries);
  duplicateSequence[4].sequence = 4;
  assert.throws(() => buildFundedCheckpointEvidence({ manifest, entries: duplicateSequence }), /missing, duplicated/);
  const duplicateIdentity = structuredClone(entries);
  duplicateIdentity[4].summary.run_id = duplicateIdentity[3].summary.run_id;
  duplicateIdentity[4].expectedControlBinding.child_run_id = duplicateIdentity[3].summary.run_id;
  duplicateIdentity[4].summary.probes[0].probe_id = duplicateIdentity[3].summary.probes[0].probe_id;
  duplicateIdentity[4].summary.probes[0].lifecycle.order_id = duplicateIdentity[3].summary.probes[0].lifecycle.order_id;
  duplicateIdentity[4].terminal.run_id = duplicateIdentity[3].terminal.run_id;
  duplicateIdentity[4].terminal.probe_id = duplicateIdentity[3].terminal.probe_id;
  duplicateIdentity[4].terminal.order_id = duplicateIdentity[3].terminal.order_id;
  assert.throws(() => buildFundedCheckpointEvidence({ manifest, entries: duplicateIdentity }), /duplicated/);

  const forgedParent = structuredClone(entries);
  forgedParent[2].progress.expected_control_binding.intent_sha256 = h("9");
  assert.throws(() => buildFundedCheckpointEvidence({ manifest, entries: forgedParent }), /parent control|order\/control evidence bindings/);

  const brokenChain = structuredClone(entries);
  brokenChain[3].progress.prior_cumulative_evidence_sha256 = h("9");
  assert.throws(() => buildFundedCheckpointEvidence({ manifest, entries: brokenChain }), /cumulative progress hash chain/);
});
