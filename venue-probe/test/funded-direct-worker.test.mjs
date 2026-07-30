import test from "node:test";
import assert from "node:assert/strict";
import { sha256 } from "../src/canary-lib.mjs";
import {
  createFundedDirectProcessor,
  loadFundedDirectConfig,
  runFundedDirectWorker
} from "../src/funded-direct-worker.mjs";

function session() {
  return {
    schema_version: "polyedge.operator_funded_session.v1",
    session_id: "dynamic-quote-2026-07-27",
    authorization_mode: "operator_direct",
    authorized_by_user_reference: "Codex task 2026-07-27 funded Dynamic Quote",
    source_simulated_pnl: 379.19,
    research_promotion_bypassed: true,
    research_lane_isolated: true,
    maker_only: true,
    no_deposits: true,
    allow_automatic_replenishment: false,
    allow_compounding: false,
    external_cash_flows: [],
    max_open_orders: 1,
    target_order_notional: 10.5,
    max_order_notional: 10.5,
    max_account_loss: 11.09862,
    starting_collateral: 11.09862,
    max_reconciliation_discrepancy: 0.01,
    evidence_trust_boundary_ready: false,
    execution_window_seconds_to_expiry: {
      minimum: 360,
      maximum: 900
    },
    candidate: {
      name: "dynamic_quote_style",
      candidate_version: "dynamic_quote_style@2026-06-14",
      config_hash: `sha256:${"a".repeat(64)}`
    },
    execution_model: {
      model_version: "conservative-execution-prior-v1",
      blob_uri: "azure://storage/models/prior.json",
      sha256: `sha256:${"b".repeat(64)}`
    },
    created_at: "2026-07-27T00:00:00Z",
    expires_at: "2026-08-27T00:00:00Z"
  };
}

function env(overrides = {}) {
  const value = session();
  return {
    FUNDED_DIRECT_WORKER_ENABLED: "true",
    ALLOW_FUNDED_DIRECT: "true",
    FUNDED_DIRECT_DRY_RUN: "false",
    FUNDED_DIRECT_SESSION_MANIFEST_JSON: JSON.stringify(value),
    FUNDED_DIRECT_SESSION_MANIFEST_BLOB_NAME: "reports/funded/session.json",
    FUNDED_DIRECT_SESSION_MANIFEST_SHA256: sha256(Buffer.from(JSON.stringify(value, null, 2))),
    FUNDED_DIRECT_MIN_REMAINING_TTL_MS: "7000",
    FUNDED_DIRECT_CHILD_MIN_REMAINING_TTL_MS: "2000",
    STRATEGY_CANARY_CANDIDATE_NAME: "dynamic_quote_style",
    STRATEGY_CANARY_CANDIDATE_VERSION: "dynamic_quote_style@2026-06-14",
    STRATEGY_CANARY_CANDIDATE_CONFIG_HASH: `sha256:${"a".repeat(64)}`,
    STRATEGY_CANARY_REQUIRED_FILL_MODEL_VERSION: "conservative-execution-prior-v1",
    STRATEGY_CANARY_REQUIRED_RESOLUTION_SOURCE: "chainlink_reference",
    STRATEGY_INTENT_TARGET_ORDER_NOTIONAL: "10.5",
    STRATEGY_CANARY_MAX_ORDER_NOTIONAL: "10.5",
    STRATEGY_INTENT_MIN_SECONDS_TO_EXPIRY: "360",
    STRATEGY_INTENT_MAX_SECONDS_TO_EXPIRY: "900",
    STRATEGY_CANARY_INTENT_PREFIX: "intents",
    STRATEGY_CANARY_INTENT_CONTAINER_NAME: "shadow",
    AZURE_STORAGE_ACCOUNT_NAME: "storage",
    AZURE_STORAGE_CONTAINER_NAME: "funded",
    FUNDED_DIRECT_MAX_ITERATIONS: "4",
    FUNDED_DIRECT_POLL_INTERVAL_MS: "1000",
    FUNDED_DIRECT_MAX_IDLE_MS: "2000",
    ...overrides
  };
}

class Container {
  constructor(values = {}) {
    this.values = new Map(Object.entries(values));
  }
  async *listBlobsFlat({ prefix }) {
    for (const name of [...this.values.keys()].filter((value) => value.startsWith(prefix))) yield { name };
  }
  getBlobClient(name) {
    return {
      exists: async () => this.values.has(name),
      download: async () => ({ readableStreamBody: stream(this.values.get(name)) })
    };
  }
  getBlockBlobClient(name) {
    return {
      uploadData: async (bytes) => {
        if (this.values.has(name)) {
          const error = new Error("exists");
          error.statusCode = 409;
          throw error;
        }
        this.values.set(name, Buffer.from(bytes));
      }
    };
  }
}

function intent(now, id = "c".repeat(64)) {
  const value = session();
  const decision = new Date(now.getTime() - 1_000);
  const valid = new Date(decision.getTime() + 10_000);
  return {
    schema: "polyedge.execution_intent.v1",
    decision_id: id,
    candidate_name: value.candidate.name,
    candidate_version: value.candidate.candidate_version,
    candidate_config_hash: value.candidate.config_hash,
    required_fill_model_version: value.execution_model.model_version,
    execution_model_blob_uri: value.execution_model.blob_uri,
    execution_model_sha256: value.execution_model.sha256,
    resolution_source: "chainlink_reference",
    exact_resolution_source: true,
    side: "BUY",
    post_only: true,
    order_kind: "post_only_gtd",
    price: "0.45",
    shares: "22.46",
    notional: "10.107",
    fee_allowance: "0.017325",
    minimum_order_size: "5",
    net_edge_lower_bound: "0.02",
    decision_ts: decision.toISOString(),
    market_end_ts: new Date(decision.getTime() + 600_000).toISOString(),
    valid_until: valid.toISOString(),
    gtd_expiry_ts: new Date(valid.getTime() + 300_000).toISOString(),
    ttl_ms: 10_000
  };
}

function stream(value) {
  return (async function* () { yield Buffer.from(value || ""); })();
}

test("operator-funded config rejects the old one-dollar cap", () => {
  assert.throws(
    () => loadFundedDirectConfig(env({ STRATEGY_CANARY_MAX_ORDER_NOTIONAL: "1" })),
    /must be in \(1, 100\]/
  );
});

test("operator-funded config requires the profitable 6-15 minute window", () => {
  assert.throws(
    () => loadFundedDirectConfig(env({ STRATEGY_INTENT_MIN_SECONDS_TO_EXPIRY: "30" })),
    /exactly 360-900 seconds/
  );
});

test("worker TTL gates reserve time for the authenticated child preflight", () => {
  const config = loadFundedDirectConfig(env());
  assert.equal(config.minRemainingTtlMs, 7_000);
  assert.equal(config.childMinRemainingTtlMs, 2_000);
  assert.throws(
    () => loadFundedDirectConfig(env({ FUNDED_DIRECT_MIN_REMAINING_TTL_MS: "4000" })),
    /must be in \[5000, 30000\]/
  );
});

test("operator-funded config accepts only an explicit non-compounding profit quarantine", () => {
  const value = session();
  value.session_id = "dynamic-quote-funded-test-v6";
  value.profit_quarantine = {
    enabled: true,
    mode: "verified_internal_profit_quarantine",
    risk_headroom: "starting_collateral_only",
    settlement_ledger_prefix:
      "reports/funded/dynamic-quote/sessions/dynamic-quote-funded-test-v6/verified-internal-profits"
  };
  value.verified_internal_settlements = [{
    id: "manual-redeem-1",
    type: "internal_manual_settlement",
    transaction_hash: `0x${"a".repeat(64)}`,
    condition_id: `0x${"b".repeat(64)}`,
    payout: 17.015,
    principal: 10.209,
    realized_pnl: 6.806,
    fill_transaction_hashes: [`0x${"c".repeat(64)}`],
    settled_at: "2026-07-29T19:47:17Z"
  }];
  const manifestJson = JSON.stringify(value);
  const fundedEnv = env({
    FUNDED_DIRECT_SESSION_MANIFEST_JSON: manifestJson,
    FUNDED_DIRECT_SESSION_MANIFEST_SHA256: sha256(Buffer.from(JSON.stringify(value, null, 2)))
  });
  assert.equal(loadFundedDirectConfig(fundedEnv).session.allow_compounding, false);
  value.profit_quarantine.risk_headroom = "verified_profit";
  assert.throws(
    () => loadFundedDirectConfig({
      ...fundedEnv,
      FUNDED_DIRECT_SESSION_MANIFEST_JSON: JSON.stringify(value),
      FUNDED_DIRECT_SESSION_MANIFEST_SHA256: sha256(Buffer.from(JSON.stringify(value, null, 2)))
    }),
    /operator-funded session contract/
  );
});

test("worker executes a fresh Dynamic Quote intent under the operator session", async () => {
  const now = new Date("2026-07-27T12:00:00Z");
  const value = intent(now);
  const control = new Container();
  const intents = new Container({ [`intents/${value.decision_id}.json`]: Buffer.from(JSON.stringify(value)) });
  let calls = 0;
  const output = await runFundedDirectWorker({
    env: env({ FUNDED_DIRECT_MAX_ITERATIONS: "2" }),
    containers: { control, intents },
    clock: () => now,
    sleep: async () => {},
    invokeChild: async (childEnv) => {
      calls += 1;
      assert.equal(childEnv.EXECUTION_MODE, "funded_direct");
      assert.equal(childEnv.STRATEGY_CANARY_MAX_ORDER_NOTIONAL, "10.5");
      assert.equal(childEnv.STRATEGY_CANARY_MIN_REMAINING_TTL_MS, "2000");
      assert.equal(childEnv.VENUE_PROBE_CAMPAIGN_CASH_FLOWS, "[]");
      return { exitCode: 0, error: "" };
    }
  });
  assert.equal(calls, 1);
  assert.equal(output.status, "iteration_limit_reached");
  assert.equal(output.childInvocations, 1);
});

test("worker rejects principal-only sizing that exceeds the funded target after fees", async () => {
  const now = new Date("2026-07-27T12:00:00Z");
  const value = intent(now, "f".repeat(64));
  value.shares = "23.33";
  value.notional = "10.4985";
  const output = await runFundedDirectWorker({
    env: env({ FUNDED_DIRECT_MAX_ITERATIONS: "1" }),
    containers: {
      control: new Container(),
      intents: new Container({ [`intents/${value.decision_id}.json`]: Buffer.from(JSON.stringify(value)) })
    },
    clock: () => now,
    sleep: async () => {},
    invokeChild: async () => assert.fail("fee-excess intent must not execute")
  });
  assert.equal(output.childInvocations, 0);
});

test("stale handoff is rejected before authorization and creates no reservation", async () => {
  const now = new Date("2026-07-27T12:00:00Z");
  const value = intent(now, "1".repeat(64));
  const control = new Container();
  let clockCalls = 0;
  const output = await runFundedDirectWorker({
    env: env({ FUNDED_DIRECT_MAX_ITERATIONS: "1" }),
    containers: {
      control,
      intents: new Container({ [`intents/${value.decision_id}.json`]: Buffer.from(JSON.stringify(value)) })
    },
    clock: () => clockCalls++ === 0 ? now : new Date(now.getTime() + 3_000),
    sleep: async () => {},
    invokeChild: async () => assert.fail("stale handoff must not launch a child")
  });
  assert.equal(output.childInvocations, 0);
  assert.equal([...control.values.keys()].some((name) => name.includes("/authorizations/")), false);
  assert.equal([...control.values.keys()].some((name) => name.includes("risk-reservations")), false);
});

test("authorization that loses launch TTL is terminally sealed with no reservation", async () => {
  const now = new Date("2026-07-27T12:00:00Z");
  const value = intent(now, "2".repeat(64));
  const control = new Container();
  const times = [now, now, new Date(now.getTime() + 8_000)];
  const output = await runFundedDirectWorker({
    env: env({ FUNDED_DIRECT_MAX_ITERATIONS: "1" }),
    containers: {
      control,
      intents: new Container({ [`intents/${value.decision_id}.json`]: Buffer.from(JSON.stringify(value)) })
    },
    clock: () => times.shift() || times.at(-1) || now,
    sleep: async () => {},
    invokeChild: async () => assert.fail("expired authorization must not launch a child")
  });
  assert.equal(output.childInvocations, 0);
  const completionName = [...control.values.keys()].find((name) => name.includes("/completed/"));
  const completion = JSON.parse(control.values.get(completionName).toString("utf8"));
  assert.equal(completion.status, "expired_before_child_launch");
  assert.equal(completion.authorization_consumed, false);
  assert.equal(completion.risk_reservation_created, false);
  assert.equal([...control.values.keys()].some((name) => name.includes("risk-reservations")), false);
});

test("worker rejects an otherwise valid intent inside the final six minutes", async () => {
  const now = new Date("2026-07-27T12:00:00Z");
  const value = intent(now, "e".repeat(64));
  value.market_end_ts = new Date(Date.parse(value.decision_ts) + 359_000).toISOString();
  const output = await runFundedDirectWorker({
    env: env(),
    containers: {
      control: new Container(),
      intents: new Container({ [`intents/${value.decision_id}.json`]: Buffer.from(JSON.stringify(value)) })
    },
    clock: () => now,
    sleep: async () => {},
    invokeChild: async () => assert.fail("out-of-window intent must not execute")
  });
  assert.equal(output.status, "iteration_limit_reached");
  assert.equal(output.childInvocations, 0);
});

test("worker downloads only blob bodies that can still contain a fresh intent", async () => {
  const now = new Date("2026-07-27T12:00:00Z");
  const value = intent(now, "3".repeat(64));
  let downloads = 0;
  const old = {
    name: `intents/${"4".repeat(64)}.json`,
    properties: { createdOn: new Date(now.getTime() - 31_000) }
  };
  const fresh = {
    name: `intents/${value.decision_id}.json`,
    properties: { createdOn: now }
  };
  const intents = {
    async *listBlobsFlat() {
      yield old;
      yield fresh;
    },
    getBlobClient(name) {
      return {
        download: async () => {
          downloads += 1;
          return { readableStreamBody: stream(name === fresh.name ? JSON.stringify(value) : "{}") };
        }
      };
    }
  };
  await runFundedDirectWorker({
    env: env({ FUNDED_DIRECT_MAX_ITERATIONS: "1" }),
    containers: { control: new Container(), intents },
    clock: () => now,
    sleep: async () => {},
    invokeChild: async () => ({ exitCode: 0, error: "" })
  });
  assert.equal(downloads, 1);
});

test("worker reports projected account-risk blockers as a pause without retrying", async () => {
  const now = new Date("2026-07-27T12:00:00Z");
  const value = intent(now, "d".repeat(64));
  const output = await runFundedDirectWorker({
    env: env(),
    containers: {
      control: new Container(),
      intents: new Container({ [`intents/${value.decision_id}.json`]: Buffer.from(JSON.stringify(value)) })
    },
    clock: () => now,
    sleep: async () => {},
    invokeChild: async () => ({
      exitCode: 1,
      error: "campaign equity/risk gate failed (projected_equity_floor_breach, projected_campaign_drawdown_breach)"
    })
  });
  assert.equal(output.status, "paused_by_account_risk_state");
  assert.equal(output.childInvocations, 1);
});

test("persistent pre-submission failure is terminally sealed without authorization leakage", async () => {
  const now = new Date("2026-07-27T12:00:00Z");
  const value = intent(now, "6".repeat(64));
  value.market_id = "btc-market";
  value.condition_id = "condition";
  value.token_id = "token-up";
  const bytes = Buffer.from(JSON.stringify(value));
  const control = new Container();
  const processor = await createFundedDirectProcessor({
    env: env({ FUNDED_DIRECT_ENGINE: "persistent_v1" }),
    containers: {
      control,
      intents: new Container({ [`intents/${value.decision_id}.json`]: bytes })
    },
    clock: () => now,
    executeCanary: async () => {
      throw new Error("fail closed: exact resolution source is not confirmed");
    }
  });
  const handoff = {
    schema: "polyedge.funded_intent_handoff.v1",
    decision_id: value.decision_id,
    intent_blob_name: `intents/${value.decision_id}.json`,
    intent_sha256: sha256(bytes),
    decision_ts: value.decision_ts,
    valid_until: value.valid_until
  };

  const first = await processor.process(handoff);
  const duplicate = await processor.process(handoff);

  assert.equal(first.status, "child_failed_closed_pre_submission");
  assert.equal(first.childInvocations, 1);
  assert.equal(duplicate.status, "already_completed_idempotent");
  assert.equal(duplicate.completion.authorization_consumed, false);
  assert.equal(duplicate.completion.risk_reservation_created, false);
  assert.equal(duplicate.completion.order_submission_attempted, false);
});

test("persistent handoff verifies the immutable hash and executes exactly once across duplicate delivery", async () => {
  const now = new Date("2026-07-27T12:00:00Z");
  const value = intent(now, "8".repeat(64));
  value.market_id = "btc-market";
  value.condition_id = "condition";
  value.token_id = "token-up";
  const bytes = Buffer.from(JSON.stringify(value));
  const control = new Container();
  const intents = new Container({ [`intents/${value.decision_id}.json`]: bytes });
  let executions = 0;
  const processor = await createFundedDirectProcessor({
    env: env({ FUNDED_DIRECT_ENGINE: "persistent_v1" }),
    containers: { control, intents },
    clock: () => now,
    executeCanary: async () => {
      executions += 1;
      return { order_submission_attempted: true, lifecycle: { send_wall_ms: now.getTime() + 500 } };
    }
  });
  const handoff = {
    schema: "polyedge.funded_intent_handoff.v1",
    decision_id: value.decision_id,
    intent_blob_name: `intents/${value.decision_id}.json`,
    intent_sha256: sha256(bytes),
    decision_ts: value.decision_ts,
    valid_until: value.valid_until
  };
  const first = await processor.process(handoff);
  const duplicate = await processor.process(handoff);
  assert.equal(first.status, "persistent_intent_completed");
  assert.equal(duplicate.status, "already_completed_idempotent");
  assert.equal(executions, 1);
});

test("persistent handoff seals a post-submit evidence failure only from exact terminal no-fill risk", async () => {
  const now = new Date("2026-07-27T12:00:00Z");
  const value = intent(now, "7".repeat(64));
  value.market_id = "btc-market";
  value.condition_id = "condition";
  value.token_id = "token-up";
  const bytes = Buffer.from(JSON.stringify(value));
  const control = new Container();
  const intents = new Container({ [`intents/${value.decision_id}.json`]: bytes });
  let executions = 0;
  const processor = await createFundedDirectProcessor({
    env: env({ FUNDED_DIRECT_ENGINE: "persistent_v1" }),
    containers: { control, intents },
    clock: () => now,
    executeCanary: async (childEnv) => {
      executions += 1;
      const reservationName =
        `reports/research/venue-probe/risk-reservations/2026-07-27/funded-direct-${value.decision_id}.json`;
      control.values.set(reservationName, Buffer.from(JSON.stringify({
        schema_version: 1,
        state: "finalized_no_fill",
        run_id: childEnv.STRATEGY_CANARY_RUN_ID,
        probe_id: `funded-direct-${value.decision_id}`,
        market_id: value.market_id,
        condition_id: value.condition_id,
        token_id: value.token_id,
        order_submission_intended: true,
        order_submitted: true,
        order_id: `0x${"6".repeat(64)}`,
        matched_notional: 0,
        reconciliation_complete: true,
        zero_open_orders_confirmed: true,
        updated_ts: now.toISOString()
      })));
      const error = new Error("summary evidence serialization failed");
      error.orderSubmissionAttempted = true;
      throw error;
    }
  });
  const handoff = {
    schema: "polyedge.funded_intent_handoff.v1",
    decision_id: value.decision_id,
    intent_blob_name: `intents/${value.decision_id}.json`,
    intent_sha256: sha256(bytes),
    decision_ts: value.decision_ts,
    valid_until: value.valid_until
  };
  const first = await processor.process(handoff);
  const duplicate = await processor.process(handoff);
  assert.equal(first.status, "persistent_intent_completed");
  assert.equal(first.execution.status, "terminal_no_fill_evidence_degraded");
  assert.equal(first.execution.lifecycle.zero_open_orders_confirmed, true);
  assert.equal(duplicate.status, "already_completed_idempotent");
  assert.equal(duplicate.completion.evidence_upload_status, "degraded_post_submission");
  assert.equal(executions, 1);
});

test("persistent post-submit unresolved risk is accurately sealed and paused", async () => {
  const now = new Date("2026-07-27T12:00:00Z");
  const value = intent(now, "5".repeat(64));
  value.market_id = "btc-market";
  value.condition_id = "condition";
  value.token_id = "token-up";
  const bytes = Buffer.from(JSON.stringify(value));
  const control = new Container();
  const orderId = `0x${"5".repeat(64)}`;
  const processor = await createFundedDirectProcessor({
    env: env({ FUNDED_DIRECT_ENGINE: "persistent_v1" }),
    containers: {
      control,
      intents: new Container({ [`intents/${value.decision_id}.json`]: bytes })
    },
    clock: () => now,
    executeCanary: async () => {
      const error = new Error("post-ack evidence failed closed");
      error.orderSubmissionAttempted = true;
      error.executionEvidence = {
        status: "post_submission_unresolved",
        order_submission_attempted: true,
        order_submitted: true,
        lifecycle: {
          order_id: orderId,
          send_wall_ms: now.getTime() + 500,
          matched_notional: 10.5,
          reconciliation_complete: false,
          zero_open_orders_confirmed: true
        }
      };
      throw error;
    }
  });
  const handoff = {
    schema: "polyedge.funded_intent_handoff.v1",
    decision_id: value.decision_id,
    intent_blob_name: `intents/${value.decision_id}.json`,
    intent_sha256: sha256(bytes),
    decision_ts: value.decision_ts,
    valid_until: value.valid_until
  };

  const first = await processor.process(handoff);
  const duplicate = await processor.process(handoff);

  assert.equal(first.status, "paused_by_account_risk_state");
  assert.equal(first.execution.lifecycle.order_id, orderId);
  assert.equal(first.execution.order_submission_attempted, true);
  assert.equal(duplicate.status, "already_completed_idempotent");
  assert.equal(duplicate.completion.status, "child_failed_closed_post_submission_unresolved");
  assert.equal(duplicate.completion.authorization_consumed, true);
  assert.equal(duplicate.completion.risk_reservation_created, true);
  assert.equal(duplicate.completion.order_submission_attempted, true);
  assert.equal(duplicate.completion.reconciliation_complete, false);
});

test("persistent handoff rejects expired, tampered, and authorization-leak deliveries before execution", async () => {
  const now = new Date("2026-07-27T12:00:00Z");
  const value = intent(now, "9".repeat(64));
  value.market_id = "btc-market";
  value.condition_id = "condition";
  value.token_id = "token-up";
  const bytes = Buffer.from(JSON.stringify(value));
  const control = new Container();
  const intents = new Container({ [`intents/${value.decision_id}.json`]: bytes });
  let executions = 0;
  const processor = await createFundedDirectProcessor({
    env: env({ FUNDED_DIRECT_ENGINE: "persistent_v1" }),
    containers: { control, intents },
    clock: () => now,
    executeCanary: async () => { executions += 1; }
  });
  const handoff = {
    schema: "polyedge.funded_intent_handoff.v1",
    decision_id: value.decision_id,
    intent_blob_name: `intents/${value.decision_id}.json`,
    intent_sha256: sha256(bytes),
    decision_ts: value.decision_ts,
    valid_until: value.valid_until
  };
  await assert.rejects(
    processor.process({ ...handoff, intent_sha256: `sha256:${"0".repeat(64)}` }),
    /SHA-256 mismatch/
  );
  await assert.rejects(
    createFundedDirectProcessor({
      env: env({ FUNDED_DIRECT_ENGINE: "persistent_v1" }),
      containers: { control: new Container(), intents },
      clock: () => new Date(now.getTime() + 10_000),
      executeCanary: async () => { executions += 1; }
    }).then((expired) => expired.process(handoff)),
    /binding or TTL is invalid/
  );
  const authorizationName = `reports/funded/dynamic-quote/sessions/${session().session_id}/authorizations/${value.decision_id}.json`;
  control.values.set(authorizationName, Buffer.from("{}"));
  await assert.rejects(processor.process(handoff), /already has an authorization/);
  assert.equal(executions, 0);
});
