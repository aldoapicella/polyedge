import test from "node:test";
import assert from "node:assert/strict";
import { sha256 } from "../src/canary-lib.mjs";
import {
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
    max_open_orders: 1,
    max_order_notional: 10.5,
    max_account_loss: 11.09862,
    starting_collateral: 11.09862,
    evidence_trust_boundary_ready: false,
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
    STRATEGY_CANARY_CANDIDATE_NAME: "dynamic_quote_style",
    STRATEGY_CANARY_CANDIDATE_VERSION: "dynamic_quote_style@2026-06-14",
    STRATEGY_CANARY_CANDIDATE_CONFIG_HASH: `sha256:${"a".repeat(64)}`,
    STRATEGY_CANARY_REQUIRED_FILL_MODEL_VERSION: "conservative-execution-prior-v1",
    STRATEGY_CANARY_REQUIRED_RESOLUTION_SOURCE: "chainlink_reference",
    STRATEGY_CANARY_MAX_ORDER_NOTIONAL: "10.5",
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
  const valid = new Date(now.getTime() + 29_000);
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
    shares: "10",
    notional: "4.5",
    minimum_order_size: "5",
    net_edge_lower_bound: "0.02",
    decision_ts: decision.toISOString(),
    valid_until: valid.toISOString(),
    gtd_expiry_ts: new Date(valid.getTime() + 60_000).toISOString(),
    ttl_ms: 30_000
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
      return { exitCode: 0, error: "" };
    }
  });
  assert.equal(calls, 1);
  assert.equal(output.status, "iteration_limit_reached");
  assert.equal(output.childInvocations, 1);
});

test("worker reports an account-risk pause without retrying a funded child", async () => {
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
      error: "campaign equity/risk gate failed (existing_unresolved_position_blocks_submission)"
    })
  });
  assert.equal(output.status, "paused_by_account_risk_state");
  assert.equal(output.childInvocations, 1);
});
