import test from "node:test";
import assert from "node:assert/strict";
import {
  loadFundedDynamicQuoteWorkerConfig,
  runFundedDynamicQuoteWorker
} from "../src/funded-dynamic-quote-worker.mjs";

function env(overrides = {}) {
  return {
    FUNDED_DYNAMIC_QUOTE_WORKER_ENABLED: "true",
    FUNDED_LADDER_CONTROLLER_ENABLED: "true",
    ALLOW_FUNDED_LADDER: "true",
    STRATEGY_CANARY_CANDIDATE_NAME: "dynamic_quote_style",
    STRATEGY_CANARY_CANDIDATE_VERSION: "dynamic_quote_style@2026-06-14",
    FUNDED_DYNAMIC_QUOTE_MAX_ITERATIONS: "20",
    FUNDED_DYNAMIC_QUOTE_POLL_INTERVAL_MS: "1000",
    FUNDED_DYNAMIC_QUOTE_MAX_IDLE_MS: "5000",
    ...overrides
  };
}

test("worker configuration is disabled and candidate-bound by default", () => {
  assert.throws(
    () => loadFundedDynamicQuoteWorkerConfig(env({ FUNDED_DYNAMIC_QUOTE_WORKER_ENABLED: "false" })),
    /FUNDED_DYNAMIC_QUOTE_WORKER_ENABLED must be true/
  );
  assert.throws(
    () => loadFundedDynamicQuoteWorkerConfig(env({ STRATEGY_CANARY_CANDIDATE_NAME: "other" })),
    /must equal dynamic_quote_style/
  );
});

test("worker continues across orders until the funded stage is complete", async () => {
  const results = [
    { status: "funded_stage_order_completed", remaining: 2 },
    { status: "funded_stage_order_completed", remaining: 1 },
    { status: "funded_stage_order_completed", remaining: 0, checkpoint: { stage_target_orders: 5 } }
  ];
  const summary = await runFundedDynamicQuoteWorker({
    env: env(),
    runController: async () => results.shift(),
    sleep: async () => {}
  });
  assert.equal(summary.status, "funded_stage_completed");
  assert.equal(summary.order_completions, 3);
  assert.equal(summary.iterations, 3);
  assert.equal(summary.remaining, 0);
});

test("worker waits for a fresh intent and then resumes funded execution", async () => {
  const results = [
    { status: "stage_waiting_for_fresh_intent", remaining: 2 },
    { status: "funded_stage_order_completed", remaining: 1 },
    { status: "funded_stage_checkpoint_recovered", remaining: 0 }
  ];
  let now = 0;
  const summary = await runFundedDynamicQuoteWorker({
    env: env(),
    runController: async () => results.shift(),
    clock: () => new Date(now),
    sleep: async (ms) => { now += ms; }
  });
  assert.equal(summary.status, "funded_stage_completed");
  assert.equal(summary.order_completions, 1);
  assert.equal(summary.iterations, 3);
});

test("worker exits cleanly after its bounded fresh-intent wait", async () => {
  let now = 0;
  const summary = await runFundedDynamicQuoteWorker({
    env: env({ FUNDED_DYNAMIC_QUOTE_MAX_IDLE_MS: "2000" }),
    runController: async () => ({ status: "stage_waiting_for_fresh_intent", remaining: 4 }),
    clock: () => new Date(now),
    sleep: async (ms) => { now += ms; }
  });
  assert.equal(summary.status, "idle_waiting_for_fresh_intent");
  assert.equal(summary.iterations, 3);
  assert.equal(summary.order_completions, 0);
});

test("worker fails closed on an unknown controller state", async () => {
  await assert.rejects(
    runFundedDynamicQuoteWorker({
      env: env(),
      runController: async () => ({ status: "unexpected" }),
      sleep: async () => {}
    }),
    /fail closed: unexpected controller status unexpected/
  );
});
