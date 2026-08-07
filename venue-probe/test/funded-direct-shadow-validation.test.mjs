import test from "node:test";
import assert from "node:assert/strict";
import {
  buildFundedDirectShadowSeal,
  LISTED_FAILURES,
  validateFundedDirectShadowSeal
} from "../src/funded-direct-shadow-validation.mjs";

function transitions() {
  return Array.from({ length: 100 }, (_, index) => ({
    schema: "polyedge.funded_direct_shadow_transition.v1",
    sequence: index + 1,
    observed_at: new Date(Date.parse("2026-07-28T22:00:00Z") + index * 30_000).toISOString(),
    mode: index < 70 ? "paper" : "shadow",
    market_id: `market-${(index % 20) + 1}`,
    eligible: true,
    funded_order_submitted: false,
    failures: Object.fromEntries(LISTED_FAILURES.map((name) => [name, false]))
  }));
}

function input(rows = transitions()) {
  return {
    transitions: rows,
    candidateName: "dynamic_quote_style",
    candidateVersion: "dynamic_quote_style@2026-06-14",
    candidateConfigHash: `sha256:${"a".repeat(64)}`,
    generatedAt: new Date("2026-07-28T23:00:00Z")
  };
}

test("isolated funded repair validation seals exactly 100 time-ordered 70/30 transitions across 20 markets", () => {
  const seal = buildFundedDirectShadowSeal(input());
  assert.equal(seal.passed, true);
  assert.equal(seal.eligible_transition_count, 100);
  assert.equal(seal.distinct_market_count, 20);
  assert.deepEqual(seal.split, {
    method: "time_ordered_70_30",
    training_transitions: 70,
    holdout_transitions: 30
  });
  assert.equal(validateFundedDirectShadowSeal(seal, {
    candidateName: "dynamic_quote_style",
    candidateVersion: "dynamic_quote_style@2026-06-14",
    candidateConfigHash: `sha256:${"a".repeat(64)}`
  }), seal);
});

test("validation rejects out-of-order evidence, too few markets, and every listed failure", () => {
  const outOfOrder = transitions();
  [outOfOrder[49].observed_at, outOfOrder[50].observed_at] = [outOfOrder[50].observed_at, outOfOrder[49].observed_at];
  assert.throws(() => buildFundedDirectShadowSeal(input(outOfOrder)), /not time ordered/);

  const tooFewMarkets = transitions().map((row, index) => ({ ...row, market_id: `market-${(index % 19) + 1}` }));
  assert.throws(() => buildFundedDirectShadowSeal(input(tooFewMarkets)), /at least 20/);

  for (const failure of LISTED_FAILURES) {
    const rows = transitions();
    rows[99].failures[failure] = true;
    assert.throws(() => buildFundedDirectShadowSeal(input(rows)), /listed failure count must be zero/);
  }
});

test("validation rejects funded execution and any sample other than the frozen 100 transitions", () => {
  const funded = transitions();
  funded[0].funded_order_submitted = true;
  assert.throws(() => buildFundedDirectShadowSeal(input(funded)), /attempted funded execution/);
  assert.throws(() => buildFundedDirectShadowSeal(input(transitions().slice(0, 99))), /must equal 100/);
  assert.throws(() => buildFundedDirectShadowSeal(input([...transitions(), transitions()[0]])), /must equal 100/);
});
