import assert from "node:assert/strict";
import fs from "node:fs";
import { spawnSync } from "node:child_process";
import test from "node:test";
import { tmpdir } from "node:os";
import { join } from "node:path";

const workflow = fs.readFileSync(
  new URL("./validate-funded-direct.yml", import.meta.url),
  "utf8"
);
const proof = workflow.match(
  /# funded-v10-intent-risk-proof-begin\n\s+node - funded-v10-intent\.json "\$decision_id" <<'NODE'\n([\s\S]*?)\n\s+NODE\n\s+# funded-v10-intent-risk-proof-end/
);
assert.ok(proof, "funded v10 immutable-intent risk proof was not found");

const decision = "4".repeat(64);
const legitimateRoundedIntent = {
  decision_id: decision,
  market_id: "123",
  price: "0.45",
  shares: "22.46",
  notional: "10.107",
  fee_allowance: "0.017325",
  minimum_order_size: "5"
};

function provesAuthorizedRisk(intent) {
  const path = join(tmpdir(), `funded-v10-intent-${process.pid}-${Math.random()}.json`);
  fs.writeFileSync(path, JSON.stringify(intent));
  const result = spawnSync(process.execPath, ["-", path, decision], {
    input: proof[1],
    encoding: "utf8"
  });
  fs.unlinkSync(path);
  return result.status === 0;
}

test("funded intent proof accepts fee-aware rounding instead of exact principal", () => {
  assert.notEqual(legitimateRoundedIntent.notional, "10.5");
  assert.equal(provesAuthorizedRisk(legitimateRoundedIntent), true);
});

test("funded intent proof accepts minimum dominance and an exact target boundary", () => {
  assert.equal(provesAuthorizedRisk({
    ...legitimateRoundedIntent,
    shares: "23",
    notional: "10.35",
    minimum_order_size: "23"
  }), true);
  assert.equal(provesAuthorizedRisk({
    ...legitimateRoundedIntent,
    shares: "23.001",
    notional: "10.35045",
    minimum_order_size: "23.001"
  }), true);
  assert.equal(provesAuthorizedRisk({
    ...legitimateRoundedIntent,
    price: "0.07",
    shares: "150",
    notional: "10.50",
    fee_allowance: "0"
  }), true);
});

test("funded intent proof rejects sizing, cap, arithmetic, and numeric violations", () => {
  assert.equal(provesAuthorizedRisk({
    ...legitimateRoundedIntent,
    market_id: [123]
  }), false);
  assert.equal(provesAuthorizedRisk({
    ...legitimateRoundedIntent,
    shares: "22.47",
    notional: "10.1115"
  }), false);
  assert.equal(provesAuthorizedRisk({
    ...legitimateRoundedIntent,
    shares: "22.45",
    notional: "10.1025"
  }), false);
  assert.equal(provesAuthorizedRisk({
    ...legitimateRoundedIntent,
    price: "0.07",
    shares: "149.99",
    notional: "10.4993",
    fee_allowance: "0"
  }), false);
  assert.equal(provesAuthorizedRisk({
    ...legitimateRoundedIntent,
    shares: "23.01",
    notional: "10.3545",
    minimum_order_size: "23"
  }), false);
  assert.equal(provesAuthorizedRisk({
    ...legitimateRoundedIntent,
    shares: "24",
    notional: "10.8",
    minimum_order_size: "24"
  }), false);
  assert.equal(provesAuthorizedRisk({
    ...legitimateRoundedIntent,
    notional: "10.1070001"
  }), false);
  assert.equal(provesAuthorizedRisk({
    ...legitimateRoundedIntent,
    notional: "NaN"
  }), false);
  assert.equal(provesAuthorizedRisk({
    ...legitimateRoundedIntent,
    minimum_order_size: "0"
  }), false);
});
