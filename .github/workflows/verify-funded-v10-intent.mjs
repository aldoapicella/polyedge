import assert from "node:assert/strict";
import fs from "node:fs";
import test from "node:test";

function decimal(value, name) {
  assert.equal(typeof value, "string", `${name} must be a decimal string`);
  const match = /^(0|[1-9][0-9]*)(?:\.([0-9]+))?$/.exec(value);
  assert.ok(match, `${name} must be a canonical nonnegative decimal string`);
  const fraction = match[2] ?? "";
  return { coefficient: BigInt(`${match[1]}${fraction}`), scale: fraction.length };
}

function scaledCoefficient(value, scale) {
  return value.coefficient * (10n ** BigInt(scale - value.scale));
}

function compare(left, right) {
  const scale = Math.max(left.scale, right.scale);
  const leftCoefficient = scaledCoefficient(left, scale);
  const rightCoefficient = scaledCoefficient(right, scale);
  return leftCoefficient < rightCoefficient ? -1 : leftCoefficient > rightCoefficient ? 1 : 0;
}

function add(left, right) {
  const scale = Math.max(left.scale, right.scale);
  return {
    coefficient: scaledCoefficient(left, scale) + scaledCoefficient(right, scale),
    scale
  };
}

function multiply(left, right) {
  return {
    coefficient: left.coefficient * right.coefficient,
    scale: left.scale + right.scale
  };
}

export function verifyFundedV10Intent(intent, expectedDecision) {
  const authorizedTarget = decimal("10.5", "authorized target");
  const zero = decimal("0", "zero");
  const one = decimal("1", "one");
  assert.equal(intent.decision_id, expectedDecision);
  assert.equal(typeof intent.market_id, "string", "market_id must use the Rust string wire type");
  assert.match(intent.market_id, /^[0-9]+$/);

  const price = decimal(intent.price, "price");
  const shares = decimal(intent.shares, "shares");
  const notional = decimal(intent.notional, "notional");
  const fee = decimal(intent.fee_allowance, "fee_allowance");
  const minimum = decimal(intent.minimum_order_size, "minimum_order_size");
  assert.ok(compare(price, zero) > 0 && compare(price, one) < 0);
  assert.ok(compare(shares, zero) > 0);
  assert.ok(compare(minimum, zero) > 0);

  const riskPerShare = add(price, fee);
  const targetCentsNumerator = authorizedTarget.coefficient
    * 100n
    * (10n ** BigInt(riskPerShare.scale));
  const targetCentsDenominator = riskPerShare.coefficient
    * (10n ** BigInt(authorizedTarget.scale));
  const sizedShares = {
    coefficient: targetCentsNumerator / targetCentsDenominator,
    scale: 2
  };
  const expectedShares = compare(sizedShares, minimum) >= 0 ? sizedShares : minimum;
  assert.equal(compare(shares, expectedShares), 0, "shares do not match fee-aware 2dp floor");
  assert.equal(compare(notional, multiply(price, shares)), 0, "notional does not equal price * shares");
  assert.ok(compare(notional, authorizedTarget) <= 0, "principal notional exceeds authorization cap");
}

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
  try {
    verifyFundedV10Intent(intent, decision);
    return true;
  } catch {
    return false;
  }
}

if (process.argv[2] === "--verify") {
  const intent = JSON.parse(fs.readFileSync(process.argv[3], "utf8"));
  verifyFundedV10Intent(intent, process.argv[4]);
} else {
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
    for (const intent of [
      { ...legitimateRoundedIntent, market_id: [123] },
      { ...legitimateRoundedIntent, shares: "22.47", notional: "10.1115" },
      { ...legitimateRoundedIntent, shares: "22.45", notional: "10.1025" },
      { ...legitimateRoundedIntent, price: "0.07", shares: "149.99", notional: "10.4993", fee_allowance: "0" },
      { ...legitimateRoundedIntent, shares: "23.01", notional: "10.3545", minimum_order_size: "23" },
      { ...legitimateRoundedIntent, shares: "24", notional: "10.8", minimum_order_size: "24" },
      { ...legitimateRoundedIntent, notional: "10.1070001" },
      { ...legitimateRoundedIntent, notional: "NaN" },
      { ...legitimateRoundedIntent, minimum_order_size: "0" }
    ]) assert.equal(provesAuthorizedRisk(intent), false);
  });
}
