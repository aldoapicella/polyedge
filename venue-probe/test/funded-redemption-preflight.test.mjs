import assert from "node:assert/strict";
import test from "node:test";
import { collectApprovedRedemptionPreflight, validateClobRead, validateTerminalRedemptionControl } from "../../ops/conduit/bin/polyedge-funded-redemption-preflight.mjs";
import { loadAccountPositions } from "../src/canary.mjs";

test("approved redemption preflight binds a complete stable inventory and exact chain payout", async () => {
  const condition = `0x${"a".repeat(64)}`;
  const timestamp = new Date("2026-09-17T04:00:00Z");
  const positions = Array.from({ length: 501 }, (_, i) => ({ asset: String(i + 1), size: 1, currentValue: 0, redeemable: true }));
  positions.push({ asset: "502", size: 5, currentValue: 5, redeemable: true });
  const selection = {
    payout_source: "onchain_balances_and_payout_vector", available_winner_conditions: 1,
    skipped_winner_conditions: 0, selected_gross_payout: 5,
    selected: [{ condition_id: condition, asset_ids: ["11", "22"],
      onchain_balances_base_units: ["5000000", "0"], payout_numerators: ["1", "0"],
      payout_denominator: "1", onchain_expected_payout: 5, gross_payout: 5 }]
  };
  const input = { condition, targetImage: `ghcr.io/aldoapicella/polyedge-venue-probe@sha256:${"b".repeat(64)}`,
    targetRevision: "c".repeat(40), block: { chain_id: 137, number: "99", hash: `0x${"d".repeat(64)}`,
      timestamp: timestamp.getTime() / 1000 - 2 }, now: () => timestamp,
    readOrders: async () => [], readPositions: async () => positions,
    readReservations: async () => [], discover: async () => structuredClone(selection) };
  const result = await collectApprovedRedemptionPreflight(input);
  assert.equal(result.position_inventory.rows, 502);
  assert.equal(result.position_inventory.readbacks, 2);
  assert.equal(result.position_inventory.complete, true);
  assert.equal(result.payout_base_units, "5000000");
  assert.equal(result.redemption_submitted, false);

  let localTime = timestamp.getTime() - 673, waited = 0, readsBeforeClock = 0;
  const future = { ...input, block: { ...input.block, timestamp: timestamp.getTime() / 1000 },
    now: () => new Date(localTime), wait: async milliseconds => { waited = milliseconds; localTime += milliseconds; },
    readOrders: async () => { assert(localTime >= timestamp.getTime()); readsBeforeClock++; return []; } };
  const afterWait = await collectApprovedRedemptionPreflight(future);
  assert.equal(waited, 673);
  assert.equal(readsBeforeClock, 2);
  assert.equal(afterWait.started_ts, timestamp.toISOString());
  await assert.rejects(collectApprovedRedemptionPreflight({ ...future,
    now: () => new Date(timestamp.getTime() - 673), wait: async () => {} }));
  await assert.rejects(collectApprovedRedemptionPreflight({ ...future,
    now: () => new Date(timestamp.getTime() - 1001), wait: async () => assert.fail("must not wait") }), /more than one second/);

  for (const override of [
    { readOrders: async () => [{}] },
    { readReservations: async () => [{}] },
    { readPositions: async () => [...positions, { asset: "503", size: 1, currentValue: 0, redeemable: false }] },
    { readPositions: async () => [...positions, { asset: "503", size: 0, currentValue: 1, redeemable: false }] },
    { readPositions: async () => [{ ...positions[0], asset: undefined }] },
    { discover: async () => ({ ...selection, available_winner_conditions: 2 }) },
    { condition: `0x${"e".repeat(64)}` },
    { discover: async () => ({ ...selection, selected_gross_payout: 6 }) },
    { discover: async () => ({ ...selection, selected: [{ ...selection.selected[0], payout_numerators: ["2", "0"] }] }) },
    { block: { ...input.block, timestamp: input.block.timestamp - 120 } }
  ]) await assert.rejects(collectApprovedRedemptionPreflight({ ...input, ...override }));
  let reads = 0;
  await assert.rejects(collectApprovedRedemptionPreflight({ ...input,
    readPositions: async () => ++reads === 1 ? positions : positions.slice(1)
  }), /inventory changed/);

  const overlapping = [...positions.slice(0, 500), positions[0], positions[501]];
  await assert.rejects(collectApprovedRedemptionPreflight({ ...input,
    readPositions: () => loadAccountPositions({ user: result.funder, fetcher: async url => {
      const offset = Number(new URL(url).searchParams.get("offset"));
      return overlapping.slice(offset, offset + 500);
    } })
  }), /overlapping position pages/);
  assert.equal(validateClobRead("/time", 1789620000), 1789620000);
  assert.deepEqual(validateClobRead("/data/orders", { data: [], next_cursor: "LTE=" }), { data: [], next_cursor: "LTE=" });
  assert.throws(() => validateClobRead("/data/orders", { error: "auth failed", data: [], next_cursor: "LTE=" }), /CLOB error/);
  assert.throws(() => validateClobRead("/data/orders", { data: [], next_cursor: "next" }), /incomplete/);
  assert.throws(() => validateClobRead("/time", { timestamp: 1789620000 }), /invalid server time/);
});

test("redemption preflight rejects pending control and recovery publication", () => {
  const value = {schema_version:1,state:"confirmed_and_verified",funder:"0x3d701b05d7c36afab01a06fd26ebe789c0b7bad8",
    submission_attempted:true,transaction_id:"verified-transaction",condition_ids:[`0x${"b".repeat(64)}`],
    run_id:"venue-redemption-20260917024026119-737e056f",transaction_hash:`0x${"a".repeat(64)}`,
    internal_settlement_blobs:[`reports/funded/dynamic-quote/sessions/dynamic-quote-funded-2026-09-16-v11/internal-settlements/${"a".repeat(64)}.json`]};
  assert.equal(validateTerminalRedemptionControl(value), "confirmed_and_verified");
  assert.throws(() => validateTerminalRedemptionControl({...value,state:"submission_attempted"}));
  assert.throws(() => validateTerminalRedemptionControl({...value,recovery_journal_blob_name:"pending"}));
  for (const mutation of [{submission_attempted:false},{transaction_id:""},{condition_ids:[]},
    {internal_settlement_blobs:[null]},{recovery_journal_blob_name:false}]) {
    assert.throws(() => validateTerminalRedemptionControl({...value,...mutation}));
  }
});
