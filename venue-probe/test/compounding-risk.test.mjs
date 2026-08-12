import test from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import {
  discoverVerifiedAutomaticInternalSettlements,
  internalSettlementBlobName,
  loadDurableInternalSettlements,
  migrateProtectedReserveState,
  putVerifiedInternalSettlement,
  reconcileProtectedCompoundingState,
  sizeProtectedOrder,
  validateProtectedCompoundingManifest,
  validateProtectedCompoundingPredecessorState,
  verifyAutomaticSettlementEvidence
} from "../src/compounding-risk.mjs";

function manifest() {
  return {
    schema_version: "polyedge.operator_funded_session.v2",
    session_id: "dynamic-quote-funded-test-v5",
    allow_compounding: true,
    starting_collateral: 11.09862,
    max_reconciliation_discrepancy: 0.01,
    created_at: "2026-07-30T00:00:00.000Z",
    capital_policy: {
      reserve_ratio: 0.3,
      operating_buffer_ratio: 0.01,
      minimum_order_notional: 1,
      high_water_update: "full_reconciliation_only",
      reserve_monotonic: true,
      state_blob_name:
        "reports/funded/dynamic-quote/sessions/dynamic-quote-funded-test-v5/capital-reserve-state.json"
    },
    internal_settlements: []
  };
}

class Container {
  constructor() {
    this.values = new Map();
    this.etags = new Map();
  }
  async *listBlobsFlat({ prefix }) {
    for (const name of this.values.keys()) {
      if (name.startsWith(prefix)) yield { name };
    }
  }
  getBlobClient(name) {
    return {
      download: async () => {
        if (!this.values.has(name)) {
          throw Object.assign(new Error("missing"), { statusCode: 404 });
        }
        return {
          readableStreamBody: stream(this.values.get(name)),
          etag: this.etags.get(name)
        };
      }
    };
  }
  getBlockBlobClient(name) {
    return {
      download: async () => this.getBlobClient(name).download(),
      uploadData: async (bytes, options = {}) => {
        const current = this.etags.get(name);
        if (options.conditions?.ifNoneMatch === "*" && this.values.has(name)) {
          throw Object.assign(new Error("exists"), { statusCode: 412 });
        }
        if (options.conditions?.ifMatch && options.conditions.ifMatch !== current) {
          throw Object.assign(new Error("etag mismatch"), { statusCode: 412 });
        }
        const next = `"${Number(String(current || "\"0\"").replaceAll("\"", "")) + 1}"`;
        this.values.set(name, Buffer.from(bytes));
        this.etags.set(name, next);
      }
    };
  }
}

function stream(value) {
  return (async function* () { yield Buffer.from(value); })();
}

test("protected compounding contract fixes the reserve at 30% with a 1% buffer", () => {
  assert.deepEqual(validateProtectedCompoundingManifest(manifest()), {
    reserveRatio: 0.3,
    operatingBufferRatio: 0.01,
    minimumOrderNotional: 1,
    reserveBasis: "fully_reconciled_high_water_equity",
    reserveMonotonic: true,
    lossResponse: null,
    stateSchema: "polyedge.protected_compounding_state.v1",
    priorStateBlobName: null,
    priorStateSessionId: null,
    priorStateSha256: null,
    minimumHistoricalHighWaterEquity: null,
    stateBlobName:
      "reports/funded/dynamic-quote/sessions/dynamic-quote-funded-test-v5/capital-reserve-state.json",
    internalSettlements: []
  });
});

function lossResizingManifest() {
  const value = manifest();
  value.schema_version = "polyedge.operator_funded_session.v3";
  value.session_id = "dynamic-quote-funded-test-v7";
  value.continue_after_loss = true;
  value.capital_policy = {
    ...value.capital_policy,
    reserve_basis: "fully_reconciled_current_equity",
    loss_response: "resize_from_fully_reconciled_current_equity",
    prior_state_session_id: "dynamic-quote-funded-test-v5",
    prior_state_blob_name:
      "reports/funded/dynamic-quote/sessions/dynamic-quote-funded-test-v5/capital-reserve-state.json",
    minimum_historical_high_water_equity: 17.90462,
    reserve_monotonic: false,
    state_blob_name:
      "reports/funded/dynamic-quote/sessions/dynamic-quote-funded-test-v7/capital-reserve-state.json"
  };
  return value;
}

function lossTolerantManifest() {
  const value = lossResizingManifest();
  value.session_id = "dynamic-quote-funded-test-v8";
  value.starting_collateral = 31.655501;
  value.capital_policy = {
    ...value.capital_policy,
    reserve_ratio: 0.1,
    minimum_reserve: 2,
    target_order_ratio: 0.05,
    prior_state_sha256: documentHash(priorState({
      highWater: 31.655501,
      protectedReserve: 9.49665
    })),
    minimum_historical_high_water_equity: 31.655501,
    state_blob_name:
      "reports/funded/dynamic-quote/sessions/dynamic-quote-funded-test-v8/capital-reserve-state.json"
  };
  return value;
}

function priorState({
  highWater = 17.90462,
  protectedReserve = 5.371386
} = {}) {
  return {
    schema: "polyedge.protected_compounding_state.v1",
    session_id: "dynamic-quote-funded-test-v5",
    high_water_equity: highWater,
    protected_reserve: protectedReserve,
    reconciliation_complete: true,
    reserve_monotonic: true
  };
}

function documentHash(value) {
  return `sha256:${createHash("sha256").update(JSON.stringify(value)).digest("hex")}`;
}

function seedPriorState(container, options = {}) {
  const name =
    "reports/funded/dynamic-quote/sessions/dynamic-quote-funded-test-v5/capital-reserve-state.json";
  container.values.set(name, Buffer.from(JSON.stringify(priorState(options))));
  container.etags.set(name, '"1"');
}

function manualSettlement({ id, sessionId, transaction, condition, payout, principal }) {
  return {
    schema: "polyedge.verified_internal_settlement.v1",
    id,
    type: "internal_manual_settlement",
    session_id: sessionId,
    transaction_hash: `0x${transaction.repeat(64)}`,
    condition_id: `0x${condition.repeat(64)}`,
    payout,
    principal,
    realized_pnl: payout - principal,
    fill_transaction_hashes: [`0x${transaction.toUpperCase().repeat(64)}`],
    evidence_source: "polymarket_data_api_fills_plus_polygon_receipt",
    receipt_block_number: "12345678",
    receipt_confirmations: 3
  };
}

function configuredSettlement(value) {
  const {
    schema: _schema,
    session_id: _sessionId,
    evidence_source: _evidenceSource,
    receipt_block_number: _block,
    receipt_confirmations: _confirmations,
    ...configured
  } = value;
  return configured;
}

async function reserveMigrationFixture() {
  const container = new Container();
  const target = manifest();
  const source = lossResizingManifest();
  const existing = manualSettlement({
    id: "manual-existing",
    sessionId: target.session_id,
    transaction: "a",
    condition: "b",
    payout: 17.015,
    principal: 10.209
  });
  const sourceExisting = { ...existing, session_id: source.session_id };
  const sourceNew = manualSettlement({
    id: "manual-v7-profit",
    sessionId: source.session_id,
    transaction: "c",
    condition: "d",
    payout: 57.997,
    principal: 0
  });
  target.internal_settlements = [configuredSettlement(existing)];
  source.internal_settlements = [configuredSettlement(sourceExisting)];
  await putVerifiedInternalSettlement(container, existing);
  await putVerifiedInternalSettlement(container, sourceExisting);
  await putVerifiedInternalSettlement(container, sourceNew);
  seedPriorState(container);
  const sourceSessionBlobName =
    `reports/funded/dynamic-quote/sessions/${source.session_id}/session.json`;
  const sourceStateBlobName = source.capital_policy.state_blob_name;
  const sourceBytes = Buffer.from(JSON.stringify(source, null, 2));
  container.values.set(sourceSessionBlobName, sourceBytes);
  container.etags.set(sourceSessionBlobName, '"1"');
  container.values.set(sourceStateBlobName, Buffer.from(JSON.stringify({
    schema: "polyedge.protected_compounding_state.v2",
    session_id: source.session_id,
    reserve_ratio: 0.3,
    operating_buffer_ratio: 0.01,
    minimum_order_notional: 1,
    high_water_equity: 75.90162,
    historical_high_water_equity: 75.90162,
    protected_reserve: 14.94963,
    last_reconciled_equity: 49.832101,
    operating_buffer: 0.498321,
    operable_capital: 34.38415,
    authorized_equity_ceiling: 75.90162,
    verified_realized_pnl: 64.803,
    verified_settlement_ids: [existing.id, sourceNew.id].sort(),
    reconciliation_complete: true,
    prior_state_session_id: target.session_id,
    prior_state_blob_name: target.capital_policy.state_blob_name,
    reserve_basis: "fully_reconciled_current_equity",
    loss_response: "resize_from_fully_reconciled_current_equity",
    continue_after_loss: true,
    reserve_monotonic: false
  }, null, 2)));
  container.etags.set(sourceStateBlobName, '"7"');
  return {
    container,
    target,
    source: {
      sessionId: source.session_id,
      sessionBlobName: sourceSessionBlobName,
      sessionHash: `sha256:${createHash("sha256").update(sourceBytes).digest("hex")}`,
      stateBlobName: sourceStateBlobName,
      minimumHistoricalHighWaterEquity: 75.90162
    },
    existing,
    sourceNew
  };
}

test("v7-to-v5 migration preserves the fully reconciled high water and is idempotent", async () => {
  const fixture = await reserveMigrationFixture();
  const first = await migrateProtectedReserveState({
    container: fixture.container,
    manifest: fixture.target,
    source: fixture.source,
    accountEquity: 49.832101,
    fullyReconciled: true,
    openOrderCount: 0,
    positionCount: 0,
    unresolvedReservationCount: 0,
    now: () => new Date("2026-08-06T03:00:00.000Z")
  });
  assert.equal(first.state.high_water_equity, 75.90162);
  assert.equal(first.state.protected_reserve, 22.770486);
  assert.equal(first.state.authorized_equity_ceiling, 75.90162);
  assert.equal(first.state.reserve_basis, "fully_reconciled_high_water_equity");
  assert.equal(first.state.reserve_monotonic, true);
  assert.equal(first.state.migration_source_state_etag, '"7"');
  assert.equal(first.state.migration_minimum_historical_high_water_equity, 75.90162);
  assert.deepEqual(first.state.migration_verified_settlement_ids, [
    fixture.existing.id,
    fixture.sourceNew.id
  ].sort());
  const migratedLedger = await loadDurableInternalSettlements(
    fixture.container,
    fixture.target.session_id
  );
  assert.equal(migratedLedger.length, 2);
  assert.equal(
    migratedLedger.find((row) => row.id === fixture.sourceNew.id)
      .migration_source_session_id,
    fixture.source.sessionId
  );
  const targetEtag = fixture.container.etags.get(
    fixture.target.capital_policy.state_blob_name
  );
  const postMigration = manualSettlement({
    id: "manual-v5-after-migration",
    sessionId: fixture.target.session_id,
    transaction: "e",
    condition: "f",
    payout: 2,
    principal: 1
  });
  await putVerifiedInternalSettlement(fixture.container, postMigration);
  const reconciled = await reconcileProtectedCompoundingState({
    container: fixture.container,
    manifest: fixture.target,
    accountEquity: 48,
    fullyReconciled: true,
    now: () => new Date("2026-08-06T03:30:00.000Z")
  });
  assert.ok(reconciled.verified_settlement_ids.includes(postMigration.id));
  const reconciledEtag = fixture.container.etags.get(
    fixture.target.capital_policy.state_blob_name
  );
  const second = await migrateProtectedReserveState({
    container: fixture.container,
    manifest: fixture.target,
    source: fixture.source,
    accountEquity: 40,
    fullyReconciled: false,
    openOrderCount: 1,
    positionCount: 1,
    unresolvedReservationCount: 1,
    now: () => new Date("2026-08-06T04:00:00.000Z")
  });
  assert.equal(fixture.container.etags.get(
    fixture.target.capital_policy.state_blob_name
  ), reconciledEtag);
  assert.notEqual(reconciledEtag, targetEtag);
  assert.equal(second.state.migration_completed_at, first.state.migration_completed_at);
  assert.equal(reconciled.migration_source_state_etag, '"7"');
  assert.deepEqual(
    reconciled.migration_verified_settlement_ids,
    first.state.migration_verified_settlement_ids
  );

  const stateBlobName = fixture.target.capital_policy.state_blob_name;
  const stateEtag = fixture.container.etags.get(stateBlobName);
  const validStateBytes = fixture.container.values.get(stateBlobName);
  const tamperedState = JSON.parse(validStateBytes);
  tamperedState.authorized_equity_ceiling += 1;
  fixture.container.values.set(stateBlobName, Buffer.from(JSON.stringify(tamperedState)));
  await assert.rejects(migrateProtectedReserveState({
    container: fixture.container,
    manifest: fixture.target,
    source: fixture.source,
    accountEquity: 40,
    fullyReconciled: false,
    openOrderCount: 1,
    positionCount: 1,
    unresolvedReservationCount: 1
  }), /migration checkpoint is invalid/);
  fixture.container.values.set(stateBlobName, validStateBytes);
  const migratedBlobName = internalSettlementBlobName(
    fixture.target.session_id,
    fixture.sourceNew.transaction_hash,
    fixture.sourceNew.condition_id
  );
  fixture.container.values.delete(migratedBlobName);
  fixture.container.etags.delete(migratedBlobName);
  await assert.rejects(migrateProtectedReserveState({
    container: fixture.container,
    manifest: fixture.target,
    source: fixture.source,
    accountEquity: 40,
    fullyReconciled: false,
    openOrderCount: 1,
    positionCount: 1,
    unresolvedReservationCount: 1
  }), /migration checkpoint is invalid/);
  await assert.rejects(reconcileProtectedCompoundingState({
    container: fixture.container,
    manifest: fixture.target,
    accountEquity: 10,
    fullyReconciled: true
  }), /migration checkpoint is invalid/);
  assert.equal(fixture.container.etags.get(stateBlobName), stateEtag);
});

test("reserve migration makes no target writes when reconciliation or source floor fails", async () => {
  for (const [name, mutate, expected] of [
    ["open order", (input) => { input.openOrderCount = 1; }, /zero orders/],
    ["unresolved position", (input) => { input.positionCount = 1; }, /zero orders/],
    ["unresolved reservation", (input) => { input.unresolvedReservationCount = 1; }, /zero orders/],
    ["excess equity", (input) => { input.accountEquity = 76; }, /verified ceiling/],
    ["stale high water", (input) => {
      const state = JSON.parse(input.container.values.get(input.source.stateBlobName));
      state.high_water_equity = 75;
      state.historical_high_water_equity = 75;
      input.container.values.set(input.source.stateBlobName, Buffer.from(JSON.stringify(state)));
    }, /fully reconciled/],
    ["source ledger mismatch", (input) => {
      const state = JSON.parse(input.container.values.get(input.source.stateBlobName));
      state.verified_realized_pnl = 0;
      input.container.values.set(input.source.stateBlobName, Buffer.from(JSON.stringify(state)));
    }, /fully reconciled/]
  ]) {
    const fixture = await reserveMigrationFixture();
    const targetLedgerBefore = (await loadDurableInternalSettlements(
      fixture.container,
      fixture.target.session_id
    )).length;
    const input = {
      container: fixture.container,
      manifest: fixture.target,
      source: fixture.source,
      accountEquity: 49.832101,
      fullyReconciled: true,
      openOrderCount: 0,
      positionCount: 0,
      unresolvedReservationCount: 0
    };
    mutate(input);
    await assert.rejects(migrateProtectedReserveState(input), expected, name);
    assert.equal((await loadDurableInternalSettlements(
      fixture.container,
      fixture.target.session_id
    )).length, targetLedgerBefore, name);
  }
});

test("loss-resizing contract binds the reserve to fully reconciled current equity", () => {
  assert.deepEqual(validateProtectedCompoundingManifest(lossResizingManifest()), {
    reserveRatio: 0.3,
    operatingBufferRatio: 0.01,
    minimumOrderNotional: 1,
    reserveBasis: "fully_reconciled_current_equity",
    reserveMonotonic: false,
    lossResponse: "resize_from_fully_reconciled_current_equity",
    stateSchema: "polyedge.protected_compounding_state.v2",
    priorStateBlobName:
      "reports/funded/dynamic-quote/sessions/dynamic-quote-funded-test-v5/capital-reserve-state.json",
    priorStateSessionId: "dynamic-quote-funded-test-v5",
    priorStateSha256: null,
    minimumHistoricalHighWaterEquity: 17.90462,
    stateBlobName:
      "reports/funded/dynamic-quote/sessions/dynamic-quote-funded-test-v7/capital-reserve-state.json",
    internalSettlements: []
  });
});

test("predecessor validation reuses the exact loss-resizing invariant", () => {
  const policy = validateProtectedCompoundingManifest(lossResizingManifest());
  const state = {
    schema: "polyedge.protected_compounding_state.v1",
    session_id: policy.priorStateSessionId,
    high_water_equity: 17.90462,
    protected_reserve: 5.371386,
    reconciliation_complete: true,
    reserve_monotonic: true
  };
  assert.equal(validateProtectedCompoundingPredecessorState(state, policy), state);
  assert.throws(
    () => validateProtectedCompoundingPredecessorState({ ...state, reserve_monotonic: false }, policy),
    /prior funded high-water state is unavailable or incompatible/
  );
});

test("loss-tolerant predecessor validation requires exact durable bytes", () => {
  const manifest = lossTolerantManifest();
  const policy = validateProtectedCompoundingManifest(manifest);
  const exact = priorState({
    highWater: 31.655501,
    protectedReserve: 9.49665
  });
  const exactHash = documentHash(exact);
  assert.equal(policy.priorStateSha256, exactHash);
  assert.equal(
    validateProtectedCompoundingPredecessorState(exact, policy, exactHash),
    exact
  );

  const modified = { ...exact, high_water_equity: 32 };
  assert.throws(
    () => validateProtectedCompoundingPredecessorState(
      modified,
      policy,
      documentHash(modified)
    ),
    /prior funded high-water state is unavailable or incompatible/
  );

  delete manifest.capital_policy.prior_state_sha256;
  assert.throws(
    () => validateProtectedCompoundingManifest(manifest),
    /prior_state_sha256 is required for loss-tolerant sizing/
  );
});

test("successive losses resize orders instead of freezing the historical high water", async () => {
  const container = new Container();
  seedPriorState(container);
  const fundedManifest = lossResizingManifest();
  const verifiedConfiguredSettlements = [{
    id: "manual-redeem-1",
    type: "internal_manual_settlement",
    transaction_hash: `0x${"a".repeat(64)}`,
    condition_id: `0x${"b".repeat(64)}`,
    payout: 17.015,
    principal: 10.209,
    realized_pnl: 6.806,
    fill_transaction_hashes: [`0x${"c".repeat(64)}`]
  }];
  fundedManifest.internal_settlements = verifiedConfiguredSettlements;
  const first = await reconcileProtectedCompoundingState({
    container,
    manifest: fundedManifest,
    accountEquity: 13.323639,
    fullyReconciled: true,
    verifiedConfiguredSettlements
  });
  const afterLoss = await reconcileProtectedCompoundingState({
    container,
    manifest: fundedManifest,
    accountEquity: 5,
    fullyReconciled: true,
    verifiedConfiguredSettlements
  });
  assert.equal(first.high_water_equity, 17.90462);
  assert.equal(afterLoss.high_water_equity, 17.90462);
  assert.equal(first.protected_reserve, 3.997092);
  assert.equal(first.operable_capital, 9.193311);
  assert.equal(afterLoss.protected_reserve, 1.5);
  assert.equal(afterLoss.operable_capital, 3.45);
  assert.equal(afterLoss.continue_after_loss, true);
  assert.equal(afterLoss.reserve_monotonic, false);

  const sizing = sizeProtectedOrder({
    state: afterLoss,
    accountEquity: 5,
    price: 0.2,
    requestedShares: 52.5,
    requestedNotional: 10.5,
    minimumOrderSize: 5,
    maximumOrderNotional: 10.5,
    feePerShare: 0.001
  });
  assert.equal(sizing.executable, true);
  assert.ok(sizing.reserved_notional <= 3.45);
  assert.ok(sizing.notional >= 1);
});

test("loss resizing still fails closed below the venue and policy minimum", async () => {
  const container = new Container();
  seedPriorState(container);
  const state = await reconcileProtectedCompoundingState({
    container,
    manifest: lossResizingManifest(),
    accountEquity: 1.4,
    fullyReconciled: true
  });
  const sizing = sizeProtectedOrder({
    state,
    accountEquity: 1.4,
    price: 0.2,
    requestedShares: 52.5,
    requestedNotional: 10.5,
    minimumOrderSize: 5,
    maximumOrderNotional: 10.5,
    feePerShare: 0
  });
  assert.equal(sizing.executable, false);
  assert.ok(sizing.blockers.includes("protected_order_below_policy_minimum"));
});

test("loss-tolerant sizing uses a 10% current-equity reserve and minimum orders after losses", async () => {
  const container = new Container();
  seedPriorState(container, {
    highWater: 31.655501,
    protectedReserve: 9.49665
  });
  const fundedManifest = lossTolerantManifest();
  const policy = validateProtectedCompoundingManifest(fundedManifest);
  assert.equal(policy.reserveRatio, 0.1);
  assert.equal(policy.minimumReserve, 2);
  assert.equal(policy.targetOrderRatio, 0.05);

  const initial = await reconcileProtectedCompoundingState({
    container,
    manifest: fundedManifest,
    accountEquity: 31.655501,
    fullyReconciled: true
  });
  assert.equal(initial.protected_reserve, 3.16555);
  assert.equal(initial.operating_buffer, 0.316555);
  assert.equal(initial.operable_capital, 28.173396);
  assert.equal(initial.prior_state_sha256, fundedManifest.capital_policy.prior_state_sha256);

  const input = {
    price: 0.3224734,
    requestedShares: 20,
    requestedNotional: 6.449468,
    minimumOrderSize: 5,
    maximumOrderNotional: 10.5,
    feePerShare: 0.017052
  };
  const initialSizing = sizeProtectedOrder({
    state: initial,
    accountEquity: 31.655501,
    ...input
  });
  assert.equal(initialSizing.executable, true);
  assert.equal(initialSizing.shares, 5);
  assert.equal(initialSizing.notional, 1.612367);
  assert.equal(initialSizing.fee_risk_upper_bound, 0.08526);
  assert.equal(initialSizing.reserved_notional, 1.697627);
  assert.equal(initialSizing.order_risk_budget, 1.697627);

  const afterLoss = await reconcileProtectedCompoundingState({
    container,
    manifest: fundedManifest,
    accountEquity: 5,
    fullyReconciled: true
  });
  const stateBlobName = fundedManifest.capital_policy.state_blob_name;
  const stateEtag = container.etags.get(stateBlobName);
  const readOnlyState = await reconcileProtectedCompoundingState({
    container,
    manifest: fundedManifest,
    accountEquity: 5,
    fullyReconciled: true,
    readOnly: true
  });
  assert.deepEqual(readOnlyState, afterLoss);
  assert.equal(container.etags.get(stateBlobName), stateEtag);
  await assert.rejects(reconcileProtectedCompoundingState({
    container,
    manifest: fundedManifest,
    accountEquity: 4,
    fullyReconciled: true,
    readOnly: true
  }), /read-only reconciliation requires exact current protected capital state/);
  assert.equal(container.etags.get(stateBlobName), stateEtag);
  const afterLossSizing = sizeProtectedOrder({
    state: afterLoss,
    accountEquity: 5,
    ...input
  });
  assert.equal(afterLoss.high_water_equity, 31.655501);
  assert.equal(afterLoss.protected_reserve, 2);
  assert.equal(afterLoss.continue_after_loss, true);
  assert.equal(afterLossSizing.executable, true);
  assert.equal(afterLossSizing.shares, 5);
  assert.equal(afterLossSizing.reserved_notional, 1.697627);

  const boundarySizing = sizeProtectedOrder({
    state: afterLoss,
    accountEquity: 5,
    price: 0.22,
    requestedShares: 20,
    requestedNotional: 4.4,
    minimumOrderSize: 5,
    maximumOrderNotional: 10.5,
    feePerShare: 0.00736164
  });
  assert.equal(boundarySizing.executable, true);
  assert.equal(boundarySizing.shares, 5);
  assert.equal(boundarySizing.order_risk_budget, 1.136809);
  assert.equal(boundarySizing.reserved_notional, 1.136808);

  const insolvent = await reconcileProtectedCompoundingState({
    container,
    manifest: fundedManifest,
    accountEquity: 3.6,
    fullyReconciled: true
  });
  const insolventSizing = sizeProtectedOrder({
    state: insolvent,
    accountEquity: 3.6,
    ...input
  });
  assert.equal(insolventSizing.executable, false);
  assert.ok(insolventSizing.blockers.includes("protected_order_below_venue_minimum"));
});

test("loss resizing refuses a missing predecessor or incompatible cached state", async () => {
  const missing = new Container();
  await assert.rejects(
    reconcileProtectedCompoundingState({
      container: missing,
      manifest: lossResizingManifest(),
      accountEquity: 5,
      fullyReconciled: true
    }),
    /prior funded high-water state/
  );

  const container = new Container();
  seedPriorState(container);
  const fundedManifest = lossResizingManifest();
  const state = await reconcileProtectedCompoundingState({
    container,
    manifest: fundedManifest,
    accountEquity: 5,
    fullyReconciled: true
  });
  const name = fundedManifest.capital_policy.state_blob_name;
  container.values.set(name, Buffer.from(JSON.stringify({
    ...state,
    reserve_basis: "fully_reconciled_high_water_equity"
  })));
  await assert.rejects(
    reconcileProtectedCompoundingState({
      container,
      manifest: fundedManifest,
      accountEquity: 4,
      fullyReconciled: false
    }),
    /persisted protected compounding state is incompatible/
  );
});

test("a loss cannot lower the reconciled high-water reserve", async () => {
  const container = new Container();
  const verifiedConfiguredSettlements = [{
    id: "manual-redeem-1",
    type: "internal_manual_settlement",
    transaction_hash: `0x${"a".repeat(64)}`,
    condition_id: `0x${"b".repeat(64)}`,
    payout: 17.015,
    principal: 10.209,
    realized_pnl: 6.806,
    fill_transaction_hashes: [`0x${"c".repeat(64)}`]
  }];
  const fundedManifest = manifest();
  fundedManifest.internal_settlements = verifiedConfiguredSettlements;
  const first = await reconcileProtectedCompoundingState({
    container,
    manifest: fundedManifest,
    accountEquity: 17.90462,
    fullyReconciled: true,
    verifiedConfiguredSettlements
  });
  const afterLoss = await reconcileProtectedCompoundingState({
    container,
    manifest: fundedManifest,
    accountEquity: 7.57122,
    fullyReconciled: true,
    verifiedConfiguredSettlements
  });
  assert.equal(first.high_water_equity, 17.90462);
  assert.equal(afterLoss.high_water_equity, 17.90462);
  assert.equal(afterLoss.protected_reserve, 5.371386);
  assert.equal(afterLoss.operating_buffer, 0.075712);
  assert.equal(afterLoss.operable_capital, 2.124122);
});

test("current-funds sizing rounds down and never breaches the reserve", () => {
  const sizing = sizeProtectedOrder({
    state: {
      high_water_equity: 17.90462,
      protected_reserve: 5.371386,
      operating_buffer_ratio: 0.01,
      minimum_order_notional: 1,
      authorized_equity_ceiling: 17.90462
    },
    accountEquity: 7.57122,
    price: 0.2,
    requestedShares: 52.5,
    requestedNotional: 10.5,
    minimumOrderSize: 5,
    maximumOrderNotional: 10.5,
    feePerShare: 0.001
  });
  assert.equal(sizing.executable, true);
  assert.equal(sizing.shares, 10.56);
  assert.equal(sizing.notional, 2.112);
  assert.equal(sizing.fee_risk_upper_bound, 0.01056);
  assert.ok(sizing.reserved_notional <= sizing.operable_capital);
  assert.ok(sizing.shares <= sizing.source_shares);
});

test("venue or policy minimums produce a no-trade instead of reserve leakage", () => {
  const sizing = sizeProtectedOrder({
    state: {
      high_water_equity: 17.90462,
      protected_reserve: 5.371386,
      operating_buffer_ratio: 0.01,
      minimum_order_notional: 1,
      authorized_equity_ceiling: 17.90462
    },
    accountEquity: 7.57122,
    price: 0.5,
    requestedShares: 21,
    requestedNotional: 10.5,
    minimumOrderSize: 5,
    maximumOrderNotional: 10.5,
    feePerShare: 0
  });
  assert.equal(sizing.executable, false);
  assert.ok(sizing.blockers.includes("protected_order_below_venue_minimum"));
});

const automaticCondition = `0x${"d".repeat(64)}`;
const secondCondition = `0x${"c".repeat(64)}`;
const automaticRedemption = `0x${"e".repeat(64)}`;
const automaticFillTransaction = `0x${"f".repeat(64)}`;
const secondFillTransaction = `0x${"b".repeat(64)}`;
const expectedWallet = `0x${"a".repeat(40)}`;
const adapter = "0xada100db00ca00073811820692005400218fce1f";
const conditionalTokens = "0x4d97dcd97ec945f40cf65f87097ace5ea0476045";
const collateralToken = "0x2791bca1f2de4661ed88a30c99a7a9449aa84174";
const pusdToken = "0xc011a7e12a19f7b1f670d46f03b03f3342e82dfb";
const zeroAddress = `0x${"0".repeat(40)}`;
const parentCollection = `0x${"0".repeat(64)}`;
const automaticToken = "111111111111111111111111111111111111111";
const secondToken = "222222222222222222222222222222222222222";
const fillTimestampMs = Date.parse("2026-07-30T00:01:00.000Z");
const redemptionTimestamp = Date.parse("2026-07-30T00:10:00.000Z") / 1_000;

function settlementId(transactionHash, conditionId) {
  return `automatic-redeem-${createHash("sha256")
    .update(`${transactionHash}\u0000${conditionId}`)
    .digest("hex")}`;
}

function automaticReservation(overrides = {}) {
  return {
    campaign_id: "dynamic-quote-funded-test-v5",
    run_id: "run-funded-1",
    probe_id: "probe-funded-1",
    order_id: "order-funded-1",
    condition_id: automaticCondition,
    token_id: automaticToken,
    order_submission_intended: true,
    order_submitted: true,
    matched_notional: 2,
    fee_risk_upper_bound: 0.1,
    created_ts: "2026-07-30T00:00:30.000Z",
    ...overrides
  };
}

function tradeActivity({
  conditionId = automaticCondition,
  asset = automaticToken,
  transactionHash = automaticFillTransaction,
  size = 10,
  usdcSize = 2,
  ...overrides
} = {}) {
  return {
    type: "TRADE",
    side: "BUY",
    proxyWallet: expectedWallet,
    asset,
    transactionHash,
    conditionId,
    size,
    usdcSize,
    timestamp: fillTimestampMs / 1_000,
    ...overrides
  };
}

function redeemActivity({
  conditionId = automaticCondition,
  asset = "",
  payout = 10,
  ...overrides
} = {}) {
  return {
    type: "REDEEM",
    proxyWallet: expectedWallet,
    transactionHash: automaticRedemption,
    conditionId,
    asset,
    usdcSize: payout,
    timestamp: redemptionTimestamp,
    ...overrides
  };
}

function automaticFill({
  id = "authenticated-clob-fill-1",
  orderId = "order-funded-1",
  conditionId = automaticCondition,
  assetId = automaticToken,
  tradeAssetId = secondToken,
  nestedMakerOrderMatchCount = 1,
  directMakerOrder = false,
  transactionHash = automaticFillTransaction,
  size = 10,
  price = 0.2,
  ...overrides
} = {}) {
  return {
    id,
    size,
    price,
    timestampMs: fillTimestampMs,
    orderRole: "MAKER",
    transactionHash,
    market: conditionId,
    assetId,
    tradeAssetId,
    makerAssetId: assetId,
    nestedMakerOrderMatchCount,
    directMakerOrder,
    status: "TRADE_STATUS_CONFIRMED",
    orderId,
    makerOrderId: orderId,
    takerOrderId: "taker-order-1",
    owner: "authenticated-owner-1",
    makerAddress: expectedWallet,
    orderSide: "BUY",
    ...overrides
  };
}

function decodedRedemption({
  conditionId = automaticCondition,
  payout = 10,
  ...overrides
} = {}) {
  return {
    contract_address: conditionalTokens,
    transaction_hash: automaticRedemption,
    redeemer: adapter,
    collateral_token: collateralToken,
    parent_collection_id: parentCollection,
    condition_id: conditionId,
    index_sets: [1],
    payout_base_units: String(Math.round(payout * 1_000_000)),
    payout,
    ...overrides
  };
}

function confirmedReceipt(redemptions = [decodedRedemption()], overrides = {}) {
  const ctfTransfers = redemptions.flatMap((redemption) => [
    {
      event: "TransferBatch",
      contract_address: conditionalTokens,
      operator: adapter,
      from: expectedWallet,
      to: adapter,
      ids: [redemption.condition_id === automaticCondition ? automaticToken : secondToken],
      values: [redemption.payout_base_units]
    },
    {
      event: "TransferSingle",
      contract_address: conditionalTokens,
      operator: adapter,
      from: adapter,
      to: zeroAddress,
      ids: [redemption.condition_id === automaticCondition ? automaticToken : secondToken],
      values: [redemption.payout_base_units]
    }
  ]);
  const erc20Transfers = redemptions.flatMap((redemption) => [
    {
      token: collateralToken,
      from: conditionalTokens,
      to: adapter,
      value_base_units: redemption.payout_base_units
    },
    {
      token: collateralToken,
      from: adapter,
      to: pusdToken,
      value_base_units: redemption.payout_base_units
    },
    {
      token: pusdToken,
      from: zeroAddress,
      to: expectedWallet,
      value_base_units: redemption.payout_base_units
    }
  ]);
  const collateralWraps = redemptions.map((redemption) => ({
    contract_address: pusdToken,
    caller: adapter,
    asset: collateralToken,
    to: expectedWallet,
    amount_base_units: redemption.payout_base_units
  }));
  return {
    status: "success",
    chain_id: 137,
    block_number: "12345678",
    confirmations: 3,
    transaction_hash: automaticRedemption,
    redemptions,
    ctf_transfers: ctfTransfers,
    erc20_transfers: erc20Transfers,
    collateral_wraps: collateralWraps,
    ...overrides
  };
}

function verifyFixture(overrides = {}) {
  const reservation = automaticReservation();
  const activity = [tradeActivity(), redeemActivity()];
  return {
    manifest: manifest(),
    reservations: [reservation],
    redemption: activity[1],
    activity,
    orderFills: [automaticFill()],
    receipt: confirmedReceipt(),
    expectedWallet,
    ...overrides
  };
}

test("automatic settlement binds exact reservation, CLOB, Data API, wallet, and decoded receipt", async () => {
  const settlements = await discoverVerifiedAutomaticInternalSettlements({
    manifest: manifest(),
    reservations: [automaticReservation()],
    activity: [tradeActivity(), redeemActivity()],
    expectedWallet,
    getOrderFills: async (reservation) => {
      assert.equal(reservation.order_id, "order-funded-1");
      return [automaticFill()];
    },
    getTransactionReceipt: async (transactionHash) => {
      assert.equal(transactionHash, automaticRedemption);
      return confirmedReceipt();
    }
  });
  assert.equal(settlements.length, 1);
  assert.deepEqual(settlements[0], {
    id: settlementId(automaticRedemption, automaticCondition),
    type: "internal_automatic_settlement",
    session_id: "dynamic-quote-funded-test-v5",
    campaign_id: "dynamic-quote-funded-test-v5",
    run_ids: ["run-funded-1"],
    probe_ids: ["probe-funded-1"],
    order_ids: ["order-funded-1"],
    token_ids: [automaticToken],
    proxy_wallet: expectedWallet,
    transaction_hash: automaticRedemption,
    condition_id: automaticCondition,
    payout: 10,
    principal: 2,
    realized_pnl: 8,
    fill_transaction_hashes: [automaticFillTransaction],
    authenticated_clob_fill_ids: ["authenticated-clob-fill-1:order-funded-1"],
    reservation_matched_notional: 2,
    reservation_fee_risk_upper_bound: 0.1,
    evidence_source: "polymarket_data_api_plus_onchain_redemption",
    redemption_evidence_decoded: true,
    redemption_adapter_address: adapter,
    redemption_contract_address: conditionalTokens,
    redemption_collateral_token: collateralToken,
    redemption_parent_collection_id: parentCollection,
    redemption_index_sets: ["1"],
    redemption_payout_base_units: "10000000",
    redemption_transfer_chain_verified: true,
    redemption_token_id: automaticToken,
    receipt_block_number: "12345678",
    receipt_confirmations: 3,
    settled_at: "2026-07-30T00:10:00.000Z"
  });

  const container = new Container();
  await putVerifiedInternalSettlement(container, settlements[0]);
  await putVerifiedInternalSettlement(container, settlements[0]);
  const durable = await loadDurableInternalSettlements(
    container,
    "dynamic-quote-funded-test-v5"
  );
  assert.equal(durable.length, 1);
  assert.deepEqual(durable[0], {
    schema: "polyedge.verified_internal_settlement.v1",
    ...settlements[0]
  });
  const state = await reconcileProtectedCompoundingState({
    container,
    manifest: manifest(),
    accountEquity: 19.09862,
    fullyReconciled: true
  });
  assert.equal(state.verified_realized_pnl, 8);
  assert.equal(state.authorized_equity_ceiling, 19.09862);
  assert.equal(state.high_water_equity, 19.09862);
  assert.equal(state.protected_reserve, 5.729586);
});

test("automatic settlement accepts a flat maker row only with the exact reservation asset", () => {
  const fixture = verifyFixture();
  Object.assign(fixture.orderFills[0], {
    tradeAssetId: automaticToken,
    makerAssetId: null,
    nestedMakerOrderMatchCount: 0,
    directMakerOrder: true
  });
  const settlement = verifyAutomaticSettlementEvidence(fixture);
  assert.equal(settlement.condition_id, automaticCondition);
  assert.equal(settlement.token_ids[0], automaticToken);
});

test("automatic settlement accepts only explicit confirmed CLOB response enums", () => {
  for (const status of ["CONFIRMED", "TRADE_STATUS_CONFIRMED"]) {
    const settlement = verifyAutomaticSettlementEvidence(verifyFixture({
      orderFills: [automaticFill({ status })]
    }));
    assert.equal(settlement.condition_id, automaticCondition, status);
  }
});

test("automatic settlement accepts an exact optional Data API REDEEM asset", () => {
  const fixture = verifyFixture();
  fixture.redemption.asset = automaticToken;
  const settlement = verifyAutomaticSettlementEvidence(fixture);
  assert.equal(settlement.token_ids[0], automaticToken);
});

test("an existing manual redemption identity remains backward-compatible and idempotent", async () => {
  const calls = [];
  const settlements = await discoverVerifiedAutomaticInternalSettlements({
    manifest: manifest(),
    reservations: [automaticReservation()],
    activity: [tradeActivity(), redeemActivity()],
    expectedWallet,
    durableSettlements: [{
      schema: "polyedge.verified_internal_settlement.v1",
      id: "manual-redeem-2026-07-31-0109z",
      type: "internal_manual_settlement",
      session_id: "dynamic-quote-funded-test-v5",
      transaction_hash: automaticRedemption,
      condition_id: automaticCondition,
      payout: 10,
      principal: 2,
      realized_pnl: 8,
      fill_transaction_hashes: [automaticFillTransaction],
      evidence_source: "polymarket_data_api_fills_plus_polygon_receipt",
      receipt_block_number: "12345678",
      receipt_confirmations: 3
    }],
    getOrderFills: async () => { calls.push("fills"); return [automaticFill()]; },
    getTransactionReceipt: async () => { calls.push("receipt"); return confirmedReceipt(); }
  });
  assert.deepEqual(settlements, []);
  assert.deepEqual(calls, []);
});

test("multiple exact reservations and maker orders aggregate for one condition", async () => {
  const reservations = [
    automaticReservation({
      order_id: "order-funded-1",
      probe_id: "probe-funded-1",
      run_id: "run-funded-1",
      matched_notional: 0.8
    }),
    automaticReservation({
      order_id: "order-funded-2",
      probe_id: "probe-funded-2",
      run_id: "run-funded-2",
      matched_notional: 1.2
    })
  ];
  const fills = {
    "order-funded-1": automaticFill({
      id: "trade-shared",
      orderId: "order-funded-1",
      size: 4,
      price: 0.2
    }),
    "order-funded-2": automaticFill({
      id: "trade-shared",
      orderId: "order-funded-2",
      size: 6,
      price: 0.2
    })
  };
  const [settlement] = await discoverVerifiedAutomaticInternalSettlements({
    manifest: manifest(),
    reservations,
    activity: [
      tradeActivity({ size: 4, usdcSize: 0.8 }),
      tradeActivity({ size: 6, usdcSize: 1.2 }),
      redeemActivity()
    ],
    expectedWallet,
    getOrderFills: async (reservation) => [fills[reservation.order_id]],
    getTransactionReceipt: async () => confirmedReceipt()
  });
  assert.deepEqual(settlement.order_ids, ["order-funded-1", "order-funded-2"]);
  assert.deepEqual(settlement.authenticated_clob_fill_ids, [
    "trade-shared:order-funded-1",
    "trade-shared:order-funded-2"
  ]);
  assert.equal(settlement.principal, 2);
  assert.equal(settlement.payout, 10);
  assert.equal(settlement.reservation_matched_notional, 2);
});

test("one confirmed transaction redeeming multiple conditions creates separate records", async () => {
  const reservations = [
    automaticReservation(),
    automaticReservation({
      condition_id: secondCondition,
      token_id: secondToken,
      order_id: "order-funded-2",
      probe_id: "probe-funded-2",
      run_id: "run-funded-2",
      matched_notional: 2
    })
  ];
  const activity = [
    tradeActivity(),
    tradeActivity({
      conditionId: secondCondition,
      asset: secondToken,
      transactionHash: secondFillTransaction,
      size: 5,
      usdcSize: 2
    }),
    redeemActivity(),
    redeemActivity({ conditionId: secondCondition, payout: 5 })
  ];
  const receipt = confirmedReceipt([
    decodedRedemption(),
    decodedRedemption({ conditionId: secondCondition, payout: 5, index_sets: [2] })
  ]);
  let receiptCalls = 0;
  const settlements = await discoverVerifiedAutomaticInternalSettlements({
    manifest: manifest(),
    reservations,
    activity,
    expectedWallet,
    getOrderFills: async (reservation) => [reservation.condition_id === automaticCondition
      ? automaticFill()
      : automaticFill({
          id: "authenticated-clob-fill-2",
          orderId: "order-funded-2",
          conditionId: secondCondition,
          assetId: secondToken,
          transactionHash: secondFillTransaction,
          size: 5,
          price: 0.4
        })],
    getTransactionReceipt: async () => { receiptCalls += 1; return receipt; }
  });
  assert.equal(receiptCalls, 1);
  assert.equal(settlements.length, 2);
  assert.deepEqual(settlements.map((row) => row.condition_id).sort(), [
    secondCondition,
    automaticCondition
  ].sort());
  assert.equal(new Set(settlements.map((row) => row.id)).size, 2);
  assert.ok(settlements.every((row) => row.transaction_hash === automaticRedemption));
});

test("automatic settlement fails closed on CLOB hash, asset, wallet, status, or receipt mismatch", () => {
  const cases = [
    {
      name: "transaction hash",
      mutate: (fixture) => {
        fixture.orderFills[0].transactionHash = secondFillTransaction;
      },
      error: /Data API proxy wallet\/asset\/transaction/
    },
    {
      name: "asset",
      mutate: (fixture) => {
        fixture.orderFills[0].assetId = secondToken;
      },
      error: /CLOB trade hash\/asset\/market/
    },
    {
      name: "maker asset",
      mutate: (fixture) => {
        fixture.orderFills[0].makerAssetId = secondToken;
      },
      error: /CLOB trade hash\/asset\/market/
    },
    {
      name: "duplicate nested maker order",
      mutate: (fixture) => {
        fixture.orderFills[0].nestedMakerOrderMatchCount = 2;
      },
      error: /CLOB trade hash\/asset\/market/
    },
    {
      name: "flat direct maker with complementary top-level asset",
      mutate: (fixture) => {
        fixture.orderFills[0].makerAssetId = null;
        fixture.orderFills[0].nestedMakerOrderMatchCount = 0;
        fixture.orderFills[0].directMakerOrder = true;
      },
      error: /CLOB trade hash\/asset\/market/
    },
    {
      name: "missing top-level trade asset",
      mutate: (fixture) => {
        fixture.orderFills[0].tradeAssetId = "";
      },
      error: /CLOB trade hash\/asset\/market/
    },
    {
      name: "wallet",
      mutate: (fixture) => {
        fixture.activity[0].proxyWallet = `0x${"9".repeat(40)}`;
      },
      error: /Data API proxy wallet\/asset\/transaction/
    },
    {
      name: "redemption asset",
      mutate: (fixture) => {
        fixture.redemption.asset = secondToken;
      },
      error: /Data API redemption asset/
    },
    {
      name: "status",
      mutate: (fixture) => {
        fixture.orderFills[0].status = "MATCHED";
      },
      error: /CLOB trade hash\/asset\/market/
    },
    {
      name: "decoded redemption transaction",
      mutate: (fixture) => {
        fixture.receipt.transaction_hash = secondFillTransaction;
      },
      error: /decoded confirmed redemption/
    },
    {
      name: "decoded redemption adapter",
      mutate: (fixture) => {
        fixture.receipt.redemptions[0].redeemer = `0x${"9".repeat(40)}`;
      },
      error: /decoded confirmed redemption/
    },
    {
      name: "decoded redemption payout",
      mutate: (fixture) => {
        fixture.receipt.redemptions[0].payout = 9;
      },
      error: /decoded confirmed redemption/
    },
    {
      name: "wrong but self-consistent adapter",
      mutate: (fixture) => {
        const wrongAdapter = `0x${"9".repeat(40)}`;
        fixture.receipt.redemptions[0].redeemer = wrongAdapter;
        for (const row of fixture.receipt.ctf_transfers) {
          for (const field of ["operator", "from", "to"]) {
            if (row[field] === adapter) row[field] = wrongAdapter;
          }
        }
        for (const row of fixture.receipt.erc20_transfers) {
          for (const field of ["from", "to"]) {
            if (row[field] === adapter) row[field] = wrongAdapter;
          }
        }
        fixture.receipt.collateral_wraps[0].caller = wrongAdapter;
      },
      error: /decoded confirmed redemption/
    },
    {
      name: "decoded redemption transaction hash",
      mutate: (fixture) => {
        fixture.receipt.redemptions[0].transaction_hash = secondFillTransaction;
      },
      error: /decoded confirmed redemption/
    },
    {
      name: "decoded redemption contract",
      mutate: (fixture) => {
        fixture.receipt.redemptions[0].contract_address = `0x${"9".repeat(40)}`;
      },
      error: /decoded confirmed redemption/
    },
    {
      name: "decoded redemption collateral",
      mutate: (fixture) => {
        fixture.receipt.redemptions[0].collateral_token = pusdToken;
      },
      error: /decoded confirmed redemption/
    },
    {
      name: "decoded redemption parent",
      mutate: (fixture) => {
        fixture.receipt.redemptions[0].parent_collection_id = automaticCondition;
      },
      error: /decoded confirmed redemption/
    },
    {
      name: "wallet to adapter CTF transfer",
      mutate: (fixture) => {
        fixture.receipt.ctf_transfers[0].from = adapter;
      },
      error: /adapter\/CTF\/USDC.e\/pUSD/
    },
    {
      name: "ambiguous duplicate token in wallet to adapter CTF transfer",
      mutate: (fixture) => {
        fixture.receipt.ctf_transfers[0].ids.push(automaticToken);
        fixture.receipt.ctf_transfers[0].values.push("1");
      },
      error: /adapter\/CTF\/USDC.e\/pUSD/
    },
    {
      name: "adapter CTF burn",
      mutate: (fixture) => {
        fixture.receipt.ctf_transfers[1].to = expectedWallet;
      },
      error: /adapter\/CTF\/USDC.e\/pUSD/
    },
    {
      name: "USDC.e CTF to adapter",
      mutate: (fixture) => {
        fixture.receipt.erc20_transfers[0].from = expectedWallet;
      },
      error: /adapter\/CTF\/USDC.e\/pUSD/
    },
    {
      name: "USDC.e adapter to pUSD",
      mutate: (fixture) => {
        fixture.receipt.erc20_transfers[1].to = expectedWallet;
      },
      error: /adapter\/CTF\/USDC.e\/pUSD/
    },
    {
      name: "pUSD mint to wallet",
      mutate: (fixture) => {
        fixture.receipt.erc20_transfers[2].from = adapter;
      },
      error: /adapter\/CTF\/USDC.e\/pUSD/
    },
    {
      name: "pUSD wrapped to wallet",
      mutate: (fixture) => {
        fixture.receipt.collateral_wraps[0].caller = expectedWallet;
      },
      error: /adapter\/CTF\/USDC.e\/pUSD/
    },
    {
      name: "decoded redemption overflowing index set",
      mutate: (fixture) => {
        fixture.receipt.redemptions[0].index_sets = [(1n << 256n).toString()];
      },
      error: /decoded confirmed redemption/
    }
  ];
  for (const { name, mutate, error } of cases) {
    const fixture = verifyFixture();
    mutate(fixture);
    assert.throws(
      () => verifyAutomaticSettlementEvidence(fixture),
      error,
      name
    );
  }
});
