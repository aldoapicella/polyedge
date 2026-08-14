import test from "node:test";
import assert from "node:assert/strict";
import { Readable } from "node:stream";
import { encodeAbiParameters, encodeEventTopics } from "viem";
import {
  beginFillMarkoutCapture,
  artifactLocationFromUri,
  canonicalBookHash,
  consumeOneShotAuthorization,
  deterministicNoOrderRejection,
  executeStrategyCanary,
  assertFundedSignalToSendDeadline,
  loadHashedJson,
  sha256,
  validateDeterministicNoOrderReconciliation
} from "../src/canary-lib.mjs";
import {
  putOperatorSessionManifest,
  requireExecutionModelArtifact,
  assertPersistentIntentRemainingTtl,
  decodePayoutRedemptions,
  decodeSettlementReceiptEvidence,
  fundedCapitalSnapshotRecord,
  initializeProtectedCompounding,
  loadAccountPositions,
  loadSettlementActivity,
  PREFLIGHT_COMPONENT_TIMEOUT_MS,
  refreshCampaignRiskSnapshot,
  runBoundedPreflightComponent,
  SAFETY_CACHE_MAINTENANCE_QUIESCE_MS,
  selectFreshCachedSafetySnapshot,
  startSafetySnapshotCache,
  streamBookEvidence,
  createAndPostFundedOrderWithinSignalToSendDeadline,
  waitForSafetySnapshotIdle
} from "../src/canary.mjs";
import { automaticSettlementReceiptEvidence } from "../src/redeem.mjs";

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
const payoutRedemptionEvent = [{
  type: "event",
  name: "PayoutRedemption",
  anonymous: false,
  inputs: [
    { name: "redeemer", type: "address", indexed: true },
    { name: "collateralToken", type: "address", indexed: true },
    { name: "parentCollectionId", type: "bytes32", indexed: true },
    { name: "conditionId", type: "bytes32", indexed: false },
    { name: "indexSets", type: "uint256[]", indexed: false },
    { name: "payout", type: "uint256", indexed: false }
  ]
}];
const erc20TransferEvent = [{
  type: "event",
  name: "Transfer",
  anonymous: false,
  inputs: [
    { name: "from", type: "address", indexed: true },
    { name: "to", type: "address", indexed: true },
    { name: "value", type: "uint256", indexed: false }
  ]
}];
const erc1155TransferSingleEvent = [{
  type: "event",
  name: "TransferSingle",
  anonymous: false,
  inputs: [
    { name: "operator", type: "address", indexed: true },
    { name: "from", type: "address", indexed: true },
    { name: "to", type: "address", indexed: true },
    { name: "id", type: "uint256", indexed: false },
    { name: "value", type: "uint256", indexed: false }
  ]
}];
const erc1155TransferBatchEvent = [{
  type: "event",
  name: "TransferBatch",
  anonymous: false,
  inputs: [
    { name: "operator", type: "address", indexed: true },
    { name: "from", type: "address", indexed: true },
    { name: "to", type: "address", indexed: true },
    { name: "ids", type: "uint256[]", indexed: false },
    { name: "values", type: "uint256[]", indexed: false }
  ]
}];
const collateralWrappedEvent = [{
  type: "event",
  name: "Wrapped",
  anonymous: false,
  inputs: [
    { name: "caller", type: "address", indexed: true },
    { name: "asset", type: "address", indexed: true },
    { name: "to", type: "address", indexed: true },
    { name: "amount", type: "uint256", indexed: false }
  ]
}];

async function flushMicrotasks(rounds = 12) {
  for (let index = 0; index < rounds; index += 1) await Promise.resolve();
}

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

test("persistent executor rejects missing execution-model provenance before execution", () => {
  assert.deepEqual(
    requireExecutionModelArtifact({
      account: "storage",
      container: "polyedge-research",
      blobName: "reports/research/venue-probe/conservative_execution_prior_v1.json"
    }),
    {
      account: "storage",
      container: "polyedge-research",
      blobName: "reports/research/venue-probe/conservative_execution_prior_v1.json"
    }
  );
  for (const value of [
    null,
    {},
    { container: "polyedge-research", blobName: "../model.json" },
    { container: "INVALID", blobName: "model.json" }
  ]) {
    assert.throws(
      () => requireExecutionModelArtifact(value),
      /execution-model artifact provenance is unavailable/
    );
  }
});

test("settlement activity is condition-scoped, session-bounded, and fully paginated", async () => {
  const wallet = "0x1111111111111111111111111111111111111111";
  const firstCondition = `0x${"a".repeat(64)}`;
  const secondCondition = `0x${"b".repeat(64)}`;
  const startedAt = "2026-07-30T00:00:00.000Z";
  const calls = [];
  const row = (conditionId, transaction, timestamp, type = "TRADE") => ({
    type,
    conditionId,
    transactionHash: `0x${transaction.repeat(64)}`,
    proxyWallet: wallet,
    asset: "123",
    size: 1,
    usdcSize: 0.5,
    timestamp
  });
  const fetcher = async (rawUrl) => {
    const url = new URL(rawUrl);
    calls.push(url);
    assert.equal(url.searchParams.get("user"), wallet);
    assert.equal(url.searchParams.get("start"), String(Date.parse(startedAt) / 1_000));
    assert.equal(url.searchParams.get("sortBy"), "TIMESTAMP");
    assert.equal(url.searchParams.get("sortDirection"), "ASC");
    assert.equal(url.searchParams.get("type"), "TRADE,REDEEM");
    assert.equal(url.searchParams.get("market"), `${firstCondition},${secondCondition}`);
    const offset = Number(url.searchParams.get("offset"));
    if (offset === 0) {
      return [
        row(firstCondition, "1", Date.parse("2026-07-30T00:01:00.000Z") / 1_000),
        row(firstCondition, "2", Date.parse("2026-07-30T00:02:00.000Z") / 1_000, "REDEEM")
      ];
    }
    if (offset === 2) {
      return [
        row(firstCondition, "3", Date.parse("2026-07-30T00:03:00.000Z") / 1_000),
        row(secondCondition, "4", Date.parse("2026-07-30T00:04:00.000Z") / 1_000)
      ];
    }
    return [];
  };

  const activity = await loadSettlementActivity({
    user: wallet,
    conditionIds: [secondCondition, firstCondition, firstCondition],
    sessionStartedAt: startedAt,
    fetcher,
    pageSize: 2
  });

  assert.equal(calls.length, 3);
  assert.deepEqual(activity.map((value) => value.transactionHash), [
    `0x${"1".repeat(64)}`,
    `0x${"2".repeat(64)}`,
    `0x${"3".repeat(64)}`,
    `0x${"4".repeat(64)}`
  ]);
});

test("settlement activity fails closed instead of truncating a full final page", async () => {
  await assert.rejects(
    loadSettlementActivity({
      user: "0x1111111111111111111111111111111111111111",
      conditionIds: [`0x${"a".repeat(64)}`],
      sessionStartedAt: "2026-07-30T00:00:00.000Z",
      pageSize: 1,
      maxPagesPerBatch: 2,
      fetcher: async () => [{
        type: "TRADE",
        conditionId: `0x${"a".repeat(64)}`,
        transactionHash: `0x${crypto.randomUUID().replaceAll("-", "").padEnd(64, "0")}`,
        proxyWallet: "0x1111111111111111111111111111111111111111",
        asset: "123",
        size: 1,
        usdcSize: 0.5,
        timestamp: Date.parse("2026-07-30T00:01:00.000Z") / 1_000
      }]
    }),
    /exceeds pagination bound/
  );
});

test("protected-reserve startup paginates every account position beyond 500 rows", async () => {
  const offsets = [];
  const position = (asset) => ({ asset, size: "0", currentValue: "0" });
  const positions = await loadAccountPositions({
    user: "0x1111111111111111111111111111111111111111",
    fetcher: async (value) => {
      const offset = Number(new URL(value).searchParams.get("offset"));
      offsets.push(offset);
      if (offset === 0) {
        return Array.from({ length: 500 }, (_, index) => position(String(index)));
      }
      return [position("500")];
    }
  });
  assert.deepEqual(offsets, [0, 500]);
  assert.equal(positions.length, 501);
  assert.equal(positions.every((row) => row.size === 0 && row.currentValue === 0), true);
});

test("protected-reserve startup fails closed at the positions API offset ceiling", async () => {
  const offsets = [];
  await assert.rejects(loadAccountPositions({
    user: "0x1111111111111111111111111111111111111111",
    fetcher: async (value) => {
      offsets.push(Number(new URL(value).searchParams.get("offset")));
      return Array.from({ length: 500 }, (_, index) => ({
        asset: String(index),
        size: 0,
        currentValue: 0
      }));
    }
  }), /exceeds API offset bound/);
  assert.equal(offsets.length, 21);
  assert.equal(offsets.at(-1), 10_000);
});

test("protected-reserve startup rejects malformed or negative position amounts", async () => {
  for (const [field, value] of [
    ["size", undefined],
    ["size", ""],
    ["size", "not-a-number"],
    ["size", -1],
    ["currentValue", null],
    ["currentValue", ""],
    ["currentValue", "not-a-number"],
    ["currentValue", -1]
  ]) {
    await assert.rejects(loadAccountPositions({
      user: "0x1111111111111111111111111111111111111111",
      fetcher: async () => [{
        size: 0,
        currentValue: 0,
        [field]: value
      }]
    }), new RegExp(`account position ${field} is invalid`));
  }
});

test("protected-compounding startup skips settlement activity for an empty manifest ledger", async () => {
  let activityCalls = 0;
  const manifest = {
    allow_compounding: true,
    session_id: "dynamic-quote-funded-empty-settlements",
    internal_settlements: [],
    capital_policy: {
      reserve_monotonic: true,
      high_water_update: "full_reconciliation_only",
      reserve_ratio: 0.3,
      operating_buffer_ratio: 0.01,
      minimum_order_notional: 1,
      state_blob_name:
        "reports/funded/dynamic-quote/sessions/dynamic-quote-funded-empty-settlements/capital-reserve-state.json"
    }
  };
  const container = {
    async *listBlobsFlat() {}
  };
  const result = await initializeProtectedCompounding({
    container,
    manifest,
    loadActivity: async () => {
      activityCalls += 1;
      throw new Error("network must not be queried");
    }
  });
  assert.equal(activityCalls, 0);
  assert.deepEqual(result.verifiedConfiguredSettlements, []);
});

test("protected-compounding successor imports exact durable predecessor settlement without historical API scan", async () => {
  const predecessorSessionId = "dynamic-quote-funded-test-v5";
  const sessionId = "dynamic-quote-funded-test-v7";
  const settlement = {
    schema: "polyedge.verified_internal_settlement.v1",
    session_id: predecessorSessionId,
    id: "manual-redeem-1",
    type: "internal_manual_settlement",
    transaction_hash: `0x${"a".repeat(64)}`,
    condition_id: `0x${"b".repeat(64)}`,
    payout: 17.015,
    principal: 10.209,
    realized_pnl: 6.806,
    fill_transaction_hashes: [`0x${"c".repeat(64)}`],
    evidence_source: "polymarket_data_api_fills_plus_polygon_receipt",
    receipt_block_number: "123",
    receipt_confirmations: 2
  };
  const predecessorBlob =
    `reports/funded/dynamic-quote/sessions/${predecessorSessionId}/internal-settlements/manual.json`;
  const values = new Map([[predecessorBlob, Buffer.from(JSON.stringify(settlement))]]);
  const container = {
    async *listBlobsFlat({ prefix }) {
      for (const name of values.keys()) {
        if (name.startsWith(prefix)) yield { name };
      }
    },
    getBlobClient: (name) => ({
      download: async () => {
        if (!values.has(name)) throw Object.assign(new Error("missing"), { statusCode: 404 });
        return { readableStreamBody: Readable.from([values.get(name)]) };
      }
    }),
    getBlockBlobClient: (name) => ({
      uploadData: async (bytes, options) => {
        assert.equal(options.conditions.ifNoneMatch, "*");
        if (values.has(name)) throw Object.assign(new Error("exists"), { statusCode: 412 });
        values.set(name, Buffer.from(bytes));
      }
    })
  };
  const manifest = {
    schema_version: "polyedge.operator_funded_session.v3",
    session_id: sessionId,
    starting_collateral: 11.09862,
    allow_compounding: true,
    continue_after_loss: true,
    internal_settlements: [{
      id: settlement.id,
      type: settlement.type,
      transaction_hash: settlement.transaction_hash,
      condition_id: settlement.condition_id,
      payout: settlement.payout,
      principal: settlement.principal,
      realized_pnl: settlement.realized_pnl,
      fill_transaction_hashes: settlement.fill_transaction_hashes
    }],
    capital_policy: {
      reserve_ratio: 0.3,
      operating_buffer_ratio: 0.01,
      minimum_order_notional: 1,
      reserve_basis: "fully_reconciled_current_equity",
      loss_response: "resize_from_fully_reconciled_current_equity",
      prior_state_session_id: predecessorSessionId,
      prior_state_blob_name:
        `reports/funded/dynamic-quote/sessions/${predecessorSessionId}/capital-reserve-state.json`,
      minimum_historical_high_water_equity: 41.34362,
      high_water_update: "full_reconciliation_only",
      reserve_monotonic: false,
      state_blob_name:
        `reports/funded/dynamic-quote/sessions/${sessionId}/capital-reserve-state.json`
    }
  };
  let activityCalls = 0;
  await assert.rejects(initializeProtectedCompounding({
    container,
    manifest,
    readOnly: true,
    loadActivity: async () => {
      activityCalls += 1;
      return [];
    }
  }), /read-only reconciliation cannot persist a verified settlement/);
  assert.equal([...values.keys()].filter((name) =>
    name.includes(`/sessions/${sessionId}/internal-settlements/`)).length, 0);

  const result = await initializeProtectedCompounding({
    container,
    manifest,
    loadActivity: async () => {
      activityCalls += 1;
      return [];
    }
  });
  assert.equal(activityCalls, 0);
  assert.equal(result.verifiedConfiguredSettlements.length, 1);
  assert.equal(result.verifiedConfiguredSettlements[0].session_id, sessionId);
  assert.equal([...values.keys()].filter((name) =>
    name.includes(`/sessions/${sessionId}/internal-settlements/`)).length, 1);
});

test("Polygon receipt decoder preserves the exact CTF-to-pUSD adapter chain", () => {
  const transactionHash = `0x${"c".repeat(64)}`;
  const wallet = "0x1111111111111111111111111111111111111111";
  const adapter = "0x2222222222222222222222222222222222222222";
  const conditionalTokens = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045";
  const usdce = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174";
  const pusd = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB";
  const zero = "0x0000000000000000000000000000000000000000";
  const parentCollectionId = `0x${"0".repeat(64)}`;
  const conditionId = `0x${"d".repeat(64)}`;
  const tokenId = 123456789n;
  const amount = 22_180_000n;
  const log = (address, abi, eventName, indexedArgs, dataParameters, dataValues) => ({
    address,
    topics: encodeEventTopics({ abi, eventName, args: indexedArgs }),
    data: encodeAbiParameters(dataParameters, dataValues)
  });
  const receipt = {
    transactionHash,
    status: "success",
    blockNumber: 77n,
    logs: [{
      ...log(
        conditionalTokens,
        erc1155TransferBatchEvent,
        "TransferBatch",
        { operator: adapter, from: wallet, to: adapter },
        [{ type: "uint256[]" }, { type: "uint256[]" }],
        [[999n, tokenId], [0n, amount]]
      )
    }, {
      ...log(
        conditionalTokens,
        erc1155TransferSingleEvent,
        "TransferSingle",
        { operator: adapter, from: adapter, to: zero },
        [{ type: "uint256" }, { type: "uint256" }],
        [tokenId, amount]
      )
    }, {
      ...log(
        usdce,
        erc20TransferEvent,
        "Transfer",
        { from: conditionalTokens, to: adapter },
        [{ type: "uint256" }],
        [amount]
      )
    }, {
      ...log(
        conditionalTokens,
        payoutRedemptionEvent,
        "PayoutRedemption",
        { redeemer: adapter, collateralToken: usdce, parentCollectionId },
        [{ type: "bytes32" }, { type: "uint256[]" }, { type: "uint256" }],
        [conditionId, [1n, 2n], amount]
      )
    }, {
      ...log(
        usdce,
        erc20TransferEvent,
        "Transfer",
        { from: adapter, to: pusd },
        [{ type: "uint256" }],
        [amount]
      )
    }, {
      ...log(
        pusd,
        erc20TransferEvent,
        "Transfer",
        { from: zero, to: wallet },
        [{ type: "uint256" }],
        [amount]
      )
    }, {
      ...log(
        pusd,
        collateralWrappedEvent,
        "Wrapped",
        { caller: adapter, asset: usdce, to: wallet },
        [{ type: "uint256" }],
        [amount]
      )
    }]
  };

  const decoded = decodeSettlementReceiptEvidence(receipt, transactionHash);
  assert.deepEqual(decoded.redemptions, [{
    contract_address: conditionalTokens.toLowerCase(),
    transaction_hash: transactionHash,
    redeemer: adapter,
    collateral_token: usdce.toLowerCase(),
    parent_collection_id: parentCollectionId,
    condition_id: conditionId,
    index_sets: ["1", "2"],
    payout_base_units: String(amount),
    payout: 22.18
  }]);
  assert.deepEqual(decoded.ctf_transfers, [{
    event: "TransferBatch",
    contract_address: conditionalTokens.toLowerCase(),
    operator: adapter,
    from: wallet,
    to: adapter,
    ids: ["999", String(tokenId)],
    values: ["0", String(amount)]
  }, {
    event: "TransferSingle",
    contract_address: conditionalTokens.toLowerCase(),
    operator: adapter,
    from: adapter,
    to: zero,
    ids: [String(tokenId)],
    values: [String(amount)]
  }]);
  assert.deepEqual(decoded.erc20_transfers, [{
    token: usdce.toLowerCase(),
    from: conditionalTokens.toLowerCase(),
    to: adapter,
    value_base_units: String(amount)
  }, {
    token: usdce.toLowerCase(),
    from: adapter,
    to: pusd.toLowerCase(),
    value_base_units: String(amount)
  }, {
    token: pusd.toLowerCase(),
    from: zero,
    to: wallet,
    value_base_units: String(amount)
  }]);
  assert.deepEqual(decoded.collateral_wraps, [{
    contract_address: pusd.toLowerCase(),
    caller: adapter,
    asset: usdce.toLowerCase(),
    to: wallet,
    amount_base_units: String(amount)
  }]);
  assert.deepEqual(decodePayoutRedemptions(receipt, transactionHash), decoded.redemptions);
  assert.deepEqual(automaticSettlementReceiptEvidence(receipt, transactionHash), {
    status: "success",
    chain_id: 137,
    transaction_hash: transactionHash,
    block_number: "77",
    confirmations: 2,
    ...decoded
  });
});

test("persistent executor selects only a fresh exact-market safety snapshot", () => {
  const runtime = {
    capturedCompletedWallMs: 10_000,
    risk: {
      passed: true,
      account_equity: 5,
      historical_high_water_equity: 102.78112,
      protected_reserve: 2,
      operating_buffer: 0.05,
      operable_capital: 2.95,
      reserve_basis: "fully_reconciled_current_equity",
      continue_after_loss: true,
      proposed_notional: 1.7,
      order_notional: 1.65,
      blockers: [],
      open_order_count: 0,
      unresolved_position_count: 0,
      unresolved_risk_reservation_count: 0
    },
    exactResolutionSource: false,
    resolutionSource: null
  };
  const resources = {
    safetyCache: {
      latest: {
        market_id: "market-1",
        condition_id: "condition-1",
        token_id: "token-up",
        runtime
      }
    }
  };
  const intent = {
    market_id: "market-1",
    condition_id: "condition-1",
    token_id: "token-up",
    decision_id: "decision-1",
    exact_resolution_source: true,
    resolution_source: "chainlink_reference"
  };
  const selected = selectFreshCachedSafetySnapshot(resources, intent, 10_650);
  assert.deepEqual(selected, {
    ...runtime,
    exactResolutionSource: true,
    resolutionSource: "chainlink_reference"
  });
  assert.deepEqual(fundedCapitalSnapshotRecord(
    selected,
    intent,
    { session_id: "session-v8" },
    { source: "persistent_safety_cache", nowMs: 10_650 }
  ), {
    schema: "polyedge.funded_capital_snapshot.v1",
    session_id: "session-v8",
    decision_id: "decision-1",
    snapshot_source: "persistent_safety_cache",
    snapshot_completed_wall_ms: 10_000,
    snapshot_age_ms: 650,
    account_equity: 5,
    historical_high_water_equity: 102.78112,
    protected_reserve: 2,
    operating_buffer: 0.05,
    operable_capital: 2.95,
    reserve_basis: "fully_reconciled_current_equity",
    continue_after_loss: true,
    proposed_notional: 1.7,
    order_notional: 1.65,
    risk_passed: true,
    blockers: [],
    open_order_count: 0,
    unresolved_position_count: 0,
    unresolved_risk_reservation_count: 0
  });
  assert.equal(selectFreshCachedSafetySnapshot(resources, intent, 10_651), null);
  assert.equal(
    selectFreshCachedSafetySnapshot(resources, { ...intent, token_id: "token-down" }, 10_600),
    null
  );
});

test("campaign risk snapshot avoids duplicate list loaders and does not cross runs", async () => {
  const calls = { control: 0, reservations: 0 };
  const campaignConfig = { campaignId: "campaign-a" };
  const container = {};
  const loadControl = async (_config, options) => {
    calls.control += 1;
    assert.equal(options.container, container);
    return { campaign_id: "campaign-a", revision: calls.control };
  };
  const loadUnresolved = async (_config, options) => {
    calls.reservations += 1;
    assert.equal(options.container, container);
    return [{ reservation: { probe_id: `probe-${calls.reservations}` } }];
  };
  const firstRun = { container, lease: { assertHealthy() {} } };

  const first = await refreshCampaignRiskSnapshot(firstRun, {
    campaignConfig,
    loadControl,
    loadUnresolved
  });
  for (let preflight = 0; preflight < 3; preflight += 1) {
    assert.equal(firstRun.campaignRiskSnapshot.control.revision, 1);
    assert.equal(firstRun.campaignRiskSnapshot.reservationRecords[0].reservation.probe_id, "probe-1");
  }
  assert.deepEqual(calls, { control: 1, reservations: 1 });

  const secondRun = { container, lease: { assertHealthy() {} } };
  const second = await refreshCampaignRiskSnapshot(secondRun, {
    campaignConfig,
    loadControl,
    loadUnresolved
  });
  assert.notStrictEqual(second, first);
  assert.equal(second.control.revision, 2);
  assert.deepEqual(calls, { control: 2, reservations: 2 });
});

test("persistent executor honors the configured child TTL gate", () => {
  const nowMs = Date.parse("2026-07-30T12:00:00.000Z");
  assert.equal(
    assertPersistentIntentRemainingTtl(
      { valid_until: new Date(nowMs + 2_000).toISOString() },
      2_000,
      nowMs
    ),
    2_000
  );
  assert.throws(
    () => assertPersistentIntentRemainingTtl(
      { valid_until: new Date(nowMs + 1_999).toISOString() },
      2_000,
      nowMs
    ),
    /less than 2000ms/
  );
});

test("funded executor fails closed at the reviewed signal-to-send deadline", () => {
  const decisionMs = Date.parse("2026-07-30T12:00:00.000Z");
  const intent = {
    decision_ts: new Date(decisionMs).toISOString(),
    valid_until: new Date(decisionMs + 10_000).toISOString(),
    ttl_ms: 10_000
  };
  assert.deepEqual(assertFundedSignalToSendDeadline(intent, 7_000, 2_000, decisionMs + 7_000), {
    elapsedMs: 7_000,
    remainingTtlMs: 3_000
  });
  assert.throws(
    () => assertFundedSignalToSendDeadline(intent, 7_000, 2_000, decisionMs + 7_001),
    /signal-to-send latency exceeded 7000ms \(7001ms\)/
  );
  assert.throws(
    () => assertFundedSignalToSendDeadline({
      decision_ts: new Date(decisionMs).toISOString(),
      valid_until: new Date(decisionMs + 8_999).toISOString()
    }, 7_000, 2_000, decisionMs + 7_000),
    /less than 2000ms TTL at transport \(1999ms\)/
  );
});

test("async order construction cannot cross the deadline into venue transport", async () => {
  const decisionMs = Date.parse("2026-07-30T12:00:00.000Z");
  const reservation = { probe_id: "funded-direct-decision" };
  let clockMs = decisionMs + 7_000;
  let venueCalls = 0;
  const finalized = [];
  const client = {
    host: "https://clob.example",
    retryOnError: false,
    post: async () => { venueCalls += 1; },
    createOrder: async () => { clockMs += 1; return { signed: true }; },
    async postOrder() {
      return this.post(`${this.host}/order`, { data: {} }, true);
    }
  };
  await assert.rejects(createAndPostFundedOrderWithinSignalToSendDeadline({
    client,
    intent: {
      decision_ts: new Date(decisionMs).toISOString(),
      valid_until: new Date(decisionMs + 10_000).toISOString(),
      ttl_ms: 10_000
    },
    sloMs: 7_000,
    minimumRemainingTtlMs: 2_000,
    reservation,
    userOrder: {},
    orderOptions: {},
    nowMs: () => clockMs,
    finalizeNoOrder: async (value, finalization) => finalized.push({
      reservation: value,
      ...finalization
    }),
    onTransportStarted: () => assert.fail("late order must not start transport")
  }), /signal-to-send latency exceeded 7000ms/);
  assert.equal(venueCalls, 0);
  assert.deepEqual(finalized, [{
    reservation,
    state: "released_no_order",
    order_submitted: false,
    matched_notional: 0,
    reconciliation_complete: true,
    zero_open_orders_confirmed: true
  }]);
  assert.equal(client.post.name, "post");
});

test("a failure after the first order transport remains ambiguous and reserved", async () => {
  const decisionMs = Date.parse("2026-07-30T12:00:00.000Z");
  let transportStarts = 0;
  let finalized = 0;
  const originalPost = async () => { throw new Error("network acknowledgement lost"); };
  const client = {
    host: "https://clob.example",
    retryOnError: false,
    post: originalPost,
    createOrder: async () => ({ signed: true }),
    async postOrder() { return this.post(`${this.host}/order`, { data: {} }, true); }
  };
  await assert.rejects(createAndPostFundedOrderWithinSignalToSendDeadline({
    client,
    intent: {
      decision_ts: new Date(decisionMs).toISOString(),
      valid_until: new Date(decisionMs + 10_000).toISOString()
    },
    sloMs: 7_000,
    minimumRemainingTtlMs: 2_000,
    reservation: { probe_id: "funded-direct-decision" },
    userOrder: {},
    orderOptions: {},
    nowMs: () => decisionMs + 6_000,
    finalizeNoOrder: async () => { finalized += 1; },
    onTransportStarted: () => { transportStarts += 1; }
  }), /network acknowledgement lost/);
  assert.equal(transportStarts, 1);
  assert.equal(finalized, 0);
  assert.equal(client.post, originalPost);
});

test("funded order transport rejects SDK endpoint drift before any venue call", async () => {
  const decisionMs = Date.parse("2026-07-30T12:00:00.000Z");
  let venueCalls = 0;
  let finalized = 0;
  const client = {
    host: "https://clob.example",
    retryOnError: false,
    post: async () => { venueCalls += 1; },
    createOrder: async () => ({ signed: true }),
    async postOrder() { return this.post(`${this.host}/orders`, { data: {} }, true); }
  };
  await assert.rejects(createAndPostFundedOrderWithinSignalToSendDeadline({
    client,
    intent: {
      decision_ts: new Date(decisionMs).toISOString(),
      valid_until: new Date(decisionMs + 10_000).toISOString()
    },
    sloMs: 7_000,
    minimumRemainingTtlMs: 2_000,
    reservation: { probe_id: "funded-direct-decision" },
    userOrder: {},
    orderOptions: {},
    nowMs: () => decisionMs + 6_000,
    finalizeNoOrder: async () => { finalized += 1; },
    onTransportStarted: () => assert.fail("drifted endpoint must not start transport")
  }), /not one exact \/order request/);
  assert.equal(venueCalls, 0);
  assert.equal(finalized, 1);
});

test("safety cache permits only one pending preflight across warmup generations", async () => {
  const cache = {
    generation: 0,
    timer: null,
    inFlight: 0,
    latest: null,
    lastError: null,
    market_id: null,
    condition_id: null,
    token_id: null
  };
  const resources = {
    busy: false,
    client: {},
    manifestDocument: { value: {} },
    profitQuarantineSnapshot: null,
    readOnly: true,
    campaignRiskSnapshot: {
      control: { campaign_id: "campaign-a" },
      reservationRecords: []
    },
    safetyCache: cache
  };
  const captures = [];
  const timers = [];
  const capture = (_client, intent, _manifest, _ignoredReservationId, options) => new Promise((resolve) => {
    assert.strictEqual(options.preflightResources, resources);
    assert.equal(options.readOnly, true);
    captures.push({ market_id: intent.market_id, resolve });
  });
  const setIntervalFn = (callback) => {
    const timer = { callback, cleared: false, unref() {} };
    timers.push(timer);
    return timer;
  };
  const clearIntervalFn = (timer) => { timer.cleared = true; };
  const options = {
    capture,
    createIntent: (market) => ({ market_id: market.market_id }),
    setIntervalFn,
    clearIntervalFn
  };
  const marketA = { market_id: "market-a", condition_id: "condition-a", token_id: "token-a" };
  const marketB = { market_id: "market-b", condition_id: "condition-b", token_id: "token-b" };

  startSafetySnapshotCache(resources, marketA, options);
  timers[0].callback();
  timers[0].callback();
  await flushMicrotasks();
  assert.equal(captures.length, 1);
  assert.equal(cache.inFlight, 1);

  startSafetySnapshotCache(resources, marketB, options);
  await flushMicrotasks();
  assert.equal(timers[0].cleared, true);
  assert.equal(captures.length, 1, "a market change must not reset the global in-flight budget");
  assert.equal(cache.inFlight, 1);

  captures[0].resolve({ capturedCompletedWallMs: 1 });
  await flushMicrotasks();
  assert.equal(cache.inFlight, 0);
  timers[1].callback();
  await flushMicrotasks();
  assert.equal(captures.length, 2);
  assert.equal(captures.at(-1).market_id, "market-b");
  assert.equal(cache.inFlight, 1);

  for (const captureEntry of captures) captureEntry.resolve({ capturedCompletedWallMs: 2 });
  await flushMicrotasks();
  assert.equal(cache.inFlight, 0);
});

test("every independent preflight component has a hard latency bound", async () => {
  assert.equal(PREFLIGHT_COMPONENT_TIMEOUT_MS, 2_000);
  await assert.rejects(
    runBoundedPreflightComponent("open_orders", () => new Promise(() => {}), 20),
    /open_orders preflight timed out after 20ms/
  );
  assert.equal(
    await runBoundedPreflightComponent("open_orders", async () => "ok", 20),
    "ok"
  );
});

test("funded maintenance gives bounded preflights room to quiesce", async () => {
  assert.equal(SAFETY_CACHE_MAINTENANCE_QUIESCE_MS, 30_000);
  const resources = { safetyCache: { inFlight: 1 } };
  const completion = setTimeout(() => {
    resources.safetyCache.inFlight = 0;
  }, 40);
  try {
    await waitForSafetySnapshotIdle(resources, 250);
  } finally {
    clearTimeout(completion);
  }
  assert.equal(resources.safetyCache.inFlight, 0);
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

  const quarantined = structuredClone(input);
  quarantined.documents.manifest.session_id = "dynamic-quote-funded-test-v6";
  quarantined.documents.manifest.profit_quarantine = {
    enabled: true,
    mode: "verified_internal_profit_quarantine",
    risk_headroom: "starting_collateral_only",
    settlement_ledger_prefix:
      "reports/funded/dynamic-quote/sessions/dynamic-quote-funded-test-v6/verified-internal-profits"
  };
  quarantined.documents.manifest.verified_internal_settlements = [{
    id: "manual-redeem-1",
    type: "internal_manual_settlement",
    transaction_hash: `0x${"a".repeat(64)}`,
    condition_id: `0x${"b".repeat(64)}`,
    payout: 17.015,
    principal: 10.209,
    realized_pnl: 6.806,
    fill_transaction_hashes: [`0x${"c".repeat(64)}`],
    settled_at: "2026-07-12T11:30:00Z"
  }];
  quarantined.documents.authorization.session_id = quarantined.documents.manifest.session_id;
  quarantined.runtime.risk = {
    ...quarantined.runtime.risk,
    account_equity: 17.90462,
    authorized_equity_ceiling: 17.90462,
    risk_eligible_equity: 11.09862,
    profit_quarantine_enabled: true,
    verified_internal_realized_pnl: 6.806,
    verified_internal_settlement_ids: ["manual-redeem-1"],
    quarantined_internal_profit: 6.806,
    risk_headroom: "starting_collateral_only"
  };
  const quarantineControls = spies();
  const quarantineResult = await executeStrategyCanary({
    ...quarantined,
    ...quarantineControls
  });
  assert.equal(quarantineResult.status, "funded_direct_executed");
  assert.deepEqual(quarantineControls.calls, { reserve: 1, consume: 1, execute: 1, finalize: 0 });
});

test("loss-resizing protected capital submits only the current-equity size bound to the source intent", async () => {
  const input = fixture(false);
  input.config.operatorDirect = true;
  input.config.trustBoundaryReady = false;
  input.config.maxOrderNotional = 10.5;
  input.config.campaignBaselineEquity = 11.09862;
  input.config.maxReconciliationDiscrepancy = 0.01;
  input.documents.intent.shares = "20";
  input.documents.intent.notional = "4";
  input.documents.manifest = {
    schema_version: "polyedge.operator_funded_session.v3",
    session_id: "dynamic-quote-funded-test-v7",
    authorization_mode: "operator_direct",
    authorized_by_user_reference: "Codex task protected compounding",
    research_promotion_bypassed: true,
    research_lane_isolated: true,
    maker_only: true,
    no_deposits: true,
    allow_automatic_replenishment: false,
    allow_compounding: true,
    continue_after_loss: true,
    external_cash_flows: [],
    max_open_orders: 1,
    target_order_notional: 10.5,
    max_order_notional: 10.5,
    max_account_loss: 11.09862,
    starting_collateral: 11.09862,
    max_reconciliation_discrepancy: 0.01,
    capital_policy: {
      reserve_ratio: 0.3,
      operating_buffer_ratio: 0.01,
      minimum_order_notional: 1,
      reserve_basis: "fully_reconciled_current_equity",
      loss_response: "resize_from_fully_reconciled_current_equity",
      prior_state_session_id: "dynamic-quote-funded-test-v5",
      prior_state_blob_name:
        "reports/funded/dynamic-quote/sessions/dynamic-quote-funded-test-v5/capital-reserve-state.json",
      minimum_historical_high_water_equity: 17.90462,
      high_water_update: "full_reconciliation_only",
      reserve_monotonic: false,
      state_blob_name:
        "reports/funded/dynamic-quote/sessions/dynamic-quote-funded-test-v7/capital-reserve-state.json"
    },
    internal_settlements: [],
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
    authorized_equity_ceiling: 17.90462,
    no_replenishment: true,
    no_compounding: false,
    allow_compounding: true,
    net_external_cash_flow: 0,
    cash_flow_count: 0,
    cash_flow_ids: [],
    maximum_reconciliation_discrepancy: 0.01,
    account_equity: 5,
    high_water_equity: 17.90462,
    historical_high_water_equity: 17.90462,
    prior_state_session_id: "dynamic-quote-funded-test-v5",
    prior_state_blob_name:
      "reports/funded/dynamic-quote/sessions/dynamic-quote-funded-test-v5/capital-reserve-state.json",
    protected_reserve: 1.5,
    reserve_basis: "fully_reconciled_current_equity",
    reserve_monotonic: false,
    continue_after_loss: true,
    last_reconciled_equity: 5,
    operating_buffer: 0.05,
    operable_capital: 3.45,
    order_notional: 3.45,
    proposed_notional: 3.45
  };
  input.runtime.executionSizing = {
    schema: "polyedge.protected_order_sizing.v1",
    executable: true,
    source_shares: 20,
    source_notional: 4,
    price: 0.2,
    shares: 17.25,
    notional: 3.45,
    fee_risk_upper_bound: 0,
    reserved_notional: 3.45,
    blockers: []
  };
  let submittedIntent;
  let reservation;
  const controls = spies();
  controls.reserveRisk = async (value) => {
    controls.calls.reserve += 1;
    reservation = value;
    return value;
  };
  controls.executeLifecycle = async (value) => {
    controls.calls.execute += 1;
    submittedIntent = value.intent;
    return { order_id: "order-1" };
  };
  const result = await executeStrategyCanary({ ...input, ...controls });
  assert.equal(result.status, "funded_direct_executed");
  assert.equal(result.execution_sizing.scaled_to_current_funds, true);
  assert.equal(submittedIntent.shares, "17.25");
  assert.equal(submittedIntent.notional, "3.45");
  assert.equal(submittedIntent.source_requested_notional, "4");
  assert.equal(reservation.principal_notional, 3.45);
  assert.equal(reservation.reserved_notional, 3.45);
  assert.deepEqual(controls.calls, { reserve: 1, consume: 1, execute: 1, finalize: 0 });
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

test("only exact venue rejection messages are classified as deterministic no-order", () => {
  assert.deepEqual(
    deterministicNoOrderRejection(new Error("invalid expiration value, must be in the future for GTD orders")),
    {
      code: "invalid_gtd_expiration",
      message: "invalid expiration value, must be in the future for GTD orders"
    }
  );
  assert.deepEqual(
    deterministicNoOrderRejection({
      response: { data: { error: "invalid post-only order: order crosses book" } }
    }),
    {
      code: "post_only_crosses_book",
      message: "invalid post-only order: order crosses book"
    }
  );
  assert.equal(
    deterministicNoOrderRejection(new Error("invalid post-only order: order crosses book after submission")),
    null
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
  assert.equal(
    validateDeterministicNoOrderReconciliation({
      ...proof,
      error: new Error("invalid post-only order: order crosses book")
    }).code,
    "post_only_crosses_book"
  );
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

test("persistent startup atomically bootstraps or verifies the exact funded session manifest", async () => {
  const values = new Map();
  let uploadCalls = 0;
  const container = {
    getBlockBlobClient: (name) => ({
      uploadData: async (bytes, options) => {
        uploadCalls += 1;
        assert.equal(options.conditions.ifNoneMatch, "*");
        if (values.has(name)) {
          throw Object.assign(new Error("exists"), { statusCode: 412 });
        }
        values.set(name, Buffer.from(bytes));
      }
    }),
    getBlobClient: (name) => ({
      download: async () => {
        if (!values.has(name)) {
          throw Object.assign(new Error("missing"), { statusCode: 404 });
        }
        return { readableStreamBody: Readable.from([values.get(name)]) };
      }
    })
  };
  const value = { schema_version: "polyedge.operator_funded_session.v1", session_id: "session-v6" };
  const expectedHash = sha256(Buffer.from(JSON.stringify(value, null, 2)));
  const input = { blobName: "sessions/v6/session.json", expectedHash, value };
  assert.deepEqual((await putOperatorSessionManifest(container, input)).value, value);
  assert.deepEqual((await putOperatorSessionManifest(container, input)).value, value);
  assert.equal(uploadCalls, 2);
  assert.deepEqual((await putOperatorSessionManifest(container, { ...input, readOnly: true })).value, value);
  assert.equal(uploadCalls, 2);
  await assert.rejects(
    putOperatorSessionManifest(container, {
      ...input,
      readOnly: true,
      value: { ...value, session_id: "tampered" }
    }),
    /SHA-256 mismatch/
  );
  assert.equal(uploadCalls, 2);
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
    horizons: [100, 200, 300], horizonScaleMs: 1, pollMs: 1,
    feeParameters: { rate: 0, rateBps: 0, exponent: 0, takerOnly: true }
  });
  await new Promise((resolve) => setTimeout(resolve, 2));
  visible = [];
  const rows = await capture.finish(fills);
  assert.equal(rows.length, 6);
  assert.deepEqual([...new Set(rows.map((row) => row.fill_id))], ["fill-a", "fill-b"]);
  assert.deepEqual([...new Set(rows.map((row) => row.horizon_seconds))], [100, 200, 300]);
  assert.equal(calls.length, 6);
  assert.ok(rows.every((row) => row.fill_size > 0));
  assert.ok(rows.every((row) => row.midpoint !== null && row.executable_price !== null));
  assert.ok(rows.every((row) => row.request_started_at <= row.response_completed_at));
  assert.ok(rows.every((row) => row.observed_at === row.response_completed_at));
  assert.ok(rows.every((row) => row.response_duration_ms >= 0));
  assert.ok(rows.every((row) => /^sha256:[0-9a-f]{64}$/.test(row.book_hash)));
  assert.ok(Date.now() - started < 500, "concurrent deadlines should complete near the longest horizon");
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
