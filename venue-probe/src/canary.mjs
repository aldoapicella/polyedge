import { AssetType, Chain, ClobClient, OrderType, Side } from "@polymarket/clob-client-v2";
import { pathToFileURL } from "node:url";
import { createWalletClient, decodeEventLog, formatUnits, http } from "viem";
import { privateKeyToAccount } from "viem/accounts";
import { polygon } from "viem/chains";
import {
  EVIDENCE_PROTOCOL_VERSION,
  EventLedger,
  HORIZONS_SECONDS,
  acquireCampaignLease,
  assertEligibleOrigin,
  finalizeProbeRisk,
  isRiskReservationResolved,
  loadCampaignRiskControl,
  loadCampaignRiskReservationRecords,
  marketContext,
  modelObservations,
  publishTerminalRiskPortfolioEvidence,
  reserveProbeRisk,
  settleProbeRiskReservations,
  sanitize,
  storageContainer,
  summarizeCampaignRisk,
  uploadEvidence,
  validateFillMarkouts
} from "./lib.mjs";
import {
  consumeOneShotAuthorization,
  beginFillMarkoutCapture,
  artifactLocationFromUri,
  deterministicNoOrderRejection,
  executeStrategyCanary,
  loadCanaryConfig,
  loadHashedJson,
  polymarketV2FeePerShare,
  sha256,
  validateDeterministicNoOrderReconciliation,
  validateCanaryPreflight
} from "./canary-lib.mjs";
import {
  cancelOrderWithMetrics,
  cancellationEventReceivedAt,
  connectLifecycleChannel,
  firstFillTimestamp,
  hasExactEligibleHorizons,
  marketMessagesThrough,
  maximumMatchedSize,
  mergeTradeFills,
  nearlyEqualSize,
  postCancelFillStats,
  publicTradeThroughStats,
  sameStringSet,
  sum,
  tradeFillsFromRest,
  tradeFillsFromUserEvents,
  waitForStablePostCancelReconciliation
} from "./canary-lifecycle-lib.mjs";
import {
  discoverVerifiedAutomaticInternalSettlements,
  loadDurableInternalSettlements,
  putVerifiedInternalSettlement,
  reconcileProtectedCompoundingState,
  sizeProtectedOrder,
  verifyConfiguredInternalSettlements
} from "./compounding-risk.mjs";
import { initializeProfitQuarantine } from "./profit-quarantine.mjs";

let config;
let runKind;
let runId;
let ledger;
let lease;
let userChannel;
let marketChannel;
let orderSubmissionAttempted = false;
let activeResources = null;
const AUTOMATIC_SETTLEMENT_RETRY_MS = 10_000;
const CONDITIONAL_TOKENS_ADDRESS = "0x4d97dcd97ec945f40cf65f87097ace5ea0476045";
const PUSD_ADDRESS = "0xc011a7e12a19f7b1f670d46f03b03f3342e82dfb";
const USDCE_ADDRESS = "0x2791bca1f2de4661ed88a30c99a7a9449aa84174";
const PAYOUT_REDEMPTION_TOPIC = "0x2682012a4a4f1973119f1c9b90745d1bd91fa2bab387344f044cb3586864d18d";
const ERC20_TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
const ERC1155_TRANSFER_SINGLE_TOPIC = "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62";
const ERC1155_TRANSFER_BATCH_TOPIC = "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb";
const COLLATERAL_WRAPPED_TOPIC = "0xc00a5c84859ae82a7f5e6a2773283fb525335d5b3195f61174aa1ecc7e15dd84";
const PAYOUT_REDEMPTION_EVENT = [{
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
const ERC20_TRANSFER_EVENT = [{
  type: "event",
  name: "Transfer",
  anonymous: false,
  inputs: [
    { name: "from", type: "address", indexed: true },
    { name: "to", type: "address", indexed: true },
    { name: "value", type: "uint256", indexed: false }
  ]
}];
const ERC1155_TRANSFER_SINGLE_EVENT = [{
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
const ERC1155_TRANSFER_BATCH_EVENT = [{
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
const COLLATERAL_WRAPPED_EVENT = [{
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

function setExecutionContext(env) {
  config = loadCanaryConfig(env);
  runKind = config.operatorDirect ? "funded-direct" : "strategy-canary";
  runId = env.STRATEGY_CANARY_RUN_ID || `${runKind}-${new Date().toISOString().replace(/[-:.TZ]/g, "")}-${crypto.randomUUID().slice(0, 8)}`;
  ledger = new EventLedger(runId);
  orderSubmissionAttempted = false;
}

async function initializeResources({ persistent = false } = {}) {
  const container = storageContainer(config);
  if (!container) throw new Error("fail closed: durable Azure Blob storage is unavailable");
  await container.createIfNotExists();
  const intentContainer = storageContainer({ ...config, storageContainer: config.intentContainerName });
  const manifestContainer = storageContainer({ ...config, storageContainer: config.manifestContainerName });
  if (!intentContainer || !manifestContainer) throw new Error("fail closed: intent or manifest source container is unavailable");
  if (config.operatorDirect) {
    await putOperatorSessionManifest(manifestContainer, {
      blobName: config.manifestBlobName,
      expectedHash: config.manifestBlobHash,
      value: config.operatorSessionManifest
    });
  }
  const modelArtifact = artifactLocationFromUri(config.executionModelBlobUri, config.storageAccount);
  const modelContainer = storageContainer({ ...config, storageContainer: modelArtifact.container });
  if (!modelContainer) throw new Error("fail closed: execution model source container is unavailable");
  const [manifestDocument, executionModelDocument] = await Promise.all([
    loadHashedJson(manifestContainer, config.manifestBlobName, config.manifestBlobHash),
    loadHashedJson(modelContainer, modelArtifact.blobName, config.executionModelHash)
  ]);
  const account = privateKeyToAccount(normalizePrivateKey(config.privateKey));
  const signer = createWalletClient({ account, chain: polygon, transport: http("https://polygon-bor-rpc.publicnode.com") });
  const client = new ClobClient({
    host: config.clobUrl,
    chain: Chain.POLYGON,
    signer,
    creds: { key: config.apiKey, secret: config.apiSecret, passphrase: config.apiPassphrase },
    signatureType: config.signatureType,
    funderAddress: config.funderAddress,
    useServerTime: true,
    throwOnError: true
  });
  const protectedCompoundingContext = manifestDocument.value?.allow_compounding === true
    ? await initializeProtectedCompounding({
        container,
        manifest: manifestDocument.value
      })
    : null;
  const profitQuarantineSnapshot = manifestDocument.value?.profit_quarantine?.enabled === true
    ? await initializeProfitQuarantine({
        container,
        manifest: manifestDocument.value,
        activity: await fetchJson(
          `https://data-api.polymarket.com/activity?user=${encodeURIComponent(config.funderAddress)}&limit=500`
        ),
        getTransactionReceipt: confirmedPolygonReceipt
      })
    : null;
  const resourceLease = !config.dryRun ? await acquireCampaignLease(config, persistent ? `funded-direct-service-${crypto.randomUUID()}` : runId) : null;
  const ledgerMultiplexer = {
    current: persistent ? null : ledger,
    record(event, payload) {
      this.current?.record(event, payload);
    }
  };
  let persistentUserChannel = null;
  if (persistent) {
    persistentUserChannel = await connectLifecycleChannel({
      url: config.userWsUrl,
      subscription: {
        auth: { apiKey: config.apiKey, secret: config.apiSecret, passphrase: config.apiPassphrase },
        type: "user"
      },
      ledger: ledgerMultiplexer,
      eventType: "venue_user_channel"
    });
  }
  return {
    persistent,
    container,
    intentContainer,
    modelArtifact,
    manifestDocument,
    executionModelDocument,
    profitQuarantineSnapshot,
    protectedCompoundingContext,
    client,
    lease: resourceLease,
    ledgerMultiplexer,
    userChannel: persistentUserChannel,
    marketChannel: null,
    warmedMarket: null,
    safetyCache: {
      generation: 0,
      timer: null,
      inFlight: 0,
      latest: null,
      lastError: null,
      market_id: null,
      condition_id: null,
      token_id: null
    },
    busy: false,
    baseBinding: {
      manifestBlobName: config.manifestBlobName,
      manifestBlobHash: config.manifestBlobHash,
      executionModelBlobUri: config.executionModelBlobUri,
      executionModelHash: config.executionModelHash,
      candidateName: config.candidateName,
      candidateVersion: config.candidateVersion,
      candidateConfigHash: config.candidateConfigHash
    }
  };
}

export async function initializeProtectedCompounding({
  container,
  manifest,
  loadActivity = loadSettlementActivity
}) {
  const durableSettlements = await loadDurableInternalSettlements(
    container,
    manifest.session_id
  );
  const configuredSettlements = manifest.internal_settlements || [];
  const activity = configuredSettlements.length
    ? await loadActivity({
        user: config.funderAddress,
        conditionIds: configuredSettlements.map((row) => row.condition_id),
        sessionStartedAt: manifest.created_at
      })
    : [];
  const verifiedConfiguredSettlements = await verifyConfiguredInternalSettlements({
    manifest,
    activity,
    getTransactionReceipt: confirmedPolygonReceipt,
    durableSettlements
  });
  for (const settlement of verifiedConfiguredSettlements) {
    if (!durableSettlements.some((row) => row.id === settlement.id)) {
      await putVerifiedInternalSettlement(container, {
        ...settlement,
        session_id: manifest.session_id
      });
    }
  }
  return {
    state: null,
    verifiedConfiguredSettlements,
    automaticSettlementPromise: null,
    automaticSettlementLastAttemptMs: 0
  };
}

async function reconcileProtectedCompoundingWithAutomaticSettlement({
  client,
  container,
  manifest,
  accountEquity,
  fullyReconciled,
  context,
  reservationRecords,
  allowAutomaticDiscovery
}) {
  const reconcile = () => reconcileProtectedCompoundingState({
    container,
    manifest,
    accountEquity,
    fullyReconciled,
    verifiedConfiguredSettlements: context.verifiedConfiguredSettlements
  });
  try {
    return await reconcile();
  } catch (error) {
    if (!fullyReconciled ||
        !String(error?.message || "").includes("unauthorized external deposit detected")) {
      throw error;
    }
    if (!allowAutomaticDiscovery) {
      throw new Error(
        "fail closed: authenticated automatic settlement reconciliation is pending the background safety cache"
      );
    }
    if (!context.automaticSettlementPromise) {
      const elapsedSinceAttempt = Date.now() -
        Number(context.automaticSettlementLastAttemptMs || 0);
      if (elapsedSinceAttempt < AUTOMATIC_SETTLEMENT_RETRY_MS) {
        throw new Error(
          "fail closed: authenticated automatic settlement reconciliation retry is cooling down"
        );
      }
      context.automaticSettlementLastAttemptMs = Date.now();
      const work = (async () => {
        const durableSettlements = await loadDurableInternalSettlements(
          container,
          manifest.session_id
        );
        const activity = await loadSettlementActivity({
          user: config.funderAddress,
          conditionIds: reservationRecords.map((record) => record.reservation?.condition_id),
          sessionStartedAt: manifest.created_at
        });
        const settlements = await discoverVerifiedAutomaticInternalSettlements({
          manifest,
          reservations: reservationRecords.map((record) => record.reservation),
          activity,
          durableSettlements,
          expectedWallet: config.funderAddress,
          getOrderFills: async (reservation) => {
            const trades = await client.getTrades({ market: reservation.condition_id });
            if (!Array.isArray(trades)) {
              throw new Error("fail closed: authenticated CLOB trade history is invalid");
            }
            return tradeFillsFromRest(trades, reservation.order_id);
          },
          getTransactionReceipt: confirmedPolygonReceipt
        });
        if (!settlements.length) {
          throw new Error(
            "fail closed: excess equity has no new exact reservation-bound authenticated redemption"
          );
        }
        for (const settlement of settlements) {
          await putVerifiedInternalSettlement(container, settlement);
        }
        return reconcile();
      })();
      const shared = work.finally(() => {
        if (context.automaticSettlementPromise === shared) {
          context.automaticSettlementPromise = null;
        }
      });
      context.automaticSettlementPromise = shared;
    }
    return context.automaticSettlementPromise;
  }
}

export function requireExecutionModelArtifact(value) {
  const container = String(value?.container || "").trim();
  const blobName = String(value?.blobName || "").trim();
  if (!/^[a-z0-9](?:[a-z0-9-]{1,61}[a-z0-9])?$/.test(container) ||
      !blobName ||
      blobName.startsWith("/") ||
      blobName.includes("\\") ||
      blobName.split("/").some((part) => !part || part === "." || part === "..")) {
    throw new Error("fail closed: persistent execution-model artifact provenance is unavailable");
  }
  return { ...value, container, blobName };
}

async function closeResources(resources) {
  if (resources?.safetyCache?.timer) clearInterval(resources.safetyCache.timer);
  if (resources?.safetyCache) {
    resources.safetyCache.timer = null;
    resources.safetyCache.generation += 1;
    resources.safetyCache.latest = null;
  }
  if (resources?.ledgerMultiplexer) resources.ledgerMultiplexer.current = null;
  resources?.userChannel?.clearHistory?.();
  resources?.marketChannel?.clearHistory?.();
  resources?.userChannel?.close();
  resources?.marketChannel?.close();
  if (resources?.lease) await resources.lease.release();
}

export async function putOperatorSessionManifest(container, {
  blobName,
  expectedHash,
  value
}) {
  if (!container || !blobName || !value) {
    throw new Error("fail closed: operator session manifest bootstrap is incomplete");
  }
  const bytes = Buffer.from(JSON.stringify(value, null, 2));
  if (sha256(bytes) !== expectedHash) {
    throw new Error("fail closed: embedded operator session manifest SHA-256 mismatch");
  }
  try {
    await container.getBlockBlobClient(blobName).uploadData(bytes, {
      conditions: { ifNoneMatch: "*" },
      blobHTTPHeaders: { blobContentType: "application/json" }
    });
  } catch (error) {
    if (![409, 412].includes(Number(error?.statusCode))) throw error;
  }
  return loadHashedJson(container, blobName, expectedHash);
}

function validatePersistentBinding(resources) {
  const expected = resources.baseBinding;
  const actual = {
    manifestBlobName: config.manifestBlobName,
    manifestBlobHash: config.manifestBlobHash,
    executionModelBlobUri: config.executionModelBlobUri,
    executionModelHash: config.executionModelHash,
    candidateName: config.candidateName,
    candidateVersion: config.candidateVersion,
    candidateConfigHash: config.candidateConfigHash
  };
  for (const [field, value] of Object.entries(expected)) {
    if (actual[field] !== value) throw new Error(`fail closed: persistent executor binding drifted for ${field}`);
  }
}

async function ensurePersistentMarket(resources, { market_id, condition_id, token_id, token_ids, market_end_ts }) {
  const requestedTokens = [...new Set(
    (Array.isArray(token_ids) ? token_ids : [token_id]).map(String).filter(Boolean)
  )];
  const next = {
    market_id: String(market_id || ""),
    condition_id: String(condition_id || ""),
    token_id: String(token_id || requestedTokens[0] || ""),
    token_ids: requestedTokens,
    market_end_ts: String(market_end_ts || "")
  };
  if (!next.market_id || !next.condition_id || !next.token_id || !Number.isFinite(Date.parse(next.market_end_ts))) {
    throw new Error("fail closed: persistent market warmup is incomplete");
  }
  const subscriptionStartedWallMs = Date.now();
  let needsFreshBook = false;
  if (!resources.marketChannel) {
    needsFreshBook = true;
    resources.marketChannel = await connectLifecycleChannel({
      url: config.marketWsUrl,
      subscription: {
        assets_ids: next.token_ids,
        type: "market",
        custom_feature_enabled: true
      },
      ledger: resources.ledgerMultiplexer,
      eventType: "venue_market_channel"
    });
  } else if (!resources.warmedMarket ||
      resources.warmedMarket.condition_id !== next.condition_id ||
      !next.token_ids.every((value) => resources.warmedMarket.token_ids.includes(value))) {
    needsFreshBook = true;
    await resources.marketChannel.updateSubscription({ operation: "subscribe", assets_ids: next.token_ids });
    if (resources.warmedMarket?.token_ids?.length &&
        resources.warmedMarket.condition_id !== next.condition_id) {
      await resources.marketChannel.updateSubscription({
        operation: "unsubscribe",
        assets_ids: resources.warmedMarket.token_ids
      });
    }
  } else {
    const latest = [...resources.marketChannel.messages].reverse().find((message) =>
      String(message?.event_type || message?.type || "").toLowerCase() === "book" &&
      String(message?.asset_id || message?.token_id || message?.tokenId || "") === next.token_id
    );
    if (!latest) {
      needsFreshBook = true;
      await resources.marketChannel.updateSubscription({ operation: "subscribe", assets_ids: [next.token_id] });
    }
  }
  if (needsFreshBook) {
    await resources.marketChannel.waitForMessage((message) =>
      Number(message?._received_wall_ms) >= subscriptionStartedWallMs &&
      String(message?.event_type || message?.type || "").toLowerCase() === "book" &&
      String(message?.asset_id || message?.token_id || message?.tokenId || "") === next.token_id
    , 2_000);
  }
  resources.warmedMarket = next;
  return next;
}

async function reconcilePersistentChannels(resources, market) {
  if (!resources.userChannel.requiresReconciliation() &&
      !resources.marketChannel.requiresReconciliation()) return;
  const [openOrders, trades] = await Promise.all([
    getOpenOrdersStrict(resources.client),
    resources.client.getTrades({ market: market.condition_id })
  ]);
  if (openOrders.length !== 0 || !Array.isArray(trades)) {
    throw new Error("fail closed: websocket reconnect reconciliation did not prove a coherent account");
  }
  resources.userChannel.markReconciled();
  resources.marketChannel.markReconciled();
}

export async function createPersistentCanaryExecutor({ env = process.env } = {}) {
  setExecutionContext(env);
  const resources = await initializeResources({ persistent: true });
  return {
    async warmMarket(value) {
      resources.userChannel?.beginEvidenceWindow?.();
      resources.marketChannel?.beginEvidenceWindow?.();
      const warmed = await ensurePersistentMarket(resources, value);
      const warmupStartedMonotonicMs = performance.now();
      const [geoblock, serverTime, book] = await Promise.all([
        fetchJson("https://polymarket.com/api/geoblock"),
        resources.client.getServerTime(),
        resources.client.getOrderBook(String(warmed.token_id))
      ]);
      assertEligibleOrigin(geoblock, config);
      const venueClock = Number(serverTime?.server_time ?? serverTime?.time ?? serverTime);
      const restBestAsk = Math.min(...(book?.asks || []).map((row) => Number(row.price)).filter(Number.isFinite));
      const streamEvidence = streamBookEvidence(resources.marketChannel.messages, warmed.token_id);
      if (!Number.isFinite(venueClock) ||
          !Number.isFinite(restBestAsk) ||
          !Number.isFinite(streamEvidence?.bestAsk) ||
          Math.abs(restBestAsk - streamEvidence.bestAsk) > 1e-9) {
        throw new Error("fail closed: persistent warmup REST, clock, and public stream evidence disagree");
      }
      startSafetySnapshotCache(resources, warmed);
      resources.userChannel?.beginEvidenceWindow?.();
      resources.marketChannel?.beginEvidenceWindow?.();
      return {
        ...warmed,
        no_sign: true,
        rest_stream_book_agreement: true,
        warmup_duration_ms: performance.now() - warmupStartedMonotonicMs
      };
    },
    async execute(executionEnv) {
      if (resources.busy) throw new Error("fail closed: persistent executor is already processing an intent");
      resources.busy = true;
      try {
        setExecutionContext(executionEnv);
        validatePersistentBinding(resources);
        resources.ledgerMultiplexer.current = ledger;
        resources.userChannel?.beginEvidenceWindow?.();
        resources.marketChannel?.beginEvidenceWindow?.();
        const warmedMarket = await ensurePersistentMarket(resources, {
          market_id: executionEnv.STRATEGY_CANARY_MARKET_ID,
          condition_id: executionEnv.STRATEGY_CANARY_CONDITION_ID,
          token_id: executionEnv.STRATEGY_CANARY_TOKEN_ID,
          market_end_ts: executionEnv.STRATEGY_CANARY_MARKET_END_TS
        });
        await reconcilePersistentChannels(resources, warmedMarket);
        lease = resources.lease;
        userChannel = resources.userChannel;
        marketChannel = resources.marketChannel;
        activeResources = resources;
        try {
          const result = await main(resources);
          return sanitize({ schema: "polyedge.strategy_canary_run.v1", run_id: runId, ...result });
        } catch (error) {
          // The persistent worker must distinguish a pre-submit failure from a
          // post-submit evidence failure. It may only seal the latter after
          // independently verifying the durable terminal risk reservation.
          error.orderSubmissionAttempted = orderSubmissionAttempted;
          throw error;
        }
      } finally {
        resources.ledgerMultiplexer.current = null;
        resources.userChannel?.beginEvidenceWindow?.();
        resources.marketChannel?.beginEvidenceWindow?.();
        activeResources = null;
        resources.busy = false;
      }
    },
    async close() {
      await closeResources(resources);
    },
    status() {
      const safetySnapshotCompletedWallMs = Number(
        resources.safetyCache?.latest?.runtime?.capturedCompletedWallMs
      );
      const userChannelHistory = resources.userChannel?.historyStats?.();
      const marketChannelHistory = resources.marketChannel?.historyStats?.();
      return {
        user_channel_ready: resources.userChannel?.isOpen() === true,
        market_channel_ready: resources.marketChannel?.isOpen() === true,
        user_channel_gaps: resources.userChannel?.gapCount() || 0,
        market_channel_gaps: resources.marketChannel?.gapCount() || 0,
        user_channel_unparsed: resources.userChannel?.unparsedCount() || 0,
        market_channel_unparsed: resources.marketChannel?.unparsedCount() || 0,
        user_channel_reconnects: resources.userChannel?.reconnectCount() || 0,
        market_channel_reconnects: resources.marketChannel?.reconnectCount() || 0,
        reconnect_reconciliation_required:
          resources.userChannel?.requiresReconciliation() === true ||
          resources.marketChannel?.requiresReconciliation() === true,
        warmed_market: resources.warmedMarket,
        safety_snapshot_cache_ready: Number.isFinite(safetySnapshotCompletedWallMs),
        safety_snapshot_cache_age_ms: Number.isFinite(safetySnapshotCompletedWallMs)
          ? Math.max(0, Date.now() - safetySnapshotCompletedWallMs)
          : null,
        safety_snapshot_cache_in_flight: resources.safetyCache?.inFlight || 0,
        safety_snapshot_cache_error: resources.safetyCache?.lastError || null,
        user_channel_history_entries: userChannelHistory?.message_count || 0,
        user_channel_history_bytes: userChannelHistory?.approximate_bytes || 0,
        user_channel_history_evictions: userChannelHistory?.evicted_count || 0,
        market_channel_history_entries: marketChannelHistory?.message_count || 0,
        market_channel_history_bytes: marketChannelHistory?.approximate_bytes || 0,
        market_channel_history_evictions: marketChannelHistory?.evicted_count || 0,
        busy: resources.busy
      };
    }
  };
}

export async function runCanaryOnce({ env = process.env } = {}) {
  setExecutionContext(env);
  const resources = await initializeResources({ persistent: false });
  lease = resources.lease;
  activeResources = resources;
  try {
    return sanitize({ schema: "polyedge.strategy_canary_run.v1", run_id: runId, ...await main(resources) });
  } finally {
    activeResources = null;
    userChannel?.close();
    marketChannel?.close();
    await closeResources(resources);
  }
}

async function main(resources) {
  const {
    container,
    intentContainer,
    modelArtifact: rawModelArtifact,
    manifestDocument,
    executionModelDocument,
    client
  } = resources;
  // Provenance is needed after venue reconciliation to publish the immutable
  // summary. Validate it before any reservation, authorization, or signing.
  const modelArtifact = requireExecutionModelArtifact(rawModelArtifact);
  const [intentDocument, authorizationDocument] = await Promise.all([
    loadHashedJson(intentContainer, config.intentBlobName, config.intentBlobHash),
    loadHashedJson(container, config.authorizationBlobName, config.authorizationBlobHash)
  ]);
  const cachedRuntime = selectFreshCachedSafetySnapshot(
    resources,
    intentDocument.value,
    Date.now()
  );
  const runtime = cachedRuntime || await capturePreflight(
    client,
    intentDocument.value,
    manifestDocument.value
  );
  ledger.record(cachedRuntime ? "funded_safety_snapshot_cache_hit" : "funded_safety_snapshot_cache_miss", {
    wall_ms: Date.now(),
    monotonic_ms: performance.now(),
    decision_id: intentDocument.value.decision_id,
    snapshot_completed_wall_ms: runtime.capturedCompletedWallMs,
    snapshot_age_ms: Date.now() - Number(runtime.capturedCompletedWallMs)
  });
  const documents = {
    intent: intentDocument.value,
    manifest: manifestDocument.value,
    authorization: authorizationDocument.value,
    authorizationHash: authorizationDocument.hash,
    executionModel: executionModelDocument.value,
    executionModelHash: executionModelDocument.hash
  };
  const result = await executeStrategyCanary({
    config,
    documents,
    runtime,
    runId,
    reserveRisk: async (reservation) => {
      const startedWallMs = Date.now();
      const startedMonotonicMs = performance.now();
      ledger.record("funded_risk_reservation_started", {
        wall_ms: startedWallMs,
        monotonic_ms: startedMonotonicMs,
        decision_id: documents.intent.decision_id
      });
      const value = await reserveProbeRisk(config, reservation);
      ledger.record("funded_risk_reservation_completed", {
        wall_ms: Date.now(),
        monotonic_ms: performance.now(),
        duration_ms: performance.now() - startedMonotonicMs,
        probe_id: value.probe_id
      });
      return value;
    },
    finalizeNoOrder: (reservation) => finalizeProbeRisk(config, reservation, {
      state: "released_no_order",
      order_submitted: false,
      matched_notional: 0,
      reconciliation_complete: true,
      zero_open_orders_confirmed: true
    }),
    consumeAuthorization: async (value) => {
      const startedMonotonicMs = performance.now();
      ledger.record("funded_authorization_consumption_started", {
        wall_ms: Date.now(),
        monotonic_ms: startedMonotonicMs,
        decision_id: documents.intent.decision_id
      });
      const consumed = await consumeOneShotAuthorization(container, value);
      ledger.record("funded_authorization_consumed", {
        wall_ms: Date.now(),
        monotonic_ms: performance.now(),
        duration_ms: performance.now() - startedMonotonicMs,
        decision_id: documents.intent.decision_id
      });
      return consumed;
    },
    executeLifecycle: (value) => executeLifecycle(client, value)
  });
  const evidenceProbe = result.lifecycle?.evidence_probe;
  if (!evidenceProbe) return result;
  let terminalEvidence = null;
  if (Number(evidenceProbe.lifecycle.actual_matched_size) === 0) {
    const terminalRuntime = await capturePreflight(client, documents.intent, documents.manifest);
    const campaign = await loadCampaignRiskControl(config);
    terminalEvidence = await publishTerminalRiskPortfolioEvidence(container, {
      reservation: {
        run_id: runId,
        probe_id: evidenceProbe.probe_id,
        order_id: evidenceProbe.lifecycle.order_id,
        condition_id: documents.intent.condition_id,
        state: "finalized_no_fill",
        matched_notional: 0
      },
      settlement: {
        settlement_verified: true,
        trust_boundary_ready: config.trustBoundaryReady,
        zero_open_orders_confirmed: terminalRuntime.openOrderCount === 0,
        evidence_source: "authenticated_no_fill",
        settled_ts: new Date().toISOString(),
        terminal_portfolio: {
          liquid_collateral: terminalRuntime.risk.liquid_collateral,
          current_position_value: terminalRuntime.risk.conservative_position_value,
          account_equity: terminalRuntime.risk.account_equity
        }
      },
      campaign
    });
  }
  const summary = {
    schema_version: 3,
    evidence_protocol_version: EVIDENCE_PROTOCOL_VERSION,
    run_id: runId,
    status: "completed",
    started_ts: evidenceProbe.started_ts,
    finished_ts: evidenceProbe.finished_ts,
    order_submission_attempted: true,
    order_submitted: true,
    submitted_order_count: 1,
    completed_probe_count: evidenceProbe.status === "completed" ? 1 : 0,
    candidate: {
      name: documents.intent.candidate_name,
      candidate_version: documents.intent.candidate_version,
      config_hash: documents.intent.candidate_config_hash
    },
    prediction_model: {
      schema: documents.executionModel.schema,
      blob_uri: config.executionModelBlobUri,
      container_name: modelArtifact.container,
      blob_name: modelArtifact.blobName,
      sha256: documents.executionModelHash,
      model_version: documents.executionModel.model_version,
      generated_at: documents.executionModel.generated_at,
      training_data_end_ts: documents.executionModel.training_data_end_ts || null
    },
    provenance: {
      decision_id: documents.intent.decision_id,
      authorization_kind: documents.authorization.schema === "polyedge.operator_funded_intent_authorization.v1"
        ? "operator_direct"
        : documents.authorization.schema === "polyedge.funded_stage_intent_authorization.v1"
          ? "funded_stage"
          : "checkpoint_1_canary",
      operator_session_id: documents.authorization.session_id || null,
      research_promotion_bypassed: documents.authorization.research_promotion_bypassed === true,
      human_grant_id: documents.authorization.human_grant_id || null,
      human_grant_consumption_blob_name: documents.authorization.human_grant_consumption_blob_name || null,
      human_grant_consumption_sha256: documents.authorization.human_grant_consumption_sha256 || null,
      funded_stage_grant_id: documents.authorization.schema === "polyedge.funded_stage_intent_authorization.v1" ? config.humanGrantId : null,
      funded_stage_grant_sha256: documents.authorization.schema === "polyedge.funded_stage_intent_authorization.v1" ? config.humanGrantHash : null,
      funded_stage_consumption_blob_name: documents.authorization.funded_stage_consumption_blob_name || null,
      funded_stage_consumption_sha256: documents.authorization.funded_stage_consumption_sha256 || null,
      funded_stage_source_state_sha256: documents.authorization.funded_stage_source_state_sha256 || null,
      funded_stage_target_orders: documents.authorization.funded_stage_target_orders || null,
      authorization_blob_name: config.authorizationBlobName,
      authorization_sha256: documents.authorizationHash,
      authorization_container_name: config.storageContainer,
      intent_container_name: config.intentContainerName,
      intent_blob_name: config.intentBlobName,
      intent_sha256: config.intentBlobHash,
      promotion_manifest_container_name: documents.authorization.source_promotion_manifest_container_name || config.manifestContainerName,
      promotion_manifest_blob_name: documents.authorization.source_promotion_manifest_blob_name || config.manifestBlobName,
      promotion_manifest_sha256: documents.authorization.source_promotion_manifest_sha256 || config.manifestBlobHash,
      execution_manifest_container_name: config.manifestContainerName,
      execution_manifest_blob_name: config.manifestBlobName,
      execution_manifest_sha256: config.manifestBlobHash,
      terminal_evidence_blob_name: terminalEvidence?.blob_name || null,
      terminal_evidence_sha256: terminalEvidence?.sha256 || null
    },
    execution_origin: config.executionOrigin,
    execution_country: runtime.geoblock.country,
    funder_address: config.funderAddress,
    static_egress_verified: runtime.geoblock.ip === config.expectedEgressIp,
    probes: [evidenceProbe],
    market: evidenceProbe.market,
    order: evidenceProbe.order,
    pre_send_context: evidenceProbe.pre_send_context,
    lifecycle: evidenceProbe.lifecycle,
    markouts: evidenceProbe.markouts,
    model_observations: evidenceProbe.model_observations,
    queue_position_source: "authenticated_lifecycle_plus_public_l2",
    queue_position_metric: "inferred_size_ahead",
    literal_fifo_rank_available: false,
    research_only: !config.operatorDirect,
    live_trading_enabled: config.operatorDirect,
    evidence_trust_boundary_ready: config.trustBoundaryReady
  };
  ledger.record("strategy_canary_protocol_v3_completed", {
    probe_id: evidenceProbe.probe_id,
    lifecycle: evidenceProbe.lifecycle,
    markouts: evidenceProbe.markouts,
    model_observations: evidenceProbe.model_observations
  });
  const evidenceUpload = await uploadEvidence(config, runId, summary, ledger);
  const { evidence_probe: _evidenceProbe, ...publicLifecycle } = result.lifecycle;
  return { ...result, lifecycle: publicLifecycle, evidence_upload: evidenceUpload };
}

async function capturePreflight(
  client,
  intent,
  manifest,
  ignoredReservationId = null,
  {
    recordLedger = true,
    profitQuarantineSnapshot = activeResources?.profitQuarantineSnapshot || null,
    protectedCompoundingContext =
      activeResources?.protectedCompoundingContext || null
  } = {}
) {
  const capturedStartedWallMs = Date.now();
  const capturedStartedMonotonicMs = performance.now();
  const clock = async () => {
    const requestStarted = Date.now();
    const serverTimeResponse = await client.getServerTime();
    const requestFinished = Date.now();
    const serverValue = Number(serverTimeResponse?.server_time ?? serverTimeResponse?.time ?? serverTimeResponse);
    const serverMs = serverValue < 1e12 ? serverValue * 1000 : serverValue;
    const clockRoundTripMs = requestFinished - requestStarted;
    const localMidpointMs = (requestStarted + requestFinished) / 2;
    const clockServerMinusLocalMs = serverMs - localMidpointMs;
    const serverClockQuantizationMs = serverValue < 1e12 && Number.isInteger(serverValue) ? 500 : 1;
    const clockUncertaintyMs = clockRoundTripMs / 2 + serverClockQuantizationMs;
    return {
      clockDriftMs: Math.abs(clockServerMinusLocalMs),
      clockServerMinusLocalMs,
      clockRoundTripMs,
      clockUncertaintyMs
    };
  };
  const [
    geoblock,
    clockEvidence,
    market,
    book,
    clobMarketInfo,
    openOrders,
    riskControl,
    balance,
    positionsResponse,
    valueResponse,
    campaignReservationRecords
  ] = await Promise.all([
    fetchJson("https://polymarket.com/api/geoblock"),
    clock(),
    loadExactMarket(intent),
    client.getOrderBook(String(intent.token_id)),
    client.getClobMarketInfo(String(intent.condition_id)),
    getOpenOrdersStrict(client),
    loadCampaignRiskControl(config),
    client.getBalanceAllowance({ asset_type: AssetType.COLLATERAL, signature_type: config.signatureType }),
    fetch(`https://data-api.polymarket.com/positions?user=${encodeURIComponent(config.funderAddress)}&sizeThreshold=0&limit=500`, { signal: AbortSignal.timeout(10_000) }),
    fetch(`https://data-api.polymarket.com/value?user=${encodeURIComponent(config.funderAddress)}`, { signal: AbortSignal.timeout(10_000) }),
    loadCampaignRiskReservationRecords(config)
  ]);
  assertEligibleOrigin(geoblock, config);
  const {
    clockDriftMs,
    clockServerMinusLocalMs,
    clockRoundTripMs,
    clockUncertaintyMs
  } = clockEvidence;
  if (!Number.isFinite(clockDriftMs)) throw new Error("fail closed: venue clock is invalid");
  const feeRate = Number(clobMarketInfo?.fd?.r ?? 0);
  const feeExponent = Number(clobMarketInfo?.fd?.e ?? 0);
  const feeTakerOnly = clobMarketInfo?.fd?.to === true || feeRate === 0;
  const feeRateBps = feeRate * 10_000;
  if (!Number.isFinite(feeRate) || feeRate < 0 || feeRate > 1 ||
      !Number.isFinite(feeExponent) || feeExponent < 0 || feeExponent > 10 ||
      !Number.isFinite(feeRateBps) || feeRateBps < 0 || feeRateBps > 10_000 ||
      (feeRate > 0 && !feeTakerOnly)) {
    throw new Error("fail closed: Polymarket V2 market fee rate/exponent/taker-only parameters are invalid");
  }
  if (!positionsResponse.ok || !valueResponse.ok) throw new Error("fail closed: account reconciliation endpoint failed");
  const positions = await positionsResponse.json();
  const reportedValues = await valueResponse.json();
  if (!Array.isArray(positions) || !Array.isArray(reportedValues)) throw new Error("fail closed: account reconciliation payload is invalid");
  const terminalConditionIds = [...new Set(positions
    .filter((row) => row.redeemable === true && row.conditionId)
    .map((row) => String(row.conditionId)))];
  const campaignReservations = campaignReservationRecords.map((record) => record.reservation);
  let reservations = campaignReservations.filter(
    (reservation) => !isRiskReservationResolved(reservation)
  );
  const terminalConditions = new Set(terminalConditionIds.map((value) => value.toLowerCase()));
  const terminalRiskNeedsSettlement = reservations.some((reservation) =>
    Number(reservation?.matched_notional) > 0
      && terminalConditions.has(String(reservation?.condition_id || "").toLowerCase())
  );
  if (terminalRiskNeedsSettlement) {
    await settleProbeRiskReservations(config, {
      condition_ids: terminalConditionIds,
      terminal_settlement_verified: true,
      evidence_source: "polymarket_data_api_redeemable",
      run_id: runId
    }, {
      reservationRecords: campaignReservationRecords
    });
    reservations = reservations.filter((reservation) =>
      !(Number(reservation?.matched_notional) > 0
        && terminalConditions.has(String(reservation?.condition_id || "").toLowerCase()))
    );
  }
  const liquidCollateral = Number(balance.balance) / 1_000_000;
  const summedPositionValue = positions.reduce(
    (sum, row) => sum + Math.max(0, Number(row.currentValue) || 0),
    0
  );
  const reportedPositionValue = reportedValues.reduce(
    (sum, row) => sum + Math.max(0, Number(row.value) || 0),
    0
  );
  const unresolvedPositionCount = positions.filter(
    (row) => Number(row.size) > 1e-9 && row.redeemable !== true
  ).length;
  const relevantReservations = reservations.filter(
    (row) => String(row.probe_id) !== String(ignoredReservationId || "")
  );
  const accountEquity =
    liquidCollateral + Math.min(summedPositionValue, reportedPositionValue);
  let protectedCompoundingState = null;
  if (config.operatorDirect && manifest?.allow_compounding === true) {
    if (!protectedCompoundingContext) {
      throw new Error("fail closed: protected compounding startup verification is unavailable");
    }
    const fullyReconciled =
      Math.abs(summedPositionValue - reportedPositionValue) <=
        Number(manifest.max_reconciliation_discrepancy) + 1e-9
      && openOrders.length === 0
      && unresolvedPositionCount === 0
      && relevantReservations.length === 0;
    const cachedState = protectedCompoundingContext.state;
    const stateMatchesEquity = cachedState
      && Math.abs(Number(cachedState.last_reconciled_equity) - accountEquity) <= 0.0000011;
    if ((fullyReconciled && !stateMatchesEquity) || !cachedState) {
      protectedCompoundingContext.state =
        await reconcileProtectedCompoundingWithAutomaticSettlement({
          client,
          container: storageContainer(config),
          manifest,
          accountEquity,
          fullyReconciled,
          context: protectedCompoundingContext,
          reservationRecords: campaignReservationRecords,
          allowAutomaticDiscovery: recordLedger === false
        });
    }
    protectedCompoundingState = protectedCompoundingContext.state;
  }
  const capturedCompletedWallMs = Date.now();
  const captureDurationMs = Math.max(0, performance.now() - capturedStartedMonotonicMs);
  const boundRuntime = bindIntentSizingAndRisk({
    geoblock,
    clockDriftMs,
    clockServerMinusLocalMs,
    clockRoundTripMs,
    clockUncertaintyMs,
    market,
    book,
    feeModel: "polymarket_clob_v2_curve",
    feeRate,
    feeRateBps,
    feeExponent,
    feeTakerOnly,
    openOrderCount: openOrders.length,
    fillModelVersion: config.requiredFillModelVersion,
    exactResolutionSource: intent.exact_resolution_source === true,
    resolutionSource: intent.resolution_source,
    capturedStartedWallMs,
    capturedCompletedWallMs,
    captureDurationMs,
    riskBasis: {
      control: riskControl,
      liquidCollateral,
      summedPositionValue,
      reportedPositionValue,
      unresolvedPositionCount,
      unresolvedReservationCount: relevantReservations.length,
      accountEquity,
      profitQuarantineSnapshot,
      protectedCompoundingState
    },
    client
  }, intent, manifest);
  if (recordLedger) {
    ledger?.record("funded_safety_snapshot_completed", {
      wall_ms: capturedCompletedWallMs,
      monotonic_ms: performance.now(),
      duration_ms: captureDurationMs,
      decision_id: intent.decision_id,
      open_order_count: openOrders.length,
      risk_passed: boundRuntime.risk.passed === true,
      submitted_notional: boundRuntime.executionSizing?.notional ??
        Number(intent.notional),
      current_funds_scaled:
        Number(boundRuntime.executionSizing?.notional) < Number(intent.notional) - 1e-9
    });
  }
  return boundRuntime;
}

function bindIntentSizingAndRisk(runtime, intent, manifest) {
  const basis = runtime.riskBasis;
  if (!basis) throw new Error("fail closed: account risk basis is unavailable");
  const feePerShare = polymarketV2FeePerShare(
    intent.price,
    runtime.feeRate,
    runtime.feeExponent
  );
  const executionSizing = basis.protectedCompoundingState
    ? sizeProtectedOrder({
        state: basis.protectedCompoundingState,
        accountEquity: basis.accountEquity,
        price: intent.price,
        requestedShares: intent.shares,
        requestedNotional: intent.notional,
        minimumOrderSize:
          runtime.book?.min_order_size ?? runtime.book?.minOrderSize,
        maximumOrderNotional: config.maxOrderNotional,
        feePerShare
      })
    : null;
  const principal = executionSizing
    ? executionSizing.notional
    : Number(intent.notional);
  const feeRisk = executionSizing
    ? executionSizing.fee_risk_upper_bound
    : Number(intent.shares) * feePerShare;
  const risk = summarizeCampaignRisk({
    control: basis.control,
    liquidCollateral: basis.liquidCollateral,
    summedPositionValue: basis.summedPositionValue,
    reportedPositionValue: basis.reportedPositionValue,
    openOrderCount: runtime.openOrderCount,
    unresolvedPositionCount: basis.unresolvedPositionCount,
    unresolvedReservationCount: basis.unresolvedReservationCount,
    proposedNotional: principal + feeRisk,
    orderNotional: principal,
    authorizedStartingCollateral:
      config.operatorDirect ? Number(manifest?.starting_collateral) : null,
    requireZeroExternalCashFlows: config.operatorDirect,
    profitQuarantineSnapshot: basis.profitQuarantineSnapshot,
    protectedCompoundingState: basis.protectedCompoundingState,
    additionalBlockers: executionSizing?.blockers || []
  });
  return {
    ...runtime,
    risk,
    executionSizing
  };
}

const SAFETY_CACHE_REFRESH_MS = 700;
const SAFETY_CACHE_MAX_IN_FLIGHT = 3;
const SAFETY_CACHE_MAX_SELECTION_AGE_MS = 650;

export function startSafetySnapshotCache(resources, market, {
  capture = capturePreflight,
  createIntent = conservativeWarmIntent,
  setIntervalFn = setInterval,
  clearIntervalFn = clearInterval
} = {}) {
  const cache = resources.safetyCache;
  if (cache.timer &&
      cache.market_id === String(market.market_id) &&
      cache.condition_id === String(market.condition_id) &&
      cache.token_id === String(market.token_id)) {
    return;
  }
  if (cache.timer) clearIntervalFn(cache.timer);
  cache.timer = null;
  cache.generation += 1;
  cache.latest = null;
  cache.lastError = null;
  cache.market_id = String(market.market_id);
  cache.condition_id = String(market.condition_id);
  cache.token_id = String(market.token_id);
  const generation = cache.generation;
  const syntheticIntent = createIntent(market);
  const refresh = async () => {
    if (resources.busy || cache.generation !== generation ||
        cache.inFlight >= SAFETY_CACHE_MAX_IN_FLIGHT) return;
    cache.inFlight += 1;
    try {
      const runtime = await capture(
        resources.client,
        syntheticIntent,
        resources.manifestDocument.value,
        null,
        {
          recordLedger: false,
          profitQuarantineSnapshot: resources.profitQuarantineSnapshot,
          protectedCompoundingContext: resources.protectedCompoundingContext
        }
      );
      if (cache.generation === generation) {
        cache.latest = {
          market_id: String(market.market_id),
          condition_id: String(market.condition_id),
          token_id: String(market.token_id),
          runtime
        };
        cache.lastError = null;
      }
    } catch (error) {
      if (cache.generation === generation) cache.lastError = error.message;
    } finally {
      cache.inFlight = Math.max(0, cache.inFlight - 1);
    }
  };
  void refresh();
  cache.timer = setIntervalFn(() => { void refresh(); }, SAFETY_CACHE_REFRESH_MS);
  cache.timer.unref?.();
}

function conservativeWarmIntent(market) {
  const price = 0.5;
  const notional = Number(config.maxOrderNotional);
  return {
    market_id: String(market.market_id),
    condition_id: String(market.condition_id),
    token_id: String(market.token_id),
    price: String(price),
    shares: String(notional / price),
    notional: String(notional),
    decision_id: `non-executable-warm-snapshot-${market.market_id}`
  };
}

export function selectFreshCachedSafetySnapshot(resources, intent, nowMs = Date.now()) {
  const cached = resources?.safetyCache?.latest;
  const completedWallMs = Number(cached?.runtime?.capturedCompletedWallMs);
  if (!cached ||
      cached.market_id !== String(intent?.market_id || "") ||
      cached.condition_id !== String(intent?.condition_id || "") ||
      cached.token_id !== String(intent?.token_id || "") ||
      !Number.isFinite(completedWallMs) ||
      nowMs < completedWallMs ||
      nowMs - completedWallMs > SAFETY_CACHE_MAX_SELECTION_AGE_MS) {
    return null;
  }
  // The warm snapshot is captured with a deliberately non-executable
  // synthetic intent. Rebind only the immutable resolution provenance from
  // the verified executable intent; all volatile venue/account evidence
  // remains the independently captured warm snapshot.
  const rebound = {
    ...cached.runtime,
    exactResolutionSource: intent.exact_resolution_source === true,
    resolutionSource: intent.resolution_source
  };
  if (!rebound.riskBasis || !resources?.manifestDocument?.value) return rebound;
  return bindIntentSizingAndRisk(
    rebound,
    intent,
    resources.manifestDocument.value
  );
}

async function executeLifecycle(client, { intent, documents, runtime, reservation }) {
  let refreshed;
  let preSendCapturedWallMs;
  let preSendContext;
  try {
    lease.assertHealthy();
    // Both channels are opened before signing so partial fills, cancellation races,
    // public trade-through, and markout evidence have no intentional blind window.
    const userChannelPromise = activeResources?.persistent
      ? Promise.resolve(userChannel)
      : connectLifecycleChannel({
          url: config.userWsUrl,
          subscription: {
            auth: { apiKey: config.apiKey, secret: config.apiSecret, passphrase: config.apiPassphrase },
            markets: [intent.condition_id],
            type: "user"
          },
          ledger,
          eventType: "venue_user_channel"
        }).then((channel) => {
          userChannel = channel;
          return channel;
        });
    const marketChannelPromise = activeResources?.persistent
      ? Promise.resolve(marketChannel)
      : connectLifecycleChannel({
          url: config.marketWsUrl,
          subscription: {
            assets_ids: [intent.token_id],
            type: "market",
            custom_feature_enabled: true
          },
          ledger,
          eventType: "venue_market_channel"
        }).then((channel) => {
          marketChannel = channel;
          return channel;
        });
    [, , refreshed] = await Promise.all([
      userChannelPromise,
      marketChannelPromise,
      activeResources?.persistent
        ? captureFinalGate(client, intent, runtime)
        : capturePreflight(
            client,
            documents.intent,
            documents.manifest,
            reservation.probe_id
          )
    ]);
    // Repeat the full immutable-intent, book, risk, clock, geoblock, model, and
    // authorization contract immediately before the only signing call.
    const preSendValidation = validateCanaryPreflight({ config, ...documents, runtime: refreshed, now: new Date() });
    if (Number(intent.shares) !== Number(preSendValidation.shares)
        || Number(intent.notional) !== Number(preSendValidation.notional)) {
      throw new Error("fail closed: current-funds execution sizing changed after reservation");
    }
    lease.assertHealthy();
    await Promise.all([userChannel.ensureOpen(), marketChannel.ensureOpen()]);
    if (userChannel.gapCount() > 0 || marketChannel.gapCount() > 0 ||
        userChannel.unparsedCount() > 0 || marketChannel.unparsedCount() > 0) {
      throw new Error("fail closed: authenticated/public websocket completeness was lost before submission");
    }
    if (activeResources?.persistent) {
      const userChannelHistory = userChannel.historyStats?.();
      const marketChannelHistory = marketChannel.historyStats?.();
      if (userChannelHistory?.evicted_count > 0 || marketChannelHistory?.evicted_count > 0) {
        throw new Error("fail closed: websocket evidence history was truncated before submission");
      }
      const snapshotAgeMs = Date.now() - Number(runtime.capturedCompletedWallMs);
      const finalGateAgeMs = Date.now() - Number(refreshed.finalGateCompletedWallMs);
      if (!Number.isFinite(snapshotAgeMs) || snapshotAgeMs > 1_500) {
        throw new Error(`fail closed: full safety snapshot exceeded 1500ms at signing (${snapshotAgeMs}ms)`);
      }
      if (!Number.isFinite(finalGateAgeMs) || finalGateAgeMs > 500) {
        throw new Error(`fail closed: final volatile gate exceeded 500ms at signing (${finalGateAgeMs}ms)`);
      }
      assertPersistentIntentRemainingTtl(intent, config.minRemainingTtlMs);
    }
    preSendCapturedWallMs = Date.now();
    preSendContext = {
      ...marketContext(marketMessagesThrough(marketChannel.messages, preSendCapturedWallMs)),
      source: "public_market_channel_before_submission",
      captured_wall_ms: preSendCapturedWallMs,
      intent_book_hash: intent.book_hash,
      current_book_hash: preSendValidation.actualBookHash,
      intent_book_hash_matched: preSendValidation.bookHashMatched
    };
  } catch (error) {
    try {
      await finalizeProbeRisk(config, reservation, {
        state: "released_no_order",
        order_submitted: false,
        matched_notional: 0,
        reconciliation_complete: true,
        zero_open_orders_confirmed: true
      });
    } catch (releaseError) {
      throw new Error(`fail closed: pre-submit lifecycle failed and no-order risk release also failed (${error.message}; ${releaseError.message})`);
    }
    throw error;
  }
  const expiration = Math.floor(Date.parse(intent.gtd_expiry_ts) / 1000);
  while (Date.now() <= preSendCapturedWallMs) await sleep(1);
  const sentAt = new Date();
  const sentMonotonicMs = performance.now();
  let response;
  try {
    orderSubmissionAttempted = true;
    ledger.record("venue_order_send", {
      probe_id: reservation.probe_id,
      wall_ms: Date.now(),
      monotonic_ms: sentMonotonicMs,
      active_valid_until: intent.valid_until,
      venue_gtd_expiry_ts: intent.gtd_expiry_ts,
      order: {
        token_id: intent.token_id,
        price: intent.price,
        shares: intent.shares,
        notional: intent.notional,
        source_requested_shares: intent.source_requested_shares,
        source_requested_notional: intent.source_requested_notional,
        current_funds_scaled: intent.current_funds_scaled === true,
        post_only: true
      }
    });
    response = await client.createAndPostOrder(
      { tokenID: intent.token_id, price: Number(intent.price), size: Number(intent.shares), side: Side.BUY, expiration },
      { tickSize: String(refreshed.book.tick_size ?? refreshed.book.tickSize), negRisk: refreshed.book.neg_risk === true || refreshed.book.negRisk === true },
      OrderType.GTD,
      true
    );
  } catch (error) {
    const rejection = deterministicNoOrderRejection(error);
    if (rejection) {
      await releaseDeterministicRejectedOrder(client, {
        intent,
        manifest: documents.manifest,
        reservation,
        sentAt,
        rejection
      });
      throw new Error(`fail closed: venue rejected the order without acknowledgement; no-order risk was reconciled and released (${rejection.code}: ${rejection.message})`);
    }
    await cancelAllAndConfirm(client);
    throw new Error(`fail closed: ambiguous strategy-canary submission; authorization is consumed and risk remains reserved (${error.message})`);
  }
  if (!response?.success || !response.orderID || !["live", "matched"].includes(String(response.status).toLowerCase())) {
    await cancelAllAndConfirm(client);
    throw new Error(`fail closed: canary order was not acknowledged (${response?.status || response?.errorMsg || "unknown"})`);
  }
  const acknowledgedAt = new Date();
  const acknowledgementLatencyMs = Math.max(0, performance.now() - sentMonotonicMs);
  const acknowledgedMonotonicMs = performance.now();
  const orderId = String(response.orderID);
  ledger.record("venue_order_http_ack", {
    probe_id: reservation.probe_id,
    order_id: orderId,
    response,
    wall_ms: acknowledgedAt.getTime(),
    monotonic_ms: acknowledgedMonotonicMs,
    client_to_http_ack_ms: acknowledgementLatencyMs
  });
  let markoutCapture;
  try {
    markoutCapture = beginFillMarkoutCapture(
      client,
      intent.token_id,
      () => normalizeFillClock(
        tradeFillsFromUserEvents(userChannel.messages, orderId),
        refreshed.clockServerMinusLocalMs
      ),
      {
        feeParameters: {
          rate: refreshed.feeRate,
          rateBps: refreshed.feeRateBps,
          exponent: refreshed.feeExponent,
          takerOnly: refreshed.feeTakerOnly
        }
      }
    );
    await finalizeProbeRisk(config, reservation, {
      state: "submitted_pending_reconciliation",
      order_submitted: true,
      order_id: orderId,
      matched_notional: 0,
      reconciliation_complete: false,
      zero_open_orders_confirmed: false
    });
    const plannedRestMs = Math.min(
      config.restSeconds * 1_000,
      Math.max(0, Date.parse(intent.valid_until) - Date.now())
    );
    await sleep(plannedRestMs);
    const openBeforeCancel = (await getOpenOrdersStrict(client)).some((row) => String(row.id) === orderId);
    const cancellation = openBeforeCancel
      ? await cancelOrderWithMetrics(client, orderId, ledger)
      : {
          cancelSendWallMs: null,
          cancelResponseWallMs: Date.now(),
          cancelRoundTripMs: null,
          cancelResponse: { already_terminal: true },
          failedAttempts: 0
        };
    ledger.record("venue_cancel_http_response", {
      probe_id: reservation.probe_id,
      order_id: orderId,
      response: cancellation.cancelResponse,
      client_cancel_round_trip_ms: cancellation.cancelRoundTripMs
    });
    const reconciliationStartedMonotonicMs = performance.now();
    ledger.record("funded_terminal_reconciliation_started", {
      wall_ms: Date.now(),
      monotonic_ms: reconciliationStartedMonotonicMs,
      order_id: orderId
    });
    const reconciliation = await waitForStablePostCancelReconciliation({
      client,
      conditionId: intent.condition_id,
      orderId,
      userChannel,
      ledger,
      assertHealthy: () => lease.assertHealthy()
    });
    await Promise.all([userChannel.ensureOpen(), marketChannel.ensureOpen()]);
    const userFills = normalizeFillClock(
      tradeFillsFromUserEvents(reconciliation.userEvents, orderId),
      refreshed.clockServerMinusLocalMs
    );
    const restFills = normalizeFillClock(
      tradeFillsFromRest(reconciliation.relatedTrades, orderId),
      refreshed.clockServerMinusLocalMs
    );
    const fills = mergeTradeFills(userFills, restFills);
    const markouts = await markoutCapture.finish(fills);
    const restOrderMatched = Number(reconciliation.finalOrder?.size_matched || 0);
    const userOrderMatched = maximumMatchedSize(reconciliation.userEvents);
    const restTradesMatched = sum(restFills.map((fill) => fill.size));
    const userTradesMatched = sum(userFills.map((fill) => fill.size));
    const matchedShares = Math.max(restOrderMatched, userOrderMatched, restTradesMatched, userTradesMatched);
    const matchedSizeSourceAgreement = [restOrderMatched, userOrderMatched, restTradesMatched, userTradesMatched]
      .every((value) => nearlyEqualSize(value, matchedShares));
    const tradeIdSourceAgreement = sameStringSet(restFills.map((fill) => fill.id), userFills.map((fill) => fill.id));
    const restOrderReturned = Boolean(reconciliation.finalOrder);
    const reconciliationComplete = reconciliation.zeroOpenOrders && reconciliation.stableFinality &&
      reconciliation.terminalConfirmed && restOrderReturned && matchedSizeSourceAgreement && tradeIdSourceAgreement;
    ledger.record("funded_terminal_reconciliation_completed", {
      wall_ms: Date.now(),
      monotonic_ms: performance.now(),
      duration_ms: performance.now() - reconciliationStartedMonotonicMs,
      order_id: orderId,
      reconciliation_complete: reconciliationComplete,
      zero_open_orders_confirmed: reconciliation.zeroOpenOrders
    });
    const matchedRisk = matchedShares * (Number(intent.price) +
      polymarketV2FeePerShare(intent.price, refreshed.feeRate, refreshed.feeExponent));
    await finalizeProbeRisk(config, reservation, {
      state: reconciliationComplete
        ? (matchedShares > 0 ? "position_unresolved" : "finalized_no_fill")
        : "unresolved_reconciliation",
      order_submitted: true,
      order_id: orderId,
      matched_notional: matchedRisk,
      reconciliation_complete: reconciliationComplete,
      zero_open_orders_confirmed: reconciliation.zeroOpenOrders
    });
    if (!reconciliationComplete) throw new Error("canary lifecycle did not reconcile across REST and authenticated user channel");

    const terminalAt = new Date();
    const firstFillWallMs = firstFillTimestamp(fills);
    const cancellationReceivedWallMs = cancellationEventReceivedAt(reconciliation.userEvents);
    const order = evidenceOrder(intent, refreshed.book);
    const tradeThrough = publicTradeThroughStats(
      marketChannel.messages,
      order,
      acknowledgedAt.getTime(),
      cancellationReceivedWallMs ?? cancellation.cancelResponseWallMs ?? terminalAt.getTime(),
      fills
    );
    const cancelRace = postCancelFillStats(fills, cancellation.cancelSendWallMs);
    const fullContext = marketContext(marketChannel.messages);
    const userChannelHistory = userChannel.historyStats?.();
    const marketChannelHistory = marketChannel.historyStats?.();
    const channelHistoryTruncated = userChannelHistory?.evicted_count > 0 ||
      marketChannelHistory?.evicted_count > 0;
    const dataGapDetected = !reconciliation.stableFinality ||
      userChannel.gapCount() > 0 || marketChannel.gapCount() > 0 ||
      userChannel.unparsedCount() > 0 || marketChannel.unparsedCount() > 0 ||
      channelHistoryTruncated ||
      (cancellation.cancelSendWallMs !== null && cancellationReceivedWallMs === null) ||
      (cancellation.cancelSendWallMs === null && matchedShares < Number(intent.shares)) ||
      (matchedShares > 0 && (!tradeIdSourceAgreement || !matchedSizeSourceAgreement));
    const terminalWallMs = cancellationReceivedWallMs ?? cancellation.cancelResponseWallMs ?? firstFillWallMs ?? terminalAt.getTime();
    const markoutCoverage = validateFillMarkouts(markouts, restFills.map((fill) => fill.id), matchedShares);
    const markoutCaptureComplete = markoutCoverage.complete && markoutCoverage.timing_valid;
    const lifecycle = {
      order_id: orderId,
      send_wall_ms: sentAt.getTime(),
      ack_wall_ms: acknowledgedAt.getTime(),
      submitted_ts: sentAt.toISOString(),
      acknowledged_ts: acknowledgedAt.toISOString(),
      client_to_http_ack_ms: acknowledgementLatencyMs,
      acknowledgement_latency_ms: acknowledgementLatencyMs,
      acknowledgement_latency_clock: "monotonic_performance_now",
      clock_server_minus_local_ms: refreshed.clockServerMinusLocalMs,
      clock_round_trip_ms: refreshed.clockRoundTripMs,
      clock_uncertainty_ms: refreshed.clockUncertaintyMs,
      fill_timestamp_clock: "venue_timestamp_normalized_to_local_wall_clock",
      cancel_send_wall_ms: cancellation.cancelSendWallMs,
      cancel_http_response_wall_ms: cancellation.cancelResponseWallMs,
      client_cancel_round_trip_ms: cancellation.cancelRoundTripMs,
      user_channel_cancel_received_wall_ms: cancellationReceivedWallMs,
      client_to_user_cancel_ack_ms: cancellation.cancelSendWallMs === null || cancellationReceivedWallMs === null
        ? null
        : cancellationReceivedWallMs - cancellation.cancelSendWallMs,
      cancel_requested_ts: cancellation.cancelSendWallMs === null ? null : new Date(cancellation.cancelSendWallMs).toISOString(),
      cancel_acknowledged_ts: cancellation.cancelSendWallMs === null
        ? null
        : new Date(cancellation.cancelResponseWallMs).toISOString(),
      cancel_failed_attempts: cancellation.failedAttempts,
      planned_rest_seconds: plannedRestMs / 1_000,
      planned_rest_until_ts: intent.valid_until,
      live_duration_ms: Math.max(0, terminalWallMs - acknowledgedAt.getTime()),
      first_fill_after_ack_ms: firstFillWallMs === null ? null : Math.max(0, firstFillWallMs - acknowledgedAt.getTime()),
      actual_matched_size: matchedShares,
      partial_fill: matchedShares > 0 && matchedShares < Number(intent.shares),
      fully_filled: matchedShares >= Number(intent.shares),
      post_cancel_fill_count: cancelRace.postCancelFillCount,
      first_fill_after_cancel_ms: cancelRace.firstFillAfterCancelMs,
      fill_raced_cancellation: cancelRace.postCancelFillCount > 0,
      public_touch_trade_count: tradeThrough.touch_count,
      public_strict_trade_through_count: tradeThrough.strict_trade_through_count,
      public_trade_through_without_fill_count: tradeThrough.trade_through_without_fill_count,
      venue_status: reconciliation.finalOrder?.status || "terminal_not_returned",
      venue_fee_model: refreshed.feeModel,
      venue_fee_rate: refreshed.feeRate,
      venue_fee_rate_bps: Number(refreshed.feeRateBps || 0),
      venue_fee_exponent: refreshed.feeExponent,
      venue_fee_taker_only: refreshed.feeTakerOnly,
      related_trade_ids: restFills.map((fill) => fill.id),
      live_user_trade_ids: userFills.map((fill) => fill.id),
      rest_order_matched_size: restOrderMatched,
      user_order_matched_size: userOrderMatched,
      rest_trade_matched_size: restTradesMatched,
      user_trade_matched_size: userTradesMatched,
      matched_size_source_agreement: matchedSizeSourceAgreement,
      trade_id_source_agreement: tradeIdSourceAgreement,
      rest_user_trade_ids_agree: tradeIdSourceAgreement,
      rest_order_returned: restOrderReturned,
      post_cancel_finality_stable: reconciliation.stableFinality,
      post_cancel_observation_ms: reconciliation.observationMs,
      authenticated_user_channel_reconnects: userChannel.reconnectCount(),
      public_market_channel_reconnects: marketChannel.reconnectCount(),
      authenticated_user_channel_duplicates: userChannel.duplicateCount(),
      public_market_channel_duplicates: marketChannel.duplicateCount(),
      authenticated_user_channel_unparsed: userChannel.unparsedCount(),
      public_market_channel_unparsed: marketChannel.unparsedCount(),
      authenticated_user_channel_history_evictions: userChannelHistory?.evicted_count || 0,
      public_market_channel_history_evictions: marketChannelHistory?.evicted_count || 0,
      reconciliation_complete: reconciliationComplete,
      zero_open_orders_confirmed: reconciliation.zeroOpenOrders,
      data_gap_detected: dataGapDetected,
      cancellation_failure: cancellation.failedAttempts > 0 && !reconciliation.zeroOpenOrders,
      markout_capture_complete: markoutCaptureComplete,
      public_trade_messages: marketChannel.messages.filter((row) => String(row.event_type || row.type).toLowerCase().includes("trade")).length
    };
    const market = {
      id: String(refreshed.market.marketId),
      conditionId: String(refreshed.market.conditionId),
      tokenId: String(refreshed.market.tokenId),
      endTs: refreshed.market.endTs || null
    };
    const observations = modelObservations({ order, market, lifecycle, context: preSendContext, markouts });
    lifecycle.estimated_round_trip_cost_per_share = observations[0]?.estimated_round_trip_cost_per_share ?? null;
    const exactEligibleHorizons = hasExactEligibleHorizons(observations, HORIZONS_SECONDS);
    const evidenceStatus = !dataGapDetected && markoutCaptureComplete && exactEligibleHorizons
      ? "completed"
      : "completed_ineligible";
    const evidenceProbe = {
      schema_version: 3,
      evidence_protocol_version: EVIDENCE_PROTOCOL_VERSION,
      probe_id: reservation.probe_id,
      status: evidenceStatus,
      started_ts: sentAt.toISOString(),
      finished_ts: terminalAt.toISOString(),
      order_submitted: true,
      market,
      order,
      context: fullContext,
      pre_send_context: preSendContext,
      lifecycle,
      markouts,
      model_observations: observations
    };
    return {
      ...lifecycle,
      fills: fills.map((fill) => ({ ...fill, markouts: markouts.filter((row) => row.fill_id === fill.id) })),
      evidence_probe: evidenceProbe
    };
  } catch (error) {
    if (markoutCapture) await markoutCapture.abort().catch(() => null);
    const emergency = await emergencyReconcileAfterAck(client, intent.condition_id, orderId);
    const matchedRisk = emergency.matchedShares * (Number(intent.price) +
      polymarketV2FeePerShare(intent.price, refreshed.feeRate, refreshed.feeExponent));
    let reservationPersistenceError = null;
    try {
      await finalizeProbeRisk(config, reservation, {
        state: "unresolved_reconciliation",
        order_submitted: true,
        order_id: orderId,
        matched_notional: matchedRisk,
        reconciliation_complete: false,
        zero_open_orders_confirmed: emergency.zeroOpenOrders
      });
    } catch (persistenceError) {
      reservationPersistenceError = persistenceError;
    }
    ledger.record("strategy_canary_post_ack_failed_closed", {
      probe_id: reservation.probe_id,
      order_id: orderId,
      zero_open_orders_confirmed: emergency.zeroOpenOrders,
      matched_shares: emergency.matchedShares,
      error: error.message
    });
    await uploadFailedPostAckEvidence({
      intent,
      runtime: refreshed,
      reservation,
      orderId,
      acknowledgedAt,
      sentAt,
      acknowledgementLatencyMs,
      preSendContext,
      emergency,
      originalError: error
    }).catch((uploadError) => {
      ledger.record("strategy_canary_failed_evidence_upload", { error: uploadError.message });
    });
    const failure = reservationPersistenceError
      ? new Error(`fail closed: post-ack error and unresolved reservation persistence failed; prior durable reservation remains blocking (${error.message}; ${reservationPersistenceError.message})`)
      : !emergency.zeroOpenOrders
        ? new Error(`fail closed: post-ack error and emergency zero-open confirmation failed; unresolved risk preserved (${error.message})`)
        : new Error(`fail closed: post-ack error; tracked order canceled, zero open orders confirmed, unresolved risk preserved (${error.message})`);
    failure.executionEvidence = {
      status: "post_submission_unresolved",
      order_submission_attempted: true,
      order_submitted: true,
      lifecycle: {
        order_id: orderId,
        send_wall_ms: sentAt.getTime(),
        ack_wall_ms: acknowledgedAt.getTime(),
        client_to_http_ack_ms: acknowledgementLatencyMs,
        matched_notional: matchedRisk,
        reconciliation_complete: false,
        zero_open_orders_confirmed: emergency.zeroOpenOrders
      }
    };
    throw failure;
  }
}

async function captureFinalGate(client, intent, runtime) {
  const gateStartedWallMs = Date.now();
  const gateStartedMonotonicMs = performance.now();
  const clock = async () => {
    const requestStarted = Date.now();
    const serverTimeResponse = await client.getServerTime();
    const requestFinished = Date.now();
    const serverValue = Number(serverTimeResponse?.server_time ?? serverTimeResponse?.time ?? serverTimeResponse);
    const serverMs = serverValue < 1e12 ? serverValue * 1000 : serverValue;
    const clockRoundTripMs = requestFinished - requestStarted;
    const localMidpointMs = (requestStarted + requestFinished) / 2;
    const clockServerMinusLocalMs = serverMs - localMidpointMs;
    const serverClockQuantizationMs = serverValue < 1e12 && Number.isInteger(serverValue) ? 500 : 1;
    return {
      clockDriftMs: Math.abs(clockServerMinusLocalMs),
      clockServerMinusLocalMs,
      clockRoundTripMs,
      clockUncertaintyMs: clockRoundTripMs / 2 + serverClockQuantizationMs
    };
  };
  const [book, openOrders, clockEvidence] = await Promise.all([
    client.getOrderBook(String(intent.token_id)),
    getOpenOrdersStrict(client),
    clock(),
    userChannel.forceHeartbeat(),
    marketChannel.forceHeartbeat()
  ]);
  if (openOrders.length !== 0) throw new Error("fail closed: final volatile gate found an open order");
  const streamEvidence = streamBookEvidence(marketChannel.messages, intent.token_id);
  if (!streamEvidence) throw new Error("fail closed: final volatile gate lacks public stream top-of-book evidence");
  const restBestAsk = Math.min(...(book.asks || []).map((row) => Number(row.price)).filter(Number.isFinite));
  const streamBestAsk = Number(streamEvidence.bestAsk);
  const restTick = Number(book.tick_size ?? book.tickSize);
  if (![restBestAsk, streamBestAsk, restTick].every(Number.isFinite) ||
      Math.abs(restBestAsk - streamBestAsk) > 1e-9) {
    throw new Error("fail closed: REST and public stream book disagree at the final volatile gate");
  }
  const finalGateCompletedWallMs = Date.now();
  const finalGateDurationMs = Math.max(0, performance.now() - gateStartedMonotonicMs);
  ledger?.record("funded_final_volatile_gate_completed", {
    wall_ms: finalGateCompletedWallMs,
    monotonic_ms: performance.now(),
    duration_ms: finalGateDurationMs,
    decision_id: intent.decision_id,
    rest_best_ask: restBestAsk,
    stream_best_ask: streamBestAsk,
    open_order_count: 0
  });
  return {
    ...runtime,
    ...clockEvidence,
    book,
    openOrderCount: 0,
    finalGateStartedWallMs: gateStartedWallMs,
    finalGateCompletedWallMs,
    finalGateDurationMs
  };
}

export function streamBookEvidence(messages, tokenId) {
  const expectedToken = String(tokenId);
  for (const message of [...(messages || [])].reverse()) {
    const type = String(message?.event_type || message?.type || "").toLowerCase();
    if (type === "best_bid_ask" &&
        String(message?.asset_id || message?.token_id || message?.tokenId || "") === expectedToken) {
      const bestAsk = Number(message.best_ask ?? message.bestAsk);
      if (Number.isFinite(bestAsk)) return { bestAsk, source: type, receivedWallMs: Number(message._received_wall_ms) };
    }
    if (type === "price_change") {
      const change = [...(message.price_changes || [])].reverse().find((value) =>
        String(value?.asset_id || value?.token_id || value?.tokenId || "") === expectedToken
      );
      const bestAsk = Number(change?.best_ask ?? change?.bestAsk);
      if (Number.isFinite(bestAsk)) return { bestAsk, source: type, receivedWallMs: Number(message._received_wall_ms) };
    }
    if (type === "book" &&
        String(message?.asset_id || message?.token_id || message?.tokenId || "") === expectedToken) {
      const bestAsk = Math.min(...(message.asks || []).map((row) => Number(row.price)).filter(Number.isFinite));
      if (Number.isFinite(bestAsk)) return { bestAsk, source: type, receivedWallMs: Number(message._received_wall_ms) };
    }
  }
  return null;
}

export function assertPersistentIntentRemainingTtl(intent, minimumMs, nowMs = Date.now()) {
  const minimum = Number(minimumMs);
  const remaining = Date.parse(intent?.valid_until) - nowMs;
  if (!Number.isFinite(minimum) || minimum < 1_000 ||
      !Number.isFinite(remaining) || remaining < minimum) {
    throw new Error(`fail closed: persistent executor has less than ${minimum}ms of intent TTL before signing`);
  }
  return remaining;
}

async function uploadFailedPostAckEvidence({ intent, runtime, reservation, orderId, acknowledgedAt, sentAt, acknowledgementLatencyMs, preSendContext, emergency, originalError }) {
  const finishedAt = new Date();
  const context = marketChannel ? marketContext(marketChannel.messages) : {
    observed_trade_count: 0,
    observed_trade_size: 0,
    observed_depth_changes: 0,
    price_volatility: 0
  };
  const order = evidenceOrder(intent, runtime.book);
  const lifecycle = {
    order_id: orderId,
    send_wall_ms: sentAt.getTime(),
    ack_wall_ms: acknowledgedAt.getTime(),
    submitted_ts: sentAt.toISOString(),
    acknowledged_ts: acknowledgedAt.toISOString(),
    client_to_http_ack_ms: acknowledgementLatencyMs,
    acknowledgement_latency_ms: acknowledgementLatencyMs,
    live_duration_ms: Math.max(0, finishedAt.getTime() - acknowledgedAt.getTime()),
    first_fill_after_ack_ms: null,
    actual_matched_size: emergency.matchedShares,
    related_trade_ids: [],
    venue_fee_model: runtime.feeModel,
    venue_fee_rate: runtime.feeRate,
    venue_fee_rate_bps: Number(runtime.feeRateBps || 0),
    venue_fee_exponent: runtime.feeExponent,
    venue_fee_taker_only: runtime.feeTakerOnly,
    reconciliation_complete: false,
    zero_open_orders_confirmed: emergency.zeroOpenOrders,
    data_gap_detected: true,
    cancellation_failure: !emergency.zeroOpenOrders
  };
  const market = {
    id: String(runtime.market.marketId),
    conditionId: String(runtime.market.conditionId),
    tokenId: String(runtime.market.tokenId),
    endTs: runtime.market.endTs || null
  };
  const observations = modelObservations({ order, market, lifecycle, context: preSendContext, markouts: [] });
  lifecycle.estimated_round_trip_cost_per_share = observations[0]?.estimated_round_trip_cost_per_share ?? null;
  const probe = {
    schema_version: 3,
    evidence_protocol_version: EVIDENCE_PROTOCOL_VERSION,
    probe_id: reservation.probe_id,
    status: "completed_ineligible",
    started_ts: sentAt.toISOString(),
    finished_ts: finishedAt.toISOString(),
    order_submitted: true,
    market,
    order,
    context,
    pre_send_context: preSendContext,
    lifecycle,
    markouts: [],
    model_observations: observations,
    error: originalError.message
  };
  const summary = {
    schema_version: 3,
    evidence_protocol_version: EVIDENCE_PROTOCOL_VERSION,
    run_id: runId,
    status: "failed_closed",
    started_ts: sentAt.toISOString(),
    finished_ts: finishedAt.toISOString(),
    order_submission_attempted: true,
    order_submitted: true,
    submitted_order_count: 1,
    completed_probe_count: 0,
    probes: [probe],
    market,
    order,
    pre_send_context: preSendContext,
    lifecycle,
    markouts: [],
    model_observations: observations,
    queue_position_source: "authenticated_lifecycle_plus_public_l2",
    queue_position_metric: "inferred_size_ahead",
    literal_fifo_rank_available: false,
    research_only: !config.operatorDirect,
    live_trading_enabled: config.operatorDirect,
    evidence_trust_boundary_ready: config.trustBoundaryReady
  };
  return uploadEvidence(config, runId, summary, ledger);
}

function evidenceOrder(intent, book) {
  const price = Number(intent.price);
  const bids = (book.bids || []).map((row) => ({ price: Number(row.price), size: Number(row.size) }));
  const asks = (book.asks || []).map((row) => ({ price: Number(row.price), size: Number(row.size) }));
  const bestBid = bids.length ? Math.max(...bids.map((row) => row.price)) : null;
  const bestAsk = asks.length ? Math.min(...asks.map((row) => row.price)) : null;
  const samePrice = bids.filter((row) => row.price === price).reduce((sum, row) => sum + row.size, 0);
  const betterPrice = bids.filter((row) => row.price > price).reduce((sum, row) => sum + row.size, 0);
  return {
    side: "BUY",
    price,
    size: Number(intent.shares),
    notional: Number(intent.notional),
    source_requested_size: Number(intent.source_requested_shares ?? intent.shares),
    source_requested_notional: Number(intent.source_requested_notional ?? intent.notional),
    current_funds_scaled: intent.current_funds_scaled === true,
    post_only: true,
    spread: bestBid === null || bestAsk === null ? null : bestAsk - bestBid,
    samePricePublicSize: samePrice,
    betterPricePublicSize: betterPrice,
    inferredSizeAhead: samePrice + betterPrice,
    minimumOrderSize: Number(intent.minimum_order_size)
  };
}

async function emergencyReconcileAfterAck(client, conditionId, orderId) {
  await client.cancelOrder({ orderID: orderId }).catch(() => null);
  await client.cancelAll().catch(() => null);
  const reconciliation = await waitForStablePostCancelReconciliation({
    client,
    conditionId,
    orderId,
    userChannel: userChannel || { messages: [], ensureOpen: async () => true },
    ledger
  }).catch(() => null);
  const openOrders = await getOpenOrdersStrict(client).catch(() => null);
  const zeroOpenOrders = Array.isArray(openOrders) && openOrders.length === 0;
  const restFills = reconciliation ? tradeFillsFromRest(reconciliation.relatedTrades, orderId) : [];
  const matchedShares = Math.max(
    Number(reconciliation?.finalOrder?.size_matched || 0),
    restFills.reduce((sum, fill) => sum + fill.size, 0)
  );
  return { zeroOpenOrders, matchedShares };
}

async function loadExactMarket(intent) {
  const values = await fetchJson(`${config.gammaUrl}/markets?id=${encodeURIComponent(intent.market_id)}`);
  const market = Array.isArray(values) ? values[0] : null;
  if (!market) throw new Error("fail closed: intent market was not found at the venue");
  const tokenIds = parseArray(market.clobTokenIds).map(String);
  if (!tokenIds.includes(String(intent.token_id))) throw new Error("fail closed: intent token is not part of the venue market");
  return {
    marketId: String(market.id),
    conditionId: String(market.conditionId),
    tokenId: String(intent.token_id),
    endTs: market.endDate || market.end_date || null,
    closed: market.closed === true,
    acceptingOrders: market.acceptingOrders !== false && market.enableOrderBook !== false
  };
}

async function cancelAllAndConfirm(client) {
  await client.cancelAll().catch(() => null);
  if ((await getOpenOrdersStrict(client)).length) throw new Error("fail closed: emergency cancellation did not produce zero open orders");
}

async function releaseDeterministicRejectedOrder(client, {
  intent,
  manifest,
  reservation,
  sentAt,
  rejection
}) {
  await cancelAllAndConfirm(client);
  await sleep(250);
  await userChannel.ensureOpen();
  const postSendTrades = userChannel.messages.filter((message) =>
    Number(message?._received_wall_ms) >= sentAt.getTime() &&
    String(message?.event_type || message?.type || "").toLowerCase().includes("trade")
  );
  if (userChannel.gapCount() > 0 || userChannel.unparsedCount() > 0 || postSendTrades.length > 0) {
    throw new Error("fail closed: deterministic venue rejection could not prove an authenticated zero-fill window; risk remains reserved");
  }
  const reconciled = await capturePreflight(client, intent, manifest, reservation.probe_id);
  validateDeterministicNoOrderReconciliation({
    error: rejection.message,
    openOrderCount: reconciled.openOrderCount,
    unresolvedPositionCount: reconciled.risk?.unresolved_position_count,
    userChannelGapCount: userChannel.gapCount(),
    userChannelUnparsedCount: userChannel.unparsedCount(),
    postSendTradeCount: postSendTrades.length
  });
  ledger.record("venue_order_rejected_no_order", {
    probe_id: reservation.probe_id,
    rejection_code: rejection.code,
    venue_message: rejection.message,
    zero_open_orders_confirmed: true,
    zero_unresolved_positions_confirmed: true
  });
  await finalizeProbeRisk(config, reservation, {
    state: "released_no_order",
    order_submitted: false,
    matched_notional: 0,
    reconciliation_complete: true,
    zero_open_orders_confirmed: true,
    reconciliation_reason: rejection.code,
    reconciliation_evidence: {
      source: "authenticated_clob_and_user_channel",
      zero_open_orders: true,
      zero_unresolved_positions: true,
      post_send_authenticated_trade_count: 0
    }
  });
}

async function getOpenOrdersStrict(client) {
  const value = await client.getOpenOrders();
  if (!Array.isArray(value)) throw new Error("fail closed: venue open-order response is invalid");
  return value;
}

function normalizeFillClock(fills, serverMinusLocalMs) {
  if (!Number.isFinite(Number(serverMinusLocalMs))) {
    throw new Error("fail closed: signed venue clock offset is unavailable for authenticated fills");
  }
  return (fills || []).map((fill) => {
    const venueTimestampMs = Number(fill.timestampMs);
    if (!Number.isFinite(venueTimestampMs)) {
      throw new Error("fail closed: authenticated fill timestamp is invalid");
    }
    return {
      ...fill,
      venueTimestampMs,
      timestampMs: venueTimestampMs - Number(serverMinusLocalMs)
    };
  });
}

function normalizePrivateKey(value) { const clean = String(value || "").trim(); return clean.startsWith("0x") ? clean : `0x${clean}`; }
function parseArray(value) { if (Array.isArray(value)) return value; try { return JSON.parse(value || "[]"); } catch { return []; } }
async function fetchJson(url) { const response = await fetch(url, { signal: AbortSignal.timeout(10_000) }); if (!response.ok) throw new Error(`HTTP ${response.status} from ${url}`); return response.json(); }

export async function loadSettlementActivity({
  user,
  conditionIds,
  sessionStartedAt,
  fetcher = fetchJson,
  pageSize = 500,
  conditionBatchSize = 25,
  maxPagesPerBatch = null
}) {
  const wallet = normalizedAddress(user);
  const startedMs = activityTimestampMs(sessionStartedAt);
  const conditions = [...new Set((conditionIds || []).map(normalizedBytes32).filter(Boolean))].sort();
  const apiMaxOffset = 5_000;
  const apiPageBound = Math.floor(apiMaxOffset / pageSize) + 1;
  const pageBound = maxPagesPerBatch === null ? apiPageBound : maxPagesPerBatch;
  if (!wallet || !Number.isFinite(startedMs) || startedMs <= 0 || !conditions.length) {
    throw new Error("fail closed: settlement activity query binding is invalid");
  }
  if (!Number.isInteger(pageSize) || pageSize < 1 || pageSize > 500
      || !Number.isInteger(conditionBatchSize) || conditionBatchSize < 1 || conditionBatchSize > 50
      || !Number.isInteger(pageBound) || pageBound < 1 || pageBound > apiPageBound) {
    throw new Error("fail closed: settlement activity pagination bounds are invalid");
  }
  const values = [];
  const identities = new Set();
  const batches = [];
  for (let index = 0; index < conditions.length; index += conditionBatchSize) {
    batches.push(conditions.slice(index, index + conditionBatchSize));
  }
  await Promise.all(batches.map(async (batch) => {
    const batchConditions = new Set(batch);
    let offset = 0;
    let complete = false;
    for (let page = 0; page < pageBound; page += 1) {
      const url = new URL("https://data-api.polymarket.com/activity");
      url.searchParams.set("user", wallet);
      url.searchParams.set("market", batch.join(","));
      url.searchParams.set("type", "TRADE,REDEEM");
      url.searchParams.set("start", String(Math.floor(startedMs / 1_000)));
      url.searchParams.set("sortBy", "TIMESTAMP");
      url.searchParams.set("sortDirection", "ASC");
      url.searchParams.set("limit", String(pageSize));
      url.searchParams.set("offset", String(offset));
      const rows = await fetcher(url.toString());
      if (!Array.isArray(rows)) {
        throw new Error("fail closed: settlement activity page is invalid");
      }
      for (const row of rows) {
        const conditionId = normalizedBytes32(row?.conditionId);
        if (!batchConditions.has(conditionId)
            || activityTimestampMs(row?.timestamp) < startedMs
            || !["TRADE", "REDEEM"].includes(String(row?.type || "").toUpperCase())) {
          continue;
        }
        const identity = [
          String(row.type || "").toUpperCase(),
          normalizedBytes32(row.conditionId),
          normalizedBytes32(row.transactionHash),
          String(row.asset || ""),
          normalizedAddress(row.proxyWallet),
          String(row.timestamp ?? ""),
          String(row.size ?? ""),
          String(row.usdcSize ?? "")
        ].join("\u0000");
        if (identities.has(identity)) {
          throw new Error("fail closed: settlement activity pagination returned duplicate evidence");
        }
        identities.add(identity);
        values.push(row);
      }
      if (rows.length < pageSize) {
        complete = true;
        break;
      }
      offset += pageSize;
    }
    if (!complete) {
      throw new Error("fail closed: settlement activity history exceeds pagination bound");
    }
  }));
  return values.sort((left, right) =>
    activityTimestampMs(left?.timestamp) - activityTimestampMs(right?.timestamp)
      || String(left?.transactionHash || "").localeCompare(String(right?.transactionHash || ""))
      || String(left?.type || "").localeCompare(String(right?.type || ""))
  );
}

async function confirmedPolygonReceipt(transactionHash) {
  const expectedHash = normalizedBytes32(transactionHash);
  if (!expectedHash) {
    throw new Error("fail closed: Polygon profit-settlement transaction hash is invalid");
  }
  const [receipt, latestBlock] = await Promise.all([
    polygonRpc("eth_getTransactionReceipt", [expectedHash]),
    polygonRpc("eth_blockNumber", [])
  ]);
  if (!receipt?.blockNumber || !latestBlock
      || normalizedBytes32(receipt.transactionHash) !== expectedHash) {
    throw new Error("fail closed: Polygon profit-settlement receipt is unavailable");
  }
  const receiptBlock = BigInt(receipt.blockNumber);
  const head = BigInt(latestBlock);
  const settlementEvidence = decodeSettlementReceiptEvidence(receipt, expectedHash);
  return {
    status: receipt.status === "0x1" ? "success" : "failed",
    chain_id: 137,
    transaction_hash: expectedHash,
    block_number: receiptBlock.toString(),
    confirmations: Number(head >= receiptBlock ? head - receiptBlock + 1n : 0n),
    ...settlementEvidence
  };
}

export function decodeSettlementReceiptEvidence(
  receipt,
  transactionHash = receipt?.transactionHash
) {
  const hash = normalizedBytes32(transactionHash);
  if (!hash || normalizedBytes32(receipt?.transactionHash) !== hash
      || !Array.isArray(receipt?.logs)) {
    throw new Error("fail closed: Polygon redemption receipt evidence is invalid");
  }
  const redemptions = [];
  const ctfTransfers = [];
  const erc20Transfers = [];
  const collateralWraps = [];
  for (const log of receipt.logs) {
    const contractAddress = normalizedAddress(log?.address);
    const topic = String(log?.topics?.[0] || "").toLowerCase();
    if (contractAddress === CONDITIONAL_TOKENS_ADDRESS
        && topic === PAYOUT_REDEMPTION_TOPIC) {
      const args = decodeReceiptEvent(log, PAYOUT_REDEMPTION_EVENT).args || {};
      redemptions.push({
        contract_address: CONDITIONAL_TOKENS_ADDRESS,
        transaction_hash: hash,
        redeemer: normalizedAddress(args.redeemer),
        collateral_token: normalizedAddress(args.collateralToken),
        parent_collection_id: normalizedBytes32(args.parentCollectionId),
        condition_id: normalizedBytes32(args.conditionId),
        index_sets: (args.indexSets || []).map((value) => String(value)),
        payout_base_units: String(args.payout),
        payout: Number(formatUnits(args.payout, 6))
      });
      continue;
    }
    if (contractAddress === CONDITIONAL_TOKENS_ADDRESS
        && topic === ERC1155_TRANSFER_BATCH_TOPIC) {
      const args = decodeReceiptEvent(log, ERC1155_TRANSFER_BATCH_EVENT).args || {};
      ctfTransfers.push({
        event: "TransferBatch",
        contract_address: CONDITIONAL_TOKENS_ADDRESS,
        operator: normalizedAddress(args.operator),
        from: normalizedAddress(args.from),
        to: normalizedAddress(args.to),
        ids: (args.ids || []).map((value) => String(value)),
        values: (args.values || []).map((value) => String(value))
      });
      continue;
    }
    if (contractAddress === CONDITIONAL_TOKENS_ADDRESS
        && topic === ERC1155_TRANSFER_SINGLE_TOPIC) {
      const args = decodeReceiptEvent(log, ERC1155_TRANSFER_SINGLE_EVENT).args || {};
      ctfTransfers.push({
        event: "TransferSingle",
        contract_address: CONDITIONAL_TOKENS_ADDRESS,
        operator: normalizedAddress(args.operator),
        from: normalizedAddress(args.from),
        to: normalizedAddress(args.to),
        ids: [String(args.id)],
        values: [String(args.value)]
      });
      continue;
    }
    if ([USDCE_ADDRESS, PUSD_ADDRESS].includes(contractAddress)
        && topic === ERC20_TRANSFER_TOPIC) {
      const args = decodeReceiptEvent(log, ERC20_TRANSFER_EVENT).args || {};
      erc20Transfers.push({
        token: contractAddress,
        from: normalizedAddress(args.from),
        to: normalizedAddress(args.to),
        value_base_units: String(args.value)
      });
      continue;
    }
    if (contractAddress === PUSD_ADDRESS && topic === COLLATERAL_WRAPPED_TOPIC) {
      const args = decodeReceiptEvent(log, COLLATERAL_WRAPPED_EVENT).args || {};
      collateralWraps.push({
        contract_address: PUSD_ADDRESS,
        caller: normalizedAddress(args.caller),
        asset: normalizedAddress(args.asset),
        to: normalizedAddress(args.to),
        amount_base_units: String(args.amount)
      });
    }
  }
  return {
    redemptions,
    ctf_transfers: ctfTransfers,
    erc20_transfers: erc20Transfers,
    collateral_wraps: collateralWraps
  };
}

export function decodePayoutRedemptions(receipt, transactionHash = receipt?.transactionHash) {
  return decodeSettlementReceiptEvidence(receipt, transactionHash).redemptions;
}

function decodeReceiptEvent(log, abi) {
  return decodeEventLog({
    abi,
    data: log.data,
    topics: log.topics,
    strict: true
  });
}

async function polygonRpc(method, params) {
  for (const url of [
    "https://polygon.drpc.org",
    "https://tenderly.rpc.polygon.community",
    "https://polygon.publicnode.com"
  ]) {
    try {
      const response = await fetch(url, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }),
        signal: AbortSignal.timeout(10_000)
      });
      if (!response.ok) continue;
      const payload = await response.json();
      if (!payload?.error && payload?.result !== undefined && payload.result !== null) {
        return payload.result;
      }
    } catch {
      // Try the next independent endpoint. All failures remain fail-closed.
    }
  }
  throw new Error(`fail closed: Polygon RPC ${method} failed`);
}
function sleep(ms) { return new Promise((resolve) => setTimeout(resolve, ms)); }

function normalizedAddress(value) {
  const text = String(value || "").trim().toLowerCase();
  return /^0x[0-9a-f]{40}$/.test(text) ? text : "";
}

function normalizedBytes32(value) {
  const text = String(value || "").trim().toLowerCase();
  return /^0x[0-9a-f]{64}$/.test(text) ? text : "";
}

function activityTimestampMs(value) {
  if (typeof value === "string" && /[T:-]/.test(value)) return Date.parse(value);
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return NaN;
  return parsed < 1e12 ? parsed * 1_000 : parsed;
}

const isMain = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isMain) {
  runCanaryOnce().then((result) => {
    console.log(JSON.stringify(result));
  }).catch((error) => {
    process.exitCode = 1;
    console.error(JSON.stringify({
      schema: "polyedge.strategy_canary_run.v1",
      run_id: runId || null,
      status: "failed_closed",
      order_submission_attempted: orderSubmissionAttempted,
      error: error.message
    }));
  });
}
