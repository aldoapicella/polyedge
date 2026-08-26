import WebSocket from "ws";
import { createHash } from "node:crypto";

const TERMINAL_ORDER_STATES = ["CANCELED", "CANCELLED", "MATCHED", "FILLED", "EXPIRED"];
const DEFAULT_MAX_MESSAGE_HISTORY = 32_768;
const DEFAULT_MAX_MESSAGE_HISTORY_BYTES = 64 * 1024 * 1024;
const DEFAULT_EVIDENCE_BASELINE_MESSAGES = 4_096;
const DEFAULT_EVIDENCE_BASELINE_BYTES = 8 * 1024 * 1024;

function unparsedFrameMetadata(frame, isBinary, receivedWallMs) {
  const hash = createHash("sha256");
  let byteLength = 0;
  for (const chunk of Array.isArray(frame) ? frame : [frame]) {
    const bytes = Buffer.isBuffer(chunk)
      ? chunk
      : chunk instanceof ArrayBuffer
        ? Buffer.from(chunk)
        : ArrayBuffer.isView(chunk)
          ? Buffer.from(chunk.buffer, chunk.byteOffset, chunk.byteLength)
          : Buffer.from(String(chunk), "utf8");
    byteLength += bytes.byteLength;
    hash.update(bytes);
  }
  return {
    received_wall_ms: receivedWallMs,
    byte_length: byteLength,
    sha256: `sha256:${hash.digest("hex")}`,
    classification: isBinary === true ? "binary" : "text"
  };
}

export async function connectLifecycleChannel({
  url,
  subscription,
  ledger,
  eventType,
  recordMessages = true,
  maxMessageHistory = DEFAULT_MAX_MESSAGE_HISTORY,
  maxMessageHistoryBytes = DEFAULT_MAX_MESSAGE_HISTORY_BYTES,
  evidenceBaselineMessages = DEFAULT_EVIDENCE_BASELINE_MESSAGES,
  evidenceBaselineBytes = DEFAULT_EVIDENCE_BASELINE_BYTES,
  WebSocketImpl = WebSocket,
  reconnectAttempts = 5,
  heartbeatIntervalMs = 8_000,
  heartbeatTimeoutMs = 8_000,
  openTimeoutMs = 8_000,
  settleMs = 250,
  sleep = defaultSleep,
  nowMs = Date.now,
  setTimer = setTimeout,
  clearTimer = clearTimeout
}) {
  const configuredHeartbeatIntervalMs = Number(heartbeatIntervalMs);
  if (!Number.isFinite(configuredHeartbeatIntervalMs) || configuredHeartbeatIntervalMs <= 0) {
    throw new Error("fail closed: websocket heartbeat interval must be positive");
  }
  const heartbeatEveryMs = Math.min(10_000, configuredHeartbeatIntervalMs);
  const configuredHeartbeatTimeoutMs = Number(heartbeatTimeoutMs);
  if (!Number.isFinite(configuredHeartbeatTimeoutMs) || configuredHeartbeatTimeoutMs <= 0) {
    throw new Error("fail closed: websocket heartbeat timeout must be positive");
  }
  const historyLimit = positiveInteger(maxMessageHistory, "websocket message history limit");
  const historyByteLimit = positiveInteger(maxMessageHistoryBytes, "websocket message history byte limit");
  const evidenceBaselineLimit = Math.min(
    historyLimit,
    positiveInteger(evidenceBaselineMessages, "websocket evidence baseline message limit")
  );
  const evidenceBaselineByteLimit = Math.min(
    historyByteLimit,
    positiveInteger(evidenceBaselineBytes, "websocket evidence baseline byte limit")
  );
  const heartbeatResponseTimeoutMs = Math.min(heartbeatEveryMs, configuredHeartbeatTimeoutMs);
  let open = false;
  let stopped = false;
  let socket = null;
  let socketState = null;
  let gaps = 0;
  let reconnects = 0;
  let duplicates = 0;
  let unparsed = 0;
  let latestUnparsedFrameMetadata = null;
  let reconnectPromise = null;
  let requiresReconciliation = false;
  let currentSubscription = structuredClone(subscription);
  const messages = [];
  const fingerprints = new Set();
  const messageMetadata = new WeakMap();
  let messageHistoryBytes = 0;
  let historyEvictions = 0;

  function appendMessage(message, fingerprint = null) {
    const bytes = fingerprint ? Buffer.byteLength(fingerprint, "utf8") : 64;
    messages.push(message);
    messageMetadata.set(message, { fingerprint, bytes });
    if (fingerprint) fingerprints.add(fingerprint);
    messageHistoryBytes += bytes;
    trimMessageHistory();
  }

  function trimMessageHistory() {
    if (messages.length <= historyLimit && messageHistoryBytes <= historyByteLimit) return;
    const targetLength = Math.max(1, Math.floor(historyLimit * 0.75));
    const targetBytes = Math.max(1, Math.floor(historyByteLimit * 0.75));
    let count = 0;
    let releasedBytes = 0;
    while (count < messages.length &&
      (messages.length - count > targetLength || messageHistoryBytes - releasedBytes > targetBytes)) {
      releasedBytes += messageMetadata.get(messages[count])?.bytes || 0;
      count += 1;
    }
    releaseOldest(count, releasedBytes, true);
  }

  function releaseOldest(count, releasedBytes, trackEvictions) {
    const evicted = messages.splice(0, count);
    for (const message of evicted) {
      const fingerprint = messageMetadata.get(message)?.fingerprint;
      if (fingerprint) fingerprints.delete(fingerprint);
    }
    if (trackEvictions) historyEvictions += evicted.length;
    messageHistoryBytes = Math.max(0, messageHistoryBytes - releasedBytes);
  }

  function clearHistory() {
    messages.length = 0;
    fingerprints.clear();
    messageHistoryBytes = 0;
    historyEvictions = 0;
  }

  function beginEvidenceWindow() {
    let count = 0;
    let releasedBytes = 0;
    while (count < messages.length &&
      (messages.length - count > evidenceBaselineLimit ||
        messageHistoryBytes - releasedBytes > evidenceBaselineByteLimit)) {
      releasedBytes += messageMetadata.get(messages[count])?.bytes || 0;
      count += 1;
    }
    if (count > 0) releaseOldest(count, releasedBytes, false);
    historyEvictions = 0;
  }

  function clearSocketHeartbeat(state, error = null) {
    if (!state) return;
    if (state.heartbeatTimer !== null) {
      clearTimer(state.heartbeatTimer);
      state.heartbeatTimer = null;
    }
    if (state.pendingPong) {
      const pending = state.pendingPong;
      state.pendingPong = null;
      clearTimer(pending.timeoutTimer);
      if (error) pending.reject(error);
    }
  }

  function handleSocketClose(state, code, reason, countGap = state.ready) {
    if (state.closeHandled) return;
    state.closeHandled = true;
    state.ready = false;
    clearSocketHeartbeat(state, new Error("fail closed: websocket closed while awaiting fresh PONG"));
    ledger?.record(`${eventType}_closed`, { code, reason: reason?.toString?.() || "" });
    if (socketState !== state) return;
    open = false;
    if (!stopped && countGap) {
      gaps += 1;
      requiresReconciliation = true;
      void reconnectIfNeeded();
    }
  }

  function failHeartbeat(state, error) {
    if (stopped || socketState !== state || state.closeHandled) return;
    ledger?.record(`${eventType}_heartbeat_failed`, { message: error.message });
    const countGap = state.ready;
    handleSocketClose(state, 4000, "heartbeat timeout", countGap);
    try {
      if (typeof state.ws.terminate === "function") state.ws.terminate();
      else state.ws.close();
    } catch (closeError) {
      ledger?.record(`${eventType}_error`, { message: closeError.message });
    }
  }

  function sendHeartbeat(state) {
    if (stopped || socketState !== state || state.closeHandled ||
      state.ws.readyState !== WebSocketImpl.OPEN) {
      return Promise.reject(new Error("fail closed: websocket is unavailable for heartbeat"));
    }
    if (state.pendingPong) {
      return state.pendingPong.promise;
    }
    let resolvePending;
    let rejectPending;
    const promise = new Promise((resolve, reject) => {
      resolvePending = resolve;
      rejectPending = reject;
    });
    const pongSequenceAtSend = state.pongSequence;
    const pending = {
      pongSequenceAtSend,
      resolve: resolvePending,
      reject: rejectPending,
      promise,
      timeoutTimer: null
    };
    state.pendingPong = pending;
    pending.timeoutTimer = setTimer(() => {
      if (state.pendingPong !== pending) return;
      state.pendingPong = null;
      rejectPending(new Error("fail closed: websocket heartbeat timeout"));
    }, heartbeatResponseTimeoutMs);
    try {
      state.lastPingWallMs = nowMs();
      state.ws.send("PING");
      ledger?.record(`${eventType}_ping`);
    } catch (error) {
      if (state.pendingPong === pending) state.pendingPong = null;
      clearTimer(pending.timeoutTimer);
      rejectPending(error);
    }
    return promise;
  }

  function scheduleHeartbeat(state) {
    if (stopped || socketState !== state || state.closeHandled || !state.ready ||
      state.heartbeatTimer !== null) return;
    const elapsedSincePingMs = Number.isFinite(state.lastPingWallMs)
      ? Math.max(0, nowMs() - state.lastPingWallMs)
      : 0;
    const nextHeartbeatDelayMs = Math.max(0, heartbeatEveryMs - elapsedSincePingMs);
    state.heartbeatTimer = setTimer(() => {
      state.heartbeatTimer = null;
      void sendHeartbeat(state)
        .then(() => scheduleHeartbeat(state))
        .catch((error) => failHeartbeat(state, error));
    }, nextHeartbeatDelayMs);
  }

  async function openSocket(isReconnect = false) {
    const ws = new WebSocketImpl(url);
    const state = {
      ws,
      ready: false,
      closeHandled: false,
      pongSequence: 0,
      lastPingWallMs: null,
      heartbeatTimer: null,
      pendingPong: null
    };
    if (socketState && socketState !== state) {
      clearSocketHeartbeat(socketState, new Error("fail closed: websocket superseded during reconnect"));
    }
    socketState = state;
    socket = ws;
    ws.on("message", (buffer, isBinary) => {
      if (state.closeHandled) return;
      const text = buffer.toString();
      if (text === "PONG") {
        state.pongSequence += 1;
        appendMessage({ _pong: true, _received_wall_ms: nowMs() });
        if (recordMessages) ledger?.record(`${eventType}_pong`);
        const pending = state.pendingPong;
        if (pending && state.pongSequence > pending.pongSequenceAtSend) {
          state.pendingPong = null;
          clearTimer(pending.timeoutTimer);
          pending.resolve();
        }
        return;
      }
      if (text === "NO NEW ASSETS") {
        if (recordMessages) ledger?.record(`${eventType}_subscription_noop`);
        return;
      }
      try {
        const parsed = JSON.parse(text);
        for (const value of Array.isArray(parsed) ? parsed : [parsed]) {
          const fingerprint = JSON.stringify(value);
          if (fingerprints.has(fingerprint)) {
            duplicates += 1;
            if (recordMessages) ledger?.record(`${eventType}_duplicate_ignored`, { duplicate_count: duplicates });
            continue;
          }
          const captured = { ...value, _received_wall_ms: nowMs() };
          appendMessage(captured, fingerprint);
          if (recordMessages) ledger?.record(eventType, captured);
        }
      } catch {
        unparsed += 1;
        latestUnparsedFrameMetadata = unparsedFrameMetadata(buffer, isBinary, nowMs());
        requiresReconciliation = true;
        if (recordMessages) ledger?.record(`${eventType}_unparsed`, { unparsed_count: unparsed });
      }
    });
    ws.on("close", (code, reason) => {
      handleSocketClose(state, code, reason);
    });
    ws.on("error", (error) => ledger?.record(`${eventType}_error`, { message: error.message }));
    try {
      await new Promise((resolve, reject) => {
        const timer = setTimer(() => {
          cleanup();
          reject(new Error(`fail closed: websocket open timeout: ${url}`));
        }, openTimeoutMs);
        const cleanup = () => {
          clearTimer(timer);
          ws.off("open", onOpen);
          ws.off("error", onOpenError);
          ws.off("close", onOpenClose);
        };
        const onOpen = () => {
          cleanup();
          resolve();
        };
        const onOpenError = (error) => {
          cleanup();
          reject(error);
        };
        const onOpenClose = () => {
          cleanup();
          reject(new Error("fail closed: websocket closed before opening"));
        };
        ws.once("open", onOpen);
        ws.once("error", onOpenError);
        ws.once("close", onOpenClose);
      });
      ws.send(JSON.stringify(currentSubscription));
      await sleep(settleMs);
      await sendHeartbeat(state);
      state.ready = true;
      open = true;
      ledger?.record(`${eventType}_ready`, { subscription_type: subscription.type, reconnect: isReconnect });
      scheduleHeartbeat(state);
    } catch (error) {
      clearSocketHeartbeat(state, error);
      open = false;
      try {
        if (typeof ws.terminate === "function") ws.terminate();
        else ws.close();
      } catch (closeError) {
        ledger?.record(`${eventType}_error`, { message: closeError.message });
      }
      throw error;
    }
  }

  async function reconnect() {
    for (let attempt = 1; attempt <= reconnectAttempts && !stopped; attempt += 1) {
      await sleep(Math.min(2_000, 200 * attempt));
      try {
        await openSocket(true);
        reconnects += 1;
        ledger?.record(`${eventType}_reconnected`, { attempt, reconnect_count: reconnects });
        return;
      } catch (error) {
        ledger?.record(`${eventType}_reconnect_failed`, { attempt, message: error.message });
      }
    }
  }

  function reconnectIfNeeded() {
    if (!stopped && (!open || socket?.readyState !== WebSocketImpl.OPEN)) {
      reconnectPromise ||= reconnect().finally(() => { reconnectPromise = null; });
    }
    return reconnectPromise;
  }

  async function ensureChannelOpen() {
    if (open && socket?.readyState === WebSocketImpl.OPEN) return true;
    const reconnecting = reconnectIfNeeded();
    if (reconnecting) await reconnecting;
    await waitUntil(
      () => open && socket?.readyState === WebSocketImpl.OPEN,
      openTimeoutMs,
      "websocket reconnect timeout",
      sleep
    );
    return true;
  }

  try {
    await openSocket(false);
  } catch (error) {
    stopped = true;
    clearSocketHeartbeat(socketState, error);
    throw error;
  }
  return {
    messages,
    beginEvidenceWindow,
    clearHistory,
    historyStats: () => ({
      message_count: messages.length,
      fingerprint_count: fingerprints.size,
      approximate_bytes: messageHistoryBytes,
      evicted_count: historyEvictions
    }),
    isOpen: () => open && socket?.readyState === WebSocketImpl.OPEN,
    ensureOpen: ensureChannelOpen,
    gapCount: () => gaps,
    reconnectCount: () => reconnects,
    duplicateCount: () => duplicates,
    unparsedCount: () => unparsed,
    latestUnparsedFrameMetadata: () => latestUnparsedFrameMetadata
      ? { ...latestUnparsedFrameMetadata }
      : null,
    requiresReconciliation: () => requiresReconciliation,
    markReconciled: () => {
      if (!open || socket?.readyState !== WebSocketImpl.OPEN) {
        throw new Error("fail closed: disconnected websocket cannot be marked reconciled");
      }
      requiresReconciliation = false;
      gaps = 0;
      unparsed = 0;
    },
    lastPongWallMs: () => {
      const values = messages
        .filter((message) => message?._pong === true)
        .map((message) => Number(message._received_wall_ms))
        .filter(Number.isFinite);
      return values.length ? Math.max(...values) : null;
    },
    forceHeartbeat: async () => {
      await ensureChannelOpen();
      if (socketState.heartbeatTimer !== null) {
        clearTimer(socketState.heartbeatTimer);
        socketState.heartbeatTimer = null;
      }
      await sendHeartbeat(socketState);
      scheduleHeartbeat(socketState);
      return true;
    },
    updateSubscription: async ({ operation, markets = [], assets_ids = [] }) => {
      if (!["subscribe", "unsubscribe"].includes(operation)) {
        throw new Error("fail closed: websocket subscription operation is invalid");
      }
      await ensureChannelOpen();
      const marketValues = [...new Set(markets.map(String).filter(Boolean))];
      const assetValues = [...new Set(assets_ids.map(String).filter(Boolean))];
      if (!marketValues.length && !assetValues.length) {
        throw new Error("fail closed: websocket subscription update is empty");
      }
      const frame = { operation };
      if (marketValues.length) frame.markets = marketValues;
      if (assetValues.length) frame.assets_ids = assetValues;
      socket.send(JSON.stringify(frame));
      for (const [field, additions] of [["markets", marketValues], ["assets_ids", assetValues]]) {
        if (!additions.length) continue;
        const existing = new Set((currentSubscription[field] || []).map(String));
        if (operation === "subscribe") additions.forEach((value) => existing.add(value));
        else additions.forEach((value) => existing.delete(value));
        currentSubscription[field] = [...existing];
      }
      ledger?.record(`${eventType}_subscription_updated`, {
        operation,
        markets: marketValues,
        assets_ids: assetValues
      });
    },
    waitForMessage: async (predicate, timeoutMs = openTimeoutMs) => {
      const started = nowMs();
      while (nowMs() - started <= timeoutMs) {
        const match = messages.find(predicate);
        if (match) return match;
        await sleep(10);
      }
      throw new Error("fail closed: websocket subscription did not produce required evidence");
    },
    subscription: () => structuredClone(currentSubscription),
    close: () => {
      stopped = true;
      open = false;
      clearHistory();
      if (socketState) {
        socketState.ready = false;
        clearSocketHeartbeat(socketState, new Error("websocket channel closed"));
      }
      socket?.close();
    }
  };
}

export function marketMessagesThrough(messages, cutoffWallMs) {
  return (messages || []).filter((message) =>
    Number.isFinite(Number(message?._received_wall_ms)) && Number(message._received_wall_ms) <= cutoffWallMs
  );
}

export function hasExactEligibleHorizons(observations, horizons) {
  const rows = Array.isArray(observations) ? observations : [];
  const expected = Array.isArray(horizons) ? horizons : [];
  return rows.length === expected.length && expected.every((horizon) => {
    const matches = rows.filter((row) => Number(row?.horizon_seconds) === Number(horizon));
    return matches.length === 1 && matches[0].label_observed === true &&
      matches[0].quality_eligible === true && matches[0].eligible === true;
  });
}

export async function cancelOrderWithMetrics(client, orderId, ledger, options = {}) {
  const nowMs = options.nowMs || Date.now;
  const nowMonotonic = options.nowMonotonic || (() => performance.now());
  const sleep = options.sleep || defaultSleep;
  const cancelSendWallMs = nowMs();
  const cancelMono = nowMonotonic();
  ledger?.record("venue_cancel_send", {
    order_id: orderId,
    wall_ms: cancelSendWallMs,
    monotonic_ms: cancelMono
  });
  let lastError;
  for (let attempt = 1; attempt <= 3; attempt += 1) {
    try {
      const cancelResponse = await client.cancelOrder({ orderID: orderId });
      return {
        cancelSendWallMs,
        cancelResponseWallMs: nowMs(),
        cancelRoundTripMs: Math.max(0, nowMonotonic() - cancelMono),
        cancelResponse,
        failedAttempts: attempt - 1
      };
    } catch (error) {
      lastError = error;
      ledger?.record("venue_cancel_attempt_failed", { order_id: orderId, attempt, message: error.message });
      await sleep(200 * attempt);
    }
  }
  const openOrders = await strictOpenOrders(client);
  const matching = openOrders.filter((order) => String(order.id) === String(orderId));
  if (!matching.length) {
    return {
      cancelSendWallMs,
      cancelResponseWallMs: nowMs(),
      cancelRoundTripMs: Math.max(0, nowMonotonic() - cancelMono),
      cancelResponse: { terminal_before_cancel_confirmation: true, last_error: lastError?.message },
      failedAttempts: 3
    };
  }
  if (openOrders.length === 1 && matching.length === 1) {
    ledger?.record("venue_emergency_cancel_all", { reason: "single canary order remained after three targeted cancel attempts" });
    const cancelResponse = await client.cancelAll();
    return {
      cancelSendWallMs,
      cancelResponseWallMs: nowMs(),
      cancelRoundTripMs: Math.max(0, nowMonotonic() - cancelMono),
      cancelResponse: { emergency_cancel_all: true, response: cancelResponse },
      failedAttempts: 3
    };
  }
  throw new Error(`fail closed: canary order remained open after cancel retries: ${lastError?.message || "unknown"}`);
}

export async function waitForStablePostCancelReconciliation({
  client,
  conditionId,
  orderId,
  userChannel,
  ledger,
  assertHealthy = () => {},
  options = {}
}) {
  const nowMs = options.nowMs || Date.now;
  const sleep = options.sleep || defaultSleep;
  const minimumObservationMs = options.minimumObservationMs ?? 10_000;
  const requiredStableMs = options.requiredStableMs ?? 5_000;
  const timeoutMs = options.timeoutMs ?? 30_000;
  const pollMs = options.pollMs ?? 500;
  const started = nowMs();
  const deadline = started + timeoutMs;
  let previousFingerprint = null;
  let stableSince = null;
  let latest = null;
  while (nowMs() < deadline) {
    assertHealthy();
    await userChannel.ensureOpen();
    let finalOrder;
    let trades;
    try {
      [finalOrder, trades] = await Promise.all([
        client.getOrder(orderId),
        client.getTrades({ market: conditionId })
      ]);
    } catch (error) {
      ledger?.record("venue_post_cancel_reconciliation_query_failed", { order_id: orderId, message: error.message });
      previousFingerprint = null;
      stableSince = null;
      await sleep(pollMs);
      continue;
    }
    const relatedTrades = uniqueTrades((trades || []).filter((trade) => orderIds(trade).includes(String(orderId))));
    const userEvents = relevantUserEvents(userChannel.messages, orderId);
    const restFills = tradeFillsFromRest(relatedTrades, orderId);
    const userFills = tradeFillsFromUserEvents(userEvents, orderId);
    const userOrderMatched = maximumMatchedSize(userEvents);
    const openOrders = await strictOpenOrders(client);
    const zeroOpenOrders = openOrders.length === 0;
    const terminalStatus = String(finalOrder?.status || "").toUpperCase();
    const terminalConfirmed = TERMINAL_ORDER_STATES.includes(terminalStatus);
    const fingerprint = JSON.stringify({
      status: terminalStatus,
      rest_order_matched: Number(finalOrder?.size_matched || 0),
      rest_fills: restFills.map(fillFingerprint).sort(),
      user_order_matched: userOrderMatched,
      user_fills: userFills.map(fillFingerprint).sort(),
      zero_open_orders: zeroOpenOrders
    });
    if (fingerprint === previousFingerprint) stableSince ??= nowMs();
    else {
      previousFingerprint = fingerprint;
      stableSince = nowMs();
    }
    latest = { finalOrder, relatedTrades, userEvents, zeroOpenOrders, terminalConfirmed };
    const observationMs = nowMs() - started;
    const stableMs = nowMs() - stableSince;
    ledger?.record("venue_post_cancel_reconciliation_snapshot", {
      order_id: orderId,
      observation_ms: observationMs,
      stable_ms: stableMs,
      terminal_confirmed: terminalConfirmed,
      zero_open_orders_confirmed: zeroOpenOrders,
      rest_trade_count: restFills.length,
      user_trade_count: userFills.length
    });
    if (observationMs >= minimumObservationMs && stableMs >= requiredStableMs && terminalConfirmed && zeroOpenOrders) {
      return { ...latest, stableFinality: true, observationMs };
    }
    await sleep(pollMs);
  }
  if (!latest) throw new Error("fail closed: no successful post-cancel reconciliation snapshot");
  return { ...latest, stableFinality: false, observationMs: nowMs() - started };
}

export function relevantUserEvents(messages, orderId) {
  return (messages || []).filter((message) => orderIds(message).includes(String(orderId)));
}

export function tradeFillsFromUserEvents(events, orderId) {
  return normalizeTradeFills((events || []).filter((event) =>
    String(event.event_type || event.type || "").toLowerCase().includes("trade")
  ), orderId);
}

export function tradeFillsFromRest(trades, orderId) {
  return normalizeTradeFills(trades || [], orderId, { includeSettlementEvidence: true });
}

export function mergeTradeFills(...groups) {
  return [...new Map(groups.flat().map((fill) => [fill.id, fill])).values()];
}

export function firstFillTimestamp(fills) {
  const values = (fills || []).map((fill) => Number(fill.timestampMs)).filter(Number.isFinite);
  return values.length ? Math.min(...values) : null;
}

export function fillRacedCancellation(fills, cancelSendWallMs) {
  return postCancelFillStats(fills, cancelSendWallMs).postCancelFillCount > 0;
}

export function postCancelFillStats(fills, cancelSendWallMs) {
  if (cancelSendWallMs === null || cancelSendWallMs === undefined || !Number.isFinite(Number(cancelSendWallMs))) {
    return { postCancelFillCount: 0, firstFillAfterCancelMs: null };
  }
  const cancelWallMs = Number(cancelSendWallMs);
  const timestamps = (fills || [])
    .map((fill) => Number(fill.timestampMs))
    .filter((timestamp) => Number.isFinite(timestamp) && timestamp >= cancelWallMs);
  return {
    postCancelFillCount: timestamps.length,
    firstFillAfterCancelMs: timestamps.length ? Math.min(...timestamps) - cancelWallMs : null
  };
}

export function publicTradeThroughStats(messages, order, startWallMs, endWallMs, fills = []) {
  const trades = (messages || [])
    .filter((message) => message.event_type === "last_trade_price")
    .filter((message) => message._received_wall_ms >= startWallMs && message._received_wall_ms <= endWallMs)
    .map((message) => ({
      received_wall_ms: Number(message._received_wall_ms),
      price: Number(message.price),
      size: Number(message.size || 0),
      side: message.side || null
    }))
    .filter((trade) => Number.isFinite(trade.price));
  const touches = trades.filter((trade) => trade.price <= order.price);
  const strict = trades.filter((trade) => trade.price < order.price);
  return {
    touch_count: touches.length,
    strict_trade_through_count: strict.length,
    trade_through_without_fill_count: strict.filter((trade) =>
      !(fills || []).some((fill) => Number(fill.timestampMs) <= trade.received_wall_ms)
    ).length,
    trades
  };
}

export function sameStringSet(left, right) {
  const leftSet = new Set((left || []).map(String).filter(Boolean));
  const rightSet = new Set((right || []).map(String).filter(Boolean));
  return leftSet.size === rightSet.size && [...leftSet].every((value) => rightSet.has(value));
}

export function nearlyEqualSize(left, right) {
  const a = Number(left);
  const b = Number(right);
  return Number.isFinite(a) && Number.isFinite(b) &&
    Math.abs(a - b) <= Math.max(1e-6, Math.max(Math.abs(a), Math.abs(b)) * 1e-6);
}

export function sum(values) {
  return (values || []).reduce((total, value) => total + (Number.isFinite(Number(value)) ? Number(value) : 0), 0);
}

export function maximumMatchedSize(events) {
  return (events || [])
    // Order lifecycle messages carry the venue's cumulative size_matched value.
    // A trade's matched_amount is per fill and must not be compared with the
    // cumulative REST order total.
    .map((event) => Number(event.size_matched || 0))
    .reduce((maximum, value) => Math.max(maximum, Number.isFinite(value) ? value : 0), 0);
}

export function cancellationEventReceivedAt(events) {
  const event = (events || []).find((value) => {
    const type = String(value.type || "").toUpperCase();
    const status = String(value.status || "").toUpperCase();
    return type.includes("CANCEL") || status.includes("CANCEL");
  });
  return Number.isFinite(Number(event?._received_wall_ms)) ? Number(event._received_wall_ms) : null;
}

function normalizeTradeFills(trades, orderId, { includeSettlementEvidence = false } = {}) {
  const fills = [];
  for (const trade of uniqueTrades(trades)) {
    const id = String(trade.id || trade.trade_id || trade.transaction_hash || "");
    if (!id) continue;
    const makerMatches = (trade.maker_orders || []).filter((row) =>
      String(row.order_id) === String(orderId)
    );
    const maker = makerMatches[0];
    const directMaker = String(trade.maker_order_id || "") === String(orderId);
    const isTaker = String(trade.taker_order_id || "") === String(orderId);
    const directOrder = String(trade.order_id || "") === String(orderId);
    if (!maker && !directMaker && !isTaker && !directOrder) continue;
    const size = Number(maker?.matched_amount ?? trade.matched_amount ?? trade.size ?? trade.amount ?? 0);
    const price = Number(maker?.price ?? trade.price ?? 0);
    const timestampMs = epochMs(trade.match_time_nano || trade.match_time || trade.matchtime || trade.timestamp || trade.created_at);
    if (!(size > 0) || !(price > 0) || !Number.isFinite(timestampMs)) continue;
    const traderSide = String(trade.trader_side || trade.traderSide || "").trim().toUpperCase() || null;
    const orderRole = isTaker ? "TAKER" : (maker || directMaker ? "MAKER" : null);
    const authenticatedFeeRateBps = optionalNumber(firstNonBlank(
      maker?.fee_rate_bps,
      maker?.feeRateBps,
      trade.fee_rate_bps,
      trade.feeRateBps
    ));
    // The authenticated trade endpoint does not currently promise a fee amount,
    // but preserve explicitly unit-labelled decimal fields if the venue adds one.
    // Ambiguous integer/micro-unit fields remain raw and are never used as money.
    const authenticatedFeeAmount = optionalNumber(
      trade.fee_amount_usdc_decimal ?? trade.feeAmountUsdcDecimal ?? trade.fee_amount ?? trade.feeAmount
    );
    const fill = {
      id, size, price, timestampMs, traderSide, orderRole,
      authenticatedFeeRateBps,
      authenticatedFeeAmount,
      authenticatedFeeRaw: {
        fee_rate_bps: firstNonBlank(
          maker?.fee_rate_bps,
          maker?.feeRateBps,
          trade.fee_rate_bps,
          trade.feeRateBps
        ),
        fee: trade.fee ?? null,
        fee_usdc: trade.fee_usdc ?? trade.feeUsdc ?? null,
        builder_fee: maker?.builder_fee ?? trade.builder_fee ?? trade.builderFee ?? null
      }
    };
    if (includeSettlementEvidence) {
      Object.assign(fill, {
        transactionHash: firstNonBlank(trade.transaction_hash, trade.transactionHash),
        market: firstNonBlank(trade.market, trade.condition_id, trade.conditionId),
        assetId: firstNonBlank(maker?.asset_id, trade.asset_id, trade.assetId),
        tradeAssetId: firstNonBlank(trade.asset_id, trade.assetId),
        makerAssetId: firstNonBlank(maker?.asset_id),
        status: String(trade.status || "").trim().toUpperCase() || null,
        orderId: String(orderId),
        makerOrderId: firstNonBlank(maker?.order_id, trade.maker_order_id),
        nestedMakerOrderMatchCount: makerMatches.length,
        directMakerOrder: directMaker,
        takerOrderId: firstNonBlank(trade.taker_order_id),
        owner: firstNonBlank(maker?.owner, trade.owner),
        makerAddress: firstNonBlank(maker?.maker_address, trade.maker_address),
        orderSide: String(maker?.side || trade.side || "").trim().toUpperCase() || null
      });
    }
    fills.push(fill);
  }
  return fills;
}

function optionalNumber(value) {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function firstNonBlank(...values) {
  return values.find((value) => value !== null && value !== undefined && String(value).trim() !== "") ?? null;
}

export function orderIds(row) {
  return [
    row?.maker_order_id,
    row?.taker_order_id,
    row?.order_id,
    row?.id,
    ...(row?.maker_orders || []).map((maker) => maker.order_id)
  ].map(String);
}

function uniqueTrades(trades) {
  const seen = new Set();
  return (trades || []).filter((trade) => {
    const key = String(trade.id || trade.trade_id || trade.transaction_hash || JSON.stringify(trade));
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

async function strictOpenOrders(client) {
  const orders = await client.getOpenOrders();
  if (!Array.isArray(orders)) throw new Error("fail closed: venue open-order response is invalid");
  return orders;
}

function fillFingerprint(fill) {
  return [fill.id, fill.size, fill.price];
}

function epochMs(value) {
  const numeric = Number(value);
  if (Number.isFinite(numeric)) {
    if (numeric >= 1e17) return numeric / 1_000_000;
    if (numeric >= 1e14) return numeric / 1_000;
    return numeric < 1e12 ? numeric * 1_000 : numeric;
  }
  return Date.parse(value);
}

async function waitUntil(predicate, timeoutMs, message, sleep) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await sleep(Math.min(50, Math.max(1, deadline - Date.now())));
  }
  throw new Error(`fail closed: ${message}`);
}

function positiveInteger(value, label) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`fail closed: ${label} must be a positive integer`);
  }
  return parsed;
}

function defaultSleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
