import test from "node:test";
import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import {
  connectLifecycleChannel,
  fillRacedCancellation,
  hasExactEligibleHorizons,
  marketMessagesThrough,
  maximumMatchedSize,
  postCancelFillStats,
  tradeFillsFromUserEvents,
  waitForStablePostCancelReconciliation
} from "../src/canary-lifecycle-lib.mjs";

class ManualScheduler {
  constructor() {
    this.now = 0;
    this.nextId = 1;
    this.tasks = new Map();
  }

  setTimer = (callback, delayMs) => {
    const id = this.nextId;
    this.nextId += 1;
    this.tasks.set(id, { callback, at: this.now + Number(delayMs) });
    return id;
  };

  clearTimer = (id) => {
    this.tasks.delete(id);
  };

  advance(delayMs) {
    const target = this.now + delayMs;
    while (true) {
      const next = [...this.tasks.entries()]
        .filter(([, task]) => task.at <= target)
        .sort((left, right) => left[1].at - right[1].at || left[0] - right[0])[0];
      if (!next) break;
      const [id, task] = next;
      this.tasks.delete(id);
      this.now = task.at;
      task.callback();
    }
    this.now = target;
  }

  pendingCount() {
    return this.tasks.size;
  }
}

async function flushMicrotasks(rounds = 12) {
  for (let index = 0; index < rounds; index += 1) await Promise.resolve();
}

test("nested maker_orders user trades produce authenticated maker fills", () => {
  const fills = tradeFillsFromUserEvents([{
    event_type: "trade",
    id: "trade-1",
    match_time: "2026-07-13T12:00:01.000Z",
    trader_side: "MAKER",
    fee_rate_bps: "0",
    maker_orders: [{ order_id: "maker-order-1", matched_amount: "2.5", price: "0.40", fee_rate_bps: "0" }]
  }], "maker-order-1");

  assert.deepEqual(fills, [{
    id: "trade-1",
    size: 2.5,
    price: 0.4,
    timestampMs: Date.parse("2026-07-13T12:00:01.000Z"),
    traderSide: "MAKER",
    orderRole: "MAKER",
    authenticatedFeeRateBps: 0,
    authenticatedFeeAmount: null,
    authenticatedFeeRaw: {
      fee_rate_bps: "0",
      fee: null,
      fee_usdc: null,
      builder_fee: null
    }
  }]);
});

test("a later partial fill after cancel is classified as a cancellation race", () => {
  const cancelSendWallMs = 2_000;
  const fills = [
    { id: "before", timestampMs: 1_900 },
    { id: "after", timestampMs: 2_050 }
  ];

  assert.equal(fillRacedCancellation(fills, cancelSendWallMs), true);
  assert.equal(fillRacedCancellation(fills.slice(0, 1), cancelSendWallMs), false);
  assert.equal(fillRacedCancellation(fills, null), false);
  assert.deepEqual(postCancelFillStats(fills, cancelSendWallMs), {
    postCancelFillCount: 1,
    firstFillAfterCancelMs: 50
  });
  assert.deepEqual(postCancelFillStats(fills.slice(0, 1), cancelSendWallMs), {
    postCancelFillCount: 0,
    firstFillAfterCancelMs: null
  });
});

test("cumulative order matched size ignores per-trade matched amounts", () => {
  const events = [
    { event_type: "order", size_matched: "1.25" },
    { event_type: "trade", matched_amount: "4.5" },
    { event_type: "order", size_matched: "2.00" }
  ];

  assert.equal(maximumMatchedSize(events), 2);
});

test("pre-send market evidence excludes every message received after its cutoff", () => {
  const messages = [
    { event_type: "book", _received_wall_ms: 100 },
    { event_type: "price_change", _received_wall_ms: 101 },
    { event_type: "last_trade_price", _received_wall_ms: 102 }
  ];

  assert.deepEqual(marketMessagesThrough(messages, 101), messages.slice(0, 2));
});

test("canary completion requires one eligible row at every exact model horizon", () => {
  const rows = [1, 5, 30, 60].map((horizon_seconds) => ({
    horizon_seconds,
    label_observed: true,
    quality_eligible: true,
    eligible: true
  }));

  assert.equal(hasExactEligibleHorizons(rows, [1, 5, 30, 60]), true);
  assert.equal(hasExactEligibleHorizons(rows.slice(0, 3), [1, 5, 30, 60]), false);
  assert.equal(hasExactEligibleHorizons([...rows, rows[0]], [1, 5, 30, 60]), false);
  assert.equal(hasExactEligibleHorizons(rows.map((row, index) => index === 2
    ? { ...row, quality_eligible: false, eligible: false }
    : row), [1, 5, 30, 60]), false);
});

test("channel disconnect is counted as a gap and reconnects before reuse", async () => {
  class FakeWebSocket extends EventEmitter {
    static OPEN = 1;
    static instances = [];

    constructor() {
      super();
      this.readyState = 0;
      FakeWebSocket.instances.push(this);
      queueMicrotask(() => {
        this.readyState = FakeWebSocket.OPEN;
        this.emit("open");
      });
    }

    send(value) {
      if (value === "PING") queueMicrotask(() => this.emit("message", Buffer.from("PONG")));
    }

    forceClose() {
      this.readyState = 3;
      this.emit("close", 1006, Buffer.from("network gap"));
    }

    close() {
      this.readyState = 3;
      this.emit("close", 1000, Buffer.alloc(0));
    }
  }

  const channel = await connectLifecycleChannel({
    url: "wss://example.invalid",
    subscription: { type: "user" },
    eventType: "test_user_channel",
    WebSocketImpl: FakeWebSocket,
    settleMs: 0,
    openTimeoutMs: 100,
    heartbeatTimeoutMs: 100,
    sleep: (ms) => new Promise((resolve) => setTimeout(resolve, Math.min(ms, 1)))
  });

  FakeWebSocket.instances[0].forceClose();
  await channel.ensureOpen();

  assert.equal(FakeWebSocket.instances.length, 2);
  assert.equal(channel.gapCount(), 1);
  assert.equal(channel.reconnectCount(), 1);
  assert.equal(channel.isOpen(), true);
  channel.close();
});

test("each socket sends one non-overlapping heartbeat at least every ten seconds and close clears it", async () => {
  const scheduler = new ManualScheduler();
  class FakeWebSocket extends EventEmitter {
    static OPEN = 1;
    static instances = [];

    constructor() {
      super();
      this.readyState = 0;
      this.sent = [];
      FakeWebSocket.instances.push(this);
      queueMicrotask(() => {
        this.readyState = FakeWebSocket.OPEN;
        this.emit("open");
      });
    }

    send(value) {
      this.sent.push(value);
      if (value === "PING") queueMicrotask(() => this.emit("message", Buffer.from("PONG")));
    }

    close() {
      this.readyState = 3;
      this.emit("close", 1000, Buffer.alloc(0));
    }
  }

  const channel = await connectLifecycleChannel({
    url: "wss://example.invalid",
    subscription: { type: "user" },
    eventType: "test_user_channel",
    WebSocketImpl: FakeWebSocket,
    heartbeatIntervalMs: 20_000,
    heartbeatTimeoutMs: 100,
    openTimeoutMs: 100,
    settleMs: 0,
    sleep: async () => {},
    nowMs: () => scheduler.now,
    setTimer: scheduler.setTimer,
    clearTimer: scheduler.clearTimer
  });

  const socket = FakeWebSocket.instances[0];
  assert.equal(socket.sent.filter((value) => value === "PING").length, 1);
  assert.equal(scheduler.pendingCount(), 1, "only the next heartbeat timer may remain armed");

  scheduler.advance(9_999);
  assert.equal(socket.sent.filter((value) => value === "PING").length, 1);
  scheduler.advance(1);
  await flushMicrotasks();
  assert.equal(socket.sent.filter((value) => value === "PING").length, 2);
  assert.equal(scheduler.pendingCount(), 1, "a successful heartbeat must schedule exactly one successor");

  scheduler.advance(10_000);
  await flushMicrotasks();
  assert.equal(socket.sent.filter((value) => value === "PING").length, 3);
  assert.equal(scheduler.pendingCount(), 1);

  channel.close();
  assert.equal(scheduler.pendingCount(), 0, "close() must clear the socket heartbeat timer");
  scheduler.advance(30_000);
  assert.equal(socket.sent.filter((value) => value === "PING").length, 3);
});

test("a slow fresh PONG does not move the next PING beyond the ten-second cadence", async () => {
  const scheduler = new ManualScheduler();
  class FakeWebSocket extends EventEmitter {
    static OPEN = 1;

    constructor() {
      super();
      this.readyState = 0;
      this.pingCount = 0;
      queueMicrotask(() => {
        this.readyState = FakeWebSocket.OPEN;
        this.emit("open");
      });
    }

    send(value) {
      if (value !== "PING") return;
      this.pingCount += 1;
      if (this.pingCount === 1) queueMicrotask(() => this.emit("message", Buffer.from("PONG")));
      else if (this.pingCount === 2) {
        scheduler.setTimer(() => this.emit("message", Buffer.from("PONG")), 4_000);
      } else queueMicrotask(() => this.emit("message", Buffer.from("PONG")));
    }

    close() {
      this.readyState = 3;
      this.emit("close", 1000, Buffer.alloc(0));
    }
  }

  const channel = await connectLifecycleChannel({
    url: "wss://example.invalid",
    subscription: { type: "user" },
    eventType: "test_user_channel",
    WebSocketImpl: FakeWebSocket,
    heartbeatIntervalMs: 10_000,
    heartbeatTimeoutMs: 5_000,
    openTimeoutMs: 100,
    settleMs: 0,
    sleep: async () => {},
    nowMs: () => scheduler.now,
    setTimer: scheduler.setTimer,
    clearTimer: scheduler.clearTimer
  });

  scheduler.advance(10_000);
  await flushMicrotasks();
  scheduler.advance(4_000);
  await flushMicrotasks();
  scheduler.advance(5_999);
  assert.equal(channel.isOpen(), true);
  scheduler.advance(1);
  await flushMicrotasks();
  assert.equal(channel.messages.filter((message) => message._pong).length, 3);

  channel.close();
  assert.equal(scheduler.pendingCount(), 0);
});

test("an old PONG cannot satisfy a later heartbeat and missing freshness reconnects fail closed", async () => {
  const scheduler = new ManualScheduler();
  const ledgerEvents = [];
  class FakeWebSocket extends EventEmitter {
    static OPEN = 1;
    static instances = [];

    constructor() {
      super();
      this.readyState = 0;
      this.pingCount = 0;
      FakeWebSocket.instances.push(this);
      queueMicrotask(() => {
        this.readyState = FakeWebSocket.OPEN;
        this.emit("open");
      });
    }

    send(value) {
      if (value !== "PING") return;
      this.pingCount += 1;
      const isReconnect = FakeWebSocket.instances.indexOf(this) > 0;
      if (this.pingCount === 1 || isReconnect) {
        queueMicrotask(() => this.emit("message", Buffer.from("PONG")));
      }
    }

    close() {
      if (this.readyState === 3) return;
      this.readyState = 3;
      this.emit("close", 1000, Buffer.alloc(0));
    }
  }

  const channel = await connectLifecycleChannel({
    url: "wss://example.invalid",
    subscription: { type: "user" },
    eventType: "test_user_channel",
    ledger: { record: (event, fields) => ledgerEvents.push({ event, fields }) },
    WebSocketImpl: FakeWebSocket,
    heartbeatIntervalMs: 1_000,
    heartbeatTimeoutMs: 500,
    openTimeoutMs: 100,
    settleMs: 0,
    sleep: async () => {},
    nowMs: () => scheduler.now,
    setTimer: scheduler.setTimer,
    clearTimer: scheduler.clearTimer
  });

  const firstSocket = FakeWebSocket.instances[0];
  firstSocket.emit("message", Buffer.from("PONG"));
  scheduler.advance(1_000);
  await flushMicrotasks();
  assert.equal(firstSocket.pingCount, 2);
  assert.equal(channel.gapCount(), 0);

  scheduler.advance(499);
  await flushMicrotasks();
  assert.equal(channel.gapCount(), 0, "the timeout must not fire early");
  scheduler.advance(1);
  await flushMicrotasks(30);

  assert.equal(FakeWebSocket.instances.length, 2, "a stale PONG must not keep the dead socket eligible");
  assert.equal(channel.gapCount(), 1);
  assert.equal(channel.reconnectCount(), 1);
  assert.equal(channel.isOpen(), true);
  assert.equal(ledgerEvents.filter(({ event }) => event === "test_user_channel_heartbeat_failed").length, 1);
  assert.equal(scheduler.pendingCount(), 1, "the retired socket timer must be replaced, not duplicated");

  channel.close();
  assert.equal(scheduler.pendingCount(), 0);
});

test("terminal no-fill reconciliation waits for the full stable-finality window", async () => {
  let now = 0;
  let snapshots = 0;
  const client = {
    async getOrder() {
      snapshots += 1;
      return { status: "CANCELED", size_matched: "0" };
    },
    async getTrades() { return []; },
    async getOpenOrders() { return []; }
  };
  const userChannel = {
    messages: [],
    ensureOpen: async () => true
  };

  const result = await waitForStablePostCancelReconciliation({
    client,
    conditionId: "condition-1",
    orderId: "order-1",
    userChannel,
    options: {
      nowMs: () => now,
      sleep: async (ms) => { now += ms; },
      minimumObservationMs: 10_000,
      requiredStableMs: 5_000,
      timeoutMs: 30_000,
      pollMs: 500
    }
  });

  assert.equal(result.stableFinality, true);
  assert.ok(result.observationMs >= 10_000);
  assert.ok(snapshots >= 21, "the first terminal/zero-open snapshot must not be accepted immediately");
  assert.equal(result.relatedTrades.length, 0);
});

test("non-terminal status containing MATCHED is never accepted as terminal", async () => {
  let now = 0;
  const client = {
    async getOrder() { return { status: "UNMATCHED", size_matched: "0" }; },
    async getTrades() { return []; },
    async getOpenOrders() { return []; }
  };

  const result = await waitForStablePostCancelReconciliation({
    client,
    conditionId: "condition-1",
    orderId: "order-1",
    userChannel: { messages: [], ensureOpen: async () => true },
    options: {
      nowMs: () => now,
      sleep: async (ms) => { now += ms; },
      minimumObservationMs: 1_000,
      requiredStableMs: 500,
      timeoutMs: 2_000,
      pollMs: 100
    }
  });

  assert.equal(result.stableFinality, false);
  assert.equal(result.terminalConfirmed, false);
});
