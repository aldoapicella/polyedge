import type { MarketSummary, RuntimeEvent, Snapshot } from "@/lib/types";

export const MARKET_EVENT_BUFFER_LIMIT = 5000;

export type ChartRange = "full" | "5m" | "1m";

export type ChartPoint = {
  bucket: number;
  time: string;
  qUp?: number;
  qDown?: number;
  upBid?: number;
  upAsk?: number;
  downBid?: number;
  downAsk?: number;
  distanceBps?: number;
  fillPrice?: number;
  fillOutcome?: string;
};

export type MarketSeries = {
  marketChart: ChartPoint[];
  fills: ChartPoint[];
  domain: [number, number];
  sampleCount: number;
};

export type RecentEventsPayload = {
  source?: string;
  warning?: string;
  events: unknown[];
};

type BuildMarketSeriesOptions = {
  snapshot?: Snapshot;
  market?: MarketSummary | null;
  events: RuntimeEvent[];
  range?: ChartRange;
  now?: number;
};

const RANGE_MS: Record<Exclude<ChartRange, "full">, number> = {
  "5m": 5 * 60 * 1000,
  "1m": 60 * 1000
};

export function buildMarketSeries({
  snapshot,
  market,
  events,
  range = "full",
  now = Date.now()
}: BuildMarketSeriesOptions): MarketSeries {
  const activeMarket = market ?? snapshot?.current_market ?? undefined;
  const marketDomain = marketWindowDomain(activeMarket, now);
  const visibleDomain = rangeDomain(marketDomain, range, now);
  const startPrice = Number(activeMarket?.start_price);
  const buckets = new Map<number, ChartPoint>();

  const getBucket = (ts: string | undefined | null) => {
    const parsed = parseTs(ts);
    if (parsed === undefined || parsed < marketDomain[0] || parsed > marketDomain[1]) {
      return undefined;
    }
    const bucket = Math.floor(parsed / 1000) * 1000;
    const existing = buckets.get(bucket);
    if (existing) {
      return existing;
    }
    const created: ChartPoint = {
      bucket,
      time: formatChartTime(bucket)
    };
    buckets.set(bucket, created);
    return created;
  };

  for (const event of events.slice().reverse()) {
    if (!eventAppliesToMarket(event, activeMarket) && event.type !== "reference_update") {
      continue;
    }
    const bucket = getBucket(event.ts);
    if (!bucket) {
      continue;
    }
    if (event.type === "fair_value_update") {
      bucket.qUp = finiteNumber(event.data.q_up);
      bucket.qDown = finiteNumber(event.data.q_down);
    }
    if (event.type === "reference_update") {
      const price = finiteNumber(event.data.price);
      if (price !== undefined && Number.isFinite(startPrice) && startPrice > 0) {
        bucket.distanceBps = ((price / startPrice) - 1) * 10000;
      }
    }
    if (event.type === "book_update_summary" && activeMarket) {
      const outcome = tokenOutcome(stringValue(event.data.token_id), activeMarket);
      const bid = bookPrice(event.data.best_bid);
      const ask = bookPrice(event.data.best_ask);
      if (outcome === "UP") {
        bucket.upBid = bid;
        bucket.upAsk = ask;
      }
      if (outcome === "DOWN") {
        bucket.downBid = bid;
        bucket.downAsk = ask;
      }
    }
    if (event.type === "paper_fill") {
      const avg = finiteNumber(event.data.avg_price);
      if (avg !== undefined) {
        bucket.fillPrice = avg;
        bucket.fillOutcome = tokenOutcome(stringValue(event.data.token_id), activeMarket);
      }
    }
  }

  if (activeMarket?.fair_value) {
    const bucket = getBucket(activeMarket.fair_value.computed_ts);
    if (bucket) {
      bucket.qUp = bucket.qUp ?? finiteNumber(activeMarket.fair_value.q_up);
      bucket.qDown = bucket.qDown ?? finiteNumber(activeMarket.fair_value.q_down);
    }
  }
  const referencePrice = finiteNumber(snapshot?.status.reference?.price);
  if (referencePrice !== undefined && Number.isFinite(startPrice) && startPrice > 0) {
    const bucket = getBucket(snapshot?.status.reference?.local_ts);
    if (bucket) {
      bucket.distanceBps = bucket.distanceBps ?? ((referencePrice / startPrice) - 1) * 10000;
    }
  }

  const allPoints = [...buckets.values()].sort((a, b) => a.bucket - b.bucket);
  const points = allPoints.filter((point) => point.bucket >= visibleDomain[0] && point.bucket <= visibleDomain[1]);
  return {
    marketChart: points,
    fills: points.filter((point) => point.fillPrice !== undefined),
    domain: visibleDomain,
    sampleCount: allPoints.length
  };
}

export function normalizeRecentEvents(payload: RecentEventsPayload | undefined): RuntimeEvent[] {
  if (!payload?.events?.length) {
    return [];
  }
  return payload.events.map(normalizeRecentEvent).filter((event): event is RuntimeEvent => Boolean(event));
}

export function formatChartTime(value: unknown) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "";
  }
  return new Date(numeric).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

export function rangeLabel(range: ChartRange) {
  if (range === "full") {
    return "Full market";
  }
  return `Last ${range}`;
}

export function eventAppliesToMarket(event: RuntimeEvent, market: MarketSummary | null | undefined) {
  if (!market) {
    return true;
  }
  const marketId = stringValue(event.data.market_id);
  if (marketId) {
    return marketId === market.market_id;
  }
  const tokenId = stringValue(event.data.token_id);
  return tokenId === market.up_token_id || tokenId === market.down_token_id;
}

function normalizeRecentEvent(raw: unknown): RuntimeEvent | null {
  if (!raw || typeof raw !== "object") {
    return null;
  }
  const record = raw as Record<string, unknown>;
  if (typeof record.type === "string" && typeof record.ts === "string" && record.data && typeof record.data === "object") {
    return {
      type: record.type,
      ts: record.ts,
      data: record.data as Record<string, unknown>
    };
  }
  const payload = record.payload && typeof record.payload === "object" ? (record.payload as Record<string, unknown>) : {};
  const eventType = stringValue(record.event_type) ?? stringValue(record.eventType);
  const ts = stringValue(record.recorded_ts) ?? stringValue(record.recordedTs) ?? stringValue(payload.local_ts) ?? stringValue(payload.computed_ts);
  if (!eventType || !ts) {
    return null;
  }
  return {
    type: publishEventType(eventType),
    ts,
    data: payload
  };
}

function publishEventType(eventType: string) {
  if (eventType === "reference") {
    return "reference_update";
  }
  if (eventType === "book") {
    return "book_update_summary";
  }
  return eventType;
}

function rangeDomain(domain: [number, number], range: ChartRange, now: number): [number, number] {
  if (range === "full") {
    return domain;
  }
  const visibleEnd = Math.min(Math.max(now, domain[0]), domain[1]);
  return [Math.max(domain[0], visibleEnd - RANGE_MS[range]), visibleEnd];
}

function marketWindowDomain(market: MarketSummary | null | undefined, now: number): [number, number] {
  const start = parseTs(market?.start_ts) ?? now - 15 * 60 * 1000;
  const end = parseTs(market?.end_ts) ?? now;
  return end > start ? [start, end] : [start, start + 15 * 60 * 1000];
}

function parseTs(value: string | undefined | null) {
  if (!value) {
    return undefined;
  }
  const parsed = new Date(value).getTime();
  return Number.isFinite(parsed) ? parsed : undefined;
}

function finiteNumber(value: unknown) {
  if (value === null || value === undefined || value === "") {
    return undefined;
  }
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : undefined;
}

function stringValue(value: unknown) {
  return typeof value === "string" ? value : undefined;
}

function tokenOutcome(tokenId: string | undefined | null, market: MarketSummary | null | undefined) {
  if (!tokenId || !market) {
    return "n/a";
  }
  if (tokenId === market.up_token_id) {
    return "UP";
  }
  if (tokenId === market.down_token_id) {
    return "DOWN";
  }
  return "n/a";
}

function bookPrice(value: unknown) {
  if (!value || typeof value !== "object") {
    return undefined;
  }
  return finiteNumber((value as Record<string, unknown>).price);
}
