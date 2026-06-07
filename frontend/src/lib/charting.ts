import type { MarketSummary } from "@/lib/types";

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
  referencePrice?: number;
  fillPrice?: number;
  fillOutcome?: string;
  fillSize?: number;
};

export type MarketSeries = {
  source?: string;
  warning?: string | null;
  market_id?: string;
  range?: ChartRange;
  marketChart: ChartPoint[];
  fills: ChartPoint[];
  domain: [number, number];
  sampleCount: number;
};

export function emptyMarketSeries(market?: MarketSummary | null, range: ChartRange = "full"): MarketSeries {
  return {
    range,
    marketChart: [],
    fills: [],
    domain: marketWindowDomain(market, Date.now()),
    sampleCount: 0
  };
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
