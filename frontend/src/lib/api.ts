"use client";

import type {
  ConfigAuditEntry,
  ConfigValidation,
  MarketDetail,
  ReportJob,
  ReportPayload,
  RuntimeEvent,
  RuntimeConfig,
  RuntimeConfigPatch,
  Snapshot
} from "@/lib/types";
import type { ChartRange, MarketSeries } from "@/lib/charting";

type FetchOptions = {
  method?: "GET" | "POST";
  body?: unknown;
};

export async function backendFetch<T>(path: string, options: FetchOptions = {}): Promise<T> {
  const response = await fetch(`/api/backend/${path.replace(/^\//, "")}`, {
    method: options.method ?? "GET",
    headers: options.body ? { "Content-Type": "application/json" } : undefined,
    body: options.body ? JSON.stringify(options.body) : undefined,
    cache: "no-store"
  });
  const text = await response.text();
  const payload = text ? JSON.parse(text) : null;
  if (!response.ok) {
    const detail = payload?.detail ?? payload?.error ?? response.statusText;
    throw new Error(typeof detail === "string" ? detail : JSON.stringify(detail));
  }
  return payload as T;
}

export function getSnapshot() {
  return backendFetch<Snapshot>("snapshot");
}

export function getMarketDetail(marketId: string) {
  return backendFetch<MarketDetail>(`markets/${encodeURIComponent(marketId)}`);
}

export function getHistoricalMarkets(limit = 200) {
  return backendFetch<{ markets: Snapshot["markets"] }>(`markets/history?limit=${limit}`);
}

export function getMarketChart(marketId: string, range: ChartRange = "full") {
  const query = new URLSearchParams({ range });
  return backendFetch<MarketSeries>(`markets/${encodeURIComponent(marketId)}/chart?${query.toString()}`);
}

export function getRecentEvents(params: { marketId?: string; type?: string; limit?: number } = {}) {
  const query = new URLSearchParams();
  if (params.marketId) {
    query.set("market_id", params.marketId);
  }
  if (params.type) {
    query.set("type", params.type);
  }
  if (params.limit) {
    query.set("limit", String(params.limit));
  }
  return backendFetch<{ source?: string; warning?: string; events: RuntimeEvent[] }>(
    `events/recent${query.size ? `?${query.toString()}` : ""}`
  );
}

export function getLatestReport() {
  return backendFetch<ReportPayload>("reports/latest");
}

export function getDailyReport(date: string) {
  return backendFetch<ReportPayload>(`reports/daily/${date}`);
}

export async function buildReport(body: {
  source: "auto" | "local" | "azure";
  prefix?: string | null;
  date?: string | null;
  force?: boolean;
  settlement_window_seconds?: number;
}) {
  const payload = await backendFetch<ReportPayload | ReportJob>("reports/build", {
    method: "POST",
    body
  });
  if ("job_id" in payload) {
    return { job: payload, report: null } satisfies ReportPayload;
  }
  return payload;
}

export function getConfig() {
  return backendFetch<RuntimeConfig>("config/current");
}

export function validateConfig(config: RuntimeConfigPatch) {
  return backendFetch<ConfigValidation>("config/validate", {
    method: "POST",
    body: config
  });
}

export function applyConfig(config: RuntimeConfigPatch, reason: string) {
  return backendFetch<{ applied: boolean; audit_version: string; validation: ConfigValidation; config: RuntimeConfig }>(
    "config/apply",
    {
      method: "POST",
      body: {
        config,
        reason,
        source: "ui"
      }
    }
  );
}

export function getConfigHistory(limit = 20) {
  return backendFetch<{ history: ConfigAuditEntry[] }>(`config/history?limit=${limit}`);
}

export function rollbackConfig(version: string, reason: string) {
  const query = new URLSearchParams({ reason, source: "ui" });
  return backendFetch<{ applied: boolean; audit_version: string; config: RuntimeConfig }>(
    `config/rollback/${encodeURIComponent(version)}?${query.toString()}`,
    { method: "POST" }
  );
}

export function setKillSwitch(enabled: boolean, reason: string) {
  return backendFetch<{ enabled: boolean; audit_version: string }>("control/kill-switch", {
    method: "POST",
    body: {
      enabled,
      reason,
      source: "ui"
    }
  });
}

export function pauseBot(reason: string) {
  return backendFetch<{ control: { paused: boolean }; audit_version: string }>("control/pause", {
    method: "POST",
    body: {
      reason,
      source: "ui"
    }
  });
}

export function resumeBot(reason: string) {
  return backendFetch<{ control: { paused: boolean }; audit_version: string }>("control/resume", {
    method: "POST",
    body: {
      reason,
      source: "ui"
    }
  });
}
