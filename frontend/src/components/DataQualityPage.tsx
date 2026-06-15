"use client";

import { useQuery, useQueryClient } from "@tanstack/react-query";
import { RefreshCw } from "lucide-react";
import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { getLabDataQualityLatest, getLabExclusions, getLabHourlyQuality, validateLabExclusions } from "@/lib/api";
import type { ExclusionWindow, JsonRecord } from "@/lib/types";
import { ageText, compact, dateTime, numberText, pctText } from "@/lib/format";
import { EmptyState, IconButton, Panel, PanelHeader, Pill } from "@/components/ui";

export function DataQualityPage() {
  const queryClient = useQueryClient();
  const today = new Date().toISOString().slice(0, 10);
  const latest = useQuery({ queryKey: ["labs", "data-quality", "latest"], queryFn: getLabDataQualityLatest, retry: false, refetchInterval: 30000 });
  const hourly = useQuery({ queryKey: ["labs", "data-quality", "hourly", today], queryFn: () => getLabHourlyQuality(today), retry: false });
  const exclusions = useQuery({ queryKey: ["labs", "data-quality", "exclusions"], queryFn: getLabExclusions, retry: false });
  const validation = useQuery({ queryKey: ["labs", "data-quality", "exclusions", "validate"], queryFn: validateLabExclusions, retry: false });

  const freshness = asRecord(latest.data?.freshness)?.result ? asRecord(asRecord(latest.data?.freshness)?.result) : asRecord(latest.data?.freshness);
  const recorder = asRecord(latest.data?.recorder);
  const windows = exclusions.data?.windows ?? latest.data?.exclusions?.windows ?? [];
  const chartData = freshness ? [freshnessChartRow(freshness)] : [];

  return (
    <div className="space-y-5">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h1 className="text-xl font-semibold text-ink">Data Quality</h1>
        </div>
        <IconButton label="Refresh data quality" onClick={() => queryClient.invalidateQueries({ queryKey: ["labs", "data-quality"] })}>
          <RefreshCw className="h-4 w-4" />
        </IconButton>
      </div>

      <div className="grid gap-3 md:grid-cols-4 xl:grid-cols-8">
        <Metric label="Blob Age" value={ageFromFreshness(freshness)} />
        <Metric label="Blob Size" value={freshness?.latest_blob_size} />
        <Metric label="Hour Blobs" value={freshness?.current_hour_blob_count} />
        <Metric label="Tiny Ratio" value={pctValue(freshness?.tiny_blob_ratio)} />
        <Metric label="Worker" value={workerStatus(recorder)} tone={workerStatus(recorder) === "healthy" ? "good" : "warn"} />
        <Metric label="Dropped" value={recorderValue(recorder, "dropped_count")} />
        <Metric label="Errors" value={recorderValue(recorder, "error_count") ?? recorderValue(recorder, "failed_total")} />
        <Metric label="Exclusions" value={windows.filter((window) => window.default_exclude).length} />
      </div>

      <div className="grid gap-5 xl:grid-cols-[1fr_420px]">
        <Panel>
          <PanelHeader title="Freshness Snapshot" meta={String(freshness?.status ?? "unknown")} />
          {freshness ? (
            <div className="h-[280px] p-4">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={chartData}>
                  <CartesianGrid stroke="#d9ddd2" vertical={false} />
                  <XAxis dataKey="label" tick={{ fontSize: 11 }} />
                  <YAxis tick={{ fontSize: 11 }} />
                  <Tooltip />
                  <Bar dataKey="blob_size_mb" fill="#18705b" />
                  <Bar dataKey="hour_blobs" fill="#17201b" />
                  <Bar dataKey="tiny_ratio_pct" fill="#a45d13" />
                </BarChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <EmptyState label={latest.isLoading ? "Loading freshness" : "No freshness snapshot found"} />
          )}
        </Panel>

        <Panel>
          <PanelHeader title="Exclusion Registry" meta={validation.data?.valid ? "valid" : "pending"} />
          <div className="space-y-2 p-4">
            {windows.map((window) => (
              <ExclusionRow key={window.id} window={window} />
            ))}
            {!windows.length ? <EmptyState label="No exclusion windows found" /> : null}
          </div>
        </Panel>
      </div>

      <div className="grid gap-5 xl:grid-cols-2">
        <Panel>
          <PanelHeader title="Hourly Audits" meta={hourly.data?.date ?? today} />
          <AuditTable audits={hourly.data?.audits ?? []} loading={hourly.isLoading} />
        </Panel>
        <Panel>
          <PanelHeader title="Recorder Metrics" meta={latest.data?.generated_ts ? dateTime(latest.data.generated_ts) : "latest"} />
          {recorder ? <KeyValueTable value={recorder} /> : <EmptyState label={latest.isLoading ? "Loading recorder" : "No recorder metrics"} />}
        </Panel>
      </div>
    </div>
  );
}

function Metric({ label, value, tone }: { label: string; value: unknown; tone?: "good" | "warn" | "danger" }) {
  return (
    <div className="border border-line bg-white px-3 py-3 shadow-hairline">
      <div className="truncate text-xs text-ink/50">{label}</div>
      <div className="mt-1 truncate text-lg font-semibold text-ink">{numberText(value)}</div>
      {tone ? <Pill tone={tone}>{String(value)}</Pill> : null}
    </div>
  );
}

function ExclusionRow({ window }: { window: ExclusionWindow }) {
  return (
    <div className="border border-line bg-panel px-3 py-2">
      <div className="flex items-center justify-between gap-2">
        <span className="truncate text-sm font-semibold text-ink">{window.id}</span>
        <Pill tone={window.default_exclude ? "warn" : "neutral"}>{window.default_exclude ? "active" : "inactive"}</Pill>
      </div>
      <div className="mt-1 text-xs text-ink/60">{dateTime(window.start)} to {dateTime(window.end)}</div>
      <div className="mt-1 font-mono text-[11px] text-ink/45">{window.start}..{window.end}</div>
      <div className="mt-1 text-xs text-ink/55">{window.reason}</div>
    </div>
  );
}

function AuditTable({ audits, loading }: { audits: JsonRecord[]; loading: boolean }) {
  if (!audits.length) {
    return <EmptyState label={loading ? "Loading audits" : "No hourly audits found"} />;
  }
  return (
    <div className="max-h-[360px] overflow-auto">
      <table className="w-full min-w-[680px] text-left text-sm">
        <thead className="border-b border-line bg-panel text-xs uppercase text-ink/50">
          <tr>
            {["Generated", "Events", "Malformed", "Excluded", "Warnings"].map((header) => (
              <th key={header} className="px-3 py-2">{header}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {audits.map((audit, index) => {
            const result = asRecord(audit.result);
            return (
              <tr key={index} className="border-b border-line last:border-b-0">
                <td className="px-3 py-2">{dateTime(String(audit.generated_at ?? ""))}</td>
                <td className="px-3 py-2">{numberText(result?.total_events)}</td>
                <td className="px-3 py-2">{numberText(result?.malformed_lines)}</td>
                <td className="px-3 py-2">{numberText(result?.excluded_event_count)}</td>
                <td className="px-3 py-2">{Array.isArray(result?.warnings) ? result.warnings.length : 0}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function KeyValueTable({ value }: { value: JsonRecord }) {
  return (
    <div className="max-h-[360px] overflow-auto">
      <table className="w-full min-w-[420px] text-left text-sm">
        <tbody>
          {Object.entries(value).slice(0, 40).map(([key, child]) => (
            <tr key={key} className="border-b border-line last:border-b-0">
              <th className="w-56 bg-panel px-3 py-2 text-xs font-medium uppercase text-ink/50">{key}</th>
              <td className="px-3 py-2">{compact(child)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function freshnessChartRow(freshness: JsonRecord) {
  return {
    label: "latest",
    blob_size_mb: Number(freshness.latest_blob_size ?? 0) / 1024 / 1024,
    hour_blobs: Number(freshness.current_hour_blob_count ?? 0),
    tiny_ratio_pct: Number(freshness.tiny_blob_ratio ?? 0) * 100
  };
}

function ageFromFreshness(freshness: JsonRecord | null) {
  const modified = typeof freshness?.latest_blob_last_modified === "string" ? freshness.latest_blob_last_modified : null;
  return modified ? ageText(modified) : freshness?.latest_blob_age_seconds;
}

function pctValue(value: unknown) {
  return typeof value === "number" || typeof value === "string" ? pctText(Number(value)) : "n/a";
}

function workerStatus(recorder: JsonRecord | null) {
  if (!recorder) {
    return "unknown";
  }
  if (recorder.worker_alive === false || Number(recorder.failed_total ?? 0) > 0 || Number(recorder.dropped_count ?? 0) > 0) {
    return "warning";
  }
  return "healthy";
}

function recorderValue(recorder: JsonRecord | null, key: string) {
  return recorder?.[key] ?? asRecord(recorder?.recorder_metrics)?.[key];
}

function asRecord(value: unknown): JsonRecord | null {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as JsonRecord) : null;
}
