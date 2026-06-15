"use client";

import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Download, RefreshCw } from "lucide-react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis
} from "recharts";
import { getLabArtifacts, getLabProspective, getLabSampleSizeLatest, getLatestLabReport } from "@/lib/api";
import type { JsonRecord, LabArtifact, LabReportBundle, ProspectiveValidationRow } from "@/lib/types";
import { compact, dateTime, numberText } from "@/lib/format";
import { EmptyState, IconButton, Panel, PanelHeader, Pill } from "@/components/ui";

export function ReportsPage() {
  const queryClient = useQueryClient();
  const latest = useQuery({ queryKey: ["labs", "reports", "latest"], queryFn: getLatestLabReport, retry: false });
  const artifacts = useQuery({ queryKey: ["labs", "artifacts", "reports"], queryFn: () => getLabArtifacts(""), retry: false });
  const prospective = useQuery({ queryKey: ["labs", "prospective"], queryFn: getLabProspective, retry: false });
  const sampleSize = useQuery({ queryKey: ["labs", "sample-size"], queryFn: getLabSampleSizeLatest, retry: false });

  const bundle = latest.data;
  const report = asRecord(bundle?.report);
  const cards = reportCards(bundle, sampleSize.data?.report);
  const fillModels = fillModelRows(bundle);
  const dailyRows = prospective.data?.result?.rows ?? [];

  return (
    <div className="space-y-5">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h1 className="text-xl font-semibold text-ink">Reports</h1>
        </div>
        <IconButton label="Refresh reports" onClick={() => queryClient.invalidateQueries({ queryKey: ["labs"] })}>
          <RefreshCw className="h-4 w-4" />
        </IconButton>
      </div>

      <div className="grid gap-3 md:grid-cols-3 xl:grid-cols-6">
        {cards.map((card) => (
          <div key={card.label} className="border border-line bg-white px-3 py-3 shadow-hairline">
            <div className="truncate text-xs text-ink/50">{card.label}</div>
            <div className="mt-1 truncate text-lg font-semibold text-ink">{card.value}</div>
            {card.tone ? <Pill tone={card.tone}>{card.meta}</Pill> : <div className="mt-1 truncate text-xs text-ink/50">{card.meta}</div>}
          </div>
        ))}
      </div>

      <div className="grid gap-5 xl:grid-cols-[1fr_420px]">
        <Panel>
          <PanelHeader title="Fill-Model Sensitivity" meta={bundle?.date ?? "latest daily research"} />
          {fillModels.length ? (
            <div className="h-[320px] p-4">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={fillModels}>
                  <CartesianGrid stroke="#d9ddd2" vertical={false} />
                  <XAxis dataKey="fill_model" tick={{ fontSize: 11 }} interval={0} angle={-20} textAnchor="end" height={70} />
                  <YAxis tick={{ fontSize: 11 }} />
                  <Tooltip />
                  <Bar dataKey="net_pnl" fill="#18705b" />
                </BarChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <EmptyState label={latest.isLoading ? "Loading report data" : "No fill-model report data"} />
          )}
        </Panel>

        <Panel>
          <PanelHeader title="Latest Summary" meta={report ? "final report" : "no report"} />
          {report ? (
            <div className="overflow-auto p-4">
              <table className="w-full min-w-[360px] text-left text-sm">
                <tbody>
                  {summaryRows(report).map(([key, value]) => (
                    <tr key={key} className="border-b border-line last:border-b-0">
                      <th className="w-44 bg-panel px-3 py-2 text-xs font-medium uppercase text-ink/50">{key}</th>
                      <td className="px-3 py-2 text-ink">{compact(value)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <EmptyState label={latest.isLoading ? "Loading report" : "No daily report loaded"} />
          )}
        </Panel>
      </div>

      <div className="grid gap-5 xl:grid-cols-2">
        <Panel>
          <PanelHeader title="Prospective Validation" meta={prospective.data?.result?.status ?? "collecting"} />
          {dailyRows.length ? (
            <div className="h-[280px] p-4">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={dailyRows.map(dailyChartRow)}>
                  <CartesianGrid stroke="#d9ddd2" vertical={false} />
                  <XAxis dataKey="date" tick={{ fontSize: 11 }} />
                  <YAxis tick={{ fontSize: 11 }} />
                  <Tooltip />
                  <Line type="monotone" dataKey="static" stroke="#17201b" strokeWidth={2} dot={false} />
                  <Line type="monotone" dataKey="dynamic" stroke="#18705b" strokeWidth={2} dot={false} />
                </LineChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <EmptyState label={prospective.isLoading ? "Loading prospective rows" : "No prospective rows yet"} />
          )}
        </Panel>

        <ArtifactBrowser artifacts={artifacts.data?.artifacts ?? []} loading={artifacts.isLoading} />
      </div>
    </div>
  );
}

function ArtifactBrowser({ artifacts, loading }: { artifacts: LabArtifact[]; loading: boolean }) {
  return (
    <Panel>
      <PanelHeader title="Artifacts" meta={`${artifacts.length} files`}>
        <Download className="h-4 w-4 text-ink/45" />
      </PanelHeader>
      {artifacts.length ? (
        <div className="max-h-[320px] overflow-auto">
          <table className="w-full min-w-[560px] text-left text-sm">
            <thead className="border-b border-line bg-panel text-xs uppercase text-ink/50">
              <tr>
                <th className="px-3 py-2">Path</th>
                <th className="px-3 py-2">Kind</th>
                <th className="px-3 py-2">Size</th>
                <th className="px-3 py-2">Modified</th>
              </tr>
            </thead>
            <tbody>
              {artifacts.slice(0, 80).map((artifact) => (
                <tr key={artifact.artifact_id} className="border-b border-line last:border-b-0">
                  <td className="px-3 py-2 font-mono text-xs">{artifact.path}</td>
                  <td className="px-3 py-2">{artifact.kind}</td>
                  <td className="px-3 py-2">{numberText(artifact.size_bytes)}</td>
                  <td className="px-3 py-2">{artifact.modified_ts ? dateTime(artifact.modified_ts) : "n/a"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <EmptyState label={loading ? "Loading artifacts" : "No artifacts found"} />
      )}
    </Panel>
  );
}

function reportCards(bundle?: LabReportBundle, sampleReport?: JsonRecord | null) {
  const report = asRecord(bundle?.report);
  const sample = asRecord(sampleReport ?? bundle?.sample_size);
  const recommendation =
    text(findDeep(report, "recommendation")) ??
    text(pointer(report, "/result/executive_summary/recommendation")) ??
    "collecting";
  const fillModels = fillModelRows(bundle);
  const staticModel = fillModels.find((row) => row.fill_model === "touch_after_250ms") ?? fillModels[0];
  const dynamic = profileNet(bundle, "dynamic_quote_style");
  const cleanMarkets = findDeep(report, "complete_for_simulation") ?? findDeep(report, "settled_markets");
  const ciLow = pointer(sample, "/result/statistics/ci_low") ?? findDeep(sample, "ci_low");
  const ciHigh = pointer(sample, "/result/statistics/ci_high") ?? findDeep(sample, "ci_high");
  const quality = text(findDeep(bundle?.audit, "status")) ?? text(findDeep(report, "data_quality_status")) ?? "unknown";
  return [
    { label: "Recommendation", value: recommendation, meta: "research", tone: "neutral" as const },
    { label: "Static Net PnL", value: numberText(staticModel?.net_pnl), meta: "touch_after_250ms" },
    { label: "Dynamic Quote", value: numberText(dynamic), meta: "frozen candidate" },
    { label: "Clean Markets", value: numberText(cleanMarkets), meta: "settled sample" },
    { label: "95% CI", value: `${numberText(ciLow)} / ${numberText(ciHigh)}`, meta: "market-level" },
    { label: "Data Quality", value: quality, meta: "latest verdict", tone: quality === "healthy" ? ("good" as const) : ("warn" as const) }
  ];
}

function fillModelRows(bundle?: LabReportBundle) {
  const source = bundle?.baseline ?? bundle?.report ?? {};
  const rows = findRows(asRecord(source), "fill_model");
  return rows
    .map((row) => ({
      fill_model: text(row.fill_model) ?? "unknown",
      net_pnl: number(row.net_pnl),
      max_drawdown: number(row.max_drawdown),
      fills: number(row.fills)
    }))
    .filter((row) => row.net_pnl !== null);
}

function profileNet(bundle: LabReportBundle | undefined, profile: string) {
  const rows = findRows(asRecord(bundle?.regimes ?? bundle?.report), "profile");
  return rows.find((row) => text(row.profile) === profile)?.net_pnl ?? null;
}

function dailyChartRow(row: ProspectiveValidationRow) {
  return {
    date: row.date,
    static: number(row.static_net_pnl),
    dynamic: number(row.dynamic_quote_style_net_pnl)
  };
}

function summaryRows(report: JsonRecord): [string, unknown][] {
  const executive = asRecord(pointer(report, "/result/executive_summary"));
  const stats = asRecord(pointer(report, "/result/statistical_evidence/result/statistics"));
  const rows: [string, unknown][] = [
    ["recommendation", executive?.recommendation],
    ["research_only", executive?.research_only],
    ["live_enabled", executive?.live_trading_enabled],
    ["ci_low", stats?.ci_low],
    ["ci_high", stats?.ci_high],
    ["required_n_0_05", stats?.required_n_for_plus_minus_0_05]
  ];
  return rows.filter(([, value]) => value !== undefined);
}

function findRows(value: JsonRecord | null | undefined, key: string): JsonRecord[] {
  if (!value) {
    return [];
  }
  const rows: JsonRecord[] = [];
  visit(value, (record) => {
    if (record[key] !== undefined) {
      rows.push(record);
    }
  });
  return rows;
}

function findDeep(value: unknown, key: string): unknown {
  let found: unknown;
  visit(value, (record) => {
    if (found === undefined && record[key] !== undefined) {
      found = record[key];
    }
  });
  return found;
}

function visit(value: unknown, fn: (record: JsonRecord) => void) {
  if (Array.isArray(value)) {
    value.forEach((item) => visit(item, fn));
    return;
  }
  const record = asRecord(value);
  if (!record) {
    return;
  }
  fn(record);
  Object.values(record).forEach((child) => visit(child, fn));
}

function pointer(record: JsonRecord | null | undefined, path: string): unknown {
  if (!record) {
    return undefined;
  }
  return path
    .split("/")
    .slice(1)
    .reduce<unknown>((current, key) => asRecord(current)?.[key], record);
}

function asRecord(value: unknown): JsonRecord | null {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as JsonRecord) : null;
}

function text(value: unknown) {
  return typeof value === "string" ? value : undefined;
}

function number(value: unknown) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}
