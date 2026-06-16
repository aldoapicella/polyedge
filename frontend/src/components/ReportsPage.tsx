"use client";

import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Download, RefreshCw } from "lucide-react";
import { useState } from "react";
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
import { getLabArtifact, getLabArtifacts, getLabProspective, getLabSampleSizeLatest, getLatestLabReport } from "@/lib/api";
import type { JsonRecord, LabArtifact, LabArtifactPayload, LabReportBundle, ProspectiveValidationRow } from "@/lib/types";
import { compact, dateTime, numberText } from "@/lib/format";
import { selectFillModelSummaryRows, selectRegimeProfileRows } from "@/lib/reportRows";
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
  const [selected, setSelected] = useState<LabArtifact | null>(null);
  const artifact = useQuery({
    queryKey: ["labs", "artifact", selected?.artifact_id],
    queryFn: () => getLabArtifact(selected?.artifact_id ?? ""),
    enabled: Boolean(selected?.artifact_id),
    retry: false
  });
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
                  <td className="px-3 py-2 font-mono text-xs">
                    <button className="text-left text-good hover:underline" onClick={() => setSelected(artifact)}>
                      {artifact.path}
                    </button>
                  </td>
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
      {selected ? (
        <ArtifactPreview artifact={artifact.data ?? null} loading={artifact.isLoading} error={artifact.error?.message} />
      ) : null}
    </Panel>
  );
}

function ArtifactPreview({
  artifact,
  loading,
  error
}: {
  artifact: LabArtifactPayload | null;
  loading: boolean;
  error?: string;
}) {
  return (
    <div className="border-t border-line bg-panel p-3">
      <div className="mb-2 text-xs font-semibold uppercase text-ink/50">
        {artifact?.path ?? (loading ? "Loading artifact" : "Artifact")}
      </div>
      {error ? <div className="text-sm text-danger">{error}</div> : null}
      {!error && loading ? <div className="text-sm text-ink/55">Loading artifact</div> : null}
      {!error && artifact ? (
        <pre className="max-h-72 overflow-auto border border-line bg-white p-3 text-xs leading-relaxed text-ink/75">
          {artifact.kind === "json" ? JSON.stringify(artifact.content, null, 2) : String(artifact.content ?? "")}
        </pre>
      ) : null}
    </div>
  );
}

function reportCards(bundle?: LabReportBundle, sampleReport?: JsonRecord | null) {
  const report = asRecord(bundle?.report);
  const sample = asRecord(sampleReport ?? bundle?.sample_size);
  const recommendation =
    text(firstPointer(report, [
      "/result/executive_summary/recommendation",
      "/result/recommendation",
      "/recommendation"
    ])) ??
    "collecting";
  const fillModels = fillModelRows(bundle);
  const staticModel = fillModels.find((row) => row.fill_model === "touch_after_250ms") ?? fillModels[0];
  const dynamic = profileNet(bundle, "dynamic_quote_style");
  const cleanMarkets = firstPointer(report, [
    "/result/executive_summary/complete_for_simulation",
    "/result/executive_summary/settled_markets",
    "/result/statistical_evidence/result/statistics/n",
    "/result/statistics/n",
    "/summary/settled_markets"
  ]);
  const ciLow = firstPointer(sample, [
    "/result/statistics/ci_low",
    "/statistics/ci_low",
    "/sample_size/result/statistics/ci_low"
  ]);
  const ciHigh = firstPointer(sample, [
    "/result/statistics/ci_high",
    "/statistics/ci_high",
    "/sample_size/result/statistics/ci_high"
  ]);
  const quality =
    text(firstPointer(asRecord(bundle?.audit), ["/status", "/result/status"])) ??
    text(firstPointer(report, [
      "/result/executive_summary/data_quality_status",
      "/result/data_quality_status",
      "/data_quality_status"
    ])) ??
    "unknown";
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
  return selectFillModelSummaryRows(source)
    .map((row) => ({
      fill_model: text(row.fill_model) ?? "unknown",
      net_pnl: number(row.net_pnl),
      max_drawdown: number(row.max_drawdown),
      fills: number(row.fills)
    }))
    .filter((row) => row.net_pnl !== null);
}

function profileNet(bundle: LabReportBundle | undefined, profile: string) {
  const rows = selectRegimeProfileRows(bundle?.regimes ?? bundle?.report);
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

function pointer(record: JsonRecord | null | undefined, path: string): unknown {
  if (!record) {
    return undefined;
  }
  return path
    .split("/")
    .slice(1)
    .reduce<unknown>((current, key) => asRecord(current)?.[key], record);
}

function firstPointer(record: JsonRecord | null | undefined, paths: string[]): unknown {
  for (const path of paths) {
    const value = pointer(record, path);
    if (value !== undefined && value !== null && value !== "") {
      return value;
    }
  }
  return undefined;
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
