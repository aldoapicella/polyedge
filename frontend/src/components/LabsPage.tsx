"use client";

import { useQuery } from "@tanstack/react-query";
import { Beaker, RefreshCw } from "lucide-react";
import { useState } from "react";
import {
  getLabArtifacts,
  getLabCalibrationLatest,
  getLabFillModelsLatest,
  getLabProspective,
  getLabRegimesLatest,
  getLabSampleSizeLatest
} from "@/lib/api";
import type { JsonRecord, ProspectiveValidationRow } from "@/lib/types";
import { compact, numberText } from "@/lib/format";
import { EmptyState, IconButton, Panel, PanelHeader, Pill } from "@/components/ui";

const tabs = ["Overview", "Prospective Validation", "Regime Profiles", "Calibration", "Fill Models", "Sample Size", "Artifacts"] as const;

export function LabsPage() {
  const [tab, setTab] = useState<(typeof tabs)[number]>("Overview");
  const prospective = useQuery({ queryKey: ["labs", "prospective"], queryFn: getLabProspective, retry: false });
  const regimes = useQuery({ queryKey: ["labs", "regimes"], queryFn: getLabRegimesLatest, retry: false });
  const calibration = useQuery({ queryKey: ["labs", "calibration"], queryFn: getLabCalibrationLatest, retry: false });
  const fillModels = useQuery({ queryKey: ["labs", "fill-models"], queryFn: getLabFillModelsLatest, retry: false });
  const sampleSize = useQuery({ queryKey: ["labs", "sample-size"], queryFn: getLabSampleSizeLatest, retry: false });
  const artifacts = useQuery({ queryKey: ["labs", "artifacts", "labs"], queryFn: () => getLabArtifacts(""), retry: false });

  const rows = prospective.data?.result?.rows ?? [];
  const frozenCandidates = candidateRows(prospective.data?.result?.frozen_candidates);

  return (
    <div className="space-y-5">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h1 className="text-xl font-semibold text-ink">Labs</h1>
        </div>
        <IconButton label="Refresh labs" onClick={() => void Promise.all([prospective.refetch(), regimes.refetch(), calibration.refetch(), fillModels.refetch(), sampleSize.refetch(), artifacts.refetch()])}>
          <RefreshCw className="h-4 w-4" />
        </IconButton>
      </div>

      <div className="flex flex-wrap gap-1 border border-line bg-white p-1 shadow-hairline">
        {tabs.map((item) => (
          <button
            key={item}
            onClick={() => setTab(item)}
            className={`h-9 rounded-sm px-3 text-sm font-medium ${tab === item ? "bg-ink text-white" : "text-ink/70 hover:bg-panel"}`}
          >
            {item}
          </button>
        ))}
      </div>

      {tab === "Overview" ? <Overview rows={rows} candidates={frozenCandidates} /> : null}
      {tab === "Prospective Validation" ? <ProspectiveTable rows={rows} loading={prospective.isLoading} /> : null}
      {tab === "Regime Profiles" ? <GenericReport title="Regime Profiles" report={regimes.data?.report} keys={["profile", "net_pnl", "delta_vs_static", "regime_frequency", "regime_time_share", "fills", "cancels", "skipped_orders"]} /> : null}
      {tab === "Calibration" ? <GenericReport title="Calibration" report={calibration.data?.report} keys={["q_bucket", "decision_count", "avg_q_up", "observed_up_frequency", "calibration_error", "brier_score"]} /> : null}
      {tab === "Fill Models" ? <GenericReport title="Fill Models" report={fillModels.data?.report} keys={["fill_model", "net_pnl", "max_drawdown", "fills", "fill_rate", "cancel_fill_ratio", "queue_proxy"]} /> : null}
      {tab === "Sample Size" ? <SampleSizePanel report={sampleSize.data?.report} /> : null}
      {tab === "Artifacts" ? <ArtifactsPanel artifacts={artifacts.data?.artifacts ?? []} loading={artifacts.isLoading} /> : null}
    </div>
  );
}

function Overview({ rows, candidates }: { rows: ProspectiveValidationRow[]; candidates: JsonRecord[] }) {
  const latest = rows.at(-1);
  return (
    <div className="grid gap-5 xl:grid-cols-[360px_1fr]">
      <Panel>
        <PanelHeader title="Frozen Candidates" meta={`${candidates.length || 4} tracked`} />
        <div className="space-y-2 p-4">
          {(candidates.length ? candidates : fallbackCandidates()).map((candidate) => (
            <div key={String(candidate.name)} className="border border-line bg-panel px-3 py-2">
              <div className="flex items-center justify-between gap-2">
                <span className="truncate text-sm font-semibold text-ink">{String(candidate.name)}</span>
                <Pill tone="neutral">disabled</Pill>
              </div>
              <div className="mt-1 truncate text-xs text-ink/55">{String(candidate.profile ?? candidate.name)}</div>
            </div>
          ))}
        </div>
      </Panel>
      <Panel>
        <PanelHeader title="Prospective Status" meta={latest?.date ?? "collecting"} />
        {latest ? (
          <div className="grid gap-3 p-4 md:grid-cols-4">
            <Metric label="Static" value={latest.static_net_pnl} />
            <Metric label="Dynamic Quote" value={latest.dynamic_quote_style_net_pnl} />
            <Metric label="Full Deterministic" value={latest.full_deterministic_profile_net_pnl} />
            <Metric label="CI" value={`${numberText(latest.ci_95_low)} / ${numberText(latest.ci_95_high)}`} />
          </div>
        ) : (
          <EmptyState label="No prospective validation rows yet" />
        )}
      </Panel>
    </div>
  );
}

function ProspectiveTable({ rows, loading }: { rows: ProspectiveValidationRow[]; loading: boolean }) {
  if (!rows.length) {
    return <Panel><EmptyState label={loading ? "Loading prospective rows" : "No prospective rows yet"} /></Panel>;
  }
  return (
    <Panel>
      <PanelHeader title="Prospective Validation" meta={`${rows.length} rows`} />
      <div className="overflow-auto">
        <table className="w-full min-w-[1040px] text-left text-sm">
          <thead className="border-b border-line bg-panel text-xs uppercase text-ink/50">
            <tr>
              {["Date", "Markets", "Static", "Dynamic Quote", "Full Deterministic", "Fill Model", "Drawdown", "Cancel/Fill", "Quality", "Recommendation"].map((header) => (
                <th key={header} className="px-3 py-2">{header}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.date} className="border-b border-line last:border-b-0">
                <td className="px-3 py-2">{row.date}</td>
                <td className="px-3 py-2">{numberText(row.settled_markets)}</td>
                <td className="px-3 py-2">{numberText(row.static_net_pnl)}</td>
                <td className="px-3 py-2">{numberText(row.dynamic_quote_style_net_pnl)}</td>
                <td className="px-3 py-2">{numberText(row.full_deterministic_profile_net_pnl)}</td>
                <td className="px-3 py-2">{row.fill_model ?? "n/a"}</td>
                <td className="px-3 py-2">{numberText(row.max_drawdown)}</td>
                <td className="px-3 py-2">{numberText(row.cancel_per_fill)}</td>
                <td className="px-3 py-2"><Pill tone={row.data_quality_status === "healthy" ? "good" : "warn"}>{row.data_quality_status ?? "unknown"}</Pill></td>
                <td className="px-3 py-2">{row.recommendation ?? "collecting"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </Panel>
  );
}

function GenericReport({ title, report, keys }: { title: string; report?: JsonRecord | null; keys: string[] }) {
  const rows = findRows(report, keys);
  return (
    <Panel>
      <PanelHeader title={title} meta={`${rows.length} rows`} />
      {rows.length ? (
        <div className="overflow-auto">
          <table className="w-full min-w-[760px] text-left text-sm">
            <thead className="border-b border-line bg-panel text-xs uppercase text-ink/50">
              <tr>{keys.map((key) => <th key={key} className="px-3 py-2">{key}</th>)}</tr>
            </thead>
            <tbody>
              {rows.slice(0, 100).map((row, index) => (
                <tr key={index} className="border-b border-line last:border-b-0">
                  {keys.map((key) => <td key={key} className="px-3 py-2">{compact(row[key])}</td>)}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <EmptyState label="No report rows found" />
      )}
    </Panel>
  );
}

function SampleSizePanel({ report }: { report?: JsonRecord | null }) {
  const stats = asRecord(pointer(report, "/result/statistics"));
  return (
    <Panel>
      <PanelHeader title="Sample Size" meta="market-level confidence" />
      {stats ? (
        <div className="grid gap-3 p-4 md:grid-cols-4">
          <Metric label="N" value={stats.n} />
          <Metric label="Mean" value={stats.mean} />
          <Metric label="95% CI" value={`${numberText(stats.ci_low)} / ${numberText(stats.ci_high)}`} />
          <Metric label="Required N" value={stats.required_n_to_detect_observed_mean} />
        </div>
      ) : (
        <EmptyState label="No sample-size report found" />
      )}
    </Panel>
  );
}

function ArtifactsPanel({ artifacts, loading }: { artifacts: { artifact_id: string; path: string; kind: string }[]; loading: boolean }) {
  return (
    <Panel>
      <PanelHeader title="Artifacts" meta={`${artifacts.length} files`} />
      {artifacts.length ? (
        <div className="overflow-auto">
          <table className="w-full min-w-[640px] text-left text-sm">
            <tbody>
              {artifacts.slice(0, 100).map((artifact) => (
                <tr key={artifact.artifact_id} className="border-b border-line last:border-b-0">
                  <td className="px-3 py-2 font-mono text-xs">{artifact.path}</td>
                  <td className="px-3 py-2">{artifact.kind}</td>
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

function Metric({ label, value }: { label: string; value: unknown }) {
  return (
    <div className="border border-line bg-panel px-3 py-3">
      <div className="truncate text-xs text-ink/50">{label}</div>
      <div className="mt-1 truncate text-lg font-semibold text-ink">{numberText(value)}</div>
    </div>
  );
}

function candidateRows(value: unknown): JsonRecord[] {
  const record = asRecord(value);
  return Array.isArray(record?.candidates) ? (record.candidates.filter(Boolean) as JsonRecord[]) : [];
}

function fallbackCandidates(): JsonRecord[] {
  return ["static_baseline", "dynamic_quote_style", "full_deterministic_profile", "dynamic_safety_only"].map((name) => ({ name, profile: name }));
}

function findRows(value: unknown, keys: string[]): JsonRecord[] {
  const rows: JsonRecord[] = [];
  visit(value, (record) => {
    if (keys.some((key) => record[key] !== undefined)) {
      rows.push(record);
    }
  });
  return rows;
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

function pointer(record: unknown, path: string): unknown {
  return path
    .split("/")
    .slice(1)
    .reduce<unknown>((current, key) => asRecord(current)?.[key], record);
}

function asRecord(value: unknown): JsonRecord | null {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as JsonRecord) : null;
}
