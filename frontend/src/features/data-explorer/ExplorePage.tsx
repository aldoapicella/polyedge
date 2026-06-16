"use client";

import { useMutation, useQuery } from "@tanstack/react-query";
import { Download, FileJson, Play, Plus, Search, X } from "lucide-react";
import { useEffect, useMemo, useState } from "react";
import { Bar, BarChart, CartesianGrid, Line, LineChart, ResponsiveContainer, Scatter, ScatterChart, Tooltip, XAxis, YAxis } from "recharts";
import { getQuerySchema, getQueryTemplates, runQuery } from "@/lib/api";
import type { JsonRecord, QueryDatasetSchema, QueryFilter, QueryRequest, QueryResult, QueryTemplate } from "@/lib/types";
import { compact, numberText } from "@/lib/format";
import { downloadCsv, rowsToCsv } from "@/shared/query/csv";
import { VirtualTable } from "@/shared/ui/VirtualTable";
import { Button, EmptyState, IconButton, Panel, PanelHeader, Pill } from "@/components/ui";

type OutputMode = "table" | "bar" | "line" | "scatter";
type BuilderFilter = QueryFilter & { id: string };

const DEFAULT_DATASET = "markets";

export function ExplorePage() {
  const schema = useQuery({ queryKey: ["query", "schema"], queryFn: getQuerySchema, retry: false });
  const templates = useQuery({ queryKey: ["query", "templates"], queryFn: getQueryTemplates, retry: false });
  const [dataset, setDataset] = useState(DEFAULT_DATASET);
  const [filters, setFilters] = useState<BuilderFilter[]>([]);
  const [groupBy, setGroupBy] = useState<string[]>([]);
  const [metrics, setMetrics] = useState<string[]>(["count"]);
  const [limit, setLimit] = useState(250);
  const [outputMode, setOutputMode] = useState<OutputMode>("table");
  const [selectedRow, setSelectedRow] = useState<JsonRecord | null>(null);
  const query = useMutation({ mutationFn: runQuery });
  const datasetSchema = schema.data?.datasets.find((item) => item.id === dataset) ?? schema.data?.datasets[0];

  useEffect(() => {
    if (!datasetSchema) {
      return;
    }
    setGroupBy([]);
    setMetrics(datasetSchema.metrics.includes("count") ? ["count"] : datasetSchema.metrics.slice(0, 1));
    setFilters([]);
  }, [datasetSchema?.id]);

  const request = useMemo(
    () => buildRequest(dataset, filters, groupBy, metrics, limit),
    [dataset, filters, groupBy, limit, metrics]
  );

  return (
    <div className="space-y-5">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h1 className="text-xl font-semibold text-ink">Data Explorer</h1>
          <p className="mt-1 text-sm text-ink/55">Structured, read-only queries over curated PolyEdge datasets.</p>
        </div>
        <div className="flex flex-wrap gap-2">
          <Pill tone="good">structured only</Pill>
          <Pill tone="good">live trading disabled</Pill>
          <IconButton label="Run query" onClick={() => query.mutate(request)} disabled={query.isPending}>
            <Play className="h-4 w-4" />
          </IconButton>
        </div>
      </div>

      <div className="grid gap-5 xl:grid-cols-[360px_1fr]">
        <div className="space-y-5">
          <DatasetPanel datasets={schema.data?.datasets ?? []} dataset={dataset} onChange={setDataset} loading={schema.isLoading} />
          <TemplatePanel
            templates={templates.data?.templates ?? []}
            onSelect={(template) => {
              applyTemplate(template, setDataset, setFilters, setGroupBy, setMetrics, setLimit);
              query.mutate({ ...template.request, limit: template.request.limit ?? limit });
            }}
          />
        </div>

        <div className="space-y-5">
          <BuilderPanel
            dataset={datasetSchema}
            filters={filters}
            groupBy={groupBy}
            metrics={metrics}
            limit={limit}
            outputMode={outputMode}
            onAddFilter={() => setFilters((current) => [...current, defaultFilter(datasetSchema)])}
            onUpdateFilter={(id, patch) => setFilters((current) => current.map((filter) => (filter.id === id ? { ...filter, ...patch } : filter)))}
            onRemoveFilter={(id) => setFilters((current) => current.filter((filter) => filter.id !== id))}
            onToggleGroup={(field) => setGroupBy((current) => toggle(current, field))}
            onToggleMetric={(metric) => setMetrics((current) => toggle(current, metric))}
            onLimitChange={setLimit}
            onOutputModeChange={setOutputMode}
            onRun={() => query.mutate(request)}
            running={query.isPending}
          />

          <ResultPanel
            result={query.data}
            error={query.error?.message}
            loading={query.isPending}
            outputMode={outputMode}
            onCsv={() => {
              if (query.data) {
                downloadCsv(`polyedge-${query.data.dataset}.csv`, rowsToCsv(query.data.rows, query.data.columns));
              }
            }}
            onRowSelect={setSelectedRow}
          />
        </div>
      </div>

      {selectedRow ? <RowDrawer row={selectedRow} onClose={() => setSelectedRow(null)} /> : null}
    </div>
  );
}

function DatasetPanel({
  datasets,
  dataset,
  onChange,
  loading
}: {
  datasets: QueryDatasetSchema[];
  dataset: string;
  onChange: (dataset: string) => void;
  loading: boolean;
}) {
  return (
    <Panel>
      <PanelHeader title="Curated Datasets" meta={`${datasets.length} available`} />
      <div className="space-y-2 p-3">
        {datasets.map((item) => (
          <button
            key={item.id}
            className={[
              "w-full border px-3 py-2 text-left transition",
              dataset === item.id ? "border-ink bg-ink text-white" : "border-line bg-white text-ink/75 hover:bg-panel"
            ].join(" ")}
            onClick={() => onChange(item.id)}
          >
            <span className="block text-sm font-semibold">{item.label}</span>
            <span className={dataset === item.id ? "text-xs text-white/65" : "text-xs text-ink/45"}>
              {item.filters.slice(0, 4).join(", ")}
            </span>
          </button>
        ))}
        {!datasets.length ? <EmptyState label={loading ? "Loading query schema" : "Query schema unavailable"} /> : null}
      </div>
    </Panel>
  );
}

function TemplatePanel({ templates, onSelect }: { templates: QueryTemplate[]; onSelect: (template: QueryTemplate) => void }) {
  return (
    <Panel>
      <PanelHeader title="Query Templates" meta={`${templates.length} saved views`} />
      <div className="max-h-[420px] space-y-2 overflow-auto p-3">
        {templates.map((template) => (
          <button key={template.id} className="w-full border border-line bg-white px-3 py-2 text-left hover:bg-panel" onClick={() => onSelect(template)}>
            <span className="block text-sm font-semibold text-ink">{template.name}</span>
            <span className="block truncate text-xs text-ink/50">{template.request.dataset} · {template.request.group_by?.join(", ") || "ungrouped"}</span>
          </button>
        ))}
        {!templates.length ? <EmptyState label="No query templates returned" /> : null}
      </div>
    </Panel>
  );
}

function BuilderPanel({
  dataset,
  filters,
  groupBy,
  metrics,
  limit,
  outputMode,
  onAddFilter,
  onUpdateFilter,
  onRemoveFilter,
  onToggleGroup,
  onToggleMetric,
  onLimitChange,
  onOutputModeChange,
  onRun,
  running
}: {
  dataset?: QueryDatasetSchema;
  filters: BuilderFilter[];
  groupBy: string[];
  metrics: string[];
  limit: number;
  outputMode: OutputMode;
  onAddFilter: () => void;
  onUpdateFilter: (id: string, patch: Partial<QueryFilter>) => void;
  onRemoveFilter: (id: string) => void;
  onToggleGroup: (field: string) => void;
  onToggleMetric: (metric: string) => void;
  onLimitChange: (limit: number) => void;
  onOutputModeChange: (mode: OutputMode) => void;
  onRun: () => void;
  running: boolean;
}) {
  return (
    <Panel>
      <PanelHeader title="Query Builder" meta={dataset?.label ?? "select dataset"} help="The browser sends structured query JSON only; no arbitrary SQL or KQL is accepted.">
        <Button onClick={onRun} disabled={running}>
          <Play className="h-4 w-4" />
          Run
        </Button>
      </PanelHeader>
      <div className="space-y-4 p-4">
        <div>
          <div className="mb-2 flex items-center justify-between">
            <h2 className="text-xs font-semibold uppercase text-ink/50">Filters</h2>
            <Button className="h-8 px-2 text-xs" onClick={onAddFilter} disabled={!dataset}>
              <Plus className="h-3.5 w-3.5" />
              Filter
            </Button>
          </div>
          <div className="space-y-2">
            {filters.map((filter) => (
              <div key={filter.id} className="grid gap-2 md:grid-cols-[1fr_120px_1fr_36px]">
                <select className="h-9 border border-line bg-white px-2 text-sm" value={filter.field} onChange={(event) => onUpdateFilter(filter.id, { field: event.target.value })}>
                  {(dataset?.filters ?? []).map((field) => <option key={field}>{field}</option>)}
                </select>
                <select className="h-9 border border-line bg-white px-2 text-sm" value={filter.op} onChange={(event) => onUpdateFilter(filter.id, { op: event.target.value as QueryFilter["op"] })}>
                  {["eq", "ne", "contains", "gt", "gte", "lt", "lte", "in"].map((op) => <option key={op}>{op}</option>)}
                </select>
                <label className="flex h-9 items-center gap-2 border border-line bg-white px-2">
                  <Search className="h-4 w-4 text-ink/40" />
                  <input className="min-w-0 flex-1 bg-transparent text-sm outline-none" value={String(filter.value ?? "")} onChange={(event) => onUpdateFilter(filter.id, { value: event.target.value })} />
                </label>
                <IconButton label="Remove filter" className="h-9 w-9" onClick={() => onRemoveFilter(filter.id)}>
                  <X className="h-4 w-4" />
                </IconButton>
              </div>
            ))}
            {!filters.length ? <div className="border border-dashed border-line bg-panel px-3 py-3 text-sm text-ink/55">No filters. Add one to narrow the dataset.</div> : null}
          </div>
        </div>

        <Selector label="Group By" values={dataset?.group_by ?? []} selected={groupBy} onToggle={onToggleGroup} />
        <Selector label="Metrics" values={dataset?.metrics ?? []} selected={metrics} onToggle={onToggleMetric} />

        <div className="grid gap-3 md:grid-cols-[160px_1fr]">
          <label className="text-xs font-semibold uppercase text-ink/50">
            Limit
            <input
              type="number"
              min={1}
              max={dataset?.max_limit ?? 1000}
              className="mt-1 h-9 w-full border border-line bg-white px-2 text-sm normal-case text-ink"
              value={limit}
              onChange={(event) => onLimitChange(Math.max(1, Number(event.target.value) || 1))}
            />
          </label>
          <div>
            <div className="mb-1 text-xs font-semibold uppercase text-ink/50">Output</div>
            <div className="flex flex-wrap gap-1">
              {(["table", "bar", "line", "scatter"] as OutputMode[]).map((mode) => (
                <button
                  key={mode}
                  className={[
                    "h-9 rounded-sm border px-3 text-sm font-medium",
                    outputMode === mode ? "border-good bg-good text-white" : "border-line bg-white text-ink/65 hover:bg-panel"
                  ].join(" ")}
                  onClick={() => onOutputModeChange(mode)}
                >
                  {mode}
                </button>
              ))}
            </div>
          </div>
        </div>
      </div>
    </Panel>
  );
}

function Selector({
  label,
  values,
  selected,
  onToggle
}: {
  label: string;
  values: string[];
  selected: string[];
  onToggle: (value: string) => void;
}) {
  return (
    <div>
      <div className="mb-2 text-xs font-semibold uppercase text-ink/50">{label}</div>
      <div className="flex flex-wrap gap-2">
        {values.map((value) => (
          <label key={value} className="inline-flex h-8 items-center gap-2 border border-line bg-white px-2 text-xs text-ink/70">
            <input type="checkbox" checked={selected.includes(value)} onChange={() => onToggle(value)} />
            {value}
          </label>
        ))}
        {!values.length ? <span className="text-sm text-ink/50">No fields returned by schema.</span> : null}
      </div>
    </div>
  );
}

function ResultPanel({
  result,
  error,
  loading,
  outputMode,
  onCsv,
  onRowSelect
}: {
  result?: QueryResult;
  error?: string;
  loading: boolean;
  outputMode: OutputMode;
  onCsv: () => void;
  onRowSelect: (row: JsonRecord) => void;
}) {
  return (
    <Panel>
      <PanelHeader
        title="Query Output"
        meta={result ? `${result.returned_rows}/${result.total_rows} rows · ${result.dataset}` : "not run"}
        help="Results are limited and paginated by the backend. Use CSV export for the current returned page."
      >
        <Button onClick={onCsv} disabled={!result?.rows.length}>
          <Download className="h-4 w-4" />
          CSV
        </Button>
      </PanelHeader>
      {error ? <div className="border-b border-line px-4 py-3 text-sm text-danger">{error}</div> : null}
      {result?.warnings?.length ? <div className="border-b border-line px-4 py-3 text-sm text-warn">{result.warnings.join(", ")}</div> : null}
      <div className="p-4">
        {loading ? <EmptyState label="Running structured query" /> : null}
        {!loading && result?.rows.length ? (
          outputMode === "table" ? (
            <VirtualTable rows={result.rows} columns={result.columns} onRowSelect={onRowSelect} />
          ) : (
            <ChartOutput result={result} mode={outputMode} />
          )
        ) : null}
        {!loading && !result ? <EmptyState label="Run a query or choose a template to inspect curated data." /> : null}
        {!loading && result && !result.rows.length ? <EmptyState label="No rows matched. Change filters, choose another dataset, or increase the date range." /> : null}
      </div>
    </Panel>
  );
}

function ChartOutput({ result, mode }: { result: QueryResult; mode: OutputMode }) {
  const xField = result.columns.find((column) => column.kind !== "number")?.field ?? result.columns[0]?.field;
  const numericColumns = result.columns.filter((column) => column.kind === "number");
  const yField = numericColumns[0]?.field;
  const y2Field = numericColumns[1]?.field;
  if (!xField || !yField) {
    return <EmptyState label="Select a grouped query with at least one numeric metric for chart output." />;
  }
  return (
    <div className="h-[420px]">
      <ResponsiveContainer width="100%" height="100%">
        {mode === "bar" ? (
          <BarChart data={result.rows}>
            <CartesianGrid stroke="#d9ddd2" vertical={false} />
            <XAxis dataKey={xField} tick={{ fontSize: 11 }} />
            <YAxis tick={{ fontSize: 11 }} />
            <Tooltip formatter={(value) => numberText(value)} />
            <Bar dataKey={yField} fill="#18705b" isAnimationActive={false} />
          </BarChart>
        ) : mode === "scatter" && y2Field ? (
          <ScatterChart data={result.rows}>
            <CartesianGrid stroke="#d9ddd2" />
            <XAxis dataKey={yField} type="number" tick={{ fontSize: 11 }} />
            <YAxis dataKey={y2Field} type="number" tick={{ fontSize: 11 }} />
            <Tooltip formatter={(value) => numberText(value)} />
            <Scatter dataKey={y2Field} fill="#18705b" isAnimationActive={false} />
          </ScatterChart>
        ) : (
          <LineChart data={result.rows}>
            <CartesianGrid stroke="#d9ddd2" vertical={false} />
            <XAxis dataKey={xField} tick={{ fontSize: 11 }} />
            <YAxis tick={{ fontSize: 11 }} />
            <Tooltip formatter={(value) => numberText(value)} />
            <Line type="monotone" dataKey={yField} stroke="#18705b" dot={false} strokeWidth={2} isAnimationActive={false} />
          </LineChart>
        )}
      </ResponsiveContainer>
    </div>
  );
}

function RowDrawer({ row, onClose }: { row: JsonRecord; onClose: () => void }) {
  return (
    <div className="fixed inset-y-0 right-0 z-40 w-full max-w-xl border-l border-line bg-white shadow-hairline">
      <div className="flex items-center justify-between border-b border-line px-4 py-3">
        <div className="flex items-center gap-2">
          <FileJson className="h-4 w-4 text-ink/60" />
          <h2 className="text-sm font-semibold text-ink">Row Detail</h2>
        </div>
        <IconButton label="Close row detail" onClick={onClose}>
          <X className="h-4 w-4" />
        </IconButton>
      </div>
      <pre className="h-[calc(100vh-58px)] overflow-auto bg-panel p-4 text-xs leading-relaxed text-ink/75">{JSON.stringify(row, null, 2)}</pre>
    </div>
  );
}

function buildRequest(dataset: string, filters: BuilderFilter[], groupBy: string[], metrics: string[], limit: number): QueryRequest {
  return {
    dataset,
    filters: filters
      .filter((filter) => filter.field && (String(filter.value ?? "").trim() || filter.op === "eq"))
      .map((filter) => ({
        field: filter.field,
        op: filter.op,
        value: parseFilterValue(filter.value)
      })),
    group_by: groupBy,
    metrics,
    sort: groupBy.length ? groupBy.map((field) => ({ field, direction: "asc" as const })) : [],
    limit
  };
}

function defaultFilter(dataset?: QueryDatasetSchema): BuilderFilter {
  return {
    id: crypto.randomUUID(),
    field: dataset?.filters[0] ?? "market_id",
    op: "contains",
    value: ""
  };
}

function applyTemplate(
  template: QueryTemplate,
  setDataset: (dataset: string) => void,
  setFilters: (filters: BuilderFilter[]) => void,
  setGroupBy: (fields: string[]) => void,
  setMetrics: (metrics: string[]) => void,
  setLimit: (limit: number) => void
) {
  setDataset(template.request.dataset);
  setFilters((template.request.filters ?? []).map((filter) => ({ ...filter, id: crypto.randomUUID() })));
  setGroupBy(template.request.group_by ?? []);
  setMetrics(template.request.metrics ?? ["count"]);
  setLimit(template.request.limit ?? 250);
}

function parseFilterValue(value: unknown) {
  const text = String(value ?? "").trim();
  if (!text) {
    return "";
  }
  if (text === "null") {
    return null;
  }
  if (text === "true") {
    return true;
  }
  if (text === "false") {
    return false;
  }
  const numeric = Number(text);
  return Number.isFinite(numeric) && /^-?\d+(\.\d+)?$/.test(text) ? numeric : text;
}

function toggle(values: string[], value: string) {
  return values.includes(value) ? values.filter((item) => item !== value) : [...values, value];
}
