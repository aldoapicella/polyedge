import type { ReactElement } from "react";
import { useMemo, useState } from "react";
import {
  ComposedChart,
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ReferenceLine,
  Scatter,
  ResponsiveContainer,
  ScatterChart,
  Tooltip,
  XAxis,
  YAxis
} from "recharts";
import { formatChartTime, type ChartPoint, type ChartSummary } from "@/lib/charting";
import { dateTime, numberText } from "@/lib/format";
import type { MarketSummary, RuntimeEvent } from "@/lib/types";
import { EmptyState, Panel, PanelHeader, Pill } from "@/components/ui";
import { toneDot } from "./model";
import type { Tone } from "./types";

type ChartToggle = "probability" | "books" | "fills" | "decisions" | "distance" | "markers";

export function MarketMainChart({
  points,
  domain,
  sampleCount,
  summary,
  active,
  events = []
}: {
  points: ChartPoint[];
  domain: [number, number];
  sampleCount: number;
  summary?: ChartSummary;
  active?: MarketSummary | null;
  events?: RuntimeEvent[];
}) {
  const [toggles, setToggles] = useState<Record<ChartToggle, boolean>>({
    probability: true,
    books: true,
    fills: true,
    decisions: true,
    distance: false,
    markers: true
  });
  const markers = useMemo(() => eventMarkers(events, active, domain, toggles), [active, domain, events, toggles]);
  const fillPoints = points.filter((point) => point.fillPrice !== undefined);
  return (
    <Panel className="min-w-0 xl:col-span-8">
      <PanelHeader
        title="Market Probability & Price"
        meta={`${points.length} visible · ${sampleCount} market-window samples · q ${summary?.qSampleCount ?? qCount(points)}`}
        help="Full active-market window. Lines update in place; the x-axis stays pinned to market start and end."
      >
        <ChartToggleBar toggles={toggles} onToggle={(toggle) => setToggles((current) => ({ ...current, [toggle]: !current[toggle] }))} />
      </PanelHeader>
      <div className="h-[400px] p-3">
        {points.length ? (
          <ResponsiveContainer width="100%" height="100%">
            <ComposedChart data={points}>
              <CartesianGrid stroke="#d9ddd2" strokeDasharray="3 3" />
              <XAxis
                dataKey="bucket"
                type="number"
                domain={domain}
                tick={{ fontSize: 11 }}
                minTickGap={28}
                tickFormatter={formatChartTime}
              />
              <YAxis yAxisId="probability" domain={[0, 1]} tick={{ fontSize: 11 }} width={36} />
              {toggles.distance ? <YAxis yAxisId="distance" orientation="right" tick={{ fontSize: 11 }} width={44} /> : null}
              <Tooltip formatter={(value) => numberText(value, 3)} />
              <Legend />
              <ReferenceLine x={domain[0]} yAxisId="probability" stroke="#17201b" strokeOpacity={0.4} label={{ value: "start", fontSize: 10 }} />
              <ReferenceLine x={domain[1]} yAxisId="probability" stroke="#17201b" strokeOpacity={0.4} label={{ value: "end", fontSize: 10 }} />
              {markers.map((marker) => (
                <ReferenceLine
                  key={`${marker.bucket}-${marker.label}`}
                  x={marker.bucket}
                  yAxisId="probability"
                  stroke={marker.stroke}
                  strokeOpacity={0.55}
                  strokeDasharray={marker.dash}
                  label={{ value: marker.label, fontSize: 10 }}
                />
              ))}
              {toggles.probability ? (
                <>
                  <Line yAxisId="probability" type="monotone" dataKey="qUp" name="q Up" stroke="#18705b" dot={false} strokeWidth={2.4} connectNulls isAnimationActive={false} />
                  <Line yAxisId="probability" type="monotone" dataKey="qDown" name="q Down" stroke="#b3363a" dot={false} strokeWidth={2.4} connectNulls isAnimationActive={false} />
                </>
              ) : null}
              {toggles.books ? (
                <>
                  <Line yAxisId="probability" type="monotone" dataKey="upBid" name="UP bid" stroke="#2f7fcb" dot={false} strokeWidth={1.5} connectNulls isAnimationActive={false} />
                  <Line yAxisId="probability" type="monotone" dataKey="upAsk" name="UP ask" stroke="#74a8dd" dot={false} strokeWidth={1.5} strokeDasharray="4 4" connectNulls isAnimationActive={false} />
                  <Line yAxisId="probability" type="monotone" dataKey="downBid" name="DOWN bid" stroke="#a45d13" dot={false} strokeWidth={1.5} connectNulls isAnimationActive={false} />
                  <Line yAxisId="probability" type="monotone" dataKey="downAsk" name="DOWN ask" stroke="#d49a4e" dot={false} strokeWidth={1.5} strokeDasharray="4 4" connectNulls isAnimationActive={false} />
                </>
              ) : null}
              {toggles.distance ? (
                <Line yAxisId="distance" type="monotone" dataKey="distanceBps" name="reference bps" stroke="#17201b" dot={false} strokeWidth={1.8} connectNulls isAnimationActive={false} />
              ) : null}
              {toggles.fills ? <Scatter yAxisId="probability" data={fillPoints} dataKey="fillPrice" name="paper fills" fill="#a45d13" isAnimationActive={false} /> : null}
            </ComposedChart>
          </ResponsiveContainer>
        ) : (
          <EmptyState label="No probability or book samples yet. Keep the recorder running or inspect Data Quality for stale blobs." />
        )}
      </div>
      <ChartCoverage summary={summary} points={points} />
    </Panel>
  );
}

function ChartToggleBar({
  toggles,
  onToggle
}: {
  toggles: Record<ChartToggle, boolean>;
  onToggle: (toggle: ChartToggle) => void;
}) {
  const labels: { key: ChartToggle; label: string }[] = [
    { key: "probability", label: "q up/down" },
    { key: "books", label: "bid/ask" },
    { key: "fills", label: "fills" },
    { key: "decisions", label: "decisions" },
    { key: "distance", label: "distance" },
    { key: "markers", label: "markers" }
  ];
  return (
    <div className="hidden flex-wrap gap-1 lg:flex">
      {labels.map((item) => (
        <button
          key={item.key}
          className={[
            "h-7 rounded-sm border px-2 text-[11px] font-semibold transition",
            toggles[item.key] ? "border-good bg-good text-white" : "border-line bg-white text-ink/60 hover:bg-panel"
          ].join(" ")}
          onClick={() => onToggle(item.key)}
        >
          {item.label}
        </button>
      ))}
    </div>
  );
}

function eventMarkers(
  events: RuntimeEvent[],
  active: MarketSummary | null | undefined,
  domain: [number, number],
  toggles: Record<ChartToggle, boolean>
) {
  if (!toggles.markers) {
    return [];
  }
  const output = new Map<string, { bucket: number; label: string; stroke: string; dash?: string }>();
  for (const event of events) {
    const bucket = new Date(event.ts).getTime();
    if (!Number.isFinite(bucket) || bucket < domain[0] || bucket > domain[1]) {
      continue;
    }
    const marker = markerForEvent(event, active, toggles);
    if (!marker) {
      continue;
    }
    output.set(`${Math.floor(bucket / 5000)}-${marker.label}`, { bucket, ...marker });
    if (output.size >= 40) {
      break;
    }
  }
  return [...output.values()].sort((left, right) => left.bucket - right.bucket);
}

function markerForEvent(event: RuntimeEvent, active: MarketSummary | null | undefined, toggles: Record<ChartToggle, boolean>) {
  if ((event.type === "paper_fill" || event.type === "execution_report") && toggles.fills) {
    return { label: "fill", stroke: "#a45d13" };
  }
  if (event.type === "decision" && toggles.decisions) {
    const action = String(event.data.action ?? "decision").toLowerCase();
    if (action.includes("cancel")) {
      return { label: "cancel", stroke: "#b3363a", dash: "3 3" };
    }
    if (action.includes("place")) {
      return { label: "quote", stroke: "#18705b", dash: "3 3" };
    }
    return { label: "decision", stroke: "#17201b", dash: "2 4" };
  }
  if (event.type.includes("regime")) {
    return { label: "regime", stroke: "#2f7fcb", dash: "4 4" };
  }
  if (event.type === "paper_settlement") {
    return { label: "settle", stroke: "#17201b" };
  }
  if (event.type === "feed_error") {
    return { label: "feed error", stroke: "#b3363a" };
  }
  if (event.type === "market_start_price" && active?.market_id === event.data.market_id) {
    return { label: "start price", stroke: "#17201b", dash: "2 2" };
  }
  return null;
}

export function TrendCharts({
  points,
  fills,
  domain,
  summary
}: {
  points: ChartPoint[];
  fills: ChartPoint[];
  domain: [number, number];
  summary?: ChartSummary;
}) {
  const distancePoints = points.filter((point) => Number.isFinite(point.distanceBps));
  return (
    <div className="grid gap-5 xl:grid-cols-3">
      <ChartPanel
        title="Probability Trend"
        meta={`${points.length} buckets`}
        empty="No probability samples"
        hasData={points.length > 0}
        help="Model probability and UP quote levels over the visible market window. q lines are model-only and are not derived from quotes."
      >
        <LineChart data={points}>
          <CartesianGrid stroke="#d9ddd2" strokeDasharray="3 3" />
          <XAxis dataKey="bucket" type="number" domain={domain} tick={{ fontSize: 11 }} minTickGap={24} tickFormatter={formatChartTime} />
          <YAxis domain={[0, 1]} tick={{ fontSize: 11 }} width={32} />
          <Tooltip formatter={(value) => numberText(value, 3)} />
          <Line type="monotone" dataKey="qUp" name="q Up" stroke="#18705b" dot={false} strokeWidth={2} connectNulls isAnimationActive={false} />
          <Line type="monotone" dataKey="qDown" name="q Down" stroke="#b3363a" dot={false} strokeWidth={2} connectNulls isAnimationActive={false} />
          <Line type="monotone" dataKey="upBid" name="UP bid" stroke="#2f7fcb" dot={false} strokeWidth={1.3} connectNulls isAnimationActive={false} />
          <Line type="monotone" dataKey="upAsk" name="UP ask" stroke="#74a8dd" dot={false} strokeWidth={1.3} strokeDasharray="4 4" connectNulls isAnimationActive={false} />
        </LineChart>
      </ChartPanel>
      <ChartPanel
        title="q Coverage"
        meta={`${summary?.qSampleCount ?? qCount(points)} model samples`}
        empty="No model probability samples"
        hasData={points.length > 0}
        help="Coverage diagnostics for model-sourced q Up/q Down samples. Warnings indicate real data gaps, not quote-derived estimates."
      >
        <LineChart data={coverageRows(points)}>
          <CartesianGrid stroke="#d9ddd2" strokeDasharray="3 3" />
          <XAxis dataKey="bucket" type="number" domain={domain} tick={{ fontSize: 11 }} minTickGap={24} tickFormatter={formatChartTime} />
          <YAxis domain={[0, 1]} tick={{ fontSize: 11 }} width={32} />
          <Tooltip formatter={(value) => numberText(value, 0)} />
          <Line type="stepAfter" dataKey="qPresent" name="q present" stroke="#18705b" dot={false} strokeWidth={2} isAnimationActive={false} />
          <Line type="stepAfter" dataKey="bookPresent" name="book present" stroke="#a45d13" dot={false} strokeWidth={1.5} isAnimationActive={false} />
        </LineChart>
      </ChartPanel>
      <ChartPanel
        title="Reference Distance"
        meta="bps from start"
        empty="No reference samples"
        hasData={distancePoints.length > 0}
        help="Reference price move from the market start price. Positive means above start, negative means below start."
      >
        <LineChart data={distancePoints}>
          <CartesianGrid stroke="#d9ddd2" strokeDasharray="3 3" />
          <XAxis dataKey="bucket" type="number" domain={domain} tick={{ fontSize: 11 }} minTickGap={24} tickFormatter={formatChartTime} />
          <YAxis tick={{ fontSize: 11 }} width={42} tickFormatter={(value) => `${value}`} />
          <Tooltip formatter={(value) => `${numberText(value, 1)} bps`} />
          <ReferenceLine y={0} stroke="#17201b" strokeOpacity={0.35} />
          <Line type="monotone" dataKey="distanceBps" name="distance" stroke="#18705b" dot={false} strokeWidth={2} connectNulls isAnimationActive={false} />
        </LineChart>
      </ChartPanel>
      <ChartPanel
        title="Paper Fills"
        meta={`${fills.length} fills`}
        empty="No paper fills yet"
        hasData={fills.length > 0}
        help="Simulated paper maker fills plotted at fill price inside the market window."
      >
        <ScatterChart data={fills}>
          <CartesianGrid stroke="#d9ddd2" strokeDasharray="3 3" />
          <XAxis dataKey="bucket" type="number" domain={domain} tick={{ fontSize: 11 }} minTickGap={24} tickFormatter={formatChartTime} />
          <YAxis dataKey="fillPrice" domain={[0, 1]} tick={{ fontSize: 11 }} width={32} />
          <Tooltip formatter={(value) => numberText(value, 2)} />
          <Scatter dataKey="fillPrice" name="fills" fill="#a45d13" isAnimationActive={false} />
        </ScatterChart>
      </ChartPanel>
    </div>
  );
}

function ChartCoverage({ summary, points }: { summary: ChartSummary | undefined; points: ChartPoint[] }) {
  const warnings = summary?.warnings ?? [];
  const qSamples = summary?.qSampleCount ?? qCount(points);
  const bookSamples = summary?.bookSampleCount ?? bookCount(points);
  return (
    <div className="flex flex-wrap items-center gap-2 border-t border-line px-4 py-3 text-xs text-ink/60">
      <Pill tone={qSamples > 0 ? "good" : "warn"}>q {numberText(qSamples, 0)}</Pill>
      <Pill tone={bookSamples > 0 ? "good" : "warn"}>books {numberText(bookSamples, 0)}</Pill>
      <span>first q {summary?.firstQTs ? dateTime(summary.firstQTs) : "n/a"}</span>
      <span>last q {summary?.lastQTs ? dateTime(summary.lastQTs) : "n/a"}</span>
      {warnings.length ? <span className="text-warn">{warnings.join(", ")}</span> : null}
    </div>
  );
}

function coverageRows(points: ChartPoint[]) {
  return points.map((point) => ({
    bucket: point.bucket,
    qPresent: point.qUp !== undefined || point.qDown !== undefined ? 1 : 0,
    bookPresent:
      point.upBid !== undefined || point.upAsk !== undefined || point.downBid !== undefined || point.downAsk !== undefined ? 1 : 0
  }));
}

function qCount(points: ChartPoint[]) {
  return points.filter((point) => point.qUp !== undefined || point.qDown !== undefined).length;
}

function bookCount(points: ChartPoint[]) {
  return points.filter(
    (point) => point.upBid !== undefined || point.upAsk !== undefined || point.downBid !== undefined || point.downAsk !== undefined
  ).length;
}

function ChartPanel({
  title,
  meta,
  empty,
  help,
  hasData,
  children
}: {
  title: string;
  meta: string;
  empty: string;
  help?: string;
  hasData: boolean;
  children: ReactElement;
}) {
  return (
    <Panel className="min-w-0">
      <PanelHeader title={title} meta={meta} help={help} />
      <div className="h-64 p-3">
        {hasData ? <ResponsiveContainer width="100%" height="100%">{children}</ResponsiveContainer> : <EmptyState label={empty} />}
      </div>
    </Panel>
  );
}

function MiniLegend({ tone, label }: { tone: Tone; label: string }) {
  return (
    <span className="inline-flex items-center gap-1 text-xs text-ink/55">
      <span className={toneDot(tone)} aria-hidden />
      {label}
    </span>
  );
}
